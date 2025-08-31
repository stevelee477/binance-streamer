"""
本地订单簿管理器
根据币安官方文档正确维护本地订单簿
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
"""
import asyncio
import json
import time
import logging
from typing import Dict, List, Optional, Tuple
from collections import deque
from sortedcontainers import SortedDict
import aiohttp


def neg(x):
    """用于bids的降序排序"""
    return -x


class LocalOrderBook:
    """本地订单簿类"""
    
    def __init__(self, symbol: str, max_depth: int = 1000):
        self.symbol = symbol
        self.max_depth = max_depth
        
        # 使用SortedDict优化排序性能 
        # bids按价格降序（负价格实现），asks按价格升序
        self.bids = SortedDict(neg)  # 买单，价格从高到低（使用负值排序）
        self.asks = SortedDict()  # 卖单，价格从低到高
        
        # 状态管理
        self.last_update_id = 0
        self.is_synchronized = False
        self.last_sync_time = 0
        
        # 统计信息
        self.update_count = 0
        self.resync_count = 0
        self.consecutive_failures = 0  # 连续失败次数
        self.last_resync_time = 0  # 上次重同步时间
        
        self.logger = logging.getLogger(f"{__name__}.{symbol}")
    
    def get_best_bid_ask(self) -> Tuple[Optional[float], Optional[float]]:
        """获取最佳买卖价"""
        # SortedDict中第一个元素就是最优价格
        best_bid = -self.bids.peekitem(0)[0] if self.bids else None  # 反转负值
        best_ask = self.asks.peekitem(0)[0] if self.asks else None
        return best_bid, best_ask
    
    def get_spread(self) -> Optional[float]:
        """获取买卖价差"""
        best_bid, best_ask = self.get_best_bid_ask()
        if best_bid and best_ask:
            return best_ask - best_bid
        return None
    
    def get_depth_summary(self, levels: int = 10) -> Dict:
        """获取深度摘要"""
        # SortedDict已经排序，直接取前N档
        top_bids = [(-price, qty) for price, qty in list(self.bids.items())[:levels]]  # 恢复正价格
        top_asks = list(self.asks.items())[:levels]
        
        best_bid, best_ask = self.get_best_bid_ask()
        spread = self.get_spread()
        
        return {
            'symbol': self.symbol,
            'timestamp': time.time(),
            'last_update_id': self.last_update_id,
            'is_synchronized': self.is_synchronized,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'spread': spread,
            'bids_count': len(self.bids),
            'asks_count': len(self.asks),
            'top_bids': [[str(price), str(qty)] for price, qty in top_bids],
            'top_asks': [[str(price), str(qty)] for price, qty in top_asks],
            'update_count': self.update_count,
            'resync_count': self.resync_count
        }
    
    def initialize_from_snapshot(self, snapshot_data: Dict):
        """从REST API快照初始化订单簿"""
        self.bids.clear()
        self.asks.clear()
        
        # 初始化bids - 使用负价格实现降序
        for price_str, qty_str in snapshot_data['bids']:
            price, qty = float(price_str), float(qty_str)
            if qty > 0:
                self.bids[-price] = qty  # 存储负价格
        
        # 初始化asks  
        for price_str, qty_str in snapshot_data['asks']:
            price, qty = float(price_str), float(qty_str)
            if qty > 0:
                self.asks[price] = qty
        
        self.last_update_id = snapshot_data['lastUpdateId']
        self.is_synchronized = True
        self.last_sync_time = time.time()
        self.consecutive_failures = 0  # 重置失败计数
        
        self.logger.info(f"订单簿初始化完成: {len(self.bids)} bids, {len(self.asks)} asks, lastUpdateId: {self.last_update_id}")
    
    def update_from_depth_event(self, event_data: Dict, is_initial_sync: bool = False) -> bool:
        """
        从深度更新事件更新订单簿
        返回是否成功更新（False表示需要重新同步）
        """
        if not self.is_synchronized:
            self.logger.warning("订单簿未同步，跳过更新")
            return False
        
        first_update_id = event_data['U']
        final_update_id = event_data['u']
        prev_final_update_id = event_data['pu']
        
        # 跳过已处理的事件
        if final_update_id <= self.last_update_id:
            return True
        
        # 对于初始同步过程，处理连续性
        if is_initial_sync:
            # 初始同步时，接受任何 final_update_id > last_update_id 的事件
            if final_update_id <= self.last_update_id:
                self.logger.debug(f"初始同步: 跳过已处理事件 u={final_update_id}, lastUpdateId={self.last_update_id}")
                return True
        else:
            # 严格按照币安文档验证更新ID连续性
            # pu must be equal to the previous event's u, no exceptions
            if prev_final_update_id != self.last_update_id:
                self.logger.warning(f"订单簿连续性中断: 期望pu={self.last_update_id}, 实际pu={prev_final_update_id}")
                self.consecutive_failures += 1
                self.is_synchronized = False
                return False
        
        # 应用更新
        self._apply_updates(event_data['b'], event_data['a'])
        
        self.last_update_id = final_update_id
        self.update_count += 1
        self.consecutive_failures = 0  # 重置连续失败计数
        
        return True
    
    def _apply_updates(self, bids_updates: List, asks_updates: List):
        """应用买卖单更新"""
        # 更新bids - 使用负价格
        for price_str, qty_str in bids_updates:
            price, qty = float(price_str), float(qty_str)
            neg_price = -price
            if qty == 0:
                # 数量为0，删除该价格档位
                self.bids.pop(neg_price, None)
            else:
                # 更新数量
                self.bids[neg_price] = qty
        
        # 更新asks
        for price_str, qty_str in asks_updates:
            price, qty = float(price_str), float(qty_str)
            if qty == 0:
                # 数量为0，删除该价格档位
                self.asks.pop(price, None)
            else:
                # 更新数量
                self.asks[price] = qty
        
        # 限制深度 - SortedDict已经排序，直接截取
        if len(self.bids) > self.max_depth:
            # 删除最差的价格档位（SortedDict末尾）
            while len(self.bids) > self.max_depth:
                self.bids.popitem(-1)  # 删除最后一个（价格最低的bid）
        
        if len(self.asks) > self.max_depth:
            # 删除最差的价格档位（SortedDict末尾）
            while len(self.asks) > self.max_depth:
                self.asks.popitem(-1)  # 删除最后一个（价格最高的ask）


class OrderBookManager:
    """订单簿管理器"""
    
    def __init__(self, symbols: List[str], output_interval: int = 10, resync_threshold: int = 5, data_queue=None):
        self.symbols = symbols
        self.output_interval = output_interval
        self.resync_threshold = resync_threshold  # 重同步阈值
        self.data_queue = data_queue
        
        # 为每个交易对创建订单簿
        self.order_books: Dict[str, LocalOrderBook] = {}
        for symbol in symbols:
            self.order_books[symbol] = LocalOrderBook(symbol)
        
        # 事件缓冲区 - 在获取快照前缓冲WebSocket事件
        self.event_buffers: Dict[str, deque] = {}
        for symbol in symbols:
            self.event_buffers[symbol] = deque(maxlen=1000)
        
        self.logger = logging.getLogger(__name__)
        self.running = False
    
    async def initialize_orderbook(self, symbol: str, session: aiohttp.ClientSession):
        """
        初始化单个交易对的订单簿
        注意：在调用此方法前，WebSocket应该已经开始并在缓冲事件
        """
        orderbook = self.order_books[symbol]
        
        try:
            self.logger.info(f"开始初始化 {symbol} 订单簿，当前缓冲事件数: {len(self.event_buffers[symbol])}")
            
            # 获取深度快照
            url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
            async with session.get(url) as response:
                response.raise_for_status()
                snapshot_data = await response.json()
            
            # 从快照初始化订单簿
            orderbook.initialize_from_snapshot(snapshot_data)
            
            self.logger.info(f"{symbol} 快照初始化完成，lastUpdateId: {orderbook.last_update_id}")
            
            # 处理缓冲的事件
            await self._process_buffered_events(symbol)
            
            self.logger.info(f"{symbol} 订单簿初始化完成，同步状态: {orderbook.is_synchronized}")
            
        except Exception as e:
            self.logger.error(f"初始化 {symbol} 订单簿失败: {e}")
            orderbook.is_synchronized = False
    
    async def _process_buffered_events(self, symbol: str):
        """处理缓冲的事件"""
        orderbook = self.order_books[symbol]
        buffer = self.event_buffers[symbol]
        
        processed_count = 0
        skipped_count = 0
        
        # 创建临时列表以处理所有缓冲事件
        buffered_events = list(buffer)
        buffer.clear()
        
        # 按update ID排序确保处理顺序正确
        buffered_events.sort(key=lambda x: x['u'])
        
        first_valid_event_found = False
        
        for event in buffered_events:
            first_update_id = event['U']
            final_update_id = event['u']
            
            # 丢弃 u < lastUpdateId 的事件
            if final_update_id < orderbook.last_update_id:
                skipped_count += 1
                continue
                
            # 第一个有效事件必须满足: U <= lastUpdateId AND u >= lastUpdateId
            if not first_valid_event_found:
                if first_update_id <= orderbook.last_update_id <= final_update_id:
                    first_valid_event_found = True
                    self.logger.info(f"{symbol} 找到有效的初始事件: U={first_update_id}, u={final_update_id}, lastUpdateId={orderbook.last_update_id}")
                else:
                    skipped_count += 1
                    continue
            
            # 使用初始同步模式处理缓冲事件
            if orderbook.update_from_depth_event(event, is_initial_sync=True):
                processed_count += 1
            else:
                # 如果处理失败，将剩余事件重新放入缓冲区
                buffer.extend(buffered_events[buffered_events.index(event):])
                self.logger.warning(f"{symbol} 缓冲事件处理失败，保留 {len(buffered_events) - buffered_events.index(event)} 个事件")
                break
        
        self.logger.info(f"{symbol} 处理了 {processed_count} 个缓冲事件，跳过 {skipped_count} 个过期事件")
    
    def handle_depth_event(self, symbol: str, event_data: Dict):
        """处理深度更新事件"""
        orderbook = self.order_books.get(symbol)
        if not orderbook:
            return
        
        if not orderbook.is_synchronized:
            # 如果未同步，先缓冲事件
            self.event_buffers[symbol].append(event_data)
            return
        
        # 尝试更新订单簿
        if not orderbook.update_from_depth_event(event_data):
            self.logger.warning(f"{symbol} 订单簿失去同步，连续失败: {orderbook.consecutive_failures}")
            
            # 检查是否需要重同步
            if orderbook.consecutive_failures >= self.resync_threshold:
                current_time = time.time()
                # 避免过于频繁的重同步（至少间隔30秒）
                if current_time - orderbook.last_resync_time > 30:
                    orderbook.last_resync_time = current_time
                    orderbook.resync_count += 1
                    self.logger.info(f"{symbol} 触发重同步，失败次数: {orderbook.consecutive_failures}")
                    # 标记为需要重同步，实际的重同步在别的地方处理
                
            # 将当前事件添加到缓冲区，准备重新同步后处理
            self.event_buffers[symbol].append(event_data)
    
    async def resync_orderbook(self, symbol: str, session: aiohttp.ClientSession):
        """
        重新同步订单簿
        注意：不清空缓冲区，继续在缓冲区收集事件，然后重新获取快照并处理
        """
        orderbook = self.order_books[symbol]
        
        self.logger.info(f"开始重新同步 {symbol} 订单簿，当前缓冲事件数: {len(self.event_buffers[symbol])}")
        
        try:
            # 标记为未同步状态，继续缓冲新事件
            orderbook.is_synchronized = False
            
            # 获取新的深度快照
            url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
            async with session.get(url) as response:
                response.raise_for_status()
                snapshot_data = await response.json()
            
            # 重新初始化订单簿
            orderbook.initialize_from_snapshot(snapshot_data)
            
            self.logger.info(f"{symbol} 重新同步快照完成，lastUpdateId: {orderbook.last_update_id}")
            
            # 处理缓冲的事件（包括失去同步期间收到的事件）
            await self._process_buffered_events(symbol)
            
            self.logger.info(f"{symbol} 重新同步完成，同步状态: {orderbook.is_synchronized}")
            
        except Exception as e:
            self.logger.error(f"重新同步 {symbol} 订单簿失败: {e}")
            orderbook.is_synchronized = False
    
    async def output_orderbook_summary(self):
        """定时输出订单簿摘要"""
        while self.running:
            try:
                for symbol, orderbook in self.order_books.items():
                    if orderbook.is_synchronized:
                        summary = orderbook.get_depth_summary()
                        spread_str = f"{summary['spread']:.4f}" if summary['spread'] is not None else 'N/A'
                        
                        # 输出到日志
                        self.logger.info(f"[{symbol}] 最佳买价: {summary['best_bid']}, "
                                       f"最佳卖价: {summary['best_ask']}, "
                                       f"价差: {spread_str}, "
                                       f"更新次数: {summary['update_count']}")
                        
                        # 保存到文件（如果有数据队列）
                        if self.data_queue:
                            try:
                                self.data_queue.put(('orderbook_summary', summary))
                            except Exception as e:
                                self.logger.error(f"发送订单簿数据到队列失败: {e}")
                    else:
                        self.logger.warning(f"[{symbol}] 订单簿未同步")
                
                await asyncio.sleep(self.output_interval)
                
            except Exception as e:
                self.logger.error(f"输出订单簿摘要时出错: {e}")
                await asyncio.sleep(self.output_interval)
    
    def start(self):
        """启动订单簿管理器"""
        self.running = True
        self.logger.info("订单簿管理器已启动")
    
    def stop(self):
        """停止订单簿管理器"""
        self.running = False
        self.logger.info("订单簿管理器已停止")
    
    def get_orderbook_status(self) -> Dict:
        """获取所有订单簿状态"""
        status = {}
        for symbol, orderbook in self.order_books.items():
            status[symbol] = {
                'synchronized': orderbook.is_synchronized,
                'last_update_id': orderbook.last_update_id,
                'update_count': orderbook.update_count,
                'resync_count': orderbook.resync_count,
                'bids_count': len(orderbook.bids),
                'asks_count': len(orderbook.asks),
                'best_bid_ask': orderbook.get_best_bid_ask(),
                'spread': orderbook.get_spread()
            }
        return status