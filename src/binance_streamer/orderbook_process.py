"""
订单簿管理进程
独立进程运行本地订单簿管理
"""
import asyncio
import multiprocessing
import time
import logging
from typing import List
import aiohttp

def run_orderbook_manager_process(symbols: List[str], orderbook_config: dict, network_config: dict, symbol_queues: dict):
    """运行订单簿管理器进程"""
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('binance_streamer.log'),
            logging.StreamHandler()
        ]
    )
    
    # 在进程内部导入
    import sys
    import os
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    from src.binance_streamer.orderbook_manager import OrderBookManager
    from src.binance_streamer.websocket_client import binance_websocket_client
    
    async def run():
        """运行订单簿管理"""
        try:
            # 创建订单簿管理器
            output_interval = orderbook_config.get('output_interval', 10)
            resync_threshold = orderbook_config.get('resync_threshold', 5)
            # 创建汇总队列用于订单簿输出
            orderbook_output_queue = symbol_queues.get(symbols[0], multiprocessing.Queue()) if symbols else multiprocessing.Queue()
            orderbook_manager = OrderBookManager(symbols, output_interval, resync_threshold, orderbook_output_queue)
            orderbook_manager.start()
            
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=network_config.get('timeout', 30))
            ) as session:
                
                # 启动任务
                tasks = []
                
                # 为每个交易对启动WebSocket连接（先启动以开始缓冲）
                for symbol in symbols:
                    # 创建虚拟队列（订单簿管理器不需要数据队列）
                    dummy_queue = multiprocessing.Queue()
                    
                    # 启动WebSocket连接（只处理depth流用于订单簿）
                    task = asyncio.create_task(
                        binance_websocket_client_orderbook_only(
                            symbol, dummy_queue, ['depth@0ms'], orderbook_manager
                        )
                    )
                    tasks.append(task)
                    logging.info(f"启动 {symbol} 订单簿WebSocket连接")
                
                # 等待WebSocket连接建立并开始接收数据
                await asyncio.sleep(3)
                
                # 初始化所有订单簿（在WebSocket开始缓冲后）
                for symbol in symbols:
                    await orderbook_manager.initialize_orderbook(symbol, session)
                
                # 启动订单簿状态输出和监控任务
                tasks.append(orderbook_manager.output_orderbook_summary())
                tasks.append(orderbook_sync_monitor(orderbook_manager, session))
                
                # 运行所有任务
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except Exception as e:
            print(f"订单簿管理器进程出错: {e}")
    
    # 运行
    try:
        print(f"订单簿管理器进程启动，管理交易对: {symbols}")
        asyncio.run(run())
    except Exception as e:
        print(f"订单簿管理器进程异常退出: {e}")


async def binance_websocket_client_orderbook_only(symbol: str, dummy_queue, streams: list, orderbook_manager):
    """
    专用于订单簿管理的WebSocket客户端
    只处理depth数据，不保存到文件
    """
    import websockets
    import json
    
    stream_names = [f"{symbol.lower()}@depth@0ms"]
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"

    while True:
        try:
            async with websockets.connect(url) as websocket:
                print(f"[OrderBook] Connected to WebSocket for {symbol}")
                
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    if 'depth' in data.get('stream', ''):
                        # 处理订单簿更新
                        try:
                            orderbook_manager.handle_depth_event(symbol, data['data'])
                            
                            # 订单簿状态由监控任务处理，这里只处理事件
                                
                        except Exception as e:
                            print(f"[OrderBook] 处理 {symbol} 订单簿更新时出错: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[OrderBook] {symbol} 连接关闭: {e}，5秒后重连...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[OrderBook] {symbol} 发生错误: {e}，5秒后重连...")
            await asyncio.sleep(5)


async def orderbook_sync_monitor(orderbook_manager, session):
    """监控订单簿同步状态并自动重新同步"""
    import time
    
    while orderbook_manager.running:
        try:
            await asyncio.sleep(5)  # 每5秒检查一次，更快响应
            
            for symbol, orderbook in orderbook_manager.order_books.items():
                if not orderbook.is_synchronized:
                    # 检查是否需要重新同步
                    if (orderbook.consecutive_failures >= orderbook_manager.resync_threshold and 
                        time.time() - orderbook.last_resync_time > 5):  # 减少到5秒间隔，快速重同步
                        
                        logging.info(f"自动触发 {symbol} 重新同步 (失败次数: {orderbook.consecutive_failures})")
                        await orderbook_manager.resync_orderbook(symbol, session)
                    
        except Exception as e:
            logging.error(f"订单簿同步监控出错: {e}")
            await asyncio.sleep(10)