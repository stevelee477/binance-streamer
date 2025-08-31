import pandas as pd
from datetime import datetime
import multiprocessing
import queue
import os
import time
import json
from collections import defaultdict, deque
from typing import Dict, List, Any
from .config import config_manager

def get_daily_filename(prefix: str, symbol: str) -> str:
    """Returns a filename with the format prefix_symbol_YYYYMMDD.csv in symbol-specific folder."""
    storage_config = config_manager.get_storage_config()
    base_output_dir = storage_config.get('output_directory', './data')
    
    # 为每个交易对创建单独的文件夹
    symbol_dir = os.path.join(base_output_dir, symbol)
    
    # 确保交易对目录存在
    if not os.path.exists(symbol_dir):
        os.makedirs(symbol_dir, exist_ok=True)
    
    filename = f"{prefix}_{symbol}_{datetime.now().strftime('%Y%m%d')}.csv"
    return os.path.join(symbol_dir, filename)

def save_to_csv(df: pd.DataFrame, filename: str):
    """Appends a DataFrame to a CSV file."""
    try:
        header = not pd.io.common.file_exists(filename)
        df.to_csv(filename, mode='a', header=header, index=False)
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def writer_process(data_queue: multiprocessing.Queue, writer_id: int = 0):
    """A dedicated process for writing data from a queue to CSV files."""
    print(f"Writer process {writer_id} started.")
    while True:
        try:
            item = data_queue.get()
            if item is None:
                print(f"Writer process {writer_id} stopping.")
                break

            stream_type, data = item

            if stream_type == 'aggtrade':
                # 保存完整的aggTrade原始数据
                df = pd.DataFrame([data['data']])
                df['localtime'] = data['localtime']
                df['stream'] = data.get('stream')
                filename = get_daily_filename('aggtrade', data['data']['s'])
                save_to_csv(df, filename)
            elif stream_type == 'depth':
                # 将一个depth update存储为一行，bids和asks作为JSON字符串
                import json
                
                depth_record = {
                    'localtime': data['localtime'],
                    'stream': data.get('stream'),
                    'e': data['data']['e'],  # Event type
                    'E': data['data']['E'],  # Event time
                    'T': data['data']['T'],  # Transaction time
                    's': data['data']['s'],  # Symbol
                    'U': data['data']['U'],  # First update ID in event
                    'u': data['data']['u'],  # Final update ID in event
                    'pu': data['data']['pu'],  # Final update ID in last stream
                    'bids': json.dumps(data['data']['b']),  # Bids as JSON string
                    'asks': json.dumps(data['data']['a']),  # Asks as JSON string
                    'bids_count': len(data['data']['b']),   # Number of bid levels
                    'asks_count': len(data['data']['a'])    # Number of ask levels
                }
                
                depth_df = pd.DataFrame([depth_record])
                filename = get_daily_filename('depth', data['data']['s'])
                save_to_csv(depth_df, filename)
            elif stream_type == 'kline':
                # 保存完整的kline原始数据
                kline_data = data['data']['k'].copy()
                kline_data['localtime'] = data['localtime']
                kline_data['stream'] = data.get('stream')
                kline_data['event_type'] = data['data']['e']  # Event type
                kline_data['event_time'] = data['data']['E']  # Event time
                
                df = pd.DataFrame([kline_data])
                filename = get_daily_filename('kline_1m', data['data']['s'])
                save_to_csv(df, filename)
            elif stream_type == 'orderbook_summary':
                # 保存订单簿摘要数据
                import json
                
                orderbook_record = {
                    'timestamp': data['timestamp'],
                    'symbol': data['symbol'],
                    'last_update_id': data['last_update_id'],
                    'is_synchronized': data['is_synchronized'],
                    'best_bid': data['best_bid'],
                    'best_ask': data['best_ask'],
                    'spread': data['spread'],
                    'bids_count': data['bids_count'],
                    'asks_count': data['asks_count'],
                    'update_count': data['update_count'],
                    'resync_count': data['resync_count'],
                    'top_bids': json.dumps(data['top_bids']),  # JSON格式存储
                    'top_asks': json.dumps(data['top_asks'])   # JSON格式存储
                }
                
                orderbook_df = pd.DataFrame([orderbook_record])
                filename = get_daily_filename('orderbook', data['symbol'])
                save_to_csv(orderbook_df, filename)
            elif stream_type == 'depth_snapshot':
                symbol = data['symbol']
                storage_config = config_manager.get_storage_config()
                base_output_dir = storage_config.get('output_directory', './data')
                symbol_dir = os.path.join(base_output_dir, symbol)
                
                # 确保交易对目录存在
                if not os.path.exists(symbol_dir):
                    os.makedirs(symbol_dir, exist_ok=True)
                
                timestamp_str = datetime.fromtimestamp(data['localtime']).strftime('%Y%m%d')
                filename = os.path.join(symbol_dir, f"{symbol}_depth_snapshot_{timestamp_str}.csv")
                
                # 处理bids数据（买单），按价格从高到低排序
                bids = pd.DataFrame(data['bids'], columns=['price', 'quantity'])
                bids['price'] = bids['price'].astype(float)
                bids['quantity'] = bids['quantity'].astype(float)
                bids = bids.sort_values('price', ascending=False)  # 降序排列
                bids['type'] = 'bids'
                bids['rank'] = range(1, len(bids) + 1)
                
                # 处理asks数据（卖单），按价格从低到高排序
                asks = pd.DataFrame(data['asks'], columns=['price', 'quantity'])
                asks['price'] = asks['price'].astype(float)
                asks['quantity'] = asks['quantity'].astype(float)
                asks = asks.sort_values('price', ascending=True)   # 升序排列
                asks['type'] = 'asks'
                asks['rank'] = range(1, len(asks) + 1)
                
                # 合并数据，保持排序
                depth_df = pd.concat([bids, asks], ignore_index=True)
                depth_df['localtime'] = data['localtime']
                depth_df['lastUpdateId'] = data['lastUpdateId']
                
                # 重新排列列顺序
                columns_order = ['rank', 'type', 'price', 'quantity', 'localtime', 'lastUpdateId']
                depth_df = depth_df[columns_order]
                
                depth_df.to_csv(filename, index=False)
                print(f"Depth snapshot for {symbol} saved to {filename} (Bids: {len(bids)}, Asks: {len(asks)})")

        except queue.Empty:
            continue
        except Exception as e:
            print(f"An error occurred in the writer process: {e}")


def multi_queue_writer_process(symbol_queues: Dict[str, multiprocessing.Queue], writer_id: int = 0):
    """
    多队列写入进程，支持批量处理以提高性能
    减少DataFrame创建次数和磁盘I/O操作
    """
    print(f"Multi-queue writer process {writer_id} started for {len(symbol_queues)} symbols.")
    
    # 获取性能配置
    performance_config = config_manager.get_performance_config()
    batch_size = performance_config.get('batch_size', 100)
    flush_interval = performance_config.get('flush_interval', 1)  # 秒
    
    # 为每个数据类型维护批量缓冲区
    batches = defaultdict(lambda: defaultdict(list))  # {stream_type: {symbol: [records]}}
    last_flush = time.time()
    
    def flush_batches():
        """批量写入所有缓冲的数据"""
        nonlocal last_flush
        current_time = time.time()
        
        for stream_type, symbol_batches in batches.items():
            for symbol, records in symbol_batches.items():
                if not records:
                    continue
                    
                try:
                    if stream_type == 'aggtrade':
                        _flush_aggtrade_batch(symbol, records)
                    elif stream_type == 'depth':
                        _flush_depth_batch(symbol, records)
                    elif stream_type == 'kline':
                        _flush_kline_batch(symbol, records)
                    elif stream_type == 'orderbook_summary':
                        _flush_orderbook_batch(symbol, records)
                    elif stream_type == 'depth_snapshot':
                        _flush_depth_snapshot_batch(symbol, records)
                        
                    # 清空已处理的批次
                    records.clear()
                    
                except Exception as e:
                    print(f"Error flushing {stream_type} batch for {symbol}: {e}")
        
        last_flush = current_time
    
    while True:
        try:
            # 检查是否需要基于时间刷新
            current_time = time.time()
            if current_time - last_flush >= flush_interval:
                flush_batches()
            
            # 从所有队列收集数据
            any_data_received = False
            
            for symbol, data_queue in symbol_queues.items():
                try:
                    # 非阻塞获取数据
                    item = data_queue.get_nowait()
                    if item is None:
                        print(f"Writer process {writer_id} received stop signal from {symbol}.")
                        continue
                    
                    stream_type, data = item
                    
                    # 添加到批处理缓冲区
                    batches[stream_type][symbol].append(data)
                    any_data_received = True
                    
                    # 检查是否达到批次大小限制
                    if len(batches[stream_type][symbol]) >= batch_size:
                        # 立即刷新该类型的数据
                        try:
                            if stream_type == 'aggtrade':
                                _flush_aggtrade_batch(symbol, batches[stream_type][symbol])
                            elif stream_type == 'depth':
                                _flush_depth_batch(symbol, batches[stream_type][symbol])
                            elif stream_type == 'kline':
                                _flush_kline_batch(symbol, batches[stream_type][symbol])
                            elif stream_type == 'orderbook_summary':
                                _flush_orderbook_batch(symbol, batches[stream_type][symbol])
                            elif stream_type == 'depth_snapshot':
                                _flush_depth_snapshot_batch(symbol, batches[stream_type][symbol])
                                
                            batches[stream_type][symbol].clear()
                        except Exception as e:
                            print(f"Error processing {stream_type} batch for {symbol}: {e}")
                
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"Error reading from queue for {symbol}: {e}")
            
            # 如果没有数据，短暂休眠避免CPU空转
            if not any_data_received:
                time.sleep(0.001)  # 1ms
                
        except KeyboardInterrupt:
            print(f"Writer process {writer_id} interrupted, flushing remaining data...")
            flush_batches()
            break
        except Exception as e:
            print(f"An error occurred in the multi-queue writer process: {e}")
            
    print(f"Multi-queue writer process {writer_id} shutting down.")


def _flush_aggtrade_batch(symbol: str, records: List[Dict]):
    """批量写入aggTrade数据"""
    if not records:
        return
        
    # 构建DataFrame的数据
    df_data = []
    for data in records:
        record = data['data'].copy()
        record['localtime'] = data['localtime']
        record['stream'] = data.get('stream')
        df_data.append(record)
    
    df = pd.DataFrame(df_data)
    filename = get_daily_filename('aggtrade', symbol)
    save_to_csv(df, filename)


def _flush_depth_batch(symbol: str, records: List[Dict]):
    """批量写入depth数据"""
    if not records:
        return
        
    df_data = []
    for data in records:
        depth_record = {
            'localtime': data['localtime'],
            'stream': data.get('stream'),
            'e': data['data']['e'],
            'E': data['data']['E'],
            'T': data['data']['T'],
            's': data['data']['s'],
            'U': data['data']['U'],
            'u': data['data']['u'],
            'pu': data['data']['pu'],
            'bids': json.dumps(data['data']['b']),
            'asks': json.dumps(data['data']['a']),
            'bids_count': len(data['data']['b']),
            'asks_count': len(data['data']['a'])
        }
        df_data.append(depth_record)
    
    df = pd.DataFrame(df_data)
    filename = get_daily_filename('depth', symbol)
    save_to_csv(df, filename)


def _flush_kline_batch(symbol: str, records: List[Dict]):
    """批量写入kline数据"""
    if not records:
        return
        
    df_data = []
    for data in records:
        kline_data = data['data']['k'].copy()
        kline_data['localtime'] = data['localtime']
        kline_data['stream'] = data.get('stream')
        kline_data['event_type'] = data['data']['e']
        kline_data['event_time'] = data['data']['E']
        df_data.append(kline_data)
    
    df = pd.DataFrame(df_data)
    filename = get_daily_filename('kline_1m', symbol)
    save_to_csv(df, filename)


def _flush_orderbook_batch(symbol: str, records: List[Dict]):
    """批量写入orderbook摘要数据"""
    if not records:
        return
        
    df_data = []
    for data in records:
        orderbook_record = {
            'timestamp': data['timestamp'],
            'symbol': data['symbol'],
            'last_update_id': data['last_update_id'],
            'is_synchronized': data['is_synchronized'],
            'best_bid': data['best_bid'],
            'best_ask': data['best_ask'],
            'spread': data['spread'],
            'bids_count': data['bids_count'],
            'asks_count': data['asks_count'],
            'update_count': data['update_count'],
            'resync_count': data['resync_count'],
            'top_bids': json.dumps(data['top_bids']),
            'top_asks': json.dumps(data['top_asks'])
        }
        df_data.append(orderbook_record)
    
    df = pd.DataFrame(df_data)
    filename = get_daily_filename('orderbook', symbol)
    save_to_csv(df, filename)


def _flush_depth_snapshot_batch(symbol: str, records: List[Dict]):
    """批量写入depth snapshot数据（通常每个快照都单独写入）"""
    for data in records:
        storage_config = config_manager.get_storage_config()
        base_output_dir = storage_config.get('output_directory', './data')
        symbol_dir = os.path.join(base_output_dir, symbol)
        
        if not os.path.exists(symbol_dir):
            os.makedirs(symbol_dir, exist_ok=True)
        
        timestamp_str = datetime.fromtimestamp(data['localtime']).strftime('%Y%m%d')
        filename = os.path.join(symbol_dir, f"{symbol}_depth_snapshot_{timestamp_str}.csv")
        
        # 处理bids和asks数据
        bids = pd.DataFrame(data['bids'], columns=['price', 'quantity'])
        bids['price'] = bids['price'].astype(float)
        bids['quantity'] = bids['quantity'].astype(float)
        bids = bids.sort_values('price', ascending=False)
        bids['type'] = 'bids'
        bids['rank'] = range(1, len(bids) + 1)
        
        asks = pd.DataFrame(data['asks'], columns=['price', 'quantity'])
        asks['price'] = asks['price'].astype(float)
        asks['quantity'] = asks['quantity'].astype(float)
        asks = asks.sort_values('price', ascending=True)
        asks['type'] = 'asks'
        asks['rank'] = range(1, len(asks) + 1)
        
        # 合并数据
        depth_df = pd.concat([bids, asks], ignore_index=True)
        depth_df['localtime'] = data['localtime']
        depth_df['lastUpdateId'] = data['lastUpdateId']
        
        columns_order = ['rank', 'type', 'price', 'quantity', 'localtime', 'lastUpdateId']
        depth_df = depth_df[columns_order]
        
        depth_df.to_csv(filename, index=False)
        print(f"Depth snapshot for {symbol} saved to {filename} (Bids: {len(bids)}, Asks: {len(asks)})")
