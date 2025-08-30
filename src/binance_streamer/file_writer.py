import pandas as pd
from datetime import datetime
import multiprocessing
import queue
import os
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

def writer_process(data_queue: multiprocessing.Queue):
    """A dedicated process for writing data from a queue to CSV files."""
    print("Writer process started.")
    while True:
        try:
            item = data_queue.get()
            if item is None:
                print("Writer process stopping.")
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
