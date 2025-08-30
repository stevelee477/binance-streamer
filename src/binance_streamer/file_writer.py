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
                df = pd.DataFrame([data['data']])
                df['localtime'] = data['localtime']
                filename = get_daily_filename('aggtrade', data['data']['s'])
                save_to_csv(df, filename)
            elif stream_type == 'depth':
                symbol = data['data']['s']
                bids = pd.DataFrame(data['data']['b'], columns=['price', 'quantity'])
                bids['type'] = 'bids'
                asks = pd.DataFrame(data['data']['a'], columns=['price', 'quantity'])
                asks['type'] = 'asks'
                
                depth_df = pd.concat([bids, asks])
                depth_df['localtime'] = data['localtime']
                depth_df['E'] = data['data']['E']
                depth_df['T'] = data['data'].get('T')
                depth_df['s'] = symbol
                
                filename = get_daily_filename('depth', symbol)
                save_to_csv(depth_df, filename)
            elif stream_type == 'kline':
                kline_data = data['data']['k']
                df = pd.DataFrame([kline_data])
                df['localtime'] = data['localtime']
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
                
                bids = pd.DataFrame(data['bids'], columns=['price', 'quantity'])
                bids['type'] = 'bids'
                asks = pd.DataFrame(data['asks'], columns=['price', 'quantity'])
                asks['type'] = 'asks'
                
                depth_df = pd.concat([bids, asks])
                depth_df['localtime'] = data['localtime']
                depth_df['lastUpdateId'] = data['lastUpdateId']
                
                depth_df.to_csv(filename, index=False)
                print(f"Depth snapshot for {symbol} saved to {filename}")

        except queue.Empty:
            continue
        except Exception as e:
            print(f"An error occurred in the writer process: {e}")
