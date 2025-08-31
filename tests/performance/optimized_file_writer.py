"""
优化版文件写入器 - 去除pandas依赖，直接CSV写入
性能目标：减少60-70%的写入时间
"""
import csv
import os
import json
import time
from typing import List, Dict, Any
from datetime import datetime

# CSV字段定义 - 保持与原有格式一致
AGGTRADE_FIELDS = [
    'localtime', 'stream', 'e', 'E', 'a', 's', 'p', 'q', 'f', 'l', 'T', 'm'
]

DEPTH_FIELDS = [
    'localtime', 'stream', 'e', 'E', 'T', 's', 'U', 'u', 'pu',
    'bids', 'asks', 'bids_count', 'asks_count'
]

KLINE_FIELDS = [
    'localtime', 'stream', 'event_type', 'event_time',
    's', 'k_t', 'k_T', 'k_s', 'k_i', 'k_f', 'k_L', 'k_o', 'k_c', 'k_h', 'k_l',
    'k_v', 'k_n', 'k_x', 'k_q', 'k_V', 'k_Q', 'k_B'
]

ORDERBOOK_FIELDS = [
    'localtime', 'symbol', 'best_bid', 'best_ask', 'spread', 'update_count'
]

def ensure_csv_header(filepath: str, fields: List[str]) -> bool:
    """确保CSV文件有正确的头部，如果文件不存在则创建
    
    Returns:
        bool: True if header was written (new file), False if file already exists
    """
    file_exists = os.path.exists(filepath)
    
    if not file_exists:
        # 创建目录
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # 写入header
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
        return True
    
    return False

def append_csv_rows(filepath: str, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    """直接append写入CSV行，无需pandas
    
    Args:
        filepath: CSV文件路径
        rows: 要写入的行数据
        fields: CSV字段顺序
    """
    if not rows:
        return
        
    # 确保文件存在且有header
    ensure_csv_header(filepath, fields)
    
    # 直接append写入
    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerows(rows)

def get_daily_filename(stream_type: str, symbol: str) -> str:
    """生成日期文件名"""
    today = datetime.now().strftime('%Y%m%d')
    data_dir = f"./data/{symbol}"
    return f"{data_dir}/{stream_type}_{symbol}_{today}.csv"

# 优化版批量写入函数

def flush_aggtrade_batch_optimized(symbol: str, records: List[Dict]) -> None:
    """优化版聚合交易数据批量写入 - 无pandas"""
    if not records:
        return
    
    # 直接构造CSV行数据
    csv_rows = []
    for data in records:
        trade_data = data['data']
        row = {
            'localtime': data['localtime'],
            'stream': data.get('stream'),
            'e': trade_data['e'],
            'E': trade_data['E'],
            'a': trade_data['a'],
            's': trade_data['s'],
            'p': trade_data['p'],
            'q': trade_data['q'],
            'f': trade_data['f'],
            'l': trade_data['l'],
            'T': trade_data['T'],
            'm': trade_data['m']
        }
        csv_rows.append(row)
    
    # 直接写入CSV
    filename = get_daily_filename('aggtrade', symbol)
    append_csv_rows(filename, csv_rows, AGGTRADE_FIELDS)

def flush_depth_batch_optimized(symbol: str, records: List[Dict]) -> None:
    """优化版深度数据批量写入 - 无pandas"""
    if not records:
        return
    
    # 直接构造CSV行数据
    csv_rows = []
    for data in records:
        depth_data = data['data']
        row = {
            'localtime': data['localtime'],
            'stream': data.get('stream'),
            'e': depth_data['e'],
            'E': depth_data['E'],
            'T': depth_data['T'],
            's': depth_data['s'],
            'U': depth_data['U'],
            'u': depth_data['u'],
            'pu': depth_data['pu'],
            'bids': json.dumps(depth_data['b']),  # JSON序列化保持兼容性
            'asks': json.dumps(depth_data['a']),
            'bids_count': len(depth_data['b']),
            'asks_count': len(depth_data['a'])
        }
        csv_rows.append(row)
    
    # 直接写入CSV
    filename = get_daily_filename('depth', symbol)
    append_csv_rows(filename, csv_rows, DEPTH_FIELDS)

def flush_kline_batch_optimized(symbol: str, records: List[Dict]) -> None:
    """优化版K线数据批量写入 - 无pandas"""
    if not records:
        return
    
    # 直接构造CSV行数据
    csv_rows = []
    for data in records:
        kline_data = data['data']['k']
        row = {
            'localtime': data['localtime'],
            'stream': data.get('stream'),
            'event_type': data['data']['e'],
            'event_time': data['data']['E'],
            's': kline_data['s'],
            'k_t': kline_data['t'],
            'k_T': kline_data['T'],
            'k_s': kline_data['s'],
            'k_i': kline_data['i'],
            'k_f': kline_data['f'],
            'k_L': kline_data['L'],
            'k_o': kline_data['o'],
            'k_c': kline_data['c'],
            'k_h': kline_data['h'],
            'k_l': kline_data['l'],
            'k_v': kline_data['v'],
            'k_n': kline_data['n'],
            'k_x': kline_data['x'],
            'k_q': kline_data['q'],
            'k_V': kline_data['V'],
            'k_Q': kline_data['Q'],
            'k_B': kline_data['B']
        }
        csv_rows.append(row)
    
    # 直接写入CSV
    filename = get_daily_filename('kline_1m', symbol)
    append_csv_rows(filename, csv_rows, KLINE_FIELDS)

def flush_orderbook_batch_optimized(symbol: str, records: List[Dict]) -> None:
    """优化版订单簿数据批量写入 - 无pandas"""
    if not records:
        return
    
    # 直接构造CSV行数据
    csv_rows = []
    for data in records:
        row = {
            'localtime': data['localtime'],
            'symbol': data['symbol'],
            'best_bid': data['best_bid'],
            'best_ask': data['best_ask'],
            'spread': data['spread'],
            'update_count': data['update_count']
        }
        csv_rows.append(row)
    
    # 直接写入CSV
    filename = get_daily_filename('orderbook', symbol)
    append_csv_rows(filename, csv_rows, ORDERBOOK_FIELDS)

# 性能测试函数

def benchmark_write_performance():
    """对比pandas vs 直接CSV写入性能"""
    import time
    import pandas as pd
    
    print("=== 写入性能对比测试 ===")
    
    # 生成测试数据
    test_records = []
    for i in range(5000):
        record = {
            'localtime': time.time(),
            'stream': 'btcusdt@depth@0ms',
            'data': {
                'e': 'depthUpdate',
                'E': int(time.time() * 1000),
                'T': int(time.time() * 1000),
                's': 'BTCUSDT',
                'U': i,
                'u': i + 1,
                'pu': i - 1,
                'b': [['100.0', '1.0'], ['99.0', '2.0']],
                'a': [['101.0', '1.0'], ['102.0', '2.0']]
            }
        }
        test_records.append(record)
    
    # 测试原来的pandas写入
    print("\n1. 测试pandas写入...")
    start_time = time.time()
    
    # 模拟原来的写入方式
    df_data = []
    for data in test_records:
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
    pandas_filename = './data/TESTBTC/depth_pandas_test.csv'
    os.makedirs('./data/TESTBTC', exist_ok=True)
    df.to_csv(pandas_filename, index=False)
    
    pandas_time = time.time() - start_time
    print(f"pandas写入耗时: {pandas_time:.4f}s")
    
    # 测试优化版直接CSV写入
    print("\n2. 测试优化版直接CSV写入...")
    start_time = time.time()
    
    flush_depth_batch_optimized('TESTBTC2', test_records)
    
    optimized_time = time.time() - start_time
    print(f"优化版写入耗时: {optimized_time:.4f}s")
    
    # 性能提升计算
    improvement = (pandas_time - optimized_time) / pandas_time * 100
    speedup = pandas_time / optimized_time
    
    print(f"\n=== 性能提升结果 ===")
    print(f"性能提升: {improvement:.1f}%")
    print(f"速度倍数: {speedup:.1f}x")
    print(f"时间节省: {(pandas_time - optimized_time)*1000:.1f}ms")
    
    return improvement, speedup

if __name__ == '__main__':
    benchmark_write_performance()