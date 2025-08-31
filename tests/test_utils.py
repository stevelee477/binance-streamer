"""
测试工具函数
提供测试过程中常用的工具函数和数据生成器
"""
import time
import json
import os
import shutil
from typing import List, Dict, Any

def create_mock_aggtrade_data(symbol: str, count: int = 100) -> List[Dict]:
    """创建模拟的aggtrade数据"""
    base_time = time.time()
    base_price = 50000.0
    
    data = []
    for i in range(count):
        record = {
            'localtime': base_time + i * 0.001,
            'stream': f'{symbol.lower()}@aggTrade',
            'data': {
                'e': 'aggTrade',
                'E': int(base_time * 1000) + i,
                'a': 123456 + i,
                's': symbol,
                'p': f'{base_price + i * 0.1:.2f}',
                'q': f'{0.001 + i * 0.0001:.4f}',
                'f': 100 + i,
                'l': 200 + i,
                'T': int(base_time * 1000) + i,
                'm': i % 2 == 0
            }
        }
        data.append(record)
    
    return data

def create_mock_depth_data(symbol: str, count: int = 100) -> List[Dict]:
    """创建模拟的depth数据"""
    base_time = time.time()
    
    data = []
    for i in range(count):
        record = {
            'localtime': base_time + i * 0.001,
            'stream': f'{symbol.lower()}@depth@0ms',
            'data': {
                'e': 'depthUpdate',
                'E': int(base_time * 1000) + i,
                'T': int(base_time * 1000) + i,
                's': symbol,
                'U': 123456 + i,
                'u': 123457 + i,
                'pu': 123455 + i,
                'b': [
                    [f'{50000 - j * 0.1:.1f}', f'{j + 1}.0'] 
                    for j in range(5)
                ],
                'a': [
                    [f'{50000 + j * 0.1:.1f}', f'{j + 1}.0'] 
                    for j in range(5)
                ]
            }
        }
        data.append(record)
    
    return data

def create_mock_kline_data(symbol: str, count: int = 50) -> List[Dict]:
    """创建模拟的kline数据"""
    base_time = time.time()
    
    data = []
    for i in range(count):
        record = {
            'localtime': base_time + i * 0.001,
            'stream': f'{symbol.lower()}@kline_1m',
            'data': {
                'e': 'kline',
                'E': int(base_time * 1000) + i,
                'k': {
                    's': symbol,
                    't': int(base_time * 1000) + i * 60000,
                    'T': int(base_time * 1000) + (i + 1) * 60000,
                    'i': '1m',
                    'f': 100 + i,
                    'L': 200 + i,
                    'o': f'{50000 + i * 0.1:.2f}',
                    'c': f'{50000 + (i + 1) * 0.1:.2f}',
                    'h': f'{50000 + (i + 2) * 0.1:.2f}',
                    'l': f'{50000 + (i - 1) * 0.1:.2f}',
                    'v': f'{10.0 + i * 0.1:.1f}',
                    'n': 100 + i,
                    'x': True,
                    'q': f'{500000.0 + i * 100:.1f}',
                    'V': f'{5.0 + i * 0.05:.2f}',
                    'Q': f'{250000.0 + i * 50:.1f}',
                    'B': '0'
                }
            }
        }
        data.append(record)
    
    return data

def cleanup_test_data():
    """清理测试数据"""
    test_dirs = [
        './data/TESTBTC',
        './data/TESTBTC2', 
        './data/TESTOPT',
        './data/TESTPANDAS',
        './data/TESTOPTIMIZED',
        './data/LIVETEST'
    ]
    
    cleaned_count = 0
    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            try:
                shutil.rmtree(test_dir)
                cleaned_count += 1
                print(f"🗑️  清理测试目录: {test_dir}")
            except Exception as e:
                print(f"⚠️  清理失败 {test_dir}: {e}")
    
    print(f"✅ 清理了 {cleaned_count} 个测试目录")

def verify_file_format(filepath: str, expected_fields: List[str]) -> bool:
    """验证CSV文件格式"""
    if not os.path.exists(filepath):
        return False
    
    try:
        with open(filepath, 'r') as f:
            header = f.readline().strip()
            actual_fields = header.split(',')
            return actual_fields == expected_fields
    except Exception:
        return False

def count_records_in_file(filepath: str) -> int:
    """计算文件中的记录数"""
    if not os.path.exists(filepath):
        return 0
    
    try:
        with open(filepath, 'r') as f:
            return sum(1 for _ in f) - 1  # 减去头部行
    except Exception:
        return 0

def benchmark_function(func, *args, **kwargs) -> Tuple[Any, float]:
    """对函数进行基准测试"""
    start = time.time()
    result = func(*args, **kwargs)
    duration = time.time() - start
    return result, duration

def print_performance_summary(results: Dict[str, float]):
    """打印性能总结"""
    print("\n📊 性能总结:")
    for test_name, duration in results.items():
        print(f"  {test_name:30s}: {duration:.4f}s")

# 预定义的测试数据集大小
SMALL_DATASET = 100    # 快速测试
MEDIUM_DATASET = 1000  # 常规测试
LARGE_DATASET = 5000   # 性能测试

# CSV字段定义 (用于验证)
AGGTRADE_EXPECTED_FIELDS = [
    'e', 'E', 'a', 's', 'p', 'q', 'f', 'l', 'T', 'm', 'localtime', 'stream'
]

DEPTH_EXPECTED_FIELDS = [
    'localtime', 'stream', 'e', 'E', 'T', 's', 'U', 'u', 'pu',
    'bids', 'asks', 'bids_count', 'asks_count'
]

KLINE_EXPECTED_FIELDS = [
    'localtime', 'stream', 'event_type', 'event_time',
    's', 'k_t', 'k_T', 'k_s', 'k_i', 'k_f', 'k_L', 'k_o', 'k_c', 'k_h', 'k_l',
    'k_v', 'k_n', 'k_x', 'k_q', 'k_V', 'k_Q', 'k_B'
]