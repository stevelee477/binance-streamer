#!/usr/bin/env python3
"""
测试优化后的写入功能集成
"""
import os
import sys
import time
import multiprocessing

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.binance_streamer.file_writer import (
    _flush_aggtrade_batch_optimized, 
    _flush_depth_batch_optimized,
    _flush_kline_batch_optimized
)

def test_optimized_functions():
    """测试优化版写入函数"""
    print("=== 测试优化版写入函数 ===")
    
    # 创建测试数据
    test_aggtrade = [{
        'localtime': time.time(),
        'stream': 'btcusdt@aggTrade',
        'data': {
            'e': 'aggTrade',
            'E': int(time.time() * 1000),
            'a': 123456,
            's': 'BTCUSDT',
            'p': '50000.0',
            'q': '0.001',
            'f': 100,
            'l': 200,
            'T': int(time.time() * 1000),
            'm': True
        }
    } for _ in range(100)]
    
    test_depth = [{
        'localtime': time.time(),
        'stream': 'btcusdt@depth@0ms',
        'data': {
            'e': 'depthUpdate',
            'E': int(time.time() * 1000),
            'T': int(time.time() * 1000),
            's': 'BTCUSDT',
            'U': 123456,
            'u': 123457,
            'pu': 123455,
            'b': [['50000.0', '1.0'], ['49999.0', '2.0']],
            'a': [['50001.0', '1.0'], ['50002.0', '2.0']]
        }
    } for _ in range(100)]
    
    test_kline = [{
        'localtime': time.time(),
        'stream': 'btcusdt@kline_1m',
        'data': {
            'e': 'kline',
            'E': int(time.time() * 1000),
            'k': {
                's': 'BTCUSDT',
                't': int(time.time() * 1000),
                'T': int(time.time() * 1000) + 60000,
                'i': '1m',
                'f': 100,
                'L': 200,
                'o': '50000.0',
                'c': '50100.0',
                'h': '50200.0',
                'l': '49900.0',
                'v': '10.0',
                'n': 100,
                'x': True,
                'q': '500000.0',
                'V': '5.0',
                'Q': '250000.0',
                'B': '0'
            }
        }
    } for _ in range(50)]
    
    # 测试写入
    start_time = time.time()
    
    try:
        print("测试aggtrade写入...")
        _flush_aggtrade_batch_optimized('TESTOPT', test_aggtrade)
        print("✅ aggtrade写入成功")
        
        print("测试depth写入...")
        _flush_depth_batch_optimized('TESTOPT', test_depth)
        print("✅ depth写入成功")
        
        print("测试kline写入...")
        _flush_kline_batch_optimized('TESTOPT', test_kline)
        print("✅ kline写入成功")
        
    except Exception as e:
        print(f"❌ 写入测试失败: {e}")
        return False
    
    total_time = time.time() - start_time
    print(f"\n总写入时间: {total_time:.4f}s")
    print(f"写入记录数: {len(test_aggtrade) + len(test_depth) + len(test_kline)}")
    
    return True

if __name__ == '__main__':
    if test_optimized_functions():
        print("\n🎉 优化版写入函数测试通过！")
    else:
        print("\n❌ 优化版写入函数测试失败！")