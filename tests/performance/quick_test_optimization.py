#!/usr/bin/env python3
"""
快速测试优化版本的性能和正确性
"""
import os
import sys
import time
import cProfile
import pstats

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.binance_streamer.file_writer import (
    _flush_aggtrade_batch, 
    _flush_depth_batch,
    _flush_aggtrade_batch_optimized,
    _flush_depth_batch_optimized
)

def create_test_data():
    """创建测试数据"""
    aggtrade_data = []
    depth_data = []
    
    for i in range(1000):
        # aggtrade测试数据
        aggtrade_data.append({
            'localtime': time.time() + i * 0.001,
            'stream': 'btcusdt@aggTrade',
            'data': {
                'e': 'aggTrade',
                'E': int(time.time() * 1000) + i,
                'a': 123456 + i,
                's': 'BTCUSDT',
                'p': f'{50000 + i * 0.1:.2f}',
                'q': f'{0.001 + i * 0.0001:.4f}',
                'f': 100 + i,
                'l': 200 + i,
                'T': int(time.time() * 1000) + i,
                'm': i % 2 == 0
            }
        })
        
        # depth测试数据
        depth_data.append({
            'localtime': time.time() + i * 0.001,
            'stream': 'btcusdt@depth@0ms',
            'data': {
                'e': 'depthUpdate',
                'E': int(time.time() * 1000) + i,
                'T': int(time.time() * 1000) + i,
                's': 'BTCUSDT',
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
        })
    
    return aggtrade_data, depth_data

def test_pandas_vs_optimized():
    """对比测试pandas vs 优化版"""
    print("=== 性能对比测试 ===")
    
    aggtrade_data, depth_data = create_test_data()
    
    # 测试pandas版本
    print("1. 测试pandas版本...")
    profiler_pandas = cProfile.Profile()
    profiler_pandas.enable()
    
    start_pandas = time.time()
    _flush_aggtrade_batch('TESTPANDAS', aggtrade_data)
    _flush_depth_batch('TESTPANDAS', depth_data)
    pandas_time = time.time() - start_pandas
    
    profiler_pandas.disable()
    
    # 测试优化版本
    print("2. 测试优化版本...")
    profiler_optimized = cProfile.Profile()
    profiler_optimized.enable()
    
    start_optimized = time.time()
    _flush_aggtrade_batch_optimized('TESTOPTIMIZED', aggtrade_data)
    _flush_depth_batch_optimized('TESTOPTIMIZED', depth_data)
    optimized_time = time.time() - start_optimized
    
    profiler_optimized.disable()
    
    # 性能对比
    improvement = (pandas_time - optimized_time) / pandas_time * 100
    speedup = pandas_time / optimized_time
    
    print(f"\n=== 性能对比结果 ===")
    print(f"pandas版本耗时:   {pandas_time:.4f}s")
    print(f"优化版本耗时:     {optimized_time:.4f}s")
    print(f"性能提升:         {improvement:.1f}%")
    print(f"速度倍数:         {speedup:.1f}x")
    print(f"时间节省:         {(pandas_time - optimized_time)*1000:.1f}ms")
    
    return pandas_time, optimized_time, improvement

def verify_data_integrity():
    """验证数据完整性"""
    print("\n=== 数据完整性验证 ===")
    
    import pandas as pd
    
    # 读取pandas版本生成的文件
    try:
        pandas_aggtrade = pd.read_csv('./data/TESTPANDAS/aggtrade_TESTPANDAS_20250831.csv')
        pandas_depth = pd.read_csv('./data/TESTPANDAS/depth_TESTPANDAS_20250831.csv')
        
        print(f"pandas aggtrade记录数: {len(pandas_aggtrade)}")
        print(f"pandas depth记录数:    {len(pandas_depth)}")
        print(f"pandas aggtrade列:     {list(pandas_aggtrade.columns)}")
    except Exception as e:
        print(f"❌ 读取pandas版本文件失败: {e}")
        return False
    
    # 读取优化版本生成的文件
    try:
        optimized_aggtrade = pd.read_csv('./data/TESTOPTIMIZED/aggtrade_TESTOPTIMIZED_20250831.csv')
        optimized_depth = pd.read_csv('./data/TESTOPTIMIZED/depth_TESTOPTIMIZED_20250831.csv')
        
        print(f"优化版 aggtrade记录数: {len(optimized_aggtrade)}")
        print(f"优化版 depth记录数:    {len(optimized_depth)}")
        print(f"优化版 aggtrade列:     {list(optimized_aggtrade.columns)}")
    except Exception as e:
        print(f"❌ 读取优化版本文件失败: {e}")
        return False
    
    # 对比数据
    aggtrade_match = len(pandas_aggtrade) == len(optimized_aggtrade)
    depth_match = len(pandas_depth) == len(optimized_depth)
    columns_match = list(pandas_aggtrade.columns) == list(optimized_aggtrade.columns)
    
    print(f"\n记录数匹配: aggtrade={aggtrade_match}, depth={depth_match}")
    print(f"列名匹配:   {columns_match}")
    
    if aggtrade_match and depth_match and columns_match:
        print("✅ 数据完整性验证通过!")
        return True
    else:
        print("❌ 数据完整性验证失败!")
        return False

if __name__ == '__main__':
    print("开始快速测试优化版本...\n")
    
    # 性能测试
    pandas_time, optimized_time, improvement = test_pandas_vs_optimized()
    
    # 数据完整性验证
    integrity_ok = verify_data_integrity()
    
    print(f"\n=== 最终结果 ===")
    if improvement > 0 and integrity_ok:
        print(f"🎉 优化成功! 性能提升{improvement:.1f}%, 数据完整性验证通过")
    else:
        print("❌ 优化测试失败")