#!/usr/bin/env python3
"""
简化的实时测试 - 直接测试WebSocket到文件写入的完整流程
"""
import multiprocessing
import time
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

async def simple_live_test():
    """简化的实时测试"""
    print("=== 简化实时测试 ===")
    
    from src.binance_streamer.websocket_client import binance_websocket_client
    from src.binance_streamer.file_writer import (
        _flush_aggtrade_batch_optimized,
        _flush_depth_batch_optimized
    )
    
    # 创建数据收集
    collected_data = []
    
    class MockQueue:
        def __init__(self):
            self.data = []
        
        def put(self, item):
            self.data.append(item)
            collected_data.append(item)
            print(f"📊 收到数据: {item[0]} - {item[1].get('stream', 'unknown')}")
    
    mock_queue = MockQueue()
    
    print("🌐 开始连接Binance WebSocket...")
    
    try:
        # 收集10秒的数据
        await asyncio.wait_for(
            binance_websocket_client('BTCUSDT', mock_queue, ['aggTrade', 'depth@0ms']),
            timeout=10.0
        )
    except asyncio.TimeoutError:
        print(f"⏰ 10秒收集完成，共收到 {len(collected_data)} 条数据")
    except Exception as e:
        print(f"❌ WebSocket错误: {e}")
        return False
    
    if not collected_data:
        print("❌ 没有收集到数据")
        return False
    
    # 分类数据
    aggtrade_data = [item for item in collected_data if item[0] == 'aggtrade']
    depth_data = [item for item in collected_data if item[0] == 'depth']
    
    print(f"📈 aggtrade数据: {len(aggtrade_data)}条")
    print(f"📊 depth数据: {len(depth_data)}条")
    
    # 测试优化版写入
    print("\n💾 测试优化版写入...")
    
    start_time = time.time()
    
    try:
        if aggtrade_data:
            aggtrade_records = [item[1] for item in aggtrade_data]
            _flush_aggtrade_batch_optimized('LIVETEST', aggtrade_records)
            print(f"✅ aggtrade写入完成: {len(aggtrade_records)}条")
        
        if depth_data:
            depth_records = [item[1] for item in depth_data]
            _flush_depth_batch_optimized('LIVETEST', depth_records)
            print(f"✅ depth写入完成: {len(depth_records)}条")
    
    except Exception as e:
        print(f"❌ 写入失败: {e}")
        return False
    
    write_time = time.time() - start_time
    total_records = len(aggtrade_data) + len(depth_data)
    
    print(f"\n⚡ 写入性能:")
    print(f"   总记录数: {total_records}")
    print(f"   写入耗时: {write_time:.4f}s")
    print(f"   写入速度: {total_records/write_time:.0f} 记录/秒")
    
    return True

def verify_output_files():
    """验证输出文件"""
    print("\n=== 验证输出文件 ===")
    
    data_dir = './data/LIVETEST'
    if not os.path.exists(data_dir):
        print("❌ 数据目录不存在")
        return False
    
    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    if not files:
        print("❌ 没有生成CSV文件")
        return False
    
    print(f"✅ 生成了 {len(files)} 个文件:")
    
    for file in sorted(files):
        filepath = os.path.join(data_dir, file)
        file_size = os.path.getsize(filepath)
        
        with open(filepath, 'r') as f:
            lines = f.readlines()
            line_count = len(lines)
        
        print(f"  📄 {file}")
        print(f"     行数: {line_count}, 大小: {file_size}字节")
        
        if line_count > 1:
            print(f"     头部: {lines[0].strip()}")
            if line_count > 2:
                print(f"     示例: {lines[1].strip()[:80]}...")
        print()
    
    return True

if __name__ == '__main__':
    success = False
    try:
        success = asyncio.run(simple_live_test())
        if success:
            verify_output_files()
            print("🎉 优化版系统测试完全成功!")
        else:
            print("❌ 测试失败")
    except Exception as e:
        print(f"❌ 测试出错: {e}")
        import traceback
        traceback.print_exc()