#!/usr/bin/env python3
"""
测试实时系统运行 - 验证优化版写入功能
"""
import multiprocessing
import time
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_live_data_collection():
    """测试实时数据收集和写入"""
    print("=== 测试实时数据收集 ===")
    
    from src.binance_streamer.websocket_client import binance_websocket_client
    from src.binance_streamer.file_writer import multi_queue_writer_process
    
    # 创建数据队列
    data_queue = multiprocessing.Queue(maxsize=1000)
    
    # 启动写入进程
    writer_process = multiprocessing.Process(
        target=multi_queue_writer_process,
        args=(data_queue, 0),
        name='test-writer'
    )
    writer_process.start()
    print(f"✅ 写入进程已启动 (PID: {writer_process.pid})")
    
    async def collect_data():
        """收集数据"""
        print("🌐 连接到Binance WebSocket...")
        
        # 收集BTCUSDT数据，持续20秒
        await asyncio.wait_for(
            binance_websocket_client('BTCUSDT', data_queue, ['aggTrade', 'depth@0ms']),
            timeout=20.0
        )
    
    try:
        # 运行数据收集
        asyncio.run(collect_data())
    except asyncio.TimeoutError:
        print("⏰ 20秒数据收集完成")
    except Exception as e:
        print(f"❌ 数据收集出错: {e}")
    
    # 关闭写入进程
    print("🛑 正在关闭写入进程...")
    time.sleep(2)  # 让写入进程处理完剩余数据
    writer_process.terminate()
    writer_process.join(timeout=5)
    
    if writer_process.is_alive():
        writer_process.kill()
        print("强制关闭写入进程")
    else:
        print("✅ 写入进程已正常关闭")

def check_generated_files():
    """检查生成的文件"""
    print("\n=== 检查生成的文件 ===")
    
    btc_dir = './data/BTCUSDT'
    if os.path.exists(btc_dir):
        files = os.listdir(btc_dir)
        if files:
            print(f"✅ 生成了 {len(files)} 个文件:")
            for file in sorted(files):
                filepath = os.path.join(btc_dir, file)
                if os.path.isfile(filepath):
                    # 检查文件大小和行数
                    file_size = os.path.getsize(filepath)
                    with open(filepath, 'r') as f:
                        line_count = sum(1 for _ in f)
                    print(f"  📄 {file}: {line_count}行, {file_size}字节")
                    
                    # 显示前3行
                    if line_count > 1:
                        with open(filepath, 'r') as f:
                            lines = [next(f, '').strip() for _ in range(3)]
                        print(f"     前3行: {lines[0][:50]}...")
                        if len(lines) > 2:
                            print(f"           {lines[2][:50]}...")
        else:
            print("❌ 没有生成文件")
    else:
        print("❌ BTCUSDT目录不存在")

if __name__ == '__main__':
    print("开始测试优化后的实时系统...\n")
    
    try:
        test_live_data_collection()
        check_generated_files()
        print("\n🎉 实时系统测试完成!")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()