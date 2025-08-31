#!/usr/bin/env python3
"""
性能分析脚本 - 分析binance_streamer的性能瓶颈
"""
import cProfile
import pstats
import sys
import os
import threading
import time
import multiprocessing
import json
from collections import defaultdict

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

def profile_file_writer():
    """分析文件写入性能"""
    print("=== 分析文件写入性能 ===")
    
    from src.binance_streamer.file_writer import multi_queue_writer_process, _flush_depth_batch
    
    # 模拟大量深度数据
    test_data = []
    for i in range(5000):
        test_data.append({
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
        })
    
    profiler = cProfile.Profile()
    profiler.enable()
    
    try:
        _flush_depth_batch('TESTBTC', test_data)
    except Exception as e:
        print(f"文件写入测试失败: {e}")
    
    profiler.disable()
    return profiler

def profile_orderbook_operations():
    """分析订单簿操作性能"""
    print("=== 分析订单簿操作性能 ===")
    
    from src.binance_streamer.orderbook_manager import LocalOrderBook
    
    orderbook = LocalOrderBook('TESTBTC', 1000)
    
    # 模拟订单簿快照
    snapshot_data = {
        'lastUpdateId': 123456,
        'bids': [[f'{100 - i * 0.1:.1f}', f'{i + 1}.0'] for i in range(500)],
        'asks': [[f'{100 + i * 0.1:.1f}', f'{i + 1}.0'] for i in range(500)]
    }
    
    profiler = cProfile.Profile()
    profiler.enable()
    
    # 初始化订单簿
    orderbook.initialize_from_snapshot(snapshot_data)
    
    # 模拟大量更新
    for i in range(2000):
        update_data = {
            'U': 123456 + i,
            'u': 123456 + i + 1,
            'pu': 123456 + i - 1,
            'b': [[f'{99.5 + i * 0.01:.2f}', f'{i % 10 + 1}.0']],
            'a': [[f'{100.5 + i * 0.01:.2f}', f'{i % 10 + 1}.0']]
        }
        orderbook.update_from_depth_event(update_data)
        
        # 定期获取深度摘要
        if i % 200 == 0:
            orderbook.get_depth_summary(20)
    
    profiler.disable()
    return profiler

def profile_websocket_data_processing():
    """模拟WebSocket数据处理性能"""
    print("=== 分析WebSocket数据处理性能 ===")
    
    import json
    import time
    
    # 模拟WebSocket消息
    sample_messages = []
    for i in range(3000):
        msg = {
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
        sample_messages.append(json.dumps(msg))
    
    profiler = cProfile.Profile()
    profiler.enable()
    
    # 模拟WebSocket消息处理
    processed_data = []
    for message in sample_messages:
        data = json.loads(message)
        data['localtime'] = time.time()
        
        # 模拟数据处理逻辑
        stream = data['stream']
        if 'depth' in stream:
            processed_data.append(('depth', data))
    
    profiler.disable()
    return profiler

def analyze_profiler_results(profiler, component_name):
    """分析profiler结果"""
    print(f"\n=== {component_name} 性能分析 ===")
    stats = pstats.Stats(profiler)
    
    print(f"\n--- {component_name} 热点函数 (按累计时间) ---")
    stats.sort_stats('cumulative').print_stats(15)
    
    print(f"\n--- {component_name} 热点函数 (按自身时间) ---")
    stats.sort_stats('tottime').print_stats(10)
    
    return stats

def generate_comprehensive_report(all_stats):
    """生成综合性能报告"""
    report_file = 'performance_analysis_report.txt'
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=== Binance Streamer 性能分析报告 ===\n\n")
        f.write(f"分析时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # 合并所有统计数据
        combined_stats = all_stats[0]
        for stats in all_stats[1:]:
            combined_stats.add(stats)
        
        # 获取统计数据
        stats_dict = combined_stats.stats
        
        f.write("=== 整体性能热点函数 (按累计时间) ===\n")
        hot_functions = []
        for func, (cc, nc, tt, ct, callers) in stats_dict.items():
            filename, line, func_name = func
            if ct > 0.001:  # 累计时间超过1ms的函数
                hot_functions.append((func_name, filename, line, ct, tt, nc))
        
        # 按累计时间排序
        hot_functions.sort(key=lambda x: x[3], reverse=True)
        
        for i, (func_name, filename, line, cumtime, tottime, ncalls) in enumerate(hot_functions[:20]):
            f.write(f"{i+1:2d}. {func_name}\n")
            f.write(f"    文件: {os.path.basename(filename)}:{line}\n")
            f.write(f"    累计时间: {cumtime:.4f}s | 自身时间: {tottime:.4f}s | 调用次数: {ncalls}\n")
            if cumtime > 0:
                f.write(f"    平均每次调用: {cumtime/ncalls*1000:.2f}ms\n")
            f.write("\n")
        
        # 分析性能瓶颈类型
        f.write("=== 性能瓶颈分析 ===\n")
        
        # I/O操作时间
        io_time = sum(ct for (fname, line, func_name), (cc, nc, tt, ct, callers) in stats_dict.items()
                     if any(keyword in func_name.lower() for keyword in ['write', 'read', 'open', 'close', 'flush']))
        
        # 数据处理时间
        data_time = sum(ct for (fname, line, func_name), (cc, nc, tt, ct, callers) in stats_dict.items()
                       if any(keyword in func_name.lower() for keyword in ['json', 'pandas', 'dataframe', 'to_csv', 'loads', 'dumps']))
        
        # 排序和数据结构操作时间
        struct_time = sum(ct for (fname, line, func_name), (cc, nc, tt, ct, callers) in stats_dict.items()
                         if any(keyword in func_name.lower() for keyword in ['sort', 'sorted', '__setitem__', '__getitem__', 'update']))
        
        # 网络相关时间
        network_time = sum(ct for (fname, line, func_name), (cc, nc, tt, ct, callers) in stats_dict.items()
                          if any(keyword in func_name.lower() for keyword in ['websocket', 'connect', 'recv', 'send']))
        
        total_time = sum(ct for (fname, line, func_name), (cc, nc, tt, ct, callers) in stats_dict.items())
        
        f.write(f"总执行时间: {total_time:.4f}s\n")
        f.write(f"I/O操作时间: {io_time:.4f}s ({io_time/total_time*100:.1f}%)\n")
        f.write(f"数据处理时间: {data_time:.4f}s ({data_time/total_time*100:.1f}%)\n")
        f.write(f"数据结构操作时间: {struct_time:.4f}s ({struct_time/total_time*100:.1f}%)\n")
        f.write(f"网络操作时间: {network_time:.4f}s ({network_time/total_time*100:.1f}%)\n\n")
        
        # 性能优化建议
        f.write("=== 性能优化建议 ===\n")
        
        if io_time / total_time > 0.3:
            f.write("🔥 I/O操作占用时间过多 (>30%):\n")
            f.write("   - 考虑批量写入以减少磁盘I/O次数\n")
            f.write("   - 使用异步I/O或缓冲写入\n")
            f.write("   - 考虑压缩数据或更高效的存储格式\n\n")
        
        if data_time / total_time > 0.4:
            f.write("🔥 数据处理时间过多 (>40%):\n")
            f.write("   - 优化JSON解析，考虑使用更快的JSON库\n")
            f.write("   - 减少数据转换次数\n")
            f.write("   - 使用更高效的数据结构\n\n")
        
        if struct_time / total_time > 0.2:
            f.write("🔥 数据结构操作时间较多 (>20%):\n")
            f.write("   - 优化订单簿数据结构，考虑使用更高效的排序算法\n")
            f.write("   - 减少不必要的数据复制和排序操作\n")
            f.write("   - 考虑使用专门的金融数据结构库\n\n")
        
        # 具体函数优化建议
        f.write("=== 具体函数优化建议 ===\n")
        for func_name, filename, line, cumtime, tottime, ncalls in hot_functions[:10]:
            if cumtime > 0.01:  # 只关注累计时间超过10ms的函数
                f.write(f"函数: {func_name} ({os.path.basename(filename)}:{line})\n")
                if 'json' in func_name.lower():
                    f.write("  - 考虑使用orjson或ujson等更快的JSON库\n")
                elif 'to_csv' in func_name.lower():
                    f.write("  - 考虑批量写入或使用更高效的CSV写入方法\n")
                elif 'update' in func_name.lower() and 'orderbook' in filename.lower():
                    f.write("  - 优化订单簿更新算法，减少不必要的排序操作\n")
                elif 'sort' in func_name.lower():
                    f.write("  - 考虑使用更高效的排序算法或数据结构\n")
                f.write(f"  当前: {cumtime:.4f}s累计, {ncalls}次调用, 平均{cumtime/ncalls*1000:.2f}ms/次\n\n")
    
    print(f"\n详细性能分析报告已生成: {report_file}")
    return report_file

def main():
    print("开始Binance Streamer性能分析...")
    
    all_stats = []
    
    # 1. 分析文件写入性能
    profiler1 = profile_file_writer()
    stats1 = analyze_profiler_results(profiler1, "文件写入")
    all_stats.append(stats1)
    
    # 2. 分析订单簿操作性能
    profiler2 = profile_orderbook_operations()
    stats2 = analyze_profiler_results(profiler2, "订单簿操作")
    all_stats.append(stats2)
    
    # 3. 分析WebSocket数据处理性能
    profiler3 = profile_websocket_data_processing()
    stats3 = analyze_profiler_results(profiler3, "WebSocket数据处理")
    all_stats.append(stats3)
    
    # 生成综合报告
    report_file = generate_comprehensive_report(all_stats)
    
    print(f"\n=== 性能分析完成 ===")
    print(f"详细报告: {report_file}")

if __name__ == '__main__':
    main()