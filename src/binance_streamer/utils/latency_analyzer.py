#!/usr/bin/env python3
"""
延迟分析工具
用于分析币安数据流的接收延迟
"""

import csv
import statistics
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json


class LatencyAnalyzer:
    """数据延迟分析器"""
    
    def __init__(self):
        self.latency_buckets = [
            (0, 80, '<80ms'),
            (80, 100, '80-100ms'),
            (100, 150, '100-150ms'),
            (150, 200, '150-200ms'),
            (200, 300, '200-300ms'),
            (300, float('inf'), '>300ms')
        ]
    
    def analyze_file_latency(self, 
                            file_path: str, 
                            event_time_field: str = 'E',
                            local_time_field: str = 'localtime') -> Dict:
        """
        分析单个文件的延迟统计
        
        Args:
            file_path: CSV文件路径
            event_time_field: 事件时间字段名
            local_time_field: 本地接收时间字段名
            
        Returns:
            包含延迟统计信息的字典
        """
        latencies = []
        
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # 事件时间（毫秒）转为秒
                    event_time = float(row[event_time_field]) / 1000
                    # 本地接收时间（已经是秒）
                    local_time = float(row[local_time_field])
                    # 计算延迟（毫秒）
                    latency = (local_time - event_time) * 1000
                    latencies.append(latency)
                except (KeyError, ValueError):
                    continue
        
        if not latencies:
            return {}
        
        return {
            'count': len(latencies),
            'min': min(latencies),
            'max': max(latencies),
            'mean': statistics.mean(latencies),
            'median': statistics.median(latencies),
            'stdev': statistics.stdev(latencies) if len(latencies) > 1 else 0,
            'p95': self._percentile(latencies, 95),
            'p99': self._percentile(latencies, 99)
        }
    
    def analyze_latency_distribution(self, 
                                    file_path: str,
                                    event_time_field: str = 'E',
                                    local_time_field: str = 'localtime') -> Tuple[Dict, int]:
        """
        分析延迟分布
        
        Args:
            file_path: CSV文件路径
            event_time_field: 事件时间字段名
            local_time_field: 本地接收时间字段名
            
        Returns:
            (延迟分布字典, 总样本数)
        """
        latencies = []
        
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    event_time = float(row[event_time_field]) / 1000
                    local_time = float(row[local_time_field])
                    latency = (local_time - event_time) * 1000
                    latencies.append(latency)
                except (KeyError, ValueError):
                    continue
        
        if not latencies:
            return {}, 0
        
        # 统计延迟分布
        distribution = {}
        for latency in latencies:
            for low, high, label in self.latency_buckets:
                if low <= latency < high:
                    distribution[label] = distribution.get(label, 0) + 1
                    break
        
        return distribution, len(latencies)
    
    def analyze_directory(self, data_dir: str = './data') -> Dict:
        """
        分析整个数据目录的延迟情况
        
        Args:
            data_dir: 数据目录路径
            
        Returns:
            所有文件的延迟分析结果
        """
        results = {}
        data_path = Path(data_dir)
        
        if not data_path.exists():
            return results
        
        # 遍历所有CSV文件
        for csv_file in data_path.glob('**/*.csv'):
            # 跳过深度快照文件
            if 'depth_snapshot' in csv_file.name:
                continue
                
            # 获取交易对和数据类型
            symbol = csv_file.parent.name
            
            if 'aggtrade' in csv_file.name:
                data_type = 'aggtrade'
            elif 'depth' in csv_file.name:
                data_type = 'depth'
            elif 'kline' in csv_file.name:
                data_type = 'kline'
            else:
                continue
            
            # 分析延迟
            stats = self.analyze_file_latency(str(csv_file))
            if stats:
                key = f"{symbol}_{data_type}"
                results[key] = {
                    'file': str(csv_file),
                    'stats': stats
                }
        
        return results
    
    def print_analysis_report(self, data_dir: str = './data'):
        """
        打印延迟分析报告
        
        Args:
            data_dir: 数据目录路径
        """
        print("=" * 60)
        print("数据接收延迟分析报告")
        print("=" * 60)
        
        results = self.analyze_directory(data_dir)
        
        if not results:
            print("未找到数据文件")
            return
        
        # 按交易对和类型排序
        for key in sorted(results.keys()):
            data = results[key]
            stats = data['stats']
            
            print(f"\n{key}:")
            print(f"  文件: {Path(data['file']).name}")
            print(f"  样本数: {stats['count']:,}")
            print(f"  延迟统计 (ms):")
            print(f"    最小值: {stats['min']:.2f}")
            print(f"    中位数: {stats['median']:.2f}")
            print(f"    平均值: {stats['mean']:.2f}")
            print(f"    最大值: {stats['max']:.2f}")
            print(f"    标准差: {stats['stdev']:.2f}")
            print(f"    P95: {stats['p95']:.2f}")
            print(f"    P99: {stats['p99']:.2f}")
            
            # 分析延迟分布
            distribution, total = self.analyze_latency_distribution(data['file'])
            if distribution:
                print(f"  延迟分布:")
                for low, high, label in self.latency_buckets:
                    count = distribution.get(label, 0)
                    percentage = (count / total * 100) if total > 0 else 0
                    if count > 0:
                        bar = '█' * int(percentage / 2)
                        print(f"    {label:12s}: {count:5d} ({percentage:5.1f}%) {bar}")
        
        # 总结
        print("\n" + "=" * 60)
        print("性能总结:")
        
        all_medians = [results[k]['stats']['median'] for k in results]
        all_means = [results[k]['stats']['mean'] for k in results]
        
        if all_medians:
            print(f"  整体中位数: {statistics.mean(all_medians):.2f} ms")
            print(f"  整体平均值: {statistics.mean(all_means):.2f} ms")
            
            if statistics.mean(all_medians) < 100:
                print("  ✅ 延迟表现: 优秀 (适合高频交易)")
            elif statistics.mean(all_medians) < 200:
                print("  ✅ 延迟表现: 良好 (适合实时分析)")
            else:
                print("  ⚠️ 延迟表现: 一般 (可能需要优化网络)")
    
    @staticmethod
    def _percentile(data: List[float], percentile: float) -> float:
        """计算百分位数"""
        if not data:
            return 0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]


def main():
    """主函数 - 运行延迟分析"""
    import argparse
    
    parser = argparse.ArgumentParser(description='分析币安数据流接收延迟')
    parser.add_argument('--data-dir', '-d', default='./data',
                       help='数据目录路径 (默认: ./data)')
    parser.add_argument('--file', '-f', type=str,
                       help='分析指定文件')
    
    args = parser.parse_args()
    
    analyzer = LatencyAnalyzer()
    
    if args.file:
        # 分析单个文件
        stats = analyzer.analyze_file_latency(args.file)
        if stats:
            print(f"文件: {args.file}")
            print(f"延迟统计:")
            for key, value in stats.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.2f}")
                else:
                    print(f"  {key}: {value}")
        else:
            print("无法分析文件")
    else:
        # 分析整个目录
        analyzer.print_analysis_report(args.data_dir)


if __name__ == '__main__':
    main()