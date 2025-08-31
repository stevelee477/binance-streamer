#!/usr/bin/env python3
"""
运行所有测试的主脚本
提供了完整的测试套件执行和结果汇总
"""
import os
import sys
import time
import subprocess
from typing import Dict, List, Tuple

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

class TestRunner:
    """测试运行器"""
    
    def __init__(self):
        self.results = []
        self.start_time = time.time()
    
    def run_test(self, test_path: str, test_name: str, timeout: int = 60) -> Tuple[bool, str, float]:
        """运行单个测试"""
        print(f"\n🧪 运行测试: {test_name}")
        print(f"   路径: {test_path}")
        
        start = time.time()
        try:
            result = subprocess.run(
                [sys.executable, test_path],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=project_root
            )
            duration = time.time() - start
            
            if result.returncode == 0:
                print(f"   ✅ 通过 ({duration:.2f}s)")
                return True, result.stdout, duration
            else:
                print(f"   ❌ 失败 ({duration:.2f}s)")
                print(f"   错误: {result.stderr[:200]}...")
                return False, result.stderr, duration
                
        except subprocess.TimeoutExpired:
            duration = time.time() - start
            print(f"   ⏰ 超时 ({timeout}s)")
            return False, f"测试超时 ({timeout}s)", duration
        except Exception as e:
            duration = time.time() - start
            print(f"   💥 异常: {e}")
            return False, str(e), duration
    
    def run_performance_tests(self):
        """运行性能测试"""
        print("\n" + "="*50)
        print("🚀 性能测试套件")
        print("="*50)
        
        perf_tests = [
            ("tests/performance/optimized_file_writer.py", "写入性能对比测试"),
            ("tests/performance/quick_test_optimization.py", "快速优化验证测试"),
            ("tests/performance/profile_performance.py", "完整性能分析")
        ]
        
        perf_results = []
        for test_path, test_name in perf_tests:
            success, output, duration = self.run_test(test_path, test_name, timeout=120)
            perf_results.append((test_name, success, duration, output))
            self.results.append(("performance", test_name, success, duration))
        
        return perf_results
    
    def run_unit_tests(self):
        """运行单元测试"""
        print("\n" + "="*50)
        print("🔧 单元测试套件")
        print("="*50)
        
        unit_tests = [
            ("tests/unit/test_optimized_write.py", "优化写入功能测试")
        ]
        
        unit_results = []
        for test_path, test_name in unit_tests:
            success, output, duration = self.run_test(test_path, test_name)
            unit_results.append((test_name, success, duration, output))
            self.results.append(("unit", test_name, success, duration))
        
        return unit_results
    
    def run_integration_tests(self):
        """运行集成测试"""
        print("\n" + "="*50)
        print("🌐 集成测试套件")
        print("="*50)
        
        integration_tests = [
            ("tests/integration/simple_test.py", "实时系统集成测试")
        ]
        
        integration_results = []
        for test_path, test_name in integration_tests:
            success, output, duration = self.run_test(test_path, test_name, timeout=30)
            integration_results.append((test_name, success, duration, output))
            self.results.append(("integration", test_name, success, duration))
        
        return integration_results
    
    def generate_summary_report(self):
        """生成测试总结报告"""
        total_duration = time.time() - self.start_time
        
        print("\n" + "="*60)
        print("📊 测试结果总结")
        print("="*60)
        
        # 统计结果
        total_tests = len(self.results)
        passed_tests = sum(1 for _, _, success, _ in self.results if success)
        failed_tests = total_tests - passed_tests
        
        print(f"总测试数:   {total_tests}")
        print(f"通过:      {passed_tests} ✅")
        print(f"失败:      {failed_tests} ❌")
        print(f"成功率:    {passed_tests/total_tests*100:.1f}%")
        print(f"总耗时:    {total_duration:.2f}s")
        
        # 分类统计
        categories = {}
        for category, name, success, duration in self.results:
            if category not in categories:
                categories[category] = {'total': 0, 'passed': 0, 'duration': 0}
            categories[category]['total'] += 1
            categories[category]['duration'] += duration
            if success:
                categories[category]['passed'] += 1
        
        print(f"\n📋 分类结果:")
        for category, stats in categories.items():
            pass_rate = stats['passed'] / stats['total'] * 100
            print(f"  {category:12s}: {stats['passed']}/{stats['total']} 通过 ({pass_rate:.1f}%) - {stats['duration']:.2f}s")
        
        # 详细结果
        print(f"\n📝 详细结果:")
        for category, name, success, duration in self.results:
            status = "✅" if success else "❌"
            print(f"  {status} [{category:11s}] {name:30s} ({duration:.2f}s)")
        
        # 生成文件报告
        report_file = 'test_results_summary.txt'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"Binance Streamer 测试结果总结\n")
            f.write(f"测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"总测试数: {total_tests}, 通过: {passed_tests}, 失败: {failed_tests}\n")
            f.write(f"成功率: {passed_tests/total_tests*100:.1f}%\n")
            f.write(f"总耗时: {total_duration:.2f}s\n\n")
            
            for category, name, success, duration in self.results:
                status = "PASS" if success else "FAIL"
                f.write(f"[{status}] [{category}] {name} ({duration:.2f}s)\n")
        
        print(f"\n📄 详细报告已保存: {report_file}")
        
        return passed_tests == total_tests

def main():
    """主测试函数"""
    print("🔬 Binance Streamer 测试套件")
    print(f"⏰ 开始时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    runner = TestRunner()
    
    try:
        # 运行各类测试
        runner.run_unit_tests()
        runner.run_performance_tests()
        runner.run_integration_tests()
        
        # 生成总结报告
        all_passed = runner.generate_summary_report()
        
        if all_passed:
            print("\n🎉 所有测试通过!")
            return 0
        else:
            print("\n❌ 部分测试失败!")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⚠️  测试被用户中断")
        return 2
    except Exception as e:
        print(f"\n💥 测试套件出错: {e}")
        return 3

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)