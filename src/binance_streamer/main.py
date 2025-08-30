#!/usr/bin/env python3
"""
币安数据流收集器 - 主程序

支持多交易对、多进程并发收集，配置文件驱动
"""

import sys
import argparse
import logging
from .process_manager import ProcessManager
from .config import config_manager

def setup_argument_parser():
    """设置命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="币安数据流收集器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py                          # 使用默认配置文件启动
  python main.py -c custom.yaml           # 使用自定义配置文件
  python main.py --status                 # 显示当前配置状态
  python main.py --daemon start           # 启动守护进程
  python main.py --daemon stop            # 停止守护进程
  python main.py --daemon restart         # 重启守护进程
  python main.py --daemon status          # 查看守护进程状态
        """
    )
    
    parser.add_argument(
        '-c', '--config',
        default='config.yaml',
        help='配置文件路径 (默认: config.yaml)'
    )
    
    parser.add_argument(
        '--status',
        action='store_true',
        help='显示当前配置状态并退出'
    )
    
    parser.add_argument(
        '--list-symbols',
        action='store_true',
        help='列出所有配置的交易对并退出'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='启用详细日志输出'
    )
    
    parser.add_argument(
        '--daemon',
        choices=['start', 'stop', 'restart', 'status'],
        help='守护进程控制: start|stop|restart|status'
    )
    
    parser.add_argument(
        '--pidfile',
        default='/tmp/binance-streamer.pid',
        help='守护进程PID文件路径 (默认: /tmp/binance-streamer.pid)'
    )
    
    return parser

def show_status():
    """显示配置状态"""
    print("=== 币安数据流收集器配置状态 ===")
    
    try:
        mode_config = config_manager.get_current_mode_config()
        network_config = config_manager.get_network_config()
        storage_config = config_manager.get_storage_config()
        performance_config = config_manager.get_performance_config()
        
        print(f"当前模式: {config_manager._config.get('mode', 'development')}")
        print(f"运行时长: {mode_config.run_duration} 秒")
        print(f"启用的交易对数量: {len(mode_config.symbols)}")
        print(f"数据输出目录: {storage_config.get('output_directory', './data')}")
        print(f"队列最大大小: {performance_config.get('queue_maxsize', 10000)}")
        
        print("\n启用的交易对:")
        for symbol_config in mode_config.symbols:
            if symbol_config.enabled:
                streams_str = ', '.join(symbol_config.streams)
                print(f"  - {symbol_config.symbol}: [{streams_str}]")
        
    except Exception as e:
        print(f"获取配置状态失败: {e}")
        return False
    
    return True

def list_symbols():
    """列出所有配置的交易对"""
    print("=== 配置的交易对列表 ===")
    
    try:
        mode_config = config_manager.get_current_mode_config()
        
        print(f"总计 {len(mode_config.symbols)} 个交易对:")
        for i, symbol_config in enumerate(mode_config.symbols, 1):
            status = "启用" if symbol_config.enabled else "禁用"
            streams_str = ', '.join(symbol_config.streams)
            print(f"{i:2d}. {symbol_config.symbol:<12} [{status}] - 流: {streams_str}")
            
    except Exception as e:
        print(f"获取交易对列表失败: {e}")
        return False
    
    return True

def main():
    """主函数"""
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    
    try:
        # 使用指定的配置文件重新初始化配置管理器
        if args.config != 'config.yaml':
            global config_manager
            from .config import ConfigManager
            config_manager = ConfigManager(args.config)
        
        # 处理守护进程请求
        if args.daemon:
            from .daemon import create_daemon
            daemon = create_daemon(args.pidfile, args.config, args.verbose)
            
            if args.daemon == 'start':
                print("启动守护进程...")
                daemon.start()
            elif args.daemon == 'stop':
                print("停止守护进程...")
                daemon.stop()
            elif args.daemon == 'restart':
                print("重启守护进程...")
                daemon.restart()
            elif args.daemon == 'status':
                daemon.status()
            
            sys.exit(0)
        
        # 处理状态显示请求
        if args.status:
            sys.exit(0 if show_status() else 1)
        
        # 处理交易对列表请求
        if args.list_symbols:
            sys.exit(0 if list_symbols() else 1)
        
        # 显示启动信息
        print("=== 币安数据流收集器 ===")
        mode_config = config_manager.get_current_mode_config()
        print(f"模式: {config_manager._config.get('mode', 'development')}")
        print(f"配置文件: {args.config}")
        print(f"交易对数量: {len(mode_config.symbols)}")
        print(f"运行时长: {mode_config.run_duration} 秒")
        print("=" * 30)
        
        # 启动进程管理器
        manager = ProcessManager()
        manager.start()
        
    except KeyboardInterrupt:
        print("\n收到中断信号，程序退出")
        sys.exit(0)
    except FileNotFoundError as e:
        print(f"配置文件未找到: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"程序启动失败: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
