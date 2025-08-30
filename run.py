#!/usr/bin/env python3
"""
币安数据流收集器启动脚本
避免使用 python -m 导致的模块导入警告
"""

if __name__ == "__main__":
    from src.binance_streamer.main import main
    main()
