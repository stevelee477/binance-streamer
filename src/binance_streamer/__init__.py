"""
币安数据流收集器 - High-performance multiprocess Binance futures data streamer

支持多交易对、多进程并发收集，配置文件驱动，专为低延迟数据收集而设计。
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__description__ = "High-performance multiprocess Binance futures data streamer with configurable symbols"

from .main import main

__all__ = ["main"]