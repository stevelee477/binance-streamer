# config.py

import yaml
import os
import multiprocessing
import logging
from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class SymbolConfig:
    symbol: str
    streams: List[str]
    depth_snapshot: bool = True
    enabled: bool = True

@dataclass
class ModeConfig:
    run_duration: int
    max_workers: int
    symbols: List[SymbolConfig]

class ConfigManager:
    def __init__(self, config_file: str = "config.yaml"):
        self.config_file = config_file
        self._config = None
        self._load_config()
    
    def _load_config(self):
        """加载配置文件"""
        try:
            if not os.path.exists(self.config_file):
                raise FileNotFoundError(f"配置文件 {self.config_file} 不存在")
            
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            
            print(f"配置文件 {self.config_file} 加载成功")
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            raise
    
    def get_current_mode_config(self) -> ModeConfig:
        """获取当前模式的配置"""
        current_mode = self._config.get('mode', 'development')
        mode_config = self._config['modes'][current_mode]
        
        symbols = []
        for symbol_data in mode_config['symbols']:
            if symbol_data.get('enabled', True):
                symbols.append(SymbolConfig(
                    symbol=symbol_data['symbol'],
                    streams=symbol_data['streams'],
                    depth_snapshot=symbol_data.get('depth_snapshot', True),
                    enabled=symbol_data.get('enabled', True)
                ))
        
        return ModeConfig(
            run_duration=mode_config['run_duration'],
            max_workers=mode_config['max_workers'],
            symbols=symbols
        )
    
    def get_enabled_symbols(self) -> List[str]:
        """获取启用的交易对列表"""
        mode_config = self.get_current_mode_config()
        return [s.symbol for s in mode_config.symbols if s.enabled]
    
    def get_symbol_streams(self, symbol: str) -> List[str]:
        """获取指定交易对的数据流配置"""
        mode_config = self.get_current_mode_config()
        for symbol_config in mode_config.symbols:
            if symbol_config.symbol == symbol and symbol_config.enabled:
                return symbol_config.streams
        return []
    
    def get_network_config(self) -> Dict[str, Any]:
        """获取网络配置"""
        return self._config.get('network', {})
    
    def get_storage_config(self) -> Dict[str, Any]:
        """获取存储配置"""
        return self._config.get('storage', {})
    
    def get_performance_config(self) -> Dict[str, Any]:
        """获取性能配置"""
        return self._config.get('performance', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """获取日志配置"""
        return self._config.get('logging', {})

# 全局配置管理器实例
config_manager = ConfigManager()

# 向后兼容的配置接口
def get_config():
    """获取当前配置（向后兼容）"""
    return config_manager.get_current_mode_config()

# 共享资源
performance_config = config_manager.get_performance_config()
queue_maxsize = performance_config.get('queue_maxsize', 10000)
data_queue = multiprocessing.Queue(maxsize=queue_maxsize)
