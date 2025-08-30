import asyncio
import multiprocessing
import signal
import sys
import time
from typing import List, Dict, Any
from multiprocessing import Process, Queue
import logging
import os

from .config import config_manager
from .websocket_client import binance_websocket_client
from .data_fetcher import get_depth_snapshot
from .file_writer import writer_process
import aiohttp

def _symbol_worker_process(symbol: str, streams: List[str], data_queue, network_config: Dict, performance_config: Dict):
    """单个交易对的工作进程函数（顶层函数，可被pickle序列化）"""
    
    # 在进程内部导入，避免相对导入问题
    from binance_streamer.websocket_client import binance_websocket_client
    from binance_streamer.data_fetcher import get_depth_snapshot
    import aiohttp
    
    # 设置进程优先级
    try:
        if performance_config.get('process_priority') == 'high':
            os.nice(-5)
    except (OSError, PermissionError):
        print(f"[{symbol}] 无法设置高优先级，权限不足")
    
    async def run_symbol_collection():
        """运行单个交易对的数据收集"""
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=network_config.get('timeout', 30)
                )
            ) as session:
                # 并发执行深度快照获取和WebSocket连接
                tasks = []
                
                # 获取深度快照
                tasks.append(get_depth_snapshot(session, symbol, data_queue))
                
                # 启动WebSocket流
                tasks.append(binance_websocket_client(symbol, data_queue, streams))
                
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except Exception as e:
            print(f"[{symbol}] 数据收集出错: {e}")
    
    # 运行数据收集
    try:
        print(f"[{symbol}] 进程启动，数据流: {streams}")
        asyncio.run(run_symbol_collection())
    except Exception as e:
        print(f"[{symbol}] 进程异常退出: {e}")

class ProcessManager:
    """多进程管理器，负责启动和管理各个数据收集进程"""
    
    def __init__(self):
        self.config = config_manager.get_current_mode_config()
        self.network_config = config_manager.get_network_config()
        self.performance_config = config_manager.get_performance_config()
        self.processes: List[Process] = []
        self.data_queue = multiprocessing.Queue(
            maxsize=self.performance_config.get('queue_maxsize', 10000)
        )
        self.writer_process = None
        self.running = False
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # 配置日志
        self._setup_logging()
    
    def _setup_logging(self):
        """配置日志"""
        log_config = config_manager.get_logging_config()
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_config.get('file', 'binance_streamer.log')),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"接收到信号 {signum}，正在优雅关闭...")
        self.shutdown()
    
    def _set_process_priority(self):
        """设置进程优先级"""
        try:
            if self.performance_config.get('process_priority') == 'high':
                os.nice(-5)  # 提高优先级
        except (OSError, PermissionError):
            self.logger.warning("无法设置高优先级，权限不足")
    
    
    def _start_symbol_process(self, symbol: str, streams: List[str]):
        """启动单个交易对的进程"""
        process = Process(
            target=_symbol_worker_process,
            args=(symbol, streams, self.data_queue, self.network_config, self.performance_config),
            name=f"symbol-{symbol}"
        )
        process.start()
        self.processes.append(process)
        self.logger.info(f"已启动 {symbol} 进程，PID: {process.pid}")
    
    def start(self):
        """启动所有进程"""
        if self.running:
            self.logger.warning("进程管理器已经在运行")
            return
        
        self.running = True
        self.logger.info("启动币安数据流收集器...")
        
        try:
            # 启动写入进程
            self.writer_process = Process(
                target=writer_process,
                args=(self.data_queue,),
                name="writer"
            )
            self.writer_process.start()
            self.logger.info(f"已启动写入进程，PID: {self.writer_process.pid}")
            
            # 启动所有启用的交易对进程
            enabled_symbols = [sc for sc in self.config.symbols if sc.enabled]
            self.logger.info(f"发现 {len(enabled_symbols)} 个启用的交易对")
            
            for symbol_config in enabled_symbols:
                self._start_symbol_process(
                    symbol_config.symbol,
                    symbol_config.streams
                )
            
            self.logger.info(f"总计启动了 {len(self.processes)} 个数据收集进程")
            
            # 监控进程运行
            self._monitor_processes()
            
        except Exception as e:
            self.logger.error(f"启动进程时出错: {e}")
            self.shutdown()
    
    def _monitor_processes(self):
        """监控进程状态"""
        start_time = time.time()
        run_duration = self.config.run_duration
        
        self.logger.info(f"开始监控进程，计划运行 {run_duration} 秒...")
        
        try:
            while self.running:
                current_time = time.time()
                
                # 检查运行时间
                if run_duration > 0 and (current_time - start_time) >= run_duration:
                    self.logger.info("达到预设运行时间，开始关闭...")
                    break
                
                # 检查进程状态
                dead_processes = []
                for i, process in enumerate(self.processes):
                    if not process.is_alive():
                        dead_processes.append((i, process))
                
                # 处理死亡进程
                if dead_processes:
                    for i, process in reversed(dead_processes):
                        self.logger.warning(f"进程 {process.name} (PID: {process.pid}) 已退出")
                        self.processes.pop(i)
                
                # 检查写入进程
                if self.writer_process and not self.writer_process.is_alive():
                    self.logger.error("写入进程已退出，重新启动...")
                    self.writer_process = Process(
                        target=writer_process,
                        args=(self.data_queue,),
                        name="writer"
                    )
                    self.writer_process.start()
                
                time.sleep(1)  # 监控间隔
                
        except KeyboardInterrupt:
            self.logger.info("收到中断信号，开始关闭...")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """关闭所有进程"""
        if not self.running:
            return
        
        self.running = False
        self.logger.info("开始关闭所有进程...")
        
        # 终止数据收集进程
        for process in self.processes:
            if process.is_alive():
                self.logger.info(f"终止进程 {process.name} (PID: {process.pid})")
                process.terminate()
        
        # 等待进程结束
        for process in self.processes:
            try:
                process.join(timeout=5)
                if process.is_alive():
                    self.logger.warning(f"强制杀死进程 {process.name}")
                    process.kill()
            except Exception as e:
                self.logger.error(f"关闭进程 {process.name} 时出错: {e}")
        
        # 关闭写入进程
        if self.writer_process and self.writer_process.is_alive():
            self.logger.info("关闭写入进程...")
            # 发送停止信号
            self.data_queue.put(None)
            self.writer_process.join(timeout=10)
            
            if self.writer_process.is_alive():
                self.logger.warning("强制终止写入进程")
                self.writer_process.terminate()
                self.writer_process.join()
        
        self.logger.info("所有进程已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取进程状态"""
        alive_processes = [p for p in self.processes if p.is_alive()]
        return {
            'running': self.running,
            'total_processes': len(self.processes),
            'alive_processes': len(alive_processes),
            'writer_alive': self.writer_process.is_alive() if self.writer_process else False,
            'queue_size': self.data_queue.qsize() if hasattr(self.data_queue, 'qsize') else 'N/A'
        }

def main():
    """主函数"""
    manager = ProcessManager()
    try:
        manager.start()
    except Exception as e:
        logging.error(f"程序异常: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()