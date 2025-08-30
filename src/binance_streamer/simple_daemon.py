"""
简化的Daemon实现，适用于macOS和开发环境
"""
import os
import sys
import signal
import logging
from multiprocessing import Process


class SimpleDaemon:
    """简化的守护进程实现"""
    
    def __init__(self, pidfile: str, config_file: str = 'config.yaml', verbose: bool = False):
        self.pidfile = pidfile
        self.config_file = config_file
        self.verbose = verbose
        
    def start(self):
        """启动守护进程（简化版）"""
        # 检查是否已经运行
        if self._is_running():
            print(f"Daemon already running (PID file: {self.pidfile})")
            return
            
        print("Starting daemon...")
        
        # 使用fork创建守护进程
        pid = os.fork()
        if pid > 0:
            # 父进程：写入PID文件并退出
            with open(self.pidfile, 'w') as f:
                f.write(str(pid))
            print(f"Daemon started (PID: {pid})")
            return
        
        # 子进程：脱离会话并运行守护进程
        try:
            # 创建新会话
            os.setsid()
            
            # 保存当前工作目录（不改变到根目录，保持原始路径）
            # os.chdir('/')  # 注释掉，保持在原来的工作目录
            
            # 设置文件创建掩码
            os.umask(0)
            
            # 重定向标准IO
            with open('/dev/null', 'r') as null_r:
                os.dup2(null_r.fileno(), sys.stdin.fileno())
            with open('/dev/null', 'w') as null_w:
                os.dup2(null_w.fileno(), sys.stdout.fileno())
                os.dup2(null_w.fileno(), sys.stderr.fileno())
            
            # 运行守护进程
            self._run_daemon()
        except Exception as e:
            sys.exit(1)
        
    def stop(self):
        """停止守护进程"""
        pid = self._get_pid()
        if not pid:
            print("Daemon is not running")
            return
            
        try:
            os.kill(pid, signal.SIGTERM)
            os.remove(self.pidfile)
            print(f"Daemon stopped (PID: {pid})")
        except (OSError, ProcessLookupError):
            print("Daemon process not found, removing stale PID file")
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
                
    def status(self):
        """检查守护进程状态"""
        pid = self._get_pid()
        if not pid:
            print("Daemon is not running")
            return False
            
        try:
            os.kill(pid, 0)  # 检查进程是否存在
            print(f"Daemon is running (PID: {pid})")
            return True
        except (OSError, ProcessLookupError):
            print("Daemon is not running (stale PID file)")
            os.remove(self.pidfile)
            return False
            
    def restart(self):
        """重启守护进程"""
        self.stop()
        import time
        time.sleep(1)  # 等待进程完全停止
        self.start()
        
    def _run_daemon(self):
        """守护进程主函数"""
        try:
            # 设置信号处理
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)
            
            # 设置日志（只输出到文件）
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('binance_streamer.log')
                ]
            )
            
            # 导入并启动进程管理器
            from .process_manager import ProcessManager
            from .config import ConfigManager
            
            # 如果指定了自定义配置文件，重新初始化配置管理器
            if self.config_file != 'config.yaml':
                from . import config
                config.config_manager = ConfigManager(self.config_file)
            
            # 启动进程管理器
            manager = ProcessManager()
            manager.start()
            
        except Exception as e:
            logging.error("守护进程启动失败: %s", e)
            if self.verbose:
                import traceback
                logging.error(traceback.format_exc())
            sys.exit(1)
            
    def _signal_handler(self, signum, _frame):
        """信号处理器"""
        logging.info("守护进程收到信号 %s，正在关闭...", signum)
        sys.exit(0)
        
    def _get_pid(self):
        """从PID文件获取进程ID"""
        try:
            with open(self.pidfile, 'r') as f:
                return int(f.read().strip())
        except (IOError, ValueError):
            return None
            
    def _is_running(self):
        """检查守护进程是否正在运行"""
        pid = self._get_pid()
        if not pid:
            return False
            
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            # 删除无效的PID文件
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
            return False


def create_simple_daemon(pidfile: str = '/tmp/binance-streamer.pid',
                        config_file: str = 'config.yaml',
                        verbose: bool = False) -> SimpleDaemon:
    """创建简化的守护进程实例"""
    return SimpleDaemon(pidfile, config_file, verbose)