"""
Daemon模块 - 处理进程守护化
"""
import os
import sys
import atexit
import signal
import time
import logging


class Daemon:
    """
    守护进程基类
    用法: 继承此类并重写run()方法
    """
    
    def __init__(self, pidfile: str, stdin: str = '/dev/null', 
                 stdout: str = None, stderr: str = None):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        # 确保PID文件使用绝对路径
        if not os.path.isabs(pidfile):
            self.pidfile = os.path.join(os.getcwd(), pidfile)
        else:
            self.pidfile = pidfile
        
    def _daemonize(self):
        """
        执行守护进程化的标准UNIX双fork
        """
        try:
            # 第一次fork
            pid = os.fork()
            if pid > 0:
                # 父进程退出
                sys.exit(0)
        except OSError as err:
            sys.stderr.write(f"fork #1 failed: {err}\n")
            sys.exit(1)
            
        # 从父进程环境脱离
        # 保持原工作目录，不切换到根目录（保持项目路径）
        # os.chdir('/')  # 注释掉，保持在项目目录中
        os.setsid()
        os.umask(0)
        
        # 第二次fork
        try:
            pid = os.fork()
            if pid > 0:
                # 第二个父进程退出
                sys.exit(0)
        except OSError as err:
            sys.stderr.write(f"fork #2 failed: {err}\n")
            sys.exit(1)
            
        # 重定向标准文件描述符
        sys.stdout.flush()
        sys.stderr.flush()
        
        # 重定向stdin
        with open(self.stdin, 'r', encoding='utf-8') as si:
            os.dup2(si.fileno(), sys.stdin.fileno())
            
        # 如果没有指定stdout/stderr，使用/dev/null
        if self.stdout is None:
            self.stdout = '/dev/null'
        if self.stderr is None:
            self.stderr = '/dev/null'
            
        with open(self.stdout, 'a+', encoding='utf-8') as so:
            os.dup2(so.fileno(), sys.stdout.fileno())
        with open(self.stderr, 'a+', encoding='utf-8') as se:
            os.dup2(se.fileno(), sys.stderr.fileno())
            
        # 写入pidfile
        atexit.register(self._delpid)
        pid = str(os.getpid())
        with open(self.pidfile, 'w+', encoding='utf-8') as f:
            f.write(f"{pid}\n")
            
    def _delpid(self):
        """删除pid文件"""
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)
            
    def start(self):
        """启动守护进程"""
        # 检查pidfile，判断守护进程是否已经运行
        try:
            with open(self.pidfile, 'r', encoding='utf-8') as pf:
                pid = int(pf.read().strip())
        except (IOError, ValueError):
            pid = None
            
        if pid:
            # 检查进程是否真的在运行
            try:
                os.kill(pid, 0)  # 发送信号0不会杀死进程，只是检查是否存在
                message = f"pidfile {self.pidfile} already exists. Daemon already running?\n"
                sys.stderr.write(message)
                sys.exit(1)
            except OSError:
                # 进程不存在，删除旧的pid文件
                self._delpid()
                
        # 启动守护进程
        self._daemonize()
        self.run()
        
    def stop(self):
        """停止守护进程"""
        # 从pidfile获取pid
        try:
            with open(self.pidfile, 'r', encoding='utf-8') as pf:
                pid = int(pf.read().strip())
        except (IOError, ValueError):
            message = f"pidfile {self.pidfile} does not exist. Daemon not running?\n"
            sys.stderr.write(message)
            return
            
        # 尝试杀死守护进程
        try:
            while True:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            if "No such process" in str(err):
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(str(err))
                sys.exit(1)
                
    def restart(self):
        """重启守护进程"""
        self.stop()
        self.start()
        
    def status(self):
        """检查守护进程状态"""
        try:
            with open(self.pidfile, 'r', encoding='utf-8') as pf:
                pid = int(pf.read().strip())
        except (IOError, ValueError):
            print("Daemon is not running")
            return False
            
        try:
            os.kill(pid, 0)
            print(f"Daemon is running (PID: {pid})")
            return True
        except OSError:
            print("Daemon is not running (stale pidfile)")
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
            return False
            
    def run(self):
        """
        子类需要重写此方法
        这将在守护进程启动后运行
        """
        raise NotImplementedError("Subclass must implement run method")


class BinanceStreamerDaemon(Daemon):
    """币安数据流收集器守护进程"""
    
    def __init__(self, pidfile: str, config_file: str = 'config.yaml', verbose: bool = False):
        super().__init__(pidfile)
        self.config_file = config_file
        self.verbose = verbose
        # 保存原始工作目录，用于日志和配置文件路径
        self.original_cwd = os.getcwd()
        
    def run(self):
        """运行币安数据流收集器"""
        # 设置信号处理
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        try:
            # 导入并启动进程管理器
            from .process_manager import ProcessManager
            from .config import ConfigManager
            
            # 如果指定了自定义配置文件，重新初始化配置管理器
            if self.config_file != 'config.yaml':
                from . import config
                # 确保配置文件路径正确
                config_path = self.config_file
                if not os.path.isabs(config_path):
                    config_path = os.path.join(self.original_cwd, config_path)
                config.config_manager = ConfigManager(config_path)
            
            # 配置日志到文件（daemon模式下）
            self._setup_daemon_logging()
            
            # 启动进程管理器
            self.manager = ProcessManager()
            self.manager.start()
            
        except Exception as e:
            logging.error("守护进程启动失败: %s", e)
            if self.verbose:
                import traceback
                logging.error(traceback.format_exc())
            sys.exit(1)
            
    def _setup_daemon_logging(self):
        """设置守护进程日志"""
        from .config import config_manager
        
        log_config = config_manager.get_logging_config()
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('file', 'binance_streamer.log')
        
        # 确保日志文件路径是绝对路径，使用保存的原始工作目录
        if not os.path.isabs(log_file):
            log_file = os.path.join(self.original_cwd, log_file)
            
        # 确保日志文件目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
            ],
            force=True  # 强制重新配置logging
        )
        
        # 设置输出重定向到日志文件
        self.stdout = log_file
        self.stderr = log_file
        
    def _signal_handler(self, signum, _frame):
        """信号处理器"""
        logging.info("守护进程收到信号 %s，正在关闭...", signum)
        if hasattr(self, 'manager'):
            self.manager.shutdown()
        sys.exit(0)


def create_daemon(pidfile: str = '/tmp/binance-streamer.pid',
                 config_file: str = 'config.yaml',
                 verbose: bool = False) -> BinanceStreamerDaemon:
    """创建守护进程实例"""
    return BinanceStreamerDaemon(pidfile, config_file, verbose)