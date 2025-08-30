# 币安数据流收集器

高性能、多进程的币安期货数据流收集工具，支持多交易对并发收集，配置文件驱动，专为低延迟数据收集而设计。

## 特性

- **多进程架构**: 每个交易对独立进程，最大化并发性能
- **配置文件驱动**: YAML配置文件，灵活配置交易对和数据流
- **低延迟设计**: 优化的队列和进程间通信，最小化数据延迟
- **自动重连**: WebSocket连接自动重连机制
- **数据完整性**: 支持深度快照和增量更新
- **优雅关闭**: 信号处理和资源清理

## 快速开始

### 1. 安装依赖

```bash
uv sync
```

### 2. 配置文件

编辑 `config.yaml` 文件，配置要收集的交易对：

```yaml
# 运行模式：development 或 production  
mode: development

modes:
  development:
    run_duration: 300  # 5分钟
    max_workers: 2
    symbols:
      - symbol: BTCUSDT
        streams:
          - aggTrade
          - depth@0ms  
          - kline_1m
        enabled: true
      - symbol: ETHUSDT
        streams:
          - aggTrade
          - depth@0ms
          - kline_1m
        enabled: true
```

### 3. 运行程序

```bash
# 安装依赖（使用uv）
uv sync

# 基本启动
uv run python -m binance_streamer.main

# 使用自定义配置文件
uv run python -m binance_streamer.main -c custom_config.yaml

# 查看配置状态
uv run python -m binance_streamer.main --status

# 列出配置的交易对
uv run python -m binance_streamer.main --list-symbols

# 详细日志输出
uv run python -m binance_streamer.main -v
```

## 配置说明

### 基本配置

- `mode`: 运行模式（development/production）
- `run_duration`: 运行时长（秒，0表示无限运行）

### 网络配置

- `websocket_url`: WebSocket连接地址
- `rest_api_url`: REST API地址
- `reconnect_delay`: 重连延迟（秒）
- `timeout`: 超时时间（秒）

### 交易对配置

每个交易对可以配置：
- `symbol`: 交易对名称（如 BTCUSDT）
- `streams`: 数据流类型
  - `aggTrade`: 聚合交易流
  - `depth@0ms`: 深度数据流
  - `kline_1m`: 1分钟K线
- `depth_snapshot`: 是否获取深度快照
- `enabled`: 是否启用

### 性能配置

```yaml
performance:
  queue_maxsize: 10000       # 队列最大大小
  batch_size: 100            # 批量处理大小
  flush_interval: 1          # 刷新间隔（秒）
  process_priority: "high"   # 进程优先级
```

### 存储配置

```yaml
storage:
  output_directory: "./data"  # 输出目录
  file_format: "csv"          # 文件格式
  daily_rotation: true        # 按日期轮转文件
```

### 订单簿配置

```yaml
orderbook:
  enabled: false              # 是否启用本地订单簿管理
  max_depth: 1000           # 最大维护深度档位
  output_interval: 10       # 订单簿状态输出间隔（秒）
  resync_threshold: 5       # 重新同步阈值（失败次数）
```

### 日志配置

```yaml
logging:
  level: INFO                # 日志级别：DEBUG, INFO, WARNING, ERROR
  file: "binance_streamer.log"
  max_file_size: 10          # MB
  backup_count: 5            # 备份文件数量
```

## 数据文件

程序会在配置的输出目录中生成以下文件：

- `aggtrade_{SYMBOL}_{YYYYMMDD}.csv`: 聚合交易数据
- `depth_{SYMBOL}_{YYYYMMDD}.csv`: 深度数据
- `kline_1m_{SYMBOL}_{YYYYMMDD}.csv`: 1分钟K线数据
- `{SYMBOL}_depth_snapshot_{YYYYMMDD}.csv`: 深度快照

## 架构设计

### 多进程架构

```
Main Process
├── ProcessManager (主控制进程)
├── Writer Process (数据写入进程)
├── Symbol Process 1 (BTCUSDT数据收集)
├── Symbol Process 2 (ETHUSDT数据收集)
└── Symbol Process N (其他交易对)
```

### 数据流

1. 每个交易对进程并发执行：
   - 获取深度快照（REST API）
   - 建立WebSocket连接接收实时数据
2. 数据通过队列传递给写入进程
3. 写入进程负责将数据保存到CSV文件

## 性能优化

- **进程隔离**: 每个交易对独立进程，避免相互影响
- **队列缓冲**: 大容量队列缓解突发数据
- **批量写入**: 减少磁盘I/O操作
- **进程优先级**: 可设置高优先级减少调度延迟
- **内存映射**: 大文件写入优化

## 监控和日志

程序支持详细的日志记录：

```bash
# 启用详细日志
python main.py -v
```

日志文件：`binance_streamer.log`

## 故障处理

- **网络断线**: 自动重连机制，5秒重连间隔
- **进程异常**: 进程监控和自动重启
- **队列满载**: 队列大小监控和告警
- **磁盘空间**: 文件写入错误处理

## 示例配置

### 生产环境配置

```yaml
mode: production

modes:
  production:
    run_duration: 86400  # 24小时
    symbols:
      - symbol: BTCUSDT
        streams: [aggTrade, depth@0ms, kline_1m]
        depth_snapshot: true
        enabled: true
      - symbol: ETHUSDT  
        streams: [aggTrade, depth@0ms, kline_1m]
        depth_snapshot: true
        enabled: true
      # ... 更多交易对

performance:
  queue_maxsize: 50000
  process_priority: "high"

storage:
  output_directory: "/data/binance"

orderbook:
  enabled: true
  max_depth: 1000
  resync_threshold: 5
```

## 常见问题

### Q: 如何添加新的交易对？
A: 编辑 `config.yaml` 文件，在对应模式的 `symbols` 列表中添加新交易对。

### Q: 如何启用订单簿管理？
A: 在配置文件中设置 `orderbook.enabled: true`。注意这会增加内存使用。

### Q: 如何处理大量数据？
A: 增加 `queue_maxsize`，使用SSD存储，调整 `batch_size` 和 `flush_interval`。

### Q: 程序占用内存过大怎么办？
A: 减少 `queue_maxsize`，启用 `daily_rotation`，减少同时运行的交易对数量。

## 许可证

MIT License