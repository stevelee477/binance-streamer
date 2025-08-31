# 测试套件

这个目录包含了Binance Streamer的完整测试套件，用于验证系统功能和性能。

## 目录结构

```
tests/
├── __init__.py                 # 测试套件入口
├── README.md                   # 本文档
├── run_all_tests.py           # 运行所有测试的主脚本
├── performance/               # 性能测试
│   ├── profile_performance.py    # 完整性能分析工具
│   ├── optimized_file_writer.py  # 写入性能对比测试
│   └── quick_test_optimization.py # 快速性能验证
├── integration/               # 集成测试
│   ├── test_live_system.py       # 实时系统集成测试
│   └── simple_test.py            # 简化集成测试
└── unit/                      # 单元测试
    └── test_optimized_write.py   # 优化写入功能单元测试
```

## 使用方法

### 运行所有测试
```bash
python tests/run_all_tests.py
```

### 运行特定类型测试

#### 性能测试
```bash
# 完整性能分析
python tests/performance/profile_performance.py

# 写入性能对比
python tests/performance/optimized_file_writer.py

# 快速优化验证
python tests/performance/quick_test_optimization.py
```

#### 集成测试
```bash
# 实时系统测试
python tests/integration/test_live_system.py

# 简化集成测试
python tests/integration/simple_test.py
```

#### 单元测试
```bash
# 优化写入功能测试
python tests/unit/test_optimized_write.py
```

## 测试目标

### 性能测试目标
- 验证写入性能优化效果 (目标: >30%提升)
- 识别系统性能瓶颈
- 对比不同实现方案的性能差异

### 集成测试目标
- 验证完整数据流从WebSocket到文件写入
- 测试实时数据收集和处理
- 验证系统在真实网络环境下的表现

### 单元测试目标
- 验证各个组件的功能正确性
- 确保数据格式和完整性
- 测试边界条件和错误处理

## 测试数据

测试过程中会在以下目录生成测试数据:
- `./data/TESTBTC/` - 基础功能测试数据
- `./data/TESTOPT/` - 优化功能测试数据
- `./data/LIVETEST/` - 实时测试数据
- `./data/TEST*/` - 其他测试生成的数据

测试数据可以安全删除。

## 性能基准

### 当前性能基准 (2025-08-31)
- **写入速度**: 31,806 记录/秒
- **pandas vs 优化版**: 43%性能提升
- **实时数据流**: ~28条/秒收集速度
- **写入延迟**: <10ms (279条记录)

## 添加新测试

1. **性能测试**: 添加到 `tests/performance/`
2. **集成测试**: 添加到 `tests/integration/`
3. **单元测试**: 添加到 `tests/unit/`
4. **更新**: `run_all_tests.py` 以包含新测试

## 注意事项

- 性能测试可能需要较长时间运行
- 集成测试需要网络连接到Binance API
- 测试数据会写入 `./data/` 目录
- 某些测试可能需要清理之前的测试数据