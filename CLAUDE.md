# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Running the Application
```bash
# Install dependencies
uv sync

# Basic run with default config
uv run python -m binance_streamer.main

# Run with custom config
uv run python -m binance_streamer.main -c custom_config.yaml

# Show configuration status
uv run python -m binance_streamer.main --status

# List configured symbols
uv run python -m binance_streamer.main --list-symbols

# Run with verbose logging
uv run python -m binance_streamer.main -v

# Daemon mode operations (recommended way, no warnings)
uv run python run.py --daemon start     # Start as daemon
uv run python run.py --daemon stop      # Stop daemon
uv run python run.py --daemon restart   # Restart daemon
uv run python run.py --daemon status    # Check daemon status

# Alternative (may show harmless warnings)
uv run python -m binance_streamer.main --daemon start
```

### Development
```bash
# Run complete test suite
uv run python tests/run_all_tests.py

# Run specific test categories
uv run python tests/performance/optimized_file_writer.py    # Performance tests
uv run python tests/integration/simple_test.py             # Integration tests
uv run python tests/unit/test_optimized_write.py           # Unit tests
```

## Architecture Overview

### High-Level Design
This is a multiprocess Binance futures data streamer designed for high-performance, low-latency data collection:

```
Main Process (ProcessManager)
├── Writer Process (file_writer.py)
├── Symbol Process 1 (BTCUSDT) 
├── Symbol Process 2 (ETHUSDT)
├── Symbol Process N (other symbols)
└── OrderBook Manager Process (optional)
```

### Core Components

**ProcessManager** (`src/binance_streamer/process_manager.py:62`): 
- Main orchestrator that spawns and monitors all child processes
- Handles graceful shutdown via signal handlers
- Manages process lifecycle and auto-restart for critical processes

**Configuration System** (`src/binance_streamer/config.py:22`):
- YAML-driven configuration with development/production modes
- ConfigManager class provides centralized access to all settings
- Global config_manager instance used throughout the codebase

**Data Flow Architecture**:
1. Each symbol runs in isolated process calling `_symbol_worker_process` (`src/binance_streamer/process_manager.py:18`)
2. Per-symbol processes handle both REST API depth snapshots and WebSocket streams
3. All data flows through shared multiprocessing.Queue to writer process
4. Writer process (`src/binance_streamer/file_writer.py:31`) saves to symbol-specific CSV files

**WebSocket Client** (`src/binance_streamer/websocket_client.py:7`):
- Handles multiple stream types: aggTrade, depth@0ms, kline_1m
- Automatic reconnection with 5-second delay
- Adds local timestamps to all incoming data

**OrderBook Management** (optional):
- `orderbook_manager.py`: LocalOrderBook class maintains real-time order book state
- `orderbook_process.py`: Separate process for managing multiple symbol orderbooks
- Follows Binance official documentation for correct local orderbook maintenance

### Data Storage
- CSV files organized by symbol in separate directories: `./data/{SYMBOL}/`
- Daily file rotation: `aggtrade_{SYMBOL}_{YYYYMMDD}.csv`
- Stream types: aggtrade, depth, kline, depth_snapshot

### Configuration Structure
- `config.yaml`: Main configuration with development/production modes
- Network settings: WebSocket URL, REST API URL, timeouts
- Performance tuning: queue sizes, batch processing, process priorities
- Storage options: output directories, file formats, rotation

### Key Design Patterns
- **Process Isolation**: Each trading pair runs in separate process to avoid interference
- **Shared Queue Communication**: All processes communicate via multiprocessing.Queue
- **Configuration-Driven**: Everything configurable via YAML without code changes
- **Graceful Shutdown**: Signal handlers ensure clean process termination
- **Auto-Recovery**: Critical processes (writer) auto-restart on failure

## Development Notes

### Adding New Symbols
Edit `config.yaml` under the appropriate mode section and add to `symbols` list.

### Adding New Stream Types
1. Update `websocket_client.py:7` to handle new stream patterns
2. Update `file_writer.py:31` to process new stream data format
3. Add stream name to symbol configuration in config.yaml

### Testing
The project includes a comprehensive test suite organized in the `tests/` directory:

**Test Structure:**
- `tests/performance/` - Performance benchmarking and optimization validation
- `tests/integration/` - End-to-end system integration tests
- `tests/unit/` - Component-level unit tests

**Running Tests:**
```bash
# Run all tests with comprehensive reporting
uv run python tests/run_all_tests.py

# Individual test categories
uv run python tests/performance/optimized_file_writer.py    # File writing performance comparison
uv run python tests/performance/quick_test_optimization.py  # Quick optimization validation
uv run python tests/integration/simple_test.py             # Real-time data collection test
```

**Test Data:**
Tests generate data in `./data/TEST*/` directories which can be safely deleted after testing.

### Process Communication
All inter-process communication happens via multiprocessing.Queue. Data format is tuples of `(stream_type, data)` where stream_type determines how writer process handles the data.