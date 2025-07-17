# Infinite Parser

A high-performance, multithreaded cryptocurrency data processing engine that downloads, parses, and organizes Binance historical trading data into unified tick datasets for technical analysis.

## Overview

Infinite Parser is designed to efficiently process massive volumes of cryptocurrency trading data from Binance. It downloads archived CSV files, extracts individual trades, and converts them into standardized timestamp-value pairs that can be aggregated into any timeframe and analyzed with technical indicators.

## Key Features

- **Binance Integration**: Direct support for all cryptocurrency pairs listed on Binance
- **Massive Scale Processing**: Handles hundreds of millions to billions of trades in minutes
- **Multithreaded Architecture**: Optimized concurrent processing for maximum performance
- **Flexible Timeframe Aggregation**: Convert tick data into any timeframe (1.5s, 1m, 5.99m, 6.45m, 1.3h, 4w, whatever)
- **Technical Indicators**: Apply derived functions like RSI, MACD, Bollinger Bands, and more
- **Unified Data Format**: Standardizes all data into consistent timestamp-value pairs
- **Memory Efficient**: Smart data handling for processing massive datasets

## How It Works

### 1. Data Acquisition
```
Binance Archived CSV Files → Download → Raw Trade Data / Open Interest / Metrics / More...
```

### 2. Data Parsing & Normalization
```go
// Each trade is converted to a standardized format
type TickData struct {
    Timestamp int64   // Unix timestamp in millisecond, adaptable to micro
    Value     float64 // Price/volume/other metric
}
```

### 3. Data Aggregation
```
Individual Ticks → Timeframe Aggregation → OHLCV Candles
```

### 4. Technical Analysis
```
Aggregated Data → Technical Indicators → Analysis Results
```

## Supported Data Types

- **Trading Pairs**: All cryptocurrency pairs available on Binance
- **Data Points**: Price, volume, trades, bid/ask spreads
- **Timeframes**: Any custom timeframe from 1ms
- **Indicators**: RSI, MACD, EMA, SMA, Bollinger Bands, Stochastic, and more

## Performance

- **Processing Speed**: Billions of trade records in minutes
- **Concurrency**: Multithreaded processing utilizing all CPU cores
- **Memory Optimization**: Efficient handling of large datasets
- **Data Throughput**: High-speed CSV parsing and data transformation

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Binance API   │────│   Downloader │────│   CSV Parser    │
└─────────────────┘    └──────────────┘    └─────────────────┘
                                                      │
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Technical      │────│  Aggregator  │────│  Data Normalizer│
│  Indicators     │    │              │    │                 │
└─────────────────┘    └──────────────┘    └─────────────────┘
```

## Usage Examples

### To start
```
You need to run the dashboard, the archiver, and the parser.
Then you need to add pairs from the dashboard and everything will start to setup.
```

## Web Interface

The infinite-parser provides a user-friendly web interface for configuring and monitoring data processing jobs:

### Main Dashboard
- **Pair Selection**: Choose from all available Binance cryptocurrency pairs
- **Date Range**: Select start and end dates for data processing
- **Timeframe Configuration**: Set custom aggregation timeframes
- **Indicator Setup**: Configure technical indicators and parameters

### Processing Monitor
- **Real-time Progress**: Live updates on processing status
- **Performance Metrics**: Processing speed, memory usage, and completion estimates
- **Job Queue**: View and manage multiple processing jobs
- **Error Handling**: Real-time error reporting and recovery options

### Results Viewer
- **Data Visualization**: Interactive charts for processed data
- **Export Options**: Download results in various formats (JSON, CSV, Parquet)
- **Technical Analysis**: View applied indicators and analysis results

## Performance Benchmarks

- **Data Volume**: 1 billion trades processed in ~5 minutes
- **Memory Usage**: You decided based on your available RAM+SWAP. Minimum required ~2GB
- **CPU Utilization**: You can setup the amount of thread you want to allocate for the program. 

## Dependencies

- **[GoRunner](https://github.com/Pendulea/gorunner)**: Task orchestration and parallel processing
- **[Pendule Archiver](https://github.com/Pendulea/pendule-archiver)**: Data archiving and storage
- **[Pendule Common](https://github.com/Pendulea/pendule-common)**: Shared utilities and data structures

## Use Cases

- **Quantitative Analysis**: Backtesting trading strategies
- **Market Research**: Historical market behavior analysis
- **Algorithm Development**: Training data for ML models
- **Risk Management**: Historical volatility and risk metrics
- **Academic Research**: Cryptocurrency market studies

## Contributing

Contributions are welcome for:

- Additional technical indicators
- Performance optimizations
- Support for other exchanges
- Data export formats

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

**Built for processing massive cryptocurrency datasets with Go's concurrency and performance.**
