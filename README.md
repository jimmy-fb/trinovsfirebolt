# 🚀 Trino vs Firebolt Performance Benchmark Suite

A comprehensive benchmarking toolkit for comparing SQL query performance between **Trino** and **Firebolt** using TPCH-style external tables and custom analytical workloads.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Benchmark Types](#benchmark-types)
- [Repository Structure](#repository-structure)
- [Configuration](#configuration)
- [Running Benchmarks](#running-benchmarks)
- [Results Analysis](#results-analysis)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🎯 Overview

This repository provides a complete benchmarking framework to evaluate and compare the performance of **Trino** and **Firebolt** on analytical workloads. It includes:

- **10 comprehensive SQL queries** covering different analytical patterns
- **Sequential and concurrent execution** testing
- **Mixed concurrency workloads** with parallel query execution
- **Automated result collection** and CSV export
- **Performance visualization** tools
- **Extensible framework** for custom benchmarks

## ✨ Features

- 🔄 **Multi-Engine Support**: Compare Trino and Firebolt side-by-side
- 📊 **Comprehensive Query Suite**: 10 analytical queries testing different patterns
- ⚡ **Concurrency Testing**: Test performance under concurrent load (up to 15+ parallel queries)
- 📈 **Performance Analytics**: Automated execution time measurement and comparison
- 🎨 **Data Visualization**: Generate comparison charts and performance graphs
- 🔧 **Flexible Configuration**: Easy setup with JSON-based credentials
- 📝 **Detailed Reporting**: CSV exports and summary reports
- 🛠️ **Extensible Design**: Add custom queries and benchmark scenarios

## 📋 Prerequisites

### Data Requirements
- **External tables** configured in both Trino and Firebolt with TPCH data:
  - `customer` - Customer information
  - `lineitem` - Order line items  
  - `products` - Product catalog
  - `events` - Event data
  - `orders` - Order information

### System Requirements
- Python 3.8+
- Access to both Trino and Firebolt clusters
- Network connectivity to both systems

### Required Credentials
- Trino coordinator access (host, port, credentials)
- Firebolt service account (account name, database, engine, auth credentials)

## 🚀 Quick Start

### 1. Clone & Setup
```bash
git clone https://github.com/jimmy-fb/trinovsfirebolt.git
cd trinovsfirebolt
cd clients/python
pip install -r requirements.txt
```

### 2. Configure Credentials
```bash
cp config/credentials/sample_credentials.json config/credentials/credentials.json
# Edit credentials.json with your Trino and Firebolt details
```

### 3. Test Connections
```bash
python test_connection.py
```

### 4. Run Benchmark
```bash
# Sequential benchmark
python -m src.main custom_schema --vendors firebolt,trino --execute-setup True

# Concurrent benchmark (10 parallel queries for 60 seconds)
python -m src.main custom_schema --vendors firebolt,trino --concurrency 10 --concurrency-duration-s 60
```

### 5. Mixed Concurrency Test
```bash
# Run all 10 different queries in parallel
python run_mixed_concurrency.py
```

## 📊 Benchmark Types

### 1. **Custom Schema Benchmark** (`benchmarks/custom_schema/`)
Our primary benchmark suite with 10 comprehensive queries:

| Query | Description | Complexity |
|-------|-------------|------------|
| Q1 | Customer aggregation | Basic |
| Q2 | Order summary | Basic |
| Q3 | Lineitem analysis | Medium |
| Q4 | Customer-Order join | Medium |
| Q5 | Product analysis | Basic |
| Q6 | Event analysis | Basic |
| Q7 | Multi-table join | High |
| Q8 | Revenue by date range | Medium |
| Q9 | Top customers ranking | High |
| Q10 | Product performance | High |

### 2. **TPCH External Tables** (`benchmarks/tpch_external_tables/`)
Standard TPCH queries adapted for external table access.

### 3. **Production Benchmarks** (`benchmarks/trino_production/`, `benchmarks/tpch_production/`)
Real-world production-like scenarios with complex analytical workloads.

## 📁 Repository Structure

```
trinovsfirebolt/
├── benchmarks/                          # Benchmark definitions
│   ├── custom_schema/                   # Primary Trino vs Firebolt benchmark
│   │   ├── setup.sql                   # General setup SQL
│   │   ├── benchmark.sql               # Main benchmark queries
│   │   ├── warmup.sql                  # Warmup queries
│   │   ├── queries.json               # Concurrent benchmark config
│   │   ├── firebolt/                  # Firebolt-specific SQL
│   │   └── trino/                     # Trino-specific SQL  
│   ├── tpch_external_tables/          # TPCH benchmark suite
│   ├── trino_production/              # Production Trino benchmarks
│   └── tpch_production/               # Production TPCH benchmarks
├── clients/
│   └── python/                        # Python benchmark client
│       ├── src/                       # Core benchmark framework
│       │   ├── connectors/            # Database connectors
│       │   ├── exporters/             # Result exporters
│       │   ├── main.py               # CLI entry point
│       │   └── runner.py             # Benchmark execution logic
│       ├── run_mixed_concurrency.py  # Mixed concurrency script
│       ├── benchmark_queries.csv     # Query documentation
│       ├── plot.py                   # Visualization script
│       └── requirements.txt          # Python dependencies
├── config/
│   ├── credentials/                   # Credential configuration
│   └── settings.py                   # Application settings
├── benchmark_results/                 # Generated results (not committed)
├── QUICK_START_TPCH_EXTERNAL.md      # Quick start guide
└── README.md                         # This file
```

## ⚙️ Configuration

### Credentials Setup
Edit `config/credentials/credentials.json`:

```json
{
    "firebolt": {
        "account_name": "your_firebolt_account",
        "database": "your_database", 
        "engine_name": "your_engine",
        "auth": {
            "id": "your_service_account_id",
            "secret": "your_service_account_secret"
        }
    },
    "trino": {
        "host": "your_trino_host",
        "port": 443,
        "catalog": "your_catalog",
        "schema": "your_schema", 
        "user": "your_username",
        "password": "your_password",
        "use_https": true,
        "verify_ssl": true
    }
}
```

## 🏃‍♂️ Running Benchmarks

### Sequential Benchmarks
```bash
cd clients/python

# Test both engines
python -m src.main custom_schema --vendors firebolt,trino

# Test individual engines
python -m src.main custom_schema --vendors firebolt
python -m src.main custom_schema --vendors trino
```

### Concurrent Benchmarks
```bash
# 5 parallel queries for 30 seconds
python -m src.main custom_schema --vendors firebolt,trino --concurrency 5 --concurrency-duration-s 30

# 10 parallel queries for 60 seconds  
python -m src.main custom_schema --vendors firebolt,trino --concurrency 10 --concurrency-duration-s 60

# 15 parallel queries for 120 seconds
python -m src.main custom_schema --vendors firebolt,trino --concurrency 15 --concurrency-duration-s 120
```

### Mixed Concurrency Testing
```bash
# Run all 10 different queries in parallel
python run_mixed_concurrency.py
```

This script runs each of the 10 benchmark queries simultaneously, providing a realistic mixed workload scenario.

## 📈 Results Analysis

### Generated Files
- **`benchmark_results/`** - All result files organized by vendor and test type
- **`results.csv`** - Detailed execution metrics
- **CSV exports** - Structured data for analysis
- **Summary reports** - Human-readable performance summaries

### Visualization
```bash
# Generate comparison charts
python plot.py
```

### Key Metrics
- **Execution Time** - Query completion time
- **Throughput** - Queries per second
- **Success Rate** - Percentage of successful queries
- **Concurrency Handling** - Performance under load
- **Failure Points** - Where systems reach limits

## 🔧 Troubleshooting

### Common Issues

**Table Not Found**
```bash
# Verify external tables exist
python test_connection.py
```

**Connection Errors**
```bash
# Test individual connections
python test_trino_simple.py
python -m src.test_connection
```

**Schema Mismatches**
- Check table column names match expected schema in benchmark SQL files
- Verify data types are compatible

**High Concurrency Failures**
- Trino may crash at high concurrency (>10-15 parallel queries)
- Firebolt generally handles higher concurrency better
- Reduce concurrency level if encountering connection refused errors

### Performance Insights

**Firebolt Advantages:**
- Better concurrent query handling
- Consistent performance under load
- Superior aggregation performance

**Trino Advantages:**  
- Flexible data source connectivity
- Standard SQL compatibility
- Lower resource requirements for simple queries

**Failure Patterns:**
- Trino: Connection refused errors at high concurrency
- Firebolt: Authentication/resource limit errors
- Both: Timeout issues on complex queries

## 🤝 Contributing

1. **Add New Queries**: Extend `benchmark.sql` files
2. **Custom Connectors**: Add new database connectors in `src/connectors/`
3. **Visualization**: Enhance plotting and reporting tools
4. **Benchmarks**: Create new benchmark scenarios

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Links

- [Firebolt Documentation](https://docs.firebolt.io/)
- [Trino Documentation](https://trino.io/docs/)
- [TPCH Benchmark](http://www.tpc.org/tpch/)

---

**🎯 Ready to benchmark? Start with the [Quick Start Guide](QUICK_START_TPCH_EXTERNAL.md)!**