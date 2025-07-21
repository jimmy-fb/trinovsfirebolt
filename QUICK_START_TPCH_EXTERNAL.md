# Quick Start Guide: TPCH External Tables Benchmark

This guide will help you quickly set up and run the TPCH external tables benchmark to compare Firebolt and Trino performance.

## Prerequisites

1. **External tables configured** in both Firebolt and Trino with these tables:
   - `customer`
   - `lineitem` 
   - `products`
   - `events`
   - `orders`

2. **Access credentials** for both Firebolt and Trino

## Step 1: Configure Credentials

Copy the sample credentials file and fill in your details:

```bash
cp config/credentials/sample_credentials.json config/credentials/credentials.json
```

Edit `config/credentials/credentials.json` and update the Firebolt and Trino sections:

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

## Step 2: Install Dependencies

```bash
cd clients/python
pip install -r requirements.txt
```

## Step 3: Test Setup

Run the verification script to ensure everything is configured correctly:

```bash
python test_tpch_external_benchmark.py
```

## Step 4: Run the Benchmark

### Option A: Sequential Benchmark (Recommended for first run)

```bash
cd clients/python
python -m src.main tpch_external_tables --vendors firebolt,trino --execute-setup True
```

### Option B: Concurrent Benchmark (For load testing)

```bash
cd clients/python
python -m src.main tpch_external_tables --vendors firebolt,trino --concurrency 10 --concurrency-duration-s 60
```

### Option C: Test Individual Vendors

Test Firebolt only:
```bash
python -m src.main tpch_external_tables --vendors firebolt --execute-setup True
```

Test Trino only:
```bash
python -m src.main tpch_external_tables --vendors trino --execute-setup True
```

## Step 5: View Results

Results will be saved in the `benchmark_results` directory:
- CSV files with detailed query performance data
- Summary reports comparing Firebolt vs Trino performance

## Troubleshooting

### Common Issues

1. **"Table not found" errors**
   - Ensure external tables are properly configured in both systems
   - Verify table names match: `customer`, `lineitem`, `products`, `events`, `orders`

2. **Connection errors**
   - Check credentials in `config/credentials/credentials.json`
   - Verify network connectivity to both Firebolt and Trino

3. **Schema mismatch errors**
   - Ensure table column names match the expected schema
   - Check the README in `benchmarks/tpch_external_tables/` for expected schema

### Test Connections

To test individual connections:

```bash
python test_connection.py
```

## Expected Performance Insights

The benchmark will test:

1. **Single-table queries** - Basic aggregation performance
2. **Join performance** - Multi-table join efficiency
3. **Complex analytics** - Window functions and ranking
4. **Concurrent query handling** - System scalability

## Customization

You can modify the benchmark queries in:
- `benchmarks/tpch_external_tables/benchmark.sql` - Main queries
- `benchmarks/tpch_external_tables/firebolt/benchmark.sql` - Firebolt-specific
- `benchmarks/tpch_external_tables/trino/benchmark.sql` - Trino-specific

## Next Steps

After running the benchmark:
1. Analyze the performance differences
2. Identify bottlenecks in each system
3. Optimize queries based on the results
4. Run additional tests with different data sizes or query patterns 