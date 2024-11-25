# Benchmark Tool

## Description

This project provides a benchmarking tool for various data warehouse vendors, allowing users to compare performance across different systems. The tool supports multiple vendors, including Snowflake, Firebolt, and BigQuery, and can execute custom SQL queries for benchmarking.

## Features

- Connect to multiple data warehouse vendors.
- Execute benchmark queries and collect results.
- Optionally execute a setup SQL file before running benchmarks.
- Easily configurable through command-line arguments.

## Requirements

Make sure to install the required packages listed in `requirements.txt`.

```bash
pip install -r requirements.txt
```

## Credential Files

To connect to the data warehouse vendors, you need to provide a single credentials file located at `config/credentials/credentials.json`. The expected format for this file is as follows:

```json
{
    "snowflake": {
        "account": "your_account",
        "user": "your_username",
        "password": "your_password",
        "database": "your_database",
        "schema": "your_schema",
        "warehouse": "your_warehouse"
    },
    "firebolt": {
        "account": "your_account",
        "user": "your_username",
        "password": "your_password",
        "database": "your_database"
    },
    "bigquery": {
        "project_id": "your_project_id",
        "key_file": "path_to_your_service_account_key.json"
    }
}
```

## Usage

To run the benchmark, use the following command:

```bash
python -m src.main <benchmark_name> --vendors <vendor1,vendor2,...> [--execute-setup <True|False>]
```

### Options

- `benchmark_name`: The name of the benchmark to run.
- `--vendors`: Comma-separated list of vendors to benchmark (e.g., `snowflake,firebolt`).
- `--execute-setup`: (Optional) Set to `True` to execute the `setup.sql` file before running benchmarks. Default is `False`.
- `--pool-size`: (Optional) Connection pool size. Default is `5`.
- `--concurrency`: (Optional) Concurrency level. Default is `1`.
- `--output-dir`: (Optional) Output directory. Default is `benchmark_results`.
- `--creds`: (Optional) Path to credentials file. Default is `config/credentials/credentials.json`.

### Example

```bash
python -m src.main my_benchmark --vendors snowflake,firebolt --execute-setup True
```

## Directory Structure

```
project-root/
│
├── src/
│   ├── connectors/          # Contains connector implementations for each vendor
│   ├── main.py              # Entry point for the benchmarking tool
│   └── runner.py            # Benchmark runner logic
│
├── benchmarks/              # Contains benchmark definitions
│   ├── tpcg/                # TPC-G benchmark folder
│   │   ├── tpcg.sql         # Benchmark SQL file for TPC-G
│   │   ├── snowflake_setup.sql  # Setup SQL for Snowflake
│   │   ├── firebolt_setup.sql   # Setup SQL for Firebolt
│   │   └── bigquery_setup.sql   # Setup SQL for BigQuery
│   └── another_benchmark/   # Another benchmark folder
│       ├── another_benchmark.sql # Benchmark SQL file for another benchmark
│       ├── snowflake_setup.sql
│       ├── firebolt_setup.sql
│       └── bigquery_setup.sql
│
├── tests/                   # Contains unit and integration tests
│   ├── test_connectors.py   # Tests for connector implementations
│   ├── test_benchmarks.py    # Tests for benchmark logic
│   └── test_runner.py       # Tests for the benchmark runner
│
├── config/                  # Contains configuration files for the application
│   ├── credentials/         # Contains credential files
│   │   ├── credentials.json  # Single credentials file for all vendors (ignored)
│   │   └── sample_credentials.json  # Sample credentials file with dummy data
│   ├── config.yaml          # Example configuration file
│   └── other_config.json     # Other configuration files
│
├── requirements.txt         # Python package dependencies
├── run_benchmark.sh        # Bash script to run the benchmark
├── LICENSE                # MIT License
└── README.md                # Project documentation
```

## Contributing

Contributions are welcome! Please follow these steps to contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes and commit them (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
