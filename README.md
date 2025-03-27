# Firebolt Benchmarks

In this repo, you’ll find the FireScale benchmark, as well as the benchmarking clients
and benchmark results that Firebolt has published. This includes the DDL and queries
for setting up and running FireScale on different vendors, as well as the results for
how various vendors performed on FireScale.

## FireScale Benchmark Results

[To view benchmark results, click here.](results/)

## Run FireScale Yourself

Firebolt has provided two clients in this repo: one written in Python, and one written
with Node.js with Grafana K6. The Python client is for power runs (executing one query
at a time in a sequential pattern) and for concurrency benchmarking with low expected
query throughput (<100 QPS). The K6 client is for benchmarking concurrent scenarios
with high query volumes and hundreds or thousands of queries being completed each second.

The Python client can be extended with benchmarks beyond just FireScale, though at this
time, only FireScale and TPCH queries are provided.

View each client:

* [Python client](/clients/python/)
* [K6 client](/clients/k6/)

## Directory Structure

```
project-root/
│
├── benchmarks/              # Contains benchmark definitions
│   ├── sample_benchmark/
│   │   ├── benchmark.sql          # General benchmark SQL file for sample_benchmark
│   │   ├── setup.sql              # General setup SQL file for sample_benchmark
│   │   └── firebolt/
│   │       └── setup.sql          # firebolt specific setup SQL file
│   │   
│   ├── FireScale/
│   │   ├── firebolt/
│   │   │   ├── benchmark.sql      # firebolt specific benchmark SQL file
│   │   │   ├── setup.sql          # firebolt specific setup SQL file
│   │   |   ├── warmup.sql         # firebolt specific warmup SQL file
│   │   |   └── queries.json       # queries for concurrent benchmarking w/Python
│   │   │
│   │   ├── snowflake/
│   │   |   ├── benchmark.sql      # snowflake specific benchmark SQL file
│   │   |   ├── setup.sql          # snowflake specific setup SQL file
│   │   |   ├── warmup.sql         # snowflake specific warmup SQL file
│   │   |   └── queries.json       # queries for concurrent benchmarking w/Python
│   │   │
│   │   ├── bigquery/
│   │   |   ├── benchmark.sql      # bigquery specific benchmark SQL file
│   │   |   └── setup.sql          # bigquery specific setup SQL file
│   │   │
│   │   ├── redshift/
│   │   |   ├── benchmark.sql      # redshift specific benchmark SQL file
│   │   |   ├── setup.sql          # redshift specific setup SQL file
│   │   |   ├── warmup.sql         # redshift specific warmup SQL file
│   │   |   └── queries.json       # queries for concurrent benchmarking w/Python
|   |   |
│   |   └── warmup.sql    # generic SQL warmup file for FireScale
|   |
|   ├── FireScale_k6/   # query files for K6 concurrent benchmarking for each vendor
|   |   ├── queries_firebolt.js
|   |   ├── queries_redshift.js
|   |   └── queries_snowflake.js
|   |
|   └── tpch/
|       ├── firebolt/
|       |   └── benchmark.sql      # TPCH queries for Firebolt
|       └── warmup.sql    # generic SQL warmup file for TPCH benchmark
|
├── clients/
│   ├── python/
|   |   ├── src/
|   |   │   ├── connectors/          # Contains connector implementations
|   |   │   │   ├── base.py
|   |   │   │   ├── firebolt.py
|   |   │   │   ├── bigquery.py
|   |   │   │   ├── redshift.py
|   |   │   │   └── snowflake.py
|   |   |   ├── exporters/           # Contains exporter implementations
|   |   │   │   ├── base.py
|   |   │   │   ├── csv_exporter.py
|   |   │   │   └── visual_exporter.py
|   |   │   ├── main.py              # Entry point for the Python client tool
|   |   │   └── runner.py            # Python benchmark runner logic
|   |   ├── README.md                # Python client documentation
|   |   └── requirements.txt         # Python package dependencies
|   |
│   └── k6/
|       ├── connections-cluster.js   # entrypoint for K6 benchmarking
|       ├── connections-server.js    # connections to vendors for K6
|       ├── fb-benchmark-k6.js       # K6 benchmark runner
|       ├── package-lock.json        
|       ├── package.json
|       └── README.md                # K6 client documentation
|
├── config/                  # Contains configuration files for the application
│   ├── credentials/         # Contains credential files
│   │   ├── credentials.json  # Single credentials file for all vendors (ignored)
│   │   └── sample_credentials.json  # Sample credentials file with dummy data
│   ├── k6config.json   # k6 configuration options
│   └── settings.py     # python client settings (not recommneded to change)
|
├── results/        # benchmark results for various benchmarks and vendors
|   └── ...
|
├── requirements.txt         # Python package dependencies
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