# K6 Benchmark Client

The K6 benchmark is intended to benchmark vendors at high levels of concurrency.
With at least 8 threads, it can measure 2500+ QPS.

## Installation

```bash
/clients/k6$ npm install
/clients/k6$ brew install k6
```

## Configuration

Make sure you have created a `credentials.json` file in the
[/config/credentials](../../config/credentials/) folder and provided the
necessary vendor credentials.

Adjust settings in [k6config.json](../../config/k6config.json) as desired. Config options:

* vendor: the vendor you want to benchmark. Supported options are `firebolt`, `snowflake`,
and `redshift`.
* number_of_threads: the number of threads you'd like to run the benchmark on
* connections_per_thread: how many connections you'd like to establish on each thread
* duration: how long you'd like your K6 run to last
* VUs: the number of [K6 VUs](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-vus/)
you'd like to use as part of your run

The total number of connections you establish will be `connections_per_thread * number_of_threads`.
Please be aware that your number of VUs should not be greater than this number.

## Running the benchmark

Start a local client connection SDK proxy in a terminal:

```bash
/clients/k6$ node connections-cluster.js
```

Open another terminal window, then run:

```bash
/clients/k6$ k6 run fb-benchmark-k6.js
```