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

Adjust settings in [k6config.json](../../config/k6config.json) as desired.
Please be aware that the value provided for `VUs` should not be greater than
`number_of_threads * connections_per_thread`.

## Running the benchmark

Specify your vendor, then start a local client connection SDK proxy in a terminal:

```bash
/clients/k6$ export VENDOR=<vendor>
/clients/k6$ node connections-cluster.js
```

Open another terminal window, then run:

```bash
/clients/k6$ export VENDOR=<vendor>
/clients/k6$ k6 run fb-benchmark-k6.js
```