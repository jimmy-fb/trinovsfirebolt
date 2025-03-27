# 🚦 Concurrency Test Results (k6)

This document contains concurrency benchmark results from tests across three platforms.

## 🧾 Column Explanations

| Column Name         | Description                     |
|---------------------|---------------------------------|
| Engine              | The engine configuration used   |
| $/Hour              | The hourly cost of that engine  |
| VUs                 | Number of concurrent queries    |
| QPS                 | Resultant queries per second    |
| $/Perf x 1M         | Dollars per query per hour * 1M |
| Med Latency         | Median query latency            |
| Avg Latency         | Average query latency           |
| P95 Latency         | 95th percentile query latency   |

### 📊 Firebolt Results

| Engine | $/Hour | VUs | QPS | $/Perf x 1M | Med Latency | Avg Latency | P95 Latency |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Firebolt (1 x S) | $1.4 | 60 | 122.2 | $3.18 | 0.461 | 0.487 | 0.846 |
| Firebolt (1 x M) | $2.8 | 60 | 223.4 | $3.48 | 0.26 | 0.267 | 0.403 |
| Firebolt (1 x L) | $5.6 | 60 | 354.7 | $4.39 | 0.164 | 0.168 | 0.249 |
| Firebolt (1 x XL) | $11.2 | 60 | 368.6 | $8.44 | 0.149 | 0.162 | 0.286 |
| Firebolt (1 x M) 2C | $5.6 | 120 | 422.6 | $3.68 | 0.268 | 0.283 | 0.53 |
| Firebolt (1 x M) 4C | $11.2 | 240 | 854.4 | $3.64 | 0.242 | 0.279 | 0.64 |
| Firebolt (1 x M) 8C | $22.4 | 480 | 1696.9 | $3.67 | 0.239 | 0.281 | 0.667 |
| Firebolt (1 x L) 2C | $11.2 | 120 | 663.0 | $4.69 | 0.168 | 0.18 | 0.359 |
| Firebolt (1 x L) 4C | $22.4 | 240 | 1261.6 | $4.93 | 0.151 | 0.189 | 0.472 |
| Firebolt (1 x L) 8C | $44.8 | 480 | 2494.9 | $4.99 | 0.121 | 0.191 | 0.588 |

### 📊 Redshift Results

| Engine | $/Hour | VUs | QPS | $/Perf x 1M | Med Latency | Avg Latency | P95 Latency |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Redshift (3 x ra3.xlplus) | $3.26 | 60 | 67.3 | $13.5 | 0.755 | 0.876 | 1.72 |
| Redshift (2 x ra3.4xlarge) | $6.52 | 60 | 119.4 | $15.2 | 0.414 | 0.494 | 0.943 |
| Redshift (4 x ra3.4xlarge) | $13.04 | 60 | 132.2 | $27.4 | 0.384 | 0.448 | 0.905 |
| Redshift (8 x ra3.4xlarge) | $26.08 | 60 | 140.6 | $51.5 | 0.372 | 0.423 | 0.851 |
| Redshift (2 x ra3.16xlarge) | $39.12 | 60 | 126.5 | $85.9 | 0.416 | 0.47 | 0.898 |
| Redshift (4 x ra3.4xlarge) | $13.04 | 120 | 156.6 | $23.1 | 0.638 | 0.756 | 1.52 |
| Redshift (8 x ra3.4xlarge) | $26.08 | 240 | 165.5 | $43.8 | 1.12 | 1.41 | 2.98 |
| Redshift (2 x ra3.16xlarge) | $39.12 | 480 | 136.1 | $79.8 | 2.76 | 3.4 | 7.63 |

### 📊 Snowflake Results

| Engine | $/Hour | VUs | QPS | $/Perf x 1M | Med Latency | Avg Latency | P95 Latency |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Snowflake (XS) | $3 | 60 | 61.8 | $13.5 | 0.961 | 0.967 | 1.4 |
| Snowflake (S) | $6 | 60 | 83.9 | $19.9 | 0.719 | 0.704 | 1.01 |
| Snowflake (M) | $12 | 60 | 80.9 | $41.2 | 0.715 | 0.736 | 0.88 |
| Snowflake (L) | $24 | 60 | 88.6 | $75.2 | 0.668 | 0.672 | 0.758 |
| Snowflake (XL) | $48 | 60 | 85.2 | $156 | 0.676 | 0.695 | 0.91 |
| Snowflake (S) 2C | $12 | 120 | $162.6 | 20.5 | 0.728 | 0.727 | 0.996 |
| Snowflake (S) 4C | $24 | 240 | $304.5 | 21.9 | 0.774 | 0.777 | 1.05 |
| Snowflake (S) 8C | $48 | 480 | $639.0 | 20.9 | 0.714 | 0.733 | 0.988 |
| Snowflake (M) 2C | $24 | 120 | $165.0 | 40.4 | 0.706 | 0.72 | 0.855 |
| Snowflake (M) 4C | $48 | 240 | $323.6 | 41.2 | 0.711 | 0.734 | 0.914 |
| Snowflake (M) 8C | $96 | 480 | $540.7 | 49.3 | 0.554 | 0.753 | 2.41 |