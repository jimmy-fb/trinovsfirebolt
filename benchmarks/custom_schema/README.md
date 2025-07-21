# 🎯 Custom Schema Benchmark

This benchmark is the **primary test suite** for comparing Trino and Firebolt performance on external table queries with TPCH-style data.

## 📊 Overview

The Custom Schema Benchmark consists of 10 comprehensive SQL queries designed to test different aspects of analytical query performance:

- **Basic aggregations** - Testing simple GROUP BY and COUNT operations
- **Multi-table joins** - Testing join performance across tables
- **Complex analytics** - Testing window functions, ranking, and advanced SQL features
- **Date/time operations** - Testing temporal query performance
- **Mixed workloads** - Testing various query patterns in combination

## 📋 Query Suite

| Query | Name | Description | Complexity | Tables Used |
|-------|------|-------------|------------|-------------|
| **Q1** | Customer Analysis | Basic customer aggregation with filtering | Basic | `customer` |
| **Q2** | Order Summary | Order totals by status | Basic | `orders` |
| **Q3** | Lineitem Analysis | Complex lineitem aggregation | Medium | `lineitem` |
| **Q4** | Customer-Order Join | Customer order count analysis | Medium | `customer`, `orders` |
| **Q5** | Product Analysis | Product aggregation by brand | Basic | `products` |
| **Q6** | Event Analysis | Event type counting | Basic | `events` |
| **Q7** | Multi-table Join | Complex 3-table join analysis | High | `customer`, `orders`, `lineitem` |
| **Q8** | Revenue by Date Range | Time-based revenue analysis | Medium | `orders` |
| **Q9** | Top Customers Ranking | Customer ranking by revenue | High | `customer`, `orders` |
| **Q10** | Product Performance | Product performance with lineitem data | High | `products`, `lineitem` |

## 📁 File Structure

```
custom_schema/
├── README.md              # This documentation
├── benchmark.sql          # Main benchmark queries (generic)
├── setup.sql             # Generic setup SQL
├── warmup.sql            # Warmup queries  
├── queries.json          # Concurrent benchmark configuration
├── firebolt/             # Firebolt-specific SQL files
│   ├── benchmark.sql     # Firebolt-optimized queries
│   ├── setup.sql         # Firebolt-specific setup
│   └── warmup.sql        # Firebolt warmup queries
└── trino/                # Trino-specific SQL files
    ├── benchmark.sql     # Trino-optimized queries  
    ├── setup.sql         # Trino-specific setup
    └── warmup.sql        # Trino warmup queries
```

## 🔧 Expected Schema

The benchmark expects the following tables to exist in both Trino and Firebolt:

### `customer` Table
```sql
customer (
    custkey     INTEGER,     -- Customer key (primary key)
    name        VARCHAR,     -- Customer name
    mktsegment  VARCHAR,     -- Market segment  
    acctbal     DECIMAL      -- Account balance
)
```

### `orders` Table  
```sql
orders (
    orderkey     INTEGER,    -- Order key (primary key)
    custkey      INTEGER,    -- Customer key (foreign key)
    orderstatus  VARCHAR,    -- Order status (O, F, P)
    totalprice   DECIMAL,    -- Total order price
    orderdate    DATE        -- Order date
)
```

### `lineitem` Table
```sql
lineitem (
    l_orderkey      INTEGER,    -- Order key (foreign key)
    l_partkey       INTEGER,    -- Part key (foreign key)  
    l_linenumber    INTEGER,    -- Line number
    l_quantity      DECIMAL,    -- Quantity
    l_extendedprice DECIMAL,    -- Extended price
    l_discount      DECIMAL,    -- Discount (0.00-1.00)
    l_tax           DECIMAL,    -- Tax rate
    l_returnflag    VARCHAR,    -- Return flag (R, A, N)
    l_linestatus    VARCHAR,    -- Line status (O, F)
    l_shipdate      DATE        -- Ship date
)
```

### `products` Table
```sql
products (
    p_partkey     INTEGER,    -- Part key (primary key)
    p_brand       VARCHAR,    -- Brand name
    p_type        VARCHAR,    -- Product type
    p_size        INTEGER,    -- Product size
    p_retailprice DECIMAL     -- Retail price
)
```

### `events` Table
```sql
events (
    event_type      VARCHAR,    -- Event type
    event_timestamp TIMESTAMP   -- Event timestamp
)
```

## 🚀 Running the Benchmark

### Prerequisites
1. External tables configured in both Trino and Firebolt
2. Credentials configured in `config/credentials/credentials.json`
3. Python dependencies installed (`pip install -r clients/python/requirements.txt`)

### Sequential Benchmark
```bash
cd clients/python
python -m src.main custom_schema --vendors firebolt,trino --execute-setup True
```

### Concurrent Benchmark
```bash
# 5 parallel queries for 30 seconds
python -m src.main custom_schema --vendors firebolt,trino --concurrency 5 --concurrency-duration-s 30

# 10 parallel queries for 60 seconds
python -m src.main custom_schema --vendors firebolt,trino --concurrency 10 --concurrency-duration-s 60
```

### Mixed Concurrency Test
```bash
# Run all 10 different queries in parallel
python run_mixed_concurrency.py
```

## 📈 Performance Expectations

### Typical Performance Patterns

**Firebolt Advantages:**
- **Q1, Q2, Q5, Q6**: Simple aggregations (faster execution)
- **Q3**: Complex aggregations with multiple conditions
- **Concurrent workloads**: Better handling of parallel queries

**Trino Advantages:**  
- **Q4, Q7**: Join operations (more optimized join algorithms)
- **Q8**: Date-based filtering and grouping
- **Q9, Q10**: Complex analytical queries with window functions

### Concurrency Behavior
- **Firebolt**: Handles 10-15+ concurrent queries well
- **Trino**: May experience connection issues at high concurrency (>10 queries)

## 🔍 Query Details

### Basic Queries (Q1, Q2, Q5, Q6)
- Test fundamental aggregation performance
- Single table access patterns
- Basic filtering and grouping

### Join Queries (Q4, Q7, Q9, Q10)  
- Test multi-table join performance
- Various join types and patterns
- Performance with different join sizes

### Complex Analytics (Q3, Q8, Q9, Q10)
- Window functions and ranking
- Complex filtering conditions  
- Date/time operations
- Advanced SQL features

## 🛠️ Customization

### Adding New Queries
1. Add SQL to `benchmark.sql`
2. Add vendor-specific versions to `firebolt/benchmark.sql` and `trino/benchmark.sql`
3. Update `queries.json` for concurrent testing
4. Update this documentation

### Modifying Existing Queries
1. Test changes in both Firebolt and Trino
2. Ensure SQL compatibility across both systems
3. Update vendor-specific files if needed
4. Test both sequential and concurrent execution

## 🔧 Troubleshooting

### Common Issues
- **Table not found**: Verify external tables are configured correctly
- **Column not found**: Check schema matches expected structure  
- **Permission denied**: Verify credentials and table access permissions
- **High concurrency failures**: Reduce concurrency level for Trino

### SQL Dialect Differences
- **Date functions**: Use `EXTRACT(YEAR FROM date)` instead of `YEAR(date)` for Firebolt
- **String functions**: Some string functions may differ between systems
- **Window functions**: Syntax may vary slightly between Firebolt and Trino

## 📊 Results Analysis

Results will be generated in:
- `benchmark_results/custom_schema/` - Sequential benchmark results
- `benchmark_results/mixed_concurrency/` - Mixed concurrency results  
- CSV files with detailed execution metrics
- Summary reports comparing performance

### Key Metrics
- **Execution Time**: Individual query performance
- **Throughput**: Queries per second under load
- **Success Rate**: Percentage of successful query executions  
- **Concurrency Scaling**: Performance degradation under load 