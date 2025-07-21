# TPCH External Tables Benchmark

This benchmark is designed to test performance of Firebolt and Trino when reading from external tables containing TPCH data.

## Prerequisites

Before running this benchmark, ensure you have:

1. **External tables configured** in both Firebolt and Trino with the following tables:
   - `customer` - Customer information
   - `lineitem` - Line item details from orders
   - `products` - Product information
   - `events` - Event data
   - `orders` - Order information

2. **Credentials configured** in `config/credentials/credentials.json` for both Firebolt and Trino

## Table Schema Expectations

The benchmark expects the following table schemas:

### Customer Table
- `custkey` - Customer key (primary key)
- `name` - Customer name
- `mktsegment` - Market segment
- `acctbal` - Account balance

### Orders Table
- `orderkey` - Order key (primary key)
- `custkey` - Customer key (foreign key)
- `orderstatus` - Order status
- `totalprice` - Total order price
- `orderdate` - Order date

### Lineitem Table
- `l_orderkey` - Order key (foreign key)
- `l_partkey` - Part key (foreign key)
- `l_linenumber` - Line number
- `l_quantity` - Quantity
- `l_extendedprice` - Extended price
- `l_discount` - Discount
- `l_tax` - Tax
- `l_returnflag` - Return flag
- `l_linestatus` - Line status
- `l_shipdate` - Ship date

### Products Table
- `p_partkey` - Part key (primary key)
- `p_brand` - Brand
- `p_type` - Product type
- `p_size` - Product size
- `p_retailprice` - Retail price

### Events Table
- `event_type` - Type of event
- `event_timestamp` - Event timestamp

## Running the Benchmark

### Sequential Benchmark (Power Run)

```bash
cd clients/python
python -m src.main tpch_external_tables --vendors firebolt,trino --execute-setup True
```

### Concurrent Benchmark

```bash
cd clients/python
python -m src.main tpch_external_tables --vendors firebolt,trino --concurrency 10 --concurrency-duration-s 60
```

### Running Individual Vendors

For Firebolt only:
```bash
python -m src.main tpch_external_tables --vendors firebolt --execute-setup True
```

For Trino only:
```bash
python -m src.main tpch_external_tables --vendors trino --execute-setup True
```

## Benchmark Queries

The benchmark includes 10 comprehensive queries:

1. **Customer Analysis** - Basic aggregation on customer table
2. **Order Summary** - Aggregation on orders table
3. **Lineitem Analysis** - Complex aggregation on lineitem table
4. **Customer-Order Join** - Testing join performance
5. **Products Analysis** - Testing products table
6. **Events Analysis** - Testing events table
7. **Complex Multi-table Join** - Customer, Orders, Lineitem join
8. **Revenue by Date Range** - Time-based analysis
9. **Top Customers by Revenue** - Ranking query
10. **Product Performance** - Products and Lineitem join

## Expected Results

The benchmark will generate:
- Query execution times for each vendor
- Performance comparisons between Firebolt and Trino
- CSV export of results
- Visual charts (if visual exporter is enabled)

## Troubleshooting

### Common Issues

1. **Table not found errors**: Ensure external tables are properly configured
2. **Connection errors**: Verify credentials in `config/credentials/credentials.json`
3. **Schema mismatch**: Check that table column names match the expected schema

### Debugging

To test connections individually:
```bash
python test_connection.py
```

## Customization

You can modify the queries in:
- `benchmark.sql` - Main benchmark queries
- `firebolt/benchmark.sql` - Firebolt-specific queries
- `trino/benchmark.sql` - Trino-specific queries
- `queries.json` - Concurrent benchmark queries 