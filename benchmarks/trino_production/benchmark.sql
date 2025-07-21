-- Production Trino Benchmark Queries
-- These queries test different aspects of Trino performance

-- Query 1: Simple Aggregation (tests basic query processing)
-- Replace 'your_catalog.your_schema.your_table' with actual table
-- SELECT COUNT(*), AVG(price), SUM(quantity) FROM your_catalog.sales.orders WHERE order_date >= date '2023-01-01';

-- Query 2: JOIN Performance (tests distributed joins)
-- SELECT c.customer_tier, COUNT(*) as order_count, SUM(o.total_amount) as total_sales
-- FROM your_catalog.sales.orders o
-- JOIN your_catalog.sales.customers c ON o.customer_id = c.customer_id
-- WHERE o.order_date >= date '2023-01-01'
-- GROUP BY c.customer_tier
-- ORDER BY total_sales DESC;

-- Query 3: Window Functions (tests analytical queries)
-- SELECT 
--     product_id,
--     sale_date,
--     daily_sales,
--     AVG(daily_sales) OVER (PARTITION BY product_id ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7d
-- FROM (
--     SELECT product_id, date(sale_timestamp) as sale_date, SUM(amount) as daily_sales
--     FROM your_catalog.sales.transactions
--     WHERE sale_timestamp >= timestamp '2023-01-01'
--     GROUP BY product_id, date(sale_timestamp)
-- ) daily_sales
-- ORDER BY product_id, sale_date;

-- Query 4: Complex Aggregation with Multiple Grouping Sets
-- SELECT 
--     region,
--     product_category,
--     COUNT(DISTINCT customer_id) as unique_customers,
--     SUM(revenue) as total_revenue,
--     AVG(order_value) as avg_order_value
-- FROM your_catalog.analytics.sales_summary
-- WHERE report_date >= date '2023-01-01'
-- GROUP BY GROUPING SETS (
--     (region, product_category),
--     (region),
--     (product_category),
--     ()
-- )
-- ORDER BY region NULLS LAST, product_category NULLS LAST;

-- For immediate testing with any Trino server, use information_schema queries:

-- Test query 1: Catalog discovery
SELECT catalog_name, COUNT(*) as schema_count 
FROM information_schema.schemata 
GROUP BY catalog_name 
ORDER BY schema_count DESC;

-- Test query 2: Table analysis  
SELECT table_schema, COUNT(*) as table_count
FROM information_schema.tables 
WHERE table_schema NOT IN ('information_schema')
GROUP BY table_schema
ORDER BY table_count DESC
LIMIT 10;

-- Test query 3: Column statistics
SELECT 
    table_schema,
    table_name,
    COUNT(*) as column_count,
    COUNT(CASE WHEN is_nullable = 'YES' THEN 1 END) as nullable_columns
FROM information_schema.columns 
WHERE table_schema NOT IN ('information_schema')
GROUP BY table_schema, table_name
ORDER BY column_count DESC
LIMIT 20;

-- Test query 4: Cross-catalog analysis
SELECT 
    table_catalog,
    COUNT(DISTINCT table_schema) as schemas,
    COUNT(DISTINCT table_name) as tables,
    COUNT(*) as total_columns
FROM information_schema.columns
GROUP BY table_catalog
ORDER BY total_columns DESC; 