-- Custom Trino Benchmark Queries for tpch.sf1
-- Generated based on your environment discovery

-- Query 1: Table row counts (tests basic aggregation)
SELECT 'customer' as table_name, COUNT(*) as row_count FROM tpch.sf1.customer;
SELECT 'lineitem' as table_name, COUNT(*) as row_count FROM tpch.sf1.lineitem;
SELECT 'nation' as table_name, COUNT(*) as row_count FROM tpch.sf1.nation;
SELECT 'orders' as table_name, COUNT(*) as row_count FROM tpch.sf1.orders;
SELECT 'part' as table_name, COUNT(*) as row_count FROM tpch.sf1.part;

-- Query 2: Schema analysis (tests metadata queries)
SELECT table_name, COUNT(*) as column_count 
FROM information_schema.columns 
WHERE table_catalog = 'tpch' AND table_schema = 'sf1'
GROUP BY table_name
ORDER BY column_count DESC;

-- Query 3: Cross-table analysis (customize based on your data relationships)
-- Add JOIN queries here based on your table relationships

-- Query 4: Analytical query template (customize based on your data)
-- SELECT 
--     date_column,
--     COUNT(*) as daily_count,
--     SUM(numeric_column) as daily_sum
-- FROM tpch.sf1.your_table
-- WHERE date_column >= DATE '2023-01-01'
-- GROUP BY date_column
-- ORDER BY date_column;
