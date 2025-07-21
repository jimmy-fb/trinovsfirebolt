-- Trino TPCH External Tables Setup
-- This assumes external tables are already configured in Trino

-- Check if external tables exist and are accessible
SELECT 'Checking Trino external tables...' as status;

-- Verify external table access
SELECT COUNT(*) as customer_count FROM customers LIMIT 1;
SELECT COUNT(*) as lineitem_count FROM lineitem LIMIT 1;
SELECT COUNT(*) as products_count FROM products LIMIT 1;
SELECT COUNT(*) as events_count FROM events LIMIT 1;
SELECT COUNT(*) as orders_count FROM orders LIMIT 1;

SELECT 'Trino TPCH external tables ready for benchmarking' as status; 