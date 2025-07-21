-- Firebolt TPCH External Tables Setup
-- This assumes external tables are already configured in Firebolt

-- Check if external tables exist and are accessible
SELECT 'Checking Firebolt external tables...' as status;

-- Verify external table access
SELECT COUNT(*) as customer_count FROM customer LIMIT 1;
SELECT COUNT(*) as lineitem_count FROM lineitem LIMIT 1;
SELECT COUNT(*) as products_count FROM products LIMIT 1;
SELECT COUNT(*) as events_count FROM events LIMIT 1;
SELECT COUNT(*) as orders_count FROM orders LIMIT 1;

SELECT 'Firebolt TPCH external tables ready for benchmarking' as status; 