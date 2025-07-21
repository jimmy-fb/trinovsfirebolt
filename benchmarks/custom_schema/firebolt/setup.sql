-- Firebolt Custom Schema Setup
-- This assumes external tables are already configured in Firebolt

-- Check if external tables exist and are accessible
SELECT 'Checking Firebolt custom schema tables...' as status;

-- Verify external table access with Firebolt syntax
SELECT COUNT(*) as customer_count FROM customers LIMIT 1;
SELECT COUNT(*) as lineitem_count FROM lineitem LIMIT 1;
SELECT COUNT(*) as products_count FROM products LIMIT 1;
SELECT COUNT(*) as events_count FROM events LIMIT 1;
SELECT COUNT(*) as orders_count FROM orders LIMIT 1;

SELECT 'Firebolt custom schema tables ready for benchmarking' as status; 