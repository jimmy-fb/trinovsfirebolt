-- Firebolt TPCH External Tables Warmup Queries
-- Simple queries to warm up the cache before running the main benchmark

-- Warmup 1: Simple count on customer table
SELECT COUNT(*) as customer_count FROM customer;

-- Warmup 2: Simple count on lineitem table  
SELECT COUNT(*) as lineitem_count FROM lineitem;

-- Warmup 3: Simple count on products table
SELECT COUNT(*) as products_count FROM products;

-- Warmup 4: Simple count on events table
SELECT COUNT(*) as events_count FROM events;

-- Warmup 5: Simple count on orders table
SELECT COUNT(*) as orders_count FROM orders; 