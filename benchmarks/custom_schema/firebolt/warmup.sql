-- Firebolt Custom Schema Warmup Queries
-- Simple queries to warm up the Firebolt engine and cache

-- Warmup 1: Simple customer count
SELECT COUNT(*) FROM customers;

-- Warmup 2: Simple order count with basic filter
SELECT COUNT(*) FROM orders WHERE order_date >= TIMESTAMP '2023-01-01 00:00:00';

-- Warmup 3: Simple lineitem aggregation
SELECT SUM(quantity) FROM lineitem;

-- Warmup 4: Simple product count
SELECT COUNT(*) FROM products;

-- Warmup 5: Simple event count
SELECT COUNT(*) FROM events WHERE event_timestamp >= TIMESTAMP '2023-01-01 00:00:00'; 