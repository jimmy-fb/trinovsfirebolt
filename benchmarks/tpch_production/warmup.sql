-- TPC-H Production Warmup Queries
-- These queries warm up the cluster before running performance tests

-- Basic connectivity test
SELECT 1;

-- Warm up small tables
SELECT COUNT(*) FROM region;
SELECT COUNT(*) FROM nation;

-- Warm up larger tables with simple queries
SELECT COUNT(*) FROM customer;
SELECT COUNT(*) FROM supplier;
SELECT COUNT(*) FROM orders LIMIT 1000; 