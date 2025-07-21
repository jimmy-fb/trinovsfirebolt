-- Production Trino Benchmark Setup
-- This script will create test data for benchmarking

-- First, let's check what catalogs are available
-- SHOW CATALOGS;

-- Example setup for different catalog types:

-- FOR HIVE CATALOG (most common):
-- CREATE SCHEMA IF NOT EXISTS hive.benchmark_test;
-- 
-- CREATE TABLE IF NOT EXISTS hive.benchmark_test.orders (
--     order_id BIGINT,
--     customer_id BIGINT,
--     order_date DATE,
--     total_amount DECIMAL(10,2),
--     status VARCHAR(20),
--     region VARCHAR(50)
-- );
-- 
-- CREATE TABLE IF NOT EXISTS hive.benchmark_test.customers (
--     customer_id BIGINT,
--     name VARCHAR(100),
--     email VARCHAR(100),
--     signup_date DATE,
--     tier VARCHAR(20)
-- );

-- FOR ICEBERG CATALOG:
-- CREATE SCHEMA IF NOT EXISTS iceberg.benchmark_test;
-- 
-- CREATE TABLE IF NOT EXISTS iceberg.benchmark_test.sales (
--     sale_id BIGINT,
--     product_id BIGINT,
--     quantity INTEGER,
--     price DECIMAL(10,2),
--     sale_date TIMESTAMP,
--     store_id INTEGER
-- );

-- FOR POSTGRESQL/MYSQL CATALOGS (if connecting to existing databases):
-- Note: Tables already exist, no setup needed

-- FOR MEMORY CATALOG (if available - good for testing):
-- CREATE SCHEMA IF NOT EXISTS memory.benchmark_test;
-- 
-- CREATE TABLE memory.benchmark_test.test_data AS
-- SELECT 
--     row_number() OVER () as id,
--     'customer_' || cast(row_number() OVER () as varchar) as name,
--     random() * 1000 as value,
--     date_add('day', cast(random() * 365 as integer), date '2023-01-01') as created_date
-- FROM (SELECT 1) t
-- CROSS JOIN UNNEST(sequence(1, 100000)) AS t(i);

-- Generic test using information_schema (works with any catalog)
SELECT 'Setup checking available schemas...' as status; 