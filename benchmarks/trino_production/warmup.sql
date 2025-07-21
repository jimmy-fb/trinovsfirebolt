-- Production Trino Warmup Queries
-- These queries warm up the cluster and cache before running benchmarks

-- Simple warmup queries
SELECT 1;

-- Catalog discovery warmup
SELECT COUNT(*) FROM information_schema.schemata;

-- Table metadata warmup  
SELECT COUNT(*) FROM information_schema.tables;

-- Column metadata warmup
SELECT COUNT(*) FROM information_schema.columns LIMIT 1000; 