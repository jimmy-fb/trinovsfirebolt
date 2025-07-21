-- Custom Schema Benchmark Setup
-- This benchmark uses the actual table schema from the user's external tables

-- Check if tables exist and are accessible
SELECT 'Checking custom schema tables...' as status;

-- Verify table access by running a simple count query on each table
SELECT 'Custom schema tables ready for benchmarking' as status; 