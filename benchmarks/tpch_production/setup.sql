-- TPC-H Production Benchmark Setup
-- This uses the standard TPC-H dataset available in your Starburst cluster

-- The TPC-H dataset is already available, no setup needed
-- Available tables in tpch.sf1:
-- - customer (150,000 rows)
-- - lineitem (6,001,215 rows) 
-- - nation (25 rows)
-- - orders (1,500,000 rows)
-- - part (200,000 rows)
-- - partsupp (800,000 rows)
-- - region (5 rows)
-- - supplier (10,000 rows)

SELECT 'TPC-H dataset ready for benchmarking' as status; 