-- Firebolt TPCH External Tables Benchmark Queries
-- Optimized for Firebolt SQL dialect

-- Query 1: Customer Analysis - Basic aggregation on customer table
SELECT 
    COUNT(*) as total_customers,
    COUNT(DISTINCT custkey) as unique_customers,
    AVG(acctbal) as avg_account_balance,
    MIN(acctbal) as min_account_balance,
    MAX(acctbal) as max_account_balance
FROM customer;

-- Query 2: Order Summary - Aggregation on orders table
SELECT 
    orderstatus,
    COUNT(*) as order_count,
    SUM(totalprice) as total_revenue,
    AVG(totalprice) as avg_order_value,
    MIN(orderdate) as earliest_order,
    MAX(orderdate) as latest_order
FROM orders 
WHERE orderdate >= DATE '1995-01-01'
GROUP BY orderstatus
ORDER BY total_revenue DESC;

-- Query 3: Lineitem Analysis - Complex aggregation on lineitem table
SELECT 
    l_returnflag,
    l_linestatus,
    COUNT(*) as line_count,
    SUM(l_quantity) as total_quantity,
    SUM(l_extendedprice) as total_extended_price,
    AVG(l_discount) as avg_discount,
    AVG(l_tax) as avg_tax
FROM lineitem
WHERE l_shipdate >= DATE '1995-01-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- Query 4: Customer-Order Join - Testing join performance
SELECT 
    c.mktsegment,
    COUNT(DISTINCT c.custkey) as customer_count,
    COUNT(o.orderkey) as total_orders,
    SUM(o.totalprice) as total_revenue,
    AVG(o.totalprice) as avg_order_value
FROM customer c
JOIN orders o ON c.custkey = o.custkey
WHERE o.orderdate >= DATE '1995-01-01'
GROUP BY c.mktsegment
ORDER BY total_revenue DESC;

-- Query 5: Products Analysis - Testing products table
SELECT 
    COUNT(*) as total_products,
    COUNT(DISTINCT p_type) as unique_product_types,
    COUNT(DISTINCT p_brand) as unique_brands,
    AVG(p_size) as avg_product_size,
    MIN(p_retailprice) as min_retail_price,
    MAX(p_retailprice) as max_retail_price
FROM products;

-- Query 6: Events Analysis - Testing events table
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT event_type) as unique_event_types,
    MIN(event_timestamp) as earliest_event,
    MAX(event_timestamp) as latest_event
FROM events
WHERE event_timestamp >= TIMESTAMP '1995-01-01 00:00:00';

-- Query 7: Complex Multi-table Join - Customer, Orders, Lineitem
SELECT 
    c.mktsegment,
    COUNT(DISTINCT c.custkey) as customer_count,
    COUNT(DISTINCT o.orderkey) as order_count,
    COUNT(l.linenumber) as lineitem_count,
    SUM(l.l_extendedprice) as total_revenue,
    AVG(l.l_quantity) as avg_quantity
FROM customer c
JOIN orders o ON c.custkey = o.custkey
JOIN lineitem l ON o.orderkey = l.l_orderkey
WHERE o.orderdate >= DATE '1995-01-01'
  AND l.l_shipdate >= DATE '1995-01-01'
GROUP BY c.mktsegment
ORDER BY total_revenue DESC;

-- Query 8: Revenue by Date Range - Time-based analysis
SELECT 
    EXTRACT(YEAR FROM o.orderdate) as order_year,
    EXTRACT(MONTH FROM o.orderdate) as order_month,
    COUNT(DISTINCT o.orderkey) as order_count,
    SUM(o.totalprice) as monthly_revenue,
    AVG(o.totalprice) as avg_order_value
FROM orders o
WHERE o.orderdate >= DATE '1995-01-01' AND o.orderdate < DATE '1997-01-01'
GROUP BY EXTRACT(YEAR FROM o.orderdate), EXTRACT(MONTH FROM o.orderdate)
ORDER BY order_year, order_month;

-- Query 9: Top Customers by Revenue - Ranking query
SELECT 
    c.custkey,
    c.name,
    c.mktsegment,
    COUNT(o.orderkey) as order_count,
    SUM(o.totalprice) as total_revenue,
    AVG(o.totalprice) as avg_order_value
FROM customer c
JOIN orders o ON c.custkey = o.custkey
WHERE o.orderdate >= DATE '1995-01-01'
GROUP BY c.custkey, c.name, c.mktsegment
ORDER BY total_revenue DESC
LIMIT 20;

-- Query 10: Product Performance - Products and Lineitem join
SELECT 
    p.p_brand,
    p.p_type,
    COUNT(DISTINCT l.l_orderkey) as order_count,
    SUM(l.l_quantity) as total_quantity,
    SUM(l.l_extendedprice) as total_revenue,
    AVG(l.l_discount) as avg_discount
FROM products p
JOIN lineitem l ON p.p_partkey = l.l_partkey
WHERE l.l_shipdate >= DATE '1995-01-01'
GROUP BY p.p_brand, p.p_type
ORDER BY total_revenue DESC
LIMIT 15; 