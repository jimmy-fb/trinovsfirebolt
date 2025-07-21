-- TPC-H Production Benchmark Queries (Fixed for Starburst column names)
-- These are industry-standard queries for database performance testing

-- Query 1: Simple Aggregation - Order Summary (tests basic aggregation)
SELECT 
    orderstatus,
    COUNT(*) as order_count,
    SUM(totalprice) as total_revenue,
    AVG(totalprice) as avg_order_value
FROM orders 
WHERE orderdate >= DATE '1995-01-01'
GROUP BY orderstatus
ORDER BY total_revenue DESC;

-- Query 2: Complex JOIN - Customer Analysis (tests distributed joins)
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

-- Query 3: Multi-table JOIN with Aggregation (tests complex query processing)
SELECT 
    n.name as nation,
    COUNT(DISTINCT s.suppkey) as supplier_count,
    COUNT(DISTINCT ps.partkey) as part_count,
    SUM(ps.availqty) as total_available_quantity,
    AVG(ps.supplycost) as avg_supply_cost
FROM nation n
JOIN supplier s ON n.nationkey = s.nationkey
JOIN partsupp ps ON s.suppkey = ps.suppkey
GROUP BY n.name
ORDER BY total_available_quantity DESC
LIMIT 10;

-- Query 4: Window Functions - Revenue Trends (tests analytical processing)
SELECT 
    order_year,
    order_month,
    monthly_revenue,
    LAG(monthly_revenue) OVER (ORDER BY order_year, order_month) as prev_month_revenue,
    monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY order_year, order_month) as revenue_change,
    SUM(monthly_revenue) OVER (ORDER BY order_year, order_month ROWS UNBOUNDED PRECEDING) as cumulative_revenue
FROM (
    SELECT 
        YEAR(orderdate) as order_year,
        MONTH(orderdate) as order_month,
        SUM(totalprice) as monthly_revenue
    FROM orders
    WHERE orderdate >= DATE '1995-01-01' AND orderdate < DATE '1997-01-01'
    GROUP BY YEAR(orderdate), MONTH(orderdate)
) monthly_summary
ORDER BY order_year, order_month;

-- Query 5: Top 10 Customers by Revenue (tests sorting and limiting)
SELECT 
    c.name,
    c.mktsegment,
    n.name as nation,
    COUNT(o.orderkey) as order_count,
    SUM(o.totalprice) as total_revenue,
    MAX(o.orderdate) as last_order_date
FROM customer c
JOIN orders o ON c.custkey = o.custkey
JOIN nation n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1995-01-01'
GROUP BY c.custkey, c.name, c.mktsegment, n.name
ORDER BY total_revenue DESC
LIMIT 10; 