-- Trino Benchmark Queries using actual sample data

-- Query 1: Simple aggregation - Count customers by country
SELECT country, COUNT(*) as customer_count
FROM jauneet.sf1.customers
GROUP BY country
ORDER BY customer_count DESC;

-- Query 2: Date-based filtering and aggregation - Orders by month
SELECT 
    DATE_TRUNC('month', order_date) as order_month,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM jauneet.sf1.orders
WHERE order_date >= DATE '2023-01-01'
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY order_month;

-- Query 3: Complex JOIN - Customer order summary
SELECT 
    c.customer_id,
    c.name,
    c.country,
    c.segment,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date
FROM jauneet.sf1.customers c
LEFT JOIN jauneet.sf1.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.country, c.segment
HAVING COUNT(o.order_id) > 0
ORDER BY total_spent DESC
LIMIT 100;

-- Query 4: Window functions - Running totals
SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount,
    SUM(total_amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as order_sequence
FROM jauneet.sf1.orders
ORDER BY customer_id, order_date
LIMIT 1000;

-- Query 5: Complex filtering and sorting - High-value orders analysis
SELECT 
    o.order_id,
    o.order_date,
    o.total_amount,
    o.status,
    o.region,
    o.priority,
    c.name as customer_name,
    c.country,
    c.segment,
    CASE 
        WHEN o.total_amount > 4000 THEN 'Premium'
        WHEN o.total_amount > 2000 THEN 'High Value'
        WHEN o.total_amount > 1000 THEN 'Standard'
        ELSE 'Low Value'
    END as order_category
FROM jauneet.sf1.orders o
JOIN jauneet.sf1.customers c ON o.customer_id = c.customer_id
WHERE o.total_amount > 1000
  AND o.status IN ('shipped', 'delivered')
  AND o.order_date >= DATE '2023-06-01'
ORDER BY o.total_amount DESC, o.order_date DESC
LIMIT 500; 