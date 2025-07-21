-- Firebolt Custom Schema Benchmark Queries
-- Optimized for Firebolt SQL dialect and performance characteristics

-- Query 1: Customer Analysis - Basic aggregation on customers table
SELECT 
    COUNT(*) as total_customers,
    COUNT(DISTINCT customer_id) as unique_customers,
    AVG(credit_score) as avg_credit_score,
    AVG(lifetime_value) as avg_lifetime_value,
    COUNT(CASE WHEN is_premium = true THEN 1 END) as premium_customers,
    COUNT(CASE WHEN is_premium = false THEN 1 END) as standard_customers
FROM customers;

-- Query 2: Order Summary - Aggregation on orders table
SELECT 
    order_status,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MIN(order_date) as earliest_order,
    MAX(order_date) as latest_order
FROM orders 
WHERE order_date >= TIMESTAMP '2023-01-01 00:00:00'
GROUP BY order_status
ORDER BY total_revenue DESC;

-- Query 3: Lineitem Analysis - Complex aggregation on lineitem table
SELECT 
    line_status,
    COUNT(*) as line_count,
    SUM(quantity) as total_quantity,
    SUM(extended_price) as total_extended_price,
    AVG(discount) as avg_discount,
    AVG(tax) as avg_tax
FROM lineitem
WHERE ship_date >= TIMESTAMP '2023-01-01 00:00:00'
GROUP BY line_status
ORDER BY total_extended_price DESC;

-- Query 4: Customer-Order Join - Testing join performance
SELECT 
    c.region,
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= TIMESTAMP '2023-01-01 00:00:00'
GROUP BY c.region
ORDER BY total_revenue DESC;

-- Query 5: Products Analysis - Testing products table
SELECT 
    COUNT(*) as total_products,
    COUNT(DISTINCT category) as unique_categories,
    COUNT(DISTINCT brand) as unique_brands,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(CASE WHEN is_active = true THEN 1 END) as active_products
FROM products;

-- Query 6: Events Analysis - Testing events table
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT event_type) as unique_event_types,
    COUNT(DISTINCT device_type) as unique_device_types,
    MIN(event_timestamp) as earliest_event,
    MAX(event_timestamp) as latest_event
FROM events
WHERE event_timestamp >= TIMESTAMP '2023-01-01 00:00:00';

-- Query 7: Complex Multi-table Join - Customer, Orders, Lineitem
SELECT 
    c.region,
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(l.lineitem_id) as lineitem_count,
    SUM(l.extended_price) as total_revenue,
    AVG(l.quantity) as avg_quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN lineitem l ON o.order_id = l.order_id
WHERE o.order_date >= TIMESTAMP '2023-01-01 00:00:00'
  AND l.ship_date >= TIMESTAMP '2023-01-01 00:00:00'
GROUP BY c.region
ORDER BY total_revenue DESC;

-- Query 8: Revenue by Date Range - Time-based analysis (Firebolt optimized)
SELECT 
    EXTRACT(YEAR FROM o.order_date) as order_year,
    EXTRACT(MONTH FROM o.order_date) as order_month,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(o.total_amount) as monthly_revenue,
    AVG(o.total_amount) as avg_order_value
FROM orders o
WHERE o.order_date >= TIMESTAMP '2023-01-01 00:00:00' 
  AND o.order_date < TIMESTAMP '2024-01-01 00:00:00'
GROUP BY EXTRACT(YEAR FROM o.order_date), EXTRACT(MONTH FROM o.order_date)
ORDER BY order_year, order_month;

-- Query 9: Top Customers by Revenue - Ranking query
SELECT 
    c.customer_id,
    c.customer_name,
    c.region,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= TIMESTAMP '2023-01-01 00:00:00'
GROUP BY c.customer_id, c.customer_name, c.region
ORDER BY total_revenue DESC
LIMIT 20;

-- Query 10: Product Performance - Products and Lineitem join
SELECT 
    p.brand,
    p.category,
    COUNT(DISTINCT l.order_id) as order_count,
    SUM(l.quantity) as total_quantity,
    SUM(l.extended_price) as total_revenue,
    AVG(l.discount) as avg_discount
FROM products p
JOIN lineitem l ON p.product_id = l.product_id
WHERE l.ship_date >= TIMESTAMP '2023-01-01 00:00:00'
GROUP BY p.brand, p.category
ORDER BY total_revenue DESC
LIMIT 15; 