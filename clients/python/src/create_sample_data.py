#!/usr/bin/env python3
"""
Script to create sample data in Trino/Starburst for benchmarking
"""

import sys
import json
import os
from datetime import datetime, timedelta
import random

# Add project paths
sys.path.append('/Users/jauneetsingh/otel-exporter/benchmarks/clients/python/src')
sys.path.append('/Users/jauneetsingh/otel-exporter/benchmarks/config')

from connectors.trino import TrinoConnector

def load_credentials():
    """Load Trino credentials"""
    cred_path = '/Users/jauneetsingh/otel-exporter/benchmarks/config/credentials/credentials.json'
    with open(cred_path, 'r') as f:
        return json.load(f)['trino']

def create_sample_tables(connector):
    """Create sample tables for benchmarking"""
    
    print("=== Creating sample tables ===")
    
    # 1. Orders table - for aggregation and sorting benchmarks
    print("Creating orders table...")
    create_orders = """
    CREATE TABLE IF NOT EXISTS jauneet.sf1.orders (
        order_id BIGINT,
        customer_id BIGINT,
        order_date DATE,
        total_amount DECIMAL(10,2),
        status VARCHAR(20),
        region VARCHAR(50),
        priority VARCHAR(10)
    )
    """
    connector.execute_query(create_orders)
    
    # 2. Customers table - for JOIN benchmarks
    print("Creating customers table...")
    create_customers = """
    CREATE TABLE IF NOT EXISTS jauneet.sf1.customers (
        customer_id BIGINT,
        name VARCHAR(100),
        email VARCHAR(100),
        country VARCHAR(50),
        signup_date DATE,
        segment VARCHAR(20)
    )
    """
    connector.execute_query(create_customers)
    
    # 3. Products table - for complex JOINs
    print("Creating products table...")
    create_products = """
    CREATE TABLE IF NOT EXISTS jauneet.sf1.products (
        product_id BIGINT,
        product_name VARCHAR(100),
        category VARCHAR(50),
        price DECIMAL(10,2),
        supplier_id BIGINT
    )
    """
    connector.execute_query(create_products)
    
    # 4. Order items table - for detailed analysis
    print("Creating order_items table...")
    create_order_items = """
    CREATE TABLE IF NOT EXISTS jauneet.sf1.order_items (
        order_id BIGINT,
        product_id BIGINT,
        quantity INTEGER,
        unit_price DECIMAL(10,2)
    )
    """
    connector.execute_query(create_order_items)
    
    print("✅ All tables created successfully!")

def insert_sample_data(connector):
    """Insert sample data into tables"""
    
    print("\n=== Inserting sample data ===")
    
    # Insert customers (1000 records)
    print("Inserting customers data...")
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan', 'Australia']
    segments = ['Consumer', 'Corporate', 'Home Office']
    
    customers_data = []
    for i in range(1, 1001):
        signup_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))
        customers_data.append(f"({i}, 'Customer_{i}', 'customer{i}@email.com', '{random.choice(countries)}', DATE '{signup_date.strftime('%Y-%m-%d')}', '{random.choice(segments)}')")
    
    # Insert in batches
    batch_size = 100
    for i in range(0, len(customers_data), batch_size):
        batch = customers_data[i:i+batch_size]
        insert_customers = f"""
        INSERT INTO jauneet.sf1.customers (customer_id, name, email, country, signup_date, segment) 
        VALUES {', '.join(batch)}
        """
        connector.execute_query(insert_customers)
    
    # Insert products (500 records)
    print("Inserting products data...")
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty']
    
    products_data = []
    for i in range(1, 501):
        price = round(random.uniform(10.0, 1000.0), 2)
        products_data.append(f"({i}, 'Product_{i}', '{random.choice(categories)}', {price}, {random.randint(1, 50)})")
    
    for i in range(0, len(products_data), batch_size):
        batch = products_data[i:i+batch_size]
        insert_products = f"""
        INSERT INTO jauneet.sf1.products (product_id, product_name, category, price, supplier_id) 
        VALUES {', '.join(batch)}
        """
        connector.execute_query(insert_products)
    
    # Insert orders (5000 records)
    print("Inserting orders data...")
    statuses = ['pending', 'shipped', 'delivered', 'cancelled']
    regions = ['North', 'South', 'East', 'West', 'Central']
    priorities = ['low', 'medium', 'high', 'urgent']
    
    orders_data = []
    for i in range(1, 5001):
        customer_id = random.randint(1, 1000)
        order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        total_amount = round(random.uniform(50.0, 5000.0), 2)
        orders_data.append(f"({i}, {customer_id}, DATE '{order_date.strftime('%Y-%m-%d')}', {total_amount}, '{random.choice(statuses)}', '{random.choice(regions)}', '{random.choice(priorities)}')")
    
    for i in range(0, len(orders_data), batch_size):
        batch = orders_data[i:i+batch_size]
        insert_orders = f"""
        INSERT INTO jauneet.sf1.orders (order_id, customer_id, order_date, total_amount, status, region, priority) 
        VALUES {', '.join(batch)}
        """
        connector.execute_query(insert_orders)
    
    # Insert order items (15000 records - multiple items per order)
    print("Inserting order items data...")
    order_items_data = []
    item_id = 1
    
    for order_id in range(1, 5001):
        # Each order has 1-5 items
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_id = random.randint(1, 500)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(10.0, 500.0), 2)
            order_items_data.append(f"({order_id}, {product_id}, {quantity}, {unit_price})")
            item_id += 1
    
    for i in range(0, len(order_items_data), batch_size):
        batch = order_items_data[i:i+batch_size]
        insert_order_items = f"""
        INSERT INTO jauneet.sf1.order_items (order_id, product_id, quantity, unit_price) 
        VALUES {', '.join(batch)}
        """
        connector.execute_query(insert_order_items)
    
    print("✅ All sample data inserted successfully!")

def verify_data(connector):
    """Verify the data was inserted correctly"""
    
    print("\n=== Verifying data ===")
    
    tables = ['customers', 'products', 'orders', 'order_items']
    
    for table in tables:
        result = connector.execute_query(f"SELECT COUNT(*) FROM jauneet.sf1.{table}")
        count = list(result)[0][0]
        print(f"Table {table}: {count:,} records")
    
    print("\n=== Sample queries ===")
    
    # Test a simple aggregation
    print("Top 5 customers by total order value:")
    result = connector.execute_query("""
        SELECT c.name, SUM(o.total_amount) as total_spent
        FROM jauneet.sf1.customers c
        JOIN jauneet.sf1.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.name
        ORDER BY total_spent DESC
        LIMIT 5
    """)
    
    for row in result:
        print(f"  {row[0]}: ${row[1]:,.2f}")

def main():
    """Main function"""
    try:
        print("🚀 Setting up Trino sample data for benchmarking")
        
        # Load credentials and connect
        credentials = load_credentials()
        connector = TrinoConnector(credentials)
        connector.connect()
        
        print(f"✅ Connected to {credentials['host']}")
        print(f"📁 Using catalog: {credentials['catalog']}, schema: {credentials['schema']}")
        
        # Create tables and insert data
        create_sample_tables(connector)
        insert_sample_data(connector)
        verify_data(connector)
        
        print("\n🎉 Sample data setup complete! Ready for benchmarking.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 