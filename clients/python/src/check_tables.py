#!/usr/bin/env python3
"""
Check current state of tables in Trino
"""

import sys
import json

# Add project paths
sys.path.append('/Users/jauneetsingh/otel-exporter/benchmarks/clients/python/src')
sys.path.append('/Users/jauneetsingh/otel-exporter/benchmarks/config')

from connectors.trino import TrinoConnector

def main():
    # Load credentials
    with open('/Users/jauneetsingh/otel-exporter/benchmarks/config/credentials/credentials.json', 'r') as f:
        credentials = json.load(f)['trino']

    connector = TrinoConnector(credentials)
    connector.connect()

    print('=== Current tables in jauneet.sf1 ===')
    result = connector.execute_query('SHOW TABLES FROM jauneet.sf1')
    tables = []
    for row in result:
        # Handle different result formats
        if isinstance(row, dict) and 'Table' in row:
            table_name = row['Table']
        elif isinstance(row, (list, tuple)):
            table_name = row[0]
        else:
            table_name = str(row)
        tables.append(table_name)
        print(f'✅ Table: {table_name}')

    print('\n=== Data counts ===')
    for table in tables:
        try:
            result = connector.execute_query(f'SELECT COUNT(*) FROM jauneet.sf1.{table}')
            result_list = list(result)
            if result_list:
                count_row = result_list[0]
                if isinstance(count_row, dict):
                    count = list(count_row.values())[0]
                elif isinstance(count_row, (list, tuple)):
                    count = count_row[0]
                else:
                    count = count_row
                print(f'{table}: {count:,} records')
            else:
                print(f'{table}: No data returned')
        except Exception as e:
            print(f'{table}: Error - {e}')

    if 'customers' in tables:
        print('\n=== Sample data from customers table ===')
        try:
            result = connector.execute_query('SELECT customer_id, name, country FROM jauneet.sf1.customers LIMIT 5')
            for row in result:
                if isinstance(row, dict):
                    print(f'Customer ID: {row.get("customer_id", "N/A")}, Name: {row.get("name", "N/A")}, Country: {row.get("country", "N/A")}')
                elif isinstance(row, (list, tuple)) and len(row) >= 3:
                    print(f'Customer ID: {row[0]}, Name: {row[1]}, Country: {row[2]}')
                else:
                    print(f'Row: {row}')
        except Exception as e:
            print(f'Error accessing customers: {e}')

    if 'customers' in tables and 'orders' in tables:
        print('\n=== Testing a JOIN query ===')
        try:
            result = connector.execute_query('''
                SELECT c.name, COUNT(o.order_id) as order_count, SUM(o.total_amount) as total_spent
                FROM jauneet.sf1.customers c
                LEFT JOIN jauneet.sf1.orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.name
                ORDER BY total_spent DESC NULLS LAST
                LIMIT 5
            ''')
            print('Top customers by spending:')
            for row in result:
                if isinstance(row, dict):
                    name = row.get('name', 'N/A')
                    order_count = row.get('order_count', 0)
                    spent = row.get('total_spent', 0) or 0
                    print(f'  {name}: {order_count} orders, ${spent:.2f}')
                elif isinstance(row, (list, tuple)) and len(row) >= 3:
                    spent = row[2] if row[2] is not None else 0
                    print(f'  {row[0]}: {row[1]} orders, ${spent:.2f}')
                else:
                    print(f'  Row: {row}')
        except Exception as e:
            print(f'JOIN query error: {e}')

if __name__ == "__main__":
    main() 