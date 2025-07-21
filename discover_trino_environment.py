#!/usr/bin/env python3
"""
Trino Environment Discovery Script
This script connects to your Trino server and discovers available catalogs, schemas, and tables
to help you create custom benchmarks for your specific environment.
"""

import json
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'clients', 'python', 'src'))

from connectors.trino import TrinoConnector

def load_credentials():
    """Load Trino credentials from config file."""
    creds_file = 'config/credentials/credentials.json'
    try:
        with open(creds_file, 'r') as f:
            credentials = json.load(f)
            return credentials.get('trino', {})
    except FileNotFoundError:
        print(f"❌ Credentials file not found: {creds_file}")
        print("Please create it based on config/credentials/sample_credentials.json")
        return None
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON in credentials file: {creds_file}")
        return None

def discover_environment(connector):
    """Discover the Trino environment."""
    print("🔍 Discovering Trino Environment...")
    print("=" * 50)
    
    try:
        # Discover catalogs
        print("\n📁 Available Catalogs:")
        catalogs = connector.execute_query("SHOW CATALOGS")
        for catalog in catalogs:
            catalog_name = catalog.get('Catalog', catalog.get('catalog_name', 'Unknown'))
            print(f"  - {catalog_name}")
        
        # Get catalog details
        print(f"\n📊 Catalog Details:")
        catalog_info = connector.execute_query("""
            SELECT catalog_name, COUNT(*) as schema_count 
            FROM information_schema.schemata 
            GROUP BY catalog_name 
            ORDER BY schema_count DESC
        """)
        
        for info in catalog_info:
            catalog = info.get('catalog_name', 'Unknown')
            schema_count = info.get('schema_count', 0)
            print(f"  - {catalog}: {schema_count} schemas")
        
        # Find tables in catalogs (excluding system schemas)
        print(f"\n🗃️  Available Tables (top 20):")
        tables = connector.execute_query("""
            SELECT table_catalog, table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'sys')
            ORDER BY table_catalog, table_schema, table_name
            LIMIT 20
        """)
        
        current_catalog = None
        for table in tables:
            catalog = table.get('table_catalog', 'Unknown')
            schema = table.get('table_schema', 'Unknown')  
            table_name = table.get('table_name', 'Unknown')
            
            if catalog != current_catalog:
                print(f"\n  📁 {catalog}:")
                current_catalog = catalog
            print(f"    - {schema}.{table_name}")
        
        return catalog_info, tables
        
    except Exception as e:
        print(f"❌ Error discovering environment: {str(e)}")
        return None, None

def generate_custom_benchmarks(catalog_info, tables):
    """Generate custom benchmark queries based on discovered environment."""
    if not catalog_info or not tables:
        print("⚠️  Cannot generate custom benchmarks without environment data")
        return
    
    print("\n🚀 Generating Custom Benchmark Queries...")
    print("=" * 50)
    
    # Find the best catalog for benchmarking (most tables)
    best_catalog = None
    best_schema = None
    max_tables = 0
    
    schema_table_counts = {}
    for table in tables:
        catalog = table.get('table_catalog', '')
        schema = table.get('table_schema', '')
        key = f"{catalog}.{schema}"
        schema_table_counts[key] = schema_table_counts.get(key, 0) + 1
        
        if schema_table_counts[key] > max_tables:
            max_tables = schema_table_counts[key]
            best_catalog = catalog
            best_schema = schema
    
    if best_catalog and best_schema:
        print(f"\n📋 Recommended for benchmarking: {best_catalog}.{best_schema} ({max_tables} tables)")
        
        # Generate sample benchmark queries
        benchmark_queries = f"""-- Custom Trino Benchmark Queries for {best_catalog}.{best_schema}
-- Generated based on your environment discovery

-- Query 1: Table row counts (tests basic aggregation)
"""
        
        for i, table in enumerate(tables[:5]):  # Top 5 tables
            catalog = table.get('table_catalog', '')
            schema = table.get('table_schema', '')
            table_name = table.get('table_name', '')
            if catalog == best_catalog and schema == best_schema:
                benchmark_queries += f"SELECT '{table_name}' as table_name, COUNT(*) as row_count FROM {catalog}.{schema}.{table_name};\n"
        
        benchmark_queries += f"""
-- Query 2: Schema analysis (tests metadata queries)
SELECT table_name, COUNT(*) as column_count 
FROM information_schema.columns 
WHERE table_catalog = '{best_catalog}' AND table_schema = '{best_schema}'
GROUP BY table_name
ORDER BY column_count DESC;

-- Query 3: Cross-table analysis (customize based on your data relationships)
-- Add JOIN queries here based on your table relationships

-- Query 4: Analytical query template (customize based on your data)
-- SELECT 
--     date_column,
--     COUNT(*) as daily_count,
--     SUM(numeric_column) as daily_sum
-- FROM {best_catalog}.{best_schema}.your_table
-- WHERE date_column >= DATE '2023-01-01'
-- GROUP BY date_column
-- ORDER BY date_column;
"""
        
        # Write to file
        with open('benchmarks/trino_production/custom_benchmark.sql', 'w') as f:
            f.write(benchmark_queries)
        
        print(f"✅ Custom benchmark queries saved to: benchmarks/trino_production/custom_benchmark.sql")
        print(f"💡 Edit this file to add your specific business logic and table relationships")

def main():
    """Main discovery function."""
    print("🔧 Trino Environment Discovery & Benchmark Generator")
    print("=" * 60)
    
    # Load credentials
    creds = load_credentials()
    if not creds:
        return
    
    # Connect to Trino
    try:
        connector = TrinoConnector(creds)
        connector.connect()
        print(f"✅ Connected to Trino at {creds.get('host', 'localhost')}")
    except Exception as e:
        print(f"❌ Failed to connect to Trino: {str(e)}")
        print("Please check your credentials in config/credentials/credentials.json")
        return
    
    # Discover environment
    catalog_info, tables = discover_environment(connector)
    
    # Generate custom benchmarks
    generate_custom_benchmarks(catalog_info, tables)
    
    # Show next steps
    print(f"\n🎯 Next Steps:")
    print(f"1. Review the generated custom_benchmark.sql file")
    print(f"2. Edit benchmarks/trino_production/setup.sql for your data")
    print(f"3. Run production benchmarks:")
    print(f"   cd clients/python/src")
    print(f"   python main.py trino_production --vendors trino --creds ../../../config/credentials/credentials.json")
    print(f"4. For concurrency testing:")
    print(f"   python main.py trino_production --vendors trino --concurrency 10 --concurrency-duration-s 60")
    
    connector.close()

if __name__ == "__main__":
    main() 