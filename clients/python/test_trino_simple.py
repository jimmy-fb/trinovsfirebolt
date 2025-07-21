#!/usr/bin/env python3
"""
Simple Trino connection test that avoids dependency conflicts
"""
import json
import sys
import os
import time

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Only import Trino connector
from connectors.trino import TrinoConnector

def test_trino_connection():
    print("🔌 Testing Trino Connection...")
    print("=" * 40)
    
    # Check if credentials file exists
    creds_file = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'credentials', 'credentials.json')
    
    if not os.path.exists(creds_file):
        print(f"❌ Credentials file not found: {creds_file}")
        print("\n📝 Please create the credentials file:")
        print("1. Copy the sample: cp config/credentials/sample_credentials.json config/credentials/credentials.json")
        print("2. Edit with your Trino details:")
        print("""
{
    "trino": {
        "host": "54.159.239.25",
        "port": 8080,
        "catalog": "your_catalog",
        "schema": "your_schema", 
        "user": "admin",
        "password": "",
        "use_https": false,
        "verify_ssl": false
    }
}
        """)
        return False
    
    try:
        with open(creds_file, 'r') as f:
            creds = json.load(f)['trino']
    except Exception as e:
        print(f"❌ Could not load credentials: {e}")
        return False
    
    print(f"📡 Connecting to: {creds['host']}:{creds['port']}")
    print(f"👤 User: {creds['user']}")
    print(f"📁 Catalog: {creds['catalog']}")
    print(f"📂 Schema: {creds['schema']}")
    
    for attempt in range(3):
        try:
            print(f"\n🔄 Attempt {attempt + 1}/3...")
            
            connector = TrinoConnector(creds)
            connector.connect()
            
            # Test basic connection
            result = connector.execute_query("SELECT 1 as connection_test")
            print(f"✅ Basic connection: {result[0]['connection_test']}")
            
            # Test if we can see the tables
            result = connector.execute_query("SHOW TABLES")
            print(f"✅ Available tables: {len(result)} tables found")
            
            # Test specific table access
            try:
                result = connector.execute_query("SELECT COUNT(*) as row_count FROM customers")
                print(f"✅ Customers table: {result[0]['row_count']} rows")
            except Exception as e:
                print(f"⚠️  Customers table not accessible: {e}")
            
            try:
                result = connector.execute_query("SELECT COUNT(*) as row_count FROM orders")
                print(f"✅ Orders table: {result[0]['row_count']} rows")
            except Exception as e:
                print(f"⚠️  Orders table not accessible: {e}")
            
            connector.close()
            print(f"\n🎉 CONNECTION SUCCESSFUL!")
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Attempt {attempt + 1} failed: {error_msg}")
            
            if "404" in error_msg:
                print("   → Cluster appears to be stopped/paused")
            elif "catalog" in error_msg.lower():
                print("   → Check catalog name in credentials")
            elif "schema" in error_msg.lower():
                print("   → Check schema name in credentials")
            
            if attempt < 2:
                print("   ⏳ Waiting 5 seconds before retry...")
                time.sleep(5)
    
    print(f"\n❌ CONNECTION FAILED")
    return False

if __name__ == "__main__":
    test_trino_connection() 