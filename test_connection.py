#!/usr/bin/env python3
"""
Trino Connection Test Script
Tests connection to Starburst cluster and runs basic queries
"""

import json
import sys
import os
import time

# Add the src directory to Python path
sys.path.insert(0, '.')

from connectors.trino import TrinoConnector

def test_connection():
    """Test Trino connection with retry logic."""
    print("🔌 Testing Trino Connection...")
    print("=" * 40)
    
    # Load credentials
    try:
        with open('../../../config/credentials/credentials.json', 'r') as f:
            creds = json.load(f)['trino']
    except Exception as e:
        print(f"❌ Could not load credentials: {e}")
        return False
    
    print(f"📡 Connecting to: {creds['host']}")
    print(f"📁 Catalog: {creds['catalog']}")
    print(f"📂 Schema: {creds['schema']}")
    
    # Test connection with retries
    for attempt in range(3):
        try:
            print(f"\n🔄 Attempt {attempt + 1}/3...")
            
            connector = TrinoConnector(creds)
            connector.connect()
            
            # Test basic connectivity
            result = connector.execute_query("SELECT 1 as connection_test")
            print(f"✅ Basic connection: {result[0]['connection_test']}")
            
            # Test catalog access
            result = connector.execute_query("SHOW SCHEMAS")
            print(f"✅ Available schemas: {len(result)} found")
            
            # Test table access
            result = connector.execute_query("SHOW TABLES")
            print(f"✅ Available tables: {len(result)} found")
            
            # Test actual data query
            result = connector.execute_query("SELECT COUNT(*) as row_count FROM orders")
            print(f"✅ Orders table: {result[0]['row_count']} rows")
            
            connector.close()
            print(f"\n🎉 CONNECTION SUCCESSFUL!")
            print(f"✅ Ready to run TPC-H benchmarks")
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Attempt {attempt + 1} failed: {error_msg}")
            
            if "404" in error_msg or "Destination not found" in error_msg:
                print("   → Cluster appears to be stopped/paused")
            elif "CATALOG_NOT_FOUND" in error_msg:
                print("   → Catalog issue - check credentials")
            elif "SCHEMA_NOT_FOUND" in error_msg:
                print("   → Schema issue - check credentials")
            else:
                print(f"   → Connection error: {error_msg}")
            
            if attempt < 2:
                print("   ⏳ Waiting 5 seconds before retry...")
                time.sleep(5)
    
    print(f"\n❌ CONNECTION FAILED after 3 attempts")
    print(f"💡 Check Starburst console: https://galaxy.starburst.io/")
    return False

if __name__ == "__main__":
    success = test_connection()
    if success:
        print(f"\n🚀 Next steps:")
        print(f"   python main.py tpch_production --vendors trino --creds ../../../config/credentials/credentials.json")
    else:
        print(f"\n🔧 Troubleshooting:")
        print(f"   1. Check cluster status at https://galaxy.starburst.io/")
        print(f"   2. Start your cluster if it's stopped")
        print(f"   3. Wait 2-3 minutes for cluster to be ready")
        print(f"   4. Re-run: python test_connection.py") 