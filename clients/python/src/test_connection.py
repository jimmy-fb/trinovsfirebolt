#!/usr/bin/env python3
import json
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connectors.trino import TrinoConnector

def test_connection():
    print("🔌 Testing Trino Connection...")
    print("=" * 40)
    
    try:
        with open('../../../config/credentials/credentials.json', 'r') as f:
            creds = json.load(f)['trino']
    except Exception as e:
        print(f"❌ Could not load credentials: {e}")
        return False
    
    print(f"📡 Connecting to: {creds['host']}")
    print(f"📁 Catalog: {creds['catalog']}")
    print(f"📂 Schema: {creds['schema']}")
    
    for attempt in range(3):
        try:
            print(f"\n🔄 Attempt {attempt + 1}/3...")
            
            connector = TrinoConnector(creds)
            connector.connect()
            
            result = connector.execute_query("SELECT 1 as connection_test")
            print(f"✅ Basic connection: {result[0]['connection_test']}")
            
            result = connector.execute_query("SELECT COUNT(*) as row_count FROM orders")
            print(f"✅ Orders table: {result[0]['row_count']} rows")
            
            connector.close()
            print(f"\n🎉 CONNECTION SUCCESSFUL!")
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Attempt {attempt + 1} failed: {error_msg}")
            
            if "404" in error_msg:
                print("   → Cluster appears to be stopped/paused")
            
            if attempt < 2:
                print("   ⏳ Waiting 5 seconds before retry...")
                time.sleep(5)
    
    print(f"\n❌ CONNECTION FAILED - Cluster likely stopped")
    return False

if __name__ == "__main__":
    test_connection()
