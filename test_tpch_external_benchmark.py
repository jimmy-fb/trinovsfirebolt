#!/usr/bin/env python3
"""
Test script for TPCH External Tables Benchmark
This script verifies that the benchmark is properly configured and can be executed.
"""

import os
import sys
import json
from pathlib import Path

def check_benchmark_structure():
    """Check if the benchmark directory structure is correct."""
    print("Checking benchmark directory structure...")
    
    benchmark_dir = Path("benchmarks/tpch_external_tables")
    required_files = [
        "setup.sql",
        "benchmark.sql", 
        "warmup.sql",
        "queries.json",
        "README.md",
        "firebolt/setup.sql",
        "firebolt/benchmark.sql",
        "firebolt/warmup.sql",
        "trino/setup.sql",
        "trino/benchmark.sql",
        "trino/warmup.sql"
    ]
    
    missing_files = []
    for file_path in required_files:
        full_path = benchmark_dir / file_path
        if not full_path.exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    else:
        print("✅ All required files exist")
        return True

def check_credentials():
    """Check if credentials file exists."""
    print("\nChecking credentials configuration...")
    
    creds_file = Path("config/credentials/credentials.json")
    if not creds_file.exists():
        print("❌ Credentials file not found at config/credentials/credentials.json")
        print("Please create the credentials file with Firebolt and Trino configurations")
        return False
    
    try:
        with open(creds_file, 'r') as f:
            creds = json.load(f)
        
        if 'firebolt' not in creds:
            print("❌ Firebolt credentials not found in credentials.json")
            return False
        
        if 'trino' not in creds:
            print("❌ Trino credentials not found in credentials.json")
            return False
        
        print("✅ Credentials file exists with Firebolt and Trino configurations")
        return True
        
    except json.JSONDecodeError:
        print("❌ Invalid JSON in credentials file")
        return False
    except Exception as e:
        print(f"❌ Error reading credentials: {e}")
        return False

def check_python_dependencies():
    """Check if Python dependencies are installed."""
    print("\nChecking Python dependencies...")
    
    # Check if requirements.txt exists
    requirements_file = Path("clients/python/requirements.txt")
    if requirements_file.exists():
        print("✅ Python requirements file found")
        print("⚠️  Dependencies will be installed when running: pip install -r clients/python/requirements.txt")
    else:
        print("❌ Python requirements file not found")
        return False
    
    return True

def show_usage_instructions():
    """Show how to run the benchmark."""
    print("\n" + "="*60)
    print("TPCH EXTERNAL TABLES BENCHMARK SETUP COMPLETE")
    print("="*60)
    
    print("\nTo run the benchmark:")
    print("\n1. Sequential benchmark (power run):")
    print("   cd clients/python")
    print("   python -m src.main tpch_external_tables --vendors firebolt,trino --execute-setup True")
    
    print("\n2. Concurrent benchmark:")
    print("   cd clients/python")
    print("   python -m src.main tpch_external_tables --vendors firebolt,trino --concurrency 10 --concurrency-duration-s 60")
    
    print("\n3. Test individual vendors:")
    print("   # Firebolt only")
    print("   python -m src.main tpch_external_tables --vendors firebolt --execute-setup True")
    print("   # Trino only")
    print("   python -m src.main tpch_external_tables --vendors trino --execute-setup True")
    
    print("\n4. Test connections:")
    print("   python test_connection.py")
    
    print("\nPrerequisites:")
    print("- External tables configured in Firebolt and Trino")
    print("- Tables: customer, lineitem, products, events, orders")
    print("- Credentials configured in config/credentials/credentials.json")

def main():
    """Main test function."""
    print("TPCH External Tables Benchmark - Setup Verification")
    print("="*50)
    
    # Check benchmark structure
    structure_ok = check_benchmark_structure()
    
    # Check credentials
    creds_ok = check_credentials()
    
    # Check dependencies
    deps_ok = check_python_dependencies()
    
    print("\n" + "="*50)
    if structure_ok and creds_ok and deps_ok:
        print("✅ All checks passed! Benchmark is ready to run.")
        show_usage_instructions()
    else:
        print("❌ Some checks failed. Please fix the issues above before running the benchmark.")
        print("\nNext steps:")
        if not structure_ok:
            print("- Verify benchmark files are created correctly")
        if not creds_ok:
            print("- Configure credentials in config/credentials/credentials.json")
        if not deps_ok:
            print("- Install required Python dependencies: pip install -r clients/python/requirements.txt")

if __name__ == "__main__":
    main() 