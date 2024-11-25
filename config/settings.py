from pathlib import Path

# Base paths
ROOT_DIR = Path(__file__).parent.parent
CREDENTIALS_DIR = ROOT_DIR / "config" / "credentials"
DEFAULT_CREDS_FILE = CREDENTIALS_DIR / "credentials.json"

# Default settings
DEFAULT_POOL_SIZE = 5
DEFAULT_CONCURRENCY = 1
DEFAULT_OUTPUT_DIR = ROOT_DIR / "benchmark_results"

# Vendor-specific settings
VENDOR_SETTINGS = {
    'firebolt': {
        'max_retries': 3,
        'timeout': 300,
    },
    'snowflake': {
        'max_retries': 3,
        'timeout': 300,
    },
    # ... other vendor settings ...
}
