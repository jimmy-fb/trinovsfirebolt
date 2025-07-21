import subprocess
import os
import json
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

VENDOR = "trino"  # Change to "trino" to test Trino
BENCHMARK = "custom_schema"
DURATION = 60  # seconds for each query
OUTPUT_DIR = "benchmark_results/mixed_concurrency"
BENCHMARK_DIR = f"../../benchmarks/{BENCHMARK}"
QUERIES_PATH = os.path.join(BENCHMARK_DIR, "queries.json")
BACKUP_PATH = os.path.join(BENCHMARK_DIR, "queries.json.bak")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load all queries
with open(QUERIES_PATH, "r") as f:
    all_queries = json.load(f)

def get_query_by_index(idx):
    if idx <= len(all_queries):
        return [all_queries[idx-1]]
    else:
        raise IndexError(f"Query index {idx} out of range (only {len(all_queries)} queries available)")

def run_query(query_id):
    # Write only the selected query to queries.json
    with open(QUERIES_PATH, "w") as f:
        json.dump(get_query_by_index(query_id), f)
    # Run the benchmark for this query only
    cmd = [
        "python", "-m", "src.main", BENCHMARK,
        "--vendors", VENDOR,
        "--concurrency", "1",
        "--concurrency-duration-s", str(DURATION),
        "--output-dir", OUTPUT_DIR
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    # Save stdout/stderr for debugging
    with open(f"{OUTPUT_DIR}/query_{query_id}_output.txt", "w") as f:
        f.write(result.stdout)
        f.write("\n--- STDERR ---\n")
        f.write(result.stderr)
    return query_id, result.returncode

if __name__ == "__main__":
    # Backup the original queries.json
    shutil.copyfile(QUERIES_PATH, BACKUP_PATH)
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(run_query, i) for i in range(1, 11)]
            for future in as_completed(futures):
                query_id, code = future.result()
                print(f"Query {query_id} finished with exit code {code}")
    finally:
        # Restore the original queries.json
        shutil.move(BACKUP_PATH, QUERIES_PATH)
    print(f"All queries complete. Check {OUTPUT_DIR} for results.") 
