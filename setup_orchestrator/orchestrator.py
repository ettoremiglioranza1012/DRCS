
"""
Orchestrator Script for Geospatial Initialization

This utility script sequentially executes two critical setup scripts required 
for the wildfire detection pipeline:

    1. geo_grid_processor.py  - Generates geospatial grid definitions and region IDs.
    2. preload_redis.py       - Loads geospatial metadata (e.g., microarea bounding boxes) 
                                into Redis for real-time enrichment.

The script automatically retries a script if it fails, with a 10-second delay between attempts.
This ensures robustness during initial system startup when services like Redis may not yet be ready.

Usage:
    python orchestrator.py

This script is typically used during container or pipeline bootstrapping to ensure that
Redis and spatial structures are correctly initialized before processing begins.
"""

# Utilities
import subprocess
import time

def run_script(script_name):
    while True:
        try:
            print(f"[ORCHESTRATOR] Running {script_name}...")
            subprocess.run(["python", script_name], check=True)
            print(f"[ORCHESTRATOR] {script_name} completed successfully.")
            break
        except subprocess.CalledProcessError as e:
            print(f"[ORCHESTRATOR] {script_name} failed: {e}. Retrying in 10s...")
            time.sleep(10)

if __name__ == "__main__":
    run_script("geo_grid_processor.py")
    run_script("preload_redis.py") 
