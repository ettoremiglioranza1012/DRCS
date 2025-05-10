
# Utilities
import subprocess
import time

def run_script(script_name):
    while True:
        try:
            print(f"[ORCHESTRATOR] Running {script_name}...")
            subprocess.run(["python", script_name], check=True) 
            # 'check=true' raise exception if scripts being run exits with a non-zero exit code (fail)
            print(f"[ORCHESTRATOR] {script_name} completed successfully.")
            break
        except subprocess.CalledProcessError as e:
            print(f"[ORCHESTRATOR] {script_name} failed: {e}. Retrying in 10s...")
            time.sleep(10)

if __name__ == "__main__":
    run_script("geo_grid_processor.py")
    run_script("kafka_cluster_config.py")


