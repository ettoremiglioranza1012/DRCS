#!/bin/bash
set -e

echo "[INIT] Waiting for PostgreSQL to be ready..."

# Loop finché la porta 5432 su 'postgres' non risponde
until pg_isready -h postgres -p 5432 > /dev/null 2>&1; do
  sleep 1
done

echo "[INIT] PostgreSQL is available. Running geo_grid_processor.py..."
python geo_grid_processor.py
