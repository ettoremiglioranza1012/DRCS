#!/bin/bash

echo "===================================================="
echo " EARLY WARNING SYSTEM: INITIALIZING"
echo "===================================================="
echo ""

# Step 1: Run the early warning system containers in the background
echo "[INFO] Launching early warning containers..."
docker-compose -f docker-compose.early_warning.yml up --build -d

echo "[INFO] Waiting for 'early_warning_model' container to complete analysis..."
docker wait early_warning_model
echo ""

# Step 2: Retrieve exit code from early_warning_model container
EXIT_CODE=$(docker inspect early_warning_model --format='{{.State.ExitCode}}')
echo "===================================================="
echo " EARLY WARNING MODEL COMPLETED"
echo "===================================================="
echo ""
echo "[INFO] Container 'early_warning_model' exited with code: $EXIT_CODE"
echo ""

# Step 3: Clean up early warning containers, leave database running
echo "[INFO] Stopping and removing early warning containers..."
docker-compose -f docker-compose.early_warning.yml stop meteo_data_loader early_warning_model
docker-compose -f docker-compose.early_warning.yml rm -f meteo_data_loader early_warning_model
echo ""

# Step 4: Decide what to do based on the exit code
if [ "$EXIT_CODE" -eq 1 ] || [ "$EXIT_CODE" -eq 2 ]; then
    echo "===================================================="
    echo " FIRE RISK DETECTED"
    echo " Exit Code: $EXIT_CODE"
    echo " Starting Main Disaster Response System..."
    echo "===================================================="
    echo ""

    docker-compose up --build

else
    echo "===================================================="
    echo " NO FIRE RISK DETECTED"
    echo " Exit Code: $EXIT_CODE"
    echo " System remains in standby mode."
    echo "===================================================="
    echo ""
fi

