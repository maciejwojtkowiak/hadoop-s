#!/bin/bash
# =============================================================
# run_all.sh - Complete End-to-End Pipeline
# Runs: HDFS setup -> MapReduce -> Hive -> Result verification
# Usage: ./run_all.sh [streaming|classic]
# =============================================================
set -e

VARIANT=${1:-streaming}

echo "=========================================================="
echo "  Hotel Reservations Processing Pipeline"
echo "  Variant: $VARIANT"
echo "=========================================================="

# 1. Setup HDFS directories and upload datasets
echo ""
echo ">>> STEP 1: Setting up HDFS data..."
bash /opt/scripts/setup_hdfs.sh

# 2. Run MapReduce (3)
echo ""
echo ">>> STEP 2: Running MapReduce ($VARIANT)..."
bash /opt/scripts/run_mr.sh \
    /user/root/input/reservations \
    /user/root/output/mr_results \
    "$VARIANT"

# 3. Run Hive (5) -> (6)
echo ""
echo ">>> STEP 3: Running Hive analysis..."
bash /opt/scripts/run_hive.sh \
    /user/root/output/mr_results \
    /user/root/input/hotels \
    /user/root/output/hive_results

echo ""
echo "=========================================================="
echo "  Pipeline finished successfully!"
echo "=========================================================="
echo ""
echo "=== Final Output (6) in JSON ==="
hdfs dfs -cat /user/root/output/hive_results/* 2>/dev/null || true
