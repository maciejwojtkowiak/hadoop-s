#!/bin/bash
# =============================================================
# run_hive.sh - Run Hive processing using beeline
# Compliant with criterion H14:
#   Accepts: input_dir3, input_dir4, output_dir6
#   Uses: beeline
# =============================================================
set -e

input_dir3=${1:-/user/root/output/mr_results}
input_dir4=${2:-/user/root/input/hotels}
output_dir6=${3:-/user/root/output/hive_results}

echo "=== Running Hive Query via Beeline ==="
echo "input_dir3:  $input_dir3"
echo "input_dir4:  $input_dir4"
echo "output_dir6: $output_dir6"

# Ensure output directory is clean
hdfs dfs -rm -r -f "$output_dir6" 2>/dev/null || true

# Run Hive HQL via beeline
beeline -u "jdbc:hive2://localhost:10000/default" -n root \
    --hivevar input_dir3="$input_dir3" \
    --hivevar input_dir4="$input_dir4" \
    --hivevar output_dir6="$output_dir6" \
    -f /opt/hive_scripts/hotel_ranking.hql || \
beeline -u "jdbc:hive2://" \
    --hivevar input_dir3="$input_dir3" \
    --hivevar input_dir4="$input_dir4" \
    --hivevar output_dir6="$output_dir6" \
    -f /opt/hive_scripts/hotel_ranking.hql

echo "=== Hive query finished ==="
echo "Output JSON files:"
hdfs dfs -cat "$output_dir6"/* 2>/dev/null || true
