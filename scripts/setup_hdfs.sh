#!/bin/bash
# =============================================================
# setup_hdfs.sh - Upload input data to HDFS
# =============================================================
set -e

echo "=== Setting up HDFS directories ==="

# Create directories
hdfs dfs -mkdir -p /user/root/input/reservations
hdfs dfs -mkdir -p /user/root/input/hotels
hdfs dfs -mkdir -p /user/root/output

# Upload reservations (datasource1)
echo "Uploading reservations data..."
if [ -d /opt/data/datasource1 ]; then
    hdfs dfs -put -f /opt/data/datasource1/* /user/root/input/reservations/
elif [ -f /opt/data/reservations.csv ]; then
    hdfs dfs -put -f /opt/data/reservations.csv /user/root/input/reservations/
fi

# Upload hotels (datasource4)
echo "Uploading hotels data..."
if [ -d /opt/data/datasource4 ]; then
    hdfs dfs -put -f /opt/data/datasource4/* /user/root/input/hotels/
elif [ -f /opt/data/hotels.csv ]; then
    hdfs dfs -put -f /opt/data/hotels.csv /user/root/input/hotels/
fi

echo "=== HDFS setup complete ==="
hdfs dfs -ls -R /user/root/input/
