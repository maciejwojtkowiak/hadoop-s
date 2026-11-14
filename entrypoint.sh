#!/bin/bash
set -e

echo "=== Starting SSH ==="
service ssh start

echo "=== Starting HDFS ==="
start-dfs.sh

echo "Waiting for HDFS to be ready..."
for i in $(seq 1 30); do
    if hdfs dfs -ls / >/dev/null 2>&1; then
        echo "HDFS is ready!"
        break
    fi
    echo "Waiting... ($i)"
    sleep 2
done

echo "=== Starting YARN ==="
start-yarn.sh
sleep 5

echo "=== Initializing Hive Metastore ==="
cd /opt/hive
schematool -dbType derby -initSchema 2>/dev/null || echo "Metastore already initialized"

echo ""
echo "========================================"
echo "  Hadoop + Hive container is ready!"
echo "  HDFS UI:  http://localhost:9870"
echo "  YARN UI:  http://localhost:8088"
echo "========================================"
echo ""

# If arguments provided, run them
if [ $# -gt 0 ]; then
    exec "$@"
else
    tail -f /dev/null
fi
