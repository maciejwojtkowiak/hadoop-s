#!/bin/bash
# =============================================================
# run_mr.sh - Run MapReduce job (Streaming or Classic)
# Compliant with criteria M11 and M12:
#   Accepts: input_dir1, output_dir3, [variant]
#   variant: streaming (default) | classic
# =============================================================
set -e

input_dir1=${1:-/user/root/input/reservations}
output_dir3=${2:-/user/root/output/mr_results}
variant=${3:-streaming}

echo "=== Running MapReduce ($variant) ==="
echo "input_dir1:  $input_dir1"
echo "output_dir3: $output_dir3"

# M12: Clean output directory before running (safe for repeat runs)
hdfs dfs -rm -r -f "$output_dir3" 2>/dev/null || true

if [ "$variant" = "classic" ]; then
    echo "Running MapReduce Classic (Java JAR)..."
    hadoop jar /opt/mapreduce/target/hotel-reservations-1.0.jar \
        hotels.ReservationDriver \
        "$input_dir1" \
        "$output_dir3"
elif [ "$variant" = "streaming" ]; then
    echo "Running Hadoop Streaming with Combiner..."
    mapred streaming \
        -files /opt/mapreduce/mapper.py,/opt/mapreduce/combiner.py,/opt/mapreduce/reducer.py \
        -input "$input_dir1" \
        -output "$output_dir3" \
        -mapper "python3 mapper.py" \
        -combiner "python3 combiner.py" \
        -reducer "python3 reducer.py"
else
    echo "Unknown variant: $variant. Use 'streaming' or 'classic'."
    exit 1
fi

echo "=== MapReduce ($variant) finished successfully ==="
echo "Output sample:"
hdfs dfs -cat "$output_dir3"/part-* | head -10
