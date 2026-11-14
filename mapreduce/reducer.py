#!/usr/bin/env python3
"""
Reducer for hotel reservations statistics.
Input (sorted by key): hotel_id\tyear\t<value>
  where <value> can be:
    - price,is_cancelled (directly from mapper)
    - count,sum_price,cancelled_count (from combiner)
Output: hotel_id\tyear\treservation_count\tavg_price\tcancelled_count
"""
import sys

current_key = None
count = 0
sum_price = 0.0
cancelled = 0

def emit(key, count, sum_price, cancelled):
    """Emit result for the current group."""
    hotel_id, year = key.split("\t")
    avg_price = round(sum_price / count, 2) if count > 0 else 0.0
    print(f"{hotel_id}\t{year}\t{count}\t{avg_price}\t{cancelled}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 3:
        continue

    key = f"{parts[0]}\t{parts[1]}"
    val_parts = parts[2].split(",")

    try:
        if len(val_parts) == 2:
            # From mapper: price, is_cancelled
            p = float(val_parts[0])
            c = int(val_parts[1])
            k_count = 1
        elif len(val_parts) == 3:
            # From combiner: count, sum_price, cancelled
            k_count = int(val_parts[0])
            p = float(val_parts[1])
            c = int(val_parts[2])
        else:
            continue
    except (ValueError, IndexError):
        continue

    if current_key and current_key != key:
        emit(current_key, count, sum_price, cancelled)
        count = 0
        sum_price = 0.0
        cancelled = 0

    current_key = key
    count += k_count
    sum_price += p
    cancelled += c

if current_key:
    emit(current_key, count, sum_price, cancelled)
