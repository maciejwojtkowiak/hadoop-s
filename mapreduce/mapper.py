#!/usr/bin/env python3
"""
Mapper for hotel reservations statistics.
Input: CSV lines from reservations (datasource1)
Output: hotel_id\tyear\tprice,is_cancelled (1 or 0)

Key = hotel_id\tyear
Value = price,is_cancelled
"""
import sys

first_line = True
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Skip header
    if first_line:
        first_line = False
        if line.startswith("reservation_id"):
            continue

    parts = line.split(",")
    if len(parts) < 7:
        continue

    try:
        hotel_id = parts[1].strip()
        check_in = parts[3].strip()
        price = parts[5].strip()
        status = parts[6].strip()

        # Extract year from check_in (format: yyyy-MM-dd'T'HH:mm)
        year = check_in[:4]

        # Validate year is numeric
        int(year)

        # Validate price is numeric
        price_val = float(price)

        # is_cancelled: 1 if Cancelled, 0 otherwise
        is_cancelled = 1 if status == "Cancelled" else 0

        # Emit: key=hotel_id\tyear, value=price,is_cancelled
        print(f"{hotel_id}\t{year}\t{price},{is_cancelled}")
    except (ValueError, IndexError):
        # Skip malformed records
        continue
