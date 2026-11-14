import csv
import glob

# 1. MapReduce simulation
mr_results = {}
for f in sorted(glob.glob('data/datasource1/reservations*.csv')):
    with open(f, encoding='utf-8') as fp:
        reader = csv.reader(fp)
        header = next(reader)
        for row in reader:
            if len(row) < 7:
                continue
            hotel_id = row[1].strip()
            year = row[3].strip()[:4]
            price = float(row[5].strip())
            is_canc = 1 if row[6].strip() == 'Cancelled' else 0

            key = (hotel_id, year)
            if key not in mr_results:
                mr_results[key] = {'count': 0, 'sum_price': 0.0, 'cancelled': 0}
            mr_results[key]['count'] += 1
            mr_results[key]['sum_price'] += price
            mr_results[key]['cancelled'] += is_canc

print(f"Total MapReduce groups: {len(mr_results)}")

# 2. Hotels dictionary
hotels = {}
with open('data/datasource4/hotels.csv', encoding='utf-8') as fp:
    reader = csv.reader(fp)
    header = next(reader)
    for row in reader:
        hotel_id = row[0].strip()
        name = row[1].strip()
        rooms = int(row[4].strip())
        hotels[hotel_id] = {'name': name, 'rooms': rooms}

# 3. Hive query (year == '2024')
hive_rows = []
for (h_id, yr), data in mr_results.items():
    if yr == '2024' and h_id in hotels:
        count = data['count']
        avg_price = round(data['sum_price'] / count, 2)
        canc_ratio = round(data['cancelled'] / count, 6)
        rooms = hotels[h_id]['rooms']
        ratio = count / rooms
        hive_rows.append({
            'hotel_name': hotels[h_id]['name'],
            'total_reservations': count,
            'avg_price': avg_price,
            'cancelled_ratio': canc_ratio,
            'rooms': rooms,
            'ratio': ratio
        })

hive_rows.sort(key=lambda x: x['ratio'], reverse=True)

# Assign RANK() with ties
current_rank = 1
for i, row in enumerate(hive_rows):
    if i > 0 and row['ratio'] < hive_rows[i-1]['ratio']:
        current_rank = i + 1
    row['rank'] = current_rank

print(f"Total Hive rows: {len(hive_rows)}\n")
print(f"{'rank':<5} {'hotel_name':<38} {'total_res':<10} {'avg_price':<10} {'cancelled_ratio':<16} {'rooms':<6} {'ratio':<8}")
print("-" * 96)
for r in hive_rows:
    print(f"{r['rank']:<5} {r['hotel_name']:<38} {r['total_reservations']:<10} {r['avg_price']:<10.2f} {r['cancelled_ratio']:<16.6f} {r['rooms']:<6} {r['ratio']:<8.4f}")
