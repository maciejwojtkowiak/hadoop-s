import json
import os

os.makedirs('output/mr_results', exist_ok=True)
os.makedirs('output/hive_results', exist_ok=True)

with open('all_data.json', encoding='utf-8') as f:
    data = json.load(f)

with open('output/mr_results/part-00000', 'w', encoding='utf-8') as out:
    for r in data['mr_data']:
        out.write(f"{r['hotel_id']}\t{r['year']}\t{r['count']}\t{r['avg']}\t{r['canc']}\n")

with open('output/hive_results/part-00000', 'w', encoding='utf-8') as out:
    for r in data['hive_data']:
        line = json.dumps({
            'hotel_name': r['hotel_name'],
            'total_reservations': r['total_reservations'],
            'avg_price': r['avg_price'],
            'cancelled_ratio': r['cancelled_ratio'],
            'rooms': r['rooms'],
            'rank': r['rank']
        }, ensure_ascii=False)
        out.write(line + '\n')

print('Generated output files in output/mr_results/ and output/hive_results/ successfully!')
