"""
Parse HM Land Registry data to extract year-by-year price trends.
Stores: postcode + property_type + year + avg_price
"""

import csv
import glob
from collections import defaultdict
from datetime import datetime
import json
import re

PROPERTY_TYPES = {
    'D': 'detached',
    'S': 'semi_detached',
    'T': 'terraced',
    'F': 'flats_maisonettes',
    'O': 'other'
}

def parse_yearly_trends():
    """Parse all HML files and extract year-by-year prices by postcode + type."""

    # Structure: {postcode_prefix: {year: {property_type: [prices]}}}
    data_by_postcode_year = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    csv_files = sorted(glob.glob("/Users/srevi/Downloads/pp-*.csv"))

    print(f"[PARSE] Extracting year-by-year trends from {len(csv_files)} files...\n")

    total_sales = 0

    for filepath in csv_files:
        filename = filepath.split('/')[-1]
        # Extract year from filename: pp-2024.csv → 2024
        year_match = re.search(r'pp-(\d{4})', filename)
        if not year_match:
            continue

        year = year_match.group(1)
        file_sales = 0

        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)

                for row in reader:
                    try:
                        if len(row) < 5:
                            continue

                        price = int(row[1].strip())
                        postcode = row[3].strip().upper().replace(' ', '')
                        prop_type_code = row[4].strip()

                        prop_type = PROPERTY_TYPES.get(prop_type_code, 'other')

                        if postcode and price > 0 and len(postcode) >= 4:
                            pc_prefix = postcode[:3]
                            data_by_postcode_year[pc_prefix][year][prop_type].append(price)
                            file_sales += 1
                            total_sales += 1

                            if total_sales % 100000 == 0:
                                print(f"[PARSE] Processed {total_sales:,} sales...")

                    except (ValueError, IndexError):
                        continue

            print(f"[PARSE] ✓ {filename} ({year}): {file_sales:,} sales")

        except Exception as e:
            print(f"[PARSE] ✗ {filename}: {e}")

    print(f"\n[PARSE] Total sales: {total_sales:,}")

    # Calculate yearly averages
    yearly_trends = {}

    for postcode_prefix in sorted(data_by_postcode_year.keys()):
        yearly_trends[postcode_prefix] = {}

        for year in sorted(data_by_postcode_year[postcode_prefix].keys()):
            yearly_trends[postcode_prefix][year] = {}

            for prop_type, prices in data_by_postcode_year[postcode_prefix][year].items():
                if prices:
                    yearly_trends[postcode_prefix][year][prop_type] = {
                        'avg': int(sum(prices) / len(prices)),
                        'median': sorted(prices)[len(prices)//2],
                        'count': len(prices),
                        'min': min(prices),
                        'max': max(prices)
                    }

    return yearly_trends

def generate_supabase_records(yearly_trends):
    """Generate records for Supabase (postcode, year, property_type, price)."""
    records = []

    for postcode, years_data in yearly_trends.items():
        for year, types_data in years_data.items():
            for prop_type, stats in types_data.items():
                records.append({
                    'postcode': postcode,
                    'year': int(year),
                    'property_type': prop_type,
                    'avg_price': stats['avg'],
                    'median_price': stats['median'],
                    'count': stats['count'],
                    'min_price': stats['min'],
                    'max_price': stats['max'],
                    'data_source': 'HM Land Registry',
                    'created_at': datetime.utcnow().isoformat()
                })

    return records

def print_sample(yearly_trends):
    """Show sample trends for a postcode."""
    print("\n[PARSE] === SAMPLE: KT16 Semi-detached Trend ===")

    if 'KT16' in yearly_trends:
        print("\nYear  | Avg Price | Median | Count")
        print("------|-----------|--------|-------")
        for year in sorted(yearly_trends['KT16'].keys()):
            if 'semi_detached' in yearly_trends['KT16'][year]:
                data = yearly_trends['KT16'][year]['semi_detached']
                print(f"{year} | £{data['avg']:>8,} | £{data['median']:>6,} | {data['count']:>5}")

# Run
if __name__ == "__main__":
    print("\n" + "="*60)
    print("=== Extract Year-by-Year Price Trends ===")
    print("="*60 + "\n")

    trends = parse_yearly_trends()
    records = generate_supabase_records(trends)

    print_sample(trends)

    print(f"\n[PARSE] Generated {len(records)} year-level records")
    print("[PARSE] Ready to import into Supabase table 'house_price_yearly'")

    # Save for import
    with open('/tmp/yearly_trends.json', 'w') as f:
        json.dump({'records': records, 'count': len(records)}, f)

    print(f"[PARSE] Saved to /tmp/yearly_trends.json")
