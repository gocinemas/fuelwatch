"""
Import ALL HM Land Registry yearly data files into a unified price database.
Combines 2018-2026 data for comprehensive UK house price coverage.
"""

import csv
import glob
from collections import defaultdict
from datetime import datetime
import json

PROPERTY_TYPES = {
    'D': 'detached',
    'S': 'semi_detached',
    'T': 'terraced',
    'F': 'flats_maisonettes',
    'O': 'other'
}

def parse_all_files():
    """Parse all HML CSV files and aggregate by postcode + property type."""

    data_by_postcode = defaultdict(lambda: defaultdict(list))
    total_sales = 0
    total_files = 0
    files_processed = []

    # Find all pp-*.csv files
    csv_files = sorted(glob.glob("/Users/srevi/Downloads/pp-*.csv"))

    print(f"[IMPORT] Found {len(csv_files)} files to process:")
    for f in csv_files:
        print(f"  - {f.split('/')[-1]}")

    for filepath in csv_files:
        filename = filepath.split('/')[-1]
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
                            data_by_postcode[pc_prefix][prop_type].append(price)
                            file_sales += 1
                            total_sales += 1

                            if total_sales % 100000 == 0:
                                print(f"[IMPORT] Processed {total_sales:,} sales...")

                    except (ValueError, IndexError):
                        continue

            files_processed.append({'file': filename, 'sales': file_sales})
            total_files += 1
            print(f"[IMPORT] ✓ {filename}: {file_sales:,} sales")

        except Exception as e:
            print(f"[IMPORT] ✗ {filename}: {e}")

    print(f"\n[IMPORT] === SUMMARY ===")
    print(f"[IMPORT] Total files: {total_files}")
    print(f"[IMPORT] Total sales: {total_sales:,}")
    print(f"[IMPORT] Postcode prefixes: {len(data_by_postcode)}")

    # Calculate statistics
    stats = {}
    for postcode_prefix in sorted(data_by_postcode.keys()):
        stats[postcode_prefix] = {}

        for prop_type, prices in data_by_postcode[postcode_prefix].items():
            if prices:
                prices_sorted = sorted(prices)
                stats[postcode_prefix][prop_type] = {
                    'avg': int(sum(prices) / len(prices)),
                    'median': prices_sorted[len(prices)//2],
                    'count': len(prices),
                    'min': min(prices),
                    'max': max(prices),
                    'p25': prices_sorted[len(prices)//4],
                    'p75': prices_sorted[3*len(prices)//4]
                }

    return stats, files_processed, total_sales

def generate_supabase_records(stats):
    """Generate records for Supabase insertion."""
    records = []

    for postcode, types in stats.items():
        for prop_type, data in types.items():
            records.append({
                'postcode': postcode,
                'property_type': prop_type,
                'avg_price': data['avg'],
                'median_price': data['median'],
                'count': data['count'],
                'min_price': data['min'],
                'max_price': data['max'],
                'p25_price': data['p25'],
                'p75_price': data['p75'],
                'data_source': 'HM Land Registry (2018-2026)',
                'last_updated': datetime.utcnow().isoformat(),
            })

    return records

def print_sample_results(stats):
    """Print sample results including key postcodes."""
    print("\n[IMPORT] Sample results (with 8 years of data):")

    key_postcodes = ['KT1', 'KT16', 'SW1', 'GU2', 'M1', 'B1']

    for pc in key_postcodes:
        if pc in stats:
            print(f"\n  {pc}:")
            for prop_type in ['detached', 'semi_detached', 'terraced', 'flats_maisonettes']:
                if prop_type in stats[pc]:
                    data = stats[pc][prop_type]
                    print(f"    {prop_type:20} avg=£{data['avg']:>10,} median=£{data['median']:>10,} ({data['count']:>5} sales)")

# Run import
if __name__ == "__main__":
    print("\n" + "="*60)
    print("=== HM Land Registry FULL DATA IMPORT (2018-2026) ===")
    print("="*60 + "\n")

    stats, files_processed, total_sales = parse_all_files()

    if stats:
        print_sample_results(stats)

        # Generate Supabase records
        records = generate_supabase_records(stats)
        print(f"\n[IMPORT] Generated {len(records)} records (postcode + property type combinations)")

        # Save for later Supabase import
        with open('/tmp/hml_full_import.json', 'w') as f:
            json.dump({
                'records': records,
                'metadata': {
                    'total_sales': total_sales,
                    'files_processed': len(files_processed),
                    'postcode_prefixes': len(stats),
                    'generated_at': datetime.utcnow().isoformat()
                }
            }, f)

        print(f"\n[IMPORT] ✓ Ready to import into Supabase")
        print(f"[IMPORT] Data includes {total_sales:,} real property sales")
        print(f"[IMPORT] Covers {len(stats)} UK postcode areas")

    else:
        print("[IMPORT] Failed to parse files")
