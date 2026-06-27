"""
Import HM Land Registry Price Paid Data into Supabase.
Parses CSV and creates postcode-level property type pricing data.
"""

import csv
from collections import defaultdict
from datetime import datetime

# Property type codes from HM Land Registry
PROPERTY_TYPES = {
    'D': 'detached',
    'S': 'semi_detached',
    'T': 'terraced',
    'F': 'flats_maisonettes',
    'O': 'other'  # Other (e.g., bungalow)
}

def parse_hml_csv(filepath):
    """Parse HM Land Registry CSV and aggregate by postcode + property type."""

    data_by_postcode = defaultdict(lambda: defaultdict(list))
    count = 0
    errors = 0

    print(f"[IMPORT] Reading {filepath}...")

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            # CSV is quoted, no header
            reader = csv.reader(f)

            for row in reader:
                try:
                    if len(row) < 5:
                        continue

                    # Extract fields
                    price = int(row[1].strip())
                    date_str = row[2].strip()
                    postcode = row[3].strip().upper().replace(' ', '')
                    prop_type_code = row[4].strip()

                    # Map property type
                    prop_type = PROPERTY_TYPES.get(prop_type_code, 'other')

                    if postcode and price > 0 and len(postcode) >= 4:
                        # Group by postcode prefix (first 3 chars: KT1, SW1, etc.)
                        pc_prefix = postcode[:3]
                        data_by_postcode[pc_prefix][prop_type].append(price)
                        count += 1

                        if count % 50000 == 0:
                            print(f"[IMPORT] Processed {count:,} sales...")

                except (ValueError, IndexError) as e:
                    errors += 1
                    continue

    except Exception as e:
        print(f"[IMPORT] Error reading file: {e}")
        return None

    print(f"[IMPORT] Total valid sales: {count:,}")
    print(f"[IMPORT] Postcode prefixes found: {len(data_by_postcode)}")

    # Calculate statistics
    stats = {}
    for postcode_prefix, types_data in sorted(data_by_postcode.items()):
        stats[postcode_prefix] = {}

        for prop_type, prices in types_data.items():
            if prices:
                prices_sorted = sorted(prices)
                stats[postcode_prefix][prop_type] = {
                    'avg': int(sum(prices) / len(prices)),
                    'median': prices_sorted[len(prices)//2],
                    'count': len(prices),
                    'min': min(prices),
                    'max': max(prices)
                }

    return stats

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
                'data_source': 'HM Land Registry Price Paid Data',
                'last_updated': datetime.utcnow().isoformat(),
            })

    return records

def print_sample_results(stats):
    """Print sample results."""
    print("\n[IMPORT] Sample results:")

    for postcode in sorted(list(stats.keys())[:10]):
        print(f"\n  {postcode}:")
        for prop_type, data in stats[postcode].items():
            print(f"    {prop_type:20} avg=£{data['avg']:>10,} ({data['count']:>5} sales)")

# Run import
if __name__ == "__main__":
    filepath = "/Users/srevi/Downloads/pp-monthly-update-new-version.csv"

    print("\n=== HM Land Registry Data Import ===\n")

    stats = parse_hml_csv(filepath)

    if stats:
        print_sample_results(stats)

        # Generate Supabase records
        records = generate_supabase_records(stats)
        print(f"\n[IMPORT] Generated {len(records)} records for Supabase")

        # Show how to insert
        print("\n[IMPORT] Next step: Import into Supabase table 'house_price_real'")
        print("         This data will be used for real house price lookups")

        # Save to file for inspection
        import json
        with open('/tmp/hml_import.json', 'w') as f:
            json.dump(records[:10], f, indent=2)
        print("\n[IMPORT] Sample records saved to /tmp/hml_import.json")
    else:
        print("[IMPORT] Failed to parse CSV")
