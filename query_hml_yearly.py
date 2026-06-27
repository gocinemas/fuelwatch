"""
Query HM Land Registry yearly CSV files for price trends by postcode.
Called dynamically when user searches a postcode - extracts from source files.
"""

import csv
import glob
import re
from collections import defaultdict

PROPERTY_TYPES = {
    'D': 'detached',
    'S': 'semi_detached',
    'T': 'terraced',
    'F': 'flats_maisonettes',
    'O': 'other'
}

def query_postcode_yearly_trend(postcode: str, property_type: str = None):
    """
    Query yearly CSV files for a specific postcode.
    Returns: {year: {property_type: {avg, median, count, min, max}}}

    Example:
    query_postcode_yearly_trend("KT16", "semi_detached")
    →
    {
      2018: {semi_detached: {avg: 580000, median: 550000, count: 45, ...}},
      2019: {semi_detached: {avg: 595000, median: 570000, count: 52, ...}},
      ...
    }
    """

    postcode_clean = postcode.replace(" ", "").upper()
    pc_prefix = postcode_clean[:3]

    # Data structure: {year: {prop_type: [prices]}}
    prices_by_year = defaultdict(lambda: defaultdict(list))

    csv_files = sorted(glob.glob("/Users/srevi/Downloads/pp-*.csv"))

    print(f"[QUERY] Searching for {postcode} ({pc_prefix}) in {len(csv_files)} yearly files...")

    total_matches = 0

    for filepath in csv_files:
        filename = filepath.split('/')[-1]
        year_match = re.search(r'pp-(\d{4})', filename)
        if not year_match:
            continue

        year = year_match.group(1)
        file_matches = 0

        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)

                for row in reader:
                    try:
                        if len(row) < 5:
                            continue

                        price = int(row[1].strip())
                        pc = row[3].strip().upper().replace(' ', '')
                        prop_type_code = row[4].strip()

                        # Match postcode prefix
                        if not pc.startswith(pc_prefix):
                            continue

                        prop_type = PROPERTY_TYPES.get(prop_type_code, 'other')

                        # If searching specific type, skip others
                        if property_type and prop_type != property_type:
                            continue

                        if price > 0:
                            prices_by_year[year][prop_type].append(price)
                            file_matches += 1
                            total_matches += 1

                    except (ValueError, IndexError):
                        continue

            if file_matches > 0:
                print(f"  {year}: {file_matches} sales for {pc_prefix}")

        except Exception as e:
            print(f"  {year}: Error reading file")

    # Calculate statistics
    yearly_stats = {}

    for year in sorted(prices_by_year.keys()):
        yearly_stats[year] = {}

        for prop_type, prices in prices_by_year[year].items():
            if prices:
                prices_sorted = sorted(prices)
                yearly_stats[year][prop_type] = {
                    'avg': int(sum(prices) / len(prices)),
                    'median': prices_sorted[len(prices)//2],
                    'count': len(prices),
                    'min': min(prices),
                    'max': max(prices),
                    'p25': prices_sorted[len(prices)//4],
                    'p75': prices_sorted[3*len(prices)//4]
                }

    print(f"[QUERY] Found {total_matches} sales for {postcode}")

    return yearly_stats if yearly_stats else None

# Test it
if __name__ == "__main__":
    print("\n=== Query HM Land Registry Yearly Data ===\n")

    # Test with a postcode
    result = query_postcode_yearly_trend("KT16", "semi_detached")

    if result:
        print("\n=== KT16 Semi-Detached Yearly Trend ===")
        print("Year | Avg Price | Median | Count | Min | Max")
        print("-----|-----------|--------|-------|-----|----")
        for year in sorted(result.keys()):
            if 'semi_detached' in result[year]:
                data = result[year]['semi_detached']
                print(f"{year} | £{data['avg']:>8,} | £{data['median']:>6,} | {data['count']:>5} | £{data['min']:>7,} | £{data['max']:>8,}")
    else:
        print("\n✗ No data found for KT16")
