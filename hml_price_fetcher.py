"""
HM Land Registry Price Paid Data Importer
Downloads and parses real UK house sales data by postcode and property type.

Reference: https://use-land-property-data.service.gov.uk/
Dataset: Price Paid Data (30.5 MB, updated monthly, FREE)
"""

import requests
import csv
import io
from collections import defaultdict
from datetime import datetime

class HMLandRegistryImporter:
    def __init__(self):
        # HM Land Registry publishes CSV dumps
        self.dataset_url = "https://use-land-property-data.service.gov.uk/api/v1/datasets/ppd/csv"
        self.data_by_postcode = defaultdict(lambda: defaultdict(list))

    def download_dataset(self):
        """Download latest Price Paid Data CSV from HM Land Registry."""
        try:
            print("[HML] Downloading Price Paid Data CSV...")
            print(f"[HML] URL: {self.dataset_url}")

            response = requests.get(self.dataset_url, timeout=60, stream=True)
            print(f"[HML] Status: {response.status_code}")

            if response.status_code == 200:
                print(f"[HML] Downloaded {len(response.content) / 1024 / 1024:.1f} MB")
                return response.text
            else:
                print(f"[HML] Download failed: {response.status_code}")
                print(f"[HML] Response: {response.text[:500]}")
                return None

        except Exception as e:
            print(f"[HML] Download error: {e}")
            print(f"[HML] Note: API might require authentication. See https://use-land-property-data.service.gov.uk/")
            return None

    def parse_csv(self, csv_text):
        """Parse Price Paid Data CSV and aggregate by postcode + property type."""
        if not csv_text:
            return None

        try:
            print("[HML] Parsing CSV data...")

            reader = csv.DictReader(io.StringIO(csv_text))
            count = 0

            for row in reader:
                try:
                    postcode = (row.get('postcode') or '').strip().upper().replace(' ', '')
                    price = int(row.get('price', 0))
                    prop_type = (row.get('type') or 'unknown').lower()

                    if postcode and price > 0:
                        # Group by postcode + property type
                        self.data_by_postcode[postcode[:3]][prop_type].append(price)
                        count += 1

                        if count % 10000 == 0:
                            print(f"[HML] Processed {count:,} sales...")

                except Exception as e:
                    continue

            print(f"[HML] Total sales processed: {count:,}")
            return self.data_by_postcode

        except Exception as e:
            print(f"[HML] Parse error: {e}")
            return None

    def calculate_stats(self):
        """Calculate average prices by postcode and property type."""
        stats = {}

        for postcode_prefix, types_data in self.data_by_postcode.items():
            stats[postcode_prefix] = {}

            for prop_type, prices in types_data.items():
                if prices:
                    stats[postcode_prefix][prop_type] = {
                        'avg': int(sum(prices) / len(prices)),
                        'median': sorted(prices)[len(prices)//2],
                        'count': len(prices),
                        'min': min(prices),
                        'max': max(prices)
                    }

        return stats

    def generate_supabase_insert(self, stats):
        """Generate Supabase insert statements."""
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
                    'year_month': datetime.utcnow().strftime('%Y-%m')
                })

        return records

# Test it
if __name__ == "__main__":
    print("\n=== HM Land Registry Price Paid Data Importer ===\n")

    importer = HMLandRegistryImporter()

    # Step 1: Download
    csv_data = importer.download_dataset()

    if csv_data:
        # Step 2: Parse
        data = importer.parse_csv(csv_data)

        if data:
            # Step 3: Calculate stats
            stats = importer.calculate_stats()
            print(f"\n[HML] Found data for {len(stats)} postcode prefixes")

            # Show sample
            for pc, types in list(stats.items())[:3]:
                print(f"\n  {pc}:")
                for ptype, s in types.items():
                    print(f"    {ptype}: avg=£{s['avg']:,} ({s['count']} sales)")

            # Step 4: Generate insert records
            records = importer.generate_supabase_insert(stats)
            print(f"\n[HML] Ready to insert {len(records)} records into Supabase")
    else:
        print("\n[HML] Could not download data.")
        print("\nOptions:")
        print("1. Check if API requires authentication")
        print("2. Manually download CSV from: https://use-land-property-data.service.gov.uk/")
        print("3. Save as 'price_paid_data.csv' and re-run with local file")
