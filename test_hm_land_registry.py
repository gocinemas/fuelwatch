"""
Test HM Land Registry API integration for real house price data.
Reference: https://use-land-property-data.service.gov.uk/
"""

import requests
import json
from datetime import datetime

class HMLandRegistryClient:
    def __init__(self):
        self.base_url = "https://use-land-property-data.service.gov.uk/api/v1"
        self.datasets_url = f"{self.base_url}/datasets"

    def get_available_datasets(self):
        """Check available datasets from HM Land Registry."""
        try:
            print("[HML] Fetching available datasets...")
            response = requests.get(self.datasets_url, timeout=10)
            print(f"[HML] Status: {response.status_code}")

            if response.status_code == 200:
                datasets = response.json()
                print(f"[HML] Found {len(datasets.get('datasets', []))} datasets:")
                for ds in datasets.get('datasets', [])[:5]:
                    print(f"  - {ds.get('name')} ({ds.get('type')})")
                return datasets
            else:
                print(f"[HML] Error: {response.text[:500]}")
                return None
        except Exception as e:
            print(f"[HML] Connection error: {e}")
            return None

    def fetch_price_paid_data(self, postcode: str):
        """Fetch Price Paid Data for a specific postcode."""
        try:
            print(f"[HML] Fetching Price Paid Data for {postcode}...")

            # Try the Price Paid Data endpoint
            endpoint = f"{self.base_url}/datasets/ppd/data"
            params = {
                "postcode": postcode.replace(" ", ""),
                "limit": 100,
            }

            response = requests.get(endpoint, params=params, timeout=10)
            print(f"[HML] Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                print(f"[HML] Found {len(data.get('data', []))} sales for {postcode}")
                return data
            else:
                print(f"[HML] Error: {response.text[:500]}")
                return None
        except Exception as e:
            print(f"[HML] Error: {e}")
            return None

    def parse_property_types(self, sales_data: dict):
        """Parse sales data by property type and calculate averages."""
        if not sales_data or not sales_data.get('data'):
            return None

        by_type = {}
        for sale in sales_data['data']:
            prop_type = sale.get('propertyType', 'unknown')
            price = sale.get('price', 0)

            if prop_type not in by_type:
                by_type[prop_type] = {'prices': [], 'count': 0}

            by_type[prop_type]['prices'].append(price)
            by_type[prop_type]['count'] += 1

        # Calculate averages
        summary = {}
        for prop_type, data in by_type.items():
            if data['prices']:
                avg = sum(data['prices']) / len(data['prices'])
                summary[prop_type] = {
                    'avg': int(avg),
                    'count': data['count'],
                    'min': min(data['prices']),
                    'max': max(data['prices'])
                }

        return summary

# Test it
if __name__ == "__main__":
    client = HMLandRegistryClient()

    print("\n=== HM Land Registry API Test ===\n")

    # Check available datasets
    datasets = client.get_available_datasets()

    if datasets:
        print("\n✓ API connection successful!")
    else:
        print("\n✗ Could not connect to HM Land Registry API")
        print("\nNote: The API might require authentication or have rate limits.")
        print("Next steps:")
        print("1. Check if you need an API key from HM Land Registry")
        print("2. Or try downloading their CSV data files instead")
        print("3. See: https://use-land-property-data.service.gov.uk/")
