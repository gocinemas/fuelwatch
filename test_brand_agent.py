#!/usr/bin/env python3
"""
Stress test the brand request agent
Submits multiple brand requests and monitors processing
"""

import os
import time
import requests
from datetime import datetime

# Test brands to request
TEST_BRANDS = [
    {"brand_name": "Nutella", "category": "snacks", "email": "test1@example.com"},
    {"brand_name": "Tesla", "category": "automotive", "email": "test2@example.com"},
    {"brand_name": "Dyson", "category": "appliances", "email": "test3@example.com"},
    {"brand_name": "Lululemon", "category": "apparel", "email": "test4@example.com"},
    {"brand_name": "Airbnb", "category": "hospitality", "email": "test5@example.com"},
]

BASE_URL = "http://localhost:5000"  # Change if running on Railway

def submit_brand_request(brand_name, category, email):
    """Submit a brand request"""
    try:
        response = requests.post(
            f"{BASE_URL}/api/intel/request-brand",
            json={"brand_name": brand_name, "category": category, "email": email},
            timeout=5
        )
        print(f"✓ Submitted: {brand_name} ({category})")
        return True
    except Exception as e:
        print(f"✗ Failed to submit {brand_name}: {e}")
        return False

def main():
    print(f"🚀 Stress Testing Brand Agent\n")
    print(f"Submitting {len(TEST_BRANDS)} brand requests...\n")

    # Submit all requests
    submitted = 0
    for brand in TEST_BRANDS:
        if submit_brand_request(brand["brand_name"], brand["category"], brand["email"]):
            submitted += 1
        time.sleep(0.5)  # Small delay between requests

    print(f"\n✅ Submitted {submitted}/{len(TEST_BRANDS)} requests")
    print(f"\n⏳ Check back in 2-3 minutes to see if agent processed them:")
    print(f"   Supabase → brand_data_requests table → look for status 'collected'")
    print(f"\n📝 Or search for brands at: {BASE_URL}/brand?search=Nutella")

if __name__ == "__main__":
    main()
