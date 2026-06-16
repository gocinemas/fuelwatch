#!/usr/bin/env python3
"""
Post-deployment verification agent
Runs immediately after Railway deploy to verify all modules are working
"""

import requests
import time
import sys
from datetime import datetime

# Configuration
SERVICES = {
    "miru": "https://miru.humanagency.co",
    "intel": "https://intel.humanagency.co",
    "ai": "https://ai.humanagency.co"
}

TESTS = {
    "intel": [
        {
            "name": "Brand search API",
            "url": "/api/brands/search?q=Apple",
            "checks": ["ok", "brand"]
        },
        {
            "name": "Competitors data",
            "url": "/api/brands/search?q=Tesla",
            "checks": ["competitors_count"]
        },
        {
            "name": "Refresh parameter",
            "url": "/api/brands/search?q=Nike&refresh=true",
            "checks": ["ok"]
        }
    ]
}

def verify_service(service_name, base_url):
    """Verify a service is running"""
    try:
        response = requests.get(f"{base_url}/", timeout=5)
        return response.status_code < 500
    except:
        return False

def test_endpoint(service_name, base_url, test):
    """Test a specific API endpoint"""
    try:
        url = f"{base_url}{test['url']}"
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            return False, f"Status {response.status_code}"

        data = response.json()

        # Check for required fields
        for check in test.get("checks", []):
            if check not in data:
                return False, f"Missing '{check}'"

        return True, "OK"
    except Exception as e:
        return False, str(e)

def main():
    print(f"\n🚀 Post-Deploy Verification — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    all_passed = True

    for service_name, base_url in SERVICES.items():
        print(f"\n📍 {service_name.upper()}")

        # Check if service is up
        if not verify_service(service_name, base_url):
            print(f"  ❌ Service not responding")
            all_passed = False
            continue

        print(f"  ✓ Service responding")

        # Run endpoint tests for this service
        if service_name in TESTS:
            for test in TESTS[service_name]:
                passed, msg = test_endpoint(service_name, base_url, test)
                status = "✓" if passed else "✗"
                print(f"  {status} {test['name']}: {msg}")
                if not passed:
                    all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All systems operational")
        return 0
    else:
        print("❌ Some services failed verification")
        return 1

if __name__ == '__main__':
    sys.exit(main())
