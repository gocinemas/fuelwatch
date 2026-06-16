#!/usr/bin/env python3
"""
Test suite for FuelWatch/Miru/Intel
Tests backend APIs, data fetching, and core functions
Run: python3 test_suite.py
"""

import sys
import os
import json
import requests
from datetime import datetime

sys.path.insert(0, '/Users/srevi/fuelwatch')

# Color codes for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.tests = []

    def test(self, name, fn):
        """Run a test and track results"""
        try:
            fn()
            self.passed += 1
            print(f"{GREEN}✓{RESET} {name}")
        except AssertionError as e:
            self.failed += 1
            print(f"{RED}✗{RESET} {name}: {e}")
        except Exception as e:
            self.failed += 1
            print(f"{RED}✗{RESET} {name}: {type(e).__name__}: {e}")

    def summary(self):
        total = self.passed + self.failed
        status = GREEN if self.failed == 0 else RED
        print(f"\n{status}Results: {self.passed}/{total} passed{RESET}")
        return self.failed == 0

# Initialize test runner
runner = TestRunner()

# ────────────────────────────────────────────────────────────────────────────
# INTEL BRAND INTELLIGENCE TESTS
# ────────────────────────────────────────────────────────────────────────────

print(f"\n{YELLOW}━━ INTEL: Brand Intelligence ━━{RESET}")

def test_wikipedia_fetch():
    from brands_intelligence import fetch_brand_from_wikipedia
    result = fetch_brand_from_wikipedia("Apple Inc")
    assert result is not None, "Wikipedia fetch returned None"
    assert "description" in result, "Missing description"
    assert len(result["description"]) > 10, "Description too short"
    assert "wikipedia_url" in result, "Missing Wikipedia URL"

runner.test("Wikipedia API fetch", test_wikipedia_fetch)

def test_wikidata_fetch():
    from brands_intelligence import fetch_brand_from_wikidata
    result = fetch_brand_from_wikidata("BMW")
    assert result is not None, "Wikidata fetch returned None"
    assert "description" in result, "Missing description"
    assert len(result["description"]) > 5, "Description too short"

runner.test("Wikidata API fetch", test_wikidata_fetch)

def test_competitors_fetch():
    from brands_intelligence import fetch_brand_competitors
    result = fetch_brand_competitors("Tesla")
    assert len(result) > 0, "No competitors returned"
    assert "name" in result[0], "Missing competitor name"
    assert "market_cap" in result[0], "Missing market_cap"

runner.test("Competitors data fetch", test_competitors_fetch)

def test_brand_search():
    from brands_intelligence import search_and_store_brand
    result = search_and_store_brand("Nike")
    assert result is not None, "Search returned None"
    assert "name" in result, "Missing name"
    assert "description" in result, "Missing description"
    assert "competitors" in result, "Missing competitors"

runner.test("Full brand search flow", test_brand_search)

# ────────────────────────────────────────────────────────────────────────────
# API ENDPOINT TESTS (requires live server)
# ────────────────────────────────────────────────────────────────────────────

print(f"\n{YELLOW}━━ API Endpoints ━━{RESET}")

BASE_URL = "https://intel.humanagency.co"

def test_brands_search_api():
    url = f"{BASE_URL}/api/brands/search?q=Samsung"
    response = requests.get(url, timeout=10)
    assert response.status_code == 200, f"Status {response.status_code}"
    data = response.json()
    assert data.get("ok") == True, "API returned ok=False"
    assert "brand" in data, "Missing brand in response"
    assert data["brand"].get("name") is not None, "Missing brand name"

runner.test("GET /api/brands/search", test_brands_search_api)

def test_brands_search_with_refresh():
    url = f"{BASE_URL}/api/brands/search?q=Adidas&refresh=true"
    response = requests.get(url, timeout=10)
    assert response.status_code == 200, f"Status {response.status_code}"
    data = response.json()
    assert data.get("ok") == True, "API returned ok=False"

runner.test("GET /api/brands/search with refresh", test_brands_search_with_refresh)

def test_brands_competitors():
    url = f"{BASE_URL}/api/brands/search?q=Nike"
    response = requests.get(url, timeout=10)
    data = response.json()
    assert "competitors_count" in data, "Missing competitors_count"
    assert data["competitors_count"] >= 0, "Invalid competitors_count"

runner.test("Competitors data in API response", test_brands_competitors)

# ────────────────────────────────────────────────────────────────────────────
# MIRU CORE TESTS (basic smoke tests)
# ────────────────────────────────────────────────────────────────────────────

print(f"\n{YELLOW}━━ Miru: Core Functions ━━{RESET}")

def test_search_module_exists():
    """Check if search module can be imported"""
    try:
        import search
        assert hasattr(search, 'postcode_to_latlon'), "Missing postcode_to_latlon"
        assert hasattr(search, 'fetch_all_stations'), "Missing fetch_all_stations"
    except ImportError:
        raise AssertionError("search module not found")

runner.test("Search module imports", test_search_module_exists)

def test_library_module():
    """Check if library module works"""
    import library as lib
    assert lib is not None, "library module is None"
    # Check if Supabase client can be initialized (won't actually connect)
    assert hasattr(lib, '_sb'), "Missing _sb function"

runner.test("Library module initialization", test_library_module)

# ────────────────────────────────────────────────────────────────────────────
# DEPLOYMENT VALIDATION
# ────────────────────────────────────────────────────────────────────────────

print(f"\n{YELLOW}━━ Deployment Health ━━{RESET}")

def test_flask_app_loads():
    """Verify Flask app can be imported without errors"""
    try:
        sys.path.insert(0, '/Users/srevi/fuelwatch')
        from sms_service import app
        assert app is not None, "Flask app is None"
        assert hasattr(app, 'route'), "Flask app missing route decorator"
    except Exception as e:
        raise AssertionError(f"Flask app failed to load: {e}")

runner.test("Flask app loads", test_flask_app_loads)

def test_html_template_exists():
    """Check if main template exists"""
    import os
    template_path = '/Users/srevi/fuelwatch/templates/index.html'
    assert os.path.exists(template_path), f"Template not found at {template_path}"
    assert os.path.getsize(template_path) > 10000, "Template file too small"

runner.test("HTML template exists", test_html_template_exists)

def test_api_responds():
    """Quick health check on deployed API"""
    try:
        response = requests.get(f"{BASE_URL}/api/brands/search?q=test", timeout=5)
        # Should return either valid response or 400 (invalid input)
        assert response.status_code in [200, 400], f"Unexpected status: {response.status_code}"
    except requests.exceptions.ConnectionError:
        raise AssertionError("Cannot connect to deployed API")

runner.test("Deployed API responds", test_api_responds)

# ────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    success = runner.summary()
    sys.exit(0 if success else 1)
