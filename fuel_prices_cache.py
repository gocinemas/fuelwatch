#!/usr/bin/env python3
"""
Fuel Prices Cache — runs locally every 30 min
Fetches from Fuel Finder API (UK IP) and uploads to Supabase.
Railway reads from Supabase (no geofencing).
"""
import os
import sys
import requests
import json
import time
from datetime import datetime, timedelta
from typing import Optional

# Config
CLIENT_ID = "2VLf28fLFwZrNBJpwLjYnaHM2vRBhT1p"
CLIENT_SECRET = "kfUxvLNVTIZay6LnTeHSTXrMNd4E5yhqkRcIW93NDsb9CecdSJhAsl0O2PVB5JVH"
REFRESH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJraW5kIjoicHVibGljIiwiY2xpZW50X2lkIjoiMlZMZjI4ZkxGd1pyTkJKcHdMalluYUhNMnZSQmhUMXAiLCJpbmZvX3JlY2lwaWVudF9pZCI6ImNhZGY1MDYxLWRkYzAtNDZlMC04NDIxLTE1MjRlZTQyYzc3ZiIsInRva2VuX3VzZSI6InJlZnJlc2giLCJzdWIiOiIyVkxmMjhmTEZ3WnJOQkpwd0xqWW5hSE0ydlJCaFQxcCIsImF1ZCI6Im9hdXRoIiwiaWF0IjoxNzg2MDMwMTgyLCJleHAiOjE3ODYyMDI5ODJ9.VZK_XW6BRDoW2mqsqV2nKEJx-Y-X0DoKau2RmJl3PQw"

# Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


def get_fresh_access_token() -> Optional[str]:
    """Get access_token from refresh_token (from UK IP)."""
    try:
        resp = requests.post(
            "https://www.fuel-finder.service.gov.uk/api/v1/oauth/generate_access_token",
            data={
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": REFRESH_TOKEN,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )

        if resp.status_code == 200:
            token = resp.json().get("data", {}).get("access_token")
            if token:
                return token

        print(f"✗ Failed to get access_token: {resp.status_code}")
        return None

    except Exception as e:
        print(f"✗ Exception: {e}")
        return None


PFS_URL = "https://www.fuel-finder.service.gov.uk/api/v1/pfs"
PRICES_URL = "https://www.fuel-finder.service.gov.uk/api/v1/pfs/fuel-prices"
MAX_PAGES = 20
MAX_TRIES = 3
# A full national list is ~8,000 stations. Anything much smaller is a partial pull.
MIN_STATIONS = 5000


def _get_page(url: str, token: str, batch_number: int):
    """Fetch one page of the Fuel Finder API, retrying transient failures.
    Returns (items, ok). ok=False means the page could not be fetched at all."""
    last_err = ""
    for attempt in range(1, MAX_TRIES + 1):
        try:
            resp = requests.get(
                url,
                params={"batch-number": batch_number},
                headers={"Authorization": f"Bearer {token}"},
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                return (data if isinstance(data, list) else []), True
            if resp.status_code == 404:
                # The API answers 404 for the page after the last one: end of list, not a failure.
                return [], True
            last_err = f"HTTP {resp.status_code}"
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = str(e)[:120]
        time.sleep(2 * attempt)
    print(f"✗ page {batch_number} failed after {MAX_TRIES} tries: {last_err}")
    return None, False


def fetch_fuel_prices(token: str) -> dict:
    """Fetch all fuel prices from Fuel Finder API (paginated, ~8000 stations).
    Returns complete=False if any page could not be fetched, so the caller
    can refuse to overwrite good data with a partial pull."""
    stations_by_id = {}
    complete = True

    print("[fuel-cache] Fetching PFS locations...")
    batch_number = 1
    while True:
        if batch_number > MAX_PAGES:
            print("✗ location list longer than page limit — treating as incomplete")
            complete = False
            break
        data, ok = _get_page(PFS_URL, token, batch_number)
        if not ok:
            complete = False
            break
        if not data:
            break
        for item in data:
            try:
                node_id = item.get("node_id", "")
                if not node_id:
                    continue
                location = item.get("location", {})
                stations_by_id[node_id] = {
                    "node_id": node_id,
                    "brand": item.get("brand_name", item.get("trading_name", "Unknown")),
                    "address": location.get("address_line_1", ""),
                    "postcode": location.get("postcode", ""),
                    "lat": float(location.get("latitude") or 0),
                    "lon": float(location.get("longitude") or 0),
                    "petrol": None,
                    "diesel": None,
                }
            except (ValueError, TypeError, KeyError, AttributeError):
                continue
        batch_number += 1

    print(f"[fuel-cache] Fetched {len(stations_by_id)} locations")

    print("[fuel-cache] Fetching fuel prices...")
    price_count = 0
    batch_number = 1
    while True:
        if batch_number > MAX_PAGES:
            print("✗ price list longer than page limit — treating as incomplete")
            complete = False
            break
        data, ok = _get_page(PRICES_URL, token, batch_number)
        if not ok:
            complete = False
            break
        if not data:
            break
        for item in data:
            try:
                node_id = item.get("node_id", "")
                fuel_prices = item.get("fuel_prices", [])
                if node_id in stations_by_id and fuel_prices:
                    for fuel in fuel_prices:
                        fuel_type = fuel.get("fuel_type", "")
                        price = float(fuel.get("price") or 0)
                        if fuel_type == "E10" and price > 0:
                            stations_by_id[node_id]["petrol"] = price
                            price_count += 1
                        elif fuel_type in ("B7S", "B7P", "B7_STANDARD", "B7_PREMIUM") and price > 0:
                            stations_by_id[node_id]["diesel"] = price
            except (ValueError, TypeError, KeyError, AttributeError):
                continue
        batch_number += 1

    print(f"[fuel-cache] Fetched {price_count} prices")

    all_stations = [s for s in stations_by_id.values() if (s["lat"] and s["lon"]) and (s["petrol"] or s["diesel"])]
    print(f"[fuel-cache] Total: {len(all_stations)} valid stations (complete={complete})")

    return {"stations": all_stations, "complete": complete, "updated_at": datetime.utcnow().isoformat()}


def upload_to_supabase(fuel_data: dict) -> bool:
    """Upload fuel prices to Supabase fuel_prices_cache table."""
    if not SUPABASE_KEY:
        print("✗ SUPABASE_KEY not set")
        return False

    try:
        # Store as single JSON doc with timestamp
        payload = {
            "id": "current",
            "data": fuel_data,
            "updated_at": datetime.utcnow().isoformat()
        }

        resp = requests.patch(
            f"{SUPABASE_URL}/rest/v1/fuel_prices_cache?id=eq.current",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=representation"
            },
            json=payload,
            timeout=10
        )

        if resp.status_code in (200, 201):
            print("✓ Uploaded to Supabase")
            return True
        else:
            print(f"✗ Supabase upload failed: {resp.status_code}")
            print(f"  Response: {resp.text[:200]}")
            return False

    except Exception as e:
        print(f"✗ Exception uploading: {e}")
        return False


def refresh_cache():
    """Single refresh cycle. Only overwrites the cache with a complete, full-sized pull."""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Refreshing fuel prices cache...")

    token = get_fresh_access_token()
    if not token:
        print("✗ Could not get access_token")
        return False

    fuel_data = fetch_fuel_prices(token)
    n = len(fuel_data.get("stations", []))
    if not fuel_data.get("complete"):
        print("✗ Fuel Finder pull was incomplete — keeping the previous cache, not uploading")
        return False
    if n < MIN_STATIONS:
        print(f"✗ Only {n} stations (need at least {MIN_STATIONS}) — keeping the previous cache")
        return False

    return upload_to_supabase(fuel_data)


if __name__ == "__main__":
    if "--daemon" in sys.argv:
        print("Starting daemon (refresh every 30 min)...")
        import time
        while True:
            refresh_cache()
            time.sleep(1800)  # 30 minutes
    else:
        refresh_cache()
