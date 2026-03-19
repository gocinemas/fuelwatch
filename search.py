#!/usr/bin/env python3
"""
FuelWatch UK — Postcode Search
================================
Find cheapest fuel stations near any UK postcode.

Data: CMA-mandated retailer price feeds (updated daily, no API key needed)
Geocoding: postcodes.io (free, no API key needed)
"""

import math
import requests
import sys
from typing import Optional

# ── CMA Retailer Price Feed URLs ──────────────────────────────────────────────
# Confirmed working (tested March 2026)
RETAILER_FEEDS = {
    "Asda":       "https://storelocator.asda.com/fuel_prices_data.json",
    "Tesco":      "https://www.tesco.com/fuel_prices/fuel_prices_data.json",
    "BP":         "https://www.bp.com/en_gb/united-kingdom/home/fuelprices/fuel_prices_data.json",
    "Jet":        "https://jetlocal.co.uk/fuel_prices_data.json",
    "Applegreen": "https://applegreenstores.com/fuel-prices/data.json",
    "Rontec":     "https://www.rontec-servicestations.co.uk/fuel-prices/data/fuel_prices_data.json",
    "Moto":       "https://www.moto-way.com/fuel-price/fuel_prices.json",
    "SGN":        "https://www.sgnretail.uk/files/data/SGN_daily_fuel_prices.json",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# ── Geocoding ─────────────────────────────────────────────────────────────────

def postcode_to_latlon(postcode: str) -> Optional[tuple]:
    """Convert a UK postcode to (lat, lon) using postcodes.io."""
    postcode = postcode.strip().replace(" ", "").upper()
    try:
        resp = requests.get(
            f"https://api.postcodes.io/postcodes/{postcode}",
            timeout=5, headers=HEADERS
        )
        data = resp.json()
        if data.get("status") == 200:
            r = data["result"]
            return (r["latitude"], r["longitude"])
        print(f"Postcode not found: {postcode}")
    except Exception as e:
        print(f"Geocoding failed: {e}")
    return None


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── Price Feed Parsing ────────────────────────────────────────────────────────

def fetch_retailer(name: str, url: str) -> list:
    """
    Fetch and normalise a CMA retailer price feed.
    CMA standard format: {stations: [{location: {latitude, longitude}, prices: {E10, B7}}]}
    """
    try:
        resp = requests.get(url, timeout=10, headers=HEADERS)
        if resp.status_code != 200:
            return []
        data = resp.json()

        stations_raw = (
            data.get("stations") or
            data.get("sites") or
            []
        )

        stations = []
        for s in stations_raw:
            prices = s.get("prices", {})

            petrol = (
                prices.get("E10") or prices.get("E5") or
                prices.get("Unleaded") or prices.get("unleaded") or None
            )
            diesel = (
                prices.get("B7") or prices.get("Diesel") or
                prices.get("diesel") or None
            )

            if petrol is None and diesel is None:
                continue

            # CMA standard format nests coordinates under 'location'
            loc = s.get("location", {})
            lat = loc.get("latitude") or s.get("lat") or s.get("latitude")
            lon = loc.get("longitude") or s.get("lng") or s.get("lon") or s.get("longitude")

            if lat is None or lon is None:
                continue

            # Some feeds give prices in tenths of a penny (e.g. 1389 = 138.9p)
            def normalise(p):
                if p is None:
                    return None
                p = float(p)
                return p / 10 if p > 500 else p

            stations.append({
                "brand":    s.get("brand", name),
                "address":  s.get("address", s.get("postcode", "")),
                "postcode": s.get("postcode", ""),
                "lat":      float(lat),
                "lon":      float(lon),
                "petrol":   normalise(petrol),
                "diesel":   normalise(diesel),
            })
        return stations

    except Exception:
        return []


def fetch_all_stations() -> list:
    """Fetch from all CMA retailer feeds. Returns combined station list."""
    all_stations = []
    print("Fetching live prices from CMA retailer feeds...")
    for name, url in RETAILER_FEEDS.items():
        stations = fetch_retailer(name, url)
        if stations:
            all_stations.extend(stations)
            print(f"  {name}: {len(stations)} stations loaded")
        else:
            print(f"  {name}: unavailable")

    print(f"\n  Total: {len(all_stations)} stations\n")
    return all_stations


# ── Search ────────────────────────────────────────────────────────────────────

def search_near_postcode(postcode: str, fuel: str = "petrol",
                         radius_miles: float = 5.0, top_n: int = 10):
    """Find cheapest fuel stations within radius of a postcode."""

    latlon = postcode_to_latlon(postcode)
    if not latlon:
        return
    lat, lon = latlon
    radius_km = radius_miles * 1.60934

    stations = fetch_all_stations()
    if not stations:
        print("No station data available.")
        return

    nearby = []
    for s in stations:
        price = s.get(fuel)
        if not price or price <= 0:
            continue
        dist_km = haversine_km(lat, lon, s["lat"], s["lon"])
        if dist_km <= radius_km:
            nearby.append({**s, "distance_miles": dist_km / 1.60934, "price": price})

    if not nearby:
        print(f"No {fuel} stations found within {radius_miles} miles of {postcode.upper()}.")
        print(f"Try a larger radius, e.g.: python3 search.py {postcode} {fuel} 10")
        return

    nearby.sort(key=lambda x: (x["price"], x["distance_miles"]))
    cheapest_price = nearby[0]["price"]
    avg_price = sum(s["price"] for s in nearby) / len(nearby)

    fuel_label = "Petrol (E10)" if fuel == "petrol" else "Diesel (B7)"
    print(f"{'='*62}")
    print(f"  {fuel_label} near {postcode.upper()}")
    print(f"  Radius: {radius_miles} miles  |  {len(nearby)} stations found")
    print(f"{'='*62}")
    print(f"  {'':3} {'Brand':<14} {'Price':>7}  {'vs area avg':>11}  {'Miles':>5}  Address")
    print(f"  {'─'*3} {'─'*14} {'─'*7}  {'─'*11}  {'─'*5}  {'─'*18}")

    for i, s in enumerate(nearby[:top_n], 1):
        saving = avg_price - s["price"]
        saving_str = f"-{saving:.1f}p" if saving > 0 else f"+{abs(saving):.1f}p"
        rank = ">>>" if i == 1 else f"{i:>3}."
        print(f"  {rank} {s['brand']:<14} {s['price']:>6.1f}p  {saving_str:>11}  {s['distance_miles']:>4.1f}mi  {s['address']}")

    cheapest = nearby[0]
    tank_saving = (avg_price - cheapest_price) * 55 / 100

    print(f"\n  Cheapest : {cheapest['brand']} — {cheapest['price']:.1f}p — {cheapest['address']}")
    print(f"  Area avg : {avg_price:.1f}p")
    print(f"  You save : {avg_price - cheapest_price:.1f}p/litre  |  Full tank (55L): £{tank_saving:.2f}")
    print()

    return nearby


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("\nUsage:   python3 search.py <POSTCODE> [petrol|diesel] [radius_miles]")
        print("Example: python3 search.py SW1A1AA petrol 5\n")
        sys.exit(1)

    postcode     = sys.argv[1]
    fuel         = sys.argv[2].lower() if len(sys.argv) > 2 else "petrol"
    radius_miles = float(sys.argv[3])  if len(sys.argv) > 3 else 5.0

    if fuel not in ("petrol", "diesel"):
        print("Fuel must be 'petrol' or 'diesel'")
        sys.exit(1)

    search_near_postcode(postcode, fuel=fuel, radius_miles=radius_miles)


if __name__ == "__main__":
    main()
