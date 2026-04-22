#!/usr/bin/env python3
"""
FuelWatch UK — Postcode Search
================================
Find cheapest fuel stations near any UK postcode.

Data: CMA-mandated retailer price feeds (updated daily, no API key needed)
Geocoding: postcodes.io (free, no API key needed)
"""

import math
import os
import requests
import sys
import time
from datetime import datetime
from typing import Optional

# ── CMA Retailer Price Feed URLs ──────────────────────────────────────────────
# Confirmed working (tested March 2026)
RETAILER_FEEDS = {
    "Asda":        "https://storelocator.asda.com/fuel_prices_data.json",
    "Tesco":       "https://www.tesco.com/fuel_prices/fuel_prices_data.json",
    "BP":          "https://www.bp.com/en_gb/united-kingdom/home/fuelprices/fuel_prices_data.json",
    "Shell":       "https://www.shell.co.uk/fuel-prices-data.html",
    "Sainsburys":  "https://api.sainsburys.co.uk/v1/exports/latest/fuel_prices_data.json",
    "Morrisons":   "https://www.morrisons.com/fuel-prices/fuel.json",
    "Esso":        "https://fuelprices.esso.co.uk/latestdata.json",
    "MFG":         "https://fuel.motorfuelgroup.com/fuel_prices_data.json",
    "Jet":         "https://jetlocal.co.uk/fuel_prices_data.json",
    "Applegreen":  "https://applegreenstores.com/fuel-prices/data.json",
    "Rontec":      "https://www.rontec-servicestations.co.uk/fuel-prices/data/fuel_prices_data.json",
    "Moto":        "https://www.moto-way.com/fuel-price/fuel_prices.json",
    "SGN":         "https://www.sgnretail.uk/files/data/SGN_daily_fuel_prices.json",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# ── Geocoding ─────────────────────────────────────────────────────────────────

_postcode_cache: dict = {}

def postcode_to_latlon(postcode: str) -> Optional[tuple]:
    """Convert a UK postcode to (lat, lon) using postcodes.io. Cached indefinitely."""
    postcode = postcode.strip().replace(" ", "").upper()
    if postcode in _postcode_cache:
        return _postcode_cache[postcode]
    try:
        resp = requests.get(
            f"https://api.postcodes.io/postcodes/{postcode}",
            timeout=5, headers=HEADERS
        )
        data = resp.json()
        if data.get("status") == 200:
            r = data["result"]
            result = (r["latitude"], r["longitude"])
            _postcode_cache[postcode] = result
            return result
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


# ── Weather ───────────────────────────────────────────────────────────────────

WEATHER_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Foggy", 48: "Icy fog", 51: "Light drizzle", 53: "Drizzle",
    55: "Heavy drizzle", 61: "Light rain", 63: "Rain", 65: "Heavy rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow", 80: "Light showers",
    81: "Showers", 82: "Heavy showers", 95: "Thunderstorm",
}

def get_weather(lat: float, lon: float) -> str:
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,weathercode,windspeed_10m"
            f"&timezone=Europe/London"
        )
        r = requests.get(url, timeout=5)
        c = r.json()["current"]
        temp = round(c["temperature_2m"])
        desc = WEATHER_CODES.get(c["weathercode"], "")
        wind = round(c["windspeed_10m"])
        return f"{temp}°C {desc}, Wind {wind}km/h"
    except Exception:
        return ""


# ── Nearby Amenities ──────────────────────────────────────────────────────────

def _places_nearby(lat: float, lon: float, radius_m: int, place_type: str, api_key: str) -> list:
    """Fetch places from Google Places API with ratings."""
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lon}",
        "radius": radius_m,
        "type": place_type,
        "key": api_key,
    }
    resp = requests.get(url, params=params, timeout=6)
    results = resp.json().get("results", [])
    places = []
    for r in results:
        name = r.get("name")
        loc = r.get("geometry", {}).get("location", {})
        plat, plon = loc.get("lat"), loc.get("lng")
        if not name or plat is None:
            continue
        dist_mi = haversine_km(lat, lon, plat, plon) / 1.60934
        rating = r.get("rating")
        n_ratings = r.get("user_ratings_total", 0)
        rating_str = f" {rating}★({n_ratings})" if rating else ""
        places.append({"name": name, "dist_mi": dist_mi, "rating": rating_str})
    places.sort(key=lambda x: x["dist_mi"])
    return places[:5]


def fetch_nearby_amenities(lat: float, lon: float, radius_km: float = 8.0) -> dict:
    """Fetch nearby supermarkets and cafes. Uses Google Places if API key set, else OSM."""
    radius_m = int(radius_km * 1000)
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")

    if api_key:
        try:
            supermarkets = _places_nearby(lat, lon, radius_m, "supermarket", api_key)
            cafes        = _places_nearby(lat, lon, radius_m, "cafe", api_key)
            return {"supermarkets": supermarkets, "cafes": cafes}
        except Exception:
            pass  # fall through to OSM

    # OSM fallback
    query = f"""
[out:json][timeout:5];
(
  node["shop"="supermarket"](around:{radius_m},{lat},{lon});
  node["amenity"="supermarket"](around:{radius_m},{lat},{lon});
  node["amenity"="cafe"](around:{radius_m},{lat},{lon});
  node["amenity"="fast_food"]["brand"~"Costa|Starbucks|Pret|Greggs|Caffe Nero|Coffee#1|Esquires|Nero",i](around:{radius_m},{lat},{lon});
);
out body 40;
"""
    try:
        elements = _overpass(query)
        supermarkets, cafes = [], []
        for e in elements:
            tags = e.get("tags", {})
            name = tags.get("name")
            if not name:
                continue
            elat, elon = e.get("lat"), e.get("lon")
            if elat is None or elon is None:
                continue
            dist_mi = haversine_km(lat, lon, elat, elon) / 1.60934
            rating = tags.get("stars") or tags.get("rating") or tags.get("michelin:stars")
            rating_str = f" ({rating}★)" if rating else ""
            entry = {"name": name, "dist_mi": dist_mi, "rating": rating_str}
            if tags.get("shop") == "supermarket" or tags.get("amenity") == "supermarket":
                supermarkets.append(entry)
            elif tags.get("amenity") in ("cafe", "fast_food"):
                cafes.append(entry)
        supermarkets.sort(key=lambda x: x["dist_mi"])
        cafes.sort(key=lambda x: x["dist_mi"])
        return {"supermarkets": supermarkets[:5], "cafes": cafes[:5]}
    except Exception:
        return {"supermarkets": [], "cafes": []}


import re as _re
import concurrent.futures as _cf

FSA_API = "https://api.ratings.food.gov.uk/Establishments"
FSA_HEADERS = {"x-api-version": "2", "User-Agent": "FuelWatchUK/1.0"}

def _norm(name: str) -> str:
    n = name.lower()
    n = _re.sub(r"\b(the|pub|bar|cafe|coffee|restaurant|inn|arms|head|house|tavern)\b", "", n)
    n = _re.sub(r"[^a-z0-9 ]", "", n)
    return " ".join(n.split())

def fetch_fsa_ratings(lat: float, lon: float, radius_km: float = 2.5) -> dict:
    """Return {fhrs_id: rating, norm_name: rating} for food establishments near lat/lon."""
    ratings = {}
    radius_miles = radius_km / 1.60934
    for btype in (7843, 1, 7844):
        try:
            r = requests.get(FSA_API, params={
                "latitude": lat, "longitude": lon,
                "maxDistanceLimit": radius_miles,
                "businessTypeId": btype,
                "pageSize": 100,
            }, headers=FSA_HEADERS, timeout=8)
            if r.status_code != 200:
                continue
            for e in r.json().get("establishments", []):
                name = e.get("BusinessName", "")
                rating = e.get("RatingValue", "")
                fhrs_id = str(e.get("FHRSID", ""))
                if rating and rating.isdigit():
                    val = int(rating)
                    if fhrs_id:
                        ratings[fhrs_id] = val
                    if name:
                        ratings[_norm(name)] = val
        except Exception:
            continue
    return ratings

_BROWSER_UA = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"

def fetch_ofsted_rating(urn: str) -> str:
    """Return Ofsted grade by scraping the provider page. Cached via _local_cache indirectly."""
    try:
        r = requests.get(
            f"https://reports.ofsted.gov.uk/provider/ELS/{urn}",
            timeout=8,
            headers={"User-Agent": _BROWSER_UA, "Accept": "text/html"},
        )
        if r.status_code == 200:
            import re as _r
            m = _r.search(r"(Outstanding|Good|Requires improvement|Inadequate)", r.text)
            if m:
                return m.group(1)
    except Exception:
        pass
    return ""

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

def _overpass(query: str) -> list:
    """POST an Overpass query, trying multiple mirrors."""
    for url in OVERPASS_URLS:
        try:
            r = requests.post(url, data={"data": query}, timeout=30,
                              headers={"User-Agent": "FuelWatchUK/1.0"})
            if r.status_code == 200:
                data = r.json()
                elements = data.get("elements", [])
                print(f"Overpass OK ({url}): {len(elements)} elements")
                return elements
            print(f"Overpass {r.status_code} from {url}")
        except Exception as e:
            print(f"Overpass error {url}: {e}")
            continue
    return []


# Cache for local amenities keyed by (lat_rounded, lon_rounded)
_local_cache: dict = {}
_LOCAL_CACHE_TTL = 3600  # 1 hour

# Cache for house prices keyed by normalised postcode
_house_cache: dict = {}
_HOUSE_CACHE_TTL = 1800  # 30 minutes


def fetch_local_amenities(lat: float, lon: float, school_km: float = 5.0, pub_km: float = 3.0) -> dict:
    """Single Overpass query for schools, universities, pubs, bars and cafes.
    Results are cached for 1 hour per location to dramatically speed up the Area Report."""
    cache_key = (round(lat, 3), round(lon, 3))
    cached = _local_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < _LOCAL_CACHE_TTL:
        return cached["data"]

    school_m = int(school_km * 1000)
    pub_m    = int(pub_km * 1000)
    query = f"""
[out:json][timeout:25];
(
  node["amenity"="school"](around:{school_m},{lat},{lon});
  way["amenity"="school"](around:{school_m},{lat},{lon});
  node["amenity"="college"](around:{school_m},{lat},{lon});
  way["amenity"="college"](around:{school_m},{lat},{lon});
  node["amenity"="university"](around:{school_m},{lat},{lon});
  way["amenity"="university"](around:{school_m},{lat},{lon});
  node["amenity"="pub"](around:{pub_m},{lat},{lon});
  way["amenity"="pub"](around:{pub_m},{lat},{lon});
  node["amenity"="bar"](around:{pub_m},{lat},{lon});
  way["amenity"="bar"](around:{pub_m},{lat},{lon});
  node["amenity"="cafe"](around:{pub_m},{lat},{lon});
  way["amenity"="cafe"](around:{pub_m},{lat},{lon});
  node["amenity"="fast_food"]["brand"~"Costa|Starbucks|Pret|Greggs|Caffe Nero|Nero",i](around:{pub_m},{lat},{lon});
);
out center 200;
"""
    elements = _overpass(query)
    schools, universities, pubs, cafes = [], [], [], []

    for e in elements:
        tags   = e.get("tags", {})
        name   = tags.get("name")
        if not name:
            continue
        amenity = tags.get("amenity", "")
        # resolve lat/lon for both nodes and ways
        if e.get("type") == "way":
            c = e.get("center", {})
            elat, elon = c.get("lat"), c.get("lon")
        else:
            elat, elon = e.get("lat"), e.get("lon")
        if elat is None or elon is None:
            continue

        dist_mi = haversine_km(lat, lon, elat, elon) / 1.60934
        entry = {"name": name, "dist_mi": dist_mi}

        if amenity in ("school", "college"):
            entry["urn"] = tags.get("ref:edubase", "")
            schools.append(entry)
        elif amenity == "university":
            entry["urn"] = tags.get("ref:edubase", "")
            universities.append(entry)
        elif amenity in ("pub", "bar"):
            real_ale = tags.get("real_ale") == "yes"
            cuisine  = tags.get("cuisine", "")
            entry["note"]    = "Real ale" if real_ale else ("Gastropub" if cuisine else "")
            entry["fhrs_id"] = tags.get("fhrs:id", "")
            pubs.append(entry)
        elif amenity in ("cafe", "fast_food"):
            entry["fhrs_id"] = tags.get("fhrs:id", "")
            cafes.append(entry)

    schools.sort(key=lambda x: x["dist_mi"])
    universities.sort(key=lambda x: x["dist_mi"])
    pubs.sort(key=lambda x: x["dist_mi"])
    cafes.sort(key=lambda x: x["dist_mi"])

    # Parallel enrichment: FSA hygiene ratings + Ofsted ratings
    def _enrich_fsa(item, fsa):
        fhrs_id = item.pop("fhrs_id", "")
        rating = fsa.get(fhrs_id) if fhrs_id else None
        if rating is None:
            key = _norm(item["name"])
            rating = fsa.get(key)
            if rating is None:
                for fkey, fval in fsa.items():
                    if key and fkey and len(key) > 3 and (key in fkey or fkey in key):
                        rating = fval
                        break
        if rating is not None:
            stars = "★" * rating + "☆" * (5 - rating)
            item["hygiene"] = f"Hygiene {stars} {rating}/5"
        return item

    try:
        with _cf.ThreadPoolExecutor(max_workers=6) as pool:
            fsa_future = pool.submit(fetch_fsa_ratings, lat, lon, pub_km)

            # Ofsted lookups for schools with URN
            ofsted_futures = {
                i: pool.submit(fetch_ofsted_rating, s["urn"])
                for i, s in enumerate(schools[:10]) if s.get("urn")
            }

            fsa = fsa_future.result(timeout=10)
            pubs  = [_enrich_fsa(p, fsa) for p in pubs]
            cafes = [_enrich_fsa(c, fsa) for c in cafes]

            for i, fut in ofsted_futures.items():
                grade = fut.result(timeout=6)
                if grade:
                    schools[i]["ofsted"] = grade
    except Exception:
        pass

    # Clean up internal fields not needed by frontend
    for s in schools + universities:
        s.pop("urn", None)

    result = {
        "schools":      schools[:10],
        "universities": universities[:3],
        "pubs":         pubs[:8],
        "cafes":        cafes[:6],
    }
    _local_cache[cache_key] = {"ts": time.time(), "data": result}
    return result


# Kept for backward compatibility with the SMS service
def fetch_nearby_schools(lat: float, lon: float, radius_km: float = 5.0) -> dict:
    data = fetch_local_amenities(lat, lon, school_km=radius_km)
    return {"schools": data["schools"], "universities": data["universities"]}


def fetch_nearby_pubs(lat: float, lon: float, radius_km: float = 2.5) -> list:
    data = fetch_local_amenities(lat, lon, pub_km=radius_km)
    return data["pubs"]


def _format_postcode(postcode: str) -> str:
    """Ensure postcode has a space: KT160DA -> KT16 0DA."""
    pc = postcode.strip().upper().replace(" ", "")
    return f"{pc[:-3]} {pc[-3:]}" if len(pc) >= 5 else pc


def _parse_lr_items(items: list) -> dict:
    """Parse Land Registry transaction items into a price summary dict."""
    buckets = {}
    for item in items:
        pt_obj = item.get("propertyType", {})
        labels = pt_obj.get("prefLabel", []) if isinstance(pt_obj, dict) else []
        pt = labels[0].get("_value", "").capitalize() if labels else ""
        price = item.get("pricePaid")
        date  = item.get("transactionDate", "")
        if pt and price:
            buckets.setdefault(pt, []).append({"price": int(price), "date": date})
    summary = {}
    for pt, entries in buckets.items():
        prices = [e["price"] for e in entries]
        summary[pt] = {
            "avg":    round(sum(prices) / len(prices) / 1000) * 1000,
            "latest": entries[0]["date"],
            "count":  len(prices),
        }
    return summary


def _get_postcode_info(postcode: str) -> dict:
    """Return admin_district and outward code for a postcode via postcodes.io."""
    pc = postcode.strip().replace(" ", "").upper()
    try:
        r = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=5, headers=HEADERS)
        res = r.json().get("result", {})
        return {
            "admin_district": res.get("admin_district", "").upper(),
            "outward": res.get("outcode", pc[:-3]),
        }
    except Exception:
        return {"admin_district": "", "outward": pc[:-3]}


def fetch_house_prices(postcode: str) -> dict:
    """Fetch last 3 years of sold prices from Land Registry for any postcode.
    Falls back to local authority district if the unit postcode has too few sales."""
    from datetime import date, timedelta, datetime as dt
    cache_key = postcode.strip().upper().replace(" ", "")
    cached = _house_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < _HOUSE_CACHE_TTL:
        return cached["data"]
    cutoff = date.today() - timedelta(days=3*365)
    pc_formatted = _format_postcode(postcode)
    pc_enc = pc_formatted.replace(" ", "%20")

    def _fetch(param_name, param_value):
        url = (
            f"https://landregistry.data.gov.uk/data/ppi/transaction-record.json"
            f"?{param_name}={param_value}"
            f"&_pageSize=100&_sort=-transactionDate"
        )
        try:
            resp = requests.get(url, timeout=10, headers=HEADERS)
            items = resp.json().get("result", {}).get("items", [])
            # Filter client-side to last 3 years
            filtered = []
            for item in items:
                date_str = item.get("transactionDate", "")
                try:
                    sale_date = dt.strptime(date_str, "%a, %d %b %Y").date()
                    if sale_date >= cutoff:
                        filtered.append(item)
                except Exception:
                    filtered.append(item)  # include if date unparseable
            return filtered
        except Exception:
            return []

    # Try exact postcode first
    items = _fetch("propertyAddress.postcode", pc_enc)
    scope = pc_formatted

    # Fall back to local authority district if fewer than 5 recent sales
    if len(items) < 5:
        info = _get_postcode_info(postcode)
        admin = info["admin_district"]
        if admin:
            fallback_items = _fetch("propertyAddress.district", admin.replace(" ", "%20"))
            if fallback_items:
                items = fallback_items
                scope = admin.title()

    summary = _parse_lr_items(items)
    for v in summary.values():
        v["scope"] = scope
    _house_cache[cache_key] = {"ts": time.time(), "data": summary}
    return summary


# ── Search ────────────────────────────────────────────────────────────────────

def search_near_postcode(postcode: str, fuel: str = "petrol",
                         radius_miles: float = 5.0, top_n: int = 10,
                         retailer: str = None):
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
        if retailer and retailer.lower() not in s.get("brand", "").lower():
            continue
        dist_km = haversine_km(lat, lon, s["lat"], s["lon"])
        if dist_km <= radius_km:
            nearby.append({**s, "distance_miles": dist_km / 1.60934, "price": price})

    if not nearby:
        retailer_msg = f" {retailer.title()}" if retailer else ""
        print(f"No{retailer_msg} {fuel} stations found within {radius_miles} miles of {postcode.upper()}.")
        print(f"Try a larger radius, e.g.: python3 search.py {postcode} {fuel} 10")
        return

    nearby.sort(key=lambda x: (x["price"], x["distance_miles"]))
    cheapest_price = nearby[0]["price"]
    avg_price = sum(s["price"] for s in nearby) / len(nearby)

    # ── Header: Today / Date / Time / Weather ─────────────────────────────────
    now = datetime.now()
    weather = get_weather(lat, lon)
    print(f"\n{'='*62}")
    print(f"  Today  {now.strftime('%d %b %Y')}  {now.strftime('%H:%M')}")
    if weather:
        print(f"  {weather}")
    print(f"{'='*62}\n")

    # ── Petrol Prices ─────────────────────────────────────────────────────────
    fuel_label = "Petrol (E10)" if fuel == "petrol" else "Diesel (B7)"
    retailer_label = f" — {retailer.title()}" if retailer else ""
    print(f"  {'─'*58}")
    print(f"  {fuel_label}{retailer_label} near {postcode.upper()}  |  Radius: {radius_miles:.0f} miles  |  {len(nearby)} stations")
    print(f"  {'─'*58}")
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

    # ── Nearby Amenities ──────────────────────────────────────────────────────
    amenities = fetch_nearby_amenities(lat, lon, radius_km)

    if amenities["supermarkets"]:
        print(f"\n  {'─'*58}")
        print(f"  Supermarkets nearby")
        print(f"  {'─'*58}")
        for s in amenities["supermarkets"]:
            print(f"    • {s['name']}{s['rating']} ({s['dist_mi']:.1f}mi)")

    if amenities["cafes"]:
        print(f"\n  {'─'*58}")
        print(f"  Coffee nearby")
        print(f"  {'─'*58}")
        for c in amenities["cafes"]:
            print(f"    • {c['name']}{c['rating']} ({c['dist_mi']:.1f}mi)")

    print()
    return nearby


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("\nUsage:   python3 search.py <POSTCODE> [petrol|diesel] [radius_miles] [retailer]")
        print("Example: python3 search.py SW1A1AA petrol 5")
        print("         python3 search.py SW1A1AA petrol 5 tesco\n")
        sys.exit(1)

    postcode     = sys.argv[1]
    fuel         = sys.argv[2].lower() if len(sys.argv) > 2 else "petrol"
    radius_miles = float(sys.argv[3])  if len(sys.argv) > 3 else 5.0
    retailer     = sys.argv[4]         if len(sys.argv) > 4 else None

    if fuel not in ("petrol", "diesel"):
        print("Fuel must be 'petrol' or 'diesel'")
        sys.exit(1)

    search_near_postcode(postcode, fuel=fuel, radius_miles=radius_miles, retailer=retailer)


if __name__ == "__main__":
    main()
