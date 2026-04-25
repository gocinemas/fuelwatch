#!/usr/bin/env python3
"""
FuelWatch UK — Postcode Search
================================
Find cheapest fuel stations near any UK postcode.

Data: CMA-mandated retailer price feeds (updated daily, no API key needed)
Geocoding: postcodes.io (free, no API key needed)
"""

import json
import math
import os
import requests
import sys
import time
from datetime import datetime
from typing import Optional

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyDSJyUiYSCADhDdtBcOFI_iF-b-HOlUEq8")

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
import threading as _threading
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


def fetch_local_amenities(lat: float, lon: float, school_km: float = 3.0, pub_km: float = 1.5) -> dict:
    """Single Overpass query for schools, universities, pubs, bars and cafes.
    Results are cached for 1 hour per location to dramatically speed up the Area Report."""
    cache_key = (round(lat, 3), round(lon, 3))
    cached = _local_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < _LOCAL_CACHE_TTL:
        return cached["data"]

    school_m = int(school_km * 1000)
    pub_m    = int(pub_km * 1000)
    query = f"""
[out:json][timeout:20];
(
  node["amenity"="school"](around:{school_m},{lat},{lon});
  way["amenity"="school"](around:{school_m},{lat},{lon});
  node["amenity"="university"](around:{school_m},{lat},{lon});
  way["amenity"="university"](around:{school_m},{lat},{lon});
  node["amenity"="pub"](around:{pub_m},{lat},{lon});
  way["amenity"="pub"](around:{pub_m},{lat},{lon});
  node["amenity"="cafe"](around:{pub_m},{lat},{lon});
  way["amenity"="cafe"](around:{pub_m},{lat},{lon});
  node["amenity"="fast_food"]["brand"~"Costa|Starbucks|Pret|Greggs|Caffe Nero|Nero",i](around:{pub_m},{lat},{lon});
);
out center 80;
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
            entry["website"] = tags.get("website", tags.get("contact:website", ""))
            entry["phone"]   = tags.get("phone", tags.get("contact:phone", ""))
            entry["lat"]     = elat
            entry["lon"]     = elon
            pubs.append(entry)
        elif amenity in ("cafe", "fast_food"):
            entry["fhrs_id"] = tags.get("fhrs:id", "")
            entry["website"] = tags.get("website", tags.get("contact:website", ""))
            entry["phone"]   = tags.get("phone", tags.get("contact:phone", ""))
            entry["lat"]     = elat
            entry["lon"]     = elon
            cafes.append(entry)

    schools.sort(key=lambda x: x["dist_mi"])
    universities.sort(key=lambda x: x["dist_mi"])
    pubs.sort(key=lambda x: x["dist_mi"])
    cafes.sort(key=lambda x: x["dist_mi"])

    # Parallel: Ofsted for schools + Google ratings for pubs/cafes
    try:
        with _cf.ThreadPoolExecutor(max_workers=12) as pool:
            ofsted_futures = {
                i: pool.submit(fetch_ofsted_rating, s["urn"])
                for i, s in enumerate(schools[:10]) if s.get("urn")
            }
            pub_rating_futures = {
                i: pool.submit(_google_rating, p["name"], p["lat"], p["lon"])
                for i, p in enumerate(pubs[:8])
            }
            cafe_rating_futures = {
                i: pool.submit(_google_rating, c["name"], c["lat"], c["lon"])
                for i, c in enumerate(cafes[:6])
            }
            for i, fut in ofsted_futures.items():
                try:
                    grade = fut.result(timeout=6)
                    if grade:
                        schools[i]["ofsted"] = grade
                except Exception:
                    pass
            for i, fut in pub_rating_futures.items():
                try:
                    rating, total = fut.result(timeout=5)
                    if rating:
                        pubs[i]["google_rating"] = rating
                        pubs[i]["google_count"]  = total or 0
                except Exception:
                    pass
            for i, fut in cafe_rating_futures.items():
                try:
                    rating, total = fut.result(timeout=5)
                    if rating:
                        cafes[i]["google_rating"] = rating
                        cafes[i]["google_count"]  = total or 0
                except Exception:
                    pass
    except Exception:
        pass

    for p in pubs:
        p.pop("fhrs_id", None)
    for c in cafes:
        c.pop("fhrs_id", None)

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


# ── Google Places (New API) ───────────────────────────────────────────────────
_PLACES_URL = "https://places.googleapis.com/v1/places:searchText"

def _google_rating(name: str, lat: float = None, lon: float = None):
    """Return (rating, total_ratings) using the Places API (New)."""
    if not GOOGLE_API_KEY:
        return None, None
    body = {"textQuery": name}
    if lat is not None and lon is not None:
        body["locationBias"] = {
            "circle": {"center": {"latitude": lat, "longitude": lon}, "radius": 2000.0}
        }
    try:
        r = requests.post(
            _PLACES_URL,
            headers={
                "X-Goog-Api-Key":   GOOGLE_API_KEY,
                "X-Goog-FieldMask": "places.displayName,places.rating,places.userRatingCount",
                "Content-Type":     "application/json",
            },
            json=body,
            timeout=5,
        )
        places = r.json().get("places", [])
        if places:
            p = places[0]
            return p.get("rating"), p.get("userRatingCount")
    except Exception:
        pass
    return None, None


# ── Share price ───────────────────────────────────────────────────────────────

def _fetch_share_price(company: str) -> dict:
    """Fetch current price + 1-month daily closes.
    Tries Yahoo Finance first, falls back to Stooq CSV."""
    from datetime import datetime as _dt

    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    hdrs = {"User-Agent": ua, "Accept": "application/json"}

    # ── Step 1: resolve ticker via Yahoo Finance search ───────────────────────
    ticker, disp_name, exchange, currency = "", company, "", "USD"
    try:
        sr = requests.get(
            "https://query2.finance.yahoo.com/v1/finance/search",
            params={"q": company, "quotesCount": 5, "newsCount": 0},
            timeout=6, headers=hdrs,
        )
        quotes = sr.json().get("quotes", [])
        equity = [q for q in quotes if q.get("quoteType") == "EQUITY"]
        # Prefer UK/LSE listings (.L suffix or London exchange)
        uk = [q for q in equity if
              q.get("symbol", "").endswith(".L") or
              "london" in (q.get("exchDisp") or q.get("exchange") or "").lower()]
        pick = uk[0] if uk else (equity[0] if equity else (quotes[0] if quotes else None))
        if pick:
            ticker    = pick.get("symbol", "")
            disp_name = pick.get("shortname") or pick.get("longname") or company
            exchange  = pick.get("exchDisp") or pick.get("exchange", "")
            currency  = pick.get("currency", "GBp" if ticker.endswith(".L") else "USD")
    except Exception as e:
        print(f"[share_price] ticker lookup failed: {e}")

    if not ticker:
        return {}

    # ── Step 2a: try Yahoo Finance chart API ──────────────────────────────────
    prices, dates, meta = [], [], {}
    try:
        cr = requests.get(
            f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={"interval": "1d", "range": "1mo"},
            timeout=6, headers=hdrs,
        )
        result = cr.json().get("chart", {}).get("result", [None])[0]
        if result:
            meta      = result.get("meta", {})
            currency  = meta.get("currency", currency)
            timestamps = result.get("timestamp", [])
            closes    = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            pairs = [(t, round(c, 4)) for t, c in zip(timestamps, closes) if c is not None]
            if pairs:
                dates  = [_dt.fromtimestamp(t).strftime("%d %b") for t, _ in pairs]
                prices = [p for _, p in pairs]
    except Exception as e:
        print(f"[share_price] Yahoo chart failed: {e}")

    # ── Step 2b: fallback to Stooq CSV ────────────────────────────────────────
    if not prices:
        try:
            stooq_ticker = ticker.replace(".", "-")
            sc = requests.get(
                f"https://stooq.com/q/d/l/?s={stooq_ticker}&i=d",
                timeout=8, headers={"User-Agent": ua},
            )
            lines = [l for l in sc.text.strip().splitlines() if l and not l.startswith("Date")]
            lines = lines[-22:]  # ~1 month of trading days
            for line in lines:
                parts = line.split(",")
                if len(parts) >= 5:
                    try:
                        dates.append(_dt.strptime(parts[0], "%Y-%m-%d").strftime("%d %b"))
                        prices.append(round(float(parts[4]), 4))  # Close
                    except Exception:
                        pass
        except Exception as e:
            print(f"[share_price] Stooq failed: {e}")

    if not prices:
        return {}

    current     = prices[-1]
    month_start = prices[0]
    prev_close  = meta.get("previousClose") or meta.get("regularMarketPreviousClose") or prices[-2]
    symbol_char = {"USD": "$", "GBP": "£", "GBp": "p", "EUR": "€"}.get(currency, currency + " ")
    day_chg     = round((current - prev_close) / prev_close * 100, 2) if prev_close else 0
    month_chg   = round((current - month_start) / month_start * 100, 2)

    return {
        "ticker":     ticker,
        "name":       disp_name,
        "exchange":   exchange,
        "currency":   currency,
        "symbol":     symbol_char,
        "current":    current,
        "day_chg":    day_chg,
        "month_chg":  month_chg,
        "month_high": max(prices),
        "month_low":  min(prices),
        "prices":     prices,
        "dates":      dates,
    }


# ── Supabase L2 cache (survives restarts) ─────────────────────────────────────
# Requires table: CREATE TABLE ai_cache (key TEXT PRIMARY KEY, data JSONB NOT NULL, cached_at TIMESTAMPTZ DEFAULT NOW());
_SB_CACHE_TTL = 86400  # 24 h for brand; company uses same table with its own key

def _sb_cache_get(key: str):
    try:
        from supabase import create_client
        sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
        rows = sb.table("ai_cache").select("data,cached_at").eq("key", key).execute().data
        if not rows:
            return None
        import datetime as _datetime
        cached_at_str = rows[0]["cached_at"]
        cached_at = _datetime.datetime.fromisoformat(cached_at_str.replace("Z", "+00:00"))
        age = (_datetime.datetime.now(_datetime.timezone.utc) - cached_at).total_seconds()
        if age > _SB_CACHE_TTL:
            return None
        return rows[0]["data"]
    except Exception:
        return None

def _sb_cache_set(key: str, data: dict) -> None:
    try:
        from supabase import create_client
        sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
        sb.table("ai_cache").upsert({"key": key, "data": data, "cached_at": "now()"}).execute()
    except Exception:
        pass


# ── Brand research ────────────────────────────────────────────────────────────
_BRAND_CACHE: dict = {}
_BRAND_TTL = 3600
_BRAND_INFLIGHT: dict = {}
_BRAND_LOCK = _threading.Lock()

def _fetch_brand_ai(brand: str, extract: str) -> dict:
    """Ask Groq for timeline, campaigns, competitors, and key facts for a brand."""
    import json
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return {"timeline": [], "campaigns": [], "competitors": [], "facts": {}}
    prompt = f"""Return ONLY valid JSON for brand "{brand}". No markdown, no explanation.
{{"facts":{{"founded":"","hq":"","industry":"","employees":"","revenue":""}},"competitors":[{{"name":"","revenue":"","description":""}}],"campaigns":[{{"name":"","year":"","description":""}}],"timeline":[{{"year":"","title":"","description":""}}]}}
Rules: facts fill all 5 fields. competitors: 4 items each with revenue. campaigns: 4 items. timeline: 8-10 milestones oldest to newest each with title+description."""
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
                "max_tokens": 2500,
            },
            timeout=25,
        )
        if r.status_code != 200:
            print(f"[brand_ai] HTTP {r.status_code}: {r.text[:200]}")
            return {"timeline": [], "campaigns": [], "competitors": [], "facts": {}}
        content = r.json()["choices"][0]["message"]["content"].strip()
        print(f"[brand_ai] raw ({len(content)} chars): {content[:300]}")
        m = _re.search(r'\{.*\}', content, _re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception as je:
                print(f"[brand_ai] JSON parse error: {je} | snippet: {m.group(0)[:200]}")
        else:
            print(f"[brand_ai] no JSON object found in response")
    except Exception as e:
        print(f"[brand_ai] error: {e}")
    return {"timeline": [], "campaigns": [], "competitors": [], "facts": {}}


def _fetch_brand_ads(brand: str) -> list:
    """Search YouTube for brand ad videos — recent first, falling back to all-time."""
    key = os.environ.get("YOUTUBE_API_KEY", "")
    if not key:
        return []

    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    two_years_ago = (_dt.now(_tz.utc) - _td(days=730)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _search(extra_params: dict) -> list:
        q = f"{brand} advertisement OR commercial OR brand campaign"
        r = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={"q": q, "part": "snippet", "type": "video",
                    "maxResults": 6, "key": key, "relevanceLanguage": "en",
                    **extra_params},
            timeout=8,
        )
        if r.status_code != 200:
            return []
        out = []
        for item in r.json().get("items", []):
            vid_id  = item.get("id", {}).get("videoId", "")
            snippet = item.get("snippet", {})
            if not vid_id:
                continue
            pub  = snippet.get("publishedAt", "")
            year = pub[:4] if pub else ""
            out.append({
                "video_id":  vid_id,
                "title":     snippet.get("title", ""),
                "channel":   snippet.get("channelTitle", ""),
                "thumbnail": snippet.get("thumbnails", {}).get("medium", {}).get("url", ""),
                "url":       f"https://www.youtube.com/watch?v={vid_id}",
                "year":      year,
            })
        return out

    try:
        recent = _search({"order": "viewCount", "publishedAfter": two_years_ago})
        if len(recent) >= 2:
            return recent[:4]
        return _search({"order": "viewCount"})[:4]
    except Exception as e:
        print(f"[brand_ads] {e}")
        return []


def _fetch_wiki_images(wiki_title: str) -> list:
    """Fetch up to 6 real article photos from Wikipedia's media-list endpoint."""
    if not wiki_title:
        return []
    try:
        slug = requests.utils.quote(wiki_title.replace(" ", "_"))
        r = requests.get(
            f"https://en.wikipedia.org/api/rest_v1/page/media-list/{slug}",
            timeout=8, headers={"User-Agent": "Miru/1.0"},
        )
        if r.status_code != 200:
            return []
        _skip = {"logo", "flag", "icon", "seal", "signature", "map",
                 "coat_of_arms", "coa", "blank", "wordmark", "logotype"}
        photos = []
        for item in r.json().get("items", []):
            if item.get("type") != "image":
                continue
            title = (item.get("title") or "").lower()
            if title.endswith(".svg"):
                continue
            if any(w in title for w in _skip):
                continue
            # srcset has {src, scale} entries — pick 1x (first) for mobile-friendly size
            srcset = item.get("srcset", [])
            if not srcset:
                continue
            best = srcset[0].get("src", "")
            if not best:
                continue
            if best.startswith("//"):
                best = "https:" + best
            photos.append(best)
            if len(photos) >= 6:
                break
        return photos
    except Exception as e:
        print(f"[wiki_images] {e}")
        return []


def _fetch_brand_financials(brand: str) -> dict:
    """Fetch market cap, revenue, net income, margins from Yahoo Finance."""
    from datetime import datetime as _dt
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    hdrs = {"User-Agent": ua, "Accept": "application/json"}

    ticker = ""
    currency = "USD"
    try:
        sr = requests.get(
            "https://query2.finance.yahoo.com/v1/finance/search",
            params={"q": brand, "quotesCount": 5, "newsCount": 0},
            timeout=6, headers=hdrs,
        )
        quotes = sr.json().get("quotes", [])
        equity = [q for q in quotes if q.get("quoteType") == "EQUITY"]
        if equity:
            pick = equity[0]
            ticker   = pick.get("symbol", "")
            currency = pick.get("currency", "USD")
    except Exception:
        pass

    if not ticker:
        return {}

    def _fmt(val_dict, curr="$"):
        v = val_dict.get("raw") if isinstance(val_dict, dict) else val_dict
        if not v:
            return ""
        try:
            v = float(v)
        except Exception:
            return ""
        sym = {"USD": "$", "GBP": "£", "GBp": "£", "EUR": "€"}.get(currency, "$")
        if v >= 1e12: return f"{sym}{v/1e12:.2f}T"
        if v >= 1e9:  return f"{sym}{v/1e9:.1f}B"
        if v >= 1e6:  return f"{sym}{v/1e6:.0f}M"
        return f"{sym}{v:,.0f}"

    def _pct(val_dict):
        v = val_dict.get("raw") if isinstance(val_dict, dict) else val_dict
        if v is None: return ""
        try: return f"{round(float(v)*100, 1)}%"
        except Exception: return ""

    try:
        r = requests.get(
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}",
            params={"modules": "defaultKeyStatistics,summaryDetail,financialData"},
            timeout=8, headers=hdrs,
        )
        result = r.json().get("quoteSummary", {}).get("result", [None])[0] or {}
        fin    = result.get("financialData", {})
        stats  = result.get("defaultKeyStatistics", {})
        summ   = result.get("summaryDetail", {})
        return {
            "ticker":         ticker,
            "market_cap":     _fmt(summ.get("marketCap", {})),
            "total_revenue":  _fmt(fin.get("totalRevenue", {})),
            "net_income":     _fmt(fin.get("netIncomeToCommon") or stats.get("netIncomeToCommon") or {}),
            "profit_margin":  _pct(fin.get("profitMargins", {})),
            "gross_margin":   _pct(fin.get("grossMargins", {})),
            "revenue_growth": _pct(fin.get("revenueGrowth", {})),
            "ebitda":         _fmt(fin.get("ebitda", {})),
        }
    except Exception as e:
        print(f"[brand_financials] {e}")
        return {"ticker": ticker}


def fetch_brand_data(brand: str) -> dict:
    original = brand.strip()
    canonical = original

    # Canonicalise via Groq before cache lookup (handles misspellings like "Nkie" → "Nike")
    try:
        groq_key = os.environ.get("GROQ_API_KEY", "")
        if groq_key:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                json={"model": "llama-3.1-8b-instant",
                      "messages": [{"role": "user", "content":
                          f'The user searched for brand/company: "{original}".\n'
                          'Return ONLY the canonical well-known brand or company name (fix spelling, '
                          'expand abbreviations). If already correct, return it unchanged. '
                          'Return ONLY the name, nothing else.'}],
                      "max_tokens": 40, "temperature": 0.1},
                timeout=6,
            )
            resolved = r.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
            if resolved and len(resolved) < 80:
                canonical = resolved
    except Exception:
        pass

    suggested = canonical if canonical.lower() != original.lower() else ""
    brand = canonical

    cache_key = brand.strip().lower() + "|brandv16"

    # L1: in-memory
    cached = _BRAND_CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _BRAND_TTL:
        data = dict(cached["data"])
        data["suggested_name"] = suggested
        return data

    # Stampede protection: if another thread is already fetching, wait for it
    with _BRAND_LOCK:
        # Re-check after acquiring lock — another thread may have just finished
        cached = _BRAND_CACHE.get(cache_key)
        if cached and time.time() - cached["ts"] < _BRAND_TTL:
            return cached["data"]
        if cache_key in _BRAND_INFLIGHT:
            ev = _BRAND_INFLIGHT[cache_key]
        else:
            ev = _threading.Event()
            _BRAND_INFLIGHT[cache_key] = ev
            ev = None  # this thread is the fetcher

    if ev is not None:
        ev.wait(timeout=25)
        cached = _BRAND_CACHE.get(cache_key)
        return cached["data"] if cached else {}

    try:
        # L2: Supabase persistent cache — skip if AI data is absent
        sb_data = _sb_cache_get("brand:" + cache_key)
        if sb_data and (sb_data.get("timeline") or sb_data.get("competitors")):
            _BRAND_CACHE[cache_key] = {"ts": time.time(), "data": sb_data}
            return {**sb_data, "suggested_name": suggested}

        fetch_start = time.time()

        with _cf.ThreadPoolExecutor(max_workers=6) as pool:
            wiki_f = pool.submit(_fetch_wikipedia, brand)
            news_f = pool.submit(_fetch_news, brand, "brand OR campaign OR advertising OR revenue OR launch", 6)
            ads_f  = pool.submit(_fetch_brand_ads, brand)
            fin_f  = pool.submit(_fetch_brand_financials, brand)
            ai_f   = pool.submit(_fetch_brand_ai, brand, "")

            wiki = {}
            try: wiki = wiki_f.result(timeout=10) or {}
            except Exception: pass

            news = []
            try: news = news_f.result(timeout=8) or []
            except Exception: pass

            ads = []
            try: ads = ads_f.result(timeout=8) or []
            except Exception: pass

            financials = {}
            try: financials = fin_f.result(timeout=8) or {}
            except Exception: pass

            # Give AI the remainder of a 27s wall-clock budget from fetch start
            ai_budget = max(27 - (time.time() - fetch_start), 3)
            ai = {}
            try:
                ai = ai_f.result(timeout=ai_budget) or {}
            except Exception as e:
                print(f"[brand_ai] timeout/error after {time.time()-fetch_start:.1f}s: {e}")
            print(f"[brand_ai] {brand}: tl={len(ai.get('timeline',[]))} rivals={len(ai.get('competitors',[]))} facts={bool(ai.get('facts'))} elapsed={time.time()-fetch_start:.1f}s")

        ai_facts = ai.get("facts", {}) or {}

        def _val(wiki_key):
            v = wiki.get(wiki_key, "")
            return v if v else ai_facts.get(wiki_key, "")

        result = {
            "name":        brand,
            "suggested_name": suggested,
            "description": wiki.get("description", ""),
            "extract":     wiki.get("extract", ""),
            "founded":     _val("founded"),
            "hq":          _val("hq"),
            "revenue":     _val("revenue"),
            "employees":   _val("employees"),
            "industry":    _val("industry"),
            "wiki_url":    wiki.get("wiki_url", ""),
            "domain":      wiki.get("domain", ""),
            "thumbnail":   wiki.get("thumbnail", ""),
            "timeline":    ai.get("timeline", []),
            "campaigns":   ai.get("campaigns", []),
            "competitors": ai.get("competitors", []),
            "ads":         ads,
            "financials":  financials,
            "news":        news,
        }
        # Only cache if AI data came back — never lock in empty timeline/rivals
        ai_ok = bool(result["timeline"] or result["competitors"])
        if ai_ok:
            _BRAND_CACHE[cache_key] = {"ts": time.time(), "data": result}
            _sb_cache_set("brand:" + cache_key, result)
        else:
            print(f"[brand_ai] {brand}: AI empty — skipping all caches so next request retries")
        return result
    finally:
        with _BRAND_LOCK:
            ev = _BRAND_INFLIGHT.pop(cache_key, None)
        if ev:
            ev.set()


# ── Company research ──────────────────────────────────────────────────────────
_COMPANY_CACHE: dict = {}
_COMPANY_TTL = 3600
_COMPANY_VER = "v13"
_COMPANY_INFLIGHT: dict = {}
_COMPANY_LOCK = _threading.Lock()

def _fetch_news(company: str, extra: str = "", limit: int = 6) -> list:
    """Fetch recent news via Google News RSS. Pass extra to narrow the search."""
    try:
        import xml.etree.ElementTree as ET
        q = f"{company} {extra}".strip()
        r = requests.get(
            "https://news.google.com/rss/search",
            params={"q": q, "hl": "en-GB", "gl": "GB", "ceid": "GB:en"},
            timeout=8, headers={"User-Agent": "Mozilla/5.0"},
        )
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
        out = []
        for item in root.findall(".//item")[:limit]:
            title = item.findtext("title", "")
            link  = item.findtext("link", "")
            pub   = item.findtext("pubDate", "")[:16].strip()
            src_el = item.find("{https://news.google.com/rss}source")
            source = src_el.text if src_el is not None else _re.search(r" - ([^-]+)$", title)
            if isinstance(source, type(None)):
                source = ""
            elif not isinstance(source, str):
                source = source.group(1) if source else ""
            clean_title = _re.sub(r"\s+-\s+[^-]+$", "", title).strip()
            out.append({"title": clean_title, "source": source, "date": pub, "url": link})
        return out
    except Exception:
        return []

def _job_signals(jobs: list) -> dict:
    """Derive hiring signals from job listings."""
    if not jobs:
        return {}
    dept_keywords = {
        "Engineering & Tech":  ["engineer", "developer", "architect", "data", "ml", "ai", "backend", "frontend", "platform", "devops", "security", "cloud"],
        "Product & Design":    ["product", "designer", "ux", "ui", "researcher", "design"],
        "Sales & Marketing":   ["sales", "marketing", "growth", "partnerships", "account", "revenue", "brand"],
        "Finance & Legal":     ["finance", "legal", "compliance", "tax", "audit", "accounting", "risk"],
        "Operations & People": ["operations", "hr", "people", "recruiting", "talent", "support", "operations", "customer"],
    }
    depts = {}
    remote_count = 0
    for j in jobs:
        t = j.get("title", "").lower()
        matched = False
        for dept, kws in dept_keywords.items():
            if any(kw in t for kw in kws):
                depts[dept] = depts.get(dept, 0) + 1
                matched = True
                break
        if not matched:
            depts["Other"] = depts.get("Other", 0) + 1
        loc = j.get("location", "").lower()
        if "remote" in loc:
            remote_count += 1
    top_depts = sorted(depts.items(), key=lambda x: -x[1])[:4]
    return {
        "total":       len(jobs),
        "departments": [{"name": d, "count": c} for d, c in top_depts],
        "remote":      remote_count,
    }

_AI_KEYWORDS = [
    "ai", "artificial intelligence", "machine learning", "ml engineer", "llm",
    "deep learning", "nlp", "data scientist", "computer vision", "generative",
    "foundation model", "prompt", "reinforcement learning",
]

def _ai_job_signals(jobs: list) -> dict:
    """Count AI/ML-specific roles from job listings."""
    if not jobs:
        return {}
    ai_roles = []
    for j in jobs:
        t = j.get("title", "").lower()
        if any(kw in t for kw in _AI_KEYWORDS):
            ai_roles.append(j.get("title", ""))
    pct = round(len(ai_roles) / len(jobs) * 100) if jobs else 0
    return {
        "ai_role_count": len(ai_roles),
        "total_jobs":    len(jobs),
        "ai_pct":        pct,
        "sample_roles":  ai_roles[:5],
    }


def _fetch_wikipedia(company: str) -> dict:
    """Fetch company overview from Wikipedia summary API with search fallback."""
    ua = {"User-Agent": "Miru/1.0 (company research tool)"}

    def _summary(title: str) -> dict:
        slug = requests.utils.quote(title.replace(" ", "_"))
        r = requests.get(
            f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}",
            timeout=8, headers=ua,
        )
        if r.status_code != 200:
            return {}
        d = r.json()
        if d.get("type") in ("disambiguation", "no-extract"):
            return {}
        extract = (d.get("extract") or "")[:600]
        if not extract:
            return {}
        return {
            "_wiki_title": d.get("title", title),  # actual resolved article title
            "description": d.get("description", ""),
            "extract":     extract,
            "wiki_url":    d.get("content_urls", {}).get("desktop", {}).get("page", ""),
            "thumbnail":   (d.get("thumbnail") or d.get("originalimage") or {}).get("source", ""),
        }

    # Step 1: try direct title match
    result = _summary(company)

    # Step 2: if that fails, search Wikipedia for the best matching article
    if not result:
        try:
            sr = requests.get(
                "https://en.wikipedia.org/w/api.php",
                params={"action": "query", "list": "search", "srsearch": company,
                        "srlimit": 3, "format": "json"},
                timeout=6, headers=ua,
            )
            hits = sr.json().get("query", {}).get("search", [])
            for hit in hits:
                result = _summary(hit["title"])
                if result:
                    break
        except Exception:
            pass

    if not result:
        return {}

    # Step 3: fetch wikitext using the ACTUAL resolved title (not the original search term),
    # so we get the right infobox even when the query was a redirect or disambiguation.
    wiki_title = result.get("_wiki_title", company)
    employees = hq = industry = founded = revenue = ""
    try:
        r2 = requests.get("https://en.wikipedia.org/w/api.php", params={
            "action": "query", "titles": wiki_title, "prop": "revisions",
            "rvprop": "content", "rvslots": "main", "rvsection": 0, "format": "json",
            "redirects": 1,
        }, timeout=6, headers=ua)
        pages = r2.json().get("query", {}).get("pages", {})
        page = list(pages.values())[0] if pages else {}
        revs = page.get("revisions", [])
        wikitext = ""
        if revs:
            slots = revs[0].get("slots", {})
            wikitext = slots.get("main", {}).get("*", "") or revs[0].get("*", "")

        def _field(key):
            m = _re.search(rf'\|\s*{key}\s*=\s*([^\n]+)', wikitext, _re.IGNORECASE)
            if not m: return ""
            v = m.group(1).strip()
            # Strip <ref>...</ref> blocks first
            v = _re.sub(r'<ref[^>]*>.*?</ref>', '', v, flags=_re.DOTALL)
            v = _re.sub(r'<ref[^/]*/>', '', v)
            # Normalise non-breaking spaces
            v = v.replace('&nbsp;', ' ')
            # Strip named template params (e.g. |class=nowrap, |style=..., |abbr=on)
            # so positional params like [[Footwear]] are captured correctly
            v = _re.sub(r'\|[a-zA-Z][a-zA-Z0-9_-]*\s*=\s*[^|{}]*', '', v)
            # Strip simple no-param templates: {{increase}}, {{decrease}}, etc.
            v = _re.sub(r'\{\{[^|{}]{1,30}\}\}', '', v)
            # Extract first positional param from templates: {{US$|60.1 billion}} → 60.1 billion
            v = _re.sub(r'\{\{[^|{}]+\|([^|{}]+)(?:\|[^{}]*)?\}\}', r'\1', v)
            # Drop any remaining templates
            v = _re.sub(r'\{\{[^{}]*\}\}', '', v)
            # Unwrap wiki links: [[London|London, UK]] → London, UK
            v = _re.sub(r'\[\[(?:[^\]|]*\|)?([^\]]*)\]\]', r'\1', v)
            # Strip HTML tags and reference markers
            v = _re.sub(r'<[^>]+>|\[\d+\]', '', v)
            return " ".join(v.split())[:120]

        employees = _field("num_employees") or _field("employees")
        hq        = (_field("hq_location_city") or _field("headquarters") or
                     _field("location_city") or _field("location") or
                     _field("origin") or _field("country"))
        industry  = _field("industry") or _field("type") or _field("genre")
        # Only use dedicated brands/subsidiaries fields — not products (which gives service descriptions)
        brands_raw = _field("brands") or _field("subsidiaries")
        _service_words = {"account", "card", "trading", "payment", "transfer", "loan",
                          "insurance", "banking", "service", "plan", "fee", "deposit",
                          "withdrawal", "exchange", "invest", "crypto", "saving"}
        brands = []
        if brands_raw:
            parts = _re.split(r'[,\n•]+', brands_raw)
            for p in parts:
                p = p.strip()
                if not p or len(p) < 2:
                    continue
                # Skip items that look like service descriptions (mostly lowercase or contain service words)
                words = p.split()
                if any(w.lower() in _service_words for w in words):
                    continue
                if not words[0][0].isupper():
                    continue
                brands.append(p[:40])
                if len(brands) >= 6:
                    break

        mf = _re.search(
            r'\|\s*(?:founded|foundation|introduced|launch_date|inception|start_date)\s*=\s*([^\n]+)',
            wikitext, _re.IGNORECASE)
        if mf:
            ym = _re.search(r'\b(1[5-9]\d{2}|20\d{2})\b', mf.group(1))
            founded = ym.group(1) if ym else ""

        rev_raw = _field("revenue")
        if rev_raw:
            vm = _re.search(r'[£$€]?\s*[\d,\.]+\s*(?:billion|million|trillion)', rev_raw, _re.IGNORECASE)
            revenue = vm.group(0).strip() if vm else rev_raw[:60]

        # Extract domain from website infobox field for logo lookup
        domain = ""
        website_raw = _field("website") or _field("url") or ""
        if website_raw:
            dm = _re.search(r'(?:https?://)?(?:www\.)?([a-zA-Z0-9][a-zA-Z0-9\-]+\.[a-zA-Z]{2,})', website_raw)
            if dm:
                domain = dm.group(1).lower()
    except Exception:
        domain = ""

    return {**result, "employees": employees, "revenue": revenue,
            "founded": founded, "hq": hq, "industry": industry, "brands": brands,
            "domain": domain, "thumbnail": result.get("thumbnail", "")}

def _co_slugs(name: str) -> list:
    """Generate ATS slug candidates from a company name."""
    s = name.lower().strip()
    s = _re.sub(r"\s+(uk|ltd|plc|inc|corp|group|the|&|and)\s*$", "", s).strip()
    plain   = _re.sub(r"[^a-z0-9]", "", s)
    hyphen  = _re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    # deduplicate while preserving order
    seen, out = set(), []
    for slug in [plain, hyphen, plain.replace("-", "")]:
        if slug and slug not in seen:
            seen.add(slug)
            out.append(slug)
    return out

def _fetch_greenhouse(slugs: list) -> list:
    for slug in slugs:
        try:
            r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs", timeout=6, headers=HEADERS)
            if r.status_code == 200:
                jobs = r.json().get("jobs", [])
                if jobs:
                    return [{"title": j.get("title",""), "location": j.get("location",{}).get("name",""), "url": j.get("absolute_url","")} for j in jobs[:30]]
        except Exception:
            pass
    return []

def _fetch_lever(slugs: list) -> list:
    for slug in slugs:
        try:
            r = requests.get(f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=6, headers=HEADERS)
            if r.status_code == 200 and isinstance(r.json(), list) and r.json():
                return [{"title": j.get("text",""), "location": j.get("categories",{}).get("location",""), "url": j.get("hostedUrl","")} for j in r.json()[:30]]
        except Exception:
            pass
    return []

def _fetch_smartrecruiters(slugs: list) -> list:
    for slug in slugs:
        try:
            r = requests.get(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings", timeout=6, headers=HEADERS)
            if r.status_code == 200:
                content = r.json().get("content", [])
                if content:
                    return [{"title": j.get("name",""), "location": ", ".join(filter(None,[j.get("location",{}).get("city",""), j.get("location",{}).get("country","")])), "url": f"https://jobs.smartrecruiters.com/{slug}/{j.get('id','')}"} for j in content[:30]]
        except Exception:
            pass
    return []

def _fetch_ashby(slugs: list) -> list:
    for slug in slugs:
        try:
            r = requests.get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}", timeout=6, headers=HEADERS)
            if r.status_code == 200:
                jobs = r.json().get("jobPostings", [])
                if jobs:
                    return [{"title": j.get("title",""), "location": j.get("locationName","") or j.get("isRemote","") and "Remote" or "", "url": j.get("jobUrl", f"https://jobs.ashbyhq.com/{slug}/{j.get('id','')}")} for j in jobs[:30]]
        except Exception:
            pass
    return []

def _fetch_youtube(company: str) -> list:
    """Search YouTube for company videos using the Data API v3."""
    key = os.environ.get("YOUTUBE_API_KEY", "")
    if not key:
        return []
    try:
        q = f"{company} CEO interview OR company overview OR founder story OR podcast OR earnings"
        r = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={"q": q, "part": "snippet", "type": "video",
                    "maxResults": 5, "key": key, "relevanceLanguage": "en"},
            timeout=8,
        )
        if r.status_code != 200:
            return []
        out = []
        for item in r.json().get("items", []):
            vid_id  = item.get("id", {}).get("videoId", "")
            snippet = item.get("snippet", {})
            if not vid_id:
                continue
            out.append({
                "video_id":  vid_id,
                "title":     snippet.get("title", ""),
                "channel":   snippet.get("channelTitle", ""),
                "published": snippet.get("publishedAt", "")[:10],
                "thumbnail": snippet.get("thumbnails", {}).get("medium", {}).get("url", ""),
                "url":       f"https://www.youtube.com/watch?v={vid_id}",
            })
        return out
    except Exception:
        return []


def fetch_company_info(company: str) -> dict:
    original = company.strip()

    # Step 1: canonicalise via Groq BEFORE cache lookup so we always use the right key
    canonical = original
    try:
        groq_key = os.environ.get("GROQ_API_KEY", "")
        if groq_key:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.1-8b-instant",
                    "messages": [{"role": "user", "content":
                        f'What is the correct full legal or well-known name of the company "{original}"? '
                        f'Reply with ONLY the company name, nothing else. '
                        f'If it is already correct, repeat it unchanged.'}],
                    "temperature": 0.1,
                    "max_tokens": 30,
                },
                timeout=6,
            )
            resolved = r.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
            if resolved and len(resolved) < 120:
                canonical = resolved
    except Exception:
        pass

    suggested = canonical if canonical.lower() != original.lower() else ""
    company = canonical

    key = company.strip().lower() + "|" + _COMPANY_VER

    # L1: in-memory (under canonical key)
    cached = _COMPANY_CACHE.get(key)
    if cached and time.time() - cached["ts"] < _COMPANY_TTL:
        data = dict(cached["data"])
        data["suggested_name"] = suggested  # always reflect current query's suggestion
        return data

    # Stampede protection
    with _COMPANY_LOCK:
        cached = _COMPANY_CACHE.get(key)
        if cached and time.time() - cached["ts"] < _COMPANY_TTL:
            data = dict(cached["data"])
            data["suggested_name"] = suggested
            return data
        if key in _COMPANY_INFLIGHT:
            ev = _COMPANY_INFLIGHT[key]
        else:
            ev = _threading.Event()
            _COMPANY_INFLIGHT[key] = ev
            ev = None

    if ev is not None:
        ev.wait(timeout=30)
        cached = _COMPANY_CACHE.get(key)
        if cached:
            data = dict(cached["data"])
            data["suggested_name"] = suggested
            return data
        return {}

    try:
        # L2: Supabase persistent cache (under canonical key)
        sb_data = _sb_cache_get("company:" + key)
        if sb_data:
            _COMPANY_CACHE[key] = {"ts": time.time(), "data": sb_data}
            data = dict(sb_data)
            data["suggested_name"] = suggested
            return data

        slugs = _co_slugs(company)
        enc = requests.utils.quote(company)
        slug = slugs[0] if slugs else ""
        links = {
            "linkedin":       f"https://www.linkedin.com/company/{slug}",
            "linkedin_jobs":  f"https://www.linkedin.com/jobs/search/?keywords={enc}",
            "glassdoor":      f"https://www.glassdoor.co.uk/Search/results.htm?keyword={enc}",
            "indeed":         f"https://uk.indeed.com/jobs?q={enc}",
            "reed":           f"https://www.reed.co.uk/jobs/{enc}-jobs",
            "totaljobs":      f"https://www.totaljobs.com/jobs/{enc}",
        }

        with _cf.ThreadPoolExecutor(max_workers=10) as pool:
            wiki_f     = pool.submit(_fetch_wikipedia, company)
            news_f     = pool.submit(_fetch_news, company, "", 6)
            ai_news_f  = pool.submit(_fetch_news, company, "AI OR \"artificial intelligence\" OR \"machine learning\"", 5)
            strat_f    = pool.submit(_fetch_news, company, "strategy OR acquisition OR partnership OR expansion OR growth plan", 5)
            results_f  = pool.submit(_fetch_news, company, 'results OR earnings OR "annual results" OR "quarterly results" OR "full year results" OR "half year results"', 3)
            youtube_f  = pool.submit(_fetch_youtube, company)
            share_f    = pool.submit(_fetch_share_price, company)
            gh_f       = pool.submit(_fetch_greenhouse, slugs)
            lv_f       = pool.submit(_fetch_lever, slugs)
            sr_f       = pool.submit(_fetch_smartrecruiters, slugs)
            ab_f       = pool.submit(_fetch_ashby, slugs)

            wiki = {}
            try:
                wiki = wiki_f.result(timeout=10) or {}
            except Exception:
                pass

            news = []
            try:
                news = news_f.result(timeout=10) or []
            except Exception:
                pass

            ai_news = []
            try:
                ai_news = ai_news_f.result(timeout=10) or []
            except Exception:
                pass

            strategy_news = []
            try:
                strategy_news = strat_f.result(timeout=10) or []
            except Exception:
                pass

            results_news = []
            try:
                results_news = results_f.result(timeout=10) or []
            except Exception:
                pass

            youtube = []
            try:
                youtube = youtube_f.result(timeout=10) or []
            except Exception:
                pass

            share = {}
            try:
                share = share_f.result(timeout=8) or {}
            except Exception:
                pass

            jobs, source = [], ""
            for fut, src in [(gh_f, "Greenhouse"), (lv_f, "Lever"), (ab_f, "Ashby"), (sr_f, "SmartRecruiters")]:
                try:
                    j = fut.result(timeout=10)
                    if j:
                        jobs, source = j, src
                        break
                except Exception:
                    pass

        result = {
            "name":           company,
            "suggested_name": suggested,
            "wiki":           wiki,
            "news":           news,
            "ai_news":        ai_news,
            "strategy_news":  strategy_news,
            "results_news":   results_news,
            "youtube":        youtube,
            "share":          share,
            "ai_signals":     _ai_job_signals(jobs),
            "jobs":           jobs,
            "jobs_source":    source,
            "job_signals":    _job_signals(jobs),
            "links":          links,
            "slug":           slug,
        }
        _COMPANY_CACHE[key] = {"ts": time.time(), "data": result}
        _sb_cache_set("company:" + key, result)
        return result
    finally:
        with _COMPANY_LOCK:
            ev = _COMPANY_INFLIGHT.pop(key, None)
        if ev:
            ev.set()


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
        if pt and price and pt.lower() != "other":
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
    summary = _parse_lr_items(items)

    # Fall back to district if: too few sales OR only 1 property type found
    # (e.g. a postcode that's entirely flats gives misleading single-type results)
    if len(items) < 5 or len(summary) < 2:
        info = _get_postcode_info(postcode)
        admin = info["admin_district"]
        if admin:
            fallback_items = _fetch("propertyAddress.district", admin.replace(" ", "%20"))
            fallback_summary = _parse_lr_items(fallback_items)
            if len(fallback_summary) > len(summary):
                items = fallback_items
                summary = fallback_summary
                scope = admin.title()
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
