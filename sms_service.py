#!/usr/bin/env python3
from __future__ import annotations
# Patch stdlib for gevent async I/O — must be first
from gevent import monkey as _gmonkey
_gmonkey.patch_all(thread=True, socket=True, ssl=True)

"""
FuelWatch UK — SMS Service
===========================
Text a postcode to your Twilio number and get back live fuel prices.

Usage:
  Text: "KT16 0DA"           → petrol prices, 5 mile radius
  Text: "KT16 0DA diesel"    → diesel prices
  Text: "KT16 0DA petrol 10" → petrol, 10 mile radius

Setup:
  1. pip3 install flask twilio
  2. Sign up at twilio.com (free trial)
  3. Copy your Account SID, Auth Token, and phone number into .env
  4. Run: python3 sms_service.py
  5. Expose with: ngrok http 5000
  6. Set Twilio webhook to: https://YOUR-NGROK-URL/sms
"""

import hashlib
import hmac
import io
import json
import os
import re
import time
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from flask import Flask, request, send_file, render_template, jsonify, Response, redirect
from twilio.twiml.messaging_response import MessagingResponse
from search import (postcode_to_latlon, fetch_all_stations, haversine_km,
                    fetch_nearby_amenities, fetch_nearby_schools,
                    fetch_nearby_pubs, fetch_house_prices, fetch_local_amenities,
                    fetch_company_info, fetch_brand_data,
                    _fetch_wikipedia, _fetch_news, _fetch_trustpilot)
import analytics
import library as lib
import school_service

app = Flask(__name__)

_CORS_ORIGINS = {"https://ai.humanagency.co", "http://ai.humanagency.co", "http://localhost:8080",
                 "https://mekalav.com", "https://www.mekalav.com"}

def _cors_headers(response):
    origin = request.headers.get("Origin", "")
    if origin in _CORS_ORIGINS:
        response.headers["Access-Control-Allow-Origin"]  = origin
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Edit-Token"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Max-Age"]       = "600"
    return response

@app.after_request
def _cors(response):
    return _cors_headers(response)

@app.errorhandler(500)
def handle_500(e):
    import traceback
    tb = traceback.format_exc()
    print(f"[500] {tb}")
    if request.path.startswith("/api/"):
        return jsonify({"error": str(e), "detail": tb[-500:]}), 500
    return f"<pre>500 Error:\n{tb}</pre>", 500

# Initialise analytics DB on startup
analytics.init_db()

# WhatsApp response cache — keyed by "postcode:fuel:radius", 30-min TTL
_WA_CACHE: dict = {}
_WA_CACHE_TTL = 1800

# ── Price history files ────────────────────────────────────────────────────────
NATIONAL_HISTORY_FILE = "price_history_national.json"
POSTCODE_HISTORY_FILE = "price_history_postcodes.json"

def _load_json(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return []

def _save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f)

def log_national_snapshot(stations):
    """Append national avg petrol/diesel to history after each cache refresh."""
    petrol_prices = [s["petrol"] for s in stations if s.get("petrol") and s["petrol"] > 0]
    diesel_prices = [s["diesel"] for s in stations if s.get("diesel") and s["diesel"] > 0]
    if not petrol_prices:
        return
    record = {
        "ts": datetime.now().isoformat(timespec="minutes"),
        "petrol_avg": round(sum(petrol_prices) / len(petrol_prices), 2),
        "diesel_avg": round(sum(diesel_prices) / len(diesel_prices), 2) if diesel_prices else None,
        "station_count": len(stations),
    }
    history = _load_json(NATIONAL_HISTORY_FILE)
    # Deduplicate by minute
    if not history or history[-1]["ts"] != record["ts"]:
        history.append(record)
        history = history[-2016:]  # keep ~6 weeks of 30-min snapshots
        _save_json(NATIONAL_HISTORY_FILE, history)

def log_postcode_snapshot(postcode, fuel, nearby):
    """Append cheapest + area avg for a postcode search to history."""
    if not nearby:
        return
    prices = [s["price"] for s in nearby]
    record = {
        "ts": datetime.now().isoformat(timespec="minutes"),
        "fuel": fuel,
        "cheapest": round(min(prices), 2),
        "avg": round(sum(prices) / len(prices), 2),
        "count": len(prices),
    }
    all_history = _load_json(POSTCODE_HISTORY_FILE)
    if not isinstance(all_history, dict):
        all_history = {}
    key = f"{postcode.upper()}_{fuel}"
    entries = all_history.get(key, [])
    if not entries or entries[-1]["ts"] != record["ts"]:
        entries.append(record)
        entries = entries[-336:]  # keep ~1 week of 30-min snapshots
        all_history[key] = entries
        _save_json(POSTCODE_HISTORY_FILE, all_history)


# ── Cache stations in memory (refresh every 30 min) ───────────────────────────
import bisect as _bisect
import math as _math

_station_cache = {"data": [], "lats": [], "loaded_at": 0}
CACHE_TTL = 1800  # 30 minutes

# ── Fuel search result cache (5 min TTL, keyed by postcode+fuel+radius) ────────
_fuel_search_cache: dict = {}  # key → (result_dict, ts)
_FUEL_SEARCH_TTL = 300

def get_stations():
    now = time.time()
    if not _station_cache["data"] or (now - _station_cache["loaded_at"]) > CACHE_TTL:
        stations = fetch_all_stations()
        stations.sort(key=lambda s: s["lat"])          # sort by lat for bisect slicing
        _station_cache["data"] = stations
        _station_cache["lats"] = [s["lat"] for s in stations]
        _station_cache["loaded_at"] = now
        log_national_snapshot(stations)
    return _station_cache["data"]


# Pre-warm station cache on startup so first user request after a deploy is instant
import threading as _threading_early
_threading_early.Thread(target=get_stations, daemon=True).start()

_DEG_PER_KM = 1 / 111.0  # 1° lat ≈ 111 km

def _nearby_stations(lat: float, lon: float, fuel: str, radius_km: float, retailer: str = None) -> list:
    """Return stations within radius using a spatial index — ~40x faster than full scan."""
    stations = get_stations()
    lats = _station_cache["lats"]

    lat_delta = radius_km * _DEG_PER_KM
    lon_delta = radius_km * _DEG_PER_KM / _math.cos(_math.radians(lat))

    lo = _bisect.bisect_left(lats, lat - lat_delta)
    hi = _bisect.bisect_right(lats, lat + lat_delta)

    nearby = []
    for s in stations[lo:hi]:
        if abs(s["lon"] - lon) > lon_delta:
            continue
        price = s.get(fuel)
        if not price or price <= 0:
            continue
        if retailer and retailer.lower() not in s.get("brand", "").lower():
            continue
        dist_km = haversine_km(lat, lon, s["lat"], s["lon"])
        if dist_km <= radius_km:
            nearby.append({**s, "dist_mi": round(dist_km / 1.60934, 2), "price": price})
    return nearby


# ── SMS Parser ────────────────────────────────────────────────────────────────

KNOWN_RETAILERS = ["tesco", "asda", "bp", "shell", "esso", "sainsburys",
                   "sainsbury", "morrisons", "jet", "applegreen", "rontec",
                   "moto", "sgn", "texaco"]

def parse_sms(body: str):
    """
    Parse incoming SMS into (postcode, fuel, radius_miles, retailer).
    Examples:
      "KT16 0DA"              -> ("KT160DA", "petrol", 5.0, None)
      "KT160DA diesel"        -> ("KT160DA", "diesel", 5.0, None)
      "KT16 0DA petrol 10"    -> ("KT160DA", "petrol", 10.0, None)
      "KT16 0DA tesco"        -> ("KT160DA", "petrol", 5.0, "tesco")
      "KT16 0DA bp diesel 10" -> ("KT160DA", "diesel", 10.0, "bp")
    """
    body = body.strip().upper()

    # Extract UK postcode (handles spaced and non-spaced)
    postcode_match = re.search(
        r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body
    )
    if not postcode_match:
        return None, None, None, None

    postcode = postcode_match.group(1).replace(" ", "")

    fuel = "diesel" if "DIESEL" in body else "petrol"

    radius_match = re.search(r'\b(\d+)\s*(?:MILE|MI|MILES)?\b', body.replace(postcode, ""))
    radius = float(radius_match.group(1)) if radius_match else 5.0
    radius = min(max(radius, 1), 20)

    retailer = None
    for r in KNOWN_RETAILERS:
        if r.upper() in body:
            retailer = r
            break

    return postcode, fuel, radius, retailer


# ── Weather ───────────────────────────────────────────────────────────────────

WEATHER_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Foggy", 48: "Icy fog", 51: "Light drizzle", 53: "Drizzle",
    55: "Heavy drizzle", 61: "Light rain", 63: "Rain", 65: "Heavy rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow", 77: "Snow grains",
    80: "Light showers", 81: "Showers", 82: "Heavy showers",
    85: "Snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm+hail", 99: "Thunderstorm+hail",
}

_weather_cache: dict = {}

def get_weather(lat: float, lon: float) -> str:
    """Fetch current weather from Open-Meteo (free, no API key). Cached 10min."""
    key = (round(lat, 2), round(lon, 2))
    cached = _weather_cache.get(key)
    if cached and (time.time() - cached["ts"]) < 600:
        return cached["v"]
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,weathercode,windspeed_10m"
            f"&timezone=Europe/London"
        )
        r = requests.get(url, timeout=3)
        c = r.json()["current"]
        temp    = round(c["temperature_2m"])
        code    = c["weathercode"]
        wind    = round(c["windspeed_10m"])
        desc    = WEATHER_CODES.get(code, "")
        result  = f"{temp}°C {desc}, Wind {wind}km/h"
        _weather_cache[key] = {"ts": time.time(), "v": result}
        return result
    except Exception:
        return ""


# ── TfL Tube ──────────────────────────────────────────────────────────────────

TFL_LINE_ALIASES = {
    "bakerloo": "bakerloo",
    "central": "central",
    "circle": "circle",
    "district": "district",
    "hammersmith": "hammersmith-city",
    "hammersmith & city": "hammersmith-city",
    "hammersmith and city": "hammersmith-city",
    "jubilee": "jubilee",
    "metropolitan": "metropolitan",
    "northern": "northern",
    "piccadilly": "piccadilly",
    "victoria": "victoria",
    "waterloo & city": "waterloo-city",
    "waterloo and city": "waterloo-city",
    "elizabeth": "elizabeth",
    "dlr": "dlr",
    "overground": "london-overground",
}

_TUBE_STATUS_EMOJI = {
    "Good Service": "✅",
    "Minor Delays": "⚠️",
    "Severe Delays": "🔴",
    "Part Suspended": "🔴",
    "Suspended": "🚫",
    "Part Closure": "🚫",
    "Closed": "🚫",
    "Planned Closure": "🔵",
    "Bus Service": "🚌",
    "Reduced Service": "⚠️",
}

_tube_cache: dict = {}


def get_tube_status(line: str = None) -> str:
    key = line or "all"
    cached = _tube_cache.get(key)
    if cached and (time.time() - cached["ts"]) < 120:
        return cached["v"]
    try:
        if line:
            url = f"https://api.tfl.gov.uk/Line/{line}/Status"
        else:
            url = "https://api.tfl.gov.uk/Line/Mode/tube,elizabeth-line,dlr,overground/Status"
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return f"Couldn't fetch tube status: {e}"

    lines_out = []
    for entry in data:
        name = entry.get("name", "")
        statuses = entry.get("lineStatuses", [])
        if statuses:
            sev_desc = statuses[0].get("statusSeverityDescription", "Unknown")
            disruption = statuses[0].get("disruption") or {}
            reason = disruption.get("description", "") if isinstance(disruption, dict) else ""
            emoji = _TUBE_STATUS_EMOJI.get(sev_desc, "ℹ️")
            line_str = f"{emoji} {name}: {sev_desc}"
            if reason and sev_desc != "Good Service":
                reason = reason[:100] + "…" if len(reason) > 100 else reason
                line_str += f"\n   {reason}"
            lines_out.append(line_str)

    result = "🚇 *Tube Status*\n" + "\n".join(lines_out)
    _tube_cache[key] = {"v": result, "ts": time.time()}
    return result


_STATION_QUALIFIER_RE = re.compile(
    r'\b(national rail|dlr|tube|underground|overground|tfl|elizabeth line)\b', re.I
)

def _resolve_tube_station(name: str):
    """Return (naptan_id, display_name) for the best-matching TfL station, or (None, None)."""
    clean = _STATION_QUALIFIER_RE.sub('', name).strip()
    if not clean:
        clean = name.strip()

    def _tfl_search(q):
        r = requests.get(
            "https://api.tfl.gov.uk/StopPoint/Search/" + requests.utils.quote(q),
            params={"modes": "tube,dlr,elizabeth-line,overground", "includeHubs": "false", "maxResults": "6"},
            timeout=8,
        )
        r.raise_for_status()
        return r.json().get("matches", [])

    try:
        matches = _tfl_search(clean)
        # "London Waterloo" → try "Waterloo" if no results
        if not matches and clean.lower().startswith("london "):
            matches = _tfl_search(clean[7:])
        # Prefer 940G IDs (true TfL metro naptan — avoids NR 910G IDs in journey planner)
        for m in matches:
            if m["id"].startswith("940G"):
                return m["id"], m["name"].replace(" Underground Station", "").replace(" Station", "")
        if matches:
            return matches[0]["id"], matches[0]["name"].replace(" Underground Station", "").replace(" Station", "")
    except Exception:
        pass
    return None, None


def get_tube_journey(from_name: str, to_name: str) -> str:
    from_id, from_display = _resolve_tube_station(from_name)
    to_id, to_display = _resolve_tube_station(to_name)
    if not from_id:
        return f"Couldn't find tube station: {from_name}"
    if not to_id:
        return f"Couldn't find tube station: {to_name}"
    try:
        r = requests.get(
            f"https://api.tfl.gov.uk/Journey/JourneyResults/{from_id}/to/{to_id}",
            params={"mode": "tube,dlr,elizabeth-line,overground"},
            timeout=10,
        )
        r.raise_for_status()
        journeys = r.json().get("journeys", [])
    except Exception as e:
        return f"Journey planner error: {e}"

    if not journeys:
        return f"No tube journey found from {from_display} to {to_display}"

    j = journeys[0]
    duration = j.get("duration", "?")
    legs = j.get("legs", [])

    def _short(name):
        return name.replace(" Underground Station", "").replace(" Station", "")

    from_short = _short(from_display)
    to_short   = _short(to_display)

    parts = [f"🚇 *{from_short} → {to_short}*", f"⏱ {duration} min"]

    tube_legs = []
    for leg in legs:
        mode = leg.get("mode", {}).get("name", "")
        leg_dur = leg.get("duration", "?")
        dep = _short(leg.get("departurePoint", {}).get("commonName", ""))
        arr = _short(leg.get("arrivalPoint", {}).get("commonName", ""))
        route_opts = leg.get("routeOptions", [])
        line_name = route_opts[0].get("name", "") if route_opts else ""
        if mode in ("tube", "dlr", "elizabeth-line", "overground", "national-rail"):
            fallback_label = {"dlr": "DLR", "elizabeth-line": "Elizabeth", "overground": "Overground"}.get(mode, "Tube")
            # Only show leg detail if it's not trivially the same as the header
            if dep != from_short or arr != to_short or len(legs) > 1:
                parts.append(f"• {line_name or fallback_label}: {dep} → {arr} ({leg_dur} min)")
            else:
                parts.append(f"• {line_name or fallback_label} line ({leg_dur} min)")
            tube_legs.append({"line": line_name, "dep_id": from_id})
        elif mode == "walking":
            if leg_dur and int(leg_dur) > 1:
                parts.append(f"• Walk {leg_dur} min")

    # Append live next departures on the first tube leg from the origin
    if tube_legs:
        first_line = tube_legs[0]["line"]
        try:
            arr_r = requests.get(
                f"https://api.tfl.gov.uk/StopPoint/{from_id}/Arrivals",
                timeout=6,
            )
            arr_r.raise_for_status()
            arrivals = arr_r.json()
            arrivals.sort(key=lambda a: a.get("timeToStation", 9999))
            next_deps = []
            seen_dir = set()
            for a in arrivals:
                if first_line and a.get("lineName", "").lower() != first_line.lower():
                    continue
                secs = a.get("timeToStation", 0)
                mins = secs // 60
                platform = a.get("platformName", "")
                dir_key = platform
                if dir_key in seen_dir:
                    continue
                seen_dir.add(dir_key)
                due = "Due" if mins < 1 else f"{mins} min"
                dest = a.get("destinationName", "").replace(" Underground Station", "")
                next_deps.append(f"  {due} → {dest}")
                if len(next_deps) >= 3:
                    break
            if next_deps:
                parts.append(f"\n*Next {first_line} from {from_short}:*")
                parts.extend(next_deps)
        except Exception:
            pass

    return "\n".join(parts)


def _get_wa_home_postcode(from_number: str):
    """Return the user's stored home postcode, or None."""
    try:
        plain = from_number.replace("whatsapp:", "").strip()
        wa    = from_number if from_number.startswith("whatsapp:") else f"whatsapp:{from_number}"
        rows = (lib._sb().table("my_area_places").select("postcode")
                .eq("category", "_home")
                .in_("from_number", [from_number, plain, wa])
                .limit(1).execute().data)
        return rows[0]["postcode"] if rows else None
    except Exception:
        return None


def _set_wa_pending_intent(from_number: str, intent: dict):
    """Store a pending intent in Supabase so it survives Railway redeploys."""
    plain = from_number.replace("whatsapp:", "").strip()
    try:
        sb = lib._sb()
        sb.table("wa_saves").delete().eq("from_number", plain).eq("status", "pending_intent").execute()
        sb.table("wa_saves").insert({
            "from_number": plain,
            "status": "pending_intent",
            "url": "pending:" + json.dumps(intent),
            "title": "Pending intent: " + intent.get("type", ""),
        }).execute()
    except Exception as e:
        print(f"[pending_intent set] {e}")


def _get_wa_pending_intent(from_number: str):
    """Return stored pending intent dict, or None."""
    plain = from_number.replace("whatsapp:", "").strip()
    wa    = f"whatsapp:{plain}"
    try:
        rows = (lib._sb().table("wa_saves")
                .select("url,created_at")
                .in_("from_number", [from_number, plain, wa])
                .eq("status", "pending_intent")
                .order("created_at", desc=True)
                .limit(1).execute().data)
        if not rows:
            return None
        url = rows[0].get("url", "")
        if url.startswith("pending:"):
            return json.loads(url[8:])
    except Exception:
        pass
    return None


def _clear_wa_pending_intent(from_number: str):
    plain = from_number.replace("whatsapp:", "").strip()
    wa    = f"whatsapp:{plain}"
    try:
        lib._sb().table("wa_saves").delete().in_("from_number", [from_number, plain, wa]).eq("status", "pending_intent").execute()
    except Exception:
        pass


def _nearest_tube_station(lat: float, lon: float, radius_m: int = 2000):
    """Return (naptan_id, display_name, distance_m) for nearest tube station, or (None,None,None)."""
    try:
        r = requests.get(
            "https://api.tfl.gov.uk/StopPoint",
            params={
                "lat": lat, "lon": lon,
                "stopTypes": "NaptanMetroStation,NaptanRailAccessArea",
                "radius": radius_m,
                "modes": "tube,dlr,elizabeth-line,overground",
                "returnLines": "false",
            },
            timeout=8,
        )
        r.raise_for_status()
        stops = r.json().get("stopPoints", [])
        if stops:
            s = stops[0]
            return s["id"], s["commonName"], round(s.get("distance", 0))
    except Exception as e:
        print(f"[tube nearest] {e}")
    return None, None, None


def get_tube_arrivals(naptan_id: str, station_name: str) -> str:
    """Return WhatsApp-formatted next arrivals at a tube station."""
    try:
        r = requests.get(
            f"https://api.tfl.gov.uk/StopPoint/{naptan_id}/Arrivals",
            timeout=8,
        )
        r.raise_for_status()
        arrivals = r.json()
    except Exception as e:
        return f"Couldn't fetch arrivals: {e}"

    if not arrivals:
        return f"🚇 *{station_name}*\nNo arrivals data right now."

    arrivals.sort(key=lambda a: a.get("timeToStation", 9999))

    seen, lines = set(), []
    for a in arrivals:
        line    = a.get("lineName", "")
        dest    = a.get("destinationName", "").replace(" Underground Station", "")
        platform = a.get("platformName", "")
        secs    = a.get("timeToStation", 0)
        mins    = secs // 60
        due     = "Due" if mins < 1 else f"{mins} min"
        key     = (line, platform)
        if key in seen:
            continue
        seen.add(key)
        dir_part = f"  _{platform}_" if platform else ""
        lines.append(f"• *{line}* → {dest}  {due}{dir_part}")
        if len(lines) >= 8:
            break

    short_name = station_name.replace(" Underground Station", "")
    return f"🚇 *{short_name}*\n" + "\n".join(lines)


def handle_tube_command(text: str, from_number: str = None) -> str:
    """Route a 'tube ...' WhatsApp message to the right TfL function."""
    remainder = text[4:].strip().lower()  # strip 'tube'

    if " to " in remainder:
        parts = remainder.split(" to ", 1)
        return get_tube_journey(parts[0].strip(), parts[1].strip())

    for alias, tfl_id in TFL_LINE_ALIASES.items():
        if remainder == alias or remainder.startswith(alias + " "):
            return get_tube_status(tfl_id)

    if remainder == "status":
        return get_tube_status()

    # "tube" or "tube near me" — show nearest station arrivals from home postcode
    if not remainder or remainder in ("near me", "nearest", "local"):
        if from_number:
            postcode = _get_wa_home_postcode(from_number)
            if postcode:
                latlon = postcode_to_latlon(postcode)
                if latlon:
                    lat, lon = latlon
                    naptan_id, name, dist_m = _nearest_tube_station(lat, lon)
                    if naptan_id:
                        dist_str = f"{dist_m}m away" if dist_m < 1000 else f"{dist_m/1000:.1f}km away"
                        header = get_tube_arrivals(naptan_id, name)
                        return header + f"\n\n_{dist_str} from {postcode}_"
                    return "No tube station found within 2km of your home postcode.\n\nTry: *tube Kings Cross to Waterloo*"
        return get_tube_status()

    return get_tube_status()


# ── Search & Format ───────────────────────────────────────────────────────────

def search_and_format(postcode: str, fuel: str, radius_miles: float, retailer: str = None) -> str:
    """Run search and format result as a concise SMS reply."""

    latlon = postcode_to_latlon(postcode)
    if not latlon:
        return f"Sorry, couldn't find postcode {postcode}. Please check and try again."

    lat, lon = latlon
    radius_km = radius_miles * 1.60934
    nearby = _nearby_stations(lat, lon, fuel, radius_km, retailer)

    if not nearby:
        retailer_msg = f" {retailer.title()}" if retailer else ""
        return (
            f"No{retailer_msg} {fuel} stations found within {radius_miles:.0f} miles of {postcode}.\n"
            f"Try: {postcode} {fuel} 10"
        )

    nearby.sort(key=lambda x: (x["price"], x["dist_mi"]))
    log_postcode_snapshot(postcode, fuel, nearby)
    avg = sum(s["price"] for s in nearby) / len(nearby)
    cheapest = nearby[0]
    tank_saving = (avg - cheapest["price"]) * 55 / 100

    fuel_label = "Petrol" if fuel == "petrol" else "Diesel"
    now = datetime.now().strftime("%d %b %Y %H:%M")
    weather = get_weather(lat, lon)

    # Today / date / time / weather
    lines = [
        f"Today {now}",
        weather if weather else "",
        "",
    ]

    # Petrol Prices
    retailer_label = f" — {retailer.title()}" if retailer else ""
    lines += [
        f"-- {fuel_label}{retailer_label} near {postcode} --",
        f"Radius: {radius_miles:.0f}mi | {len(nearby)} stations",
        "",
    ]
    for i, s in enumerate(nearby[:5], 1):
        marker = ">>>" if i == 1 else f" {i}."
        maps_url = f"https://maps.google.com/?q={s['lat']},{s['lon']}"
        lines.append(f"{marker} {s['brand']} {s['price']:.1f}p ({s['dist_mi']:.1f}mi)")
        if s["address"]:
            lines.append(f"    {s['address'][:30]}")
        lines.append(f"    {maps_url}")
    lines += [
        "",
        f"Avg: {avg:.1f}p | Save: {avg - cheapest['price']:.1f}p/L",
        f"Full tank: £{tank_saving:.2f} saving",
    ]

    # Supermarkets & Coffee (fetch with timeout so SMS isn't delayed)
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(fetch_nearby_amenities, lat, lon, radius_miles * 1.60934)
            amenities = future.result(timeout=6)
    except (FuturesTimeoutError, Exception):
        amenities = {"supermarkets": [], "cafes": []}
    if amenities["supermarkets"]:
        lines += ["", "-- Supermarkets --"]
        for s in amenities["supermarkets"][:5]:
            lines.append(f"  {s['name']}{s['rating']} ({s['dist_mi']:.1f}mi)")
    if amenities["cafes"]:
        lines += ["", "-- Coffee --"]
        for c in amenities["cafes"][:5]:
            lines.append(f"  {c['name']}{c['rating']} ({c['dist_mi']:.1f}mi)")

    return "\n".join(l for l in lines if l is not None)


def whatsapp_search_and_format(postcode: str, fuel: str, radius_miles: float, retailer: str = None) -> str:
    """Lean WhatsApp reply — fuel prices + weather only, no amenities fetch."""
    latlon = postcode_to_latlon(postcode)
    if not latlon:
        return f"Sorry, couldn't find postcode {postcode}. Please check and try again."

    lat, lon = latlon
    radius_km = radius_miles * 1.60934
    nearby = _nearby_stations(lat, lon, fuel, radius_km, retailer)

    if not nearby:
        retailer_msg = f" {retailer.title()}" if retailer else ""
        return (f"No{retailer_msg} {fuel} stations found within {radius_miles:.0f} miles of {postcode}.\n"
                f"Try: {postcode} {fuel} 10")

    nearby.sort(key=lambda x: (x["price"], x["dist_mi"]))
    log_postcode_snapshot(postcode, fuel, nearby)
    avg = sum(s["price"] for s in nearby) / len(nearby)
    cheapest = nearby[0]
    tank_saving = (avg - cheapest["price"]) * 55 / 100

    fuel_label = "Petrol" if fuel == "petrol" else "Diesel"
    now = datetime.now().strftime("%d %b %Y %H:%M")
    weather = get_weather(lat, lon)

    lines = [f"FuelWatch 🇬🇧 {now}"]
    if weather:
        lines.append(weather)
    lines += ["", f"-- {fuel_label} near {postcode} --",
              f"Radius: {radius_miles:.0f}mi | {len(nearby)} stations", ""]

    for i, s in enumerate(nearby[:5], 1):
        marker = ">>>" if i == 1 else f" {i}."
        maps_url = f"https://maps.google.com/?q={s['lat']},{s['lon']}"
        lines.append(f"{marker} {s['brand']} {s['price']:.1f}p ({s['dist_mi']:.1f}mi)")
        if s["address"]:
            lines.append(f"    {s['address'][:35]}")
        lines.append(f"    {maps_url}")

    lines += ["", f"Avg: {avg:.1f}p | Save vs avg: {avg - cheapest['price']:.1f}p/L",
              f"Full tank saving: £{tank_saving:.2f}",
              "", "Reply with postcode [diesel] [radius]",
              "🔗 miru.humanagency.co"]
    return "\n".join(lines)


# ── Webhook ───────────────────────────────────────────────────────────────────

@app.route("/sms", methods=["POST"])
def sms_reply():
    body = request.form.get("Body", "").strip()
    from_number = request.form.get("From", "unknown")

    print(f"SMS from {from_number}: {body}")

    resp = MessagingResponse()

    if not body:
        resp.message("FuelWatch UK\nText your postcode to get fuel prices.\nExample: KT16 0DA\nOr: KT16 0DA diesel 10")
        return str(resp)

    if body.lower().startswith("tube"):
        resp.message(handle_tube_command(body))
        return str(resp)

    postcode, fuel, radius, retailer = parse_sms(body)

    if not postcode:
        resp.message(
            "FuelWatch UK\nCouldn't read that postcode.\n"
            "Try: KT16 0DA\nOr: KT16 0DA tesco diesel 10"
        )
        return str(resp)

    reply = search_and_format(postcode, fuel, radius, retailer)
    resp.message(reply)
    return str(resp)


@app.route("/")
def index():
    if "space." in request.host:
        return render_template("space.html")
    intel_mode = "intel." in request.host or request.args.get("intel") == "1" or request.args.get("screen") == "intel"
    return render_template("index.html", prefill_company=None, prefill_doc=None, intel_mode=intel_mode)

@app.route("/home-v2")
def home_v2():
    if request.args.get("preview") != "miru2026":
        return "", 404
    return render_template("home_v2.html")

@app.route("/design/home")
def design_home():
    return render_template("design_home_bento.html")

@app.route("/design/lanes")
def design_lanes():
    return render_template("design_home_lanes.html")

@app.route("/design/weather")
def design_weather():
    return render_template("design_weather_icons.html")

@app.route("/privacy")
def privacy_page():
    return render_template("privacy.html")

@app.route("/terms")
def terms_page():
    return render_template("terms.html")


@app.route("/api/yt/info")
def api_yt_info():
    """Fetch YouTube video metadata server-side to avoid CORS issues."""
    vid = request.args.get("id", "").strip()
    if not vid or len(vid) > 20:
        return jsonify({"error": "Invalid video ID"}), 400
    import re as _re

    # oEmbed — reliable, no auth needed
    try:
        oe = requests.get(
            f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={vid}&format=json",
            timeout=8
        )
        oe.raise_for_status()
        oe_data = oe.json()
        title   = oe_data.get("title", "")
        channel = oe_data.get("author_name", "")
    except Exception as e:
        return jsonify({"error": f"oEmbed failed: {e}"}), 502

    # Fetch YouTube page for description + duration — may be blocked by YT on cloud IPs
    desc = ""
    duration_str = ""
    try:
        page = requests.get(
            f"https://www.youtube.com/watch?v={vid}",
            headers={"User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )},
            timeout=12,
            allow_redirects=True,
        )
        html = page.text
        m = (_re.search(r'"shortDescription":"((?:[^"\\]|\\.)*)"', html) or
             _re.search(r'property="og:description"\s+content="([^"]+)"', html) or
             _re.search(r'<meta name="description" content="([^"]+)"', html, _re.I))
        if m:
            desc = m.group(1)
            desc = desc.replace("\\n", "\n").replace("\\u0026", "&")
            for ent, ch in [("&#39;","'"),("&amp;","&"),("&quot;",'"'),("&#34;",'"')]:
                desc = desc.replace(ent, ch)
            desc = desc[:800]

        dm = _re.search(r'"lengthSeconds":"(\d+)"', html)
        if dm:
            secs = int(dm.group(1))
            if secs > 0:
                h, rem = divmod(secs, 3600)
                m2, s2 = divmod(rem, 60)
                duration_str = (f"{h}h {m2}min" if h else f"{m2} min" if m2 else f"{s2}s")
    except Exception as e:
        print(f"[yt/info] page fetch failed for {vid}: {e}")
        # Return partial result — title/channel still useful

    # Auto keywords from title + channel
    words = list(dict.fromkeys(
        w.lower() for w in _re.sub(r"[^a-zA-Z0-9 ]", " ", title + " " + channel).split()
        if len(w) > 2
    ))
    keywords = " ".join(words[:12])

    return jsonify({
        "title":    title,
        "channel":  channel,
        "desc":     desc[:1000],
        "duration": duration_str,
        "keywords": keywords,
    })

@app.route("/api/ai/summarize", methods=["POST", "OPTIONS"])
def api_ai_summarize():
    if request.method == "OPTIONS":
        resp = app.make_response(("", 204))
        return _cors_headers(resp)
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()[:4000]
    if not text:
        return jsonify({"error": "No text provided"}), 400
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return jsonify({"error": "Groq not configured"}), 500
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [
                    {"role": "system", "content": "You write concise, clear 2-sentence summaries of video/podcast descriptions for a resource library. Plain text only, no markdown, no fluff."},
                    {"role": "user", "content": f"Summarise this in 2 sentences:\n\n{text}"}
                ],
                "max_tokens": 120,
                "temperature": 0.3,
            },
            timeout=15,
        )
        result = r.json()
        summary = result["choices"][0]["message"]["content"].strip()
        return jsonify({"summary": summary})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/mekalav/chat", methods=["POST", "OPTIONS"])
def mekalav_chat():
    """AI chat proxy for mekalav.com — answers questions about Vikram using Groq."""
    if request.method == "OPTIONS":
        return "", 204
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return jsonify({"error": "Not configured"}), 500
    data = request.get_json(force=True, silent=True) or {}
    message = (data.get("message") or "").strip()
    history = data.get("history") or []
    if not message:
        return jsonify({"error": "No message"}), 400

    SYSTEM = """You are an AI assistant on Vikram Mekala's personal website (mekalav.com). \
Answer questions about Vikram accurately, warmly and concisely. You represent his portfolio.

About Vikram:
- 25 years in technology and transformation, based in London
- Started as a software engineer in India, lived in Italy, moved to London in 2005
- Career: Unilever (longest employer, multiple senior roles), Mars, Shell, BP, Leaseplan
- Founder of Human Agency — building practical AI tools for non-technical people
- Available for: AI strategy, digital transformation programmes, M&A technology workstreams

Career highlights:
- 2025: Unilever – Transformation Consultant, Magnum Ice Cream M&A separation (technology workstream)
- 2024–now: Human Agency – Founder
- 2020–2024: Unilever – Global Media Analytics Platform (£5M budget, 50-person team, 3 continents, 98% on-time delivery)
- 2018–2020: Unilever – Net Revenue Management analytics (pricing, portfolio, promotional strategy, eCommerce)
- 2013–2018: Shell (B2B eCommerce), Unilever (brand platform across 160 markets), Leaseplan (digital transformation)
- 2000–2013: Mars, Unilever, BP — engineer to analyst to programme manager

What he builds now:
- Miru: WhatsApp-first AI assistant. School comms, fuel prices, train times, area reports. miru.humanagency.co
- Human Agency: Free AI literacy site in plain language for non-technical people. ai.humanagency.co
- He builds by writing the problem clearly and directing AI — product thinking, not code authorship

Contact:
- Email: mekala@gmail.com
- WhatsApp: +44 759 507 5735
- Twitter/X: @mekalav, LinkedIn: linkedin.com/in/mekalavikram

Keep answers to 2-4 sentences unless more detail is asked. Be honest — if something isn't known, say so. \
Suggest contacting Vikram directly for specific opportunities."""

    messages = [{"role": "system", "content": SYSTEM}]
    for h in (history or [])[-6:]:
        if h.get("role") in ("user", "assistant"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": message})

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile", "messages": messages, "max_tokens": 300, "temperature": 0.6},
            timeout=20,
        )
        reply = r.json()["choices"][0]["message"]["content"].strip()
        return jsonify({"reply": reply})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/newsletter/subscribe", methods=["POST"])
def api_newsletter_subscribe():
    """Capture mekalav.com newsletter signups into Supabase."""
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"error": "Valid email required"}), 400
    source = (data.get("source") or "mekalav").strip()[:50]
    try:
        existing = (lib._sb().table("newsletter_signups")
                    .select("id").eq("email", email).limit(1).execute().data or [])
        if not existing:
            lib._sb().table("newsletter_signups").insert({
                "email": email, "source": source
            }).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/elections")
def elections_page():
    resp = app.make_response(
        render_template("index.html", prefill_company=None, prefill_doc=None, autoscreen="elections")
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.route("/elections/council-test")
def elections_council_test():
    """Standalone test page — seats by party for any council."""
    return ("""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Council Results · Test</title>
<style>
body{font-family:system-ui,sans-serif;max-width:720px;margin:40px auto;padding:0 16px;background:#f8fafc;color:#0f172a}
h1{font-size:1.4rem;margin-bottom:4px}
.sub{color:#64748b;font-size:.9rem;margin-bottom:20px}
.banner{background:#fef3c7;border:1px solid #f59e0b;border-radius:6px;padding:8px 12px;font-size:.85rem;margin-bottom:18px}
.row{display:flex;gap:8px;margin-bottom:20px}
input{flex:1;padding:10px 14px;border:1.5px solid #cbd5e1;border-radius:8px;font-size:1rem}
button{padding:10px 20px;background:#1e293b;color:#fff;border:none;border-radius:8px;font-size:1rem;cursor:pointer}
button:disabled{opacity:.5;cursor:default}
#status{margin-bottom:14px;font-size:.9rem;color:#475569}
.bar-wrap{display:flex;height:30px;border-radius:8px;overflow:hidden;margin-bottom:18px;border:1px solid #e2e8f0}
.bar-seg{display:flex;align-items:center;justify-content:center;font-size:.68rem;font-weight:700;overflow:hidden;white-space:nowrap;transition:width .4s}
table{width:100%;border-collapse:collapse;margin-bottom:24px}
th{text-align:left;font-size:.72rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.04em;padding:6px 8px;border-bottom:2px solid #e2e8f0}
td{padding:8px;border-bottom:1px solid #f1f5f9;font-size:.9rem}
.dot{display:inline-block;width:11px;height:11px;border-radius:50%;margin-right:6px;vertical-align:middle}
.declared{color:#16a34a;font-size:.78rem;font-weight:700}
.pending{color:#f59e0b;font-size:.78rem}
.wd-section{margin-top:20px}
.wd-section h3{font-size:.95rem;color:#475569;margin-bottom:8px}
.seats-big{font-size:1.4rem;font-weight:800}
</style>
</head>
<body>
<h1>Council Seat Results</h1>
<div class="banner">⚠ Test build — not part of main Miru site</div>
<p class="sub">Enter a council name to see seats by party (live from Democracy Club).</p>
<div class="row">
  <input id="q" type="text" placeholder="e.g. West Surrey, Adur, Norfolk…" list="clist" autocomplete="off" />
  <datalist id="clist"></datalist>
  <button id="btn" onclick="load()">Search</button>
</div>
<div id="status"></div>
<div id="results"></div>
<script>
function partyColor(name){
  const n=(name||"").toLowerCase();
  if(n.includes("conservative"))return["#0087dc","#fff"];
  if(n.includes("labour"))return["#e4003b","#fff"];
  if(n.includes("liberal")||n.includes("lib dem"))return["#faa61a","#000"];
  if(n.includes("green"))return["#02a95b","#fff"];
  if(n.includes("reform"))return["#12b6cf","#fff"];
  if(n.includes("snp")||n.includes("scottish national"))return["#fdf38e","#000"];
  if(n.includes("plaid"))return["#3f8428","#fff"];
  if(n.includes("independent"))return["#6b7280","#fff"];
  return["#9ca3af","#000"];
}
async function load(){
  const q=document.getElementById("q").value.trim();
  if(!q)return;
  const btn=document.getElementById("btn");
  btn.disabled=true;
  document.getElementById("status").textContent="Fetching from Democracy Club — may take a few seconds for large councils…";
  document.getElementById("results").innerHTML="";
  try{
    const res=await fetch("/api/elections/council-view?q="+encodeURIComponent(q));
    const data=await res.json();
    if(data.error){document.getElementById("status").innerHTML="<span style='color:#dc2626'>"+data.error+"</span>";return;}
    render(data);
  }catch(e){document.getElementById("status").textContent="Network error: "+e.message;}
  finally{btn.disabled=false;}
}
function render(d){
  const dec=d.declared_wards,tot=d.total_wards;
  const pct=tot?Math.round(dec/tot*100):0;
  document.getElementById("status").innerHTML="<strong>"+d.council+"</strong> &mdash; "+dec+"/"+tot+" wards declared ("+pct+"%)";
  const totalSeats=d.seats.reduce((s,p)=>s+p.seats,0);
  // Bar
  let bar='<div class="bar-wrap">';
  for(const p of d.seats){
    const w=totalSeats?(p.seats/totalSeats*100).toFixed(1):0;
    const[bg,fg]=partyColor(p.party);
    bar+=`<div class="bar-seg" style="width:${w}%;background:${bg};color:${fg}" title="${p.party}: ${p.seats}">${p.seats>2?p.seats:""}</div>`;
  }
  bar+="</div>";
  // Party table
  let tbl='<table><thead><tr><th>Party</th><th style="text-align:right">Seats</th></tr></thead><tbody>';
  for(const p of d.seats){
    const[bg]=partyColor(p.party);
    tbl+=`<tr><td><span class="dot" style="background:${bg}"></span>${p.party}</td><td style="text-align:right"><span class="seats-big">${p.seats}</span></td></tr>`;
  }
  tbl+="</tbody></table>";
  // Ward breakdown
  let wd='<div class="wd-section"><h3>Ward breakdown</h3><table><thead><tr><th>Ward</th><th>Status</th><th>Winner(s)</th></tr></thead><tbody>';
  for(const w of d.wards){
    const st=w.failed
      ?'<span style="color:#9ca3af;font-size:.78rem">⚠ Load failed</span>'
      :w.declared?'<span class="declared">&#10003; Declared</span>':'<span class="pending">Pending</span>';
    const wins=w.winners.map(x=>{const[bg]=partyColor(x.party);return`<span class="dot" style="background:${bg}"></span>${x.name}`;}).join(", ");
    wd+=`<tr${w.failed?' style="opacity:.55"':''}><td>${w.ward}</td><td>${st}</td><td>${wins||"&mdash;"}</td></tr>`;
  }
  wd+="</tbody></table></div>";
  document.getElementById("results").innerHTML=bar+tbl+wd;
}
document.getElementById("q").addEventListener("keydown",e=>{if(e.key==="Enter")load();});
// Populate autocomplete from server
fetch("/api/elections/council-list").then(r=>r.json()).then(list=>{
  const dl=document.getElementById("clist");
  list.forEach(c=>{const o=document.createElement("option");o.value=c.name;dl.appendChild(o);});
});
</script>
</body>
</html>""", 200, {"Content-Type": "text/html; charset=utf-8"})


@app.route("/test-places")
def test_places():
    resp = app.make_response(
        render_template("index.html", prefill_company=None, prefill_doc=None, autoscreen="places")
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.route("/doc/<share_id>")
def doc_page(share_id):
    return render_template("index.html", prefill_company=None, prefill_doc=share_id)

@app.route("/intel")
def intel_page():
    return render_template("index.html", prefill_company=None, prefill_doc=None, intel_mode=True)

@app.route("/<company_slug>")
def company_page(company_slug):
    return render_template("index.html", prefill_company=company_slug.replace("-", " "), prefill_doc=None)


# ── Library API ───────────────────────────────────────────────────────────────

def _check_library_pin():
    """Return 401 if unauthenticated, else None. Accepts ADMIN_PASSWORD or any 20-char user token."""
    pw = os.environ.get("ADMIN_PASSWORD", "")
    supplied = (request.headers.get("X-Library-PIN")
                or request.headers.get("X-Admin-Password")
                or request.args.get("pin", "")
                or request.args.get("pw", ""))
    if not pw:
        return None  # dev mode — open access
    if supplied == pw:
        return None  # admin password correct
    if supplied and len(supplied) == 20:
        return None  # valid 20-char user token (HMAC from phone number)
    return jsonify({"error": "Password required", "auth": True}), 401


def _library_user_token():
    """Return the per-user library token from the request, or None if admin/open."""
    pw = os.environ.get("ADMIN_PASSWORD", "")
    supplied = (request.headers.get("X-Library-PIN")
                or request.headers.get("X-Admin-Password")
                or request.args.get("pin", ""))
    if supplied and supplied != pw and len(supplied) == 20:
        return supplied
    return None


def _check_saves_pin():
    """Auth for wa-saves mutations. Accepts admin PW (all saves) OR user token (own saves only).
    Returns (from_number_or_None, error_response_or_None).
    from_number=None means admin (unrestricted). error!=None means reject request."""
    admin_pw = os.environ.get("ADMIN_PASSWORD", "")
    supplied = (request.headers.get("X-Library-PIN")
                or request.headers.get("X-Admin-Password")
                or request.args.get("pin", ""))
    if not admin_pw:
        from_number = _resolve_saves_token(supplied) if supplied else None
        return from_number, None
    if supplied == admin_pw:
        return None, None  # admin, unrestricted
    if supplied:
        from_number = _resolve_saves_token(supplied)
        if from_number:
            return from_number, None  # valid user token
    return None, (jsonify({"error": "Password required", "auth": True}), 401)


def _resolve_saves_token(token: str):
    """Resolve a user token to from_number. Fast path: verify against X-User-Phone header if present."""
    phone_hint = request.headers.get("X-User-Phone", "").strip()
    if phone_hint:
        expected = _wa_user_token(phone_hint)
        if expected == token:
            return phone_hint  # verified — no DB needed
    return _resolve_user_token(token)


@app.route("/api/library/documents")
def api_library_list():
    err = _check_library_pin()
    if err: return err
    try:
        docs = lib.list_documents(user_token=_library_user_token())
        return jsonify(docs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/library/upload", methods=["POST"])
def api_library_upload():
    err = _check_library_pin()
    if err: return err
    try:
        title = request.form.get("title", "Untitled").strip() or "Untitled"
        doc_type = request.form.get("doc_type", "note")
        text = ""
        page_count = 0

        if doc_type == "pdf" and "file" in request.files:
            f = request.files["file"]
            raw = f.read()
            try:
                import fitz
                pdf = fitz.open(stream=raw, filetype="pdf")
                text = "\n".join(page.get_text() for page in pdf)
                page_count = len(pdf)
                if not title or title == "Untitled":
                    title = f.filename.replace(".pdf", "").replace("_", " ").replace("-", " ").title()
            except Exception as e:
                return jsonify({"error": f"PDF extraction failed: {e}"}), 400
        elif doc_type == "image" and "file" in request.files:
            f = request.files["file"]
            raw = f.read()
            import base64
            if not title or title == "Untitled":
                title = f.filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " ").title() or "Photo"
            mime = (f.content_type or "").lower()
            # HEIC/HEIF not supported by vision APIs — convert via Pillow or reject
            if "heic" in mime or "heif" in mime or f.filename.lower().endswith((".heic", ".heif")):
                try:
                    from PIL import Image as _PILImg
                    import io as _io
                    _img = _PILImg.open(_io.BytesIO(raw))
                    _buf = _io.BytesIO()
                    _img.convert("RGB").save(_buf, format="JPEG")
                    raw = _buf.getvalue()
                    mime = "image/jpeg"
                except Exception:
                    return jsonify({"error": "HEIC photos aren't supported. Export as JPG from Photos first."}), 400
            if not mime or mime == "application/octet-stream":
                mime = "image/jpeg"
            if not os.environ.get("GROQ_API_KEY"):
                return jsonify({"error": "Image OCR not configured (no Groq key). Upload a PDF or paste text instead."}), 500
            img_b64 = base64.b64encode(raw).decode()
            try:
                extracted = _groq_vision(
                    img_b64, mime,
                    "Extract ALL text visible in this image exactly as shown. "
                    "If this is a receipt, include store name, items, prices, totals, date. "
                    "Return only the extracted text, no commentary."
                )
                text = extracted or ""
            except Exception as vision_err:
                app.logger.warning(f"[library upload] vision OCR failed: {vision_err}")
                text = f"[Photo: {title}]\n(Text extraction failed — try uploading a clearer image or paste the text manually.)"
            doc_type = "note"
        else:
            text = request.form.get("text", "").strip()
            doc_type = "note"

        if not text and doc_type != "note":
            return jsonify({"error": "No content found"}), 400
        if not text:
            text = f"[{title}]"

        doc = lib.upload_document(title, text, doc_type, page_count, user_token=_library_user_token())
        return jsonify(doc)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/library/doc/<share_id>")
def api_library_doc(share_id):
    err = _check_library_pin()
    if err: return err
    try:
        doc = lib.get_document(share_id)
        if not doc:
            return jsonify({"error": "Not found"}), 404
        doc.pop("chunks", None)
        return jsonify(doc)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _generate_follow_up_questions(title: str, question: str, answer: str) -> list:
    """Generate 3 follow-up questions based on the Q&A exchange."""
    import json, re as _re2
    if not os.environ.get("GROQ_API_KEY"):
        return []
    prompt = f"""Document: "{title}"

The user asked: "{question}"
The answer was: "{answer[:600]}"

Generate 3 natural follow-up questions that deepen understanding of this topic within the document.
Rules: under 12 words each, directly relevant to what was just discussed, no numbering.
Return ONLY a JSON array of 3 strings.
Example: ["What caused this?", "How does this compare to last year?", "What should be done next?"]"""
    try:
        reply = _groq_chat("You are a helpful document reading assistant.",
                           [{"role": "user", "content": prompt}], max_tokens=200)
        m = _re2.search(r'\[.*?\]', reply, _re2.DOTALL)
        if m:
            qs = json.loads(m.group(0))
            return [q for q in qs if isinstance(q, str) and len(q) > 5][:3]
    except Exception as e:
        print(f"[follow_up] {e}")
    return []


def _generate_doc_questions(title: str, text: str) -> list:
    """Generate 3 tailored suggested questions using Groq."""
    import json, re as _re2
    if not text or not os.environ.get("GROQ_API_KEY"):
        return []
    prompt = f"""Document title: "{title}"

Excerpt:
{text[:1400]}

Generate exactly 3 short, specific questions a reader would naturally want answered from this document.
Rules: under 12 words each, must be answerable from the content, no numbering.
Return ONLY a JSON array of 3 strings.
Example: ["What is the main conclusion?", "What evidence supports this?", "What action is recommended?"]"""
    try:
        reply = _groq_chat("You are a helpful document reading assistant.",
                           [{"role": "user", "content": prompt}], max_tokens=220)
        m = _re2.search(r'\[.*?\]', reply, _re2.DOTALL)
        if m:
            qs = json.loads(m.group(0))
            return [q for q in qs if isinstance(q, str) and len(q) > 5][:3]
    except Exception as e:
        print(f"[doc_questions] {e}")
    return []


@app.route("/api/library/doc-questions/<share_id>")
def api_library_doc_questions(share_id):
    err = _check_library_pin()
    if err: return err
    doc = lib.get_document(share_id)
    if not doc:
        return jsonify({"questions": []}), 404
    chunks = doc.get("chunks", [])
    if chunks:
        # Sample beginning, middle, and end for better coverage of large docs
        n = len(chunks)
        sampled = chunks[:2] + (chunks[n//2:n//2+2] if n > 4 else []) + (chunks[-1:] if n > 2 else [])
        text = " ".join(c["content"] for c in sampled)[:1400]
    else:
        text = (doc.get("text_content") or "")[:1400]
    questions = _generate_doc_questions(doc.get("title", ""), text)
    return jsonify({"questions": questions})


@app.route("/api/library/chat", methods=["POST"])
def api_library_chat():
    err = _check_library_pin()
    if err: return err
    try:
        data = request.get_json()
        share_id = data.get("share_id", "")
        question = data.get("question", "").strip()
        history  = data.get("history", [])
        if not share_id or not question:
            return jsonify({"error": "Missing share_id or question"}), 400

        doc = lib.get_document(share_id)
        if not doc:
            return jsonify({"error": "Document not found"}), 404

        # Use Algolia semantic search to find the most relevant chunks for this question.
        # Falls back to keyword sort if Algolia is unavailable.
        _complex = any(w in question.lower() for w in ("compare", "summar", "all", "every", "list", "overview", "explain"))
        top_n = 6 if _complex else 4
        relevant = lib.search_doc_chunks(share_id, doc["id"], question, n=top_n)
        if relevant:
            context = "\n\n---\n\n".join(relevant)
        else:
            context = (doc.get("text_content") or "")[:4000]

        system = (
            f'You are a helpful assistant. Answer questions ONLY based on the document "{doc["title"]}".\n\n'
            f'Document content:\n{context}\n\n'
            f'Answer concisely. If the answer is not in the document, say so briefly.'
        )
        messages = history[-6:] + [{"role": "user", "content": question}]
        answer = _groq_chat(system, messages, max_tokens=400)
        follow_ups = _generate_follow_up_questions(doc["title"], question, answer)
        return jsonify({"answer": answer, "follow_ups": follow_ups})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/library/delete/<share_id>", methods=["DELETE"])
def api_library_delete(share_id):
    err = _check_library_pin()
    if err: return err
    try:
        lib.delete_document(share_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/library/download/<share_id>")
def api_library_download(share_id):
    err = _check_library_pin()
    if err: return err
    from flask import Response
    doc = lib.get_document(share_id)
    if not doc:
        return "Not found", 404
    filename = (doc.get("title") or "document").replace(" ", "_").replace("/", "-") + ".txt"
    content = doc.get("text_content") or "\n".join(c["content"] for c in doc.get("chunks", []))
    return Response(
        content,
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/api/library/reindex")
def api_library_reindex():
    try:
        result = lib.reindex_all()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


_tweets_cache = {"data": None, "ts": 0}
_NITTER = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.1d4.us",
    "https://nitter.net",
]

@app.route("/api/tweets")
def api_tweets():
    import xml.etree.ElementTree as ET

    if _tweets_cache["data"] and (time.time() - _tweets_cache["ts"]) < 1800:
        resp = jsonify(_tweets_cache["data"])
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    ua = {"User-Agent": "Mozilla/5.0"}
    for base in _NITTER:
        try:
            r = requests.get(f"{base}/mekalav/rss", timeout=8, headers=ua)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.text)
            tweets = []
            for item in root.findall(".//item")[:10]:
                link  = item.findtext("link") or ""
                date  = item.findtext("pubDate") or ""
                desc  = item.findtext("description") or item.findtext("title") or ""
                text  = re.sub(r'<[^>]+>', '', desc).strip()
                tweets.append({"text": text, "link": link, "date": date})
            if tweets:
                _tweets_cache["data"] = tweets
                _tweets_cache["ts"] = time.time()
                resp = jsonify(tweets)
                resp.headers["Access-Control-Allow-Origin"] = "*"
                return resp
        except Exception:
            continue

    return jsonify({"error": "Could not fetch tweets"}), 503


@app.route("/api/library/search")
def api_library_search():
    err = _check_library_pin()
    if err: return err
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    try:
        return jsonify(lib.search_library(q))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/library/ask", methods=["POST"])
def api_library_ask():
    err = _check_library_pin()
    if err: return err
    try:
        data = request.get_json()
        question = data.get("question", "").strip()
        history  = data.get("history", [])
        if not question:
            return jsonify({"error": "Missing question"}), 400

        chunks = lib.search_all_chunks(question)
        if not chunks:
            return jsonify({"answer": "I couldn't find anything relevant in your library. Try rephrasing or uploading more documents.", "sources": []})

        context = "\n\n---\n\n".join(
            f'[From: {c["title"]}]\n{c["content"]}' for c in chunks
        )
        sources = list({c["share_id"]: c["title"] for c in chunks}.items())

        system = (
            "You are a helpful assistant with access to the user's personal document library. "
            "Answer the question using the relevant excerpts below. "
            "Cite which document your answer comes from.\n\n"
            f"Relevant excerpts:\n{context}"
        )
        messages = history[-6:] + [{"role": "user", "content": question}]
        answer = _groq_chat(system, messages, max_tokens=400)
        return jsonify({
            "answer":  answer,
            "sources": [{"share_id": sid, "title": t} for sid, t in sources],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _resolve_postcode(postcode):
    """Shared helper: validate postcode, return (postcode, lat, lon, pc_fmt) or None."""
    postcode = postcode.strip().upper().replace(" ", "")
    latlon = postcode_to_latlon(postcode)
    if not latlon:
        return None
    lat, lon = latlon
    pc_fmt = f"{postcode[:-3]} {postcode[-3:]}" if len(postcode) >= 5 else postcode
    return postcode, lat, lon, pc_fmt


@app.route("/api/search")
def api_search():
    import concurrent.futures as _cf
    result = _resolve_postcode(request.args.get("postcode", ""))
    if not result:
        return jsonify({"error": "Postcode not found. Please check and try again."}), 404
    postcode, lat, lon, pc_fmt = result
    analytics.log_search("fuel", postcode, request.remote_addr, request.user_agent.string)

    fuel   = request.args.get("fuel", "petrol").lower()
    radius = float(request.args.get("radius", 5))
    if fuel not in ("petrol", "diesel"): fuel = "petrol"
    radius = min(max(radius, 1), 20)

    cache_key = f"{postcode}:{fuel}:{radius}"
    cached = _fuel_search_cache.get(cache_key)
    if cached:
        payload, ts = cached
        if time.time() - ts < _FUEL_SEARCH_TTL:
            return jsonify(payload)

    radius_km = radius * 1.60934
    nearby = _nearby_stations(lat, lon, fuel, radius_km)
    nearby.sort(key=lambda x: (x["price"], x["dist_mi"]))
    avg = round(sum(s["price"] for s in nearby) / len(nearby), 1) if nearby else 0

    rightmove_url = f"https://www.rightmove.co.uk/house-prices/{pc_fmt.lower().replace(' ', '-')}.html"
    payload = {
        "postcode": postcode, "pc_fmt": pc_fmt,
        "fuel": fuel, "radius": radius,
        "stations": nearby[:10], "avg_price": avg,
        "rightmove_url": rightmove_url,
        "timestamp": datetime.now().isoformat(),
    }
    _fuel_search_cache[cache_key] = (payload, time.time())
    return jsonify(payload)


@app.route("/api/house")
def api_house():
    """Fast endpoint: house prices only (Land Registry)."""
    result = _resolve_postcode(request.args.get("postcode", ""))
    if not result:
        return jsonify({"error": "Postcode not found."}), 404
    postcode, lat, lon, pc_fmt = result
    analytics.log_search("area", postcode, request.remote_addr, request.user_agent.string)
    house = fetch_house_prices(postcode)
    rightmove_url = f"https://www.rightmove.co.uk/house-prices/{pc_fmt.lower().replace(' ', '-')}.html"
    return jsonify({"house_prices": house, "rightmove_url": rightmove_url})


@app.route("/api/local")
def api_local():
    """Slower endpoint: schools (Google Places), pubs, cafes from Overpass + Google Places phone enrichment."""
    result = _resolve_postcode(request.args.get("postcode", ""))
    if not result:
        return jsonify({"error": "Postcode not found."}), 404
    postcode, lat, lon, pc_fmt = result
    analytics.log_search("area", postcode, request.remote_addr, request.user_agent.string)

    # Schools via Google Places (more complete than OSM); pubs/cafes still from Overpass
    from concurrent.futures import ThreadPoolExecutor as _TPE
    with _TPE(max_workers=2) as ex:
        schools_f = ex.submit(fetch_nearby_schools, lat, lon, 5.0)
        local_f   = ex.submit(fetch_local_amenities, lat, lon, 5.0, 5.0)
    schools_data = schools_f.result()
    local        = local_f.result()

    # Fill missing phone numbers from Google Places for top pubs, cafes, and schools
    pubs    = local.get("pubs",  [])[:5]
    cafes   = local.get("cafes", [])[:5]
    schools = (schools_data.get("schools") or [])[:5]
    if _GOOGLE_PLACES_KEY:
        def _enrich(entry):
            if entry.get("phone"):
                return entry
            d = _lookup_venue(entry["name"], entry.get("lat"), entry.get("lon"))
            if d.get("phone"):
                entry = dict(entry)
                entry["phone"] = d["phone"]
            return entry
        try:
            combined = pubs + cafes + schools
            with ThreadPoolExecutor(max_workers=6) as ex:
                combined = list(ex.map(_enrich, combined))
            n_pubs = len(pubs); n_cafes = len(cafes)
            pubs    = combined[:n_pubs]
            cafes   = combined[n_pubs:n_pubs + n_cafes]
            schools = combined[n_pubs + n_cafes:]
        except Exception:
            pass
    schools_data = {**schools_data, "schools": schools}

    return jsonify({
        "schools": schools_data,
        "pubs":    pubs,
        "cafes":   cafes,
    })



_KAGI_CACHE: dict = {}
# Map our category names to Kagi's stable categoryId slugs
_KAGI_SLUG_MAP = {
    "business":   "business",
    "world":      "world",
    "uk":         "uk",
    "technology": "tech",
    "ai":         "ai",
    "science":    "science",
    "finance":    "economy",
    "sports":     "sports",
}
_KAGI_CAT_IDS: dict = {}   # populated dynamically, refreshed with news cache
_KAGI_CAT_IDS_TS: float = 0

def _kagi_resolve_id(slug: str) -> str:
    """Look up the current UUID for a Kagi category slug. Refreshes every 30 min."""
    import time as _time, requests as _req
    global _KAGI_CAT_IDS, _KAGI_CAT_IDS_TS
    if not _KAGI_CAT_IDS or _time.time() - _KAGI_CAT_IDS_TS > 1800:
        try:
            r = _req.get("https://kite.kagi.com/api/batches/latest/categories",
                         params={"lang": "en"}, timeout=8, allow_redirects=True)
            cats = r.json().get("categories", [])
            # Support both {categoryId, id} and flat {id, slug} response shapes
            new_ids = {}
            for c in cats:
                cid = c.get("categoryId") or c.get("slug") or ""
                uid = c.get("id") or cid
                if cid:
                    new_ids[cid] = uid
            if new_ids:
                _KAGI_CAT_IDS = new_ids
                _KAGI_CAT_IDS_TS = _time.time()
                print(f"[kagi] refreshed {len(_KAGI_CAT_IDS)} category IDs")
        except Exception as e:
            print(f"[kagi] category lookup failed: {e}")
    # Fall back to using the slug directly as the ID if lookup missed
    return _KAGI_CAT_IDS.get(slug, slug)

@app.route("/api/kagi-news")
def api_kagi_news():
    import time as _time
    category = request.args.get("category", "business").lower()
    slug = _KAGI_SLUG_MAP.get(category, category)
    cache_key = f"kagi:{category}"
    cached = _KAGI_CACHE.get(cache_key)
    if cached and _time.time() - cached["ts"] < 1800:
        resp = jsonify(cached["data"])
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    try:
        import requests as _req
        cat_id = _kagi_resolve_id(slug)
        r = _req.get(
            f"https://kite.kagi.com/api/batches/latest/categories/{cat_id}/stories",
            params={"limit": 9, "lang": "en"},
            timeout=10,
            allow_redirects=True,
        )
        raw = r.json()
        def _unwrap(link):
            """Strip kagiproxy.com wrapper and return the real article URL."""
            if not link:
                return link
            from urllib.parse import urlparse, parse_qs, unquote
            p = urlparse(link)
            if "kagiproxy" in p.netloc:
                qs = parse_qs(p.query)
                real = qs.get("url", [""])[0] or unquote(p.path.lstrip("/"))
                return real if real else link
            return link

        stories = []
        for s in raw.get("stories", []):
            img = s.get("primary_image") or {}
            articles = s.get("articles", [])
            stories.append({
                "title":    s.get("title", ""),
                "summary":  s.get("short_summary", ""),
                "emoji":    s.get("emoji", ""),
                "image":    img.get("url", ""),
                "sources":  len(articles),
                "url":      _unwrap(articles[0].get("link", "")) if articles else "",
                "domain":   articles[0].get("domain", "") if articles else "",
            })
        data = {"stories": stories, "category": category}
        _KAGI_CACHE[cache_key] = {"ts": _time.time(), "data": data}
    except Exception as e:
        print(f"[kagi-news] error: {e}")
        data = {"stories": [], "category": category}
    resp = jsonify(data)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/api/brand/debug")
def api_brand_debug():
    """Step-by-step trace of brand lookup — for debugging only."""
    import os, requests as _req
    name = request.args.get("name", "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    groq_key = os.environ.get("GROQ_API_KEY", "")
    trace = {"input": name}
    # Step 1: Groq canonicalization
    try:
        r = _req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant",
                  "messages": [{"role": "user", "content":
                      f'The user searched for brand/company: "{name}".\n'
                      'Return ONLY the canonical brand or company name. Rules:\n'
                      '1. Fix clear spelling errors only.\n'
                      '2. Expand obvious abbreviations.\n'
                      '3. If you do not recognise the brand, return the original UNCHANGED.\n'
                      '4. NEVER substitute a different brand — if unsure, return the input as-is.\n'
                      'Return ONLY the name, nothing else.'}],
                  "max_tokens": 40, "temperature": 0.0},
            timeout=6)
        resolved = r.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
        orig_words = set(name.lower().split())
        res_words  = set(resolved.lower().split())
        overlap    = bool(orig_words & res_words)
        trace["groq_resolved"] = resolved
        trace["groq_word_overlap"] = overlap
        trace["groq_accepted"] = overlap or len(orig_words) <= 1
    except Exception as e:
        trace["groq_error"] = str(e)
    # Step 2: Wikipedia direct
    try:
        slug = _req.utils.quote(name.replace(" ", "_"))
        wr = _req.get(f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}", timeout=8)
        trace["wiki_direct_status"] = wr.status_code
        if wr.status_code == 200:
            wd = wr.json()
            trace["wiki_direct_title"] = wd.get("title")
            trace["wiki_direct_extract"] = (wd.get("extract") or "")[:200]
    except Exception as e:
        trace["wiki_direct_error"] = str(e)
    # Step 3: Wikipedia search fallback
    try:
        sr = _req.get("https://en.wikipedia.org/w/api.php",
            params={"action":"query","list":"search","srsearch":name,"srlimit":3,"format":"json"}, timeout=6)
        hits = sr.json().get("query", {}).get("search", [])
        trace["wiki_search_hits"] = [h["title"] for h in hits]
    except Exception as e:
        trace["wiki_search_error"] = str(e)
    return jsonify(trace)


def _save_brand_profile(data: dict):
    """Persist brand data to brand_profiles table (upsert on name)."""
    try:
        from datetime import datetime as _dt
        name = (data.get("name") or "").strip()
        if not name:
            return
        row = {
            "name":             name,
            "description":      data.get("description") or "",
            "tagline":          data.get("slogan") or "",
            "founded":          str(data.get("founded") or ""),
            "hq":               data.get("hq") or "",
            "industry":         data.get("industry") or "",
            "domain":           data.get("domain") or "",
            "wikipedia_url":    data.get("wiki_url") or "",
            "did_you_know":     data.get("did_you_know") or "",
            "raw_data":         data,
            "last_enriched_at": _dt.utcnow().isoformat() + "Z",
        }
        lib._sb().table("brand_profiles").upsert(row, on_conflict="name").execute()
        print(f"[brand_profiles] saved: {name}")
    except Exception as e:
        print(f"[brand_profiles] save failed (table may not exist yet): {e}")


@app.route("/api/brand/basic")
def api_brand_basic():
    """Fast endpoint: Wikipedia + news + community spins. Returns in ~3-5s."""
    name = request.args.get("name", "").strip()
    if not name or len(name) < 2:
        return jsonify({"error": "name required"}), 400
    import concurrent.futures as _cf
    with _cf.ThreadPoolExecutor(max_workers=3) as pool:
        wiki_f = pool.submit(_fetch_wikipedia, name)
        news_f = pool.submit(_fetch_news, name, "", 6)
        tp_f   = pool.submit(_fetch_trustpilot, name, "")
        wiki = {}
        try: wiki = wiki_f.result(timeout=8) or {}
        except Exception: pass
        news = []
        try: news = news_f.result(timeout=8) or []
        except Exception: pass
        trustpilot = {}
        try: trustpilot = tp_f.result(timeout=8) or {}
        except Exception: pass
        if not trustpilot and wiki.get("domain"):
            try: trustpilot = _fetch_trustpilot(name, wiki["domain"]) or {}
            except Exception: pass
    spins = []
    try:
        spins = lib._sb().table("brand_spins").select("caption,url,created_at") \
            .ilike("brand_name", name.strip()).order("created_at", desc=True).limit(20).execute().data or []
    except Exception:
        pass
    return jsonify({
        "name":        name,
        "description": wiki.get("description", ""),
        "extract":     wiki.get("extract", ""),
        "founded":     wiki.get("founded", ""),
        "hq":          wiki.get("hq", ""),
        "industry":    wiki.get("industry", ""),
        "domain":      wiki.get("domain", ""),
        "thumbnail":   wiki.get("thumbnail", ""),
        "wiki_url":    wiki.get("wiki_url", ""),
        "news":        news,
        "spins":       spins,
        "trustpilot":  trustpilot,
        "_partial":    True,
    })


@app.route("/api/brand/spin", methods=["POST"])
def api_brand_spin():
    data = request.get_json(silent=True) or {}
    brand   = (data.get("brand")   or "").strip()
    caption = (data.get("caption") or "").strip()
    url     = (data.get("url")     or "").strip()
    if not brand or not caption:
        return jsonify({"error": "brand and caption required"}), 400
    try:
        lib._sb().table("brand_spins").insert({
            "brand_name": brand,
            "caption":    caption,
            "url":        url or None,
        }).execute()
        return jsonify({"ok": True})
    except Exception as e:
        print(f"[brand_spins] insert failed: {e}")
        return jsonify({"error": "could not save spin"}), 500


@app.route("/api/brand")
def api_brand():
    name = request.args.get("name", "").strip()
    if not name or len(name) < 2:
        return jsonify({"error": "Brand name required"}), 400
    analytics.log_search("brand", name, request.remote_addr, request.user_agent.string)
    data = fetch_brand_data(name)
    if data.get("timeline") or data.get("competitors"):
        _save_brand_profile(data)
    return jsonify(data)


@app.route("/api/brand/save-to-library", methods=["POST"])
def api_brand_save_to_library():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    sb_key = f"brand:{name.lower()}|brandv29"
    d = {}
    try:
        rows = lib._sb().table("ai_cache").select("data").eq("key", sb_key).execute().data
        if rows:
            d = rows[0]["data"] or {}
    except Exception:
        pass
    display_name = d.get("name") or name
    lines = [f"# 🏷️ {display_name} — Brand Profile"]
    if d.get("description") or d.get("extract"):
        lines.append(f"\n{(d.get('description') or d.get('extract',''))[:500]}")
    for label, key in [("Founded", "founded"), ("HQ", "hq"), ("Industry", "industry"), ("Revenue", "revenue")]:
        if d.get(key): lines.append(f"**{label}:** {d[key]}")
    tl = d.get("timeline") or []
    if tl:
        lines.append("\n## Timeline")
        for e in tl[:10]:
            lines.append(f"- **{e.get('year','')}** — {e.get('title','')}: {e.get('description','')}")
    camps = d.get("campaigns") or []
    if camps:
        lines.append("\n## Famous Campaigns")
        for c in camps[:8]:
            lines.append(f"- **{c.get('name','')}** ({c.get('year','')}): {c.get('description','')}")
    rivals = d.get("competitors") or []
    if rivals:
        lines.append("\n## Main Rivals")
        for r in rivals[:6]:
            lines.append(f"- **{r.get('name','')}**: {r.get('description','')}")
    news = d.get("news") or []
    if news:
        lines.append("\n## Recent News")
        for n in news[:5]:
            lines.append(f"- {n.get('title','')} ({n.get('date','')})")
    lines.append(f"\n---\n_Source: intel.humanagency.co · {display_name}_")
    try:
        doc = lib.upload_document(f"🏷️ {display_name}", "\n".join(lines), doc_type="note")
        return jsonify({"ok": True, "share_id": doc["share_id"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/brand/ask", methods=["POST"])
def api_brand_ask():
    data = request.get_json(silent=True) or {}
    name     = (data.get("name")     or "").strip()
    question = (data.get("question") or "").strip()
    ctx      = data.get("context")   or {}
    if not name or not question:
        return jsonify({"error": "name and question required"}), 400

    desc      = (ctx.get("description") or ctx.get("extract") or "")[:300]
    timeline  = ctx.get("timeline")   or []
    campaigns = ctx.get("campaigns")  or []
    rivals    = ctx.get("competitors") or []
    tp        = ctx.get("trustpilot") or {}

    ctx_lines = [f"Brand: {name}"]
    if desc:      ctx_lines.append(f"About: {desc}")
    if timeline:  ctx_lines.append("History: " + " | ".join(f"{e.get('year','')} {e.get('title','')}" for e in timeline[:5]))
    if campaigns: ctx_lines.append("Famous campaigns: " + ", ".join(c.get("name","") for c in campaigns[:4]))
    if rivals:    ctx_lines.append("Main rivals: " + ", ".join(c.get("name","") for c in rivals[:5]))
    if tp.get("rating"): ctx_lines.append(f"Trustpilot: {tp['rating']}/5 ({tp.get('count',0):,} reviews)")

    try:
        answer = _groq_chat(
            f"You are a brand intelligence assistant. Answer questions about {name} in 2-3 sentences max. "
            f"Be specific and factual. Context:\n" + "\n".join(ctx_lines),
            [{"role": "user", "content": question}],
            max_tokens=220,
        )
        return jsonify({"answer": answer.strip()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Elections ─────────────────────────────────────────────────────────────────

def _fetch_polling_station(postcode: str) -> dict | None:
    """Scrape wheredoivote.co.uk. Returns station info, a 'not yet available' notice, or None."""
    import re as _re, html as _html
    try:
        pc = postcode.replace(" ", "").upper()
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0 (compatible; Miru/1.0)"
        r = s.get(f"https://wheredoivote.co.uk/postcode/{pc}/", timeout=8)
        if r.status_code != 200:
            return None

        # Case 1: council hasn't uploaded data yet — "printed on your polling card"
        if "printed on your polling card" in r.text:
            # Try to extract council name and phone
            plain = _html.unescape(_re.sub(r"<[^>]+>", " ", r.text))
            plain = _re.sub(r"\s+", " ", plain)
            phone_m = _re.search(r"call ([^o]+?) on ([\d\s]+)", plain)
            phone = phone_m.group(2).strip() if phone_m else None
            council_m = _re.search(r"contact ([^.]+?) electoral", plain, _re.I)
            council = council_m.group(1).strip() if council_m else None
            return {
                "name":        "Not yet available",
                "address":     "Check your poll card or contact your council",
                "phone":       phone,
                "council_contact": council,
                "not_available": True,
                "url":         r.url,
            }

        csrf_m = _re.search(r'name="csrfmiddlewaretoken".*?value="([^"]+)"', r.text)
        if not csrf_m:
            return None
        csrf = csrf_m.group(1)

        # Case 2: address selection form
        opts = [(v, t) for v, t in _re.findall(r'<option value="([^"]+)">([^<]+)</option>', r.text)
                if v.isdigit()]
        if opts:
            val, _ = opts[0]
            r2 = s.post(r.url, data={"csrfmiddlewaretoken": csrf, "address": val},
                        headers={"Referer": r.url}, timeout=8)
        else:
            r2 = r  # single-address postcode — result already on this page

        if "vote in person" not in r2.text:
            return None

        addr_m = _re.search(r'<address>(.*?)</address>', r2.text, _re.DOTALL)
        if not addr_m:
            return None
        addr_html = addr_m.group(1)
        lines = [_html.unescape(_re.sub(r"<[^>]+>", "", l)).strip()
                 for l in _re.split(r"<br\s*/?>", addr_html)]
        lines = [l for l in lines if l]
        if not lines:
            return None
        return {
            "name":    lines[0],
            "address": ", ".join(lines[1:]) if len(lines) > 1 else "",
            "url":     r2.url,
        }
    except Exception:
        return None

# Old district GSS → new merged council slug
_DISTRICT_TO_COUNCIL_SLUG = {
    # West Surrey (Guildford, Runnymede, Spelthorne, Surrey Heath, Waverley, Woking)
    "E07000209": "west-surrey",  # Guildford
    "E07000212": "west-surrey",  # Runnymede
    "E07000213": "west-surrey",  # Spelthorne
    "E07000214": "west-surrey",  # Surrey Heath
    "E07000216": "west-surrey",  # Waverley
    "E07000217": "west-surrey",  # Woking
    # East Surrey (Elmbridge, Epsom & Ewell, Mole Valley, Reigate & Banstead, Tandridge)
    "E07000207": "east-surrey",  # Elmbridge
    "E07000208": "east-surrey",  # Epsom and Ewell
    "E07000210": "east-surrey",  # Mole Valley
    "E07000211": "east-surrey",  # Reigate and Banstead
    "E07000215": "east-surrey",  # Tandridge
}
def _org_name_to_dc_slug(name: str) -> str:
    """Strip civic prefixes/suffixes from a council org name and return a DC-compatible slug."""
    import re
    n = name.strip()
    for prefix in [
        "London Borough of ", "Royal Borough of ",
        "Metropolitan Borough of ", "Borough of ", "City of ",
    ]:
        if n.startswith(prefix):
            n = n[len(prefix):]
            break
    # Strip trailing suffixes like " Council", " District Council", " Borough Council"
    for suffix in [" District Council", " Borough Council", " County Council",
                   " City Council", " Council"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)]
            break
    return re.sub(r'\s+', '-', n.strip().lower())

# New council slug → human-readable predecessor area list (for "no past results" message)
_COUNCIL_PREDECESSORS = {
    "west-surrey": ["Runnymede", "Guildford", "Spelthorne", "Surrey Heath", "Waverley", "Woking"],
    "east-surrey": ["Mole Valley", "Reigate & Banstead", "Tandridge", "Elmbridge", "Epsom & Ewell"],
}
# Old county GSS → county-level council slug
_COUNTY_TO_COUNCIL_SLUG = {
    "E10000030": "surrey",
    "E10000012": "essex",
    "E10000020": "norfolk",
    "E10000029": "suffolk",
    "E10000013": "gloucestershire",
    "E10000032": "west-sussex",
    "E10000011": "east-sussex",
}
# Unitary authority GSS → council slug
_UA_TO_COUNCIL_SLUG = {
    "E06000042": "milton-keynes",
    "E06000030": "swindon",
    "E06000034": "thurrock",
}


_DC_RESULTS_CACHE: dict = {}   # {key: (results, ts, has_real)}
_DC_BALLOT_LIST_CACHE: dict = {}  # {council_slug: (ballots, ts)}
_DC_RESULTS_TTL_FINAL  = 3600  # 1h once real votes are in
_DC_RESULTS_TTL_PENDING = 300  # 5 min while still null — so fresh DC data shows quickly

def _ward_to_slug(name: str) -> str:
    import re
    s = name.lower()
    s = re.sub(r"['’‘`]", "", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", "-", s.strip())

def _fetch_dc_ballot(ballot_paper_id: str) -> list:
    """Fetch results for a known ballot_paper_id directly — no fuzzy matching needed."""
    cache_key = f"dcb:{ballot_paper_id}"
    if cache_key in _DC_RESULTS_CACHE:
        cached, ts, has_real = _DC_RESULTS_CACHE[cache_key]
        ttl = _DC_RESULTS_TTL_FINAL if has_real else _DC_RESULTS_TTL_PENDING
        if time.time() - ts < ttl:
            return cached
    try:
        r = requests.get(
            f"https://candidates.democracyclub.org.uk/api/next/ballots/{ballot_paper_id}/",
            params={"format": "json"}, timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
        )
        if r.status_code != 200:
            return []
        candidacies = r.json().get("candidacies", [])
        results = []
        for c in candidacies:
            res = c.get("result") or {}
            results.append({
                "name":    c.get("person", {}).get("name", ""),
                "party":   c.get("party_name", ""),
                "elected": c.get("elected") or res.get("elected", False),
                "votes":   res.get("num_ballots") or res.get("votes"),
            })
        results.sort(key=lambda x: -(x["votes"] or 0))
        has_real = any(r["votes"] is not None or r["elected"] for r in results)
        if results:
            _DC_RESULTS_CACHE[cache_key] = (results, time.time(), has_real)
        return results
    except Exception as e:
        print(f"[dc_ballot] {e}")
        return []


def _fetch_dc_results(council_slug: str, ward_name: str, election_date: str) -> list:
    """Fetch results for a ward from Democracy Club API via fuzzy ward matching."""
    import time as _time
    cache_key = f"dcr:{council_slug}:{ward_name}:{election_date}"
    if cache_key in _DC_RESULTS_CACHE:
        cached, ts, has_real = _DC_RESULTS_CACHE[cache_key]
        ttl = _DC_RESULTS_TTL_FINAL if has_real else _DC_RESULTS_TTL_PENDING
        if _time.time() - ts < ttl:
            return cached
    try:
        from difflib import SequenceMatcher
        slug = council_slug.lower().replace(" ", "-")
        r = requests.get(
            f"https://candidates.democracyclub.org.uk/api/next/elections/local.{slug}.{election_date}/",
            params={"format": "json"}, timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
        )
        if r.status_code != 200:
            return []
        ballots = r.json().get("ballots", [])
        ward_slug = _ward_to_slug(ward_name)
        best, best_score = None, 0
        for b in ballots:
            parts = b["ballot_paper_id"].split(".")
            if len(parts) < 3:
                continue
            bslug = parts[2]
            score = SequenceMatcher(None, ward_slug, bslug).ratio()
            if score > best_score:
                best_score, best = score, b["ballot_paper_id"]
        if not best or best_score < 0.4:
            return []
        results = _fetch_dc_ballot(best)
        if results:
            _DC_RESULTS_CACHE[cache_key] = (results, _time.time(), any(r["votes"] is not None or r["elected"] for r in results))
        return results
    except Exception as e:
        print(f"[dc_results] {e}")
        return []


def _load_elections_csv():
    """Load candidates CSV; returns {by_gss: {...}, by_slug: {council: {ward: {...}}}}."""
    import csv as _csv
    path = os.path.join(os.path.dirname(__file__), "elections_candidates.csv")
    if not os.path.exists(path):
        return {"by_gss": {}, "by_slug": {}}
    by_gss, by_slug = {}, {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            gss = row.get("gss", "").strip()
            candidate = {
                "name":      row.get("person_name", "").strip(),
                "party":     row.get("party_name", "").strip(),
                "email":     row.get("email", "").strip().strip('"'),
                "twitter":   row.get("twitter_username", "").strip().strip('"'),
                "homepage":  row.get("homepage_url", "").strip().strip('"'),
                "statement": row.get("statement_to_voters", "").strip().strip('"'),
            }
            # Real GSS codes start with E/W/S/N followed by digits.
            # Pseudo-GSS like "WSY:bagshot-windlesham-chobham" must go to by_slug.
            real_gss = gss and bool(re.match(r"^[EWSN]\d", gss))
            if real_gss:
                if gss not in by_gss:
                    bp = row.get("ballot_paper_id", "")
                    by_gss[gss] = {
                        "ward": row.get("post_label", "").strip(),
                        "council": row.get("organisation_name", "").strip().strip('"'),
                        "election_date": row.get("election_date", "").strip(),
                        "ballot_paper_id": bp,
                        "candidates": [],
                    }
                by_gss[gss]["candidates"].append(candidate)
            else:
                # Reorganised councils (or pseudo-GSS): parse council/ward slug from ballot_paper_id
                # e.g. local.west-surrey.chertsey.2026-05-07
                bp = row.get("ballot_paper_id", "")
                parts = bp.split(".")
                if len(parts) >= 4:
                    council_slug = parts[1]
                    ward_slug = parts[2]
                    by_slug.setdefault(council_slug, {})
                    if ward_slug not in by_slug[council_slug]:
                        by_slug[council_slug][ward_slug] = {
                            "ward":           row.get("post_label", "").strip().strip('"'),
                            "council":        row.get("organisation_name", "").strip().strip('"'),
                            "election_date":  row.get("election_date", "").strip(),
                            "ballot_paper_id": bp,
                            "candidates": [],
                        }
                    by_slug[council_slug][ward_slug]["candidates"].append(candidate)
    return {"by_gss": by_gss, "by_slug": by_slug}


def _best_ward_match(ward_name: str, wards: dict) -> str | None:
    """Token-overlap match; weighted by token length.
    Tiebreaker: prefer ward whose total token weight is closest to the matched weight
    (i.e. prefer exact/short names over compound ones that share a token)."""
    tokens = set(re.sub(r"[^a-z0-9 ]", "", ward_name.lower()).split()) - {"and", "the", ""}
    best_slug, best_score, best_frac = None, 0, 0.0
    for slug, data in wards.items():
        wt = set(re.sub(r"[^a-z0-9 ]", "", data["ward"].lower()).split()) - {"and", "the", ""}
        st = set(slug.replace("-", " ").split())
        all_ward_tokens = wt | st
        matched = tokens & all_ward_tokens
        score = sum(len(t) for t in matched)
        if score == 0:
            continue
        total = sum(len(t) for t in all_ward_tokens)
        frac = score / total if total else 0.0
        if score > best_score or (score == best_score and frac > best_frac):
            best_score, best_frac, best_slug = score, frac, slug
    return best_slug if best_score > 0 else None


_ELECTIONS_DATA = None

def _get_elections():
    global _ELECTIONS_DATA
    if _ELECTIONS_DATA is None:
        _ELECTIONS_DATA = _load_elections_csv()
    return _ELECTIONS_DATA


# ── ai.humanagency.co content API ───────────────────────────────────────────
# Stored in Supabase site_config table so it survives Railway redeploys.
# SQL: CREATE TABLE IF NOT EXISTS site_config (key text PRIMARY KEY, value jsonb NOT NULL DEFAULT '{}');
_AIHA_EDIT_TOKEN = "aiha-2026-edit"

def _aiha_read():
    try:
        row = lib._sb().table("site_config").select("value").eq("key", "aiha_content").execute()
        if row.data:
            return row.data[0]["value"] or {}
    except Exception:
        pass
    return {}

def _aiha_write(data: dict):
    lib._sb().table("site_config").upsert({"key": "aiha_content", "value": data}).execute()

@app.route("/api/aiha/content", methods=["GET", "OPTIONS"])
def aiha_content_get():
    if request.method == "OPTIONS":
        return _cors(Response("", 204))
    return _cors(jsonify(_aiha_read()))

@app.route("/api/aiha/content", methods=["POST"])
def aiha_content_post():
    if request.headers.get("X-Edit-Token", "") != _AIHA_EDIT_TOKEN:
        return _cors(jsonify({"error": "Unauthorized"})), 401
    try:
        data = request.get_json(force=True, silent=True) or {}
        _aiha_write(data)
        return _cors(jsonify({"ok": True}))
    except Exception as e:
        return _cors(jsonify({"error": str(e)})), 500


# ── School settings web UI ───────────────────────────────────────────────────

@app.route("/school/settings")
def school_settings_page():
    return render_template("school_settings.html")


@app.route("/api/school/settings")
def school_settings_get():
    wa = request.args.get("wa", "").strip()
    if not wa:
        return _cors(jsonify({"error": "wa required"})), 400
    try:
        # Twilio stores numbers as "whatsapp:+44..." — try both formats
        candidates = [wa, f"whatsapp:{wa}"] if not wa.startswith("whatsapp:") else [wa]
        profiles = []
        for cand in candidates:
            profiles = school_service._get_profiles(from_number=cand)
            if profiles:
                break
        if not profiles:
            return _cors(jsonify({"error": "not found"})), 404
        result = []
        for p in profiles:
            gmail_connected = bool(p.get("gmail_refresh_token"))
            result.append({
                "id":              p["id"],
                "child_name":      p.get("child_name", ""),
                "school_name":     p.get("school_name", ""),
                "class_name":      p.get("class_name", ""),
                "teacher_name":    p.get("teacher_name", ""),
                "sender_emails":   p.get("sender_emails") or [],
                "gmail_connected":    gmail_connected,
                "gmail_token_error": bool(p.get("gmail_token_error")),
                "oauth_url":         _school_oauth_url(p["id"]),
            })
        return _cors(jsonify({"profiles": result}))
    except Exception as e:
        return _cors(jsonify({"error": str(e)})), 500


@app.route("/api/school/gmail-disconnect", methods=["POST", "OPTIONS"])
def school_gmail_disconnect():
    if request.method == "OPTIONS":
        return _cors(Response("", 204))
    data       = request.get_json(force=True, silent=True) or {}
    profile_id = data.get("profile_id", "").strip()
    if not profile_id:
        return _cors(jsonify({"error": "profile_id required"})), 400
    try:
        lib._sb().table("school_profiles").update(
            {"gmail_refresh_token": None}
        ).eq("id", profile_id).execute()
        return _cors(jsonify({"ok": True, "oauth_url": _school_oauth_url(profile_id)}))
    except Exception as e:
        return _cors(jsonify({"error": str(e)})), 500


@app.route("/api/school/settings/emails", methods=["POST", "OPTIONS"])
def school_settings_emails():
    if request.method == "OPTIONS":
        return _cors(Response("", 204))
    try:
        data       = request.get_json(force=True, silent=True) or {}
        profile_id = data.get("profile_id", "").strip()
        wa         = data.get("wa", "").strip()
        emails     = data.get("emails", [])
        if not profile_id or not wa:
            return _cors(jsonify({"error": "profile_id and wa required"})), 400
        if not isinstance(emails, list):
            return _cors(jsonify({"error": "emails must be a list"})), 400
        # Verify the profile belongs to this WA number before updating (try both Twilio formats)
        candidates = [wa, f"whatsapp:{wa}"] if not wa.startswith("whatsapp:") else [wa]
        verify_data = []
        for cand in candidates:
            r = lib._sb().table("school_profiles") \
                .select("id").eq("id", profile_id).eq("from_number", cand).execute()
            if r.data:
                verify_data = r.data
                break
        if not verify_data:
            return _cors(jsonify({"error": "profile not found or wa mismatch"})), 403
        lib._sb().table("school_profiles") \
            .update({"sender_emails": emails}) \
            .eq("id", profile_id).execute()
        return _cors(jsonify({"ok": True}))
    except Exception as e:
        return _cors(jsonify({"error": str(e)})), 500


@app.route("/api/school/settings/profile", methods=["POST", "OPTIONS"])
def school_settings_profile():
    if request.method == "OPTIONS":
        return _cors(Response("", 204))
    try:
        data       = request.get_json(force=True, silent=True) or {}
        profile_id = data.get("profile_id", "").strip()
        wa         = data.get("wa", "").strip()
        if not profile_id or not wa:
            return _cors(jsonify({"error": "profile_id and wa required"})), 400
        # Verify ownership
        candidates = [wa, f"whatsapp:{wa}"] if not wa.startswith("whatsapp:") else [wa]
        verified = False
        for cand in candidates:
            r = lib._sb().table("school_profiles") \
                .select("id").eq("id", profile_id).eq("from_number", cand).execute()
            if r.data:
                verified = True
                break
        if not verified:
            return _cors(jsonify({"error": "profile not found or wa mismatch"})), 403
        allowed = {"child_name", "class_name", "teacher_name"}
        updates = {k: v.strip() for k, v in data.items() if k in allowed and isinstance(v, str)}
        if not updates:
            return _cors(jsonify({"error": "nothing to update"})), 400
        lib._sb().table("school_profiles").update(updates).eq("id", profile_id).execute()
        return _cors(jsonify({"ok": True}))
    except Exception as e:
        return _cors(jsonify({"error": str(e)})), 500


@app.route("/api/school/fetch-now", methods=["POST", "OPTIONS"])
def school_fetch_now():
    if request.method == "OPTIONS":
        return _cors(Response("", 204))
    try:
        data = request.get_json(force=True, silent=True) or {}
        wa   = data.get("wa", "").strip() or request.headers.get("X-School-WA", "").strip()
        if not wa:
            return _cors(jsonify({"error": "wa required"})), 400
        wa = _normalise_from_number(wa)
        profiles = school_service._get_profiles(from_number=wa)
        if not profiles:
            return _cors(jsonify({"error": "no profiles found"})), 404
        profile_ids = [p["id"] for p in profiles]
        import threading
        threading.Thread(
            target=school_service.poll_all_profiles,
            kwargs={"days_back": 3, "force": True, "profile_ids": profile_ids, "on_error": _school_gmail_token_alert},
            daemon=True,
        ).start()
        return _cors(jsonify({"ok": True, "started": True}))
    except Exception as e:
        return _cors(jsonify({"error": str(e)})), 500


@app.route("/api/school/events/delete", methods=["POST", "OPTIONS"])
def school_event_delete():
    if request.method == "OPTIONS":
        return _cors(Response("", 204))
    try:
        data     = request.get_json(force=True, silent=True) or {}
        event_id = data.get("event_id", "").strip()
        wa       = data.get("wa", "").strip()
        if not event_id or not wa:
            return _cors(jsonify({"error": "event_id and wa required"})), 400
        wa = _normalise_from_number(wa)
        # Verify the event belongs to this parent before deleting
        ev = lib._sb().table("school_events").select("profile_id").eq("id", event_id).execute().data
        if not ev:
            return _cors(jsonify({"error": "event not found"})), 404
        profile_id = ev[0]["profile_id"]
        owns = lib._sb().table("school_profiles") \
            .select("id").eq("id", profile_id).eq("from_number", wa).execute().data
        if not owns:
            return _cors(jsonify({"error": "not authorized"})), 403
        lib._sb().table("school_events").delete().eq("id", event_id).execute()
        return _cors(jsonify({"ok": True}))
    except Exception as e:
        return _cors(jsonify({"error": str(e)})), 500


# ── National results: live pull from Democracy Club, 6-hour cache ────────────
# National results — live from Wikipedia infobox, 30-min cache
_NAT_CACHE: dict = {"data": None, "ts": 0.0}
_NAT_TTL = 1800  # 30 minutes while counting ongoing

_NAT_PARTY_META = {
    "reform uk":                        {"name": "Reform UK",    "short": "REF", "colour": "#06b6d4", "text": "#fff"},
    "liberal democrats (uk)":           {"name": "Lib Dems",     "short": "LIB", "colour": "#f59e0b", "text": "#000"},
    "liberal democrats":                {"name": "Lib Dems",     "short": "LIB", "colour": "#f59e0b", "text": "#000"},
    "conservative party (uk)":          {"name": "Conservative", "short": "CON", "colour": "#1d4ed8", "text": "#fff"},
    "labour party (uk)":                {"name": "Labour",       "short": "LAB", "colour": "#e11d48", "text": "#fff"},
    "green party of england and wales": {"name": "Green",        "short": "GRN", "colour": "#16a34a", "text": "#fff"},
}

def _fetch_national_wikipedia() -> dict | None:
    import re, time
    from datetime import datetime, timezone

    now = time.time()
    if _NAT_CACHE["data"] and now - _NAT_CACHE["ts"] < _NAT_TTL:
        return _NAT_CACHE["data"]

    try:
        r = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params={"action": "parse", "page": "2026_United_Kingdom_local_elections",
                    "format": "json", "prop": "wikitext", "section": "0"},
            timeout=10, headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
        )
        if r.status_code != 200:
            return None
        wt = r.json()["parse"]["wikitext"]["*"]
    except Exception as e:
        print(f"[national] Wikipedia error: {e}")
        return None

    def _field(name, idx):
        m = re.search(rf"\|\s*{name}{idx}\s*=\s*([^\n|]+)", wt)
        return m.group(1).strip() if m else ""

    def _parse_change(raw):
        s = re.sub(r"\{\{increase\}\}", "+", raw)
        s = re.sub(r"\{\{decrease\}\}", "-", s)
        s = re.sub(r"\{\{nochange\}\}", "0", s)
        return re.sub(r"[\s,]", "", s)

    parties = []
    for i in range(1, 10):
        raw = re.sub(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]", r"\1",
                     _field("party", i)).lower().strip()
        if not raw:
            break
        meta = next((m for k, m in _NAT_PARTY_META.items() if k in raw or raw in k), None)
        if not meta:
            continue
        try:
            cllrs = int(re.sub(r"[,\s]", "", _field("2data", i)))
            cncls = int(re.sub(r"[,\s]", "", _field("3data", i)) or "0")
        except ValueError:
            continue
        parties.append({
            "name": meta["name"], "short": meta["short"],
            "colour": meta["colour"], "text": meta["text"],
            "councils": cncls, "net": _parse_change(_field("5data", i)),
            "councillors": cllrs, "net_c": _parse_change(_field("4data", i)),
        })

    if not parties:
        return None

    result = {
        "updated":    datetime.now(timezone.utc).strftime("%-d %b %Y · %-H:%M UTC") + " · counting live",
        "source":     "Wikipedia",
        "source_url": "https://en.wikipedia.org/wiki/2026_United_Kingdom_local_elections",
        "headline":   "2026 UK local election results",
        "parties":    parties,
    }
    _NAT_CACHE["data"] = result
    _NAT_CACHE["ts"] = now
    return result

def _party_color_bg(party_name: str) -> tuple:
    """Return (bg_hex, fg_hex) for a UK political party name."""
    n = (party_name or "").lower()
    if "conservative" in n: return ("#0087dc", "#fff")
    if "labour" in n:       return ("#e4003b", "#fff")
    if "liberal" in n or "lib dem" in n: return ("#faa61a", "#000")
    if "green" in n:        return ("#02a95b", "#fff")
    if "reform" in n:       return ("#12b6cf", "#fff")
    if "snp" in n or "scottish national" in n: return ("#fdf38e", "#000")
    if "plaid" in n:        return ("#3f8428", "#fff")
    if "independent" in n:  return ("#6b7280", "#fff")
    return ("#9ca3af", "#000")


@app.route("/api/elections/council-view")
def api_elections_council_view():
    """Aggregate live seat results for a council from Democracy Club.
    GET ?q=west-surrey  (slug or natural name, fuzzy matched)
    """
    import concurrent.futures as _cf
    import time as _time

    q = request.args.get("q", "").strip().lower()
    if not q:
        return jsonify({"error": "Missing ?q= council name"}), 400

    elections = _get_elections()
    by_slug = elections.get("by_slug", {})

    # Build org_name → slug from first ward of each council entry
    org_slug_map: dict = {}
    for slug, wards in by_slug.items():
        for ward_data in wards.values():
            org = ward_data.get("council", "").strip().lower()
            if org:
                org_slug_map[org] = slug
            break

    # Resolve: direct slug → org name match → token overlap
    q_slug = re.sub(r"\s+", "-", q)
    council_slug = None
    if q_slug in by_slug:
        council_slug = q_slug
    else:
        for org, slug in org_slug_map.items():
            if q in org or org in q:
                council_slug = slug
                break
    if not council_slug:
        q_tokens = set(q.split())
        best_slug, best_score = None, 0
        for org, slug in org_slug_map.items():
            o_tokens = set(re.sub(r"[-]", " ", org).split())
            matched = len(q_tokens & o_tokens)
            if matched > best_score:
                best_score, best_slug = matched, slug
        if best_slug and best_score > 0:
            council_slug = best_slug

    if not council_slug:
        return jsonify({"error": f"No council found matching '{q}'. Try a slug like 'west-surrey' or name like 'West Surrey'."}), 404

    wards_dict = by_slug.get(council_slug, {})
    council_name = council_slug.replace("-", " ").title()
    for w in wards_dict.values():
        council_name = w.get("council", council_name)
        break
    election_date = "2026-05-07"

    # Step 1: fetch all ballot IDs for the council (cached 30 min)
    cache_list_key = f"dcel:{council_slug}:{election_date}"
    ballots = None
    if cache_list_key in _DC_BALLOT_LIST_CACHE:
        cached_ballots, cached_ts = _DC_BALLOT_LIST_CACHE[cache_list_key]
        if _time.time() - cached_ts < 1800:
            ballots = cached_ballots
    if ballots is None:
        try:
            r = requests.get(
                f"https://candidates.democracyclub.org.uk/api/next/elections/local.{council_slug}.{election_date}/",
                params={"format": "json"}, timeout=12,
                headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
            )
            if r.status_code != 200:
                return jsonify({"error": f"DC API returned {r.status_code} for '{council_slug}'. The council may not have elections on {election_date}."}), 502
            ballots = r.json().get("ballots", [])
            _DC_BALLOT_LIST_CACHE[cache_list_key] = (ballots, _time.time())
        except Exception as e:
            return jsonify({"error": str(e)}), 502

    if not ballots:
        return jsonify({"error": f"No ballots found for '{council_slug}' on {election_date}"}), 404

    total_wards = len(ballots)

    # Step 2: fetch each ballot's results in parallel
    # Use the URL from the ballot list directly to avoid constructing stale IDs.
    ballot_url_map = {b["ballot_paper_id"]: b.get("url", "").replace("http://", "https://").split("?")[0] for b in ballots}

    def fetch_ballot(ballot_paper_id):
        cache_key = f"dcb:{ballot_paper_id}"
        if cache_key in _DC_RESULTS_CACHE:
            cached, ts, _ = _DC_RESULTS_CACHE[cache_key]
            if _time.time() - ts < 3600:
                return ballot_paper_id, cached
        url = ballot_url_map.get(ballot_paper_id) or f"https://candidates.democracyclub.org.uk/api/next/ballots/{ballot_paper_id}/"
        for attempt in range(3):
            try:
                r2 = requests.get(url, params={"format": "json"}, timeout=6, headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"})
                if r2.status_code == 429:
                    _time.sleep(0.4 * (attempt + 1))
                    continue
                if r2.status_code != 200:
                    return ballot_paper_id, None
                jdata = r2.json()
                candidacies = jdata.get("candidacies", [])
                ward_label = jdata.get("post", {}).get("label", "")
                results = []
                for c in candidacies:
                    res = c.get("result") or {}
                    results.append({
                        "name":    c.get("person", {}).get("name", ""),
                        "party":   c.get("party_name", ""),
                        "elected": bool(c.get("elected") or res.get("elected")),
                        "votes":   res.get("num_ballots"),
                    })
                has_real = any(rc["votes"] is not None or rc["elected"] for rc in results)
                payload = {"results": results, "ward": ward_label, "declared": has_real}
                if results:
                    _DC_RESULTS_CACHE[cache_key] = (payload, _time.time(), has_real)
                return ballot_paper_id, payload
            except Exception:
                return ballot_paper_id, None
        return ballot_paper_id, None

    # Build a label map from the ballot list
    ballot_label: dict = {}
    for b in ballots:
        bpid = b["ballot_paper_id"]
        label = b.get("post", {}).get("label", "") if isinstance(b, dict) else ""
        if not label:
            parts = bpid.split(".")
            label = parts[2].replace("-", " ").title() if len(parts) >= 3 else bpid
        ballot_label[bpid] = label

    ballot_ids = [b["ballot_paper_id"] for b in ballots]
    ward_results: dict = {}
    with _cf.ThreadPoolExecutor(max_workers=5) as ex:
        for bpid, payload in ex.map(fetch_ballot, ballot_ids):
            ward_results[bpid] = payload  # None means fetch failed

    # Step 3: tally seats by party; include ALL wards (failed ones shown as "Unable to load")
    party_seats: dict = {}
    declared_wards = 0
    ward_summary = []
    for bpid in ballot_ids:
        payload = ward_results.get(bpid)
        fallback_label = ballot_label.get(bpid, bpid.split(".")[2].replace("-", " ").title() if "." in bpid else bpid)
        if payload is None:
            ward_summary.append({"ward": fallback_label, "declared": False, "winners": [], "failed": True})
            continue
        ward = payload.get("ward") or fallback_label
        declared = payload.get("declared", False)
        if declared:
            declared_wards += 1
        winners = [c for c in payload.get("results", []) if c.get("elected")]
        for w in winners:
            p = w["party"]
            party_seats[p] = party_seats.get(p, 0) + 1
        ward_summary.append({
            "ward":     ward,
            "declared": declared,
            "winners":  [{"name": w["name"], "party": w["party"]} for w in winners],
            "failed":   False,
        })

    seats_list = []
    for party, seats in sorted(party_seats.items(), key=lambda x: -x[1]):
        bg, fg = _party_color_bg(party)
        seats_list.append({"party": party, "seats": seats, "color": bg, "text": fg})

    return jsonify({
        "council":        council_name,
        "slug":           council_slug,
        "total_wards":    total_wards,
        "declared_wards": declared_wards,
        "seats":          seats_list,
        "wards":          sorted(ward_summary, key=lambda x: (x.get("failed", False), not x["declared"], x["ward"])),
    })


@app.route("/api/elections/council-list")
def api_elections_council_list():
    """Return all known council slugs + names for autocomplete."""
    elections = _get_elections()
    by_slug = elections.get("by_slug", {})
    councils = []
    for slug, wards in by_slug.items():
        name = slug.replace("-", " ").title()
        for w in wards.values():
            name = w.get("council", name)
            break
        councils.append({"slug": slug, "name": name})
    councils.sort(key=lambda x: x["name"])
    return jsonify(councils)


@app.route("/api/elections/national")
def api_elections_national():
    data = _fetch_national_wikipedia()
    if data:
        return jsonify(data)
    # Fallback if Wikipedia is unreachable
    return jsonify({
        "updated": "8 May 2026 · counting",
        "source": "Wikipedia", "source_url": "https://en.wikipedia.org/wiki/2026_United_Kingdom_local_elections",
        "headline": "2026 UK local election results",
        "parties": [
            {"name": "Reform UK",    "short": "REF", "colour": "#06b6d4", "text": "#fff", "councils": 5,  "net": "+5",  "councillors": 731, "net_c": "+729"},
            {"name": "Lib Dems",     "short": "LIB", "colour": "#f59e0b", "text": "#000", "councils": 10, "net": "+1",  "councillors": 481, "net_c": "+41"},
            {"name": "Conservative", "short": "CON", "colour": "#1d4ed8", "text": "#fff", "councils": 6,  "net": "-3",  "councillors": 427, "net_c": "-311"},
            {"name": "Labour",       "short": "LAB", "colour": "#e11d48", "text": "#fff", "councils": 18, "net": "-14", "councillors": 415, "net_c": "-548"},
            {"name": "Green",        "short": "GRN", "colour": "#16a34a", "text": "#fff", "councils": 0,  "net": "0",   "councillors": 178, "net_c": "+118"},
        ]
    })


def _load_elec_alerts():
    try:
        rows = lib._sb().table("election_alerts").select("*").execute().data or []
        return rows
    except Exception:
        return []

def _save_elec_alert_sent(wa: str, postcode: str):
    """Mark a specific alert as sent in Supabase."""
    try:
        lib._sb().table("election_alerts").update({"sent": True}) \
            .eq("wa", wa).eq("postcode", postcode).execute()
    except Exception:
        pass


@app.route("/api/elections/alert", methods=["POST"])
def api_elections_alert_register():
    data     = request.get_json(force=True) or {}
    postcode = data.get("postcode", "").strip().replace(" ", "").upper()
    wa       = data.get("wa_number", "").strip()
    if not postcode or not wa:
        return jsonify({"error": "postcode and wa_number required"}), 400
    try:
        existing = lib._sb().table("election_alerts").select("id") \
            .eq("postcode", postcode).eq("wa", wa).execute().data
        if not existing:
            lib._sb().table("election_alerts").insert({
                "postcode": postcode,
                "wa": wa,
                "sent": False,
                "registered": datetime.utcnow().isoformat(),
            }).execute()
    except Exception as e:
        app.logger.error(f"election alert register: {e}")
    return jsonify({"ok": True})


@app.route("/api/elections/check-alerts")
def api_elections_check_alerts():
    """Call every 15 min via cron-job.org after polls close on election night."""
    token = request.args.get("token", "")
    if token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "forbidden"}), 403

    alerts  = _load_elec_alerts()
    pending = [a for a in alerts if not a.get("sent")]
    if not pending:
        return jsonify({"checked": 0, "sent": 0})

    sent_count = 0
    for alert in pending:
        try:
            pc = alert["postcode"]
            r  = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=6)
            if r.status_code != 200:
                continue
            result       = r.json().get("result", {})
            district     = result.get("admin_district", "")
            ward_name    = result.get("admin_ward", "")
            election_date = "2026-05-07"
            effective_council = district.lower().replace(" ", "-")
            live = _fetch_dc_results(effective_council, ward_name, election_date)
            declared = live and any(c.get("votes") is not None for c in live)
            if not declared:
                continue
            # Build result summary
            winner = next((c for c in live if c.get("elected")), None)
            lines  = [f"🗳️ *{ward_name} result declared*"]
            if winner:
                lines.append(f"✅ *{winner['name']}* ({winner['party']}) elected")
            for c in sorted(live, key=lambda x: -(x.get("votes") or 0)):
                votes = f"{c['votes']:,}" if c.get("votes") else "—"
                lines.append(f"  {c['name']} ({c['party']}): {votes} votes")
            lines.append(f"\nView full results: https://miru.humanagency.co/elections")
            body = "\n".join(lines)
            wa_to = alert["wa"] if alert["wa"].startswith("+") else "+" + alert["wa"]
            _wa_send_proactive(f"whatsapp:{wa_to}", body)
            _save_elec_alert_sent(alert["wa"], alert["postcode"])
            sent_count += 1
        except Exception:
            continue

    return jsonify({"checked": len(pending), "sent": sent_count})


# ── Councillor helpers ─────────────────────────────────────────────────────────

def _fetch_dc_elected(council_slug: str, ward_name: str, election_date: str) -> list:
    """Fetch elected councillors for a ward from Democracy Club API.
    Returns list of dicts: {name, party, email, photo_url, council_profile_url}
    """
    from difflib import SequenceMatcher
    try:
        slug = council_slug.lower().replace(" ", "-")
        r = requests.get(
            f"https://candidates.democracyclub.org.uk/api/next/elections/local.{slug}.{election_date}/",
            params={"format": "json"}, timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
        )
        if r.status_code != 200:
            return []
        ballots = r.json().get("ballots", [])
        ward_slug = _ward_to_slug(ward_name)
        best, best_score = None, 0
        for b in ballots:
            parts = b["ballot_paper_id"].split(".")
            if len(parts) < 3:
                continue
            bslug = parts[2]
            score = SequenceMatcher(None, ward_slug, bslug).ratio()
            if score > best_score:
                best_score, best = score, b["ballot_paper_id"]
        if not best or best_score < 0.4:
            return []

        r2 = requests.get(
            f"https://candidates.democracyclub.org.uk/api/next/ballots/{best}/",
            params={"format": "json"}, timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
        )
        if r2.status_code != 200:
            return []
        candidacies = r2.json().get("candidacies", [])
        elected = [c for c in candidacies if c.get("elected")]
        results = []
        for c in elected:
            person_id = (c.get("person") or {}).get("id")
            name      = (c.get("person") or {}).get("name", "")
            party     = c.get("party_name", "")
            email = ""; photo_url = ""
            profile_url = f"https://candidates.democracyclub.org.uk/person/{person_id}/" if person_id else ""
            if person_id:
                try:
                    pr = requests.get(
                        f"https://candidates.democracyclub.org.uk/api/next/people/{person_id}/",
                        params={"format": "json"}, timeout=6,
                        headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
                    )
                    if pr.status_code == 200:
                        pd = pr.json()
                        email = pd.get("email", "") or ""
                        for ident in (pd.get("identifiers") or []):
                            if ident.get("value_type") == "email" and not email:
                                email = ident.get("value", "")
                        photo_url = ((pd.get("image") or {}).get("image_url") or "")
                        # Prefer actual council website link over DC profile page
                        for lnk in (pd.get("links") or []):
                            href = lnk.get("value") or lnk.get("url") or ""
                            ltype = (lnk.get("link_type") or lnk.get("type") or "").lower()
                            if href and ltype in ("homepage", "council", "council_profile") and not href.startswith("http://democracyclub"):
                                profile_url = href
                                break
                except Exception:
                    pass
            results.append({"name": name, "party": party, "email": email,
                            "photo_url": photo_url, "council_profile_url": profile_url})
        return results
    except Exception as e:
        app.logger.warning(f"[dc-elected] {e}")
        return []


_COUNCILLOR_TTL_DAYS = 90  # re-fetch after 90 days to catch by-elections

def _councillor_row(name: str, party: str, email: str, photo_url: str, profile_url: str,
                    ward_gss: str, ward: str, council: str, election_date: str) -> dict:
    return {
        "ward_gss":            ward_gss,
        "ward":                ward,
        "council":             council,
        "name":                name,
        "party":               party,
        "email":               email,
        "photo_url":           photo_url,
        "council_profile_url": profile_url,
        "elected_date":        election_date,
        "fetched_at":          datetime.utcnow().isoformat(),
    }


def _upsert_councillors(rows: list) -> int:
    """Upsert councillor rows to Supabase. Returns count saved."""
    if not rows:
        return 0
    try:
        lib._sb().table("councillors").upsert(
            rows, on_conflict="ward_gss,name"
        ).execute()
        return len(rows)
    except Exception as e:
        app.logger.warning(f"[councillors] upsert failed: {e}")
        return 0


def _get_councillors_for_ward(ward_gss: str) -> list:
    """Return DB councillors for a ward if present and fetched within TTL, else [] to trigger re-fetch."""
    try:
        rows = lib._sb().table("councillors").select("*").eq("ward_gss", ward_gss).execute().data
        if not rows:
            return []
        # Check freshness — use the oldest fetched_at in the set
        cutoff = datetime.utcnow().isoformat()[:10]  # today as YYYY-MM-DD
        from datetime import timedelta
        threshold = (datetime.utcnow() - timedelta(days=_COUNCILLOR_TTL_DAYS)).isoformat()
        oldest = min((r.get("fetched_at") or "1970-01-01" for r in rows))
        if oldest < threshold:
            return []  # stale — fall through to DC API re-fetch
        return rows
    except Exception:
        return []


_pc_meta_cache: dict = {}  # postcode → postcodes.io result dict, cached indefinitely (ward boundaries stable)

def _resolve_councillors(postcode: str) -> dict:
    """Core councillor lookup: DB first, DC API fallback. Returns dict with councillors/ward/council."""
    if postcode in _pc_meta_cache:
        result = _pc_meta_cache[postcode]
    else:
        r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=6)
        if r.status_code != 200:
            raise ValueError("Postcode not found")
        result = r.json().get("result", {})
        _pc_meta_cache[postcode] = result
    codes         = result.get("codes", {})
    ward_gss      = codes.get("admin_ward", "")
    ward_name     = result.get("admin_ward", "")
    council       = result.get("admin_district", "")
    district_code = codes.get("admin_district", "")
    ced_gss       = codes.get("ced", "")
    county_code   = codes.get("admin_county", "")
    if not ward_gss:
        return {"councillors": [], "ward": ward_name, "council": council}

    elections    = _get_elections()
    ward_data    = elections["by_gss"].get(ward_gss, {})
    council_slug = None

    # Fallback 1: county electoral division
    if not ward_data and ced_gss:
        ward_data = elections["by_gss"].get(ced_gss, {})

    # Fallback 2: reorganised/merged council — same logic as elections API
    if not ward_data:
        council_slug = (
            _DISTRICT_TO_COUNCIL_SLUG.get(district_code) or
            _UA_TO_COUNCIL_SLUG.get(district_code) or
            _COUNTY_TO_COUNCIL_SLUG.get(county_code)
        )
        if council_slug:
            slug_wards = elections["by_slug"].get(council_slug, {})
            best = _best_ward_match(ward_name, slug_wards)
            if best:
                ward_data = slug_wards[best]

    # Fallback 3: DC ballot-for-postcode API
    if not ward_data and council_slug:
        try:
            dc_r = requests.get(
                "https://candidates.democracyclub.org.uk/api/next/ballots/",
                params={"election_date": "2026-05-07", "for_postcode": postcode, "format": "json"},
                timeout=6, headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
            )
            if dc_r.status_code == 200:
                dc_json = dc_r.json()
                if dc_json.get("count", 999) <= 5:
                    for b in dc_json.get("results", []):
                        bp = b.get("ballot_paper_id", "")
                        parts = bp.split(".")
                        if len(parts) >= 4 and parts[0] == "local":
                            c_slug, w_slug = parts[1], parts[2]
                            slug_wards = elections["by_slug"].get(c_slug, {})
                            if w_slug in slug_wards:
                                council_slug = c_slug
                                ward_data = slug_wards[w_slug]
                                break
        except Exception as e:
            app.logger.warning(f"[councillor] DC ballot fallback: {e}")

    if ward_data:
        ward_name = ward_data.get("ward") or ward_name
        council   = ward_data.get("council") or council

    rows = _get_councillors_for_ward(ward_gss)
    if rows:
        return {"councillors": rows, "ward": ward_name, "council": council}

    election_date = ward_data.get("date", "2026-05-07")
    council_slug  = council_slug or _org_name_to_dc_slug(council)
    dc_members = _fetch_dc_elected(council_slug, ward_name, election_date)
    if dc_members:
        new_rows = [_councillor_row(
            m["name"], m["party"], m["email"], m["photo_url"], m["council_profile_url"],
            ward_gss, ward_name, council, election_date
        ) for m in dc_members]
        _upsert_councillors(new_rows)
        return {"councillors": new_rows, "ward": ward_name, "council": council}

    return {"councillors": [], "ward": ward_name, "council": council}


@app.route("/api/councillor")
def api_councillor():
    """Return councillor(s) for a postcode. Checks DB first, then DC API."""
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if not postcode:
        return jsonify({"error": "Postcode required"}), 400
    try:
        return jsonify(_resolve_councillors(postcode))
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── MP data — Supabase-backed ──────────────────────────────────────────────────
# All 650 MPs stored in `mps` Supabase table (seeded once from Parliament API).
# Contacts fetched lazily per MP and stored so subsequent lookups need no
# external API calls. Refresh via /api/mp/refresh?token=<DIGEST_TOKEN>.
import threading as _threading

_mp_mem: dict = {}          # in-memory dict loaded from DB on startup
_mp_mem_lock = _threading.Lock()


def _fetch_contacts(mp_id: int) -> dict:
    """Fetch contact details for one MP from Parliament API."""
    email = phone = website = twitter = office_address = office_phone = ""
    try:
        cr = requests.get(
            f"https://members-api.parliament.uk/api/Members/{mp_id}/Contact",
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
            timeout=8,
        )
        if cr.ok:
            for c in cr.json().get("value", []):
                type_name = (c.get("type") or "").lower()
                if not email  and c.get("email"):                              email  = c["email"]
                if not twitter and "twitter" in type_name:                     twitter = c.get("line1", "").lstrip("@")
                if not website and (c.get("line1") or "").startswith("http"):  website = c["line1"]
                # Constituency office address (typeId 4) — extract full address
                if not office_address and "constituency" in type_name:
                    lines = [c.get(f"line{i}") or "" for i in range(1, 6)]
                    pc    = c.get("postcode") or c.get("postCode") or ""
                    parts = [l.strip() for l in lines if l.strip()]
                    if pc.strip():
                        parts.append(pc.strip())
                    if parts:
                        office_address = ", ".join(parts)
                    if c.get("phone") and not office_phone:
                        office_phone = c["phone"]
                # General phone fallback (Parliament office)
                if not phone and c.get("phone"):
                    phone = c["phone"]
    except Exception:
        pass
    # Prefer constituency phone for the main phone field
    return {
        "email":          email,
        "phone":          office_phone or phone,
        "website":        website,
        "twitter":        twitter,
        "office_address": office_address,
    }


def _seed_mps_to_db() -> int:
    """Paginate Parliament Members API and bulk-insert all MPs into Supabase. Returns count."""
    rows = []
    skip = 0
    while True:
        try:
            r = requests.get(
                "https://members-api.parliament.uk/api/Members/Search",
                params={"House": 1, "IsCurrentMember": True, "take": 20, "skip": skip},
                headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
                timeout=12,
            )
            if not r.ok:
                break
            items = r.json().get("items", [])
            if not items:
                break
            for item in items:
                v   = item.get("value", {})
                mem = v.get("latestHouseMembership") or {}
                seat = mem.get("membershipFrom", "")
                mp_id = v.get("id")
                if not seat or not mp_id:
                    continue
                rows.append({
                    "constituency":      seat.lower(),
                    "mp_id":             mp_id,
                    "name":              v.get("nameDisplayAs", ""),
                    "party":             (v.get("latestParty") or {}).get("name", ""),
                    "photo_url":         v.get("thumbnailUrl", ""),
                    "parliament_url":    f"https://members.parliament.uk/member/{mp_id}/contact",
                    "email":             "",
                    "phone":             "",
                    "website":           "",
                    "twitter":           "",
                    "contacts_fetched":  False,
                })
            skip += len(items)
            if len(items) < 20:
                break
        except Exception as e:
            print(f"[mp_seed] skip={skip}: {e}")
            break

    if rows:
        # Upsert in batches of 100
        sb = lib._sb()
        for i in range(0, len(rows), 100):
            sb.table("mps").upsert(rows[i:i+100]).execute()
        print(f"[mp_seed] seeded {len(rows)} MPs to Supabase")
    return len(rows)


def _load_mp_mem() -> dict:
    """Load all MPs from Supabase into memory dict keyed by lowercase constituency."""
    try:
        rows = lib._sb().table("mps").select("*").execute().data or []
        return {r["constituency"]: r for r in rows}
    except Exception as e:
        print(f"[mp_mem] load error: {e}")
        return {}


def _init_mp_cache():
    """Startup: load from DB; if empty, seed from Parliament API first."""
    global _mp_mem
    mem = _load_mp_mem()
    if not mem:
        print("[mp_mem] DB empty — seeding from Parliament API...")
        _seed_mps_to_db()
        mem = _load_mp_mem()
    with _mp_mem_lock:
        _mp_mem = mem
    print(f"[mp_mem] loaded {len(mem)} MPs into memory")


def _get_mp_mem() -> dict:
    with _mp_mem_lock:
        return _mp_mem


# Pre-warm on startup
_threading.Thread(target=_init_mp_cache, daemon=True).start()


@app.route("/api/mp")
def api_mp():
    """Return current MP for a postcode. Pure DB lookup after initial seed."""
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if not postcode:
        return jsonify({"error": "Postcode required"}), 400
    try:
        # Step 1: constituency name from postcodes.io
        pc_r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=8)
        if not pc_r.ok:
            return jsonify({"error": "Invalid postcode"}), 400
        pc = pc_r.json().get("result") or {}
        constituency = pc.get("parliamentary_constituency_2024") or pc.get("parliamentary_constituency", "")
        if not constituency:
            return jsonify({"error": "No constituency found for this postcode"})

        # Step 2: memory dict lookup (loaded from Supabase)
        mem = _get_mp_mem()
        key = constituency.lower()
        row = mem.get(key)
        if not row:
            # Fuzzy fallback: normalise & ↔ and, smart apostrophes, extra spaces
            key_norm = key.replace("&", "and").replace("  ", " ").strip()
            for k, v in mem.items():
                if k.replace("&", "and").replace("  ", " ").strip() == key_norm:
                    row = v
                    break
        if not row:
            return jsonify({"error": f"No MP found for {constituency}", "db_count": len(mem)})

        # Step 3: fetch contacts in background if not yet stored or office_address missing
        needs_fetch = not row.get("contacts_fetched") or row.get("office_address") is None
        if needs_fetch:
            mp_id   = row["mp_id"]
            con_key = constituency.lower()
            def _bg_contacts():
                contacts = _fetch_contacts(mp_id)
                # Update in-memory cache immediately regardless of DB success
                with _mp_mem_lock:
                    _mp_mem[con_key] = {**_mp_mem.get(con_key, row), **contacts, "contacts_fetched": True}
                try:
                    lib._sb().table("mps").update({**contacts, "contacts_fetched": True}) \
                        .eq("constituency", con_key).execute()
                except Exception:
                    # office_address column may not exist yet — retry without it
                    try:
                        contacts_safe = {k: v for k, v in contacts.items() if k != "office_address"}
                        lib._sb().table("mps").update({**contacts_safe, "contacts_fetched": True}) \
                            .eq("constituency", con_key).execute()
                    except Exception:
                        pass
            _threading.Thread(target=_bg_contacts, daemon=True).start()

        return jsonify({
            "name":           row["name"],
            "party":          row["party"],
            "constituency":   row["constituency"],
            "photo_url":      row["photo_url"],
            "email":          row.get("email", ""),
            "phone":          row.get("phone", ""),
            "website":        row.get("website", ""),
            "twitter":        row.get("twitter", ""),
            "parliament_url": row["parliament_url"],
            "office_address": row.get("office_address", ""),
        })
    except Exception as e:
        print(f"[mp] {e}")
        return jsonify({"error": "Could not fetch MP data"}), 500


@app.route("/api/mp/refresh")
def api_mp_refresh():
    """Re-seed MP table from Parliament API (use after a by-election)."""
    token = request.args.get("token", "")
    if token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "Forbidden"}), 403
    def _do_refresh():
        global _mp_mem
        count = _seed_mps_to_db()
        mem = _load_mp_mem()
        with _mp_mem_lock:
            _mp_mem = mem
        print(f"[mp_refresh] done — {count} MPs")
    _threading.Thread(target=_do_refresh, daemon=True).start()
    return jsonify({"status": "refresh started"})


@app.route("/api/mp/status")
def api_mp_status():
    """Debug: show how many MPs are in memory and a sample of keys."""
    mem = _get_mp_mem()
    search = request.args.get("q", "").lower()
    if search:
        keys = [k for k in sorted(mem.keys()) if search in k]
        return jsonify({"count": len(mem), "matches": keys})
    sample = sorted(mem.keys())[:10]
    return jsonify({"count": len(mem), "sample_keys": sample})


@app.route("/api/elections/sync-councillors", methods=["POST"])
def api_sync_councillors():
    """Admin: pull all elected councillors from DC API and upsert to DB."""
    token = request.args.get("token", "")
    if token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "forbidden"}), 403
    elections = _get_elections()
    total_saved = 0
    errors = []
    for ward_gss, ward_data in elections.get("by_gss", {}).items():
        try:
            ward_name  = ward_data.get("ward", "")
            council    = ward_data.get("council", "")
            elec_date  = ward_data.get("date", "2026-05-07")
            council_slug = _org_name_to_dc_slug(council)
            dc_members = _fetch_dc_elected(council_slug, ward_name, elec_date)
            if dc_members:
                rows = [_councillor_row(
                    m["name"], m["party"], m["email"], m["photo_url"], m["council_profile_url"],
                    ward_gss, ward_name, council, elec_date
                ) for m in dc_members]
                total_saved += _upsert_councillors(rows)
        except Exception as e:
            errors.append(f"{ward_gss}: {e}")
    return jsonify({"saved": total_saved, "errors": errors[:10]})


@app.route("/api/elections/send-results", methods=["POST"])
def api_elections_send_results():
    """Admin: immediately send election results to a specific WhatsApp number + postcode."""
    token = request.args.get("token", "")
    if token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "forbidden"}), 403
    data     = request.get_json(force=True) or {}
    wa       = data.get("wa_number", "").strip()
    postcode = data.get("postcode", "").strip().replace(" ", "").upper()
    if not wa or not postcode:
        return jsonify({"error": "wa_number and postcode required"}), 400
    try:
        msg = whatsapp_results_format(postcode)
        wa_to = wa if wa.startswith("+") else "+" + wa
        _wa_send_proactive(f"whatsapp:{wa_to}", msg)
        return jsonify({"ok": True, "sent_to": wa_to})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Fuel price drop alerts ──────────────────────────────────────────────────

def _get_cheapest_fuel(postcode: str, fuel: str, radius_miles: float = 5):
    """Return (price, station_dict) for the cheapest station near postcode, or (None, None)."""
    latlon = postcode_to_latlon(postcode)
    if not latlon:
        return None, None
    lat, lon = latlon
    radius_km = radius_miles * 1.60934
    nearby = _nearby_stations(lat, lon, fuel, radius_km)
    if not nearby:
        return None, None
    nearby.sort(key=lambda x: x["price"])
    return nearby[0]["price"], nearby[0]


@app.route("/api/fuel/refresh")
def api_fuel_refresh():
    """Pre-warm station cache. Call every 30 min via cron-job.org."""
    token = request.args.get("token", "")
    if token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "Forbidden"}), 403
    before = _station_cache.get("loaded_at", 0)
    stations = get_stations()
    refreshed = _station_cache.get("loaded_at", 0) != before
    return jsonify({"stations": len(stations), "refreshed": refreshed})


@app.route("/api/fuel/alert", methods=["POST"])
def api_fuel_alert_register():
    """Register for fuel price drop alerts. Body: {wa_number, postcode, fuel_type?, threshold_drop?}"""
    data      = request.get_json(force=True) or {}
    wa        = data.get("wa_number", "").strip()
    postcode  = data.get("postcode", "").strip().replace(" ", "").upper()
    fuel      = data.get("fuel_type", "petrol").lower()
    threshold = float(data.get("threshold_drop", 1.0))  # pence/litre
    if not wa or not postcode:
        return jsonify({"error": "wa_number and postcode required"}), 400
    try:
        # Snapshot current cheapest as baseline
        price, station = _get_cheapest_fuel(postcode, fuel)
        existing = lib._sb().table("fuel_alerts").select("id") \
            .eq("wa", wa).eq("postcode", postcode).eq("fuel_type", fuel).execute().data
        if existing:
            lib._sb().table("fuel_alerts").update({
                "threshold_drop": threshold,
                "last_price": price,
            }).eq("wa", wa).eq("postcode", postcode).eq("fuel_type", fuel).execute()
        else:
            lib._sb().table("fuel_alerts").insert({
                "wa": wa,
                "postcode": postcode,
                "fuel_type": fuel,
                "last_price": price,
                "threshold_drop": threshold,
            }).execute()
        fuel_label = "Petrol" if fuel == "petrol" else "Diesel"
        station_name = station.get("brand", "") if station else ""
        msg = (
            f"⛽ Fuel alert set!\n"
            f"{fuel_label} · {postcode} · alert when drops ≥{threshold:.0f}p/litre\n"
            f"Current cheapest: {price:.1f}p at {station_name}\n\n"
            f"You'll get a WhatsApp when prices dip. No action needed."
        )
        wa_to = wa if wa.startswith("+") else "+" + wa
        _wa_send_proactive(f"whatsapp:{wa_to}", msg)
        return jsonify({"ok": True, "baseline_price": price})
    except Exception as e:
        app.logger.error(f"fuel alert register: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/fuel/check-drops")
def api_fuel_check_drops():
    """Cron endpoint — check all fuel alerts and send WhatsApp if price dropped."""
    token = request.args.get("token", "")
    if token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "forbidden"}), 403
    try:
        alerts = lib._sb().table("fuel_alerts").select("*").execute().data or []
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    checked, sent = 0, 0
    for alert in alerts:
        try:
            pc        = alert["postcode"]
            fuel      = alert.get("fuel_type", "petrol")
            wa        = alert["wa"]
            last      = alert.get("last_price")
            threshold = float(alert.get("threshold_drop") or 1.0)

            price, station = _get_cheapest_fuel(pc, fuel)
            if price is None:
                continue
            checked += 1

            dropped = last is not None and price <= (float(last) - threshold)

            # Always update last_price
            lib._sb().table("fuel_alerts").update({"last_price": price}) \
                .eq("id", alert["id"]).execute()

            if not dropped:
                continue

            fuel_label = "Petrol" if fuel == "petrol" else "Diesel"
            brand = station.get("brand", "")
            addr  = station.get("address", "")[:30] if station.get("address") else ""
            dist  = station.get("dist_mi", 0)
            saving = (float(last) - price) * 55 / 100  # pence saving on ~55L tank
            msg = (
                f"⛽ Fuel price drop!\n"
                f"{fuel_label} in {pc} is now {price:.1f}p/litre\n"
                f"(was {last:.1f}p — down {float(last)-price:.1f}p)\n\n"
                f"Cheapest: {brand} · {addr} · {dist:.1f}mi away\n"
                f"Save ~{saving:.0f}p on a full tank\n\n"
                f"🔗 miru.humanagency.co"
            )
            wa_to = wa if wa.startswith("+") else "+" + wa
            _wa_send_proactive(f"whatsapp:{wa_to}", msg)
            lib._sb().table("fuel_alerts").update({"last_alerted_at": datetime.utcnow().isoformat()}) \
                .eq("id", alert["id"]).execute()
            sent += 1
        except Exception as ex:
            app.logger.error(f"fuel check-drops row: {ex}")
            continue

    return jsonify({"checked": checked, "sent": sent})


_elections_response_cache: dict = {}  # postcode → (payload, ts)
_ELECTIONS_RESPONSE_TTL = 600  # 10 minutes — candidates/results stable post-election

@app.route("/api/elections")
def api_elections():
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if not postcode:
        return jsonify({"error": "Postcode required"}), 400

    cached = _elections_response_cache.get(postcode)
    if cached and time.time() - cached[1] < _ELECTIONS_RESPONSE_TTL:
        return jsonify(cached[0])

    try:
        if postcode in _pc_meta_cache:
            result = _pc_meta_cache[postcode]
        else:
            r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=6)
            if r.status_code != 200:
                return jsonify({"error": f"Could not find postcode {postcode}"}), 404
            result = r.json().get("result", {})
            _pc_meta_cache[postcode] = result
        codes        = result.get("codes", {})
        ward_gss     = codes.get("admin_ward", "")
        ward_name    = result.get("admin_ward", "")
        ced_gss      = codes.get("ced", "")
        ced_name     = result.get("ced", "")
        district     = result.get("admin_district", "")
        district_code = codes.get("admin_district", "")
        county_code  = codes.get("admin_county", "")
        ua_code      = codes.get("admin_district", "")  # unitaries use district code
        postcode_fmt = f"{postcode[:-3]} {postcode[-3:]}"

        elections = _get_elections()
        ward_data = elections["by_gss"].get(ward_gss, {})
        council_slug = None

        # Fallback 1: county electoral division (e.g. West Sussex CC ward)
        if not ward_data and ced_gss:
            ward_data = elections["by_gss"].get(ced_gss, {})
            if ward_data:
                ward_name = ced_name or ward_name

        # Fallback 2: reorganised/merged council lookup by ward name token matching
        if not ward_data:
            council_slug = (
                _DISTRICT_TO_COUNCIL_SLUG.get(district_code) or
                _UA_TO_COUNCIL_SLUG.get(ua_code) or
                _COUNTY_TO_COUNCIL_SLUG.get(county_code)
            )
            if council_slug:
                slug_wards = elections["by_slug"].get(council_slug, {})
                best = _best_ward_match(ward_name, slug_wards)
                if best:
                    ward_data = slug_wards[best]

        # Fallback 3: ask DC directly which 2026 ballot covers this postcode
        # Only for boundary-changed councils where postcodes.io has old ward names
        # Safety: only trust DC result if count ≤ 5 (confirms for_postcode filter worked)
        if not ward_data and council_slug:
            try:
                dc_r = requests.get(
                    "https://candidates.democracyclub.org.uk/api/next/ballots/",
                    params={"election_date": "2026-05-07", "for_postcode": postcode, "format": "json"},
                    timeout=6, headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
                )
                if dc_r.status_code == 200:
                    dc_json = dc_r.json()
                    dc_count = dc_json.get("count", 999)
                    dc_ballots = dc_json.get("results", [])
                    if dc_count <= 5:  # filter worked; skip if DC returned unfiltered full list
                        for b in dc_ballots:
                            bp = b.get("ballot_paper_id", "")
                            parts = bp.split(".")
                            if len(parts) >= 4 and parts[0] == "local":
                                c_slug, w_slug = parts[1], parts[2]
                                slug_wards = elections["by_slug"].get(c_slug, {})
                                if w_slug in slug_wards:
                                    council_slug = c_slug
                                    ward_data = slug_wards[w_slug]
                                    break
                                if c_slug:
                                    council_slug = c_slug
                                    label = w_slug.replace("-", " ").title()
                                    ward_data = {"ward": label, "council": district,
                                                 "election_date": "2026-05-07", "candidates": []}
                                    break
            except Exception as e:
                print(f"[elections] DC ballot fallback error: {e}")

        candidates = sorted(
            ward_data.get("candidates", []),
            key=lambda c: (c["party"], c["name"])
        )

        # Persist candidates to Supabase (fire-and-forget)
        if candidates and ward_gss:
            try:
                rows = [{
                    "ward_gss":      ward_gss,
                    "ward":          ward_data.get("ward") or ward_name,
                    "council":       ward_data.get("council") or district,
                    "election_date": ward_data.get("election_date", "2026-05-07"),
                    "name":          c.get("name", ""),
                    "party":         c.get("party", ""),
                    "twitter":       c.get("twitter") or None,
                    "homepage":      c.get("homepage") or None,
                    "photo":         c.get("photo") or None,
                } for c in candidates]
                lib._sb().table("election_candidates").upsert(
                    rows, on_conflict="ward_gss,name,election_date"
                ).execute()
            except Exception as _ce:
                app.logger.warning(f"[elections] candidate save: {_ce}")

        is_new_council = bool(council_slug and council_slug in _COUNCIL_PREDECESSORS)
        predecessors   = _COUNCIL_PREDECESSORS.get(council_slug or "", [])
        dc_results_url = (
            None if is_new_council else
            f"https://candidates.democracyclub.org.uk/elections/local.{district.lower().replace(' ','-')}.2022-05-05/"
        )

        # Fetch polling station + past/live results in parallel
        effective_council = (council_slug or district).lower().replace(" ", "-")
        effective_ward    = ward_data.get("ward") or ward_name
        election_date_str = ward_data.get("election_date", "2026-05-07")
        ballot_paper_id   = ward_data.get("ballot_paper_id")
        import datetime as _dt
        election_happened = _dt.date.today() >= _dt.date.fromisoformat(election_date_str)

        with ThreadPoolExecutor(max_workers=3) as ex:
            f_ps   = ex.submit(_fetch_polling_station, postcode)
            f_2022 = ex.submit(_fetch_dc_results, effective_council, effective_ward, "2022-05-05") \
                     if not is_new_council else None
            # Use direct ballot fetch if we have the ID (skips fuzzy matching + election list call)
            if election_happened:
                f_live = ex.submit(_fetch_dc_ballot, ballot_paper_id) if ballot_paper_id \
                         else ex.submit(_fetch_dc_results, effective_council, effective_ward, election_date_str)
            else:
                f_live = None
            polling_station = f_ps.result()
            past_results    = f_2022.result() if f_2022 else []
            live_results    = f_live.result() if f_live else []

        county_name = result.get("admin_county") or ""

        payload = {
            "postcode":          postcode_fmt,
            "ward":              effective_ward,
            "council":           ward_data.get("council") or district,
            "county":            county_name,
            "election_date":     election_date_str,
            "ward_gss":          ward_gss,
            "candidates":        candidates,
            "is_new_council":    is_new_council,
            "predecessor_areas": predecessors,
            "dc_results_url":    dc_results_url,
            "past_results":      past_results,
            "live_results":      live_results,
            "election_happened": election_happened,
            "polling_station":   polling_station,
            "polling_station_url": (polling_station or {}).get("url") or
                                   f"https://wheredoivote.co.uk/postcode/{postcode}/",
            "found": bool(candidates),
        }
        _elections_response_cache[postcode] = (payload, time.time())
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/elections/debug")
def api_elections_debug():
    """Debug: show raw DC ballot response + cache state for a postcode or ballot_id."""
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    ballot_id = request.args.get("ballot_id", "").strip()
    clear = request.args.get("clear", "")

    if clear and postcode:
        _elections_response_cache.pop(postcode, None)
        return jsonify({"cleared": postcode})

    out = {}

    if postcode:
        try:
            r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=6)
            pc = r.json().get("result", {}) if r.status_code == 200 else {}
            codes = pc.get("codes", {})
            ward_gss = codes.get("admin_ward", "")
            ward_name = pc.get("admin_ward", "")
            district = pc.get("admin_district", "")
            district_code = codes.get("admin_district", "")
            county_code = codes.get("admin_county", "")

            elections = _get_elections()
            ward_data = elections["by_gss"].get(ward_gss, {})
            source = "by_gss" if ward_data else None
            if not ward_data:
                council_slug = (_DISTRICT_TO_COUNCIL_SLUG.get(district_code) or
                                _UA_TO_COUNCIL_SLUG.get(district_code) or
                                _COUNTY_TO_COUNCIL_SLUG.get(county_code))
                if council_slug:
                    slug_wards = elections["by_slug"].get(council_slug, {})
                    best = _best_ward_match(ward_name, slug_wards)
                    if best:
                        ward_data = slug_wards[best]
                        source = f"by_slug/{council_slug}/{best}"
            ballot_id_found = ward_data.get("ballot_paper_id", "")
            out["postcode"] = postcode
            out["ward_name_from_postcodes"] = ward_name
            out["ward_gss"] = ward_gss
            out["district"] = district
            out["district_code"] = district_code
            out["ward_data_source"] = source
            out["ballot_paper_id"] = ballot_id_found
            out["candidates_count"] = len(ward_data.get("candidates", []))
            out["cached_response"] = bool(_elections_response_cache.get(postcode))
            if not ballot_id:
                ballot_id = ballot_id_found
        except Exception as e:
            out["postcode_error"] = str(e)

    if ballot_id:
        try:
            r2 = requests.get(
                f"https://candidates.democracyclub.org.uk/api/next/ballots/{ballot_id}/",
                params={"format": "json"}, timeout=10,
                headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
            )
            out["dc_status"] = r2.status_code
            if r2.status_code == 200:
                dc_json = r2.json()
                candidacies = dc_json.get("candidacies", [])
                out["dc_candidacies_count"] = len(candidacies)
                out["dc_sample"] = [{
                    "name": c.get("person", {}).get("name"),
                    "elected": c.get("elected"),
                    "result": c.get("result"),
                } for c in candidacies[:5]]
                out["dc_top_level_keys"] = list(dc_json.keys())
            else:
                out["dc_body"] = r2.text[:500]
        except Exception as e:
            out["dc_error"] = str(e)

    return jsonify(out)


_OVERPASS_URLS = [
    "https://overpass.kumi.systems/api/interpreter",  # confirmed working on Railway
    "https://overpass-api.de/api/interpreter",        # returns 406 from Railway
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

# Simple 30-minute in-memory cache for Overpass results keyed by postcode
_services_cache: dict = {}
_SERVICES_TTL = 86400  # 24 hours — hospitals/supermarkets/police don't move

def _overpass_mirrors(query, timeout=20):
    """POST to Overpass with mirror fallback. Use for hospitals/supermarkets."""
    for url in _OVERPASS_URLS:
        try:
            r = requests.post(url, data={"data": query}, timeout=timeout)
            if r.status_code == 200:
                els = r.json().get("elements", [])
                if els:
                    print(f"[overpass] {url} returned {len(els)} elements")
                    return els
                print(f"[overpass] {url} returned 0 elements, trying next")
        except Exception as e:
            print(f"[overpass] {url} failed: {e}")
    return []

def _parse_osm_elements(elements, limit, extra_fields=None):
    results = []
    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name", "")
        if not name:
            continue
        addr = ", ".join(filter(None, [
            tags.get("addr:housenumber", ""), tags.get("addr:street", ""),
            tags.get("addr:city", ""),        tags.get("addr:postcode", ""),
        ])) or tags.get("addr:full", "")
        item = {"name": name, "address": addr.strip()}
        for f in (extra_fields or []):
            item[f] = (tags.get(f) or tags.get(f"contact:{f}") or "").strip()
        results.append(item)
        if len(results) == limit:
            break
    return results

def _el_coords(el):
    return (
        el.get("lat") or (el.get("center") or {}).get("lat"),
        el.get("lon") or (el.get("center") or {}).get("lon"),
    )

def _el_phone(tags):
    return (tags.get("phone") or tags.get("telephone") or
            tags.get("contact:phone") or tags.get("contact:telephone") or
            tags.get("contact:mobile") or tags.get("mobile") or "").strip()

def _el_address(tags):
    parts = [
        tags.get("addr:housenumber", ""), tags.get("addr:street", ""),
        tags.get("addr:city", ""),        tags.get("addr:postcode", ""),
    ]
    return ", ".join(filter(None, parts)) or tags.get("addr:full", "")

def _places_nearby(lat, lon, place_type, radius_m, max_results):
    """Fetch nearby places via Google Places Nearby Search."""
    key = os.environ.get("GOOGLE_PLACES_KEY", "")
    if not key:
        return []
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params={"location": f"{lat},{lon}", "radius": radius_m,
                    "type": place_type, "key": key},
            timeout=10,
        )
        results = r.json().get("results", [])
        items = []
        for p in results:
            loc = p.get("geometry", {}).get("location", {})
            plat, plon = loc.get("lat"), loc.get("lng")
            dist = round(haversine_km(lat, lon, plat, plon), 2) if plat and plon else 999
            items.append({
                "name":        p.get("name", ""),
                "address":     p.get("vicinity", ""),
                "distance_km": dist,
                "place_id":    p.get("place_id", ""),
            })
        items.sort(key=lambda x: x["distance_km"])
        return items[:max_results]
    except Exception as e:
        print(f"[places_nearby {place_type}] {e}")
        return []

def _fetch_hospitals_overpass(lat, lon, radius_m=20000):
    query = f"""[out:json][timeout:30];
(
  node["amenity"="hospital"](around:{radius_m},{lat},{lon});
  way["amenity"="hospital"](around:{radius_m},{lat},{lon});
  relation["amenity"="hospital"](around:{radius_m},{lat},{lon});
  node["healthcare"="hospital"](around:{radius_m},{lat},{lon});
  way["healthcare"="hospital"](around:{radius_m},{lat},{lon});
  relation["healthcare"="hospital"](around:{radius_m},{lat},{lon});
);
out center tags;"""
    _SKIP_HOSPITAL_WORDS = {
        "gp", "surgery", "dental",
        "dentist", "optician", "pharmacy", "veterinary", "vet ", "beauty",
        "cosmetic", "aesthetic", "therapy", "therapist",
    }
    try:
        elements = _overpass_mirrors(query, timeout=35)
        seen, items = set(), []
        for el in elements:
            tags = el.get("tags", {})
            name = tags.get("name", "")
            if not name or name in seen:
                continue
            seen.add(name)
            nl = name.lower()
            if any(w in nl for w in _SKIP_HOSPITAL_WORDS):
                continue
            elat, elon = _el_coords(el)
            if not elat or not elon:
                continue
            dist = round(haversine_km(lat, lon, elat, elon), 2)
            items.append({
                "name": name,
                "address": _el_address(tags),
                "distance_km": dist,
                "phone": _el_phone(tags),
            })
        items.sort(key=lambda x: x["distance_km"])
        return items[:5]
    except Exception as e:
        print(f"[overpass hospitals] {e}")
        return []

def _fetch_hospitals(lat, lon):
    results = _fetch_hospitals_overpass(lat, lon)
    if not results:
        # Google Places fallback for areas with sparse OSM hospital tagging
        gp = places_nearby(lat, lon, "hospital", radius_m=20000, max_results=5)
        results = [{"name": p["name"], "address": p.get("address",""), "distance_km": p.get("distance_km",0), "phone": ""} for p in gp]
    return results

_UK_SUPERMARKET_CHAINS = {
    "sainsbury", "tesco", "asda", "lidl", "aldi", "waitrose", "morrisons",
    "co-op", "co op", "cooperative", "marks & spencer", "m&s", "iceland",
    "costco", "spar", "budgens", "ocado", "whole foods", "booths",
}

def _is_major_supermarket(name: str) -> bool:
    n = name.lower()
    return any(chain in n for chain in _UK_SUPERMARKET_CHAINS)

def _fetch_supermarkets_overpass(lat, lon, radius_m=5000):
    query = f"""
[out:json][timeout:30];
(
  node["shop"="supermarket"](around:{radius_m},{lat},{lon});
  way["shop"="supermarket"](around:{radius_m},{lat},{lon});
  relation["shop"="supermarket"](around:{radius_m},{lat},{lon});
);
out center tags;
"""
    try:
        elements = _overpass_mirrors(query, timeout=35)
        items = []
        for el in elements:
            tags = el.get("tags", {})
            name = tags.get("name") or tags.get("brand") or ""
            if not name or not _is_major_supermarket(name):
                continue
            elat, elon = _el_coords(el)
            if not elat or not elon:
                continue
            dist = round(haversine_km(lat, lon, elat, elon), 2)
            items.append({"name": name, "address": _el_address(tags), "distance_km": dist,
                          "opening_hours": tags.get("opening_hours", "")})
        items.sort(key=lambda x: x["distance_km"])
        return items[:10]
    except Exception as e:
        print(f"[overpass supermarkets] {e}")
        return []

def _fetch_supermarkets(lat, lon):
    # Overpass-only: Google Places returns incorrect UK supermarket results
    return _fetch_supermarkets_overpass(lat, lon, radius_m=5000)  # ~3 miles, fast query


_UK_CONVENIENCE_CHAINS = {
    "londis", "spar", "nisa", "one stop", "premier", "mccoll", "costcutter",
    "family shopper", "centra", "day today", "keystore", "best-one", "bestone",
    "mace", "lifestyle express", "budgens", "jacks",
}

def _fetch_convenience_overpass(lat, lon, radius_m=2000):
    query = f"""[out:json][timeout:20];
(node["shop"="convenience"](around:{radius_m},{lat},{lon});
 way["shop"="convenience"](around:{radius_m},{lat},{lon}););
out center tags;"""
    try:
        els = _overpass_mirrors(query)
        items = []
        for e in els:
            tags = e.get("tags", {})
            name = tags.get("name") or tags.get("brand") or ""
            if not name:
                continue
            if not any(c in name.lower() for c in _UK_CONVENIENCE_CHAINS):
                continue
            elat, elon = _el_coords(e)
            if not elat:
                continue
            dist = round(haversine_km(lat, lon, elat, elon), 2)
            items.append({"name": name, "address": _el_address(tags),
                          "phone": _el_phone(tags), "distance_km": dist,
                          "opening_hours": tags.get("opening_hours", "")})
        items.sort(key=lambda x: x["distance_km"])
        return items[:5]
    except Exception as e:
        print(f"[overpass convenience] {e}")
        return []


def _fetch_off_licences_overpass(lat, lon, radius_m=3000):
    query = f"""[out:json][timeout:20];
(node["shop"="alcohol"](around:{radius_m},{lat},{lon});
 way["shop"="alcohol"](around:{radius_m},{lat},{lon});
 node["shop"="off_licence"](around:{radius_m},{lat},{lon});
 way["shop"="off_licence"](around:{radius_m},{lat},{lon}););
out center tags;"""
    try:
        els = _overpass_mirrors(query)
        items = []
        for e in els:
            tags = e.get("tags", {})
            name = tags.get("name") or tags.get("brand") or ""
            if not name:
                continue
            elat, elon = _el_coords(e)
            if not elat:
                continue
            dist = round(haversine_km(lat, lon, elat, elon), 2)
            items.append({"name": name, "address": _el_address(tags),
                          "phone": _el_phone(tags), "distance_km": dist,
                          "opening_hours": tags.get("opening_hours", "")})
        items.sort(key=lambda x: x["distance_km"])
        return items[:5]
    except Exception as e:
        print(f"[overpass off-licences] {e}")
        return []


def _fetch_police_contact(lat, lon):
    try:
        r = requests.get("https://data.police.uk/api/locate-neighbourhood",
                         params={"q": f"{lat},{lon}"}, timeout=8)
        if r.status_code != 200:
            return {}
        loc = r.json()
        force_id = loc.get("force", "")
        nb_id    = loc.get("neighbourhood", "")
        # Neighbourhood detail and force detail are independent — fetch in parallel
        with ThreadPoolExecutor(max_workers=2) as _ex:
            f_nb = _ex.submit(requests.get, f"https://data.police.uk/api/{force_id}/{nb_id}", timeout=8)
            f_f  = _ex.submit(requests.get, f"https://data.police.uk/api/forces/{force_id}",  timeout=8)
            r_nb, r_f = f_nb.result(), f_f.result()
        nb    = r_nb.json() if r_nb.status_code == 200 else {}
        force = r_f.json()  if r_f.status_code  == 200 else {}
        contact = nb.get("contact_details", {})
        return {
            "force_name":    force.get("name", ""),
            "force_url":     force.get("url", ""),
            "telephone":     contact.get("telephone") or "101",
            "email":         contact.get("email", ""),
            "neighbourhood": nb.get("name", ""),
        }
    except Exception as e:
        print(f"[police_contact] {e}")
        return {}


def _latlon_for_postcode(postcode):
    """Return (lat, lon) using the shared postcode_to_latlon cache — no extra API calls."""
    result = postcode_to_latlon(postcode)
    if result:
        return result
    return None, None


# ── Your Area ─────────────────────────────────────────────────────────────────

_WATER_MAP = [
    # (postcode_prefixes, company, phone, website)
    ({"SW","SE","E","EC","N","NW","W","WC","TW","KT","BR","CR","DA","IG","RM","EN","HA","UB","SL","RG","OX","HP","MK"},
     "Thames Water", "0800 316 9800", "thameswater.co.uk"),
    ({"ME","TN","CT","BN","RH","GU","SO","PO"},
     "Southern Water", "0330 303 0277", "southernwater.co.uk"),
    ({"KT","SM","CR","GU"},
     "South East Water", "0333 000 0002", "southeastwater.co.uk"),
    ({"IP","NR","PE","CB","CO","SS","CM","NN","LU","SG","AL"},
     "Anglian Water", "03457 145 145", "anglianwater.co.uk"),
    ({"B","CV","DE","LE","NG","NN","ST","WS","WV","DY","WR","GL","HR","SY"},
     "Severn Trent", "0800 783 4444", "stwater.co.uk"),
    ({"CA","LA","FY","PR","BB","BL","OL","SK","CW","WA","WN","L","M","CH"},
     "United Utilities", "0345 672 3723", "unitedutilities.com"),
    ({"BD","HD","HG","HU","HX","LS","S","DN","WF","YO","LN"},
     "Yorkshire Water", "0345 124 2424", "yorkshirewater.com"),
    ({"NE","SR","DH","DL","TS"},
     "Northumbrian Water", "0345 717 1100", "nwater.com"),
    ({"EH","FK","G","IV","KA","KW","ML","PA","PH","TD","AB","DD","KY"},
     "Scottish Water", "0800 077 8778", "scottishwater.co.uk"),
    ({"CF","NP","SA","LD","LL","SY","HR"},
     "Dŵr Cymru Welsh Water", "0800 052 0145", "dwrcymru.com"),
    ({"BA","BS","DT","EX","GL","PL","TA","TQ","TR","BH"},
     "South West Water", "0344 346 2020", "southwestwater.co.uk"),
]

_GAS_MAP = [
    ({"EH","FK","G","IV","KA","KW","ML","PA","PH","TD","AB","DD","KY",
       "GU","SO","PO","BN","RH","TN","CT","ME","KT","CR","SM","DA"},
     "SGN", "0800 111 999", "sgn.co.uk"),
    ({"CA","LA","FY","PR","BB","BL","OL","SK","WA","WN","CH","M","L","CW"},
     "Cadent (North West)", "0800 111 999", "cadentgas.com"),
    ({"NE","SR","DH","DL","TS","BD","HD","HG","HX","LS","S","DN","WF","YO","LN","HU"},
     "Northern Gas Networks", "0800 111 999", "northerngasnetworks.co.uk"),
    ({"CF","NP","SA","LD","LL","BA","BS","DT","EX","PL","TA","TQ","TR","BH","GL","HR"},
     "Wales & West Utilities", "0800 111 999", "wwutilities.co.uk"),
]
_CADENT_DEFAULT = ("Cadent", "0800 111 999", "cadentgas.com")

def _lookup_utility(pc_area: str, mapping: list, default: tuple) -> dict:
    for prefixes, name, phone, web in mapping:
        if pc_area in prefixes:
            return {"name": name, "emergency": phone, "website": web}
    n, p, w = default
    return {"name": n, "emergency": p, "website": w}

def _postcode_area(postcode: str) -> str:
    """Extract letter-only prefix e.g. 'SE' from 'SE135FH'."""
    import re as _re
    m = _re.match(r'^([A-Z]{1,2})', postcode.upper().replace(" ", ""))
    return m.group(1) if m else ""

def _fetch_gps_overpass(lat, lon, radius_m=3000):
    query = f"""[out:json][timeout:15];
(node["amenity"="doctors"](around:{radius_m},{lat},{lon});
 way["amenity"="doctors"](around:{radius_m},{lat},{lon});
 node["amenity"="clinic"](around:{radius_m},{lat},{lon});
 way["amenity"="clinic"](around:{radius_m},{lat},{lon}););
out center tags;"""
    try:
        els = _overpass_mirrors(query)
        items = []
        for e in els:
            tags = e.get("tags", {}); name = tags.get("name", "")
            if not name: continue
            elat, elon = _el_coords(e)
            if not elat: continue
            dist = round(haversine_km(lat, lon, elat, elon), 2)
            items.append({"name": name, "address": _el_address(tags),
                          "phone": _el_phone(tags), "distance_km": dist})
        items.sort(key=lambda x: x["distance_km"])
        return items[:5]
    except Exception: return []

def _fetch_amenity_overpass(lat, lon, amenity, radius_m=2000, limit=3):
    query = f"""[out:json][timeout:12];
(node["amenity"="{amenity}"](around:{radius_m},{lat},{lon});
 way["amenity"="{amenity}"](around:{radius_m},{lat},{lon}););
out center tags;"""
    try:
        els = _overpass_mirrors(query)
        items = []
        for e in els:
            tags = e.get("tags", {}); name = tags.get("name", amenity.replace("_", " ").title())
            elat, elon = _el_coords(e)
            if not elat: continue
            dist = round(haversine_km(lat, lon, elat, elon), 2)
            items.append({"name": name, "address": _el_address(tags),
                          "phone": _el_phone(tags), "distance_km": dist})
        items.sort(key=lambda x: x["distance_km"])
        return items[:limit]
    except Exception: return []

_your_area_cache: dict = {}
_YOUR_AREA_TTL = 86400  # 24 hours

@app.route("/api/your-area")
def api_your_area():
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if not postcode:
        return jsonify({"error": "Postcode required"}), 400
    cache_key = f"your-area:{postcode}"
    hit = _your_area_cache.get(cache_key)
    if hit and time.time() - hit["ts"] < _YOUR_AREA_TTL:
        return jsonify(hit["data"])
    try:
        lat, lon = _latlon_for_postcode(postcode)
        if lat is None:
            return jsonify({"error": "Postcode not found"}), 404

        # Postcode metadata from postcodes.io
        pc_info = {}
        try:
            r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=5)
            if r.status_code == 200:
                pc_info = r.json().get("result", {})
        except Exception: pass

        council_name    = pc_info.get("admin_district", "")
        council_website = ""
        if council_name:
            slug = council_name.lower().replace(" ", "").replace("london borough of", "").replace("city of", "").strip()
            council_website = f"https://www.{slug}.gov.uk"

        pc_area = _postcode_area(postcode)
        water = _lookup_utility(pc_area, _WATER_MAP, ("Thames Water", "0800 316 9800", "thameswater.co.uk"))
        gas   = _lookup_utility(pc_area, _GAS_MAP, _CADENT_DEFAULT)

        with ThreadPoolExecutor(max_workers=3) as ex:
            gps_f      = ex.submit(_fetch_gps_overpass, lat, lon)
            pharmacy_f = ex.submit(_fetch_amenity_overpass, lat, lon, "pharmacy", 1500, 3)
            postoff_f  = ex.submit(_fetch_amenity_overpass, lat, lon, "post_office", 2000, 2)
            gps        = gps_f.result(timeout=18)
            pharmacies = pharmacy_f.result(timeout=18)
            post_offices = postoff_f.result(timeout=18)

        result = {
            "council":      {"name": council_name, "website": council_website},
            "water":        water,
            "gas":          gas,
            "gps":          gps,
            "pharmacies":   pharmacies,
            "post_offices": post_offices,
        }
        if council_name and any([gps, pharmacies, post_offices]):
            _your_area_cache[cache_key] = {"data": result, "ts": time.time()}
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/services")
def api_services():
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    debug    = request.args.get("debug") == "1"
    if not postcode:
        return jsonify({"error": "Postcode required"}), 400
    try:
        cache_key = f"services:{postcode}"
        hit = _services_cache.get(cache_key)
        if hit and time.time() - hit["ts"] < _SERVICES_TTL and not debug:
            return jsonify(hit["data"])
        lat, lon = _latlon_for_postcode(postcode)
        if lat is None:
            return jsonify({"error": "Postcode not found"}), 404
        if debug:
            # Raw Overpass test — bypass everything
            q = (f"[out:json][timeout:18];"
                 f"(node[amenity=hospital](around:15000,{lat},{lon});"
                 f"way[amenity=hospital](around:15000,{lat},{lon}););"
                 f"out tags center 30;")
            debug_results = {}
            for url in _OVERPASS_URLS:
                try:
                    r = requests.post(url, data={"data": q}, timeout=18)
                    debug_results[url] = {"status": r.status_code, "elements": len(r.json().get("elements", [])) if r.status_code == 200 else 0}
                except Exception as e:
                    debug_results[url] = {"error": str(e)}
            return jsonify({"lat": lat, "lon": lon, "overpass": debug_results})
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_h = ex.submit(_fetch_hospitals, lat, lon)
            f_p = ex.submit(_fetch_police_contact, lat, lon)
            hospitals = f_h.result()
            police    = f_p.result()
        result = {"hospitals": hospitals, "police": police}
        if hospitals:  # only cache if we got results
            _services_cache[cache_key] = {"data": result, "ts": time.time()}
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/shops")
def api_shops():
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    debug    = request.args.get("debug") == "1"
    if not postcode:
        return jsonify({"error": "Postcode required"}), 400
    try:
        cache_key = f"shops:{postcode}"
        hit = _services_cache.get(cache_key)
        if hit and time.time() - hit["ts"] < _SERVICES_TTL and not debug:
            return jsonify(hit["data"])
        lat, lon = _latlon_for_postcode(postcode)
        if lat is None:
            return jsonify({"error": "Postcode not found"}), 404
        if debug:
            # Full query including relations — same as production
            q = f"""[out:json][timeout:25];
(node["shop"="supermarket"](around:5000,{lat},{lon});
way["shop"="supermarket"](around:5000,{lat},{lon});
relation["shop"="supermarket"](around:5000,{lat},{lon});
);out center tags;"""
            dbg = {"lat": lat, "lon": lon, "overpass_urls": {}}
            for url in _OVERPASS_URLS:
                try:
                    r = requests.post(url, data={"data": q}, timeout=20)
                    els = r.json().get("elements", []) if r.status_code == 200 else []
                    # sort by distance so sample shows nearest first
                    def _d(e):
                        c = e.get("center") or {}
                        lt = e.get("lat") or c.get("lat") or 0
                        ln = e.get("lon") or c.get("lon") or 0
                        return haversine_km(lat, lon, lt, ln) if lt else 999
                    els_sorted = sorted(els, key=_d)
                    names = [(e.get("tags", {}).get("name", "?"), e.get("type"), round(_d(e)*1000)) for e in els_sorted[:15]]
                    dbg["overpass_urls"][url] = {"status": r.status_code, "count": len(els), "nearest_15": names}
                except Exception as e:
                    dbg["overpass_urls"][url] = {"error": str(e)}
            # Also show what Google Places returns
            gp = _places_nearby(lat, lon, "supermarket", 10000, 15)
            dbg["google_places"] = [{"name": p["name"], "dist_m": int(p["distance_km"]*1000)} for p in gp]
            dbg["merged_final"] = [{"name": s["name"], "dist_m": int(s.get("distance_km",0)*1000)} for s in _fetch_supermarkets(lat, lon)]
            return jsonify(dbg)
        with ThreadPoolExecutor(max_workers=3) as ex:
            sm_f   = ex.submit(_fetch_supermarkets, lat, lon)
            offl_f = ex.submit(_fetch_off_licences_overpass, lat, lon)
            conv_f = ex.submit(_fetch_convenience_overpass, lat, lon)
            supermarkets  = sm_f.result(timeout=15)
            off_licences  = offl_f.result(timeout=15)
            convenience   = conv_f.result(timeout=15)
        result = {"supermarkets": supermarkets, "off_licences": off_licences,
                  "convenience": convenience}
        if supermarkets or off_licences or convenience:
            _services_cache[cache_key] = {"data": result, "ts": time.time()}
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/places", methods=["GET"])
def api_myarea_places_get():
    from_number = request.args.get("from_number", "").strip()
    device_id   = request.args.get("device_id", "").strip()
    if not from_number and not device_id:
        return jsonify([])
    try:
        q = lib._sb().table("my_area_places") \
            .select("id,name,address,phone,emoji,category,postcode,opening_hours") \
            .neq("category", "_home") \
            .order("created_at", desc=False)
        q = q.eq("from_number", from_number) if from_number else q.eq("device_id", device_id)
        return jsonify(q.execute().data or [])
    except Exception:
        return jsonify([])  # graceful — localStorage is primary store


@app.route("/api/myarea/places", methods=["POST"])
def api_myarea_places_post():
    from_number = request.args.get("from_number", "").strip()
    device_id   = request.args.get("device_id", "").strip()
    if not from_number and not device_id:
        return jsonify({"error": "from_number or device_id required"}), 400
    body = request.get_json(force=True, silent=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    record = {
        "device_id":     from_number or device_id,
        "name":          name,
        "address":       (body.get("address") or "").strip(),
        "phone":         (body.get("phone") or "").strip(),
        "emoji":         (body.get("emoji") or "📍").strip(),
        "category":      (body.get("category") or "place").strip(),
        "postcode":      (body.get("postcode") or "").strip(),
        "opening_hours": (body.get("opening_hours") or "").strip()[:200],
    }
    if from_number:
        record["from_number"] = from_number
    try:
        row = lib._sb().table("my_area_places").insert(record).execute().data[0]
        return jsonify(row)
    except Exception:
        record.pop("from_number", None)
        try:
            row = lib._sb().table("my_area_places").insert(record).execute().data[0]
            return jsonify(row)
        except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/places/<place_id>", methods=["DELETE"])
def api_myarea_places_delete(place_id):
    from_number = request.args.get("from_number", "").strip()
    device_id   = request.args.get("device_id", "").strip()
    if not from_number and not device_id:
        return jsonify({"error": "from_number or device_id required"}), 400
    try:
        sb = lib._sb()
        if from_number:
            try:
                sb.table("my_area_places").delete().eq("id", place_id).eq("from_number", from_number).execute()
            except Exception:
                sb.table("my_area_places").delete().eq("id", place_id).execute()
        else:
            sb.table("my_area_places").delete().eq("id", place_id).eq("device_id", device_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/places/<place_id>", methods=["PATCH"])
def api_myarea_places_patch(place_id):
    from_number = request.args.get("from_number", "").strip()
    device_id   = request.args.get("device_id", "").strip()
    if not from_number and not device_id:
        return jsonify({"error": "from_number or device_id required"}), 400
    body = request.get_json(force=True, silent=True) or {}
    allowed = ("address", "phone", "opening_hours", "name")
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        return jsonify({"error": "nothing to update"}), 400
    try:
        sb = lib._sb()
        q = sb.table("my_area_places").update(updates).eq("id", place_id)
        if from_number:
            q = q.eq("from_number", from_number)
        else:
            q = q.eq("device_id", device_id)
        result = q.execute()
        return jsonify({"ok": True, "updated": result.data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/home-postcode", methods=["GET"])
def api_myarea_home_postcode_get():
    from_number = request.args.get("from_number", "").strip()
    token       = request.args.get("token", "").strip()  # legacy device_id
    key = from_number or token
    if not key:
        return jsonify({"postcode": None})
    try:
        q = lib._sb().table("my_area_places").select("postcode").eq("category", "_home").limit(1)
        q = q.eq("from_number", from_number) if from_number else q.eq("device_id", token)
        rows = q.execute().data
        return jsonify({"postcode": rows[0]["postcode"] if rows else None})
    except Exception:
        return jsonify({"postcode": None})


@app.route("/api/myarea/home-postcode", methods=["POST"])
def api_myarea_home_postcode_post():
    body = request.get_json(force=True, silent=True) or {}
    from_number = (body.get("from_number") or "").strip()
    token       = (body.get("token") or "").strip()  # legacy device_id
    postcode    = (body.get("postcode") or "").strip().upper()
    key = from_number or token
    if not key or not postcode:
        return jsonify({"ok": False}), 400
    try:
        sb = lib._sb()
        record = {"name": "__home__", "category": "_home", "postcode": postcode, "emoji": "📍",
                  "device_id": key}
        if from_number:
            sb.table("my_area_places").delete().eq("from_number", from_number).eq("category", "_home").execute()
            record["from_number"] = from_number
        else:
            sb.table("my_area_places").delete().eq("device_id", token).eq("category", "_home").execute()
        sb.table("my_area_places").insert(record).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/intel/brand-choices", methods=["GET"])
def api_intel_brand_choices_get():
    token = request.args.get("token", "").strip()
    if not token:
        return jsonify({"choices": {}})
    try:
        rows = lib._sb().table("brand_choices") \
            .select("brand_key,chosen") \
            .eq("device_id", token) \
            .execute().data
        choices = {r["brand_key"]: r["chosen"] for r in rows}
        return jsonify({"choices": choices})
    except Exception:
        return jsonify({"choices": {}})


@app.route("/api/intel/brand-choices", methods=["POST"])
def api_intel_brand_choices_post():
    body = request.get_json(force=True, silent=True) or {}
    token = body.get("token", "").strip()
    brand_key = (body.get("brand_key") or "").strip().lower()
    chosen = (body.get("chosen") or "").strip()
    if not token or not brand_key or not chosen:
        return jsonify({"ok": False}), 400
    try:
        sb = lib._sb()
        sb.table("brand_choices").delete() \
            .eq("device_id", token).eq("brand_key", brand_key).execute()
        sb.table("brand_choices").insert({
            "device_id": token,
            "brand_key": brand_key,
            "chosen":    chosen,
        }).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/intel/brand-choices/delete", methods=["POST"])
def api_intel_brand_choices_delete():
    body = request.get_json(force=True, silent=True) or {}
    token = body.get("token", "").strip()
    brand_key = (body.get("brand_key") or "").strip().lower()
    if not token or not brand_key:
        return jsonify({"ok": False}), 400
    try:
        lib._sb().table("brand_choices").delete() \
            .eq("device_id", token).eq("brand_key", brand_key).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/intel/pins")
def api_intel_pins():
    did = (request.args.get("device_id") or "").strip()
    try:
        sb = lib._sb()
        rows = sb.table("ai_cache").select("key,data").execute().data
        pins = []
        for row in rows:
            d = row.get("data") or {}
            pinned_by = d.get("_pinned_by") or []
            # Legacy: treat old global _pinned as pinned for everyone (migrates on next pin action)
            if did and did not in pinned_by:
                if not d.get("_pinned"):
                    continue
            elif not did and not (d.get("_pinned") or pinned_by):
                continue
            key = row.get("key", "")
            name = d.get("name") or ""
            if not name:
                part = key.split(":")[1].split("|")[0] if ":" in key else key
                name = part.replace("-", " ").title()
            kind = "brand" if key.startswith("brand:") else "company"
            pins.append({
                "name":        name,
                "type":        kind,
                "description": (d.get("description") or d.get("extract") or "")[:100],
                "domain":      d.get("domain") or "",
            })
        pins.sort(key=lambda x: x["name"].lower())
        return jsonify({"pins": pins})
    except Exception as e:
        return jsonify({"pins": [], "error": str(e)})


@app.route("/api/intel/pin", methods=["POST"])
def api_intel_pin():
    body = request.get_json(force=True, silent=True) or {}
    name = (body.get("name") or "").strip()
    kind = (body.get("type") or "").strip()
    did  = (body.get("device_id") or "").strip()
    if not name or kind not in ("brand", "company"):
        return jsonify({"error": "name and type required"}), 400
    sb_key = f"brand:{name.lower()}|brandv29" if kind == "brand" else f"company:{name.lower()}|v17"
    try:
        sb = lib._sb()
        rows = sb.table("ai_cache").select("data").eq("key", sb_key).execute().data
        if not rows:
            return jsonify({"error": "No cached result to pin — search first"}), 404
        data = dict(rows[0]["data"] or {})
        pinned_by = list(data.get("_pinned_by") or [])
        if did and did not in pinned_by:
            pinned_by.append(did)
        data["_pinned_by"] = pinned_by
        data.pop("_pinned", None)  # migrate legacy flag
        sb.table("ai_cache").upsert({"key": sb_key, "data": data, "cached_at": "2099-12-31T00:00:00+00:00"}).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/intel/unpin", methods=["POST"])
def api_intel_unpin():
    body = request.get_json(force=True, silent=True) or {}
    name = (body.get("name") or "").strip()
    kind = (body.get("type") or "").strip()
    did  = (body.get("device_id") or "").strip()
    if not name or kind not in ("brand", "company"):
        return jsonify({"error": "name and type required"}), 400
    sb_key = f"brand:{name.lower()}|brandv29" if kind == "brand" else f"company:{name.lower()}|v17"
    try:
        sb = lib._sb()
        rows = sb.table("ai_cache").select("data").eq("key", sb_key).execute().data
        if rows:
            data = dict(rows[0]["data"] or {})
            pinned_by = [x for x in (data.get("_pinned_by") or []) if x != did]
            data["_pinned_by"] = pinned_by
            data.pop("_pinned", None)  # migrate legacy flag
            sb.table("ai_cache").upsert({"key": sb_key, "data": data, "cached_at": "now()"}).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/intel/research", methods=["POST"])
def api_intel_research():
    """Intel Research Agent — agentic multi-tool company research brief."""
    from intel_agent import run_research_agent
    data = request.get_json(silent=True) or {}
    company = (data.get("company") or request.args.get("company") or "").strip()
    if not company:
        return jsonify({"error": "company required"}), 400
    brief = run_research_agent(company)
    return jsonify(brief)


@app.route("/api/intel/compare")
def api_intel_compare():
    """Side-by-side brand/company comparison powered by Groq."""
    import concurrent.futures as _cf, json as _json, re as _re
    a = request.args.get("a", "").strip()
    b = request.args.get("b", "").strip()
    mode = request.args.get("mode", "brand")  # brand | company
    if not a or not b:
        return jsonify({"error": "a and b required"}), 400

    # Fetch both in parallel using existing data functions
    with _cf.ThreadPoolExecutor(max_workers=2) as ex:
        if mode == "company":
            fa = ex.submit(fetch_company_info, a)
            fb = ex.submit(fetch_company_info, b)
        else:
            fa = ex.submit(fetch_brand_data, a)
            fb = ex.submit(fetch_brand_data, b)
        da = fa.result()
        db = fb.result()

    def _summary(d, name):
        if mode == "company":
            w = d.get("wiki") or {}
            return (f"Name: {d.get('name') or name}\n"
                    f"Industry: {w.get('industry','')}\n"
                    f"Founded: {w.get('founded','')}\n"
                    f"HQ: {w.get('hq','')}\n"
                    f"Revenue: {w.get('revenue','')}\n"
                    f"Employees: {w.get('employees','')}\n"
                    f"Overview: {(w.get('extract') or '')[:400]}")
        else:
            return (f"Name: {d.get('name') or name}\n"
                    f"Founded: {d.get('founded','')}\n"
                    f"HQ: {d.get('hq','')}\n"
                    f"Revenue: {d.get('revenue','')}\n"
                    f"Slogan: {d.get('slogan','')}\n"
                    f"Description: {(d.get('description') or '')[:300]}\n"
                    f"Competitors: {', '.join((d.get('competitors') or [])[:5])}")

    a_label = da.get("name") or a
    b_label = db.get("name") or b
    prompt = (
        f"Compare these two {'companies' if mode == 'company' else 'brands'} head-to-head.\n\n"
        f"--- BRAND A: {a_label} ---\n{_summary(da, a)}\n\n"
        f"--- BRAND B: {b_label} ---\n{_summary(db, b)}\n\n"
        f"IMPORTANT: In every 'a' field put data about {a_label}. In every 'b' field put data about {b_label}.\n"
        f"a_edge = one sentence on {a_label}'s unique advantage over {b_label}.\n"
        f"b_edge = one sentence on {b_label}'s unique advantage over {a_label}.\n"
        "Return JSON only:\n"
        '{"verdict": "one punchy sentence on which is stronger and why",'
        '"dimensions": ['
        '{"label":"Positioning","a":"value","b":"value"},'
        '{"label":"Revenue / Scale","a":"value","b":"value"},'
        '{"label":"Founded","a":"value","b":"value"},'
        '{"label":"Key Markets","a":"value","b":"value"},'
        '{"label":"Brand Strength","a":"High/Medium/Low + reason","b":"value"},'
        '{"label":"Parent Company","a":"value","b":"value"}'
        '],'
        f'"a_edge": "one sentence: where {a_label} wins",'
        f'"b_edge": "one sentence: where {b_label} wins"'
        '}'
    )
    try:
        raw = _groq_chat("You are a brand strategy analyst. Reply with JSON only.", [{"role":"user","content":prompt}], max_tokens=600, json_mode=True)
        raw = _re.sub(r"^```[a-z]*\n?","",raw).rstrip("`").strip()
        result = _json.loads(raw)
        result["a_name"] = da.get("name") or a
        result["b_name"] = db.get("name") or b
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/clear-brand-cache", methods=["POST"])
def api_admin_clear_brand_cache():
    """Clear cached brand/company data so it re-fetches fresh. Token-protected."""
    if request.headers.get("X-Admin-Token") != "miru-digest-2026":
        return jsonify({"error": "Forbidden"}), 403
    body = request.get_json(force=True, silent=True) or {}
    name = (body.get("name") or "").strip().lower()
    if not name:
        return jsonify({"error": "name required"}), 400
    from search import _sb_cache_delete, _BRAND_CACHE
    sb_key_brand   = f"brand:{name}|brandv29"
    sb_key_company = f"company:{name}|v17"
    _sb_cache_delete(sb_key_brand)
    _sb_cache_delete(sb_key_company)
    # Also bust brand_profiles table
    try:
        lib._sb().table("brand_profiles").delete().ilike("name", name).execute()
    except Exception:
        pass
    # Bust in-memory cache
    for k in list(_BRAND_CACHE.keys()):
        if k.startswith(name + "|"):
            del _BRAND_CACHE[k]
    return jsonify({"ok": True, "cleared": name})


@app.route("/api/brand/scan", methods=["POST"])
def api_brand_scan():
    """Identify a brand from a photo using Groq vision."""
    body = request.get_json(force=True, silent=True) or {}
    img_b64 = (body.get("image") or "").strip()
    mime = body.get("mime", "image/jpeg")
    if not img_b64:
        return jsonify({"error": "image required"}), 400
    try:
        raw = _groq_vision(
            img_b64, mime,
            'Identify the brand shown in this image (on packaging, label, logo, or advertisement). '
            'Return JSON only — no markdown:\n'
            '{"brand": "ExactBrandName or null", "confidence": "high/medium/low", '
            '"category": "product category e.g. chocolate, beer, shampoo, trainers, phone — or null", '
            '"search_query": "the most specific unambiguous search term e.g. \'Galaxy chocolate\' or \'Heineken beer\' or \'Nike trainers\' — combine brand + category if needed to avoid confusing it with another brand of the same name"}'
        )
        import json as _json
        parsed = _json.loads(raw.strip().strip("```json").strip("```").strip())
        return jsonify(parsed)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


_WMO_EMOJI = {
    0:"☀️",1:"🌤️",2:"⛅",3:"🌥️",45:"🌫️",48:"🌫️",
    51:"🌦️",53:"🌦️",55:"🌧️",61:"🌧️",63:"🌧️",65:"🌧️",
    71:"❄️",73:"❄️",75:"❄️",77:"🌨️",
    80:"🌦️",81:"🌦️",82:"⛈️",85:"🌨️",86:"🌨️",95:"⛈️",96:"⛈️",99:"⛈️",
}
_WMO_DESC = {
    0:"Clear sky",1:"Mainly clear",2:"Partly cloudy",3:"Overcast",
    45:"Fog",48:"Icy fog",51:"Light drizzle",53:"Drizzle",55:"Heavy drizzle",
    61:"Light rain",63:"Rain",65:"Heavy rain",71:"Light snow",73:"Snow",75:"Heavy snow",
    80:"Showers",81:"Showers",82:"Heavy showers",95:"Thunderstorm",96:"Thunderstorm",99:"Thunderstorm",
}

_LOCAL_INFO_CACHE: dict = {}
_LOCAL_INFO_TTL = 1800  # 30 min

@app.route("/api/myarea/local-info")
def api_myarea_local_info():
    postcode = request.args.get("postcode", "").replace(" ", "").upper()
    cached = _LOCAL_INFO_CACHE.get(postcode)
    if cached and time.time() - cached["_ts"] < _LOCAL_INFO_TTL:
        return jsonify({k: v for k, v in cached.items() if k != "_ts"})

    result = _resolve_postcode(postcode)
    if not result:
        return jsonify({"error": "postcode not found"}), 404
    _postcode, lat, lon, _pc_fmt = result

    import concurrent.futures as _cf_li, datetime as _dt_li
    from collections import Counter as _Ctr

    def _get_weather():
        try:
            r = requests.get("https://api.open-meteo.com/v1/forecast", params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,weather_code,wind_speed_10m",
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "Europe/London", "forecast_days": 1,
            }, timeout=8)
            d = r.json()
            code = d["current"]["weather_code"]
            return {
                "temp": round(d["current"]["temperature_2m"]),
                "emoji": _WMO_EMOJI.get(code, "🌡️"),
                "desc": _WMO_DESC.get(code, ""),
                "wind": round(d["current"]["wind_speed_10m"]),
                "high": round(d["daily"]["temperature_2m_max"][0]),
                "low":  round(d["daily"]["temperature_2m_min"][0]),
            }
        except Exception as e:
            print(f"[weather] {e}"); return None

    def _get_crime():
        try:
            from search import fetch_crime_data
            data = fetch_crime_data(lat, lon)
            if not data or not data.get("total"):
                return None
            top3 = [{"category": b["category"], "count": b["count"]} for b in data["breakdown"][:3]]
            months = data.get("months_covered", [])
            month_label = f"{months[0]} to {months[-1]}" if len(months) > 1 else (months[0] if months else "")
            return {"total": data["total"], "top": top3, "month": month_label}
        except Exception as e:
            print(f"[crime] {e}"); return None

    def _get_petrol():
        try:
            nearby = _nearby_stations(lat, lon, "petrol", 5.0)
            nearby.sort(key=lambda x: (x["price"], x["dist_mi"]))
            return [{"name": s["name"], "price": s["price"], "dist_mi": round(s["dist_mi"], 1)}
                    for s in nearby[:3]]
        except Exception as e:
            print(f"[petrol] {e}"); return []

    with _cf_li.ThreadPoolExecutor(max_workers=3) as ex:
        fw = ex.submit(_get_weather)
        fc = ex.submit(_get_crime)
        fp = ex.submit(_get_petrol)
        weather, crime, petrol = fw.result(), fc.result(), fp.result()

    payload = {"weather": weather, "crime": crime, "petrol": petrol}
    _LOCAL_INFO_CACHE[postcode] = {**payload, "_ts": time.time()}
    return jsonify(payload)


_MOT_CACHE: dict = {}
_MOT_CACHE_TTL = 86400  # 24 hours — MOT status changes at most once a year

@app.route("/api/mot")
def api_mot():
    """Fetch MOT history for a UK registration from DVSA API."""
    reg = request.args.get("reg", "").strip().upper().replace(" ", "")
    if not reg:
        return jsonify({"error": "reg required"}), 400
    # Normalise common O/0 confusion: UK plates are LL DD LLL — letters in positions 0-1 and 4-6
    # digits in positions 2-3. Swap 0→O in letter positions, O→0 in digit positions.
    if len(reg) == 7:
        reg = list(reg)
        for i in (0, 1, 4, 5, 6):
            if reg[i] == "0": reg[i] = "O"
        for i in (2, 3):
            if reg[i] == "O": reg[i] = "0"
        reg = "".join(reg)
    cache_hit = _MOT_CACHE.get(reg)
    if cache_hit and time.time() - cache_hit["ts"] < _MOT_CACHE_TTL:
        return jsonify(cache_hit["data"])
    ves_key = os.environ.get("DVLA_VES_API_KEY", "")
    if not ves_key:
        return jsonify({"error": "DVLA_VES_API_KEY not configured — add your Vehicle Enquiry Service key in Railway env vars"}), 503
    try:
        import uuid as _uuid
        r = requests.post(
            "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles",
            headers={
                "x-api-key":        ves_key,
                "x-correlation-id": str(_uuid.uuid4()),
                "Content-Type":     "application/json",
            },
            json={"registrationNumber": reg},
            timeout=12,
        )
        if r.status_code == 404:
            return jsonify({"error": "Vehicle not found — check the plate and try again"}), 404
        if r.status_code == 400:
            return jsonify({"error": "Invalid registration — check the plate and try again"}), 400
        if r.status_code == 403:
            return jsonify({"error": "API key invalid or not yet active"}), 403
        if r.status_code != 200:
            return jsonify({"error": f"DVLA API error {r.status_code}"}), 502
        v = r.json()
        from datetime import date as _date
        def _days(date_str):
            if not date_str:
                return None
            try:
                return (_date.fromisoformat(date_str) - _date.today()).days
            except Exception:
                return None
        fuel_raw = v.get("fuelType", "")
        euro = v.get("euroStatus", "")
        # ULEZ exempt: petrol Euro 4+, diesel Euro 6+, electric/hybrid always exempt
        fuel_lc = fuel_raw.lower()
        ulez_exempt = None
        if "electric" in fuel_lc or "hybrid" in fuel_lc:
            ulez_exempt = True
        elif euro:
            euro_num = 0
            import re as _re
            m = _re.search(r"(\d+)", euro)
            if m:
                euro_num = int(m.group(1))
            if "diesel" in fuel_lc:
                ulez_exempt = euro_num >= 6
            else:
                ulez_exempt = euro_num >= 4
        cc = v.get("engineCapacity")
        engine_litres = f"{cc/1000:.1f}L" if cc else ""
        data = {
            "registration":  v.get("registrationNumber", reg),
            "make":          v.get("make", "").title(),
            "colour":        v.get("colour", "").title(),
            "fuel":          fuel_raw.title(),
            "year":          v.get("yearOfManufacture", ""),
            "engine":        engine_litres,
            "co2":           v.get("co2Emissions"),
            "euro":          euro,
            "ulez_exempt":   ulez_exempt,
            "art_end":       v.get("artEndDate", ""),
            "art_days_left": _days(v.get("artEndDate")),
            "v5c_issued":    v.get("dateOfLastV5CIssued", ""),
            "mot_status":    v.get("motStatus", ""),
            "mot_expiry":    v.get("motExpiryDate", ""),
            "days_left":     _days(v.get("motExpiryDate")),
            "tax_status":    v.get("taxStatus", ""),
            "tax_due":       v.get("taxDueDate", ""),
            "tax_days_left": _days(v.get("taxDueDate")),
        }
        _MOT_CACHE[reg] = {"data": data, "ts": time.time()}
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/vehicles", methods=["GET"])
def api_myarea_vehicles_get():
    device_id = request.args.get("device_id", "").strip()
    if not device_id:
        return jsonify([])
    try:
        rows = lib._sb().table("my_area_vehicles") \
            .select("id,registration,nickname") \
            .eq("device_id", device_id) \
            .order("created_at").execute().data
        return jsonify(rows or [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/vehicles", methods=["POST"])
def api_myarea_vehicles_post():
    device_id = request.args.get("device_id", "").strip()
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    body = request.get_json(force=True, silent=True) or {}
    reg  = (body.get("registration") or "").strip().upper().replace(" ", "")
    if not reg:
        return jsonify({"error": "registration required"}), 400
    try:
        row = lib._sb().table("my_area_vehicles").upsert({
            "device_id":    device_id,
            "registration": reg,
            "nickname":     (body.get("nickname") or "").strip(),
        }, on_conflict="device_id,registration").execute().data[0]
        return jsonify(row)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/vehicles/<vehicle_id>", methods=["DELETE"])
def api_myarea_vehicles_delete(vehicle_id):
    device_id = request.args.get("device_id", "").strip()
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    try:
        lib._sb().table("my_area_vehicles") \
            .delete().eq("id", vehicle_id).eq("device_id", device_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/details", methods=["GET"])
def api_myarea_details_get():
    from_number = request.args.get("from_number", "").strip()
    device_id   = request.args.get("device_id", "").strip()
    did = from_number or device_id
    if not did:
        return jsonify([])
    try:
        q = lib._sb().table("ma_details").select("id,type,label,data,created_at")
        q = q.eq("device_id", did)
        return jsonify(q.order("created_at").execute().data or [])
    except Exception:
        return jsonify([])


@app.route("/api/myarea/details", methods=["POST"])
def api_myarea_details_post():
    from_number = request.args.get("from_number", "").strip()
    device_id   = request.args.get("device_id", "").strip()
    did = from_number or device_id
    if not did:
        return jsonify({"error": "from_number or device_id required"}), 400
    body = request.get_json(force=True, silent=True) or {}
    rec_id = (body.get("id") or "").strip()
    rec = {
        "device_id": did,
        "type":      body.get("type", "other"),
        "label":     body.get("label", ""),
        "data":      body.get("data", {}),
    }
    try:
        sb = lib._sb()
        if rec_id:
            rows = sb.table("ma_details").update(rec).eq("id", rec_id).eq("device_id", did).execute().data
            saved = rows[0] if rows else {}
        else:
            saved = sb.table("ma_details").insert(rec).execute().data[0]
        # Keep provider_hints in sync so future Gmail scans include this provider
        import threading as _t
        _t.Thread(target=_ma_gmail_sync_hints_from_details, args=(did,), daemon=True).start()
        return jsonify(saved)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/details/<detail_id>", methods=["DELETE"])
def api_myarea_details_delete(detail_id):
    from_number = request.args.get("from_number", "").strip()
    device_id   = request.args.get("device_id", "").strip()
    did = from_number or device_id
    if not did:
        return jsonify({"error": "from_number or device_id required"}), 400
    try:
        lib._sb().table("ma_details").delete().eq("id", detail_id).eq("device_id", did).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/details", methods=["DELETE"])
def api_myarea_details_delete_all():
    from_number = request.args.get("from_number", "").strip()
    device_id   = request.args.get("device_id", "").strip()
    did = from_number or device_id
    if not did:
        return jsonify({"error": "from_number or device_id required"}), 400
    try:
        lib._sb().table("ma_details").delete().eq("device_id", did).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/health")
def api_health():
    """Full diagnostic: env vars, Groq API call, JSON parse."""
    import requests as _req, os, time as _t, json as _json
    out = {}

    # 1. Environment keys
    groq_key  = os.environ.get("GROQ_API_KEY", "")
    out["env"] = {
        "GROQ_API_KEY":    "SET" if groq_key else "MISSING ❌",
        "SUPABASE_URL":    "SET" if os.environ.get("SUPABASE_URL") else "MISSING",
        "YOUTUBE_API_KEY": "SET" if os.environ.get("YOUTUBE_API_KEY") else "MISSING",
    }

    if not groq_key:
        out["groq"] = "SKIPPED — no key"
        return jsonify(out)

    # 2. Live Groq call
    t0 = _t.time()
    try:
        r = _req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [{"role": "user", "content": 'Brand: "Pepsi". Return ONLY valid JSON with keys: facts (founded,hq,revenue), competitors (array, 3 items), timeline (array, 3 items).'}],
                "temperature": 0.2, "max_tokens": 800,
            },
            timeout=30,
        )
        elapsed = round(_t.time() - t0, 2)
        out["groq"] = {"http_status": r.status_code, "elapsed_s": elapsed}

        if r.status_code == 200:
            content = r.json()["choices"][0]["message"]["content"].strip()
            out["groq"]["raw_preview"] = content[:600]
            import re as _re
            m = _re.search(r'\{.*\}', content, _re.DOTALL)
            if m:
                try:
                    parsed = _json.loads(m.group(0))
                    out["groq"]["parse"] = "OK ✅"
                    out["groq"]["timeline_items"] = len(parsed.get("timeline", []))
                    out["groq"]["competitor_items"] = len(parsed.get("competitors", []))
                    out["groq"]["facts"] = parsed.get("facts", {})
                except Exception as je:
                    out["groq"]["parse"] = f"JSON error: {je}"
            else:
                out["groq"]["parse"] = "No JSON object found in response ❌"
        else:
            out["groq"]["error_body"] = r.text[:400]
    except Exception as e:
        out["groq"] = {"error": str(e), "elapsed_s": round(_t.time() - t0, 2)}

    return jsonify(out)


@app.route("/api/company")
def api_company():
    from search import _COMPANY_CACHE
    name = request.args.get("name", "").strip()
    if not name or len(name) < 2:
        return jsonify({"error": "Company name required"}), 400
    if request.args.get("refresh"):
        from search import _COMPANY_VER
        _COMPANY_CACHE.pop(name.strip().lower() + "|" + _COMPANY_VER, None)
    analytics.log_search("company", name, request.remote_addr, request.user_agent.string)
    return jsonify(fetch_company_info(name))


_GROQ_VISION_MODELS = [
    "meta-llama/llama-4-scout-17b-16e-instruct",
    "llama-3.2-11b-vision-preview",
    "llama-3.2-90b-vision-preview",
]

def _groq_vision(img_b64: str, mime: str, prompt: str, model: str = None) -> str:
    """Send a base64-encoded image to Groq vision model and return extracted text.
    Tries multiple models in order until one succeeds."""
    models = [model] if model else _GROQ_VISION_MODELS
    last_err = None
    for m in models:
        body = {
            "model": m,
            "max_tokens": 1024,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_b64}"}},
                    {"type": "text", "text": prompt},
                ],
            }],
        }
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {os.environ.get('GROQ_API_KEY', '')}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=30,
            )
            if r.status_code == 200:
                return r.json()["choices"][0]["message"]["content"]
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
            print(f"[groq_vision] {m} → {last_err}")
        except Exception as e:
            last_err = str(e)
            print(f"[groq_vision] {m} → {last_err}")
    raise RuntimeError(f"All Groq vision models failed. Last error: {last_err}")


def _groq_chat(system, messages, max_tokens=600, json_mode=False, model="llama-3.1-8b-instant"):
    """Call Groq API (OpenAI-compatible). Returns reply text."""
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": system}] + messages,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    r = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ.get('GROQ_API_KEY', '')}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


@app.route("/api/company/results", methods=["POST"])
def api_company_results():
    data = request.json or {}
    company = data.get("company", "").strip()
    search_name = data.get("search_name", "").strip() or company
    if not company:
        return jsonify({"error": "Company name required"}), 400

    try:
        from search import _fetch_news
        results_news = _fetch_news(
            search_name,
            'results OR earnings OR "annual results" OR "quarterly results" OR "full year results" OR "half year results"',
            5
        )
        context = "\n".join(f"- {n['title']} ({n['source']}, {n['date']})" for n in results_news) \
            if results_news else "No recent results news found."

        text = _groq_chat(
            "You are a financial analyst covering UK-listed companies. Always respond with valid JSON only.",
            [{"role": "user", "content": f"""Based on these recent news headlines about {company}'s financial results:

{context}

Return a JSON object with exactly these fields:
- period: string (e.g. "FY2025" or "H1 2025")
- headline: string (one sentence summary of the key result)
- highlights: array of 3 strings (key metrics or highlights)
- sentiment: string ("positive", "neutral", or "negative")
- context: string (2-3 sentences summarising the results)"""}],
            max_tokens=500,
            json_mode=True,
            model="llama-3.3-70b-versatile",
        )
        try:
            result = json.loads(text)
        except Exception:
            m = re.search(r'\{[\s\S]*\}', text)
            result = json.loads(m.group(0)) if m else {"headline": text, "highlights": [], "context": text}

        result["news"] = results_news[:3]
        return jsonify(result)
    except Exception as e:
        print(f"[company/results] error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/company/chat", methods=["POST"])
def api_company_chat():
    data = request.json or {}
    company = data.get("company", "").strip()
    message = data.get("message", "").strip()
    context = data.get("context", "")
    history = data.get("history", [])

    if not company or not message:
        return jsonify({"error": "Missing fields"}), 400

    try:
        reply = _groq_chat(
            f"You are a concise financial analyst assistant specialising in UK companies. "
            f"The user is asking about {company}'s latest financial results. "
            f"Context: {context} "
            f"Keep answers to 2-4 sentences. Be factual and direct.",
            history + [{"role": "user", "content": message}],
            max_tokens=300,
        )
        return jsonify({"reply": reply})
    except Exception as e:
        print(f"[company/chat] error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/company/media")
def api_company_media_get():
    from search import _sb_cache_get
    name = request.args.get("name", "").strip().lower()
    if not name:
        return jsonify({}), 400
    data = _sb_cache_get(f"featured_media:{name}") or {}
    return jsonify(data)


@app.route("/api/company/media", methods=["POST"])
def api_company_media_set():
    from search import _sb_cache_set
    data = request.json or {}
    pw = data.get("password", "")
    if pw != os.environ.get("ADMIN_PASSWORD", "miru2024"):
        return jsonify({"error": "Unauthorized"}), 403
    name = data.get("name", "").strip().lower()
    if not name:
        return jsonify({"error": "Company name required"}), 400
    _sb_cache_set(f"featured_media:{name}", {
        "url":   data.get("url", "").strip(),
        "note":  data.get("note", "").strip(),
        "title": data.get("title", "").strip(),
    })
    return jsonify({"ok": True})


@app.route("/api/company/youtube-test")
def api_youtube_test():
    from search import _fetch_youtube
    key = os.environ.get("YOUTUBE_API_KEY", "")
    results = _fetch_youtube("Unilever")
    return jsonify({"count": len(results), "key_set": bool(key), "items": results})


@app.route("/api/company/groq-test")
def api_groq_test():
    key = os.environ.get("GROQ_API_KEY", "")
    # Show all env var names that contain "groq" or "key" (case-insensitive) to debug naming issues
    env_keys = [k for k in os.environ if "groq" in k.lower() or "key" in k.lower()]
    if not key:
        return jsonify({"status": "error", "detail": "GROQ_API_KEY not set", "env_keys_found": env_keys}), 500
    try:
        text = _groq_chat("You are helpful.", [{"role": "user", "content": "Say OK"}], max_tokens=10)
        return jsonify({"status": "ok", "reply": text, "key_prefix": key[:8] + "..."})
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


@app.route("/admin")
def admin():
    pw = os.environ.get("ADMIN_PASSWORD", "miru2024")
    if request.args.get("pw") != pw:
        return Response(
            '<form style="font-family:sans-serif;margin:80px auto;max-width:320px">'
            '<h2>Miru Admin</h2>'
            '<input name="pw" type="password" placeholder="Password" style="padding:8px;width:100%;margin:8px 0">'
            '<button type="submit" style="padding:8px 16px">Enter</button>'
            '</form>',
            status=401, mimetype="text/html"
        )

    s = analytics.get_stats()
    if "error" in s or not s:
        err = s.get("error", "Unknown error") if s else "get_stats returned empty"
        db_url_set = "YES" if os.environ.get("DATABASE_URL") else "NO"
        return f"<p style='font-family:sans-serif;padding:40px'>Analytics unavailable.<br><b>Error:</b> {err}<br><b>DATABASE_URL set:</b> {db_url_set}</p>"

    # Build daily sparkline data
    daily_labels = [d["day"][-5:] for d in s.get("daily", [])]  # MM-DD
    daily_counts = [d["count"] for d in s.get("daily", [])]

    by_type_html = "".join(
        f'<tr><td>{r["type"].title()}</td><td><b>{r["count"]}</b></td></tr>'
        for r in s.get("by_type", [])
    )
    top_pc_html = "".join(
        f'<tr><td>{r["query"]}</td><td><b>{r["count"]}</b></td></tr>'
        for r in s.get("top_postcodes", [])
    )
    top_co_html = "".join(
        f'<tr><td>{r["query"]}</td><td><b>{r["count"]}</b></td></tr>'
        for r in s.get("top_companies", [])
    )
    recent_html = "".join(
        f'<tr><td>{r["at"]}</td><td>{r["type"].title()}</td><td>{r["query"]}</td><td style="color:#999">{r["ip"]}</td></tr>'
        for r in s.get("recent", [])
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Miru — Admin</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
          background: #0f0f13; color: #e0e0e0; padding: 24px; }}
  h1 {{ font-size: 1.4rem; font-weight: 700; margin-bottom: 4px; color: #fff; }}
  .sub {{ color: #666; font-size: 0.85rem; margin-bottom: 24px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 16px; margin-bottom: 24px; }}
  .card {{ background: #1a1a24; border-radius: 12px; padding: 16px; }}
  .card .num {{ font-size: 2rem; font-weight: 700; color: #fff; }}
  .card .lbl {{ font-size: 0.75rem; color: #666; margin-top: 4px; text-transform: uppercase; letter-spacing: .5px; }}
  .section {{ background: #1a1a24; border-radius: 12px; padding: 16px; margin-bottom: 16px; }}
  .section h2 {{ font-size: 0.85rem; text-transform: uppercase; letter-spacing: .5px; color: #666; margin-bottom: 12px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
  td, th {{ padding: 6px 8px; text-align: left; border-bottom: 1px solid #2a2a36; }}
  th {{ color: #666; font-weight: 500; font-size: 0.8rem; }}
  .bar-wrap {{ display: flex; align-items: flex-end; gap: 3px; height: 60px; margin-top: 8px; }}
  .bar {{ flex: 1; background: #6c63ff; border-radius: 3px 3px 0 0; min-height: 2px; }}
  .bar-labels {{ display: flex; gap: 3px; margin-top: 4px; }}
  .bar-labels span {{ flex: 1; font-size: 9px; color: #555; text-align: center; overflow: hidden; }}
</style>
</head>
<body>
<h1>Miru Analytics</h1>
<div class="sub">Admin dashboard — private</div>

<div class="grid">
  <div class="card"><div class="num">{s['total']}</div><div class="lbl">Total searches</div></div>
  <div class="card"><div class="num">{s['today']}</div><div class="lbl">Today</div></div>
  <div class="card"><div class="num">{s['week']}</div><div class="lbl">Last 7 days</div></div>
</div>

<div class="section">
  <h2>Activity — last 14 days</h2>
  <div class="bar-wrap">
    {''.join(f'<div class="bar" style="height:{int(c / max(daily_counts or [1]) * 56) + 4}px" title="{daily_labels[i] if i < len(daily_labels) else ""}: {c}"></div>' for i, c in enumerate(daily_counts))}
  </div>
  <div class="bar-labels">
    {''.join(f'<span>{l}</span>' for l in daily_labels)}
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-bottom:16px">
  <div class="section">
    <h2>By type</h2>
    <table><thead><tr><th>Type</th><th>Count</th></tr></thead>
    <tbody>{by_type_html}</tbody></table>
  </div>
  <div class="section">
    <h2>Top postcodes</h2>
    <table><thead><tr><th>Postcode</th><th>Count</th></tr></thead>
    <tbody>{top_pc_html}</tbody></table>
  </div>
  <div class="section">
    <h2>Top companies</h2>
    <table><thead><tr><th>Company</th><th>Count</th></tr></thead>
    <tbody>{top_co_html}</tbody></table>
  </div>
</div>

<div class="section">
  <h2>Recent searches</h2>
  <table>
    <thead><tr><th>Time</th><th>Type</th><th>Query</th><th>IP</th></tr></thead>
    <tbody>{recent_html}</tbody>
  </table>
</div>

</body>
</html>"""
    return html


@app.route("/api/admin/stats")
def api_admin_stats():
    pw = os.environ.get("ADMIN_PASSWORD", "miru2024")
    if request.args.get("pw") != pw:
        return jsonify({"error": "unauthorized"}), 401
    s = analytics.get_stats()
    return jsonify(s)



# ── Local Places ──────────────────────────────────────────────────────────────

_PLACE_CATEGORY = {
    "library":          {"label": "Library",           "emoji": "📚"},
    "community_centre": {"label": "Community Centre",  "emoji": "🏘️"},
    "arts_centre":      {"label": "Arts Centre",       "emoji": "🎭"},
    "doctors":          {"label": "GP Surgery",        "emoji": "🩺"},
    "clinic":           {"label": "Clinic",            "emoji": "🩺"},
    "hospital":         {"label": "Hospital",          "emoji": "🏥"},
    "dentist":          {"label": "Dentist",           "emoji": "🦷"},
    "pharmacy":         {"label": "Pharmacy",          "emoji": "💊"},
    "post_office":      {"label": "Post Office",       "emoji": "📮"},
    "townhall":         {"label": "Town Hall / Council","emoji": "🏛️"},
    "social_facility":  {"label": "Social Services",   "emoji": "🤝"},
    "food_bank":        {"label": "Food Bank",         "emoji": "🍞"},
    "police":           {"label": "Police Station",    "emoji": "👮"},
    "fire_station":     {"label": "Fire Station",      "emoji": "🚒"},
    "sports_centre":    {"label": "Sports Centre",     "emoji": "🏋️"},
    "leisure_centre":   {"label": "Leisure Centre",    "emoji": "🏊"},
    "swimming_pool":    {"label": "Swimming Pool",     "emoji": "🏊"},
    "fitness_centre":   {"label": "Gym / Fitness",     "emoji": "💪"},
    "park":             {"label": "Park",              "emoji": "🌳"},
    "playground":       {"label": "Playground",        "emoji": "🛝"},
    "attraction":       {"label": "Attraction",        "emoji": "🎡"},
    "cafe":             {"label": "Café",              "emoji": "☕"},
    "restaurant":       {"label": "Restaurant",        "emoji": "🍽️"},
    "fast_food":        {"label": "Fast Food",         "emoji": "🍔"},
    "pub":              {"label": "Pub",               "emoji": "🍺"},
    "bar":              {"label": "Bar",               "emoji": "🍹"},
    "fuel":             {"label": "Petrol Station",    "emoji": "⛽"},
    # Commercial services (shop=*)
    "hairdresser":      {"label": "Hairdresser",       "emoji": "✂️"},
    "beauty":           {"label": "Beauty Salon",      "emoji": "💅"},
    "car_repair":       {"label": "Garage / Mechanic", "emoji": "🔧"},
    "optician":         {"label": "Optician",          "emoji": "👓"},
    "dry_cleaning":     {"label": "Dry Cleaning",      "emoji": "👔"},
    "laundry":          {"label": "Laundry",           "emoji": "🧺"},
    "massage":          {"label": "Massage",           "emoji": "💆"},
    "tattoo":           {"label": "Tattoo & Piercing", "emoji": "🖊️"},
    "travel_agency":    {"label": "Travel Agent",      "emoji": "✈️"},
    "estate_agent":     {"label": "Estate Agent",      "emoji": "🏠"},
    "pet_grooming":     {"label": "Pet Grooming",      "emoji": "🐾"},
    "veterinary":       {"label": "Vet",               "emoji": "🐾"},
    "bank":             {"label": "Bank",              "emoji": "🏦"},
}

_PLACE_ACTIVITIES = {
    "library":          ["Borrow books, DVDs & ebooks", "Free Wi-Fi & computers", "Study & reading space", "Children's storytime & activities", "Events, talks & exhibitions"],
    "community_centre": ["Classes & workshops", "Meeting room hire", "Social clubs & activities", "Children's & senior groups", "Events & performances"],
    "arts_centre":      ["Exhibitions & galleries", "Live performances", "Art classes & workshops", "Cinema screenings", "Community events"],
    "doctors":          ["GP appointments (call or book online)", "Repeat prescriptions", "Health checks & immunisations", "Referrals to specialists", "Mental health support"],
    "hospital":         ["A&E (emergencies only)", "Outpatient clinics", "Inpatient wards", "Specialist departments", "Diagnostic services"],
    "dentist":          ["NHS & private check-ups", "Fillings & extractions", "Hygienist & cleaning", "Orthodontics", "Emergency appointments"],
    "pharmacy":         ["Collect NHS prescriptions", "Over-the-counter medicines & advice", "Flu & travel vaccinations", "Blood pressure & health checks", "Pharmacy First (minor illness)"],
    "post_office":      ["Post & parcel drop-off", "Recorded & tracked delivery", "Bill payments & banking", "Travel money & passports", "Government services (DVLA, HMRC)"],
    "townhall":         ["Council tax payments", "Planning & building control", "Licensing applications", "Register births, deaths & marriages", "Housing & benefits enquiries"],
    "social_facility":  ["Benefits & housing advice", "Disability & carer support", "Older persons' services", "Domestic abuse referrals", "Food & financial assistance"],
    "food_bank":        ["Emergency food parcels (referral needed)", "Toiletries & household essentials", "Signposting to other support"],
    "police":           ["Report non-emergency crime (101)", "Lost & found property", "Community liaison", "Firearms licensing"],
    "fire_station":     ["Emergency fire & rescue (999)", "Home fire safety visits", "Community safety advice"],
    "sports_centre":    ["Gym & fitness classes", "Sports halls (badminton, basketball)", "Racquet courts", "Spinning & aerobics", "Junior sport sessions"],
    "leisure_centre":   ["Swimming pool & lessons", "Gym & classes", "Sports halls", "Racquet sports", "Café & changing facilities"],
    "swimming_pool":    ["Public lane swimming", "Aqua aerobics", "Family & children's sessions", "Swimming lessons (adults & children)", "Early-morning & evening sessions"],
    "fitness_centre":   ["Gym equipment & free weights", "Group fitness classes", "Personal training", "Cardio & strength zones"],
    "park":             ["Walking & running paths", "Children's play areas", "Picnic & open green space", "Outdoor fitness equipment", "Sports pitches & courts"],
    "cafe":             ["Coffee, tea & hot drinks", "Light bites & pastries", "Breakfast & lunch", "Wi-Fi & a relaxed seat", "Takeaway available"],
    "restaurant":       ["Dine-in meals", "Takeaway & delivery", "Private dining & group bookings", "Special dietary options"],
    "fast_food":        ["Quick meals & snacks", "Takeaway & delivery", "Drive-through (where available)"],
    "pub":              ["Food & drinks", "Live sport screenings", "Quiz nights & events", "Beer garden (where available)"],
    "fuel":             ["Petrol & diesel", "Air & water point", "Car wash", "Shop & convenience items", "ATM (at many sites)"],
}


def _nom_search(query: str, limit: int = 5) -> list:
    """Nominatim search, returns list of (name, lat, lon, class, type, osm_id)."""
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": limit, "countrycodes": "gb"},
            headers={"User-Agent": "Miru/1.0 (miru.app)"},
            timeout=7,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return []


def _google_places_search(query: str, limit: int = 3) -> list:
    """Google Places Text Search. Returns list of dicts with name, lat, lon."""
    if not _GOOGLE_PLACES_KEY:
        return []
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": query + " UK", "key": _GOOGLE_PLACES_KEY, "region": "uk"},
            timeout=8,
        )
        results = []
        for p in r.json().get("results", [])[:limit]:
            loc = p.get("geometry", {}).get("location", {})
            if loc:
                results.append({
                    "name": p.get("name", ""),
                    "lat":  loc["lat"],
                    "lon":  loc["lng"],
                })
        return results
    except Exception:
        return []


def _get_place_suggestions(q: str) -> list:
    """Return up to 3 place name suggestions for a failed/misspelled query."""
    seen, out = set(), []

    def _add(name):
        if name and name.lower() not in seen:
            seen.add(name.lower())
            out.append(name)

    # Strategy 1: Nominatim with full query (no UK suffix — more lenient)
    for h in _nom_search(q):
        _add(h.get("display_name", "").split(",")[0].strip())
        if len(out) >= 3:
            return out

    # Strategy 2: first word + last word (e.g. "Bhakti manor" from "Bhakti vedantham manor")
    words = [w for w in q.split() if len(w) > 2]
    if len(words) >= 3:
        for h in _nom_search(f"{words[0]} {words[-1]}"):
            _add(h.get("display_name", "").split(",")[0].strip())
        if len(out) >= 3:
            return out

    # Strategy 3: Google Places (handles misspellings/phonetic variants)
    for p in _google_places_search(q):
        _add(p["name"])
        if len(out) >= 3:
            return out

    return out


def _geocode_place(q: str):
    """Return (lat, lon, display_name, osm_class, osm_type, osm_id) or None."""
    q = q.strip()
    # Postcode pattern
    if re.match(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}$', q.upper()):
        pc = q.replace(" ", "").upper()
        r = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=6)
        if r.status_code == 200:
            res = r.json().get("result", {})
            return res.get("latitude"), res.get("longitude"), res.get("admin_ward", q), "place", "postcode", None
        return None

    # Primary: Nominatim
    hits = _nom_search(q + ", UK", limit=1)
    if hits:
        h = hits[0]
        return (
            float(h["lat"]),
            float(h["lon"]),
            h.get("display_name", q).split(",")[0],
            h.get("class", ""),
            h.get("type", ""),
            str(h.get("osm_id", "")),
        )

    # Fallback: Google Places (handles misspellings, branded names, etc.)
    gp = _google_places_search(q, limit=1)
    if gp:
        p = gp[0]
        return float(p["lat"]), float(p["lon"]), p["name"], "amenity", "venue", ""

    return None


_SPECIFIC_VENUE_CLASSES = {"amenity", "tourism", "leisure", "religion", "shop", "sport", "historic"}
_AREA_TYPES = {"city", "town", "village", "suburb", "neighbourhood", "district", "county",
               "postcode", "municipality", "administrative", "region"}


def _is_specific_venue(osm_class: str, osm_type: str) -> bool:
    return osm_class in _SPECIFIC_VENUE_CLASSES or (osm_class == "place" and osm_type not in _AREA_TYPES)


_VENUE_CATEGORY = {
    "place_of_worship": {"label": "Place of Worship", "emoji": "🛕"},
    "museum":           {"label": "Museum",           "emoji": "🏛️"},
    "theatre":          {"label": "Theatre",          "emoji": "🎭"},
    "cinema":           {"label": "Cinema",           "emoji": "🎬"},
    "hotel":            {"label": "Hotel",            "emoji": "🏨"},
    "school":           {"label": "School",           "emoji": "🏫"},
    "college":          {"label": "College",          "emoji": "🎓"},
    "university":       {"label": "University",       "emoji": "🎓"},
    "stadium":          {"label": "Stadium",          "emoji": "🏟️"},
    "attraction":       {"label": "Attraction",       "emoji": "🌟"},
    "castle":           {"label": "Historic Site",    "emoji": "🏰"},
    "ruins":            {"label": "Historic Site",    "emoji": "🏰"},
    "restaurant":       {"label": "Restaurant",       "emoji": "🍽️"},
    "cafe":             {"label": "Café",             "emoji": "☕"},
    "pub":              {"label": "Pub",              "emoji": "🍺"},
    "supermarket":      {"label": "Supermarket",      "emoji": "🛒"},
}

_VENUE_ACTIVITIES = {
    "place_of_worship": ["Religious services & ceremonies", "Community events & gatherings",
                         "Visitor tours (check timings)", "Meditation & spiritual programmes",
                         "Cultural celebrations & festivals"],
    "museum":           ["Permanent & temporary exhibitions", "Guided tours", "School & group visits",
                         "Workshops & family activities", "Gift shop & café"],
    "theatre":          ["Live performances & shows", "Workshops & drama classes", "Community events",
                         "Children's productions", "Bar & interval service"],
    "school":           ["Primary / secondary education", "After-school clubs", "Parent consultations",
                         "Open days & events"],
    "university":       ["Degree & postgraduate courses", "Research & seminars", "Open lectures",
                         "Student union & societies", "Library & sports facilities"],
    "restaurant":       ["Dine-in meals", "Takeaway & delivery", "Private dining & group bookings",
                         "Special dietary menus"],
    "cafe":             ["Coffee, tea & light bites", "Breakfast & lunch", "Wi-Fi & relaxed seating",
                         "Cakes & pastries"],
}


def _fetch_specific_venue(osm_type: str, osm_id: str, display_name: str) -> dict | None:
    """Fetch full OSM tags for a specific element and return a formatted place dict."""
    import html as _html
    type_map = {"node": "node", "way": "way", "relation": "relation"}
    oql_type = type_map.get(osm_type)
    if not oql_type or not osm_id:
        return None
    query = f"[out:json];{oql_type}({osm_id});out tags;"
    elements = _overpass_query(query, timeout=15)
    if not elements:
        return None
    tags = elements[0].get("tags", {})

    name = tags.get("name", display_name)
    kind = (tags.get("amenity") or tags.get("tourism") or tags.get("leisure") or
            tags.get("historic") or "")
    cat  = _PLACE_CATEGORY.get(kind) or _VENUE_CATEGORY.get(kind) or {
        "label": kind.replace("_", " ").title() if kind else "Place", "emoji": "📍"
    }

    phone   = (tags.get("phone") or tags.get("contact:phone") or tags.get("contact:mobile") or "").strip()
    website = (tags.get("website") or tags.get("contact:website") or tags.get("url") or "").strip()
    hours   = _html.unescape(tags.get("opening_hours", "")).strip()

    parts   = [tags.get("addr:housenumber", ""), tags.get("addr:street", ""), tags.get("addr:city", "")]
    address = " ".join(p for p in parts if p).strip(", ") or ""

    description  = tags.get("description", "")
    denomination = tags.get("denomination", "")
    religion     = tags.get("religion", "")

    if description:
        summary = description[:240]
    else:
        type_label = cat["label"].lower()
        area = address.split(",")[-1].strip() if "," in address else ""
        summary = f"{name} is a {type_label}{' in ' + area if area else ''}."
        if denomination:
            summary += f" {denomination.title()} {religion}." if religion else f" {denomination.title()}."

    acts = _PLACE_ACTIVITIES.get(kind) or _VENUE_ACTIVITIES.get(kind, [])

    return {
        "name":           name,
        "kind":           kind,
        "category":       cat["label"],
        "emoji":          cat["emoji"],
        "address":        address,
        "phone":          phone,
        "website":        website,
        "hours":          hours,
        "hours_text":     [],
        "is_open":        None,
        "summary":        summary,
        "review_summary": "",
        "rating":         None,
        "rating_count":   0,
        "activities":     acts,
        "lat":            None,
        "lon":            None,
        "is_featured":    True,
    }


def _google_place_details(name: str, lat: float, lon: float) -> dict | None:
    """Fetch rich details for a named venue via Google Places Details API."""
    if not _GOOGLE_PLACES_KEY:
        return None
    try:
        ts = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": f"{name}", "key": _GOOGLE_PLACES_KEY, "region": "uk",
                    "location": f"{lat},{lon}", "radius": 500},
            timeout=8,
        )
        results = ts.json().get("results", [])
        if not results:
            return None
        place_id = results[0]["place_id"]
        gp_name  = results[0].get("name", name)

        det = requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={
                "place_id": place_id,
                "fields":   "name,formatted_address,formatted_phone_number,website,opening_hours,types,editorial_summary,rating,user_ratings_total,reviews",
                "key":      _GOOGLE_PLACES_KEY,
            },
            timeout=8,
        )
        p = det.json().get("result", {})

        hours_text   = p.get("opening_hours", {}).get("weekday_text", [])
        is_open      = p.get("opening_hours", {}).get("open_now")
        phone        = p.get("formatted_phone_number", "")
        website      = p.get("website", "")
        address      = p.get("formatted_address", "")
        summary      = (p.get("editorial_summary") or {}).get("overview", "")
        rating       = p.get("rating")
        rating_count = p.get("user_ratings_total", 0)
        raw_reviews  = [{"text": rv.get("text",""), "rating": rv.get("rating",0),
                          "author": rv.get("author_name","")}
                         for rv in p.get("reviews", []) if rv.get("text")]

        gp_types = p.get("types", [])
        # Map Google type → our category
        _GP_TYPE_MAP = {
            "hindu_temple": ("place_of_worship", "🛕", "Place of Worship"),
            "church": ("place_of_worship", "⛪", "Place of Worship"),
            "mosque": ("place_of_worship", "🕌", "Place of Worship"),
            "synagogue": ("place_of_worship", "🕍", "Place of Worship"),
            "place_of_worship": ("place_of_worship", "🛕", "Place of Worship"),
            "museum": ("museum", "🏛️", "Museum"),
            "library": ("library", "📚", "Library"),
            "school": ("school", "🏫", "School"),
            "university": ("university", "🎓", "University"),
            "park": ("park", "🌳", "Park"),
            "gym": ("fitness_centre", "🏋️", "Gym"),
            "restaurant": ("restaurant", "🍽️", "Restaurant"),
            "cafe": ("cafe", "☕", "Café"),
            "hospital": ("hospital", "🏥", "Hospital"),
            "pharmacy": ("pharmacy", "💊", "Pharmacy"),
            "tourist_attraction": ("attraction", "🌟", "Attraction"),
            "stadium": ("stadium", "🏟️", "Stadium"),
        }
        kind, emoji, cat_label = "place", "📍", "Place"
        for gt in gp_types:
            if gt in _GP_TYPE_MAP:
                kind, emoji, cat_label = _GP_TYPE_MAP[gt]
                break

        acts = _PLACE_ACTIVITIES.get(kind) or _VENUE_ACTIVITIES.get(kind, [])

        if not summary:
            area = address.split(",")[0] if address else ""
            summary = f"{gp_name} — {cat_label.lower()}{(' in ' + area) if area else ''}."

        # AI-enriched summary + review summary (non-blocking — skip on failure)
        ai = _groq_place_summary(gp_name, cat_label, address, summary, raw_reviews)

        return {
            "name":           gp_name,
            "kind":           kind,
            "category":       cat_label,
            "emoji":          emoji,
            "address":        address,
            "phone":          phone,
            "website":        website,
            "hours":          "",
            "hours_text":     hours_text,
            "is_open":        is_open,
            "summary":        ai.get("summary") or summary,
            "review_summary": ai.get("review_summary", ""),
            "rating":         rating,
            "rating_count":   rating_count,
            "activities":     acts,
            "lat":            lat,
            "lon":            lon,
            "is_featured":    True,
        }
    except Exception:
        return None


def _overpass_query(query: str, timeout: int = 30) -> list:
    """POST a query to Overpass and return elements list, or [] on any failure."""
    try:
        r = requests.post(
            "https://overpass-api.de/api/interpreter",
            data={"data": query},
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Miru/1.0; +https://miru.humanagency.co)"},
        )
        if r.status_code == 200:
            return r.json().get("elements", [])
        print(f"[overpass] HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[overpass] error: {e}")
    return []


def _overpass_places(lat: float, lon: float, radius: int = 1500):
    """Query Overpass for useful local services and return formatted list."""
    import html as _html
    services_types = "|".join([
        "library", "community_centre", "arts_centre", "hospital",
        "dentist", "doctors", "clinic", "pharmacy", "post_office",
        "townhall", "social_facility", "food_bank", "police",
        "fire_station", "leisure_centre", "veterinary", "bank",
    ])
    # Pubs/food at 5km — rural areas (e.g. Longcross) have pubs spread 3-5km out
    food_types = "cafe|restaurant|fast_food|pub|bar|fuel"
    food_radius = 5000
    leisure_types = "sports_centre|swimming_pool|fitness_centre|park|playground|attraction"
    # Commercial services: hairdressers, mechanics, beauty, opticians etc.
    shop_types = "|".join([
        "hairdresser", "beauty", "car_repair", "optician",
        "dry_cleaning", "laundry", "massage", "tattoo",
        "travel_agency", "estate_agent", "pet_grooming",
    ])
    query = f"""[out:json][timeout:25];
(
  node["amenity"~"^({services_types})$"](around:{radius},{lat},{lon});
  way["amenity"~"^({services_types})$"](around:{radius},{lat},{lon});
  node["amenity"~"^({food_types})$"](around:{food_radius},{lat},{lon});
  way["amenity"~"^({food_types})$"](around:{food_radius},{lat},{lon});
  node["leisure"~"^({leisure_types})$"](around:{radius},{lat},{lon});
  way["leisure"~"^({leisure_types})$"](around:{radius},{lat},{lon});
  node["shop"~"^({shop_types})$"](around:{radius},{lat},{lon});
  way["shop"~"^({shop_types})$"](around:{radius},{lat},{lon});
);
out center tags;"""
    elements = _overpass_query(query)
    # Retry with larger radius if still nothing found (very sparse areas)
    if not elements:
        query2 = query.replace(f"around:{radius}", f"around:{radius * 2}")
        elements = _overpass_query(query2)

    seen, results = set(), []
    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name", "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)

        kind = tags.get("amenity") or tags.get("leisure") or tags.get("shop", "")
        cat  = _PLACE_CATEGORY.get(kind, {"label": kind.replace("_", " ").title(), "emoji": "📍"})

        # Address
        parts = [tags.get("addr:housenumber", ""), tags.get("addr:street", ""),
                 tags.get("addr:city", "")]
        address = " ".join(p for p in parts if p).strip(", ") or ""

        # Phone
        phone = (tags.get("phone") or tags.get("contact:phone") or
                 tags.get("contact:mobile") or "").strip()

        # Website
        website = (tags.get("website") or tags.get("contact:website") or
                   tags.get("url") or "").strip()

        # Opening hours
        hours = _html.unescape(tags.get("opening_hours", "")).strip()

        # Brief summary
        type_label = cat["label"].lower()
        acts = _PLACE_ACTIVITIES.get(kind, [])
        act_preview = ", ".join(a.split("(")[0].strip().lower() for a in acts[:3])
        area = address.split(",")[-1].strip() if "," in address else ""
        summary = f"{name} is a {type_label}{' in ' + area if area else ''}."
        if act_preview:
            summary += f" Services include {act_preview}."

        results.append({
            "name":       name,
            "kind":       kind,
            "category":   cat["label"],
            "emoji":      cat["emoji"],
            "address":    address,
            "phone":      phone,
            "website":    website,
            "hours":      hours,
            "summary":    summary,
            "activities": acts,
            "lat":        el.get("lat") or el.get("center", {}).get("lat"),
            "lon":        el.get("lon") or el.get("center", {}).get("lon"),
        })

    # Sort by category then name
    results.sort(key=lambda x: (x["category"], x["name"]))
    return results


_PLACE_SUMMARY_CACHE: dict = {}
_PLACE_SUMMARY_TTL = 86400  # 24 hours — place descriptions rarely change

def _groq_place_summary(name: str, category: str, address: str,
                        description: str, reviews: list) -> dict:
    """Use Groq to write a place description and summarise reviews. Cached 24h."""
    cache_key = f"{name.lower().strip()}|{category}"
    cached = _PLACE_SUMMARY_CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < _PLACE_SUMMARY_TTL:
        return cached["data"]

    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return {}
    try:
        review_block = ""
        if reviews:
            snippets = [f'- "{r["text"][:150]}" ({r["rating"]}★)' for r in reviews[:3] if r.get("text")]
            review_block = "\nReviews:\n" + "\n".join(snippets)

        prompt = (
            f'Place: {name}\nType: {category}\nAddress: {address}\n'
            f'Description: {description or "(none)"}\n{review_block}\n\n'
            'Return ONLY valid JSON: {"summary":"2 sentence overview","review_summary":"1 sentence review summary or empty string"}'
        )
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant", "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 160},
            timeout=8,
        )
        if r.status_code == 200:
            content = r.json()["choices"][0]["message"]["content"].strip()
            m = re.search(r'\{.*\}', content, re.DOTALL)
            if m:
                result = json.loads(m.group(0))
                _PLACE_SUMMARY_CACHE[cache_key] = {"ts": time.time(), "data": result}
                return result
    except Exception:
        pass
    return {}


_PLACES_CACHE: dict = {}
_PLACES_CACHE_TTL = 3600  # 1 hour

_GOOGLE_PLACES_KEY = os.environ.get("GOOGLE_PLACES_KEY", "")


def _lookup_venue(name: str, lat: float = None, lon: float = None) -> dict:
    """Look up phone, website, address, hours, rating for a venue via Google Places."""
    out = {"phone": "", "website": "", "address": "", "maps_url": "",
           "hours_today": "", "open_now": None, "rating": None, "rating_count": 0, "name": name}
    if len(name) <= 60:
        maps_q = name.replace(" ", "+")
        out["maps_url"] = f"https://maps.google.com/?q={maps_q}"
    if not _GOOGLE_PLACES_KEY:
        return out
    try:
        params = {"query": name, "key": _GOOGLE_PLACES_KEY, "region": "uk"}
        if lat is not None and lon is not None:
            params["location"] = f"{lat},{lon}"
            params["radius"] = 5000  # 5km — strong local bias
        ts = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params=params,
            timeout=6,
        )
        results = ts.json().get("results", [])
        if not results:
            return out
        place_id = results[0]["place_id"]
        out["address"] = results[0].get("formatted_address", "")
        out["rating"]  = results[0].get("rating")
        out["rating_count"] = results[0].get("user_ratings_total", 0)
        loc = results[0].get("geometry", {}).get("location", {})
        if loc:
            out["maps_url"] = f"https://maps.google.com/?q={loc['lat']},{loc['lng']}"

        det = requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={
                "place_id": place_id,
                "fields":   "name,formatted_phone_number,website,formatted_address,opening_hours,rating,user_ratings_total",
                "key":      _GOOGLE_PLACES_KEY,
            },
            timeout=6,
        )
        p = det.json().get("result", {})
        out["name"]    = p.get("name", name)
        out["phone"]   = p.get("formatted_phone_number", "")
        out["website"] = p.get("website", "")
        if p.get("formatted_address"):
            out["address"] = p["formatted_address"]
        if p.get("rating"):
            out["rating"] = p["rating"]
            out["rating_count"] = p.get("user_ratings_total", 0)
        oh = p.get("opening_hours", {})
        out["open_now"] = oh.get("open_now")
        # Today's hours
        weekday_text = oh.get("weekday_text", [])
        if weekday_text:
            today_idx = __import__("datetime").datetime.now().weekday()
            # Google weekday_text starts Monday=0 in Python but Sunday=0 in Google
            google_idx = (today_idx + 1) % 7
            if google_idx < len(weekday_text):
                # Strip the day name prefix e.g. "Monday: 9:00 AM – 10:00 PM"
                parts = weekday_text[google_idx].split(": ", 1)
                out["hours_today"] = parts[1] if len(parts) > 1 else weekday_text[google_idx]
    except Exception as exc:
        print(f"[venue lookup] {exc}")
    return out


@app.route("/api/places/google")
def api_places_google():
    """Fetch Google Places rating + top reviews for a named venue."""
    name    = request.args.get("name", "").strip()
    lat     = request.args.get("lat", "")
    lon     = request.args.get("lon", "")
    if not name:
        return jsonify({"error": "name required"}), 400
    if not _GOOGLE_PLACES_KEY:
        return jsonify({"error": "Google Places API not configured"}), 503
    try:
        # Text Search to get place_id
        query = name
        if lat and lon:
            query += f" near {lat},{lon}"
        ts = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": query, "key": _GOOGLE_PLACES_KEY, "region": "uk"},
            timeout=8,
        )
        results = ts.json().get("results", [])
        if not results:
            return jsonify({"found": False})
        place_id = results[0]["place_id"]

        # Place Details
        det = requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={
                "place_id": place_id,
                "fields":   "name,rating,user_ratings_total,reviews,url,price_level,website",
                "key":      _GOOGLE_PLACES_KEY,
            },
            timeout=8,
        )
        p = det.json().get("result", {})
        reviews = [
            {
                "author":  r.get("author_name", ""),
                "rating":  r.get("rating", 0),
                "text":    r.get("text", "")[:280],
                "time":    r.get("relative_time_description", ""),
            }
            for r in p.get("reviews", [])[:3]
        ]
        return jsonify({
            "found":         True,
            "rating":        p.get("rating"),
            "total_ratings": p.get("user_ratings_total"),
            "google_url":    p.get("url"),
            "price_level":   p.get("price_level"),   # 0-4, None if unknown
            "website":       p.get("website", ""),
            "reviews":       reviews,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/places/price")
def api_places_price():
    """Extract price information from a business website using AI."""
    url      = request.args.get("url", "").strip()
    name     = request.args.get("name", "").strip()
    category = request.args.get("category", "").strip()
    if not url:
        return jsonify({"error": "url required"}), 400
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return jsonify({"error": "AI not configured"}), 503
    try:
        # Fetch website text
        page = requests.get(url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 (compatible; MiruBot/1.0; +https://miru.humanagency.co)"
        })
        import re as _re2
        text = _re2.sub(r'<[^>]+>', ' ', page.text)
        text = _re2.sub(r'\s+', ' ', text).strip()[:6000]
        # Ask Groq to extract prices
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [{"role": "user", "content":
                    f'Business: "{name}" ({category})\n'
                    f'Website text:\n{text}\n\n'
                    'Extract menu or price information. Examples: "Coffee from £3.20", "Lunch £12–£18", "Set menu £25pp", "Average main £15".\n'
                    'If no prices are listed, say "Prices not listed on website".\n'
                    'Return ONLY a short plain-text price summary, max 3 lines, no markdown.'}],
                "max_tokens": 150, "temperature": 0.1,
            },
            timeout=12,
        )
        price_text = r.json()["choices"][0]["message"]["content"].strip()
        return jsonify({"price_text": price_text})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/places/search")
def api_places_search():
    """Step 1: return candidate places for the user to pick from."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Enter a place name or postcode"}), 400

    _ICON_MAP = {
        ("amenity","place_of_worship"): "🛕",
        ("amenity","library"):          "📚",
        ("amenity","community_centre"): "🏘️",
        ("amenity","doctors"):          "🩺",
        ("amenity","hospital"):         "🏥",
        ("amenity","pharmacy"):         "💊",
        ("amenity","post_office"):      "📮",
        ("amenity","townhall"):         "🏛️",
        ("amenity","theatre"):          "🎭",
        ("amenity","cinema"):           "🎬",
        ("amenity","museum"):           "🏛️",
        ("leisure","sports_centre"):    "🏋️",
        ("leisure","swimming_pool"):    "🏊",
        ("leisure","park"):             "🌳",
        ("tourism","museum"):           "🏛️",
        ("tourism","attraction"):       "🌟",
        ("tourism","hotel"):            "🏨",
    }

    # Postcode
    if re.match(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}$', q.upper()):
        pc = q.replace(" ", "").upper()
        r = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=6)
        if r.status_code == 200:
            res = r.json().get("result", {})
            pc_display = pc[:-3] + " " + pc[-3:]
            ward = res.get("admin_ward", "")
            district = res.get("admin_district", "")
            subtitle = ", ".join(p for p in [ward, district] if p)
            return jsonify({"candidates": [{
                "name":      pc_display,
                "subtitle":  subtitle,
                "icon":      "📮",
                "type":      "postcode",
                "lat":       res.get("latitude"),
                "lon":       res.get("longitude"),
                "osm_type":  "",
                "osm_id":    "",
                "osm_class": "place",
            }], "suggestions": []})
        # Bad postcode — try to normalise
        q_up = q.upper().replace(" ", "")
        if re.match(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\d[A-Z]{2}$', q_up):
            norm = q_up[:-3] + " " + q_up[-3:]
            return jsonify({"candidates": [], "suggestions": [norm]})
        return jsonify({"candidates": [], "suggestions": []})

    # Place name — Nominatim + Google Places in parallel
    seen, candidates = set(), []

    def _add_nom_hits(hits):
        for h in hits:
            parts  = [p.strip() for p in h.get("display_name","").split(",")]
            name   = parts[0]
            if not name or name.lower() in seen:
                continue
            seen.add(name.lower())
            subtitle = ", ".join(parts[1:3])
            cls  = h.get("class", "")
            typ  = h.get("type", "")
            icon = _ICON_MAP.get((cls, typ), "🏛️" if cls in ("amenity","tourism","leisure") else "📍")
            candidates.append({
                "name":      name,
                "subtitle":  subtitle,
                "icon":      icon,
                "type":      cls,
                "lat":       float(h["lat"]),
                "lon":       float(h["lon"]),
                "osm_type":  h.get("osm_type",""),
                "osm_id":    str(h.get("osm_id","")),
                "osm_class": cls,
            })

    def _add_google_hits(gp_hits):
        for p in gp_hits:
            if p["name"].lower() not in seen:
                seen.add(p["name"].lower())
                candidates.append({
                    "name":      p["name"],
                    "subtitle":  p.get("subtitle", ""),
                    "icon":      "📍",
                    "type":      "venue",
                    "lat":       p["lat"],
                    "lon":       p["lon"],
                    "osm_type":  "",
                    "osm_id":    "",
                    "osm_class": "amenity",
                })

    # Fire Nominatim and Google Places in parallel
    words = [w for w in q.split() if len(w) > 2]
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_nom = ex.submit(_nom_search, q + ", UK", 6)
        f_gp  = ex.submit(_google_places_search, q, 4)
        _add_nom_hits(f_nom.result())
        gp_hits = f_gp.result()

    # If Nominatim found nothing, try word-pair sub-queries
    if not candidates:
        if len(words) >= 3:
            _add_nom_hits(_nom_search(f"{words[-2]} {words[-1]}, UK", limit=4))
    if not candidates:
        if len(words) >= 3:
            _add_nom_hits(_nom_search(f"{words[0]} {words[-1]}, UK", limit=4))

    # Merge Google Places results (always, not just fallback)
    _add_google_hits(gp_hits)

    if not candidates:
        return jsonify({"candidates": [], "suggestions": _get_place_suggestions(q)})

    return jsonify({"candidates": candidates, "suggestions": []})


@app.route("/api/debug/places-search")
def api_debug_places_search():
    """Debug: show raw Nominatim + Google Places results for a query."""
    q = request.args.get("q", "bhakti vedantham manor").strip()
    nom = _nom_search(q + ", UK", limit=6)
    words = [w for w in q.split() if len(w) > 2]
    nom2, nom3 = [], []
    if len(words) >= 3:
        nom2 = _nom_search(f"{words[-2]} {words[-1]}, UK", limit=4)
        nom3 = _nom_search(f"{words[0]} {words[-1]}, UK", limit=4)

    # Raw Google Places response for debugging
    gp_raw, gp_error = {}, ""
    if _GOOGLE_PLACES_KEY:
        try:
            r = requests.get(
                "https://maps.googleapis.com/maps/api/place/textsearch/json",
                params={"query": q + " UK", "key": _GOOGLE_PLACES_KEY, "region": "uk"},
                timeout=8,
            )
            gp_raw = r.json()
        except Exception as e:
            gp_error = str(e)
    gp = _google_places_search(q, limit=4)

    return jsonify({
        "query": q,
        "google_key_set": bool(_GOOGLE_PLACES_KEY),
        "google_status": gp_raw.get("status", ""),
        "google_error_message": gp_raw.get("error_message", gp_error),
        "google_results_count": len(gp_raw.get("results", [])),
        "nominatim_full": [h.get("display_name","") for h in nom],
        "nominatim_last2": [h.get("display_name","") for h in nom2],
        "nominatim_first_last": [h.get("display_name","") for h in nom3],
        "google_places": gp,
    })


@app.route("/api/places")
def api_places():
    # Accepts either lat/lon directly (from candidate selection) or q= for legacy
    lat_p = request.args.get("lat", "")
    lon_p = request.args.get("lon", "")
    q     = request.args.get("q", "").strip()

    if lat_p and lon_p:
        try:
            lat, lon = float(lat_p), float(lon_p)
        except ValueError:
            return jsonify({"error": "Invalid coordinates"}), 400
        display   = request.args.get("name", "") or q or f"{lat:.3f},{lon:.3f}"
        osm_class = request.args.get("osm_class", "")
        osm_type  = request.args.get("osm_type", "")
        osm_id    = request.args.get("osm_id", "")
        cache_key = f"places:{lat:.4f},{lon:.4f}:{osm_id}"
        cached = _PLACES_CACHE.get(cache_key)
        if cached and (time.time() - cached[0]) < _PLACES_CACHE_TTL:
            return jsonify(cached[1])
    else:
        if not q:
            return jsonify({"error": "Enter a place name or postcode"}), 400
        cache_key = f"places:{q.lower()}"
        cached = _PLACES_CACHE.get(cache_key)
        if cached and (time.time() - cached[0]) < _PLACES_CACHE_TTL:
            return jsonify(cached[1])
        geo = _geocode_place(q)
        if not geo or geo[0] is None:
            q_up = q.upper().replace(" ", "")
            if re.match(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\d[A-Z]{2}$', q_up):
                norm = q_up[:-3] + " " + q_up[-3:]
                return jsonify({"error": f"Couldn't find '{q}'.", "suggestions": [norm]}), 404
            suggestions = _get_place_suggestions(q)
            return jsonify({"error": f"Couldn't find '{q}'. Try a postcode or town name.",
                            "suggestions": suggestions}), 404
        lat, lon, display, osm_class, osm_type, osm_id = geo

    # Run area search + venue detail fetch in parallel
    is_venue  = _is_specific_venue(osm_class, osm_type) and bool(osm_id)
    # Only call Google Place Details when a real name was supplied (not a fallback coord string)
    explicit_name = request.args.get("name", "").strip() or q
    needs_gp  = not is_venue and bool(explicit_name) and bool(_GOOGLE_PLACES_KEY)
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_places = ex.submit(_overpass_places, lat, lon)
        f_venue  = ex.submit(_fetch_specific_venue, osm_type, osm_id, display) if is_venue else None
        f_gpd    = ex.submit(_google_place_details, display, lat, lon) if needs_gp else None
        places   = f_places.result()
        featured = f_venue.result() if f_venue else None
        if not featured and f_gpd:
            featured = f_gpd.result()

    if featured:
        places = [p for p in places if p["name"].lower() != featured["name"].lower()]

    all_places = ([featured] if featured else []) + places
    if not all_places:
        return jsonify({"error": "No local services found nearby. Try a different location."}), 404

    result = {"location": display, "lat": lat, "lon": lon, "count": len(all_places), "places": all_places}
    _PLACES_CACHE[cache_key] = (time.time(), result)
    return jsonify(result)


def _kids_venues_gplaces(lat: float, lon: float) -> list:
    """Google Places search for kids activity venues (soft play, trampoline parks, etc.)."""
    if not _GOOGLE_PLACES_KEY:
        return []
    queries = [
        "soft play centre", "trampoline park", "children's activity centre",
        "adventure playground", "children's farm", "mini golf",
        "bowling alley", "children's museum", "indoor play centre",
        "family entertainment centre", "children's theatre", "kids club",
    ]
    results, seen = [], set()
    from concurrent.futures import ThreadPoolExecutor as _TPE
    with _TPE(max_workers=8) as ex:
        futs = [ex.submit(_gplaces_text_search, q, lat, lon, 15000) for q in queries]
        for f in futs:
            _gplaces_collect(f.result(), lat, lon, seen, results)
    results.sort(key=lambda x: x["dist_km"])
    top = results[:30]
    if top:
        with _TPE(max_workers=8) as ex:
            details = list(ex.map(_gplaces_details, [p["_place_id"] for p in top]))
        for item, det in zip(top, details):
            if det.get("phone"):        item["phone"]        = det["phone"]
            if det.get("website"):      item["website"]      = det["website"]
            if det.get("hours_detail"): item["hours_detail"] = det["hours_detail"]
    return top


_SKIDDLE_KEY = os.environ.get("SKIDDLE_API_KEY", "")


def _kids_events_skiddle(lat: float, lon: float) -> list:
    """Fetch upcoming kids/family events from Skiddle (requires SKIDDLE_API_KEY)."""
    if not _SKIDDLE_KEY:
        return []
    try:
        r = requests.get(
            "https://www.skiddle.com/api/v1/events/search/",
            params={
                "api_key":        _SKIDDLE_KEY,
                "latitude":       lat,
                "longitude":      lon,
                "radius":         10,           # miles
                "eventcode":      "KIDS",
                "limit":          20,
                "order":          "distance",
                "ticketsavailable": 1,
                "getdistance":    1,
            },
            timeout=8,
        )
        events = []
        for e in r.json().get("results", []):
            events.append({
                "name":      e.get("eventname", ""),
                "date":      e.get("startdate", ""),
                "url":       e.get("link", ""),
                "venue":     e.get("venue", {}).get("name", ""),
                "address":   e.get("venue", {}).get("address", ""),
                "town":      e.get("venue", {}).get("town", ""),
                "image":     e.get("imageurl", ""),
                "dist_miles":e.get("distance", ""),
                "free":      e.get("entryprice") in ("0.00", "0", None, ""),
                "price":     e.get("entryprice", ""),
            })
        return [e for e in events if e["name"]]
    except Exception:
        return []


def _kids_events_search_url(lat: float, lon: float) -> str:
    """Return a Skiddle/Eventbrite website search URL for kids events near the given location."""
    try:
        r = requests.get(
            "https://api.postcodes.io/postcodes",
            params={"lon": lon, "lat": lat, "limit": 1},
            timeout=4,
        )
        pc = (r.json().get("result") or [{}])[0].get("admin_district", "")
        area = pc.replace(" ", "-").lower() if pc else ""
    except Exception:
        area = ""
    if area:
        return f"https://www.skiddle.com/whats-on/{area.replace('-',' ').title()}/Kids-Family/"
    return "https://www.skiddle.com/whats-on/Kids-Family/"


@app.route("/api/kids-activities")
def api_kids_activities():
    lat_p = request.args.get("lat", "")
    lon_p = request.args.get("lon", "")
    if not lat_p or not lon_p:
        return jsonify({"error": "lat/lon required"}), 400
    try:
        lat, lon = float(lat_p), float(lon_p)
    except ValueError:
        return jsonify({"error": "Invalid coordinates"}), 400
    cache_key = f"kids1:{lat:.4f},{lon:.4f}"
    cached = _PLACES_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < _PLACES_CACHE_TTL:
        return jsonify(cached[1])
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as ex:
        venues_f = ex.submit(_kids_venues_gplaces, lat, lon)
        events_f = ex.submit(_kids_events_skiddle, lat, lon)
        url_f    = ex.submit(_kids_events_search_url, lat, lon)
        venues     = venues_f.result()
        events     = events_f.result()
        events_url = url_f.result()
    result = {"venues": venues, "events": events, "events_url": events_url}
    _PLACES_CACHE[cache_key] = (time.time(), result)
    return jsonify(result)


def _finder_nearby(lat: float, lon: float, radius: int = 10000) -> dict:
    query = f"""[out:json][timeout:20];
(
  node["amenity"="coworking_space"](around:{radius},{lat},{lon});
  way["amenity"="coworking_space"](around:{radius},{lat},{lon});
  node["office"="coworking"](around:{radius},{lat},{lon});
  way["office"="coworking"](around:{radius},{lat},{lon});
  node["office"="coworking_space"](around:{radius},{lat},{lon});
  way["office"="coworking_space"](around:{radius},{lat},{lon});
  node["amenity"="childcare"](around:{radius},{lat},{lon});
  way["amenity"="childcare"](around:{radius},{lat},{lon});
  node["amenity"="nursery"](around:{radius},{lat},{lon});
  way["amenity"="nursery"](around:{radius},{lat},{lon});
  node["amenity"="kindergarten"](around:{radius},{lat},{lon});
  way["amenity"="kindergarten"](around:{radius},{lat},{lon});
  node["social_facility"="childcare"](around:{radius},{lat},{lon});
  way["social_facility"="childcare"](around:{radius},{lat},{lon});
);
out center tags;"""
    elements = _overpass_query(query)
    coworking, childcare = [], []
    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name", "").strip()
        if not name:
            continue
        if el.get("type") == "way":
            c = el.get("center", {})
            elat, elon = c.get("lat"), c.get("lon")
        else:
            elat, elon = el.get("lat"), el.get("lon")
        if elat is None or elon is None:
            continue
        dist_km = haversine_km(lat, lon, elat, elon)
        parts = [tags.get("addr:housenumber",""), tags.get("addr:street",""), tags.get("addr:city","")]
        address = " ".join(p for p in parts if p).strip(", ")
        amenity = tags.get("amenity","")
        office  = tags.get("office","")
        entry = {
            "name": name, "address": address, "lat": elat, "lon": elon,
            "dist_km": round(dist_km, 2),
            "website": tags.get("website", tags.get("contact:website", "")),
            "phone":   tags.get("phone", tags.get("contact:phone", "")),
            "hours":   tags.get("opening_hours", ""),
        }
        if "coworking" in amenity or "coworking" in office:
            entry["wifi"] = tags.get("internet_access") in ("wlan","wifi","yes")
            coworking.append(entry)
        else:
            # Enrich childcare entries with extra OSM fields
            min_age  = tags.get("min_age","")
            max_age  = tags.get("max_age","")
            capacity = tags.get("capacity","")
            cc_type  = ("childminder" if "childminder" in name.lower()
                        else "nursery"    if amenity in ("nursery","childcare") or "nursery" in name.lower()
                        else "pre-school" if "preschool" in name.lower() or "pre-school" in name.lower()
                        else "kindergarten" if amenity == "kindergarten"
                        else "childcare")
            if min_age or max_age:
                entry["age_range"] = f"{min_age or '0'}–{max_age or '?'} yrs"
            if capacity:
                entry["capacity"] = capacity
            entry["cc_type"] = cc_type
            childcare.append(entry)
    coworking.sort(key=lambda x: x["dist_km"])
    childcare.sort(key=lambda x: x["dist_km"])
    return {"coworking": coworking[:50], "childcare": childcare[:50]}


def _gplaces_details(place_id: str) -> dict:
    """Fetch phone, website, opening hours and open_now for a Google place_id."""
    if not _GOOGLE_PLACES_KEY or not place_id:
        return {}
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={"place_id": place_id,
                    "fields": "formatted_phone_number,website,opening_hours",
                    "key": _GOOGLE_PLACES_KEY},
            timeout=5,
        )
        res = r.json().get("result", {})
        oh = res.get("opening_hours", {})
        return {
            "phone":        res.get("formatted_phone_number", ""),
            "website":      res.get("website", ""),
            "hours_detail": oh.get("weekday_text", []),
            "open_now":     oh.get("open_now"),
        }
    except Exception:
        return {}


def _gplaces_text_search(query: str, lat: float, lon: float, radius: int) -> list:
    """Run a single Google Places Text Search and return raw result list."""
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": query, "location": f"{lat},{lon}",
                    "radius": radius, "key": _GOOGLE_PLACES_KEY, "region": "uk"},
            timeout=8,
        )
        return r.json().get("results", [])
    except Exception:
        return []


def _gplaces_nearby_search(keyword: str, lat: float, lon: float, radius: int) -> list:
    """Run a Google Places Nearby Search and return raw result list."""
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params={"keyword": keyword, "location": f"{lat},{lon}",
                    "radius": radius, "key": _GOOGLE_PLACES_KEY},
            timeout=8,
        )
        return r.json().get("results", [])
    except Exception:
        return []


def _gplaces_collect(raw_list: list, lat: float, lon: float, seen: set, results: list):
    """Append unique places from a raw Places API result list."""
    for p in raw_list:
        name = p.get("name", "")
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        loc = p.get("geometry", {}).get("location", {})
        plat, plon = loc.get("lat"), loc.get("lng")
        dist_km = haversine_km(lat, lon, plat, plon) if plat and plon else 999
        addr = p.get("formatted_address") or p.get("vicinity") or ""
        results.append({
            "name": name, "address": addr,
            "lat": plat, "lon": plon, "dist_km": round(dist_km, 2),
            "rating": p.get("rating"), "review_count": p.get("user_ratings_total", 0),
            "price_level": p.get("price_level"),
            "open_now": p.get("opening_hours", {}).get("open_now"),
            "phone": "", "website": "", "hours_detail": [],
            "_place_id": p.get("place_id", ""),
        })


def _finder_cowork_places(lat: float, lon: float) -> list:
    """Google Places search for co-working spaces — text + nearby to maximise results."""
    if not _GOOGLE_PLACES_KEY:
        return []
    results, seen = [], set()
    from concurrent.futures import ThreadPoolExecutor as _TPE
    text_queries  = ["coworking space", "shared office space", "serviced office", "hot desk", "business centre", "flexible workspace"]
    nearby_kws    = ["coworking", "shared office", "serviced offices"]
    radius        = 25000
    with _TPE(max_workers=8) as ex:
        text_futs   = [ex.submit(_gplaces_text_search,   q, lat, lon, radius) for q in text_queries]
        nearby_futs = [ex.submit(_gplaces_nearby_search, k, lat, lon, radius) for k in nearby_kws]
        for f in text_futs + nearby_futs:
            _gplaces_collect(f.result(), lat, lon, seen, results)
    results.sort(key=lambda x: x["dist_km"])
    top = results[:60]
    if top:
        with _TPE(max_workers=8) as ex:
            details = list(ex.map(_gplaces_details, [p["_place_id"] for p in top]))
        for item, det in zip(top, details):
            if det.get("phone"):        item["phone"]        = det["phone"]
            if det.get("website"):      item["website"]      = det["website"]
            if det.get("hours_detail"): item["hours_detail"] = det["hours_detail"]
    return top


def _finder_nanny_search(lat: float, lon: float) -> list:
    """Google Places search for nanny/au pair agencies only — no nurseries."""
    if not _GOOGLE_PLACES_KEY:
        return []
    results, seen = [], set()
    from concurrent.futures import ThreadPoolExecutor as _TPE
    text_queries = ["nanny agency", "au pair agency", "childminder agency", "babysitter agency",
                    "nanny service", "au pair service"]
    nearby_kws   = ["nanny agency", "au pair agency"]
    radius       = 30000
    with _TPE(max_workers=6) as ex:
        text_futs   = [ex.submit(_gplaces_text_search,   q, lat, lon, radius) for q in text_queries]
        nearby_futs = [ex.submit(_gplaces_nearby_search, k, lat, lon, radius) for k in nearby_kws]
        for f in text_futs + nearby_futs:
            _gplaces_collect(f.result(), lat, lon, seen, results)
    results.sort(key=lambda x: x["dist_km"])
    top = results[:60]
    if top:
        with _TPE(max_workers=8) as ex:
            details = list(ex.map(_gplaces_details, [p["_place_id"] for p in top]))
        for item, det in zip(top, details):
            if det.get("phone"):        item["phone"]        = det["phone"]
            if det.get("website"):      item["website"]      = det["website"]
            if det.get("hours_detail"): item["hours_detail"] = det["hours_detail"]
    return top


def _finder_nursery_places(lat: float, lon: float) -> list:
    """Google Places search for nurseries and childcare centres."""
    if not _GOOGLE_PLACES_KEY:
        return []
    results, seen = [], set()
    from concurrent.futures import ThreadPoolExecutor as _TPE
    text_queries = ["nursery", "day nursery", "childcare centre", "childcare nursery",
                    "preschool", "early years nursery"]
    nearby_kws   = ["nursery", "childcare", "preschool"]
    radius       = 15000
    with _TPE(max_workers=6) as ex:
        text_futs   = [ex.submit(_gplaces_text_search,   q, lat, lon, radius) for q in text_queries]
        nearby_futs = [ex.submit(_gplaces_nearby_search, k, lat, lon, radius) for k in nearby_kws]
        for f in text_futs + nearby_futs:
            _gplaces_collect(f.result(), lat, lon, seen, results)
    results.sort(key=lambda x: x["dist_km"])
    top = results[:60]
    if top:
        with _TPE(max_workers=8) as ex:
            details = list(ex.map(_gplaces_details, [p["_place_id"] for p in top]))
        for item, det in zip(top, details):
            if det.get("phone"):        item["phone"]        = det["phone"]
            if det.get("website"):      item["website"]      = det["website"]
            if det.get("hours_detail"): item["hours_detail"] = det["hours_detail"]
    return top


@app.route("/api/finder")
def api_finder():
    lat_p = request.args.get("lat","")
    lon_p = request.args.get("lon","")
    if not lat_p or not lon_p:
        return jsonify({"error": "lat/lon required"}), 400
    try:
        lat, lon = float(lat_p), float(lon_p)
    except ValueError:
        return jsonify({"error": "Invalid coordinates"}), 400
    cache_key = f"finder7:{lat:.4f},{lon:.4f}"
    cached = _PLACES_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < _PLACES_CACHE_TTL:
        return jsonify(cached[1])
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as ex:
        osm_f     = ex.submit(_finder_nearby, lat, lon)
        nanny_f   = ex.submit(_finder_nanny_search, lat, lon)
        cowork_f  = ex.submit(_finder_cowork_places, lat, lon)
        nursery_f = ex.submit(_finder_nursery_places, lat, lon)
        osm        = osm_f.result()
        nannies    = nanny_f.result()
        gp_cowork  = cowork_f.result()
        gp_nursery = nursery_f.result()
    # Merge OSM + Google Places co-working, deduplicate by name
    osm_cowork = osm.get("coworking", [])
    seen_cw = {p["name"].lower() for p in osm_cowork}
    for p in gp_cowork:
        if p["name"].lower() not in seen_cw:
            osm_cowork.append(p)
            seen_cw.add(p["name"].lower())
    osm_cowork.sort(key=lambda x: x["dist_km"])
    # Merge OSM + Google Places nurseries/childcare, deduplicate by name
    osm_childcare = osm.get("childcare", [])
    seen_cc = {p["name"].lower() for p in osm_childcare}
    for p in gp_nursery:
        if p["name"].lower() not in seen_cc:
            osm_childcare.append(p)
            seen_cc.add(p["name"].lower())
    osm_childcare.sort(key=lambda x: x["dist_km"])
    result = {
        "coworking":      osm_cowork[:60],
        "childcare":      osm_childcare[:60],
        "nanny_agencies": nannies,
        "platforms": {
            "nanny":  [
                {"name": "Childcare.co.uk",  "url": "https://www.childcare.co.uk/"},
                {"name": "Nannyjob.co.uk",   "url": "https://www.nannyjob.co.uk/"},
                {"name": "Care.com UK",      "url": "https://www.care.com/"},
                {"name": "Gumtree Nannies",  "url": "https://www.gumtree.com/jobs/nanny"},
            ],
            "aupair": [
                {"name": "Au Pair World",    "url": "https://www.aupairworld.com/"},
                {"name": "AuPair.com",       "url": "https://www.aupair.com/"},
                {"name": "Great Au Pair",    "url": "https://www.greataupair.com/"},
                {"name": "Cultural Care",    "url": "https://www.culturalcare.co.uk/"},
            ],
        },
    }
    _PLACES_CACHE[cache_key] = (time.time(), result)
    return jsonify(result)


_TRADE_CHECKATRADE = {
    "plumber": "Plumber", "plumbing": "Plumber",
    "electrician": "Electrician", "electrical": "Electrician",
    "gas engineer": "Gas-Engineer", "boiler": "Boiler-Repair",
    "builder": "Builder", "roofer": "Roofer", "roofing": "Roofer",
    "painter": "Painter-And-Decorator", "decorator": "Painter-And-Decorator",
    "locksmith": "Locksmith", "handyman": "Handyman",
    "carpenter": "Carpenter", "tiler": "Tiler", "plasterer": "Plasterer",
    "cleaner": "Cleaning-Services", "cleaning": "Cleaning-Services",
    "window cleaner": "Window-Cleaner", "pest control": "Pest-Control",
    "gardener": "Landscaper", "landscap": "Landscaper",
    "driveway": "Driveways", "bathroom": "Bathroom-Fitter",
    "kitchen": "Kitchen-Fitter", "extension": "House-Extension",
}


@app.route("/api/finder/search")
def api_finder_search():
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lon", ""))
    except (ValueError, TypeError):
        return jsonify({"error": "lat/lon required"}), 400
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "query required"}), 400
    open_now_filter = request.args.get("open_now") == "1"

    cache_key = f"fsearch2:{lat:.3f},{lon:.3f}:{q.lower()[:50]}"
    cached = _PLACES_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < _PLACES_CACHE_TTL:
        data = cached[1]
        if open_now_filter:
            data = {**data, "results": [p for p in data["results"] if p.get("open_now") is True]}
        return jsonify(data)

    raw = _gplaces_text_search(q, lat, lon, radius=15000)
    results, seen = [], set()
    _gplaces_collect(raw, lat, lon, seen, results)
    results.sort(key=lambda p: p.get("dist_km", 999))
    top = results[:15]

    if top:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futs = {ex.submit(_gplaces_details, p["_place_id"]): i
                    for i, p in enumerate(top) if p.get("_place_id")}
            for f, i in futs.items():
                try:
                    top[i].update(f.result(timeout=8))
                except Exception:
                    pass
    for p in top:
        p.pop("_place_id", None)

    checkatrade_url = None
    q_lower = q.lower()
    for kw, slug in _TRADE_CHECKATRADE.items():
        if kw in q_lower:
            # Resolve nearest town for location-specific URL
            # admin_ward gives actual town names (e.g. "Weybridge", "Wimbledon")
            # admin_district gives borough names (e.g. "Runnymede") which Checkatrade doesn't recognise
            town_slug = None
            try:
                import requests as _req
                pc_r = _req.get(
                    f"https://api.postcodes.io/postcodes?lon={lon}&lat={lat}&limit=1",
                    timeout=3
                ).json()
                if pc_r.get("result"):
                    res = pc_r["result"][0]
                    district = (res.get("admin_ward")
                                or res.get("admin_district")
                                or res.get("region") or "")
                    if district:
                        town_slug = district.title().replace(" ", "-")
            except Exception:
                pass
            if town_slug:
                checkatrade_url = f"https://www.checkatrade.com/Search/{slug}/in/{town_slug}"
            else:
                checkatrade_url = f"https://www.checkatrade.com/Search/{slug}"
            break

    result = {"results": top, "checkatrade_url": checkatrade_url, "query": q}
    _PLACES_CACHE[cache_key] = (time.time(), result)
    if open_now_filter:
        result = {**result, "results": [p for p in top if p.get("open_now") is True]}
    return jsonify(result)


@app.route("/api/scan-barcode", methods=["POST"])
def api_scan_barcode():
    """Decode a barcode from an uploaded image using pyzbar (server-side, works on all devices)."""
    try:
        from pyzbar import pyzbar as _pyzbar
        from PIL import Image
        import io
    except ImportError as e:
        return jsonify({"error": f"Server decode unavailable: {e}"}), 503

    f = request.files.get("image")
    if not f:
        return jsonify({"error": "No image uploaded"}), 400
    try:
        from PIL import ImageOps
        raw = f.read()
        img = Image.open(io.BytesIO(raw))
        img = ImageOps.exif_transpose(img)  # respect phone EXIF rotation
        img = img.convert("RGB")
        results = _pyzbar.decode(img)
        if results:
            return jsonify({"barcode": results[0].data.decode("utf-8"), "format": results[0].type})
        # Retry with greyscale + contrast enhance (helps with low-light photos)
        from PIL import ImageEnhance
        grey = img.convert("L")
        enhanced = ImageEnhance.Contrast(grey).enhance(2.0)
        results2 = _pyzbar.decode(enhanced.convert("RGB"))
        if results2:
            return jsonify({"barcode": results2[0].data.decode("utf-8"), "format": results2[0].type})
        return jsonify({"error": "No barcode found in image"})
    except Exception as e:
        print(f"[scan-barcode] {e}")
        return jsonify({"error": "Could not process image"}), 500


@app.route("/api/product")
def api_product():
    import requests as _req, json as _json
    barcode = request.args.get("barcode", "").strip()
    name    = request.args.get("name", "").strip()
    debug   = request.args.get("debug") == "1"

    product = None
    _errors = []
    groq_key = os.environ.get("GROQ_API_KEY", "")

    def _groq(prompt, max_tokens=500):
        r = _req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": max_tokens, "temperature": 0.3},
            timeout=15,
        )
        d = r.json()
        if "choices" not in d:
            raise RuntimeError(f"Groq error: {d.get('error', d)}")
        return d["choices"][0]["message"]["content"].strip()

    def _extract_obj(text):
        s, e = text.find("{"), text.rfind("}")
        return _json.loads(text[s:e+1]) if s != -1 and e != -1 else {}

    def _extract_arr(text):
        s, e = text.find("["), text.rfind("]")
        return _json.loads(text[s:e+1]) if s != -1 and e != -1 else []

    # --- 1. Open Food Facts (best UK coverage, try first) ---
    if barcode:
        for off_url in [
            f"https://world.openfoodfacts.org/api/v2/product/{barcode}.json",
            f"https://world.openfoodfacts.net/api/v2/product/{barcode}.json",
        ]:
            try:
                r = _req.get(off_url, timeout=6,
                             headers={"User-Agent": "MiruApp/1.0 (+https://miru.humanagency.co)", "Accept": "application/json"})
                if r.status_code == 200:
                    d = r.json()
                    if d.get("status") == 1:
                        p = d["product"]
                        cats = p.get("categories", "")
                        product = {
                            "name":     p.get("product_name_en") or p.get("product_name", ""),
                            "brand":    p.get("brands", "").split(",")[0].strip(),
                            "category": cats.split(",")[0].strip() if cats else "",
                            "image":    p.get("image_url", ""),
                            "barcode":  barcode,
                        }
                        _errors.append(f"product via OFF ({off_url.split('/')[2]})")
                        break
                else:
                    _errors.append(f"OFF http={r.status_code}")
            except Exception as e:
                _errors.append(f"OFF exc: {e}")
            if product:
                break

    # --- 2. Groq barcode identification (works for popular UK products) ---
    if not product and barcode and groq_key:
        try:
            raw = _groq(
                f'A UK product has barcode/EAN: {barcode}.\n'
                'If you know this product, give its exact name and brand. '
                'If unsure, reply with "unknown".\n'
                'Return ONLY JSON: {{"name":"...","brand":"...","category":"...","known":true/false}}',
                max_tokens=120,
            )
            obj = _extract_obj(raw)
            if obj.get("known") and obj.get("name") and obj["name"].lower() != "unknown":
                product = {"name": obj["name"], "brand": obj.get("brand", ""),
                           "category": obj.get("category", ""), "image": "", "barcode": barcode}
                _errors.append("product via groq barcode")
        except Exception as e:
            _errors.append(f"groq barcode exc: {e}")

    # --- 3. Name search via Groq AI ---
    if not product and name and groq_key:
        try:
            raw = _groq(
                f'UK grocery product: "{name}". Give the exact product name, brand, and category.\n'
                'Return ONLY JSON: {{"name":"...","brand":"...","category":"..."}}',
                max_tokens=120,
            )
            obj = _extract_obj(raw)
            if obj.get("name"):
                product = {"name": obj["name"], "brand": obj.get("brand", ""),
                           "category": obj.get("category", ""), "image": ""}
                _errors.append("product via groq name")
        except Exception as e:
            _errors.append(f"groq name exc: {e}")

    # If still no product and only have a barcode, don't guess prices
    search_term = (product["name"] if product else name) or None
    if not search_term:
        return jsonify({"product": None, "alternatives": [], "prices": {},
                        "query": barcode, "search_q": barcode,
                        "_errors": _errors if debug else None,
                        "not_found": True})
    brand_ctx   = f" by {product['brand']}" if product and product.get("brand") else ""
    full_name   = f"{search_term}{brand_ctx}"

    # --- 4. Real prices: Tesco + Waitrose APIs in parallel ---
    from concurrent.futures import ThreadPoolExecutor, as_completed
    prices = {}

    def _tesco_price(q):
        try:
            r = _req.get(
                "https://api.tesco.com/shoppingexperience/v1/api/products/search",
                params={"query": q, "count": "3", "offset": "0"},
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=6,
            )
            if r.status_code == 200:
                items = r.json().get("uk", {}).get("ghs", {}).get("products", {}).get("results", [])
                if items:
                    p = items[0].get("price") or items[0].get("unitPrice")
                    if p:
                        return ("tesco", f"£{float(p):.2f}")
        except Exception as e:
            _errors.append(f"tesco exc: {e}")
        return ("tesco", None)

    def _waitrose_price(q):
        try:
            r = _req.get(
                "https://www.waitrose.com/api/content-prod/v2/cms/page/products",
                params={"q": q, "start": "0", "size": "3", "sortBy": "RELEVANCE", "searchSource": "search"},
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json",
                         "Referer": "https://www.waitrose.com/"},
                timeout=6,
            )
            if r.status_code == 200:
                results = r.json().get("componentsAndProducts", [])
                for item in results:
                    prod = item.get("searchProduct") or item.get("product") or {}
                    price = prod.get("currentSaleUnitPrice", {}).get("price", {}).get("amount")
                    if price:
                        return ("waitrose", f"£{float(price):.2f}")
        except Exception as e:
            _errors.append(f"waitrose exc: {e}")
        return ("waitrose", None)

    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(_tesco_price, search_term), ex.submit(_waitrose_price, search_term)]
        for f in as_completed(futs):
            store, price = f.result()
            if price:
                prices[store] = price

    # --- 5. Groq: fill missing store prices + alternatives in one call ---
    alternatives = []
    missing = [s for s in ["tesco","asda","waitrose","aldi","amazon"] if s not in prices]
    if search_term and groq_key:
        try:
            stores_list = ", ".join(s.title() for s in missing) if missing else ""
            prompt = f'UK grocery product: "{full_name}".\n'
            if missing:
                prompt += (f'Give the current typical UK shelf price at: {stores_list}. '
                           f'Return as JSON object with lowercase store keys e.g. {{"tesco":"£X.XX",...}}.\n')
            prompt += ('Also suggest 3 cheaper/equivalent alternatives at UK supermarkets. '
                       'For each: product name, price, one-line reason.\n'
                       'Return ONLY JSON: {"prices":{...},"alternatives":[{"name":"...","price":"£X.XX","reason":"..."}]}')
            raw = _groq(prompt, max_tokens=500)
            obj = _extract_obj(raw)
            for k, v in obj.get("prices", {}).items():
                if k not in prices:
                    prices[k] = v
            alternatives = obj.get("alternatives", [])
            _errors.append(f"groq prices+alts ok")
        except Exception as e:
            _errors.append(f"groq prices exc: {e}")

    out = {"product": product, "alternatives": alternatives,
           "prices": prices, "query": search_term, "search_q": search_term}
    if debug:
        out["_errors"] = _errors
    return jsonify(out)


# ── WhatsApp Save & Triage ────────────────────────────────────────────────────
# Supabase table required (run once in Supabase SQL editor):
# CREATE TABLE IF NOT EXISTS wa_saves (
#   id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
#   from_number text NOT NULL,
#   url text NOT NULL,
#   title text,
#   summary text,
#   status text DEFAULT 'pending',
#   remind_day text,
#   created_at timestamptz DEFAULT now()
# );
# CREATE INDEX ON wa_saves(from_number, created_at DESC);

_PENDING_TRIAGE: dict = {}   # from_number -> {id, title, url, expires}
_PENDING_DIGEST: dict  = {}  # from_number -> [{id, title, url}, ...]  (last digest sent)
_USER_LAST_LOCATION: dict = {}  # from_number -> {lat, lon, ts}  (from WhatsApp location share)


_TOKEN_TO_NUMBER: dict  = {}  # token -> from_number, populated as users interact
_MENU_SESSION:   dict  = {}  # from_number -> {save_id, expires, page_count} for multi-photo menus

def _wa_user_token(from_number: str) -> str:
    """Stable per-user token derived from phone number + server secret.
    Normalises out 'whatsapp:' prefix so web-app phone login matches WhatsApp DB records."""
    secret = os.environ.get("DIGEST_TOKEN", "miru-secret")
    normalized = from_number.replace("whatsapp:", "").strip()
    token = hmac.new(secret.encode(), normalized.encode(), hashlib.sha256).hexdigest()[:20]
    _TOKEN_TO_NUMBER[token] = from_number  # store original (incl. whatsapp:) for DB queries
    return token

def _wa_number_variants(from_number: str) -> list:
    """Return both DB forms of a phone number: with and without 'whatsapp:' prefix.
    Needed because WhatsApp saves use 'whatsapp:+44...' but web login resolves to '+44...'."""
    clean = from_number.replace("whatsapp:", "").strip()
    return [clean, "whatsapp:" + clean]

def _resolve_user_token(token: str):
    """Return from_number for a user token, or None. Populates cache from DB on cold start."""
    if token in _TOKEN_TO_NUMBER:
        return _TOKEN_TO_NUMBER[token]
    try:
        # Primary: rebuild from wa_saves (all users who have saves)
        rows = lib._sb().table("wa_saves").select("from_number").execute().data
        seen = set()
        for row in rows:
            n = row.get("from_number", "")
            if n and n not in seen:
                seen.add(n)
                _wa_user_token(n)  # populates _TOKEN_TO_NUMBER
        resolved = _TOKEN_TO_NUMBER.get(token)
        if resolved:
            return resolved
        # Fallback: check ai_cache for persisted token→phone (survives zero-saves case)
        cache_rows = lib._sb().table("ai_cache").select("data").eq("key", f"user_token:{token}").execute().data
        if cache_rows:
            phone = (cache_rows[0].get("data") or {}).get("phone")
            if phone:
                _wa_user_token(phone)  # populates _TOKEN_TO_NUMBER
                return _TOKEN_TO_NUMBER.get(token)
        print(f"[token] resolve miss — token={token[:8]}... known_count={len(_TOKEN_TO_NUMBER)}")
        return None
    except Exception as _te:
        print(f"[token] resolve error: {_te}")
        return None


@app.route("/api/user/location", methods=["POST"])
def api_user_location():
    """Web app posts browser geolocation here; stored against the user's phone number.
    Also retroactively patches recent saves that were missing a location."""
    data = request.json or {}
    token = data.get("token", "").strip()
    lat   = data.get("lat")
    lon   = data.get("lon")
    if not token or lat is None or lon is None:
        return jsonify({"ok": False}), 400
    from_number = _resolve_user_token(token)
    if not from_number:
        return jsonify({"ok": False}), 404
    lat, lon = float(lat), float(lon)
    _USER_LAST_LOCATION[from_number] = {"lat": lat, "lon": lon, "ts": time.time()}

    # Reverse-geocode to a readable location string
    def _patch_recent():
        try:
            rg = requests.get(
                "https://api.postcodes.io/postcodes",
                params={"lon": lon, "lat": lat, "limit": 1},
                timeout=5,
            )
            rg_data = rg.json().get("result") or []
            if not rg_data:
                return
            pc = rg_data[0]
            location_str = f"{pc.get('admin_ward','')}, {pc.get('postcode','')}, {pc.get('admin_district','')}".strip(", ")
            if not location_str:
                return

            # Find saves from the last 15 minutes for this user that have no 📍
            cutoff = (time.time() - 900)
            import datetime as _dt
            cutoff_iso = _dt.datetime.utcfromtimestamp(cutoff).isoformat()
            rows = lib._sb().table("wa_saves") \
                .select("id,summary") \
                .eq("from_number", from_number) \
                .gte("created_at", cutoff_iso) \
                .execute().data or []

            for row in rows:
                summary = row.get("summary", "") or ""
                if "📍" in summary:
                    continue  # already has location
                # Inject location into META line
                updated = _re.sub(
                    r"(META:📅[^\n]*)",
                    lambda m: m.group(1) + f" · 📍 {location_str}",
                    summary,
                )
                if updated != summary:
                    lib._sb().table("wa_saves").update({"summary": updated}).eq("id", row["id"]).execute()
                    print(f"[location] patched save {row['id']} with {location_str}")
        except Exception as e:
            print(f"[location] patch_recent error: {e}")

    import threading as _th
    _th.Thread(target=_patch_recent, daemon=True).start()
    return jsonify({"ok": True})


def _fetch_url_text(url: str) -> dict:
    """Fetch a URL and extract title + body text using stdlib html.parser only."""
    import html.parser as _hp

    class _X(_hp.HTMLParser):
        def __init__(self):
            super().__init__()
            self._t, self._b = [], []
            self._in_t = False
            self._skip = 0
            self._SKIP = {"script", "style", "nav", "footer", "noscript", "head", "aside"}

        def handle_starttag(self, tag, attrs):
            if tag == "title":
                self._in_t = True
            if tag in self._SKIP:
                self._skip += 1

        def handle_endtag(self, tag):
            if tag == "title":
                self._in_t = False
            if tag in self._SKIP:
                self._skip = max(0, self._skip - 1)

        def handle_data(self, data):
            s = data.strip()
            if not s:
                return
            if self._in_t:
                self._t.append(s)
            elif self._skip == 0:
                self._b.append(s)

    try:
        r = requests.get(url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)",
            "Accept": "text/html,application/xhtml+xml",
        }, allow_redirects=True)
        ct = r.headers.get("content-type", "")
        if "html" not in ct and "plain" not in ct:
            return {"title": url, "text": "", "ok": False}
        p = _X()
        p.feed(r.text[:200000])
        p.close()
        title = " ".join(p._t).strip()[:200] or url
        text = " ".join(p._b).strip()[:10000]
        return {"title": title, "text": text, "ok": bool(text)}
    except Exception as e:
        return {"title": url, "text": "", "ok": False, "error": str(e)}


def _quick_brand_intel(brand_name: str) -> dict:
    """Fast Groq call: parent company, country of origin, founding year, one-line fact."""
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key or not brand_name:
        return {}
    try:
        import json as _json
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [{"role": "user", "content":
                    f'Brand: "{brand_name}"\n'
                    'Return ONLY valid JSON:\n'
                    '{{"parent":"parent company name, or empty if independent brand",'
                    '"country":"country of origin, 1-3 words",'
                    '"founded":"founding year or empty",'
                    '"fact":"one punchy fact in under 12 words, or empty if unknown"}}\n'
                    'If brand is unrecognised return all empty strings.'}],
                "max_tokens": 120, "temperature": 0.1,
            },
            timeout=8,
        )
        text = r.json()["choices"][0]["message"]["content"].strip()
        # Strip markdown code fences if present
        text = text.strip("`").strip()
        if text.startswith("json"):
            text = text[4:].strip()
        return _json.loads(text)
    except Exception as e:
        print(f"[brand_intel] {e}")
        return {}


def _vivino_lookup(wine_name: str) -> dict:
    """Search Vivino for a wine. Returns {rating, rating_count, url, name, winery}."""
    try:
        r = requests.get(
            "https://www.vivino.com/api/explore",
            params={"q": wine_name, "country_code": "GB", "currency_code": "GBP", "page": 1},
            headers={
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
                "Accept": "application/json",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=8,
        )
        if r.status_code == 200:
            matches = r.json().get("explore_vintage", {}).get("matches", [])
            if matches:
                v = matches[0].get("vintage", {})
                stats = v.get("statistics", {})
                wine = v.get("wine", {})
                seo = wine.get("seo_name", "")
                year = v.get("year", "")
                url = f"https://www.vivino.com/wines/{seo}/{year}" if seo and year else (f"https://www.vivino.com/wines/{seo}" if seo else "")
                return {
                    "rating": stats.get("ratings_average"),
                    "rating_count": stats.get("ratings_count"),
                    "url": url,
                    "name": wine.get("name", ""),
                    "winery": (wine.get("winery") or {}).get("name", ""),
                }
    except Exception as e:
        print(f"[vivino] {e}")
    return {}


_WINE_RATING_STATE: dict = {}  # from_number → {save_id, wine_name, expires}


def _lookup_book_by_isbn(isbn: str) -> dict | None:
    """Google Books → Open Library lookup by ISBN. Returns normalised book dict or None."""
    gbooks_key = os.environ.get("GOOGLE_BOOKS_API_KEY", "")
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"isbn:{isbn}", "maxResults": 1, **({"key": gbooks_key} if gbooks_key else {})},
            timeout=10,
        )
        items = r.json().get("items")
        if items:
            vi = items[0].get("volumeInfo", {})
            isbns = [x["identifier"] for x in vi.get("industryIdentifiers", [])
                     if x.get("type") in ("ISBN_13", "ISBN_10")]
            cover = ""
            if vi.get("imageLinks"):
                cover = (vi["imageLinks"].get("thumbnail") or vi["imageLinks"].get("smallThumbnail") or "").replace("http://", "https://")
            rating = vi.get("averageRating")
            return {
                "found": True, "isbn": isbns[0] if isbns else isbn,
                "title": vi.get("title", ""),
                "author": ", ".join(vi.get("authors", [])) or "Unknown author",
                "cover": cover,
                "description": vi.get("description", ""),
                "subjects": " · ".join(vi.get("categories", [])[:5]),
                "year": vi.get("publishedDate", "")[:4],
                "publishers": vi.get("publisher", ""),
                "communityRating": {"avg": round(float(rating), 1), "count": vi.get("ratingsCount", 0)} if rating else None,
                "status": "wishlist",
            }
    except Exception:
        pass
    # Open Library fallback
    try:
        ol = requests.get(f"https://openlibrary.org/isbn/{isbn}.json", timeout=8).json()
        title = ol.get("title", "")
        work_key = (ol.get("works") or [{}])[0].get("key", "")
        author, description, year = "", "", (ol.get("publish_date") or "")[:4]
        if work_key:
            wd = requests.get(f"https://openlibrary.org{work_key}.json", timeout=6).json()
            title = title or wd.get("title", "")
            desc = wd.get("description", "")
            description = desc.get("value", desc) if isinstance(desc, dict) else str(desc)
            for a in (wd.get("authors") or [])[:1]:
                ak = a.get("author", {}).get("key", "")
                if ak:
                    author = requests.get(f"https://openlibrary.org{ak}.json", timeout=5).json().get("name", "")
        covers = ol.get("covers") or []
        cover = f"https://covers.openlibrary.org/b/id/{covers[0]}-M.jpg" if covers else ""
        if title:
            return {"found": True, "isbn": isbn, "title": title,
                    "author": author or "Unknown author", "cover": cover,
                    "description": description[:600], "year": year,
                    "subjects": "", "publishers": "", "communityRating": None, "status": "wishlist"}
    except Exception:
        pass
    return None


def _lookup_book_by_title(query: str) -> dict | None:
    """Search Google Books by title/author query. Returns best match as normalised dict or None."""
    gbooks_key = os.environ.get("GOOGLE_BOOKS_API_KEY", "")
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": query, "maxResults": 1, "printType": "books",
                    **({"key": gbooks_key} if gbooks_key else {})},
            timeout=8,
        )
        items = r.json().get("items")
        if items:
            vi = items[0].get("volumeInfo", {})
            isbns = [x["identifier"] for x in vi.get("industryIdentifiers", [])
                     if x.get("type") in ("ISBN_13", "ISBN_10")]
            cover = ""
            if vi.get("imageLinks"):
                cover = (vi["imageLinks"].get("thumbnail") or vi["imageLinks"].get("smallThumbnail") or "").replace("http://", "https://")
            rating = vi.get("averageRating")
            return {
                "found": True, "isbn": isbns[0] if isbns else "",
                "title": vi.get("title", ""),
                "author": ", ".join(vi.get("authors", [])) or "Unknown author",
                "cover": cover,
                "description": vi.get("description", ""),
                "subjects": " · ".join(vi.get("categories", [])[:5]),
                "year": vi.get("publishedDate", "")[:4],
                "publishers": vi.get("publisher", ""),
                "communityRating": {"avg": round(float(rating), 1), "count": vi.get("ratingsCount", 0)} if rating else None,
                "status": "wishlist",
            }
    except Exception:
        pass
    # Open Library fallback
    try:
        r = requests.get("https://openlibrary.org/search.json",
                         params={"q": query, "limit": 1, "fields": "key,title,author_name,isbn,first_publish_year"},
                         timeout=10)
        docs = r.json().get("docs", [])
        if docs:
            d = docs[0]
            isbns = d.get("isbn") or []
            isbn13 = next((i for i in isbns if len(i) == 13), isbns[0] if isbns else "")
            return {"found": True, "isbn": isbn13,
                    "title": d.get("title", ""),
                    "author": ", ".join(d.get("author_name", [])) or "Unknown author",
                    "cover": f"https://covers.openlibrary.org/b/isbn/{isbn13}-M.jpg" if isbn13 else "",
                    "description": "", "year": str(d.get("first_publish_year", "")),
                    "subjects": "", "publishers": "", "communityRating": None, "status": "wishlist"}
    except Exception:
        pass
    return None


_ISBN_BARCODE_FAILED = object()  # sentinel: pyzbar ran but found no ISBN

def _try_isbn_from_image(raw_bytes: bytes):
    """Run pyzbar on image bytes; if an ISBN barcode is found, look it up via Google Books.
    Returns a book dict on success, _ISBN_BARCODE_FAILED if pyzbar ran but found nothing, None on error."""
    try:
        from pyzbar import pyzbar as _pyzbar
        from PIL import Image as _PILImage, ImageOps as _ImageOps, ImageEnhance as _IE, ImageFilter as _IF
        import io as _io
        img = _PILImage.open(_io.BytesIO(raw_bytes))
        img = _ImageOps.exif_transpose(img).convert("RGB")

        def _extract_isbn(codes):
            isbn, isbn10 = None, None
            for c in codes:
                val = c.data.decode("utf-8", errors="ignore").replace("-", "").replace(" ", "").strip()
                if re.match(r"^(978|979)\d{10}$", val):
                    isbn = val; break
                elif re.match(r"^\d{10}$", val) and not isbn10:
                    isbn10 = val
            return isbn or isbn10

        # Attempt 1: raw image
        isbn = _extract_isbn(_pyzbar.decode(img))

        # Attempt 2: high-contrast greyscale
        if not isbn:
            grey = img.convert("L")
            isbn = _extract_isbn(_pyzbar.decode(_IE.Contrast(grey).enhance(2.5).convert("RGB")))

        # Attempt 3: crop bottom third (ISBN barcode usually near bottom of back cover)
        if not isbn:
            w, h = img.size
            bottom = img.crop((0, int(h * 0.6), w, h))
            isbn = _extract_isbn(_pyzbar.decode(bottom))
            if not isbn:
                isbn = _extract_isbn(_pyzbar.decode(_IE.Contrast(bottom.convert("L")).enhance(2.5).convert("RGB")))

        # Attempt 4: sharpen then scan
        if not isbn:
            sharp = img.filter(_IF.SHARPEN)
            isbn = _extract_isbn(_pyzbar.decode(sharp))

        if not isbn:
            print("[isbn barcode] pyzbar found no ISBN barcode in image")
            return _ISBN_BARCODE_FAILED
        result = _lookup_book_by_isbn(isbn)
        if not result:
            return {"isbn": isbn, "title": f"Book (ISBN {isbn})", "found": True}
        return result
    except Exception as e:
        print(f"[isbn barcode] {e}")
        return None


def _wa_process_image(from_number: str, media_url: str, media_type: str) -> str:
    """Download a WhatsApp photo, analyse with Groq vision, save to wa_saves."""
    import base64, threading

    # Save bare record immediately — even before download — so it's never lost
    save_id = None
    try:
        row = lib._sb().table("wa_saves").insert({
            "from_number": from_number,
            "url":         media_url,
            "title":       "📷 Photo",
            "summary":     "",
            "status":      "pending",
        }).execute()
        if row.data:
            save_id = row.data[0].get("id")
    except Exception as _se:
        app.logger.warning(f"[vision] bare save failed: {_se}")

    twilio_sid   = os.environ.get("TWILIO_ACCOUNT_SID", "")
    twilio_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    try:
        r = requests.get(media_url, auth=(twilio_sid, twilio_token), timeout=12)
        if r.status_code != 200:
            app.logger.warning(f"[vision] image download failed status={r.status_code}")
            user_token = _wa_user_token(from_number)
            _wa_send_proactive(from_number,
                f"⚠️ Couldn't read your photo — saved anyway.\n📂 My Saves: miru.humanagency.co/?screen=saves&token={user_token}")
            return "⚠️ Had trouble reading the image — saved it anyway. Try resending for a better result."
        img_b64 = base64.b64encode(r.content).decode()
        mime = media_type or "image/jpeg"
    except Exception as e:
        app.logger.warning(f"[vision] image download exception: {e}")
        user_token = _wa_user_token(from_number)
        _wa_send_proactive(from_number,
            f"⚠️ Couldn't download your photo — saved anyway.\n📂 My Saves: miru.humanagency.co/?screen=saves&token={user_token}")
        return "⚠️ Had trouble downloading — saved it anyway."

    def _bg(sid=save_id, fn=from_number, b64=img_b64, m=mime, raw=r.content,
            _loc=_USER_LAST_LOCATION.get(from_number)):
        # ── Persist image to Supabase Storage (so URL never expires) ────────────
        _stored_image_url = ""
        if sid and raw:
            try:
                _ext = (m or "image/jpeg").split("/")[-1].replace("jpeg", "jpg")
                _path = f"{sid}.{_ext}"
                lib._sb().storage.from_("saves-images").upload(
                    _path, raw, {"content-type": m or "image/jpeg", "upsert": "true"}
                )
                _stored_image_url = lib._sb().storage.from_("saves-images").get_public_url(_path)
                lib._sb().table("wa_saves").update({"image_url": _stored_image_url}).eq("id", sid).execute()
            except Exception as _ue:
                print(f"[vision] storage upload failed: {_ue}")

        # ── QR code scan ────────────────────────────────────────────────────────
        qr_url = ""
        qr_event_info = ""
        try:
            from pyzbar import pyzbar as _pyzbar
            from PIL import Image as _PILImage
            import io as _io
            _img = _PILImage.open(_io.BytesIO(raw))
            _codes = _pyzbar.decode(_img)
            for _code in _codes:
                _val = _code.data.decode("utf-8", errors="ignore").strip()
                if _val.startswith("http"):
                    qr_url = _val
                    print(f"[vision] QR code found: {qr_url}")
                    # Fetch page and extract title/description
                    try:
                        _pr = requests.get(qr_url, timeout=8,
                            headers={"User-Agent": "Mozilla/5.0 (compatible; MiruBot/1.0)"})
                        import re as _re2
                        _pt = _pr.text
                        _title_m = _re2.search(r'<title[^>]*>([^<]{3,200})</title>', _pt, _re2.I)
                        _desc_m  = _re2.search(r'<meta[^>]+(?:og:description|name=["\']description["\'])[^>]+content=["\']([^"\']{10,300})', _pt, _re2.I)
                        _t = _title_m.group(1).strip() if _title_m else ""
                        _d = _desc_m.group(1).strip() if _desc_m else ""
                        if _t:
                            qr_event_info = f"🔗 QR links to: {_t}" + (f"\n{_d}" if _d else "")
                    except Exception as _qe:
                        print(f"[vision] QR URL fetch failed: {_qe}")
                    break
        except Exception as _qex:
            print(f"[vision] QR scan error: {_qex}")

        # ── Reverse-geocode stored location for context ──────────────────────────
        _loc_context = ""
        if _loc and (time.time() - _loc.get("ts", 0)) < 7200:
            try:
                _gm_key = os.environ.get("GOOGLE_PLACES_KEY", "") or os.environ.get("GOOGLE_MAPS_KEY", "")
                if _gm_key:
                    _nr = requests.get(
                        "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
                        params={
                            "location": f"{_loc['lat']},{_loc['lon']}",
                            "rankby": "distance",
                            "type": "establishment",
                            "key": _gm_key,
                        },
                        timeout=5,
                    )
                    _nr_results = _nr.json().get("results", [])
                    if _nr_results:
                        _place = _nr_results[0]
                        _loc_context = f"{_place['name']}, {_place.get('vicinity', '')}"
                        print(f"[vision] location context: {_loc_context!r}")
            except Exception as _le:
                print(f"[vision] location lookup error: {_le}")

        _vision_models = [
            "meta-llama/llama-4-scout-17b-16e-instruct",
            "llama-3.2-90b-vision-preview",
            "llama-3.2-11b-vision-preview",
        ]
        _loc_hint = (f"\nContext: this photo was taken at or near {_loc_context}." if _loc_context else "")
        prompt_text = (
            "Identify what this image is. Pick ONE type from: "
            "event/ticket, store/restaurant, billboard/ad, receipt/bill, menu, wine, sign, document, product, photo.\n"
            "IMPORTANT type rules:\n"
            "- Use 'wine' for any photo of a wine bottle or wine label — even if on a table or shelf.\n"
            "- Use 'product' for ANY photo showing physical products, items on shelves, products with price tags, "
            "or a basket/trolley of items — even if taken inside a store or supermarket.\n"
            "- Use 'billboard/ad' ONLY for printed posters, banners, or ads that are NOT showing products on shelves.\n"
            "- Use 'store/restaurant' ONLY for the exterior or entrance of a shop/restaurant, NOT for shelf or product photos.\n"
            "Then give 3 bullet points starting with • covering the key info.\n"
            "If store/restaurant: focus ONLY on the place itself — name, type of food/business, opening hours or price range if visible.\n"
            "If event/ticket: include event name, date, time, venue.\n"
            "If ad/billboard: state the brand, product name, and price/offer.\n"
            "If product: list EVERY product visible. Look at all price tags, shelf-edge labels, and packaging.\n"
            "If receipt: total and main items.\n"
            "Start your reply with: TYPE: [your choice]\n"
            "If type is event/ticket, store/restaurant, ad/billboard, or product — add: VENUE: [brand or business name only]\n"
            "If type is product OR ad/billboard — list every product on a separate PRODUCT: line:\n"
            "  PRODUCT: [full product name incl. variant & size] | [brand] | [price or n/a]\n"
            "  e.g. PRODUCT: Heinz Baked Beans 415g | Heinz | £0.89\n"
            "  e.g. PRODUCT: Simple Moisturiser 125ml | Simple | n/a\n"
            "Also add: SHOP: [retailer name if identifiable — e.g. 'Tesco' — or leave blank]\n"
            "If you can identify a city or area from signage — add: LOCATION: [city or area name]\n"
            "Always add: SEARCH: [2-5 word search term]"
            + _loc_hint
        )
        analysis = ""
        for model in _vision_models:
            try:
                body = {
                    "model": model,
                    "max_tokens": 500,
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": f"data:{m};base64,{b64}"}},
                            {"type": "text", "text": prompt_text},
                        ]
                    }],
                }
                resp = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {os.environ.get('GROQ_API_KEY','')}",
                             "Content-Type": "application/json"},
                    json=body, timeout=20,
                )
                print(f"[vision] model={model} status={resp.status_code}")
                if resp.status_code == 200:
                    analysis = resp.json()["choices"][0]["message"]["content"].strip()
                    break
                else:
                    print(f"[vision] error body: {resp.text[:300]}")
            except Exception as exc:
                print(f"[vision] model={model} exception: {exc}")

        # Derive title and search intent from type
        title = "📷 Photo"
        img_type = "photo"
        if analysis:
            first = analysis.split("\n")[0].lower()
            if "store" in first or "restaurant" in first or "cafe" in first or "coffee" in first:
                title = "🏪 Place"; img_type = "store"
            elif "event" in first or "ticket" in first:
                title = "🎫 Event/Ticket"; img_type = "event"
            elif "billboard" in first or "ad" in first:
                img_type = "ad"
                title = "📢 Billboard/Ad"  # updated below once VENUE: tag is parsed
            elif "product" in first:
                title = "🏷️ Brand"; img_type = "product"
            elif "receipt" in first or "bill" in first:
                title = "🧾 Receipt"; img_type = "receipt"
            elif "menu" in first:
                title = "🍽️ Menu"; img_type = "menu"
            elif "wine" in first:
                title = "🍷 Wine"; img_type = "wine"
            elif "sign" in first:
                title = "🪧 Sign"; img_type = "sign"
            elif "document" in first:
                title = "📄 Document"; img_type = "document"

        # Extract VENUE: / LOCATION: / SEARCH: tags before stripping metadata lines
        venue_tag = ""
        location_tag = ""
        search_tag = ""
        product_items = []   # list of {name, brand, price} dicts
        shop_tag = ""
        _BLANK = {"", "n/a", "not visible", "unknown", "none", "-"}
        for _line in analysis.split("\n"):
            _up = _line.strip().upper()
            if _up.startswith("VENUE:"):
                venue_tag = _line.split(":", 1)[1].strip()
            elif _up.startswith("LOCATION:"):
                location_tag = _line.split(":", 1)[1].strip()
            elif _up.startswith("SEARCH:"):
                search_tag = _line.split(":", 1)[1].strip()
            elif _up.startswith("PRODUCT:"):
                _raw = _line.split(":", 1)[1].strip()
                _parts = [p.strip() for p in _raw.split("|")]
                _pname  = _parts[0] if len(_parts) > 0 else ""
                _pbrand = _parts[1] if len(_parts) > 1 else ""
                _pprice = _parts[2] if len(_parts) > 2 else ""
                if _pprice.lower() in _BLANK:
                    _pprice = ""
                if _pname:
                    product_items.append({"name": _pname, "brand": _pbrand, "price": _pprice})
            elif _up.startswith("SHOP:"):
                shop_tag = _line.split(":", 1)[1].strip()
                if shop_tag.lower() in _BLANK:
                    shop_tag = ""
        # Use VENUE: brand name in ad/billboard title now that tags are parsed
        if img_type == "ad" and venue_tag:
            title = f"📢 {venue_tag[:60]}"

        # Upgrade to product type if model returned PRODUCT: lines regardless of classified type
        if product_items and img_type not in ("product",):
            print(f"[vision] upgrading img_type from {img_type!r} to 'product' — {len(product_items)} product(s) found")
            img_type = "product"

        print(f"[vision] venue_tag={venue_tag!r} location_tag={location_tag!r} search_tag={search_tag!r} img_type={img_type}")
        print(f"[vision] products={product_items} shop_tag={shop_tag!r}")

        # Strip metadata lines from summary
        _meta_prefixes = ("TYPE:", "VENUE:", "LOCATION:", "SEARCH:", "PRODUCT:", "SHOP:")
        summary = "\n".join(
            l for l in analysis.split("\n")
            if not any(l.strip().upper().startswith(p) for p in _meta_prefixes)
        ).strip()

        # Build a meaningful search URL — strip filler, keep key object + venue
        import urllib.parse, re as _re
        _FILLER = _re.compile(
            r"^(the (event|show|performance|concert|exhibition|gig|festival|billboard|image|photo|sign|ad|poster|document|receipt|menu)"
            r"(\s+(is|are))?\s*(called|named|titled|known as|promoting|showing|advertis\w+|for|of|reads|says|displays?|featuring?|from)?\s*[:\-–—\"']?\s*"
            r"|this\s+(event|show|is|appears?\s+to\s+be)\s+(called|named|titled|a|an)?\s*[:\-–—\"']?\s*"
            r"|it\s+(is|appears?\s+to\s+be)\s+(called|named|titled|promoting|showing|advertis\w+)\s*[:\-–—\"']?\s*)",
            _re.IGNORECASE,
        )
        _VENUE_PREFIX = _re.compile(
            r"^(venue|location|at|held\s+at|taking\s+place\s+at|place)\s*[:\-–—]?\s*",
            _re.IGNORECASE,
        )
        bullet_lines = [b.strip() for b in summary.split("•") if b.strip()]
        search_terms = ""
        venue_raw = venue_tag  # use explicit VENUE: tag from model
        name_raw = ""
        if bullet_lines:
            name_raw = _FILLER.sub("", bullet_lines[0]).strip().strip('"\'').strip()[:80]
        # Prefer the model's SEARCH: tag; fall back to extracted name
        if search_tag:
            if img_type == "event":
                q = f"{search_tag} {venue_raw}".strip() + " tickets"
                search_terms = urllib.parse.quote_plus(q)
            else:
                search_terms = urllib.parse.quote_plus(search_tag[:80])
        elif name_raw:
            if img_type == "event":
                q = f"{name_raw} {venue_raw}".strip() + " tickets"
                search_terms = urllib.parse.quote_plus(q)
            else:
                search_terms = urllib.parse.quote_plus(name_raw)

        # Try to resolve the first organic result for a direct link
        direct_url = ""
        if search_terms:
            try:
                ddg = requests.get(
                    "https://html.duckduckgo.com/html/",
                    params={"q": urllib.parse.unquote_plus(search_terms)},
                    headers={"User-Agent": "Mozilla/5.0 (compatible; MiruBot/1.0)"},
                    timeout=6,
                )
                m = _re.search(r'class="result__url"[^>]*>\s*([^\s<]+)', ddg.text)
                if m:
                    u = m.group(1).strip()
                    if not u.startswith("http"):
                        u = "https://" + u
                    # Skip ad/tracking domains
                    if not any(x in u for x in ["duckduckgo", "bing.com", "google.com", "amazon-adsystem"]):
                        direct_url = u
            except Exception as exc:
                print(f"[vision] DDG lookup failed: {exc}")

        search_url = direct_url or (f"https://www.google.com/search?q={search_terms}" if search_terms else "")

        # Look up venue/place details
        venue_info = {}
        lookup_name = venue_raw or (name_raw if bullet_lines else "")
        if lookup_name and img_type in ("store", "event", "ad", "menu"):
            venue_info = _lookup_venue(lookup_name)
            if venue_info.get("name") and venue_info["name"] != lookup_name:
                title = f"🏪 {venue_info['name']}" if img_type == "store" else title

        # Store URL as venue maps link for stores
        if img_type == "store" and venue_info.get("maps_url"):
            search_url = venue_info["maps_url"]

        # Brand intel lookup for product photos (use first product's brand)
        brand_intel = {}
        if img_type == "product":
            brand_name = (product_items[0]["brand"] if product_items else "") or venue_raw or (search_tag.split()[0] if search_tag else "")
            if brand_name:
                brand_intel = _quick_brand_intel(brand_name)
                print(f"[vision] brand_intel for {brand_name!r}: {brand_intel}")
            # Title: single product → use its name; multiple → count
            if product_items:
                if len(product_items) == 1:
                    title = f"🏷️ {product_items[0]['name'][:50]}"
                else:
                    title = f"🏷️ {len(product_items)} Products"
            else:
                title = f"🏷️ {(venue_raw or name_raw or 'Product')[:50]}"

        # Append QR event info to summary if found
        if qr_event_info:
            summary = (summary + "\n\n" + qr_event_info).strip()
        if qr_url and not search_url:
            search_url = qr_url

        # ── Menu: structured extraction ──────────────────────────────────────────
        menu_text = ""
        if img_type == "menu":
            try:
                _menu_prompt = (
                    "Look at this menu image carefully.\n\n"
                    "First, extract any restaurant details visible on the menu — output these lines if present:\n"
                    "NAME: [restaurant name]\n"
                    "PHONE: [phone number]\n"
                    "ADDRESS: [address or location]\n"
                    "HOURS: [opening times]\n\n"
                    "Then extract the menu items grouped by section "
                    "(e.g. Starters, Mains, Desserts, Drinks, Sides). "
                    "For each section list items with their prices.\n"
                    "Format exactly like this:\n"
                    "*Starters*\n• Soup of the day — £6.50\n• Bruschetta — £7.00\n"
                    "*Mains*\n• Fish & Chips — £14.50\n\n"
                    "Only include what is clearly visible. Skip sections or details with no readable content."
                )
                _menu_resp = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {os.environ.get('GROQ_API_KEY','')}",
                             "Content-Type": "application/json"},
                    json={
                        "model": _vision_models[0],
                        "max_tokens": 800,
                        "messages": [{"role": "user", "content": [
                            {"type": "image_url", "image_url": {"url": f"data:{m};base64,{b64}"}},
                            {"type": "text", "text": _menu_prompt},
                        ]}],
                    },
                    timeout=20,
                )
                if _menu_resp.status_code == 200:
                    _raw_menu = _menu_resp.json()["choices"][0]["message"]["content"].strip()
                    # Parse out restaurant detail lines; remainder is the menu
                    _menu_meta = {}
                    _menu_lines = []
                    for _ml in _raw_menu.split("\n"):
                        _mu = _ml.strip().upper()
                        if _mu.startswith("NAME:"):
                            _menu_meta["name"] = _ml.split(":", 1)[1].strip()
                        elif _mu.startswith("PHONE:"):
                            _menu_meta["phone"] = _ml.split(":", 1)[1].strip()
                        elif _mu.startswith("ADDRESS:"):
                            _menu_meta["address"] = _ml.split(":", 1)[1].strip()
                        elif _mu.startswith("HOURS:"):
                            _menu_meta["hours"] = _ml.split(":", 1)[1].strip()
                        else:
                            _menu_lines.append(_ml)
                    menu_text = "\n".join(_menu_lines).strip()
                    # Always prefer the restaurant name extracted from the menu itself
                    if _menu_meta.get("name"):
                        venue_tag = _menu_meta["name"]
                        _menu_loc = _menu_meta.get("address", "") or location_tag
                        _menu_loc_short = _menu_loc.split(",")[0].strip() if _menu_loc else ""
                        title = f"🍽️ {venue_tag} Menu" + (f", {_menu_loc_short}" if _menu_loc_short else "")
                    if _menu_meta.get("name") and not location_tag:
                        location_tag = _menu_meta.get("address", "")
                    # Prepend restaurant info block to menu text
                    _info_lines = []
                    if _menu_meta.get("name"):    _info_lines.append(f"🍽️ {_menu_meta['name']}")
                    if _menu_meta.get("address"): _info_lines.append(f"📍 {_menu_meta['address']}")
                    if _menu_meta.get("phone"):   _info_lines.append(f"📞 {_menu_meta['phone']}")
                    if _menu_meta.get("hours"):   _info_lines.append(f"🕐 {_menu_meta['hours']}")
                    if _info_lines:
                        menu_text = "\n".join(_info_lines) + "\n\n" + menu_text
            except Exception as _me:
                print(f"[vision] menu extraction failed: {_me}")

        # ── Multi-photo menu: append pages to the existing session save ──────────
        if img_type == "menu" and menu_text:
            session = _MENU_SESSION.get(fn)
            if session and time.time() < session.get("expires", 0):
                page_num = session["page_count"] + 1
                _MENU_SESSION[fn]["page_count"] = page_num
                _MENU_SESSION[fn]["expires"] = time.time() + 900
                try:
                    existing = lib._sb().table("wa_saves").select("summary").eq("id", session["save_id"]).execute().data
                    existing_summary = (existing[0]["summary"] if existing else "") or ""
                    appended = existing_summary + f"\n\n---\n*Page {page_num}*\n" + menu_text
                    lib._sb().table("wa_saves").update({"summary": appended}).eq("id", session["save_id"]).execute()
                    if sid:
                        lib._sb().table("wa_saves").delete().eq("id", sid).execute()
                except Exception as _me2:
                    print(f"[menu-session] append failed: {_me2}")
                user_token = _wa_user_token(fn)
                _wa_send_proactive(fn, f"🍽️ Added page {page_num} to your menu. Send more pages or just keep chatting!\n\n📂 My Saves: miru.humanagency.co/?screen=saves&token={user_token}")
                return

        # ── Wine: label extraction + Vivino rating ───────────────────────────────
        wine_info = {}
        if img_type == "wine":
            try:
                _wine_prompt = (
                    "This is a wine bottle or label. Extract what is clearly visible:\n"
                    "WINE_NAME: [full wine name]\n"
                    "WINERY: [producer/winery name]\n"
                    "VINTAGE: [year, or NV if non-vintage]\n"
                    "REGION: [region and/or country]\n"
                    "GRAPE: [grape variety or blend]\n"
                    "Only output lines where the information is clearly visible on the label."
                )
                _wine_resp = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {os.environ.get('GROQ_API_KEY','')}",
                             "Content-Type": "application/json"},
                    json={
                        "model": _vision_models[0],
                        "max_tokens": 200,
                        "messages": [{"role": "user", "content": [
                            {"type": "image_url", "image_url": {"url": f"data:{m};base64,{b64}"}},
                            {"type": "text", "text": _wine_prompt},
                        ]}],
                    },
                    timeout=15,
                )
                if _wine_resp.status_code == 200:
                    _raw_wine = _wine_resp.json()["choices"][0]["message"]["content"].strip()
                    for _wl in _raw_wine.split("\n"):
                        _wu = _wl.strip().upper()
                        if _wu.startswith("WINE_NAME:"):
                            wine_info["name"] = _wl.split(":", 1)[1].strip()
                        elif _wu.startswith("WINERY:"):
                            wine_info["winery"] = _wl.split(":", 1)[1].strip()
                        elif _wu.startswith("VINTAGE:"):
                            wine_info["vintage"] = _wl.split(":", 1)[1].strip()
                        elif _wu.startswith("REGION:"):
                            wine_info["region"] = _wl.split(":", 1)[1].strip()
                        elif _wu.startswith("GRAPE:"):
                            wine_info["grape"] = _wl.split(":", 1)[1].strip()
                    if wine_info.get("name"):
                        _wt = wine_info["name"]
                        if wine_info.get("vintage") and wine_info["vintage"].upper() != "NV":
                            _wt += f" {wine_info['vintage']}"
                        title = f"🍷 {_wt[:60]}"
                    _vivino_q = " ".join(filter(None, [wine_info.get("name", ""), wine_info.get("vintage", "")]))
                    if _vivino_q:
                        _viv = _vivino_lookup(_vivino_q)
                        wine_info["vivino_rating"] = _viv.get("rating")
                        wine_info["vivino_count"] = _viv.get("rating_count")
                        wine_info["vivino_url"] = _viv.get("url", "")
                        print(f"[vivino] {_vivino_q!r} → {_viv}")
            except Exception as _we:
                print(f"[vision] wine extraction failed: {_we}")

        # ── Reverse-geocode stored GPS to UK postcode ────────────────────────────
        gps_location = ""
        if _loc and (time.time() - _loc.get("ts", 0)) < 7200:
            try:
                _rg = requests.get(
                    "https://api.postcodes.io/postcodes",
                    params={"lon": _loc["lon"], "lat": _loc["lat"], "limit": 1},
                    timeout=5,
                )
                _rg_data = _rg.json().get("result") or []
                if _rg_data:
                    _pc = _rg_data[0]
                    gps_location = f"{_pc.get('admin_ward','')}, {_pc.get('postcode','')}, {_pc.get('admin_district','')}".strip(", ")
            except Exception:
                pass

        # Build meta line: when + where
        import datetime as _dt
        now_str = _dt.datetime.now().strftime("%-d %b %Y, %-I:%M %p")
        # Prefer real GPS postcode over vision model's guess (which can be wrong/US coords)
        where_str = gps_location or (venue_info.get("address", "").split(",")[0] if venue_info else "") or location_tag or venue_tag
        meta_line = f"META:📅 {now_str}" + (f" · 📍 {where_str}" if where_str else "")
        # For product photos, store structured JSON + readable bullets
        if img_type == "product" and product_items:
            import json as _json
            _prod_json = _json.dumps(product_items, ensure_ascii=False)
            _shop_line = f"\nSHOP:{shop_tag}" if shop_tag else ""
            _bullets = "\n".join(
                "• " + _pi["name"]
                + (f" · {_pi['brand']}" if _pi["brand"] and _pi["brand"].lower() not in _pi["name"].lower() else "")
                + (f" · {_pi['price']}" if _pi["price"] else "")
                for _pi in product_items
            )
            summary = f"PRODUCTS:{_prod_json}{_shop_line}\n{_bullets}"
        full_summary = (summary + "\n\n" + menu_text).strip() if menu_text else summary
        summary_with_meta = meta_line + "\n" + full_summary if full_summary else meta_line

        if sid:
            try:
                update_data = {"title": title, "summary": summary_with_meta}
                if search_url:
                    update_data["url"] = search_url
                lib._sb().table("wa_saves").update(update_data).eq("id", sid).execute()
            except Exception:
                pass

        # Start a menu session so subsequent menu photos get appended
        if img_type == "menu" and sid:
            _MENU_SESSION[fn] = {"save_id": sid, "expires": time.time() + 900, "page_count": 1}

        bullets = "\n".join(f"• {b.strip()}" for b in summary.split("•") if b.strip())
        if product_items or bullets or venue_info or brand_intel or menu_text:
            msg = f"{title}\n"
            if img_type == "product" and product_items:
                # Structured product list
                _plines = []
                for _pi in product_items:
                    _plines.append(f"• *{_pi['name']}*")
                    if _pi["brand"]:
                        _plines.append(f"  Brand: {_pi['brand']}")
                    if _pi["price"]:
                        _plines.append(f"  Price: {_pi['price']}")
                msg += "\n" + "\n".join(_plines)
            elif menu_text:
                msg += f"\n{menu_text}"
            elif bullets:
                msg += f"\n{bullets}"

            # Build details block
            details = []
            if img_type == "product":
                # Location / shop context
                _spotted = (shop_tag or "") + (" · " + (_loc_context.split(",")[0] if _loc_context else location_tag) if (_loc_context or location_tag) else "")
                if _spotted.strip(" ·"):
                    details.append(f"🏪 Spotted at: {_spotted.strip(' ·')}")
                elif gps_location:
                    details.append(f"📍 {gps_location}")
                # Brand intel (first brand only)
                if brand_intel.get("parent"):
                    details.append(f"🏢 Made by: {brand_intel['parent']}")
                if brand_intel.get("country"):
                    details.append(f"🌍 Origin: {brand_intel['country']}")
                if brand_intel.get("fact"):
                    details.append(f"💡 {brand_intel['fact']}")
                if search_url:
                    details.append(f"🔍 Search: {search_url}")
            elif img_type == "wine":
                if wine_info.get("winery"):  details.append(f"🏚️ {wine_info['winery']}")
                if wine_info.get("region"):  details.append(f"🌍 {wine_info['region']}")
                if wine_info.get("grape"):   details.append(f"🍇 {wine_info['grape']}")
                if wine_info.get("vivino_rating"):
                    _vr = wine_info["vivino_rating"]
                    _vc = wine_info.get("vivino_count") or 0
                    _vc_str = f" ({_vc:,} ratings)" if _vc else ""
                    details.append(f"⭐ Vivino: {_vr:.1f}/5{_vc_str}")
                if wine_info.get("vivino_url"):
                    details.append(f"🔗 {wine_info['vivino_url']}")
            elif img_type == "store" and venue_info:
                # Full place card — address, hours, open status, phone, website, directions
                if gps_location:
                    details.append(f"📍 {gps_location}")
                elif venue_info.get("address"):
                    details.append(f"📍 {venue_info['address'].split(',')[0]}")
                if venue_info.get("open_now") is not None:
                    status = "🟢 Open now" if venue_info["open_now"] else "🔴 Closed now"
                    hours = f" · {venue_info['hours_today']}" if venue_info.get("hours_today") else ""
                    details.append(f"{status}{hours}")
                elif venue_info.get("hours_today"):
                    details.append(f"🕐 Today: {venue_info['hours_today']}")
                if venue_info.get("rating"):
                    stars = int(round(venue_info["rating"]))
                    details.append(f"{'⭐'*stars} {venue_info['rating']} ({venue_info['rating_count']:,} reviews)")
                if venue_info.get("phone"):
                    details.append(f"📞 {venue_info['phone']}")
                if venue_info.get("website"):
                    details.append(f"🌐 {venue_info['website']}")
                if venue_info.get("maps_url"):
                    details.append(f"🗺️ Directions: {venue_info['maps_url']}")
            else:
                # Event/ad/other — QR link, phone, website, search
                if qr_url:
                    details.append(f"📲 QR link: {qr_url}")
                if venue_info.get("phone"):
                    details.append(f"📞 {venue_info['phone']}")
                if venue_info.get("website"):
                    details.append(f"🌐 {venue_info['website']}")
                if search_url and search_url != qr_url:
                    link_label = "🎟️ Book/search" if img_type == "event" else "🔍 Search"
                    details.append(f"{link_label}: {search_url}")
                if venue_info.get("maps_url"):
                    details.append(f"📍 Directions: {venue_info['maps_url']}")

            if details:
                msg += "\n\n" + "\n".join(details)
        else:
            msg = f"{title}\n(couldn't read — saved anyway)"
        user_token = _wa_user_token(fn)
        if img_type == "menu":
            msg += f"\n\n📸 *Got page 1!* Send more menu photos and I'll stitch them together into one save.\n\n📂 My Saves: miru.humanagency.co/?screen=saves&token={user_token}"
        elif img_type == "wine":
            _WINE_RATING_STATE[fn] = {"save_id": sid, "wine_name": title.replace("🍷 ", ""), "expires": time.time() + 3600}
            msg += f"\n\n📂 My Saves: miru.humanagency.co/?screen=saves&token={user_token}"
            msg += f"\n\n⭐ *Rate it?* Reply with score + notes, e.g.\n*4 Great with pasta, slightly dry*"
        else:
            msg += f"\n\n📂 My Saves: miru.humanagency.co/?screen=saves&token={user_token}"
        _wa_send_proactive(fn, msg)

    # ── ISBN barcode detection — short-circuit vision pipeline for books ────────
    _isbn_hit = _try_isbn_from_image(r.content)
    if _isbn_hit is _ISBN_BARCODE_FAILED:
        user_token = _wa_user_token(from_number)
        _wa_send_proactive(from_number,
            "📚 *Couldn't read the barcode* — try again with the barcode filling most of the frame, in good light.\n\n"
            "Or send the book title as a message and I'll find it.")
        return "📚 Barcode not readable."
    if _isbn_hit:
        try:
            url = f"book:{_isbn_hit['isbn']}"
            lib._sb().table("wa_saves").update({
                "url":     url,
                "title":   _isbn_hit.get("title", "Book"),
                "summary": json.dumps(_isbn_hit),
                "status":  "wishlist",
            }).eq("id", save_id).execute()
        except Exception as _be:
            print(f"[isbn] save update failed: {_be}")
        user_token = _wa_user_token(from_number)
        msg = f"📚 *{_isbn_hit.get('title','Unknown title')}*"
        if _isbn_hit.get("author"):
            msg += f"\nby {_isbn_hit['author']}"
        cr = _isbn_hit.get("communityRating")
        if cr and cr.get("avg"):
            stars = "⭐" * round(cr["avg"])
            msg += f"\n{stars} {cr['avg']}/5"
            if cr.get("count"): msg += f" ({cr['count']:,} ratings)"
        if _isbn_hit.get("description"):
            msg += f"\n\n_{_isbn_hit['description'][:220].strip()}…_"
        msg += f"\n\n📚 Added to My Books: miru.humanagency.co/?screen=scan&token={user_token}"
        msg += f"\n_ISBN: {_isbn_hit['isbn']}_"
        _wa_send_proactive(from_number, msg)
        return "📚 Book scanned!"

    def _bg_safe():
        try:
            _bg()
        except Exception as _bge:
            app.logger.error(f"[vision] background thread crashed: {_bge}", exc_info=True)
            # Best-effort fallback: tell user it was saved even if analysis failed
            try:
                user_token = _wa_user_token(from_number)
                _wa_send_proactive(from_number,
                    f"📷 Saved your photo (analysis hit an error).\n"
                    f"📂 My Saves: miru.humanagency.co/?screen=saves&token={user_token}")
            except Exception:
                pass
    threading.Thread(target=_bg_safe, daemon=True).start()
    return "📷 Got it — reading your photo now…"


def _wa_send_proactive(to: str, body: str) -> None:
    """Send an outbound WhatsApp message via Twilio (fire-and-forget)."""
    try:
        from twilio.rest import Client as _TC
        sid   = os.environ.get("TWILIO_ACCOUNT_SID", "")
        token = os.environ.get("TWILIO_AUTH_TOKEN", "")
        frm   = os.environ.get("TWILIO_WHATSAPP_FROM", "")
        if not (sid and token and frm):
            print(f"[proactive] skipped — missing Twilio env vars (sid={bool(sid)} token={bool(token)} frm={bool(frm)})")
            return
        # Normalise FROM — strip any whatsapp: prefix before adding it back
        frm_clean = frm.replace("whatsapp:", "").strip()
        msg = _TC(sid, token).messages.create(
            body=body, from_=f"whatsapp:{frm_clean}", to=to
        )
        print(f"[proactive] sent to={to} sid={msg.sid}")
    except Exception as _e:
        print(f"[proactive] FAILED to={to} error={_e}")


def _wa_doc_search(from_number: str, query: str) -> str:
    """Answer a natural-language query from the user's saves and documents."""
    try:
        # Search wa_saves via Algolia (per-user)
        save_hits = lib.saves_search(query, from_number=from_number, hits_per_page=6)
        # Search library docs
        doc_hits = lib.search_library(query, n=3)

        context_parts = []
        for h in save_hits:
            title   = h.get("title", "")
            summary = (h.get("summary", "") or "")[:500]
            date    = (h.get("created_at", "") or "")[:10]
            if summary:
                context_parts.append(f"[{date}] {title}: {summary}")
        for h in doc_hits:
            title   = h.get("title", "")
            content = (h.get("content", "") or "")[:500]
            if content:
                context_parts.append(f"DOC '{title}': {content}")

        if not context_parts:
            return "🔍 Nothing found in your saves or documents for that query."

        context = "\n\n".join(context_parts[:8])
        answer = _groq_chat(
            "You are a personal assistant. Answer the user's question using ONLY the saved content provided. "
            "Be specific and concise. If purchase date or price is mentioned, state it clearly. "
            "If you can't find the answer say so briefly.",
            [{"role": "user", "content": f"Question: {query}\n\nSaved content:\n{context}"}],
            max_tokens=220,
        )
        return f"🔍 {answer.strip()}" if answer else "Couldn't find a clear answer in your saves."
    except Exception as _e:
        print(f"[wa_doc_search] {_e}")
        return "Sorry, couldn't search your saves right now."


def _wa_save_url(from_number: str, url: str) -> str:
    """Save URL immediately, summarise in background, send proactive follow-up."""
    import threading

    # 1. Save bare URL to DB right away so it's never lost
    save_id = None
    try:
        row = lib._sb().table("wa_saves").insert({
            "from_number": from_number,
            "url":         url,
            "title":       url,   # placeholder until background fills it
            "summary":     "",
            "status":      "pending",
        }).execute()
        if row.data:
            save_id = row.data[0].get("id")
    except Exception:
        return "⚠️ Couldn't save — database error. Try again."

    # Seed pending triage immediately (title = url for now)
    _PENDING_TRIAGE[from_number] = {
        "id": save_id, "title": url, "url": url,
        "expires": time.time() + 3600,
    }

    # 2. Background: fetch → summarise → update DB → proactive WA reply
    def _bg(sid=save_id, fn=from_number, u=url):
        fetched = _fetch_url_text(u)
        title   = fetched.get("title") or u
        text    = fetched.get("text", "")

        # Detect content type from URL domain first, then title/text
        _RECIPE_DOMAINS = ("allrecipes.com","bbc.co.uk/food","bbc.co.uk/recipe","jamieoliver.com",
                           "food.com","delicious.com","taste.com","bonappetit.com","epicurious.com",
                           "recipetineats.com","nigella.com","seriouseats.com","thespruceeats.com",
                           "yummly.com","cookinglight.com","simplyrecipes.com","delish.com")
        _VIDEO_DOMAINS  = ("youtube.com","youtu.be","vimeo.com","dailymotion.com","tiktok.com")
        _PRODUCT_DOMAINS= ("amazon.co.uk/dp","amazon.com/dp","ebay.co.uk/itm","ebay.com/itm",
                           "asos.com/prd","johnlewis.com","currys.co.uk","argos.co.uk")

        import re as _re
        u_lower = u.lower()
        if any(d in u_lower for d in _RECIPE_DOMAINS):
            content_type = "recipe"
        elif any(d in u_lower for d in _VIDEO_DOMAINS):
            content_type = "video"
        elif any(d in u_lower for d in _PRODUCT_DOMAINS):
            content_type = "product"
        elif text:
            # Ask Groq to classify if domain isn't conclusive
            try:
                ct = _groq_chat(
                    system="Reply with ONLY one word: recipe, video, product, event, or article.",
                    messages=[{"role": "user", "content": f"What type of content is this?\nTitle: {title}\n\n{text[:800]}"}],
                    max_tokens=5,
                ).strip().lower()
                content_type = ct if ct in ("recipe","video","product","event") else "article"
            except Exception:
                content_type = "article"
        else:
            content_type = "article"

        _TYPE_EMOJI = {"recipe":"🍳","video":"📺","product":"📦","event":"🎫","article":""}
        emoji = _TYPE_EMOJI.get(content_type, "")
        if emoji and not title.startswith(emoji):
            title = f"{emoji} {title}"

        summary = ""
        if text:
            try:
                summary = _groq_chat(
                    system=(
                        "Summarise the article in exactly 3 bullet points starting with •. "
                        "Max 15 words each. No intro, no outro."
                    ),
                    messages=[{"role": "user", "content": f"Summarise:\n\n{text[:4000]}"}],
                    max_tokens=120,
                ).strip()
            except Exception:
                pass

        # Update DB with real title + summary
        update = {"title": title}
        if summary:
            update["summary"] = summary
        try:
            lib._sb().table("wa_saves").update(update).eq("id", sid).execute()
            rows = lib._sb().table("wa_saves").select("*").eq("id", sid).execute().data
            if rows:
                lib.saves_sync(rows[0])
        except Exception:
            pass

        # Refresh pending triage with real title
        _PENDING_TRIAGE[fn] = {
            "id": sid, "title": title, "url": u,
            "expires": time.time() + 3600,
        }

        # Send proactive summary message
        if summary:
            bullets = "\n".join(f"• {b.strip()}" for b in summary.split("•") if b.strip())
            msg = f"📖 {title[:70]}\n\n{bullets}"
        else:
            msg = f"📖 {title[:70]}\n(couldn't load summary — tap to read)"
        _wa_send_proactive(fn, msg)

    threading.Thread(target=_bg, daemon=True).start()
    token = _wa_user_token(from_number)
    return f"📌 Saved — summary on its way ✨\n📂 My Saves: miru.humanagency.co/?screen=saves&token={token}"


def _wa_triage_respond(from_number: str, cmd: str) -> str:
    """Handle READ / SKIP / REMIND reply."""
    pending = _PENDING_TRIAGE.get(from_number)
    if not pending or time.time() > pending.get("expires", 0):
        return "No recent save to update. Send a URL to save something first."

    save_id = pending.get("id")
    title = pending.get("title", "item")[:60]
    cmd_up = cmd.strip().upper()

    url = pending.get("url", "")
    if cmd_up == "READ":
        status, remind_day, reply = "read", None, f"✓ Marked as read: {title}\n{url}"
    elif cmd_up == "SKIP":
        status, remind_day, reply = "skip", None, f"🗑️ Dismissed: {title}"
    elif cmd_up.startswith("REMIND"):
        day = cmd_up.replace("REMIND", "").strip() or "later"
        status, remind_day, reply = "remind", day, f"🔔 Reminder ({day}):\n{title}\n{url}"
    else:
        return "Reply READ, SKIP, or REMIND [day] e.g. REMIND friday"

    if save_id:
        try:
            update = {"status": status}
            if remind_day:
                update["remind_day"] = remind_day
            lib._sb().table("wa_saves").update(update).eq("id", save_id).execute()
        except Exception:
            pass

    _PENDING_TRIAGE.pop(from_number, None)
    return reply


_FUEL_WORDS = {"petrol", "diesel", "unleaded", "fuel", "gas", "price", "prices", "cheapest", "nearest", "mile", "miles", "mi"} | {r.lower() for r in KNOWN_RETAILERS}
_ELECTION_WORDS = {"vote", "voting", "election", "elections", "candidate", "candidates",
                   "polling", "ballot", "stand", "standing", "mp"}
_COUNCILLOR_WORDS = {"councillor", "councilor", "council rep", "my councillor", "contact councillor"}
_RESULTS_WORDS  = {"results", "result", "winner", "won", "elected", "who won"}
_PLACES_WORDS = {"places", "services", "local", "near", "nearby", "around",
                 "library", "gp", "doctor", "pharmacy", "dentist", "leisure",
                 "gym", "pool", "community", "postoffice", "council", "park"}


_SERVICE_SYNONYMS = {
    "gp":         ["doctor", "gp", "surgery", "medical centre", "health centre", "clinic"],
    "doctor":     ["doctor", "gp", "surgery", "medical", "clinic"],
    "pharmacy":   ["pharmacy", "chemist"],
    "dentist":    ["dentist", "dental"],
    "hospital":   ["hospital"],
    "library":    ["library"],
    "gym":        ["gym", "fitness", "leisure"],
    "park":       ["park", "garden", "recreation"],
    "pub":        ["pub", "bar", "inn"],
    "cafe":       ["cafe", "coffee", "tea room"],
    "restaurant": ["restaurant", "food", "dining"],
    "school":     ["school", "academy", "college"],
    "post":       ["post office"],
    "police":     ["police"],
}


def whatsapp_places_format(q: str, service_filter: str = "") -> str:
    """Format local places info for WhatsApp reply, optionally filtered by service type."""
    try:
        geo = _geocode_place(q)
        if not geo or geo[0] is None:
            return f"Couldn't find '{q}'. Try a postcode or town name — e.g. places KT1 2BA"
        lat, lon, display = geo[0], geo[1], geo[2]
        places = _overpass_places(lat, lon)

        # Apply service filter if provided
        sf = service_filter.lower().strip()
        filter_words = None
        if sf:
            for key, synonyms in _SERVICE_SYNONYMS.items():
                if key in sf:
                    filter_words = synonyms
                    break
            if filter_words is None:
                filter_words = [w for w in sf.split() if len(w) > 2]

        filtered_places = places
        if filter_words and places:
            filtered = [
                p for p in places
                if any(fw in p.get("name", "").lower() or fw in p.get("category", "").lower()
                       for fw in filter_words)
            ]
            if filtered:
                filtered_places = filtered
            # else fall through to show everything

        # Google Places fallback for sparse OSM areas or specific service queries
        if not filtered_places and _GOOGLE_PLACES_KEY:
            gq = f"{sf or 'local services'} near {display}" if display else (sf or q)
            raw = _gplaces_text_search(gq, lat, lon, radius=5000)
            seen_g = set()
            gp_lines = []
            for p in raw[:8]:
                name = p.get("name", "")
                if not name or name.lower() in seen_g:
                    continue
                seen_g.add(name.lower())
                addr = p.get("formatted_address") or p.get("vicinity") or ""
                # Trim address to local part
                addr_short = ", ".join(addr.split(",")[:2]) if addr else ""
                line = f"🩺 {name}" if "doctor" in gq.lower() or "gp" in gq.lower() else f"📍 {name}"
                if addr_short:
                    line += f"\n  📍 {addr_short}"
                gp_lines.append(line)
            if gp_lines:
                header = f"🏛️ {sf.title() if sf else 'Local services'} near {display}"
                return header + "\n\n" + "\n\n".join(gp_lines) + f"\n\n📍 miru.humanagency.co"

        if not filtered_places:
            return f"No local services found near {display}. Try a different postcode or area."

        from itertools import groupby
        header = f"🏛️ {sf.title() if sf else 'Local services'} near {display}\n" if sf else f"🏛️ Local services near {display}\n"
        lines = [header]
        count = 0
        for cat, group in groupby(filtered_places, key=lambda p: p["category"]):
            items = list(group)[:2]
            for p in items:
                if count >= 12:
                    break
                hours = p["hours"][:40] if p["hours"] else ""
                phone = p["phone"] or ""
                line = f"{p['emoji']} {p['name']}"
                if hours:
                    import re as _re
                    hours_short = _re.sub(r'[A-Z][a-z]-[A-Z][a-z]', lambda m: m.group(), hours)
                    line += f"\n  🕐 {hours_short}"
                if phone:
                    line += f"\n  📞 {phone}"
                lines.append(line)
                count += 1

        lines.append(f"\n📍 Within 900m · miru.humanagency.co")
        return "\n\n".join(lines)
    except Exception:
        return "Sorry, couldn't load local services. Try miru.app instead."


def whatsapp_elections_format(postcode: str) -> str:
    """Format election info for WhatsApp reply."""
    try:
        pc = postcode.replace(" ", "").upper()
        r = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=6)
        if r.status_code != 200:
            return f"Couldn't find postcode {postcode}. Please check and try again."
        result   = r.json().get("result", {})
        codes    = result.get("codes", {})
        ward_gss      = codes.get("admin_ward", "")
        ward_name     = result.get("admin_ward", "")
        district      = result.get("admin_district", "")
        district_code = codes.get("admin_district", "")
        county_code   = codes.get("admin_county", "")
        ua_code       = district_code

        elections  = _get_elections()
        ward_data  = elections["by_gss"].get(ward_gss, {})
        council_slug = None
        if not ward_data:
            council_slug = (
                _DISTRICT_TO_COUNCIL_SLUG.get(district_code) or
                _UA_TO_COUNCIL_SLUG.get(ua_code) or
                _COUNTY_TO_COUNCIL_SLUG.get(county_code)
            )
            if council_slug:
                slug_wards = elections["by_slug"].get(council_slug, {})
                best = _best_ward_match(ward_name, slug_wards)
                if best:
                    ward_data = slug_wards[best]

        if not ward_data or not ward_data.get("candidates"):
            return (f"No local elections found for {postcode.upper()} on 7 May 2026.\n"
                    "Your area may not be holding elections this year.")

        candidates = sorted(ward_data["candidates"], key=lambda c: (c["party"], c["name"]))
        ward    = ward_data.get("ward") or ward_name
        council = ward_data.get("council") or district
        date    = ward_data.get("election_date", "2026-05-07")

        # Polling station
        ps = _fetch_polling_station(pc)
        postcode_fmt = f"{pc[:-3]} {pc[-3:]}"
        if ps and not ps.get("not_available"):
            ps_line = f"\n\n📍 Polling station:\n{ps['name']}\n{ps['address']}"
        elif ps and ps.get("not_available"):
            phone = f" · Call {ps['phone']}" if ps.get("phone") else ""
            ps_line = f"\n\n📍 Polling station:\nNot yet uploaded by council{phone}\nCheck your poll card when it arrives"
        else:
            ps_line = f"\n\n📍 Find polling station:\nhttps://wheredoivote.co.uk/postcode/{pc}/"

        # Build candidates block — group by party
        from itertools import groupby
        cand_lines = []
        for party, group in groupby(candidates, key=lambda c: c["party"]):
            names = [c["name"] for c in group]
            cand_lines.append(f"• {party}: {', '.join(names)}")

        return (
            f"🗳️ Local Elections — 7 May 2026\n"
            f"Ward: {ward}\n"
            f"{council}{ps_line}\n\n"
            f"Candidates:\n" + "\n".join(cand_lines) +
            "\n\nVoting hours: 7am–10pm"
            "\n🔗 miru.humanagency.co/elections"
        )
    except Exception as e:
        return f"Sorry, couldn't load election info. Try miru.app instead."

def _wa_councillor_lookup(postcode: str) -> str:
    """Return councillor contact info for a postcode via WhatsApp."""
    try:
        pc = postcode.replace(" ", "").upper()
        d = _resolve_councillors(pc)
        councillors = d.get("councillors", [])
        ward   = d.get("ward", "")
        council = d.get("council", "")

        if not councillors:
            return (
                f"🏛️ *{ward}* — {council}\n\n"
                f"No elected councillor data yet for this ward.\n"
                f"Results may still be being counted.\n\n"
                f"Check back soon or visit your council's website."
            )

        lines = [f"🏛️ *Your Councillor{'s' if len(councillors)>1 else ''}*\n{ward} · {council}\n"]
        for c in councillors:
            col = _partyColourEmoji(c.get("party",""))
            lines.append(f"{col} *{c['name']}* — {c.get('party','')}")
            if c.get("email"):
                lines.append(f"📧 {c['email']}")
            if c.get("phone"):
                lines.append(f"📞 {c['phone']}")
            if c.get("surgery_info"):
                lines.append(f"📅 Surgery: {c['surgery_info']}")
            if c.get("council_profile_url"):
                lines.append(f"🔗 {c['council_profile_url']}")
            lines.append("")

        lines.append(f"📱 miru.humanagency.co/?screen=elections&postcode={postcode}")
        return "\n".join(lines).strip()
    except Exception as e:
        app.logger.warning(f"[councillor-wa] {e}")
        return "⚠️ Couldn't load councillor info. Try again shortly."


def _partyColourEmoji(party: str) -> str:
    party_l = party.lower()
    if "labour" in party_l:      return "🔴"
    if "conservative" in party_l: return "🔵"
    if "lib dem" in party_l:     return "🟡"
    if "green" in party_l:       return "🟢"
    if "reform" in party_l:      return "🩵"
    if "snp" in party_l:         return "🟡"
    if "plaid" in party_l:       return "🟢"
    return "⚫"


def whatsapp_results_format(postcode: str) -> str:
    """Return declared election results for a postcode via WhatsApp."""
    try:
        pc = postcode.replace(" ", "").upper()
        r = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=6)
        if r.status_code != 200:
            return f"Couldn't find postcode {postcode}. Please check and try again."
        result       = r.json().get("result", {})
        codes        = result.get("codes", {})
        ward_gss     = codes.get("admin_ward", "")
        ward_name    = result.get("admin_ward", "")
        district     = result.get("admin_district", "")
        district_code = codes.get("admin_district", "")
        ua_code      = district_code
        county_code  = codes.get("admin_county", "")

        elections  = _get_elections()
        ward_data  = elections["by_gss"].get(ward_gss, {})
        council_slug = None
        if not ward_data:
            council_slug = (
                _DISTRICT_TO_COUNCIL_SLUG.get(district_code) or
                _UA_TO_COUNCIL_SLUG.get(ua_code) or
                _COUNTY_TO_COUNCIL_SLUG.get(county_code)
            )
            if council_slug:
                slug_wards = elections["by_slug"].get(council_slug, {})
                best = _best_ward_match(ward_name, slug_wards)
                if best:
                    ward_data = slug_wards[best]

        if not ward_data:
            return (f"No elections found for {pc} this year.\n"
                    "Your area may not be holding local elections in 2026.")

        ward    = ward_data.get("ward") or ward_name
        council = ward_data.get("council") or district
        date    = ward_data.get("election_date", "2026-05-07")

        # Try DC results API — returns a list of candidate dicts
        dc_results = _fetch_dc_results(council_slug or _org_name_to_dc_slug(council), ward, date)
        has_real = dc_results and any(c.get("votes") is not None or c.get("elected") for c in dc_results)
        if has_real:
            cands = sorted(dc_results, key=lambda c: -(c.get("votes") or 0))
            lines = []
            for c in cands:
                tick = "✅ " if c.get("elected") else "   "
                votes = f"  {c['votes']:,}" if c.get("votes") is not None else ""
                party = (c.get("party") or c.get("party_name") or "")[:14]
                lines.append(f"{tick}{c['name']} ({party}){votes}")
            declared = any(c.get("elected") for c in cands)
            header = "🗳️ Result declared" if declared else "🗳️ Counting in progress"
            return (
                f"{header}\n"
                f"Ward: {ward}\n"
                f"{council}\n\n"
                + "\n".join(lines) +
                "\n\n🔗 miru.humanagency.co"
            )

        # No results yet — fall back to candidates
        candidates = sorted(ward_data.get("candidates", []), key=lambda c: c["party"])
        if not candidates:
            return f"Results not yet declared for {ward}, {council}. Check back later."

        cand_lines = [f"• {c['name']} ({c['party']})" for c in candidates]
        return (
            f"⏳ Results not yet declared\n"
            f"Ward: {ward}\n"
            f"{council}\n\n"
            f"Candidates standing:\n" + "\n".join(cand_lines) +
            "\n\nCheck back later for the result.\n🔗 miru.humanagency.co"
        )
    except Exception as e:
        app.logger.error(f"whatsapp_results_format: {e}")
        return (
            f"⏳ Result not yet declared for {postcode.upper()}\n\n"
            "Counting is still underway. Try again in a few minutes or check:\n"
            "🔗 miru.humanagency.co"
        )


# ── Food & drink discovery ───────────────────────────────────────────────────


_FOOD_CHAINS = frozenset({
    "starbucks", "costa", "costa coffee", "pret", "pret a manger",
    "greggs", "mcdonalds", "mcdonald's", "subway", "kfc", "burger king",
    "nando's", "nandos", "wagamama", "itsu", "leon", "pod", "pure",
    "caffe nero", "caffè nero", "nero", "wetherspoons", "wetherspoon",
    "j d wetherspoon", "jd wetherspoon", "pizza hut", "domino's",
    "dominoes", "dominos", "five guys", "pizza express", "bella italia",
    "ask italian", "harvester", "toby carvery", "the real greek",
})

_FOOD_INTENTS = [
    (re.compile(r'\b(coffee|cafe|café|flat white|cappuccino|capuccino|espresso|latte|americano|cortado|mocha|pistachio latte|cofee)\b', re.I),
     {"type": "cafe", "keyword": "coffee", "emoji": "☕", "label": "coffee",
      "required_types": ["cafe", "bakery"],
      "review_terms": ["coffee", "latte", "cappuccino", "espresso", "flat white",
                       "americano", "cortado", "mocha", "cold brew", "barista", "oat milk"]}),
    (re.compile(r'\b(breakfast|brunch)\b', re.I),
     {"type": "cafe", "keyword": "breakfast", "emoji": "🍳", "label": "breakfast",
      "required_types": ["cafe", "bakery", "restaurant"],
      "review_terms": ["breakfast", "brunch", "eggs", "full english", "bacon",
                       "avocado", "toast", "granola", "pancake"]}),
    (re.compile(r'\b(sandwich|sarnie)\b', re.I),
     {"type": "restaurant", "keyword": "sandwich deli", "emoji": "🥪", "label": "sandwiches",
      "required_types": ["cafe", "bakery", "restaurant", "meal_takeaway", "food"],
      "review_terms": ["sandwich", "sourdough", "baguette", "ciabatta", "toastie", "deli", "roll"]}),
    (re.compile(r'\b(burger|burgers|smash burger|smash)\b', re.I),
     {"type": "restaurant", "keyword": "burger", "emoji": "🍔", "label": "burgers",
      "required_types": ["restaurant", "meal_takeaway", "food"], "always_best": True, "radius": 3000,
      "review_terms": ["burger", "smash", "patty", "brioche", "beef", "cheese", "fries", "loaded"]}),
    (re.compile(r'\b(kebab|kebabs|shawarma|doner)\b', re.I),
     {"type": "restaurant", "keyword": "kebab", "emoji": "🌯", "label": "kebab",
      "required_types": ["restaurant", "meal_takeaway", "food"],
      "review_terms": ["kebab", "doner", "shawarma", "lamb", "chicken", "wrap", "pita", "garlic sauce"]}),
    (re.compile(r'\b(steak|steakhouse|ribeye|sirloin)\b', re.I),
     {"type": "restaurant", "keyword": "steak", "emoji": "🥩", "label": "steak",
      "required_types": ["restaurant", "food"], "always_best": True, "radius": 5000,
      "review_terms": ["steak", "ribeye", "sirloin", "fillet", "medium rare", "wagyu", "sauce", "sides"]}),
    (re.compile(r'\b(indian|india\b|curry|curries|balti|tandoor|tandoori|masala|tikka|biryani|korma|vindaloo|dal|dhal|bhaji|naan|poppadom)\b', re.I),
     {"type": "restaurant", "keyword": "indian restaurant", "emoji": "🍛", "label": "Indian food",
      "required_types": ["restaurant", "meal_takeaway", "food"], "radius": 3000,
      "review_terms": ["curry", "indian", "tandoor", "tikka", "biryani", "masala", "korma", "naan", "spice", "dal"]}),
    (re.compile(r'\b(italian|italy\b|pasta|risotto|trattoria|osteria|ristorante|lasagne|lasagna|ravioli|carbonara|amatriciana|gnocchi|tiramisu)\b', re.I),
     {"type": "restaurant", "keyword": "italian restaurant", "emoji": "🍝", "label": "Italian",
      "required_types": ["restaurant", "meal_takeaway", "food"], "radius": 3000,
      "review_terms": ["italian", "pasta", "pizza", "risotto", "tiramisu", "fresh", "homemade", "authentic", "wine"]}),
    (re.compile(r'\b(mexican|mexico\b|taco|tacos|burrito|burritos|quesadilla|enchilada|guacamole|tex.?mex|nachos|fajita|jalape[nñ]o)\b', re.I),
     {"type": "restaurant", "keyword": "mexican restaurant", "emoji": "🌮", "label": "Mexican",
      "required_types": ["restaurant", "meal_takeaway", "food"], "radius": 3000,
      "review_terms": ["mexican", "taco", "burrito", "guacamole", "salsa", "spicy", "fresh", "authentic"]}),
    (re.compile(r'\b(fish[\s_&n]+chips?|chippy|chippie|chip\s+shop|fish\s+supper|fish\s+shop|cod|haddock)\b', re.I),
     {"type": "restaurant", "keyword": "fish and chips", "emoji": "🐟", "label": "fish & chips",
      "required_types": ["restaurant", "meal_takeaway", "food"], "radius": 2000,
      "review_terms": ["fish", "chips", "cod", "haddock", "batter", "crispy", "fresh", "mushy peas", "tartar"]}),
    (re.compile(r'\b(chinese|china food|dim sum|chow mein|peking|cantonese|szechuan|sichuan)\b', re.I),
     {"type": "restaurant", "keyword": "chinese restaurant", "emoji": "🥡", "label": "Chinese",
      "required_types": ["restaurant", "meal_takeaway", "food"], "radius": 3000,
      "review_terms": ["chinese", "dim sum", "wonton", "peking duck", "fried rice", "noodles", "spring roll", "dumpling"]}),
    (re.compile(r'\b(wine|wines|red wine|white wine|ros[eé]|house wine|bottle of wine|glass of wine)\b', re.I),
     {"type": "bar", "keyword": "wine bar", "emoji": "🍷", "label": "wine",
      "required_types": ["bar", "restaurant", "establishment"],
      "radius": 5000,
      "review_terms": ["wine", "red", "white", "rosé", "bottle", "glass", "selection", "sommelier", "natural wine"]}),
    (re.compile(r'\b(lunch|what.{0,25}(eat|have).{0,10}lunch)\b', re.I),
     {"type": "restaurant", "keyword": "", "emoji": "🥗", "label": "lunch",
      "required_types": ["restaurant", "cafe", "food"],
      "review_terms": ["lunch", "food", "meal", "menu", "tasty", "fresh", "delicious"]}),
    (re.compile(r'\b(pizza)\b', re.I),
     {"type": "restaurant", "keyword": "pizza", "emoji": "🍕", "label": "pizza",
      "required_types": ["restaurant", "meal_delivery", "food"],
      "review_terms": ["pizza", "dough", "crust", "toppings", "wood fired", "neapolitan"]}),
    (re.compile(r'\b(dinner|supper)\b', re.I),
     {"type": "restaurant", "keyword": "", "emoji": "🍽️", "label": "dinner",
      "required_types": ["restaurant", "food"],
      "review_terms": ["dinner", "food", "atmosphere", "service", "menu", "delicious", "evening"]}),
    (re.compile(r'\b(beer|pint|pub|ale|lager|craft beer)\b', re.I),
     {"type": "bar", "keyword": "pub", "emoji": "🍺", "label": "a pub",
      "required_types": ["bar", "night_club", "pub"],
      "review_terms": ["beer", "pint", "ale", "lager", "craft", "tap", "cask", "atmosphere", "garden"]}),
    (re.compile(r'\b(tea|english breakfast tea|chai|cuppa|brew)\b', re.I),
     {"type": "cafe", "keyword": "", "emoji": "🍵", "label": "tea",
      "required_types": ["cafe", "bakery", "restaurant"],
      "review_terms": ["tea", "chai", "english breakfast", "earl grey", "herbal", "pot of tea", "scone", "cake", "afternoon tea"]}),
]

_PRICE_LEVEL = {1: "£", 2: "££", 3: "£££", 4: "££££"}

_REVIEW_POSITIVES = frozenset([
    "great", "amazing", "excellent", "best", "lovely", "good", "awesome",
    "perfect", "fantastic", "brilliant", "superb", "nice", "wonderful",
    "incredible", "outstanding", "delicious", "exceptional", "top", "love",
])


def _find_food_nearby(lat: float, lon: float, place_type: str,
                      radius: int = 1500, cheap: bool = False,
                      keyword: str = "", required_types: list = None,
                      min_ratings: int = 100) -> list:
    key = os.environ.get("GOOGLE_PLACES_KEY", "")
    if not key:
        return []

    def _search(kw, rad):
        params = {"location": f"{lat},{lon}", "radius": rad,
                  "type": place_type, "key": key}
        if kw:
            params["keyword"] = kw
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params=params, timeout=10,
        )
        return r.json().get("results", [])

    try:
        results = _search(keyword, radius)
        # If fewer than 3 qualify after filtering, widen radius
        def _qualify(p):
            n = p.get("user_ratings_total", 0)
            if n < min_ratings:
                return False
            if required_types:
                p_types = p.get("types", [])
                if not any(t in p_types for t in required_types):
                    return False
            return True

        qualified = [p for p in results if _qualify(p)]
        if len(qualified) < 2:
            # Widen radius and relax min_ratings to 50
            results2 = _search(keyword, radius * 2)
            for p in results2:
                if p not in results:
                    results.append(p)
            qualified = [p for p in results if
                         p.get("user_ratings_total", 0) >= 50 and
                         (not required_types or any(t in p.get("types", []) for t in required_types))]

        items = []
        for p in qualified:
            loc = p.get("geometry", {}).get("location", {})
            plat, plon = loc.get("lat"), loc.get("lng")
            dist_km = haversine_km(lat, lon, plat, plon) if plat and plon else 999
            name = p.get("name", "")
            items.append({
                "name":        name,
                "place_id":    p.get("place_id", ""),
                "dist_km":     dist_km,
                "dist_mi":     round(dist_km * 0.621371, 1),
                "rating":      p.get("rating", 0),
                "n_ratings":   p.get("user_ratings_total", 0),
                "open_now":    p.get("opening_hours", {}).get("open_now"),
                "price_level": p.get("price_level"),
                "is_chain":    name.lower().strip() in _FOOD_CHAINS,
                "snippet":     "",
            })

        def _type_relevance(name, ptype, kw=""):
            """Score 0.1–1.0 for how well the place name matches the actual query."""
            n = name.lower()
            if ptype == "cafe":
                if kw == "tea":
                    # Tea search: tea rooms first, coffee shops pushed to the bottom
                    if any(w in n for w in ("tea", "tearoom", "tea room", "chai")):
                        return 1.0
                    if any(w in n for w in ("bakery", "patisserie", "deli", "garden", "house")):
                        return 0.75
                    if any(w in n for w in ("coffee", "cafe", "café", "espresso", "barista",
                                             "roast", "brew", "latte")):
                        return 0.25  # coffee shops are largely irrelevant for tea
                    if any(w in n for w in ("bistro", "bar", "pub", "grill", "brasserie",
                                             "restaurant", "hotel", "inn", "arms", "tavern")):
                        return 0.1
                    return 0.6
                else:
                    # Coffee/default search
                    if any(w in n for w in ("coffee", "cafe", "café", "espresso", "barista", "roast", "brew", "latte")):
                        return 1.0
                    if any(w in n for w in ("bakery", "patisserie", "tearoom", "tea room", "deli")):
                        return 0.85
                    if any(w in n for w in ("bistro", "bar", "pub", "grill", "brasserie",
                                             "restaurant", "hotel", "inn", "arms", "tavern", "kitchen")):
                        return 0.35
            if ptype == "restaurant":
                _ethnic = ("indian", "curry", "spice", "balti", "tandoor", "masala", "biryani",
                           "thai", "chinese", "sushi", "japanese", "dim sum", "noodle",
                           "pizza", "pizzeria", "pasta", "italian", "turkish", "lebanese",
                           "persian", "arabic", "bangladeshi", "pakistani", "sri lanka",
                           "vietnamese", "korean", "mexican", "tapas", "spanish")
                if kw == "steak":
                    if any(w in n for w in ("steak", "steakhouse", "chophouse", "chop house", "ribeye", "sirloin", "wagyu")):
                        return 1.0
                    if any(w in n for w in ("brasserie", "butcher", "prime", "miller", "carter")):
                        return 0.9
                    if "grill" in n and not any(w in n for w in ("smash", "burger", "chicken", "halal", "kebab", "fried")):
                        return 0.75  # generic grill — might have steak
                    if any(w in n for w in ("smash", "burger", "fried chicken", "halal")):
                        return 0.05  # burger/fast food joints, not steakhouses
                    if any(w in n for w in _ethnic):
                        return 0.05
                    return 0.65
                if kw == "burger":
                    if any(w in n for w in ("burger", "smash", "shack", "joint", "patty", "grill")):
                        return 1.0
                    if any(w in n for w in _ethnic):
                        return 0.15
                    return 0.7
                if kw == "kebab":
                    if any(w in n for w in ("kebab", "shawarma", "doner", "turkish", "lebanese", "arabic", "persian", "grill")):
                        return 1.0
                    if any(w in n for w in ("indian", "curry", "balti", "tandoor")):
                        return 0.5  # some crossover
                    return 0.6
                if kw == "indian restaurant":
                    if any(w in n for w in ("indian", "india", "curry", "balti", "tandoor", "masala", "tikka",
                                            "biryani", "korma", "spice", "saffron", "mughul", "mogul", "bengali",
                                            "bangladeshi", "pakistani", "punjabi", "delhi", "bombay", "mumbai")):
                        return 1.0
                    if any(w in n for w in ("restaurant", "kitchen", "house", "palace", "raj", "maharaja")):
                        return 0.7
                    if any(w in n for w in ("pizza", "burger", "kebab", "chinese", "thai", "italian", "pub")):
                        return 0.05
                    return 0.5
                if kw == "italian restaurant":
                    if any(w in n for w in ("italian", "italy", "pizza", "pasta", "trattoria", "osteria",
                                            "ristorante", "pizzeria", "napoli", "naples", "rome", "sicily",
                                            "venice", "florence", "romano", "bella", "la dolce", "al dente")):
                        return 1.0
                    if any(w in n for w in ("cafe", "bistro", "kitchen")):
                        return 0.6
                    if any(w in n for w in ("indian", "burger", "kebab", "chinese", "thai", "pub")):
                        return 0.05
                    return 0.5
                if kw == "mexican restaurant":
                    if any(w in n for w in ("mexican", "mexico", "taco", "burrito", "quesadilla", "tex mex",
                                            "cantina", "hacienda", "taqueria", "jalisco", "oaxaca", "tijuana")):
                        return 1.0
                    if any(w in n for w in ("kitchen", "grill", "bar")):
                        return 0.6
                    if any(w in n for w in ("indian", "burger", "kebab", "chinese", "italian", "pub")):
                        return 0.05
                    return 0.5
                if kw == "fish and chips":
                    if any(w in n for w in ("fish", "chip", "chippy", "cod", "haddock", "fryer", "frier",
                                            "fishmonger", "seafood", "fry", "plaice", "fisherman")):
                        return 1.0
                    if any(w in n for w in ("burger", "kebab", "pizza", "chinese", "indian", "italian")):
                        return 0.05
                    return 0.4
                if kw == "chinese restaurant":
                    if any(w in n for w in ("chinese", "china", "dim sum", "peking", "canton", "hong kong",
                                            "szechuan", "sichuan", "mandarin", "beijing", "shanghai", "wonton")):
                        return 1.0
                    if any(w in n for w in ("noodle", "oriental", "asian", "dragon", "golden", "jade",
                                            "palace", "dynasty", "emperor", "bamboo", "lotus", "panda")):
                        return 0.85  # probably Chinese
                    if any(w in n for w in ("thai", "japanese", "korean", "vietnamese", "malaysian", "singaporean")):
                        return 0.3  # Asian but not Chinese
                    if any(w in n for w in ("indian", "curry", "pizza", "burger", "kebab", "pub", "italian")):
                        return 0.05
                    return 0.6
            if ptype == "bar":
                if kw == "wine bar":
                    if any(w in n for w in ("wine", "vino", "cellar", "cave", "vineyard", "winery", "sommelier")):
                        return 1.0
                    if any(w in n for w in ("bar", "bistro", "brasserie", "kitchen")):
                        return 0.75
                    if any(w in n for w in ("pub", "inn", "arms", "tavern", "sports")):
                        return 0.3
                if kw in ("pub", "beer"):
                    if any(w in n for w in ("pub", "inn", "arms", "tavern", "brewery", "tap", "ale", "beer")):
                        return 1.0
                    if any(w in n for w in ("bar", "bistro", "kitchen")):
                        return 0.8
                    if any(w in n for w in ("wine", "cocktail", "champagne", "prosecco")):
                        return 0.4
            return 0.75  # neutral / unknown

        if cheap:
            items.sort(key=lambda x: (x["price_level"] or 9, -x["rating"], x["dist_km"]))
        else:
            items.sort(key=lambda x: (
                -x["rating"] * _type_relevance(x["name"], place_type, keyword) * (1.0 if x["open_now"] is not False else 0.9),
                x["dist_km"],
            ))
        return items
    except Exception as e:
        print(f"[food_nearby] {e}")
        return []


def _places_review_snippet(place_id: str, keywords: list) -> str:
    """Return a highly-positive sentence from Google Places reviews matching any keyword."""
    key = os.environ.get("GOOGLE_PLACES_KEY", "")
    if not key or not place_id:
        return ""
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={"place_id": place_id, "fields": "reviews", "key": key},
            timeout=4,
        )
        reviews = r.json().get("result", {}).get("reviews", [])
        # Only use 4- and 5-star reviews
        reviews = [rv for rv in reviews if rv.get("rating", 0) >= 4]
        reviews.sort(key=lambda rv: -rv.get("rating", 0))

        best_score = 0
        best_sentence = ""

        for review in reviews:
            text = review.get("text", "").replace("\n", " ")
            # Split into sentences
            sentences = re.split(r'(?<=[.!?])\s+', text)
            for sent in sentences:
                sent_l = sent.lower()
                kw_hit = any(kw in sent_l for kw in keywords)
                if not kw_hit:
                    continue
                pos_hits = sum(1 for w in _REVIEW_POSITIVES if w in sent_l)
                if pos_hits == 0:
                    continue
                score = pos_hits + review.get("rating", 4) - 4
                if score > best_score and len(sent) <= 100:
                    best_score = score
                    best_sentence = sent.strip()

        if best_sentence:
            return f'"{best_sentence}"'
    except Exception:
        pass
    return ""



def _wa_food_find(body: str, from_number: str):
    """Return WhatsApp-formatted food/drink picks, or None if not a food query."""
    # Normalise typos: collapse 3+ repeated letters (coffeee→coffee, beeer→beer)
    body_lower = re.sub(r'(.)\1{2,}', r'\1\1', body.lower())

    intent = None
    for pattern, info in _FOOD_INTENTS:
        if pattern.search(body_lower):
            intent = info
            break
    if not intent:
        return None

    # Some categories (steak, burger) are always about quality — never sort by price
    wants_cheap = bool(re.search(r'\bcheap\b', body_lower)) and not intent.get("always_best")

    # Postcode from message — accept full (KT16 0DA) or outward-only (KT16)
    _full_pc  = re.search(r'[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}', body.upper())
    _out_pc   = re.search(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?)\b', body.upper())
    lat = lon = None
    pc_fmt = ""

    if _full_pc:
        postcode = _full_pc.group(0).replace(" ", "")
        try:
            r2 = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=5)
            if r2.status_code == 200:
                res = r2.json().get("result", {})
                lat, lon = res.get("latitude"), res.get("longitude")
                pc_fmt = postcode[:-3] + " " + postcode[-3:]
        except Exception:
            pass
    if lat is None and _out_pc:
        outcode = _out_pc.group(1)
        try:
            r2 = requests.get(f"https://api.postcodes.io/outcodes/{outcode}", timeout=5)
            if r2.status_code == 200:
                res = r2.json().get("result", {})
                lat, lon = res.get("latitude"), res.get("longitude")
                pc_fmt = outcode
        except Exception:
            pass
    _wants_live = bool(re.search(r'\b(here|near me|around here|where i am|current(ly)?)\b', body_lower))

    if lat is None:
        # Use recently shared live location if available (within 2 hours)
        _loc = _USER_LAST_LOCATION.get(from_number)
        if _loc and (time.time() - _loc.get("ts", 0)) < 7200:
            lat, lon = _loc["lat"], _loc["lon"]
            pc_fmt = "your location"

    if lat is None:
        # Fall back to stored home postcode (unless user explicitly wants live location)
        if not _wants_live:
            stored = _get_wa_home_postcode(from_number)
            if stored:
                try:
                    r2 = requests.get(f"https://api.postcodes.io/postcodes/{stored}", timeout=5)
                    if r2.status_code == 200:
                        res = r2.json().get("result", {})
                        lat, lon = res.get("latitude"), res.get("longitude")
                        pc_fmt = stored[:-3] + " " + stored[-3:]
                except Exception:
                    pass

    if lat is None:
        _set_wa_pending_intent(from_number, {
            "type": "food", "food_type": intent["label"], "cheap": wants_cheap
        })
        if _wants_live:
            return (f"{intent['emoji']} Drop your location pin and I'll find the best {intent['label']} right where you are.\n\n"
                    f"_(Tap the 📎 attachment icon → Location → Send current location)_")
        return (f"{intent['emoji']} Where are you?\n\n"
                f"Reply with your postcode or drop your location pin — I'll find the best {intent['label']} near you.\n"
                f"_(Or set your home postcode at miru.humanagency.co so you never need to send it)_")

    places = _find_food_nearby(lat, lon, intent["type"],
                               cheap=wants_cheap,
                               keyword=intent.get("keyword", ""),
                               required_types=intent.get("required_types", []),
                               radius=intent.get("radius", 1500))
    if not places:
        return f"No {intent['label']} spots found near {pc_fmt}. Sorry!"

    emoji, label = intent["emoji"], intent["label"]
    keywords = intent.get("review_terms", [])

    # Pick top and optional second before fetching reviews
    top = places[0]
    second = None
    if top["is_chain"]:
        indys = [p for p in places[1:] if not p["is_chain"] and p["rating"] >= 4.0]
        if indys:
            second = indys[0]
    elif len(places) > 1 and places[1]["rating"] >= 4.0 and places[1]["dist_km"] <= 1.0:
        second = places[1]

    # Fetch review snippets in parallel (max 4s total)
    # Only show snippet if the place name itself suggests it fits the category
    _cat_words = intent.get("keyword", "").lower().split()
    def _is_relevant(p):
        if not _cat_words:
            return True
        name_l = p["name"].lower()
        return any(w in name_l for w in _cat_words)

    picks = [p for p in [top, second] if p]
    if keywords and picks:
        with ThreadPoolExecutor(max_workers=2) as exe:
            futs = {exe.submit(_places_review_snippet, p["place_id"], keywords): i
                    for i, p in enumerate(picks)}
            for fut in futs:
                i = futs[fut]
                try:
                    snippet = fut.result(timeout=4)
                    # Suppress snippet if place seems off-category (e.g. restaurant in coffee results)
                    picks[i]["snippet"] = snippet if _is_relevant(picks[i]) else ""
                except Exception:
                    picks[i]["snippet"] = ""

    def _line(p):
        parts = []
        if p["rating"]:
            parts.append(f"{p['rating']}★")
        if p["price_level"]:
            parts.append(_PRICE_LEVEL.get(p["price_level"], ""))
        parts.append(f"{p['dist_mi']}mi")
        if p["open_now"] is True:
            parts.append("Open now")
        elif p["open_now"] is False:
            parts.append("May be closed")
        out = f"*{p['name']}* · " + " · ".join(filter(None, parts))
        if p.get("snippet"):
            out += f"\n_{p['snippet']}_"
        return out

    prefix = f"Cheap {label}" if wants_cheap else f"Best {label}"
    if second:
        return (f"{emoji} *{label.title()} near {pc_fmt}*\n\n"
                f"{_line(top)}\n\n{_line(second)}\n\nmiru.humanagency.co")
    return (f"{emoji} *{prefix} near {pc_fmt}*\n\n"
            f"{_line(top)}\n\nmiru.humanagency.co")


def _split_product_postcode(body: str):
    """Return (product_name, postcode_or_None) from a freeform message."""
    m = re.search(r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body.upper())
    if not m:
        return body.strip(), None
    postcode = m.group(1).replace(" ", "")
    remaining = (body[:m.start()] + " " + body[m.end():]).strip()
    return remaining, postcode


def _wa_train_format(from_name: str, to_name: str = "") -> str:
    """Return WhatsApp-formatted train departures. Returns None if station not found."""
    if not from_name:
        return None
    try:
        # Strip qualifier words users add (e.g. "Lewisham national rail" → "lewisham")
        from_lower = _STATION_QUALIFIER_RE.sub('', from_name.lower()).strip()
        to_lower   = _STATION_QUALIFIER_RE.sub('', to_name.lower()).strip() if to_name else ""

        # Find best match in station cache
        def _find_crs(name):
            if not name:
                return None, None
            exact = [(k, s) for k, s in _STATION_CACHE.items() if k == name]
            if exact:
                return exact[0][1]["crs"], exact[0][1]["name"]
            prefix = [(k, s) for k, s in _STATION_CACHE.items() if k.startswith(name)]
            if prefix:
                return prefix[0][1]["crs"], prefix[0][1]["name"]
            contains = [(k, s) for k, s in _STATION_CACHE.items() if name in k]
            if contains:
                return contains[0][1]["crs"], contains[0][1]["name"]
            # Fuzzy
            import difflib as _dl
            close = _dl.get_close_matches(name, list(_STATION_CACHE.keys()), n=1, cutoff=0.6)
            if close:
                s = _STATION_CACHE[close[0]]
                return s["crs"], s["name"]
            return None, None

        crs, stn_display = _find_crs(from_lower)
        # Z-prefix CRS = DLR/TfL station — route directly to TfL journey planner
        if crs and crs.upper().startswith('Z'):
            if to_lower:
                tube_reply = get_tube_journey(from_lower, to_lower)
                if tube_reply and "Couldn't find" not in tube_reply:
                    return tube_reply
            return None
        if not crs:
            # Station not in National Rail — try TfL journey planner directly
            if to_lower:
                tube_reply = get_tube_journey(from_name, to_name)
                if tube_reply and "Couldn't find" not in tube_reply:
                    return tube_reply
            # Try TfL arrivals at nearest matching tube station
            tube_reply = get_tube_journey(from_name, to_name) if to_lower else None
            if tube_reply and "Couldn't find" not in tube_reply:
                return tube_reply
            return None  # genuinely unknown — let it fall through

        to_crs, to_display = _find_crs(to_lower) if to_lower else (None, None)

        # Fetch departures
        access = _get_rtt_token()
        r = requests.get(
            "https://data.rtt.io/rtt/location",
            headers={"Authorization": f"Bearer {access}"},
            params={"code": f"gb-nr:{crs}"},
            timeout=12,
        )
        if r.status_code != 200:
            return f"Couldn't load departures for {stn_display} right now. Try again shortly."
        data = r.json()
        services = data.get("services") or []

        def fmt_time(dt):
            if not dt: return ""
            s = str(dt).strip()
            if len(s) >= 16: return s[11:16]
            if len(s) == 5 and s[2] == ":": return s
            if len(s) == 4 and s.isdigit(): return s[:2] + ":" + s[2:]
            return s[:5] if len(s) >= 5 else s

        lines = []
        shown = 0
        for svc in services:
            if shown >= 6:
                break
            td  = svc.get("temporalData", {})
            dep = td.get("departure", {})
            dest_list = svc.get("destination") or [{}]
            dest = (dest_list[0].get("location") or {}).get("description", "") if dest_list else ""
            if not dest:
                continue
            # Filter by destination if given
            if to_crs:
                dest_crs = (dest_list[0].get("location") or {}).get("crs", "")
                # Try to match by name or CRS
                if to_crs.upper() != dest_crs.upper() and to_lower not in dest.lower():
                    continue
            sched = fmt_time(dep.get("scheduleAdvertised") or dep.get("scheduled") or "")
            real_raw = (dep.get("realtimeForecast") or dep.get("forecast") or
                        dep.get("realtimeActual") or dep.get("actual") or "")
            real  = fmt_time(real_raw)
            cancelled = dep.get("isCancelled", False) or dep.get("cancelled", False)
            platform_raw = (svc.get("locationMetadata") or {}).get("platform")
            if isinstance(platform_raw, dict):
                platform = str(platform_raw.get("display") or platform_raw.get("number") or "")
            else:
                platform = str(platform_raw) if platform_raw else ""
            if not sched:
                continue
            if cancelled:
                status = "❌ Cancelled"
            elif real and real != sched:
                status = f"🕐 Exp {real}"
            else:
                status = "✅ On time"
            line = f"*{sched}* → {dest}  {status}"
            if platform:
                line += f"  Plat {platform}"
            lines.append(line)
            shown += 1

        if not lines:
            dest_note = f" to {to_display or to_name.title()}" if to_name else ""
            # Try tube journey planner as fallback (catches Elizabeth line / TfL-only routes)
            if to_name:
                tube_reply = get_tube_journey(from_name, to_name)
                if tube_reply and "Couldn't find" not in tube_reply:
                    return tube_reply
            return f"No service found from {stn_display}{dest_note}.\n\nTry: *tube {from_name} to {to_name or '...'}*"
        dest_note = f" to {to_display or to_name.title()}" if to_name else ""
        header = f"🚂 *{stn_display}*{dest_note}"
        return header + "\n\n" + "\n".join(lines) + "\n\nmiru.humanagency.co"
    except RuntimeError as e:
        print(f"[wa train] {e}")
        return None  # RTT not configured — fall through
    except Exception as e:
        print(f"[wa train format] {e}")
        return None


def _wa_brand_card(brand_name: str) -> str:
    """Fetch brand intel (DB-first via fetch_brand_data cache) and format as WhatsApp card."""
    from urllib.parse import quote as _q
    try:
        d = fetch_brand_data(brand_name)
    except Exception:
        d = {}

    name = d.get("name") or brand_name
    if not name or (not d.get("description") and not d.get("founded") and not d.get("slogan")):
        return (f"🏷️ Couldn't find brand info for *{brand_name}*.\n\n"
                f"Try the full profile: intel.humanagency.co?q={_q(brand_name)}")

    lines = [f"🏷️ *{name}*"]

    meta = []
    if d.get("founded"):  meta.append(f"Est. {d['founded']}")
    if d.get("hq"):       meta.append(d["hq"])
    if d.get("industry"): meta.append(d["industry"])
    if meta: lines.append(" · ".join(meta))

    if d.get("slogan"):
        lines.append(f'_"{d["slogan"]}"_')

    if d.get("description"):
        desc = d["description"][:220].rsplit(" ", 1)[0]
        lines.append(f"\n{desc}…")

    if d.get("competitors"):
        lines.append(f"\n🆚 vs {' · '.join(d['competitors'][:3])}")

    tp = d.get("trustpilot") or {}
    if tp.get("rating"):
        stars = round(float(tp["rating"]))
        count_str = f" ({tp['count']:,} reviews)" if tp.get("count") else ""
        lines.append(f"{'⭐'*stars} Trustpilot: {tp['rating']}/5{count_str}")

    lines.append(f"\n🔍 Full profile: intel.humanagency.co?q={_q(name)}")
    return "\n".join(lines)


def whatsapp_product_format(product_name: str, postcode: str = None) -> str:
    """Look up a grocery product and return a WhatsApp-friendly price summary."""
    import requests as _req, json as _json
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

    groq_key = os.environ.get("GROQ_API_KEY", "")

    def _obj(text):
        s, e = text.find("{"), text.rfind("}")
        return _json.loads(text[s:e+1]) if s != -1 and e != -1 else {}

    def _groq(prompt, max_tokens=500, model="llama-3.1-8b-instant"):
        r = _req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={"model": model,
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": max_tokens, "temperature": 0.2},
            timeout=15,
        )
        return r.json()["choices"][0]["message"]["content"].strip()

    def _tesco(q):
        """Try Tesco grocery API for a live price."""
        try:
            r = _req.get(
                "https://api.tesco.com/shoppingexperience/v1/api/products/search",
                params={"query": q, "count": "3", "offset": "0"},
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=5,
            )
            if r.status_code == 200:
                items = r.json().get("uk", {}).get("ghs", {}).get("products", {}).get("results", [])
                if items:
                    p = items[0].get("price") or items[0].get("unitPrice")
                    if p:
                        return f"£{float(p):.2f}"
        except Exception:
            pass
        return None

    # Single Groq call: product info + all store prices + alternatives
    product = {"name": product_name, "brand": "", "category": ""}
    prices = {}
    alternatives = []

    if groq_key:
        try:
            prompt = (
                f'You are a UK grocery price expert. The user asked about: "{product_name}".\n'
                'Return ONLY valid JSON with these fields:\n'
                '{\n'
                '  "name": "exact product name and size e.g. Heinz Baked Beans 415g",\n'
                '  "brand": "brand name",\n'
                '  "category": "category",\n'
                '  "prices": {\n'
                '    "aldi": "£X.XX",\n'
                '    "lidl": "£X.XX",\n'
                '    "asda": "£X.XX",\n'
                '    "sainsburys": "£X.XX",\n'
                '    "tesco": "£X.XX",\n'
                '    "waitrose": "£X.XX"\n'
                '  },\n'
                '  "alternatives": [\n'
                '    {"name": "...", "price": "£X.XX", "store": "..."}\n'
                '  ]\n'
                '}\n'
                'Use typical current UK shelf prices. Only include stores where the product is commonly sold.'
            )
            raw = _groq(prompt, max_tokens=500)
            obj = _obj(raw)
            if obj.get("name"):
                product = {"name": obj["name"], "brand": obj.get("brand",""), "category": obj.get("category","")}
            prices = {k: v for k, v in obj.get("prices", {}).items() if v and "X" not in v}
            alternatives = obj.get("alternatives", [])[:2]
        except Exception:
            # Fallback: simpler prompt with smaller model
            try:
                raw = _groq(
                    f'UK grocery "{product_name}": give typical prices at Aldi, Asda, Tesco, Sainsburys. '
                    'Return ONLY JSON: {"name":"...","brand":"...","prices":{"aldi":"£X.XX","asda":"£X.XX","tesco":"£X.XX","sainsburys":"£X.XX"}}',
                    max_tokens=200, model="llama-3.1-8b-instant"
                )
                obj = _obj(raw)
                if obj.get("name"):
                    product["name"] = obj["name"]
                    product["brand"] = obj.get("brand", "")
                prices = {k: v for k, v in obj.get("prices", {}).items() if v and "X" not in v}
            except Exception:
                pass

    # Try to get a live Tesco price to verify/supplement AI prices
    live_tesco = _tesco(product["name"])
    if live_tesco:
        prices["tesco"] = live_tesco + " ✓"

    search_term = product["name"]
    brand = product["brand"]

    # Format reply
    lines = [f"🛒 {search_term}"]
    if brand:
        lines.append(f"Brand: {brand}" + (f" | {product['category']}" if product.get("category") else ""))
    lines.append("")

    if prices:
        store_order = ["aldi", "lidl", "asda", "sainsburys", "tesco", "waitrose", "amazon"]
        sorted_prices = sorted(prices.items(), key=lambda x: (store_order.index(x[0]) if x[0] in store_order else 99))
        lines.append("Prices:")
        for store, price in sorted_prices:
            lines.append(f"  {store.title()}: {price}")
        # Highlight cheapest (strip £, ✓, spaces for comparison)
        try:
            def _num(v): return float(re.sub(r'[^0-9.]', '', v))
            cheapest_store, cheapest_price = min(prices.items(), key=lambda x: _num(x[1]))
            lines.append(f"\n💰 Cheapest: {cheapest_store.title()} at {cheapest_price}")
        except Exception:
            pass
    else:
        lines.append("⚠️ Couldn't find live prices right now.")

    if alternatives:
        lines.append("\nAlternatives:")
        for a in alternatives[:2]:
            lines.append(f"  {a['name']} — {a.get('price','')}")

    # Nearby supermarkets if postcode given
    if postcode:
        latlon = postcode_to_latlon(postcode)
        if latlon:
            try:
                amenities = fetch_nearby_amenities(latlon[0], latlon[1], 8.0)
                supers = amenities.get("supermarkets", [])[:4]
                if supers:
                    lines.append(f"\nNearby supermarkets ({postcode}):")
                    for s in supers:
                        lines.append(f"  {s['name']} ({s['dist_mi']:.1f}mi)")
            except Exception:
                pass

    lines.append("\nReply with a product name to compare prices")
    lines.append("Or a postcode for fuel prices")
    lines.append("Not right? Reply *HELP*")
    return "\n".join(lines)


_HELP_MSG = (
    "📋 *Miru — what can I help with?*\n"
    "\n"
    "Just type naturally — no exact commands needed.\n"
    "\n"
    "☕🍺 *Food & Drink*\n"
    "  _where can I get good coffee near KT15_\n"
    "  _cheap beer KT15_\n"
    "  _good lunch near me_  |  _pizza GU25_\n"
    "  _breakfast KT16_  |  _pub KT15_  |  _dinner KT12_\n"
    "  _(set home postcode on miru.humanagency.co to skip the postcode)_\n"
    "\n"
    "🚆🚇 *Trains & Tube*\n"
    "  _train waterloo to lewisham_\n"
    "  _trains from Blackfriars_\n"
    "  _tube status_  |  _status tube_  |  _jubilee line_\n"
    "  _(National Rail · Elizabeth line · Tube · DLR · Overground)_\n"
    "\n"
    "⛽ *Fuel*\n"
    "  KT15 petrol  |  diesel GU25\n"
    "\n"
    "🏡 *My Area*\n"
    "  places KT15 3RL  |  GP near KT15  |  pubs near GU25\n"
    "\n"
    "🏫 *Schools*\n"
    "  school news  |  school week  |  school setup\n"
    "\n"
    "📚 *Books*\n"
    "  book The Alchemist  |  my books  _(or send a barcode photo)_\n"
    "\n"
    "🔖 *Saves*\n"
    "  Send any URL  |  restaurants saved last week  |  my saves\n"
    "\n"
    "🛒 *Price check*\n"
    "  price olive oil  |  compare oat milk\n"
    "\n"
    "📸 *Photo*  — send any photo to identify or describe it\n"
    "\n"
    "🌐 miru.humanagency.co  ·  Reply *HELP* anytime"
)

_WELCOME_MSG = (
    "👋 Welcome to *Miru* — your UK life assistant.\n"
    "\n"
    "No app, no signup. Just type naturally:\n"
    "\n"
    "☕  _where can I get good coffee near KT15_\n"
    "🍺  _cheap beer KT16_\n"
    "🚆  _train waterloo to lewisham_\n"
    "🚇  _tube status_\n"
    "⛽  KT15 petrol\n"
    "🏡  places KT15 3RL\n"
    "🔖  send any link — I'll save it\n"
    "📸  send a photo — I'll describe it\n"
    "\n"
    "Reply *HELP* for everything I can do.\n"
    "Set your home postcode at miru.humanagency.co 🏠"
)

_CLARIFY_MSG = (
    "Hmm, not sure what you mean 🤔\n\n"
    "Try:\n"
    "☕ _good coffee near KT15_\n"
    "🚆 _train waterloo to lewisham_\n"
    "🍺 _cheap beer KT16_\n"
    "🚇 _tube status_\n"
    "⛽ KT15 petrol\n\n"
    "Reply *HELP* for the full list."
)

_GREETING_WORDS = {"hi", "hello", "hey", "start", "help", "menu", "miru", "join"}

_SEEN_NUMBERS: set = set()

def _is_new_user(from_number: str) -> bool:
    """True if this number has no prior history in Miru."""
    if from_number in _SEEN_NUMBERS:
        return False
    try:
        variants = _wa_number_variants(from_number)
        for table, col in [("wa_saves", "from_number"), ("school_profiles", "wa_number"), ("my_area_places", "from_number")]:
            rows = lib._sb().table(table).select("id").in_(col, variants).limit(1).execute().data
            if rows:
                _SEEN_NUMBERS.add(from_number)
                return False
        return True
    except Exception:
        return False


def _wa_classify_intent(body: str) -> dict | None:
    """Use Groq to classify a free-text WhatsApp message into a known intent.
    Returns intent dict or None on failure / unknown.
    Called when no exact command matched — handles typos, word-order variations, and natural phrasing."""
    try:
        from groq import Groq as _Groq
        import json as _json
        gc = _Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
        result = gc.chat.completions.create(
            model="llama3-8b-8192",
            temperature=0,
            messages=[{"role": "system", "content": (
                "Classify the user message into ONE intent. Respond with JSON only — no markdown, no explanation.\n\n"
                "Intents:\n"
                "  train        — train/rail departures or journey. Extract: from (station name, str), to (station name or null).\n"
                "  tube         — London tube/DLR/Overground/Elizabeth line status, journey, or line info.\n"
                "                 Extract: from (station or null), to (station or null),\n"
                "                 query (status | journey | line name — infer from message, default 'status').\n"
                "  food         — food or drink discovery nearby. Extract: food_type — normalise to one of (coffee|breakfast|sandwiches|lunch|pizza|dinner|beer|pub|tea|burger|kebab|steak|wine|indian|italian|mexican|fish_chips|chinese).\n"
                "                 Map latte/cappuccino/capuccino/mocha/pistachio latte/espresso/flat white/americano/cortado/coffee/cofee/coffeeee → coffee.\n"
                "                 Map tea/english breakfast/chai/brew/cuppa → tea.\n"
                "                 Map beer/pint/ale/lager/craft beer/cheap beer/good beer → beer.\n"
                "                 Map burger/burgers/smash burger/best burger → burger. Map kebab/doner/shawarma → kebab. Map steak/steakhouse/ribeye/sirloin → steak.\n"
                "                 Map wine/red wine/white wine/rosé/house wine/glass of wine → wine.\n"
                "                 Map indian/india/curry/curries/balti/tandoori/tikka/biryani/korma/masala/best indian/indian food/indian restaurant/indian place → indian.\n"
                "                 Map italian/italy/pasta/risotto/trattoria/best italian/italian food/italian restaurant → italian.\n"
                "                 Map mexican/mexico/taco/tacos/burrito/tex-mex/best mexican/mexican food/mexican restaurant → mexican.\n"
                "                 Map fish and chips/fish n chips/fish & chips/chippy/chip shop/best fish and chips → fish_chips.\n"
                "                 Map chinese/china/dim sum/chow mein/best chinese → chinese.\n"
                "                 postcode (UK outcode like KT16 or full postcode — extract exactly as written, or null if not present),\n"
                "                 cheap (bool, true only if user explicitly says cheap/budget/affordable).\n"
                "  book_lookup  — user wants to find/save/add a book. Extract: query (title/author/ISBN).\n"
                "  search_saves — user wants to browse/filter things they already saved.\n"
                "                 Extract: filter (books|wine|recipes|menus|restaurants|videos|products|all),\n"
                "                 author (if 'by X' mentioned, else null), timeframe (today|yesterday|last_week|last_month|all).\n"
                "  worth_it     — wants review/verdict on last saved book.\n"
                "  shopping_list— wants ingredients/shopping list from last saved recipe.\n"
                "  my_saves     — wants to see their saved items list.\n"
                "  my_link      — wants their personal Miru link.\n"
                "  unknown      — anything else (fuel prices, postcodes alone, greetings, etc.).\n\n"
                "Examples (JSON only):\n"
                '{"intent":"train","from":"waterloo","to":"lewisham"}\n'
                '{"intent":"train","from":"reading","to":null}\n'
                '{"intent":"tube","from":null,"to":null,"query":"status"}\n'
                '{"intent":"tube","from":null,"to":null,"query":"jubilee"}\n'
                '{"intent":"tube","from":"canary wharf","to":"oxford circus","query":"journey"}\n'
                '{"intent":"food","food_type":"coffee","postcode":"KT16","cheap":false}\n'
                '{"intent":"food","food_type":"beer","postcode":null,"cheap":true}\n'
                '{"intent":"food","food_type":"coffee","postcode":null,"cheap":false}\n'
                '{"intent":"book_lookup","query":"midnight library"}\n'
                '{"intent":"search_saves","filter":"wine","author":null,"timeframe":"last_week"}\n'
                '{"intent":"worth_it"}\n'
                '{"intent":"shopping_list"}\n'
                '{"intent":"my_saves"}\n'
                '{"intent":"my_link"}\n'
                '{"intent":"unknown"}'
            )}, {"role": "user", "content": body[:300]}],
            max_tokens=160,
        ).choices[0].message.content.strip()
        result = re.sub(r"^```[a-z]*\n?|```$", "", result.strip()).strip()
        parsed = _json.loads(result)
        return parsed if parsed.get("intent") and parsed["intent"] != "unknown" else None
    except Exception as _e:
        print(f"[intent] classify failed: {_e}")
        return None


def _wa_search_saves(from_number: str, filter_type: str, timeframe: str, author: str = "") -> str:
    """Query wa_saves (and my_area_places for restaurants) and format for WhatsApp."""
    from datetime import datetime, timedelta, timezone
    import json as _json

    plain = from_number.replace("whatsapp:", "").strip()
    wa    = f"whatsapp:{plain}" if not plain.startswith("whatsapp:") else plain
    nums  = [from_number, plain, wa]

    # Timeframe → cutoff datetime
    now = datetime.now(timezone.utc)
    cutoff = None
    if timeframe == "today":
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif timeframe == "yesterday":
        d = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff = d
        end_cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif timeframe == "last_week":
        cutoff = now - timedelta(days=7)
    elif timeframe == "last_month":
        cutoff = now - timedelta(days=30)

    # --- Restaurant/place saves: from my_area_places ---
    if filter_type == "restaurants":
        try:
            sb = lib._sb()
            q = sb.table("my_area_places").select("name,address,phone,created_at") \
                .in_("from_number", nums) \
                .in_("emoji", ["🍺", "☕", "🍽️", "🥘"])
            if cutoff:
                q = q.gte("created_at", cutoff.isoformat())
            rows = q.order("created_at", desc=True).limit(10).execute().data or []
            if not rows:
                tf = {"today":"today","yesterday":"yesterday","last_week":"last 7 days","last_month":"last month"}.get(timeframe,"")
                return f"🍽️ No restaurants saved{' ' + tf if tf else ''}."
            lines = []
            for r in rows:
                line = f"🍽️ *{r['name']}*"
                if r.get("address"): line += f"\n   {r['address']}"
                if r.get("phone"):   line += f" · {r['phone']}"
                lines.append(line)
            tf_label = {"today":"today","yesterday":"yesterday","last_week":"this week","last_month":"this month"}.get(timeframe,"")
            header = f"🍽️ *Restaurants saved{' ' + tf_label if tf_label else ''}* ({len(rows)})"
            return header + "\n\n" + "\n\n".join(lines)
        except Exception as e:
            print(f"[search_saves] restaurants: {e}")
            return "Couldn't fetch your restaurant saves right now."

    # --- All other types: from wa_saves ---
    filter_map = {
        "books":    ("url", "book:%"),
        "wine":     ("title", "🍷%"),
        "recipes":  ("title", "🍳%"),
        "menus":    ("title", "%menu%"),
        "videos":   ("title", "📺%"),
        "products": ("title", "📦%"),
    }
    emoji_labels = {
        "books": "📚", "wine": "🍷", "recipes": "🍳",
        "menus": "📋", "videos": "📺", "products": "📦", "all": "📂",
    }

    try:
        sb = lib._sb()
        q = sb.table("wa_saves").select("id,title,url,summary,created_at") \
            .in_("from_number", nums)
        col, pat = filter_map.get(filter_type, (None, None))
        if col and pat:
            q = q.ilike(col, pat)
        if cutoff:
            q = q.gte("created_at", cutoff.isoformat())
        if timeframe == "yesterday" and "end_cutoff" in dir():
            q = q.lt("created_at", end_cutoff.isoformat())
        rows = q.order("created_at", desc=True).limit(10).execute().data or []

        # Filter by author if requested (checks summary JSON)
        if author and filter_type == "books":
            def _matches_author(row):
                try:
                    bk = _json.loads(row.get("summary") or "{}")
                    return author.lower() in (bk.get("author") or "").lower()
                except Exception:
                    return False
            rows = [r for r in rows if _matches_author(r)]

        if not rows:
            tf = {"today":"today","yesterday":"yesterday","last_week":"last 7 days","last_month":"last month"}.get(timeframe,"")
            by = f" by {author}" if author else ""
            return f"{emoji_labels.get(filter_type,'📂')} No {filter_type}{by} saved{' in the ' + tf if tf else ''}."

        lines = []
        for r in rows:
            title = r.get("title") or r.get("url", "?")
            if filter_type == "books":
                try:
                    bk = _json.loads(r.get("summary") or "{}")
                    title = bk.get("title") or title
                    auth  = bk.get("author", "")
                    cr    = bk.get("communityRating") or {}
                    line  = f"📚 *{title}*"
                    if auth: line += f"\n   by {auth}"
                    if cr.get("avg"): line += f" · {'⭐'*round(cr['avg'])} {cr['avg']}/5"
                except Exception:
                    line = f"📚 *{title}*"
            else:
                line = f"{emoji_labels.get(filter_type,'📂')} *{title}*"
            lines.append(line)

        tf_label = {"today":"today","yesterday":"yesterday","last_week":"this week","last_month":"this month"}.get(timeframe,"")
        by = f" by {author.title()}" if author else ""
        header = f"{emoji_labels.get(filter_type,'📂')} *{filter_type.title()}{by}{' — ' + tf_label if tf_label else ''}* ({len(rows)})"
        return header + "\n\n" + "\n\n".join(lines)
    except Exception as e:
        print(f"[search_saves] {filter_type}: {e}")
        return "Couldn't search your saves right now."


@app.route("/whatsapp", methods=["POST"])
def whatsapp_reply():
    body        = request.form.get("Body", "").strip()
    from_number = request.form.get("From", "unknown")
    print(f"WhatsApp from {from_number}: {body}")

    resp = MessagingResponse()

    # ── Location share (Twilio sends Latitude+Longitude for WhatsApp location messages) ──
    _lat = request.form.get("Latitude", "")
    _lon = request.form.get("Longitude", "")
    if _lat and _lon:
        try:
            _loc_lat, _loc_lon = float(_lat), float(_lon)
            _USER_LAST_LOCATION[from_number] = {
                "lat": _loc_lat, "lon": _loc_lon, "ts": time.time()
            }
            # If there's a pending food intent, serve it immediately with live location
            _pending = _get_wa_pending_intent(from_number)
            if _pending and _pending.get("type") == "food":
                _clear_wa_pending_intent(from_number)
                _ftype  = _pending.get("food_type", "food")
                _fcheap = _pending.get("cheap", False)
                _synthetic = f"{'cheap ' if _fcheap else ''}{_ftype}"
                # Location is now in _USER_LAST_LOCATION — _wa_food_find will pick it up
                _food_r = _wa_food_find(_synthetic, from_number)
                resp.message(_food_r or f"📍 Got your location! Send *{_ftype}* again and I'll search right here.")
            else:
                resp.message("📍 Got your location! Send *best steak*, *good coffee* etc. and I'll find it right where you are. Resets in 2 hours.")
            return str(resp)
        except Exception:
            pass

    # ── Image/photo capture (before body checks — body is empty when photo sent) ─
    num_media = int(request.form.get("NumMedia", "0") or 0)
    if num_media > 0:
        media_url  = request.form.get("MediaUrl0", "")
        media_type = request.form.get("MediaContentType0", "image/jpeg")
        if media_url and "image" in media_type:
            reply = _wa_process_image(from_number, media_url, media_type)
            resp.message(reply)
            return str(resp)

    body_lower = body.strip().lower()
    if not body or body_lower in _GREETING_WORDS or body_lower.startswith("join "):
        is_join = body_lower.startswith("join ") or body_lower == "join"
        if is_join or _is_new_user(from_number):
            _SEEN_NUMBERS.add(from_number)
            resp.message(_WELCOME_MSG)
            # Ask for postcode if they don't have one saved yet
            if not _get_wa_home_postcode(from_number):
                _set_wa_pending_intent(from_number, {"type": "setup_postcode"})
                resp.message("📍 One quick thing — what's your home postcode?\n\nJust reply with it (e.g. *KT16 0HY*) and I'll remember it for trains, fuel prices, local info and more.")
        else:
            resp.message(_HELP_MSG)
        return str(resp)

    # ── Pending intent: postcode reply resolves a stored intent ─────────────────
    _pc_reply = re.match(r'^([A-Z]{1,2}\d{1,2}[A-Z]?(?:\s*\d[A-Z]{2})?)\s*$', body.upper().strip())
    if _pc_reply:
        _pending = _get_wa_pending_intent(from_number)
        if _pending:
            _clear_wa_pending_intent(from_number)
            _ptype = _pending.get("type", "")
            if _ptype == "setup_postcode":
                _pc_val = _pc_reply.group(1).upper()
                try:
                    sb = lib._sb()
                    plain = from_number.replace("whatsapp:", "").strip()
                    sb.table("my_area_places").delete().eq("from_number", plain).eq("category", "_home").execute()
                    sb.table("my_area_places").insert({
                        "name": "__home__", "category": "_home",
                        "postcode": _pc_val.replace(" ", ""),
                        "emoji": "📍", "from_number": plain, "device_id": plain
                    }).execute()
                except Exception as e:
                    print(f"[setup_postcode] {e}")
                resp.message(f"✅ Got it — *{_pc_val}* saved as your home.\n\nNow try:\n🚆 _next train_\n⛽ _petrol prices_\n🏡 _places near me_\nOr visit miru.humanagency.co 🌐")
                return str(resp)
            if _ptype == "food":
                _cheap_pfx = "cheap " if _pending.get("cheap") else ""
                _food_body  = f"{_cheap_pfx}{_pending['food_type']} {_pc_reply.group(1)}"
                _food_reply = _wa_food_find(_food_body, from_number)
                resp.message(_food_reply or f"Nothing found near {_pc_reply.group(1)}. Try again!")
                return str(resp)
            elif _ptype == "train":
                _tr = _wa_train_format(_pending.get("from", ""), _pc_reply.group(1))
                if _tr:
                    resp.message(_tr)
                    return str(resp)

    # ── URL save ──────────────────────────────────────────────────────────────
    url_m = re.search(r'https?://\S+', body)
    if url_m:
        url = url_m.group(0).rstrip(".,)")
        reply = _wa_save_url(from_number, url)
        resp.message(reply)
        return str(resp)

    # ── Digest numbered reply: "1 READ", "2 SKIP", "3 REMIND Monday" ─────────
    digest_m = re.match(r'^([1-9])\s+(READ|SKIP|REMIND\S*(?:\s+\S+)?)\s*$', body.strip(), re.I)
    if digest_m:
        idx = int(digest_m.group(1)) - 1
        cmd = digest_m.group(2).strip().upper()
        # Re-fetch from Supabase if in-memory state was lost (e.g. server restart)
        items = _PENDING_DIGEST.get(from_number)
        if not items:
            try:
                rows = (lib._sb().table("wa_saves").select("id,title,url")
                        .eq("from_number", from_number)
                        .in_("status", ["pending", "remind"])
                        .order("created_at").limit(9).execute().data)
                items = [{"id": r["id"], "title": r.get("title") or "Untitled", "url": r.get("url", "")} for r in rows]
                _PENDING_DIGEST[from_number] = items
            except Exception:
                items = []
        if 0 <= idx < len(items):
            item = items[idx]
            _PENDING_TRIAGE[from_number] = {
                "id": item["id"], "title": item["title"], "url": item["url"],
                "expires": time.time() + 3600,
            }
            reply = _wa_triage_respond(from_number, cmd)
        else:
            reply = "Couldn't find that item — reply LIST to see your saves, or paste the link again."
        resp.message(reply)
        return str(resp)

    # ── Single-item triage reply (after sending a URL) ────────────────────────
    cmd_up = body.strip().upper()
    if cmd_up in ("READ", "SKIP") or cmd_up.startswith("REMIND"):
        reply = _wa_triage_respond(from_number, cmd_up)
        resp.message(reply)
        return str(resp)

    # ── Wine rating reply: "4 Great with pasta" or "3.5 too tannic" ─────────────
    _wine_state = _WINE_RATING_STATE.get(from_number)
    if _wine_state and time.time() < _wine_state.get("expires", 0):
        import re as _wre
        _wm = _wre.match(r'^([1-5](?:[.,]\d)?)\s*(.*)', body.strip(), _wre.DOTALL)
        if _wm:
            _score = _wm.group(1).replace(",", ".")
            _notes = _wm.group(2).strip()
            _WINE_RATING_STATE.pop(from_number, None)
            try:
                _wsid = _wine_state["save_id"]
                _existing = lib._sb().table("wa_saves").select("summary").eq("id", _wsid).execute().data
                _esummary = (_existing[0]["summary"] if _existing else "") or ""
                _rating_line = f"\n\n⭐ My rating: {_score}/5"
                if _notes:
                    _rating_line += f"\n📝 {_notes[:200]}"
                lib._sb().table("wa_saves").update({"summary": _esummary + _rating_line}).eq("id", _wsid).execute()
                lib.saves_sync({"id": _wsid, **(_existing[0] if _existing else {}), "summary": _esummary + _rating_line})
                resp.message(f"✅ Rated *{_wine_state['wine_name']}* {_score}/5!")
            except Exception:
                resp.message("✅ Rating saved!")
            return str(resp)

    # ── NEW command: clear menu session so next photo starts a fresh save ───────
    if body_lower in ("new", "new save", "new session"):
        _MENU_SESSION.pop(from_number, None)
        resp.message("✅ Started fresh — send your next photo to begin a new save.")
        return str(resp)

    # ── MY LINK command: send the user their personal access link ─────────────
    if body_lower in ("my link", "link", "my saves link", "get link", "access", "my access"):
        user_token = _wa_user_token(from_number)
        resp.message(
            f"🔗 Your personal Miru link:\n"
            f"miru.humanagency.co/?screen=saves&token={user_token}\n\n"
            f"Tap it to open My Saves on any browser. Your saves are private to you."
        )
        return str(resp)

    # ── MY PLACES command: list or search My Area saved places ────────────────
    if body_lower in ("my places", "my bookmarks", "places", "saved places") or body_lower.startswith("find my "):
        user_token = _wa_user_token(from_number)
        query = ""
        if body_lower.startswith("find my "):
            query = body_lower[len("find my "):].strip()
        try:
            rows = (lib._sb().table("my_area_places")
                    .select("name,address,phone,emoji,opening_hours")
                    .eq("device_id", user_token)
                    .order("created_at").execute().data)
        except Exception:
            rows = []
        if not rows:
            resp.message(
                f"📌 No saved places found.\n\n"
                f"To sync your web bookmarks with WhatsApp, open this link:\n"
                f"miru.humanagency.co/?screen=myarea&token={user_token}\n\n"
                f"Bookmarks you add after opening that link will be searchable here."
            )
            return str(resp)
        if query:
            rows = [r for r in rows if query in r.get("name","").lower() or query in r.get("address","").lower()]
            if not rows:
                resp.message(f"🔍 Nothing found for '{query}' in your saved places.")
                return str(resp)
        lines = [f"📌 My Saved Places ({len(rows)}):"]
        for r in rows[:10]:
            line = f"\n{r.get('emoji','📍')} *{r['name']}*"
            if r.get("address"):  line += f"\n   {r['address']}"
            if r.get("phone"):    line += f"\n   📞 {r['phone']}"
            if r.get("opening_hours"): line += f"\n   🕐 {r['opening_hours']}"
            lines.append(line)
        if len(rows) > 10:
            lines.append(f"\n…and {len(rows)-10} more. Open the app to see all.")
        resp.message("\n".join(lines))
        return str(resp)

    # ── SHOPPING LIST command: extract ingredients from last recipe save ─────
    if cmd_up in ("SHOPPING LIST", "INGREDIENTS", "LIST INGREDIENTS", "RECIPE LIST"):
        plain_number = from_number.replace("whatsapp:", "").strip()
        wa_number    = from_number if from_number.startswith("whatsapp:") else f"whatsapp:{from_number}"
        try:
            rows = (lib._sb().table("wa_saves").select("id,title,url,summary")
                    .in_("from_number", [from_number, plain_number, wa_number])
                    .like("title", "🍳%")
                    .order("created_at", desc=True).limit(1).execute().data)
        except Exception:
            rows = []
        if not rows:
            resp.message("🍳 No recipe saves found. Save a recipe URL first — e.g. send a link from BBC Food or AllRecipes.")
            return str(resp)
        row = rows[0]
        recipe_title = (row.get("title") or "").replace("🍳 ", "").strip()
        recipe_url   = row.get("url", "")
        # Try to fetch fresh page text; fall back to stored summary
        try:
            fetched = _fetch_url_text(recipe_url)
            page_text = fetched.get("text", "") or row.get("summary", "")
        except Exception:
            page_text = row.get("summary", "")
        if not page_text:
            resp.message(f"🍳 Couldn't load *{recipe_title}* — try opening the recipe link directly.")
            return str(resp)
        try:
            ingredients = _groq_chat(
                system=(
                    "Extract the shopping list (ingredients only, no quantities of equipment) "
                    "from this recipe. Format as a plain bullet list with • prefix, one item per line. "
                    "Group by category if helpful (e.g. Produce, Dairy, Meat). "
                    "Max 30 items. No intro or outro text."
                ),
                messages=[{"role": "user", "content": f"Recipe: {recipe_title}\n\n{page_text[:4000]}"}],
                max_tokens=400,
            ).strip()
        except Exception:
            ingredients = ""
        if not ingredients:
            resp.message(f"🍳 Couldn't extract ingredients from *{recipe_title}*. Try opening the recipe link.")
            return str(resp)
        resp.message(f"🛒 *Shopping list — {recipe_title}*\n\n{ingredients}")
        return str(resp)

    # ── WORTH IT / BOOK REVIEW command ────────────────────────────────────────
    if cmd_up in ("WORTH IT", "WORTH IT?", "REVIEWS", "BOOK REVIEW", "THOUGHTS"):
        # Books saved via app use plain number; WhatsApp scans use "whatsapp:+..." prefix
        plain_number = from_number.replace("whatsapp:", "").strip()
        wa_number    = from_number if from_number.startswith("whatsapp:") else f"whatsapp:{from_number}"
        try:
            rows = (lib._sb().table("wa_saves").select("id,title,url,summary")
                    .in_("from_number", [from_number, plain_number, wa_number])
                    .like("url", "book:%")
                    .order("created_at", desc=True).limit(1).execute().data)
        except Exception:
            rows = []
        if not rows:
            resp.message("📚 No scanned books found. Scan a book barcode in the Miru app first.")
            return str(resp)
        row = rows[0]
        book_title, book_author, book_desc, book_genre, book_rating = row.get("title", ""), "", "", "", None
        try:
            import json as _json
            bk = _json.loads(row.get("summary") or "{}")
            book_title  = bk.get("title", book_title)
            book_author = bk.get("author", "")
            book_desc   = bk.get("description", "")[:600]
            book_genre  = bk.get("subjects", "") or bk.get("genre", "")
            cr = bk.get("communityRating") or {}
            book_rating = cr.get("avg")
        except Exception:
            pass
        book_label = f"*{book_title}*" + (f" by {book_author}" if book_author else "")

        # Build grounded context from stored data so Groq doesn't hallucinate
        context_lines = [f"Title: {book_title}"]
        if book_author: context_lines.append(f"Author: {book_author}")
        if book_genre:  context_lines.append(f"Genre: {book_genre}")
        if book_rating: context_lines.append(f"Google Books rating: {book_rating}/5")
        if book_desc:   context_lines.append(f"Description: {book_desc}")
        context = "\n".join(context_lines)

        try:
            verdict = _groq_chat(
                system=(
                    "You are a concise book critic. Using ONLY the provided book information (do not invent facts), "
                    "write exactly 4 lines:\n"
                    "Line 1: ✅ Fans say: [what the description/genre suggests readers will love — max 15 words]\n"
                    "Line 2: ⚠️ Critics say: [likely criticism based on genre/style — max 15 words]\n"
                    "Line 3: 🎯 Best for: [who this book suits — max 10 words]\n"
                    "Line 4: 📖 Verdict: Worth it / Skip it / Depends on taste — one sentence reason\n"
                    "If you don't have enough information, be honest and say so on line 1. No other text."
                ),
                messages=[{"role": "user", "content": context}],
                max_tokens=220,
            ).strip()
        except Exception:
            verdict = ""
        if not verdict:
            resp.message(f"📚 Couldn't generate a review for {book_label} right now. Try again shortly.")
            return str(resp)
        rating_line = f"\n⭐ {book_rating}/5 on Google Books" if book_rating else ""
        resp.message(f"📚 {book_label}{rating_line}\n\n{verdict}")
        return str(resp)

    # ── SAVE SONG <title> command ─────────────────────────────────────────────
    _song_prefixes = ("save song ", "song ", "add song ", "music ")
    _sg_prefix = next((p for p in _song_prefixes if body_lower.startswith(p)), None)
    if _sg_prefix:
        sg_query = body[len(_sg_prefix):].strip()
        if not sg_query:
            resp.message("🎵 Tell me what to save — e.g.\n*save song Blinding Lights*")
            return str(resp)
        try:
            _it = requests.get(
                "https://itunes.apple.com/search",
                params={"term": sg_query, "entity": "song", "limit": 1, "country": "GB"},
                timeout=8,
            ).json()
            _track = (_it.get("results") or [None])[0]
        except Exception:
            _track = None
        if not _track:
            # Save raw query as unmatched so user doesn't lose the intent
            _wa_phone_um = from_number.replace("whatsapp:", "")
            try:
                _um_exists = lib._sb().table("music_saves").select("id") \
                    .eq("phone", _wa_phone_um).eq("title", sg_query).execute().data
                if not _um_exists:
                    lib._sb().table("music_saves").insert({
                        "phone":       _wa_phone_um,
                        "title":       sg_query,
                        "artist":      "",
                        "album":       "",
                        "year":        "",
                        "genre":       "",
                        "cover_url":   "",
                        "preview_url": "",
                        "spotify_url": f"unmatched:{sg_query}",
                    }).execute()
            except Exception:
                pass
            resp.message(
                f"💾 Saved *{sg_query}* to your list.\n"
                f"Couldn't match it on iTunes — open My Saves to search for it.\n"
                f"miru.humanagency.co/?screen=music"
            )
            return str(resp)
        _sg_title   = _track.get("trackName", sg_query)
        _sg_artist  = _track.get("artistName", "")
        _sg_album   = _track.get("collectionName", "")
        _sg_year    = str(_track.get("releaseDate", ""))[:4]
        _sg_genre   = _track.get("primaryGenreName", "")
        _sg_cover   = (_track.get("artworkUrl100") or "").replace("100x100", "300x300")
        _sg_preview = _track.get("previewUrl", "")
        _sg_spotify = f"https://open.spotify.com/search/{requests.utils.quote(_sg_title + ' ' + _sg_artist)}"
        _wa_phone   = from_number.replace("whatsapp:", "")
        try:
            _existing = lib._sb().table("music_saves") \
                .select("id").eq("phone", _wa_phone).eq("title", _sg_title).eq("artist", _sg_artist).execute().data
            if not _existing:
                lib._sb().table("music_saves").insert({
                    "phone":       _wa_phone,
                    "title":       _sg_title,
                    "artist":      _sg_artist,
                    "album":       _sg_album,
                    "year":        _sg_year,
                    "genre":       _sg_genre,
                    "cover_url":   _sg_cover,
                    "preview_url": _sg_preview,
                    "spotify_url": _sg_spotify,
                }).execute()
            _already = bool(_existing)
        except Exception:
            _already = False
        _save_status = "Already in" if _already else "Saved to"
        resp.message(
            f"🎵 *{_sg_title}*\n{_sg_artist}"
            + (f" · {_sg_album}" if _sg_album else "")
            + (f" · {_sg_year}" if _sg_year else "")
            + f"\n\n{'✅' if not _already else '📌'} {_save_status} your Music tab"
            + f"\nmiru.humanagency.co/?screen=music"
        )
        return str(resp)

    # ── BOOK <title or ISBN> command ──────────────────────────────────────────
    _book_prefixes = ("book ", "add book ", "save book ", "find book ")
    _bk_prefix = next((p for p in _book_prefixes if body_lower.startswith(p)), None)
    if _bk_prefix:
        bk_query = body[len(_bk_prefix):].strip()
        if not bk_query:
            resp.message("📚 Tell me what to find — e.g.\n*book The Midnight Library*\nor\n*book 9781399401739*")
            return str(resp)
        # Normalise ISBN: strip dashes/spaces, check 13 or 10 digits
        _isbn_raw = re.sub(r"[\s\-]", "", bk_query)
        if re.match(r"^(978|979)\d{10}$", _isbn_raw) or re.match(r"^\d{10}$", _isbn_raw):
            bk_isbn = _isbn_raw
            bk_info = _lookup_book_by_isbn(bk_isbn)
        else:
            bk_info = _lookup_book_by_title(bk_query)
            bk_isbn = bk_info.get("isbn", "") if bk_info else ""
        if not bk_info or not bk_info.get("title"):
            resp.message(f"📚 Couldn't find *{bk_query}* — try a more specific title or the ISBN printed below the barcode.")
            return str(resp)
        # Save to wa_saves
        try:
            plain_number = from_number.replace("whatsapp:", "").strip()
            existing = (lib._sb().table("wa_saves").select("id")
                        .in_("from_number", [from_number, plain_number])
                        .eq("url", f"book:{bk_isbn}").limit(1).execute().data)
            if existing:
                already = True
            else:
                already = False
                lib._sb().table("wa_saves").insert({
                    "from_number": plain_number,
                    "url":         f"book:{bk_isbn}",
                    "title":       bk_info["title"],
                    "summary":     json.dumps(bk_info),
                    "status":      "wishlist",
                }).execute()
                try:
                    lib.books_upsert(plain_number, {**bk_info, "isbn": bk_isbn, "source": "whatsapp_text"})
                except Exception:
                    pass
        except Exception:
            already = False
        user_token = _wa_user_token(from_number)
        msg = f"📚 *{bk_info['title']}*"
        if bk_info.get("author"):
            msg += f"\nby {bk_info['author']}"
        cr = bk_info.get("communityRating")
        if cr and cr.get("avg"):
            msg += f"\n{'⭐' * round(cr['avg'])} {cr['avg']}/5"
            if cr.get("count"): msg += f" ({cr['count']:,} ratings)"
        if bk_info.get("description"):
            msg += f"\n\n_{bk_info['description'][:220].strip()}…_"
        if already:
            msg += f"\n\n_(already in your books)_"
        else:
            msg += f"\n\n📚 Saved to My Books: miru.humanagency.co/?screen=scan&token={user_token}"
        resp.message(msg)
        return str(resp)

    # ── FIND SAVE command: search wa_saves ───────────────────────────────────
    if body_lower.startswith("find save ") or body_lower.startswith("search saves ") or body_lower.startswith("find saves "):
        prefix = next(p for p in ("find save ", "search saves ", "find saves ") if body_lower.startswith(p))
        query = body_lower[len(prefix):].strip()
        if query:
            try:
                hits = lib.saves_search(query, from_number=from_number, hits_per_page=8)
            except Exception:
                hits = []
            if not hits:
                resp.message(f"🔍 Nothing found for \"{query}\" in your saves.\n\nTip: search in the app at miru.humanagency.co")
            else:
                lines = [f"🔍 Saves matching \"{query}\":"]
                for h in hits[:6]:
                    title = h.get("title") or h.get("doc_title") or "Untitled"
                    summary = (h.get("summary") or "")[:120]
                    lines.append(f"\n*{title}*")
                    if summary:
                        lines.append(f"   {summary}")
                resp.message("\n".join(lines))
            return str(resp)

    # ── LIST command: on-demand saves summary ─────────────────────────────────
    if cmd_up == "LIST":
        try:
            rows = (lib._sb().table("wa_saves").select("id,title,url,status")
                    .eq("from_number", from_number)
                    .in_("status", ["pending", "remind"])
                    .order("created_at").limit(9).execute().data)
        except Exception:
            rows = []
        if not rows:
            resp.message("No pending saves. Send any URL to save it.")
        else:
            _PENDING_DIGEST[from_number] = [
                {"id": r["id"], "title": r.get("title") or "Untitled", "url": r.get("url", "")}
                for r in rows
            ]
            lines = [f"📋 {len(rows)} pending:\n"]
            for i, r in enumerate(rows, 1):
                lines.append(f"{i}. {(r.get('title') or r.get('url',''))[:60]}")
            lines.append("\nReply: *1 READ*, *2 SKIP*, *3 REMIND Monday*")
            resp.message("\n".join(lines))
        return str(resp)

    # ── MY BOOKS command ─────────────────────────────────────────────────────
    _MY_BOOKS_TRIGGERS = {
        "my books", "books", "my reading list", "reading list",
        "books i want to read", "my wishlist", "book list",
        "show my books", "list my books",
    }
    if body_lower in _MY_BOOKS_TRIGGERS or body_lower.startswith("my books") or body_lower.startswith("books i "):
        plain_number = from_number.replace("whatsapp:", "").strip()
        wa_number    = f"whatsapp:{plain_number}"
        try:
            rows = (lib._sb().table("wa_saves")
                    .select("title,url,status,created_at")
                    .in_("from_number", [from_number, plain_number, wa_number])
                    .like("url", "book:%")
                    .order("created_at", desc=True).limit(12).execute().data)
        except Exception:
            rows = []
        if not rows:
            user_token = _wa_user_token(from_number)
            resp.message(
                "📚 No books saved yet.\n\n"
                "• Send *book The Midnight Library* to add one\n"
                "• Or scan a barcode: miru.humanagency.co"
            )
        else:
            status_map = {"wishlist": "📌 want", "reading": "📖 reading", "read": "✅ read"}
            lines = [f"📚 Your books ({len(rows)}):"]
            for r in rows[:10]:
                title = (r.get("title") or r.get("url","")).replace("📚 ","")[:50]
                st = status_map.get(r.get("status",""), "")
                lines.append(f"\n{'📚' if not st else st.split()[0]} *{title}*" + (f"  _{st.split(None,1)[-1] if st else ''}_" if st and " " in st else ""))
            if len(rows) > 10:
                lines.append(f"\n…+{len(rows)-10} more. See all: miru.humanagency.co")
            resp.message("\n".join(lines))
        return str(resp)

    # ── PRICE CHECK / COMPARE — strip prefix so "price olive oil" works ──────
    _PRICE_PREFIX_RE = re.compile(
        r'^(?:price\s+|compare\s+prices?\s+|compare\s+|price\s+check\s+|check\s+price\s+(?:of\s+)?|how\s+much\s+is\s+)', re.I
    )
    _price_match = _PRICE_PREFIX_RE.match(body_lower.strip())
    if _price_match:
        product_q = body_lower[_price_match.end():].strip()
        if product_q:
            reply = whatsapp_product_format(product_q)
            resp.message(reply)
            return str(resp)

    # ── Natural-language school queries ──────────────────────────────────────
    _SCHOOL_NL_RE = re.compile(
        r"\b(?:school|class|teacher|term|half[\s-]?term|assembly|parents(?:'|\u2019)?\s*evening|"
        r'sports\s*day|nativity|play\s*rehearsal|inset\s*day|'
        r'what\s+time\s+does\s+(school|class)\s+(start|finish|end|open|close)|'
        r'when\s+does\s+school|school\s+times|school\s+hours|school\s+run|'
        r'school\s+event|school\s+news|school\s+week|what\'s\s+on\s+at\s+school)\b',
        re.I
    )
    if _SCHOOL_NL_RE.search(body) and from_number not in school_service._SETUP_STATE:
        try:
            # Route natural language school queries to school_service
            # Normalise: if it doesn't start with "school", prepend it so handle_wa_school can route it
            _school_body = body if body_lower.startswith("school") else "school week " + body
            reply = school_service.handle_wa_school(from_number, _school_body)
        except Exception as _e:
            reply = ""
        if reply:
            resp.message(reply)
            return str(resp)

    # ── School comms ──────────────────────────────────────────────────────────
    if body_lower.startswith("school") or from_number in school_service._SETUP_STATE:
        try:
            reply = school_service.handle_wa_school(from_number, body)
        except Exception as _e:
            print(f"[school] handle_wa_school error: {_e}")
            import traceback; traceback.print_exc()
            reply = f"Sorry, something went wrong with school comms: {_e}"
        if reply:
            resp.message(reply)
            return str(resp)

    postcode, fuel, radius, retailer = parse_sms(body)
    body_words = set(body.lower().split())

    # ── Places query ──────────────────────────────────────────────────────────
    if body_words & _PLACES_WORDS:
        pc_m = re.search(r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body.upper())
        if pc_m:
            places_q = pc_m.group(1).strip()
            # Keep anything that isn't the postcode or a stop word as a service filter
            service_filter = re.sub(
                r'[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}', '', body, flags=re.I
            )
            service_filter = re.sub(
                r'\b(?:places|services|local|near|nearby|around)\b', '', service_filter, flags=re.I
            ).strip()
        elif re.search(r'\bme\b|\bmy\s+area\b|\bmy\s+postcode\b', body_lower):
            # "coffee near me" or "GP near me" — try food handler first (uses home postcode)
            _food_r = _wa_food_find(body, from_number)
            if _food_r:
                resp.message(_food_r)
                return str(resp)
            # Not a food query — try home postcode for places
            _home_pc = _get_wa_home_postcode(from_number)
            if _home_pc:
                places_q = _home_pc
                service_filter = re.sub(
                    r'\b(?:places|services|local|near|nearby|around|me|my\s+area|my\s+postcode)\b', '', body, flags=re.I
                ).strip()
                reply = whatsapp_places_format(places_q, service_filter=service_filter)
                resp.message(reply)
                return str(resp)
            service_filter = re.sub(
                r'\b(?:places|services|local|near|nearby|around|me|my\s+area|my\s+postcode)\b', '', body, flags=re.I
            ).strip()
            label = service_filter if service_filter else "local services"
            resp.message(
                f"📍 To find {label} near you, reply with your postcode:\n\n"
                f"  *{label} KT15 3RL*\n\n"
                f"Or set your home postcode at miru.humanagency.co 🏠"
            )
            return str(resp)
        else:
            places_q = re.sub(
                r'\b(?:places|services|local|near|nearby|around)\b', '', body, flags=re.I
            ).strip()
            service_filter = ""
        if places_q:
            cache_key = f"places_wa:{places_q.lower()}:{service_filter.lower()}"
            cached = _WA_CACHE.get(cache_key)
            if cached and (time.time() - cached[0]) < _WA_CACHE_TTL:
                resp.message(cached[1])
                return str(resp)
            reply = whatsapp_places_format(places_q, service_filter=service_filter)
            _WA_CACHE[cache_key] = (time.time(), reply)
            resp.message(reply)
            return str(resp)
        else:
            resp.message("Please include a postcode or place name, e.g.:\nplaces KT1 2BA")
            return str(resp)

    # ── Councillor contact query ───────────────────────────────────────────────
    if body_words & _COUNCILLOR_WORDS or body.lower().startswith("councillor"):
        pc_m = re.search(r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body.upper())
        cllr_postcode = pc_m.group(1).replace(" ", "") if pc_m else postcode
        if cllr_postcode:
            cache_key = f"councillor:{cllr_postcode}"
            cached = _WA_CACHE.get(cache_key)
            if cached and (time.time() - cached[0]) < 3600:
                resp.message(cached[1])
                return str(resp)
            reply = _wa_councillor_lookup(cllr_postcode)
            _WA_CACHE[cache_key] = (time.time(), reply)
            resp.message(reply)
            return str(resp)
        else:
            resp.message("Include your postcode to find your councillor, e.g.:\ncouncillor KT16 0DA")
            return str(resp)

    # ── Election results query ─────────────────────────────────────────────────
    if body_words & _RESULTS_WORDS:
        pc_m = re.search(r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body.upper())
        res_postcode = pc_m.group(1).replace(" ", "") if pc_m else None
        if res_postcode:
            cache_key = f"results:{res_postcode}"
            cached = _WA_CACHE.get(cache_key)
            if cached and (time.time() - cached[0]) < 300:  # 5-min cache for results
                resp.message(cached[1])
                return str(resp)
            reply = whatsapp_results_format(res_postcode)
            _WA_CACHE[cache_key] = (time.time(), reply)
            resp.message(reply)
            return str(resp)
        else:
            resp.message("Please include your postcode, e.g.:\nVoting Results SW1A 1AA")
            return str(resp)

    # ── Elections query — election day passed, so always return results ────────
    if body_words & _ELECTION_WORDS:
        pc_m = re.search(r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body.upper())
        elec_postcode = pc_m.group(1).replace(" ", "") if pc_m else None
        if elec_postcode:
            cache_key = f"results:{elec_postcode}"
            cached = _WA_CACHE.get(cache_key)
            if cached and (time.time() - cached[0]) < 300:
                resp.message(cached[1])
                return str(resp)
            reply = whatsapp_results_format(elec_postcode)
            _WA_CACHE[cache_key] = (time.time(), reply)
            resp.message(reply)
            return str(resp)
        else:
            resp.message("Please include your postcode to get election results, e.g.:\nelection KT16 0DA")
            return str(resp)

    # ── Document / receipt / warranty / book-note search ─────────────────────
    _DOC_Q = re.compile(
        r'\b(find|search|look up|show me)\b.{0,30}\b(save|saves|doc|docs|document|receipt|receipts|library|note|notes)\b'
        r'|\b(when did i (buy|purchase)|did i buy|my receipt for|receipt for|how much (did i pay|was|is it)|what did i (buy|pay|spend))\b'
        r'|\b(is .{1,40} (under )?warranty|warranty for|check (my )?warranty|when does .{1,40} warranty expire)\b'
        r'|\bwhat (did|does|is in) (my|the) (document|doc|letter|contract|agreement|receipt|note|lease)\b',
        re.I,
    )
    if _DOC_Q.search(body):
        resp.message(_wa_doc_search(from_number, body))
        return str(resp)

    # ── Book reading note: "note for Atomic Habits: key insight text" ─────────
    _BOOK_NOTE_RE = re.compile(r'^note\s+for\s+(.+?):\s*(.+)', re.I | re.DOTALL)
    _bnm = _BOOK_NOTE_RE.match(body.strip())
    if _bnm:
        _book_title = _bnm.group(1).strip()[:100]
        _note_text  = _bnm.group(2).strip()[:600]
        try:
            lib._sb().table("wa_saves").insert({
                "from_number": from_number,
                "title":       f"📖 {_book_title}",
                "summary":     _note_text,
                "url":         f"book-note:{_book_title.lower().replace(' ','-')}",
                "status":      "pending",
            }).execute()
            resp.message(f"📖 Note saved for *{_book_title}*\n\n_{_note_text[:120]}{'…' if len(_note_text)>120 else ''}_")
        except Exception as _bne:
            resp.message(f"Sorry, couldn't save the note: {_bne}")
        return str(resp)

    # ── Fast save-filter matching (before Groq — catches "X saved last week" etc.) ──
    _SAVE_FILTER_WORDS = {
        "books": "books", "book": "books",
        "wine": "wine", "wines": "wine",
        "recipe": "recipes", "recipes": "recipes",
        "menu": "menus", "menus": "menus",
        "restaurant": "restaurants", "restaurants": "restaurants",
        "video": "videos", "videos": "videos",
        "product": "products", "products": "products",
        "saves": "all", "links": "all",
    }
    _TIMEFRAME_WORDS = {
        "today": "today", "yesterday": "yesterday",
        "last week": "last_week", "this week": "last_week",
        "last month": "last_month", "this month": "last_month",
    }
    # Trigger words that indicate "show me my saved items" context
    _SAVES_CONTEXT = re.compile(
        r'\b(saved|my saves?|show me|see my|list my|get my|find my|all my|i saved|i have saved)\b', re.I
    )
    _bl = body_lower.strip()
    _sf_filter, _sf_timeframe, _sf_author = None, "all", ""
    _by_m = re.search(r'\bby\s+([a-z ]{3,40}?)(?:\s+saved|\s+last|\s+this|\s+today|$)', _bl)
    if _by_m:
        _sf_author = _by_m.group(1).strip()
    for _fw, _fval in _SAVE_FILTER_WORDS.items():
        if re.search(r'\b' + re.escape(_fw) + r'\b', _bl):
            _sf_filter = _fval
            break
    if _sf_filter:
        # Only route here if there's a "saved/show me/my" indicator OR a timeframe word
        _has_timeframe = any(tw in _bl for tw in _TIMEFRAME_WORDS)
        _has_context   = bool(_SAVES_CONTEXT.search(_bl)) or bool(_sf_author)
        if _has_timeframe or _has_context:
            for _tw, _tval in _TIMEFRAME_WORDS.items():
                if _tw in _bl:
                    _sf_timeframe = _tval
                    break
            msg = _wa_search_saves(from_number, _sf_filter, _sf_timeframe, _sf_author)
            resp.message(msg)
            return str(resp)

    # ── Natural language fallback — Groq intent classification ───────────────
    _intent = _wa_classify_intent(body)
    if _intent:
        _itype = _intent.get("intent")
        if _itype == "book_lookup":
            bk_query = _intent.get("query", "").strip() or body.strip()
            _isbn_raw = re.sub(r"[\s\-]", "", bk_query)
            if re.match(r"^(978|979)\d{10}$", _isbn_raw) or re.match(r"^\d{10}$", _isbn_raw):
                bk_info = _lookup_book_by_isbn(_isbn_raw)
                bk_isbn = _isbn_raw
            else:
                bk_info = _lookup_book_by_title(bk_query)
                bk_isbn = bk_info.get("isbn", "") if bk_info else ""
            if not bk_info or not bk_info.get("title"):
                resp.message(f"📚 Couldn't find *{bk_query}* — try: *book the midnight library* or the ISBN number.")
                return str(resp)
            try:
                plain_number = from_number.replace("whatsapp:", "").strip()
                existing = (lib._sb().table("wa_saves").select("id")
                            .in_("from_number", [from_number, plain_number])
                            .eq("url", f"book:{bk_isbn}").limit(1).execute().data)
                already = bool(existing)
                if not already:
                    lib._sb().table("wa_saves").insert({
                        "from_number": plain_number, "url": f"book:{bk_isbn}",
                        "title": bk_info["title"], "summary": json.dumps(bk_info), "status": "wishlist",
                    }).execute()
                    try: lib.books_upsert(plain_number, {**bk_info, "isbn": bk_isbn, "source": "whatsapp_text"})
                    except Exception: pass
            except Exception: already = False
            user_token = _wa_user_token(from_number)
            msg = f"📚 *{bk_info['title']}*"
            if bk_info.get("author"): msg += f"\nby {bk_info['author']}"
            cr = bk_info.get("communityRating")
            if cr and cr.get("avg"):
                msg += f"\n{'⭐' * round(cr['avg'])} {cr['avg']}/5"
                if cr.get("count"): msg += f" ({cr['count']:,} ratings)"
            if bk_info.get("description"): msg += f"\n\n_{bk_info['description'][:220].strip()}…_"
            msg += f"\n\n📚 {'Already in' if already else 'Saved to'} My Books: miru.humanagency.co/?screen=scan&token={user_token}"
            resp.message(msg)
            return str(resp)
        elif _itype == "worth_it":
            body = "WORTH IT"
            cmd_up = "WORTH IT"
            body_lower = "worth it"
        elif _itype == "shopping_list":
            body = "SHOPPING LIST"
            cmd_up = "SHOPPING LIST"
            body_lower = "shopping list"
        elif _itype == "search_saves":
            msg = _wa_search_saves(
                from_number,
                filter_type=_intent.get("filter", "all"),
                timeframe=_intent.get("timeframe", "all"),
                author=_intent.get("author") or "",
            )
            resp.message(msg)
            return str(resp)
        elif _itype == "my_saves":
            body = "LIST"
            cmd_up = "LIST"
            body_lower = "list"
        elif _itype == "my_link":
            body = "MY LINK"
            cmd_up = "MY LINK"
            body_lower = "my link"
        elif _itype == "train":
            _from_stn = (_intent.get("from") or "").strip()
            _to_stn   = (_intent.get("to") or "").strip()
            if _from_stn:
                _train_reply = _wa_train_format(_from_stn, _to_stn)
                if _train_reply:
                    resp.message(_train_reply)
                    return str(resp)
                resp.message(f"🚂 Couldn't find '{_from_stn}' — try: *train waterloo to lewisham*")
                return str(resp)
        elif _itype == "tube":
            _from_stn = (_intent.get("from") or "").strip()
            _to_stn   = (_intent.get("to") or "").strip()
            _query    = (_intent.get("query") or "status").strip()
            if _from_stn and _to_stn:
                resp.message(get_tube_journey(_from_stn, _to_stn))
            else:
                resp.message(handle_tube_command(f"tube {_query}", from_number))
            return str(resp)
        elif _itype == "food":
            _ftype    = (_intent.get("food_type") or "coffee").strip()
            _fpc      = (_intent.get("postcode") or "").strip()
            _fcheap   = _intent.get("cheap", False)
            _synthetic = f"{'cheap ' if _fcheap else ''}{_ftype} {_fpc}".strip()
            _food_reply = _wa_food_find(_synthetic, from_number)
            if _food_reply:
                resp.message(_food_reply)
                return str(resp)

    # ── Re-check commands if intent classifier redirected ─────────────────────
    if cmd_up in ("WORTH IT", "WORTH IT?", "REVIEWS", "BOOK REVIEW", "THOUGHTS"):
        plain_number = from_number.replace("whatsapp:", "").strip()
        wa_number    = from_number if from_number.startswith("whatsapp:") else f"whatsapp:{from_number}"
        try:
            rows = (lib._sb().table("wa_saves").select("id,title,url,summary")
                    .in_("from_number", [from_number, plain_number, wa_number])
                    .like("url", "book:%")
                    .order("created_at", desc=True).limit(1).execute().data)
        except Exception: rows = []
        if not rows:
            resp.message("📚 No books saved yet — send *book <title>* to add one, then ask again.")
            return str(resp)
        row = rows[0]
        bk = {}
        try: bk = json.loads(row.get("summary") or "{}")
        except Exception: pass
        book_label  = bk.get("title") or row.get("title") or "your book"
        book_rating = (bk.get("communityRating") or {}).get("avg")
        description = bk.get("description", "")
        genre       = bk.get("subjects", "")
        author      = bk.get("author", "")
        context = f"Book: {book_label}"
        if author:      context += f"\nAuthor: {author}"
        if genre:       context += f"\nGenre/subjects: {genre}"
        if book_rating: context += f"\nGoogle Books rating: {book_rating}/5"
        if description: context += f"\nDescription: {description[:400]}"
        try:
            from groq import Groq as _Groq2
            verdict = _Groq2(api_key=os.environ.get("GROQ_API_KEY","")).chat.completions.create(
                model="llama3-8b-8192", temperature=0.4,
                system=(
                    "You are a concise book critic. Given book metadata, "
                    "write exactly 4 lines:\n"
                    "Line 1: ✅ Fans say: [what the description/genre suggests readers will love — max 15 words]\n"
                    "Line 2: ⚠️ Critics say: [likely criticism based on genre/style — max 15 words]\n"
                    "Line 3: 🎯 Best for: [who this book suits — max 10 words]\n"
                    "Line 4: 📖 Verdict: Worth it / Skip it / Depends on taste — one sentence reason\n"
                    "If you don't have enough information, be honest and say so on line 1. No other text."
                ),
                messages=[{"role": "user", "content": context}],
                max_tokens=220,
            ).choices[0].message.content.strip()
        except Exception: verdict = ""
        if not verdict:
            resp.message(f"📚 Couldn't generate a review for {book_label} right now.")
            return str(resp)
        rating_line = f"\n⭐ {book_rating}/5 on Google Books" if book_rating else ""
        resp.message(f"📚 {book_label}{rating_line}\n\n{verdict}")
        return str(resp)

    if cmd_up in ("LIST",):
        try:
            plain_number = from_number.replace("whatsapp:", "").strip()
            wa_number    = from_number if from_number.startswith("whatsapp:") else f"whatsapp:{from_number}"
            rows = (lib._sb().table("wa_saves").select("id,title,url,status")
                    .in_("from_number", [from_number, plain_number, wa_number])
                    .in_("status", ["pending", "remind"]).order("created_at", desc=True).limit(9).execute().data)
            if not rows:
                resp.message("📂 No pending saves. Send a link to save something!")
                return str(resp)
            lines = [f"{i+1}. {r.get('title') or r.get('url','?')[:60]}" for i, r in enumerate(rows)]
            resp.message("📂 Your saves:\n" + "\n".join(lines) + "\n\nReply *1 READ*, *2 SKIP*, etc.")
            return str(resp)
        except Exception: pass

    if body_lower in ("my link", "link", "my saves link", "get link", "access", "my access"):
        user_token = _wa_user_token(from_number)
        resp.message(f"🔗 Your personal Miru link:\nmiru.humanagency.co/?screen=saves&token={user_token}")
        return str(resp)

    # ── Train departures query ────────────────────────────────────────────────
    _TRAIN_RE = re.compile(
        r'\b(?:next\s+)?trains?\s+(?:from\s+)?(.+?)(?:\s+to\s+(.+))?$'
        r'|\bnext\s+train\s+(.+?)(?:\s+to\s+(.+))?$',
        re.I
    )
    _tm = _TRAIN_RE.match(body_lower.strip())
    if _tm or re.match(r'^(?:next\s+)?train\b', body_lower.strip()):
        # Parse from/to station names
        if _tm:
            from_stn = (_tm.group(1) or _tm.group(3) or "").strip().lower()
            to_stn   = (_tm.group(2) or _tm.group(4) or "").strip().lower()
        else:
            from_stn = re.sub(r'^(?:next\s+)?train\s+(?:from\s+)?', '', body_lower.strip()).strip()
            to_stn = ""
        # Strip "to X" from from_stn if not already split
        if not to_stn and " to " in from_stn:
            parts = from_stn.split(" to ", 1)
            from_stn, to_stn = parts[0].strip(), parts[1].strip()
        if from_stn:
            wa_train_reply = _wa_train_format(from_stn, to_stn)
            if wa_train_reply:
                resp.message(wa_train_reply)
                return str(resp)

    # ── Tube query ───────────────────────────────────────────────────────────
    if body_lower.strip().startswith("tube"):
        resp.message(handle_tube_command(body, from_number))
        return str(resp)

    # ── Brand intel: "brand Nike" / "intel Walkers" / "about Oreo" ─────────────
    _brand_m = re.match(r'^(?:brand|intel|about)\s+(.{2,60})$', body.strip(), re.I)
    if _brand_m:
        resp.message(_wa_brand_card(_brand_m.group(1).strip()))
        return str(resp)

    # ── Food & drink discovery ───────────────────────────────────────────────
    _food_reply = _wa_food_find(body, from_number)
    if _food_reply:
        resp.message(_food_reply)
        return str(resp)

    # ── Guard: conversational replies must not fall through to product search ────
    _CONVERSATIONAL = frozenset({
        "already there","its there","already set","already done","set already","its already there",
        "ok","okay","k","ok thanks","ok cool","ok great","thanks","thank you","ty","cheers","ta",
        "got it","cool","great","good","fine","alright","sure","right","noted","sorted","done",
        "yes","no","nope","yep","yeah","nah","correct","exactly","perfect",
        "hmm","hm","oh","ah","wait","really","seriously","come on","lol","haha",
        "already","never mind","nevermind","forget it","ignore that",
    })
    _bl = body_lower.strip()
    if _bl in _CONVERSATIONAL or (
            len(_bl.split()) <= 4 and not any(c.isdigit() for c in _bl) and
            _bl.split()[0] in {"already","ok","okay","yes","no","yeah","nope","thanks","cool",
                                "done","right","hmm","oh","sure","got","alright","wait","never",
                                "sorted","cheers","fine","great","noted","nah","correct","lol"}):
        _pending_check = _get_wa_pending_intent(from_number)
        if _pending_check:
            # They said something conversational while we're waiting for their postcode
            _clear_wa_pending_intent(from_number)
        resp.message(_CLARIFY_MSG)
        return str(resp)

    # Decide: fuel query or product query
    # Product query if: no postcode at all, OR postcode present but there's
    # substantial non-fuel text alongside it
    is_product = False
    product_name = None
    if not postcode:
        # No postcode found — treat whole message as product
        is_product = True
        product_name = body
    else:
        # Postcode found — check how much text remains after removing postcode + fuel words
        remaining = re.sub(r'[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}', '', body.upper()).strip()
        extra_words = [w for w in remaining.split() if w.lower() not in _FUEL_WORDS and not w.isdigit()]
        if extra_words:
            is_product = True
            product_name, _ = _split_product_postcode(body)

    if is_product and product_name and len(product_name.strip()) > 1:
        # Don't treat question/conversational messages as product queries — redirect to HELP
        _pn_words = product_name.strip().split()
        _question_starters = {"what","why","how","when","who","can","could","would","should",
                               "is","are","do","does","help","please","show","tell","get","give"}
        if _pn_words and (_pn_words[0].lower() in _question_starters or "?" in product_name):
            resp.message(_CLARIFY_MSG)
            return str(resp)
        _, loc_postcode = _split_product_postcode(body)
        cache_key = f"product:{product_name.lower().strip()}:{loc_postcode or ''}"
        cached = _WA_CACHE.get(cache_key)
        if cached and (time.time() - cached[0]) < _WA_CACHE_TTL:
            resp.message(cached[1])
            return str(resp)
        reply = whatsapp_product_format(product_name.strip(), loc_postcode)
        _WA_CACHE[cache_key] = (time.time(), reply)
        resp.message(reply)
        return str(resp)

    # Fuel query
    if not postcode:
        resp.message(_CLARIFY_MSG)
        return str(resp)

    cache_key = f"fuel:{postcode}:{fuel}:{radius}:{retailer or ''}"
    cached = _WA_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < _WA_CACHE_TTL:
        print(f"WhatsApp cache hit for {cache_key}")
        resp.message(cached[1])
        return str(resp)

    reply = whatsapp_search_and_format(postcode, fuel, radius, retailer)
    _WA_CACHE[cache_key] = (time.time(), reply)
    resp.message(reply)
    return str(resp)


@app.route("/debug/postcode/<postcode>")
def debug_postcode(postcode):
    import requests as req
    results = {}
    latlon = postcode_to_latlon(postcode)
    results["latlon"] = latlon
    if not latlon:
        return jsonify(results)
    lat, lon = latlon

    # Raw Land Registry response
    pc = postcode.strip().replace(" ", "%20")
    lr_url = (f"https://landregistry.data.gov.uk/data/ppi/transaction-record.json"
              f"?propertyAddress.postcode={pc}&_pageSize=5&_sort=-transactionDate")
    try:
        r = req.get(lr_url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        results["lr_status"] = r.status_code
        results["lr_raw"] = r.text[:500]
    except Exception as e:
        results["lr_error"] = str(e)

    # Raw Overpass response
    radius_m = 5000
    query = f'[out:json][timeout:8];(node["amenity"="pub"](around:{radius_m},{lat},{lon}););out body 3;'
    try:
        r2 = req.post("https://overpass-api.de/api/interpreter",
                      data=query, timeout=9,
                      headers={"User-Agent": "Mozilla/5.0"})
        results["osm_status"] = r2.status_code
        results["osm_raw"] = r2.text[:300]
    except Exception as e:
        results["osm_error"] = str(e)

    return jsonify(results)


@app.route("/debug/share/<company>")
def debug_share(company):
    from search import _fetch_share_price
    return jsonify(_fetch_share_price(company))


@app.route("/admin/debug")
def admin_debug():
    db_url = analytics._DB_URL
    masked = db_url[:40] + "..." if db_url else "NOT SET"
    try:
        import psycopg2
        conn = psycopg2.connect(db_url, connect_timeout=5)
        conn.close()
        db_status = "Connected OK"
    except Exception as e:
        db_status = f"Error: {e}"
    return jsonify({"DATABASE_URL": masked, "db_status": db_status, "analytics_db_ok": analytics._db_ok})


@app.route("/health")
def health():
    stations = get_stations()
    return {"status": "ok", "stations_loaded": len(stations)}


# ── Admin: Gmail import test ──────────────────────────────────────────────────

_ADMIN_KEY = os.environ.get("ADMIN_KEY", "miru-admin-2026")



_GMAIL_TEST_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Miru — My Details Import</title>
<style>
  body{font-family:system-ui,sans-serif;max-width:680px;margin:40px auto;padding:0 20px;background:#f8fafc;color:#1e293b}
  h1{font-size:20px;font-weight:800;margin-bottom:4px}
  .sub{font-size:13px;color:#64748b;margin-bottom:24px}
  label{font-size:12px;font-weight:700;color:#475569;display:block;margin-bottom:4px;margin-top:16px}
  input,select{width:100%;border:1.5px solid #cbd5e1;border-radius:10px;padding:10px 12px;font-size:14px;box-sizing:border-box;background:#fff;color:#1e293b;margin-bottom:4px}
  button{background:#065f46;color:#fff;border:none;border-radius:10px;padding:11px 24px;font-size:14px;font-weight:700;cursor:pointer;margin-top:14px;width:100%}
  button:hover{background:#047857}
  #json-out{margin-top:20px;display:none}
  pre{background:#1e293b;color:#e2e8f0;border-radius:10px;padding:16px;font-size:13px;overflow-x:auto;white-space:pre-wrap;word-break:break-all}
  .copy-btn{background:#2563eb;margin-top:8px}
  .hint{font-size:12px;color:#64748b;margin-top:10px;padding:12px;background:#f0fdf4;border-radius:8px;border:1px solid #bbf7d0}
</style>
</head>
<body>
<h1>📋 My Details Import Builder</h1>
<p class="sub">Fill in the fields, click Build JSON, then paste into Miru → My Area → 📋 Import.</p>

<label>TYPE</label>
<select id="f-type" onchange="updateFields()">
  <optgroup label="Home &amp; Utilities">
    <option value="home_ins">🏠 Home Insurance</option>
    <option value="energy">⚡ Energy</option>
    <option value="broadband">📡 Broadband</option>
    <option value="council_tax">🏛️ Council Tax</option>
  </optgroup>
  <optgroup label="Vehicle &amp; Personal">
    <option value="car_ins">🚗 Car Insurance</option>
    <option value="health">💳 Health / BUPA</option>
    <option value="life_ins">🛡️ Life Insurance</option>
  </optgroup>
  <optgroup label="Other">
    <option value="other">📝 Other</option>
  </optgroup>
</select>

<div id="fields-container"></div>

<button onclick="buildJson()">Build JSON</button>

<div id="json-out">
  <label>JSON — copy this and paste into Miru</label>
  <pre id="json-pre"></pre>
  <button class="copy-btn" onclick="copyJson()">📋 Copy to Clipboard</button>
  <p class="hint">In Miru: My Area → scroll to My Details → tap 📋 → paste → Import</p>
</div>

<script>
const TYPES = {
  home_ins:    {label:"Home Insurance",  icon:"🏠", fields:["Provider","Policy No","Phone","Renewal Date"]},
  energy:      {label:"Energy",          icon:"⚡", fields:["Provider","Account No","Emergency Phone","Renewal Date"]},
  broadband:   {label:"Broadband",       icon:"📡", fields:["Provider","Account No","Phone"]},
  council_tax: {label:"Council Tax",     icon:"🏛️", fields:["Council","Account No","Phone"]},
  car_ins:     {label:"Car Insurance",   icon:"🚗", fields:["Provider","Policy No","Phone","Renewal Date","Reg"]},
  health:      {label:"Health / BUPA",   icon:"💳", fields:["Provider","Member No","Phone"]},
  life_ins:    {label:"Life Insurance",  icon:"🛡️", fields:["Provider","Policy No","Phone","Renewal Date"]},
  other:       {label:"Other",           icon:"📝", fields:["Label","Detail","Phone"]},
};
function updateFields() {
  const type = TYPES[document.getElementById('f-type').value];
  document.getElementById('fields-container').innerHTML = type.fields.map(f =>
    `<label>${f.toUpperCase()}</label><input id="ff-${f.replace(/ /g,'-')}" placeholder="${f}">`
  ).join('');
  document.getElementById('json-out').style.display = 'none';
}
function buildJson() {
  const key = document.getElementById('f-type').value;
  const type = TYPES[key];
  const data = {};
  type.fields.forEach(f => {
    const v = (document.getElementById('ff-'+f.replace(/ /g,'-'))?.value || '').trim();
    if (v) data[f] = v;
  });
  const rec = {type: key, label: type.label, data};
  document.getElementById('json-pre').textContent = JSON.stringify(rec, null, 2);
  document.getElementById('json-out').style.display = '';
}
function copyJson() {
  const text = document.getElementById('json-pre').textContent;
  navigator.clipboard.writeText(text).then(() => { const b=document.querySelector('.copy-btn'); b.textContent='✓ Copied!'; setTimeout(()=>b.textContent='📋 Copy to Clipboard',2000); });
}
updateFields();
</script>
</body>
</html>"""

@app.route("/admin/gmail-test")
def admin_gmail_test():
    key = request.args.get("key", "")
    if key != _ADMIN_KEY:
        return "Unauthorized", 401
    return _GMAIL_TEST_PAGE % {"key": key}




# ── Charts ────────────────────────────────────────────────────────────────────

def _make_chart(fig):
    """Render a matplotlib figure to a PNG response."""
    import matplotlib
    matplotlib.use("Agg")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=130)
    buf.seek(0)
    import matplotlib.pyplot as plt
    plt.close(fig)
    return send_file(buf, mimetype="image/png")


@app.route("/chart")
def chart_national():
    """National average petrol & diesel price trend."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    history = _load_json(NATIONAL_HISTORY_FILE)
    if len(history) < 2:
        return "Not enough data yet — check back after a few cache refreshes (30 min each).", 404

    dates   = [datetime.fromisoformat(r["ts"]) for r in history]
    petrol  = [r["petrol_avg"] for r in history]
    diesel  = [r["diesel_avg"] for r in history if r.get("diesel_avg")]
    d_dates = [datetime.fromisoformat(r["ts"]) for r in history if r.get("diesel_avg")]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(dates, petrol, color="#e74c3c", linewidth=2, label="Petrol (national avg)")
    if diesel:
        ax.plot(d_dates, diesel, color="#2980b9", linewidth=2, label="Diesel (national avg)")

    ax.set_title("UK Fuel Price Trend — National Average", fontsize=14, fontweight="bold")
    ax.set_ylabel("Price (pence/litre)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b\n%H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=min(petrol) - 2)
    fig.tight_layout()
    return _make_chart(fig)


@app.route("/chart/<postcode>")
@app.route("/chart/<postcode>/<fuel>")
def chart_postcode(postcode, fuel="petrol"):
    """Cheapest vs area-average price trend for a specific postcode."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    postcode = postcode.upper().replace(" ", "")
    fuel = fuel.lower()
    all_history = _load_json(POSTCODE_HISTORY_FILE)
    if not isinstance(all_history, dict):
        all_history = {}

    key = f"{postcode}_{fuel}"
    entries = all_history.get(key, [])
    if len(entries) < 2:
        return (
            f"Not enough data for {postcode} {fuel} yet. "
            "Search for this postcode a few times and check back.", 404
        )

    dates    = [datetime.fromisoformat(e["ts"]) for e in entries]
    cheapest = [e["cheapest"] for e in entries]
    avg      = [e["avg"] for e in entries]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(dates, avg,      color="#95a5a6", linewidth=2, linestyle="--", label="Area average")
    ax.plot(dates, cheapest, color="#e74c3c", linewidth=2.5, label="Cheapest nearby")
    ax.fill_between(dates, cheapest, avg, alpha=0.12, color="#e74c3c", label="Saving zone")

    fuel_label = "Petrol" if fuel == "petrol" else "Diesel"
    ax.set_title(f"{fuel_label} Price Trend near {postcode}", fontsize=14, fontweight="bold")
    ax.set_ylabel("Price (pence/litre)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b\n%H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    ax.legend()
    ax.grid(True, alpha=0.3)
    all_vals = cheapest + avg
    ax.set_ylim(min(all_vals) - 1, max(all_vals) + 1)
    fig.tight_layout()
    return _make_chart(fig)


# ── Startup pre-warm (runs when gunicorn imports the module) ──────────────────
import threading as _threading

def _prewarm():
    try:
        _get_elections()
    except Exception:
        pass

_threading.Thread(target=_prewarm, daemon=True).start()


@app.route("/api/crime")
def api_crime():
    """Street-level crime stats from police.uk for a postcode (last 3 months)."""
    result = _resolve_postcode(request.args.get("postcode", ""))
    if not result:
        return jsonify({"error": "Postcode not found."}), 404
    postcode, lat, lon, pc_fmt = result
    from search import fetch_crime_data
    data = fetch_crime_data(lat, lon)
    return jsonify(data)


@app.route("/api/planning")
def api_planning():
    """Planning applications from planning.data.gov.uk for a postcode's council area."""
    raw_pc = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if not raw_pc:
        return jsonify({"error": "Postcode required."}), 400
    result = _resolve_postcode(raw_pc)
    if not result:
        return jsonify({"error": "Postcode not found."}), 404
    postcode, lat, lon, pc_fmt = result
    try:
        r = requests.get(f"https://api.postcodes.io/postcodes/{raw_pc}", timeout=6)
        codes = (r.json().get("result") or {}).get("codes", {})
        council_code = codes.get("admin_district", "")
    except Exception:
        council_code = ""
    from search import fetch_planning_data
    data = fetch_planning_data(lat, lon, council_code)
    return jsonify(data)


_DIGEST_MIN_GAP_HOURS = 20   # never send to same user more than once per 20h

def _digest_last_sent_get(from_number: str) -> float:
    """Read persisted last-sent epoch from site_config. Returns 0 if never sent."""
    try:
        key = f"digest_last:{from_number}"
        row = lib._sb().table("site_config").select("value").eq("key", key).execute()
        if row.data:
            return float(row.data[0]["value"].get("ts", 0))
    except Exception:
        pass
    return 0

def _digest_last_sent_set(from_number: str, ts: float):
    """Persist last-sent epoch to site_config so it survives Railway redeploys."""
    try:
        key = f"digest_last:{from_number}"
        lib._sb().table("site_config").upsert({"key": key, "value": {"ts": ts}}).execute()
    except Exception:
        pass

@app.route("/api/wa-digest")
def wa_digest():
    """Send daily digest of pending saves. Safe to call frequently — per-user
    rate limit of 20h prevents spam regardless of cron schedule.
    Trigger via cron-job.org: GET /api/wa-digest?token=YOUR_DIGEST_TOKEN
    """
    import time as _time
    token = request.args.get("token", "")
    if not token or token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "Unauthorized"}), 401

    try:
        rows = (lib._sb().table("wa_saves").select("*")
                .in_("status", ["pending", "remind"])
                .order("created_at")
                .execute().data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not rows:
        return jsonify({"sent": 0, "message": "No pending saves"})

    by_user = {}
    for row in rows:
        fn = row["from_number"]
        by_user.setdefault(fn, []).append(row)

    twilio_sid   = os.environ.get("TWILIO_ACCOUNT_SID", "")
    twilio_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    twilio_from  = os.environ.get("TWILIO_WHATSAPP_FROM", "")

    if not all([twilio_sid, twilio_token, twilio_from]):
        return jsonify({"error": "Twilio env vars missing (TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM)"}), 500

    from twilio.rest import Client as _TwilioClient
    client = _TwilioClient(twilio_sid, twilio_token)
    sent = skipped = 0

    for from_number, saves in by_user.items():
        # Rate limit: skip if sent within the last 20 hours (persisted in Supabase)
        now = _time.time()
        last = _digest_last_sent_get(from_number)
        if now - last < _DIGEST_MIN_GAP_HOURS * 3600:
            skipped += 1
            continue
        _digest_last_sent_set(from_number, now)

        # Only include saves added since last digest (or overdue reminders)
        cutoff = last if last else (now - 24 * 3600)
        new_saves    = [s for s in saves if s.get("status") == "remind" or
                        (s.get("created_at") or "") > _time.strftime("%Y-%m-%dT%H:%M:%S", _time.gmtime(cutoff))]
        if not new_saves:
            skipped += 1
            continue

        # Cap at 9 for numbered triage replies (1–9)
        batch = new_saves[:9]
        _PENDING_DIGEST[from_number] = [
            {"id": s["id"], "title": s.get("title") or "Untitled", "url": s.get("url", "")}
            for s in batch
        ]
        lines = [f"📬 Daily digest — {len(saves)} saved\n"]
        for i, s in enumerate(batch, 1):
            title = (s.get("title") or "Untitled")[:60]
            summary = s.get("summary", "")
            first_bullet = ""
            if summary and "•" in summary:
                first_bullet = summary.split("•")[1].strip()[:90]
            url = s.get("url", "")
            lines.append(f"{i}. {title}")
            if first_bullet:
                lines.append(f"   {first_bullet}")
            if url:
                lines.append(f"   {url}")
        if len(saves) > 9:
            lines.append(f"\n+{len(saves) - 9} more in My Saves.")
        user_token = _wa_user_token(from_number)
        lines.append("\nReply: *1 READ*, *2 SKIP*, *3 REMIND Monday*")
        lines.append(f"🔗 miru.humanagency.co/?screen=saves&token={user_token}")

        # Split into chunks ≤4000 chars (WhatsApp limit)
        body_text = "\n".join(lines)
        chunks, current = [], ""
        for line in lines:
            if len(current) + len(line) + 1 > 3800:
                chunks.append(current.strip())
                current = line + "\n"
            else:
                current += line + "\n"
        if current.strip():
            chunks.append(current.strip())

        try:
            for chunk in chunks:
                client.messages.create(
                    body=chunk,
                    from_=f"whatsapp:{twilio_from}",
                    to=from_number,
                )
            sent += 1
        except Exception:
            pass

    return jsonify({"sent": sent, "skipped": skipped, "total_users": len(by_user)})


# ── School web signup ──────────────────────────────────────────────────────────

@app.route("/school/signup")
def school_signup_page():
    prefill_wa = request.args.get("wa", "")
    return render_template("school_signup.html", prefill_wa=prefill_wa)


_SCHOOL_OAUTH_REDIRECT  = "https://miru.humanagency.co/school/oauth/callback"
_MA_GMAIL_REDIRECT      = "https://miru.humanagency.co/api/myarea/gmail/callback"
_SCHOOL_OAUTH_SCOPES    = "https://www.googleapis.com/auth/gmail.readonly"


# ── My Details Gmail import ────────────────────────────────────────────────────

def _ma_gmail_wa_number(device_id: str) -> str:
    """Return whatsapp:+XXX if device_id looks like a phone number, else empty string."""
    clean = device_id.replace("whatsapp:", "").strip()
    if clean.startswith("+") and len(clean) >= 8 and clean[1:].replace(" ", "").replace("-", "").isdigit():
        return f"whatsapp:{clean}"
    return ""


@app.route("/api/myarea/gmail/connect")
def ma_gmail_connect():
    import urllib.parse
    device_id   = request.args.get("device_id", "").strip()
    from_number = request.args.get("from_number", "").strip()
    if not device_id:
        return "Missing device_id", 400
    if not _web_client_id():
        return "Gmail OAuth not configured", 503
    # Encode both device_id and from_number in state so callback can recover them
    state = f"{device_id}|||{from_number}" if from_number else device_id
    params = {
        "client_id":     _web_client_id(),
        "redirect_uri":  _MA_GMAIL_REDIRECT,
        "response_type": "code",
        "scope":         _SCHOOL_OAUTH_SCOPES,
        "access_type":   "offline",
        "prompt":        "consent",
        "state":         state,
    }
    return redirect("https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params))


@app.route("/api/myarea/gmail/callback")
def ma_gmail_callback():
    code  = request.args.get("code", "")
    state = request.args.get("state", "")
    error = request.args.get("error", "")
    if error or not code or not state:
        return redirect("/?gmail_error=cancelled#myarea")
    # Decode state: may contain from_number after "|||"
    if "|||" in state:
        device_id, from_number = state.split("|||", 1)
    else:
        device_id, from_number = state, ""
    # Fallback: if device_id itself is a phone number, use it for notifications
    if not from_number:
        from_number = device_id if _ma_gmail_wa_number(device_id) else ""
    try:
        r = requests.post("https://oauth2.googleapis.com/token", data={
            "code":          code,
            "client_id":     _web_client_id(),
            "client_secret": _web_client_secret(),
            "redirect_uri":  _MA_GMAIL_REDIRECT,
            "grant_type":    "authorization_code",
        }, timeout=15)
        tokens = r.json()
    except Exception as e:
        print(f"[ma gmail] token exchange error: {e}")
        return redirect("/?gmail_error=auth#myarea")
    refresh_token = tokens.get("refresh_token", "")
    access_token  = tokens.get("access_token", "")
    if not (refresh_token or access_token):
        return redirect("/?gmail_error=auth#myarea")
    try:
        sb = lib._sb()
        # Upsert auth fields — provider_hints is NOT included so existing hints
        # are preserved automatically when reconnecting on the same browser/device
        row_data = {
            "device_id":     device_id,
            "refresh_token": refresh_token,
            "access_token":  access_token,
            "scan_status":   "scanning",
            "pending":       [],
        }
        if from_number:
            row_data["from_number"] = from_number
        sb.table("ma_gmail_tokens").upsert(row_data, on_conflict="device_id").execute()

        # For a new device: inherit provider_hints from any other record with the
        # same from_number (e.g. user added hints on phone, now connecting on desktop)
        if from_number:
            try:
                current = sb.table("ma_gmail_tokens").select("provider_hints") \
                    .eq("device_id", device_id).execute().data
                has_hints = bool((current[0].get("provider_hints") or []) if current else [])
                if not has_hints:
                    others = sb.table("ma_gmail_tokens").select("provider_hints") \
                        .eq("from_number", from_number).neq("device_id", device_id).execute().data
                    inherited = next(
                        (r["provider_hints"] for r in others if r.get("provider_hints")), []
                    )
                    if inherited:
                        sb.table("ma_gmail_tokens").update({"provider_hints": inherited}) \
                            .eq("device_id", device_id).execute()
                        print(f"[ma gmail] inherited {len(inherited)} hints for new device")
            except Exception:
                pass
    except Exception as e:
        print(f"[ma gmail] db upsert error: {e}")
    # Sync any already-saved providers into hints before first scan
    did_for_details = from_number or device_id
    initial_hints = _ma_gmail_sync_hints_from_details(did_for_details)
    import threading
    threading.Thread(
        target=_ma_gmail_scan_bg,
        args=(device_id, access_token, refresh_token, from_number),
        kwargs={"provider_hints": initial_hints or None},
        daemon=True,
    ).start()
    return redirect("/?gmail_scan=1#myarea")


_MA_GMAIL_FATAL_ERRORS = {"invalid_grant", "invalid_client", "invalid_request", "unauthorized_client"}

def _ma_gmail_get_token(token_row: dict) -> str:
    """Return a valid access token, refreshing if needed. Returns "" on fatal auth errors."""
    at = token_row.get("access_token", "")
    rt = token_row.get("refresh_token", "")
    if not rt:
        return at
    try:
        r = requests.post("https://oauth2.googleapis.com/token", data={
            "client_id":     _web_client_id(),
            "client_secret": _web_client_secret(),
            "refresh_token": rt,
            "grant_type":    "refresh_token",
        }, timeout=10)
        d = r.json()
        new_at = d.get("access_token", "")
        if new_at:
            try:
                lib._sb().table("ma_gmail_tokens").update({"access_token": new_at}) \
                    .eq("device_id", token_row["device_id"]).execute()
            except Exception:
                pass
            return new_at
        # Fatal OAuth error — token is no longer valid
        err = d.get("error", "")
        if err in _MA_GMAIL_FATAL_ERRORS:
            print(f"[ma gmail token] fatal OAuth error '{err}' for device={token_row.get('device_id','?')!r}")
            try:
                lib._sb().table("ma_gmail_tokens").update({
                    "access_token": "", "refresh_token": "", "scan_status": "auth_error"
                }).eq("device_id", token_row["device_id"]).execute()
            except Exception:
                pass
            return ""
    except Exception:
        pass
    return at


_MA_GMAIL_QUERIES = [
    # Energy — major UK suppliers
    ("energy",      'from:octopus.energy OR from:ovoenergy.com OR from:britishgas.co.uk OR from:edf.co.uk OR from:eonenergy.com OR from:eon-next.co.uk OR from:scottishpower.co.uk OR from:sse.co.uk OR from:utilita.co.uk OR from:bulb.co.uk OR from:npower.com OR from:e.on.com OR from:shell.co.uk OR from:so.energy OR from:goodenergy.co.uk OR from:ecotricity.co.uk newer_than:3y'),
    # EE — all ee.co.uk subdomains (Groq will classify as broadband or mobile based on email)
    ("mobile",      'from:ee.co.uk OR from:eemail.ee.co.uk OR from:account.ee.co.uk OR from:my.ee.co.uk OR from:billing.ee.co.uk newer_than:3y'),
    # Mobile operators
    ("mobile",      'from:three.co.uk OR from:o2.co.uk OR from:vodafone.co.uk OR from:giffgaff.com OR from:iD.co.uk OR from:smarty.co.uk OR from:lebara.com OR from:talkmobile.co.uk newer_than:3y'),
    # Home broadband + EE broadband
    ("broadband",   'from:bt.com OR from:sky.com OR from:virginmedia.com OR from:talktalk.co.uk OR from:plusnet.com OR from:zen.co.uk OR from:hyperoptic.com OR from:now.co.uk OR from:community-fibre.co.uk OR from:gigaclear.com newer_than:3y'),
    # TV / streaming subscriptions
    ("other",       'from:tvlicensing.co.uk OR subject:"TV Licence" OR subject:"TV License" OR from:netflix.com OR from:disneyplus.com OR from:amazon.co.uk OR from:primevideo.com newer_than:3y'),
    # Water — all major UK water companies
    ("other",       'from:thameswater.co.uk OR from:thameswater.com OR from:affinitywater.co.uk OR from:southern-water.co.uk OR from:anglianwater.co.uk OR from:yorkshirewater.com OR from:unitedutilities.com OR from:severntrent.com OR from:southwest-water.co.uk OR from:dwrcymru.com OR from:bristolwater.co.uk OR from:portsmouthwater.co.uk newer_than:3y'),
    # Car / motor insurance
    ("car_ins",     'from:admiral.com OR from:directline.com OR from:aviva.com OR from:axa.co.uk OR from:lv.com OR from:hastingsdirect.com OR from:churchill.com OR from:esure.com OR from:saga.co.uk OR from:rac.co.uk OR from:aa.com OR from:confused.com OR from:gocompare.com OR from:comparethemarket.com newer_than:3y'),
    # Home insurance
    ("home_ins",    'from:johnlewisfinance.com OR from:hiscox.co.uk OR from:policyexpert.co.uk OR from:homeprotect.co.uk OR from:axa.co.uk newer_than:3y'),
    # Health / dental / pet insurance
    ("other",       'from:bupa.co.uk OR from:vitality.co.uk OR from:axa-health.co.uk OR from:denplan.co.uk OR from:petplan.co.uk OR from:moreths.co.uk newer_than:3y'),
    # Council tax — subject is specific enough
    ("council_tax", 'subject:"council tax" newer_than:3y'),
    # Broad catch-all: billing keywords Groq will filter to real accounts only
    ("other",       'subject:"your bill" OR subject:"your invoice" OR subject:"your statement" OR subject:"direct debit" OR subject:"payment confirmation" OR subject:"your account" newer_than:2y -from:amazon.co.uk -from:paypal.com -from:ebay.co.uk'),
    # Renewal / insurance catch-all
    ("other",       'subject:"your renewal" OR subject:"renewal notice" OR subject:"policy renewal" OR subject:"renewal reminder" newer_than:2y'),
]

_MA_EXTRACT_SYSTEM = """You are a data extraction assistant for UK household accounts.

Your job: decide if this email is about the RECIPIENT'S OWN active account, and if so extract the details.

Return {"skip": true} ONLY for:
- Marketing emails inviting you to switch or sign up ("Get EE broadband", "Switch to Octopus")
- Price comparison results or quote emails from a comparison site
- Pure newsletters or promotional content with no billing/account information at all

Extract (do NOT skip) if the email is clearly about an EXISTING account the recipient holds:
- Monthly bill / statement / invoice for their account
- Payment confirmation or direct debit notification
- Account welcome / confirmation for a service they signed up for
- Renewal notice for their existing policy or contract
- Any email mentioning a specific amount charged, payment due, or account reference
Note: an account number is NOT required to extract — extract the provider name even if no reference is visible.

IMPORTANT EXCEPTIONS — always extract (never skip) these even if they mention payment:
- Any email from tvlicensing.co.uk — TV Licence renewal reminders always belong to an existing licence holder. Extract provider="TV Licensing", label="TV Licence", type="other". The licence number is the account_no (format: 3 digits space 3 digits space 4 digits, e.g. "123 456 7890").

If extracting, return ONLY this JSON (omit fields you can't find):
{
  "type": "energy|broadband|mobile|car_ins|home_ins|life_ins|council_tax|other",
  "provider": "company name e.g. EE, Three, Octopus Energy, Thames Water, TV Licensing",
  "account_no": "their account or policy number — short alphanumeric only (e.g. A-201E4423, GB50302570, 2941997383). Omit if not clearly present.",
  "phone": "customer service number for their account",
  "renewal_date": "DD/MM/YYYY if shown",
  "price": "their monthly/annual cost e.g. £31.99/month",
  "label": "short label e.g. Mobile, Broadband, TV Licence, Water, Energy, Home Insurance"
}

Type rules:
- energy = gas or electricity supply
- broadband = home internet / home broadband / fibre
- mobile = mobile phone contract or SIM — use this for EE mobile, Three, O2, Vodafone, iD Mobile
- car_ins = car or motor insurance
- home_ins = home / buildings / contents insurance
- life_ins = life insurance or life cover
- council_tax = council tax
- other = water bill, TV Licence, health insurance, dental, pet, or anything else"""

_MA_PDF_EXTRACT_SYSTEM = """You are a data extraction assistant for UK household bills and account documents.

The user has uploaded a bill or account document. Extract as much useful account information as possible.

Return ONLY this JSON (omit fields you cannot find — do not guess):
{
  "type": "energy|broadband|mobile|car_ins|home_ins|life_ins|council_tax|other",
  "provider": "company or council name",
  "account_no": "account reference / policy number / licence number — short alphanumeric only, omit if not clearly present",
  "phone": "customer service or payment phone number",
  "price": "monthly or regular payment amount e.g. £142.08/month",
  "annual": "total annual amount if shown e.g. £1704.96/year",
  "renewal_date": "policy or contract end date DD/MM/YYYY",
  "due_date": "next payment due date DD/MM/YYYY",
  "financial_year": "billing period or financial year e.g. 2025/26",
  "band": "council tax band (A–H) if shown",
  "label": "short label e.g. Council Tax, Energy, Broadband, Car Insurance, TV Licence"
}

Type rules:
- energy = gas or electricity supply
- broadband = home internet / fibre
- mobile = mobile phone contract or SIM
- car_ins = car or motor insurance
- home_ins = home / buildings / contents insurance
- life_ins = life insurance
- council_tax = council tax bill
- other = water bill, TV Licence, health, dental, or anything else

IMPORTANT: Extract every field you can find. For council tax specifically, always try to extract: band, annual amount, monthly instalment (price), financial year, and account reference."""


def _ma_gmail_extract_email(access_token: str, msg_id: str, confirmed_provider: str | None = None) -> dict | None:
    """Fetch one Gmail message and extract account details via Groq."""
    try:
        r = requests.get(
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}",
            params={"format": "full"},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        msg = r.json()
        # Extract plain text body
        body = ""
        payload = msg.get("payload", {})

        import base64, re as _re

        def _decode_part(part_data):
            if not part_data:
                return ""
            try:
                return base64.urlsafe_b64decode(part_data + "==").decode("utf-8", errors="ignore")
            except Exception:
                return ""

        def _strip_html(html):
            text = _re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=_re.DOTALL|_re.IGNORECASE)
            text = _re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=_re.DOTALL|_re.IGNORECASE)
            text = _re.sub(r"<[^>]+>", " ", text)
            text = _re.sub(r"[ \t]+", " ", text)
            text = _re.sub(r"\n{3,}", "\n\n", text)
            return text.strip()

        html_body = ""

        def _extract_text(part):
            nonlocal body, html_body
            if body and len(body) > 2000:
                return
            mime = part.get("mimeType", "")
            data = part.get("body", {}).get("data", "")
            if mime == "text/plain" and data:
                body += _decode_part(data)[:2000]
            elif mime == "text/html" and data and not html_body:
                html_body = _decode_part(data)[:6000]
            for sub in part.get("parts", []):
                _extract_text(sub)

        _extract_text(payload)
        # Fall back to stripped HTML if no plain text
        if not body.strip() and html_body:
            body = _strip_html(html_body)[:2000]
        if not body.strip():
            return None

        # Subject line
        headers = {h["name"]: h["value"] for h in payload.get("headers", [])}
        subject = headers.get("Subject", "")
        sender  = headers.get("From", "")

        groq_key = os.environ.get("GROQ_API_KEY", "")
        if not groq_key:
            return None

        print(f"[ma gmail extract] subject={subject!r} from={sender!r} body_len={len(body)} hint={confirmed_provider!r}")
        hint_note = f"\n\nNOTE: The user has confirmed they have an account with '{confirmed_provider}'. If this email is from or about '{confirmed_provider}', extract it even if it looks like a welcome or policy document. Still skip purely promotional/marketing emails that don't relate to an existing account." if confirmed_provider else ""
        prompt = f"Subject: {subject}\nFrom: {sender}\n\n{body[:2000]}{hint_note}"
        r2 = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": _MA_EXTRACT_SYSTEM},
                    {"role": "user",   "content": prompt},
                ],
                "max_tokens": 300,
                "temperature": 0.0,
                "response_format": {"type": "json_object"},
            },
            timeout=20,
        )
        if r2.status_code != 200:
            print(f"[ma gmail extract] groq error {r2.status_code}: {r2.text[:200]}")
            return None
        extracted = r2.json()["choices"][0]["message"]["content"]
        import json as _json
        d = _json.loads(extracted)
        print(f"[ma gmail extract] result={d}")
        if d.get("skip"):
            return None
        return d
    except Exception as e:
        print(f"[ma gmail extract] {msg_id}: {e}")
        return None


def _ma_gmail_clean_account(value: str) -> str:
    """Return value if it looks like a real account number, else empty string."""
    if not value:
        return ""
    v = value.strip()
    # Reject anything that looks like a base64 hash/token (contains / or = with no spaces, long)
    if len(v) > 25 and not " " in v and ("/" in v or "=" in v or "-" in v and v.count("-") > 2):
        return ""
    # Reject very short pure-numeric strings (just a line/order number like "96")
    if v.isdigit() and len(v) <= 3:
        return ""
    # Reject obvious URL fragments
    if v.startswith("http") or v.startswith("//"):
        return ""
    return v


def _ma_gmail_sync_hints_from_details(did: str) -> list:
    """Pull provider names from ma_details and merge into ma_gmail_tokens.provider_hints.
    Returns the merged list. `did` is the device_id (or from_number) used for ma_details."""
    try:
        sb = lib._sb()
        # Fetch all saved accounts for this user
        rows = sb.table("ma_details").select("data").eq("device_id", did).execute().data or []
        saved_providers = []
        for r in rows:
            d = r.get("data") or {}
            name = (d.get("Provider") or d.get("Council") or "").strip()
            if name and len(name) > 1:
                saved_providers.append(name)

        if not saved_providers:
            return []

        # Find existing hints in ma_gmail_tokens (try device_id, fallback from_number)
        token_rows = sb.table("ma_gmail_tokens").select("device_id,provider_hints") \
            .eq("device_id", did).execute().data
        if not token_rows:
            # did might be a from_number — try that column
            token_rows = sb.table("ma_gmail_tokens").select("device_id,provider_hints") \
                .eq("from_number", did).execute().data
        if not token_rows:
            return saved_providers  # no token row yet — nothing to update

        token_device_id = token_rows[0]["device_id"]
        existing = token_rows[0].get("provider_hints") or []
        existing_lower = {h.lower() for h in existing}
        # Add any saved provider not already in hints (case-insensitive)
        merged = list(existing)
        for p in saved_providers:
            if p.lower() not in existing_lower:
                merged.append(p)
                existing_lower.add(p.lower())
        merged = merged[:30]  # cap at 30

        sb.table("ma_gmail_tokens").update({"provider_hints": merged}) \
            .eq("device_id", token_device_id).execute()
        print(f"[ma gmail hints] synced {len(saved_providers)} saved providers → {len(merged)} total hints for device={token_device_id!r}")
        # Feed confirmed providers into community database
        for p in saved_providers:
            _ma_community_add_provider(p)
        return merged
    except Exception as e:
        print(f"[ma gmail hints] sync error: {e}")
        return []


def _ma_community_add_provider(provider_name: str) -> None:
    """Record a confirmed provider in the shared community table (increments count)."""
    name = provider_name.strip().lower()
    if not name or len(name) < 2:
        return
    try:
        sb = lib._sb()
        rows = sb.table("ma_provider_hints").select("id,count").eq("provider", name).execute().data
        if rows:
            sb.table("ma_provider_hints").update({"count": rows[0]["count"] + 1}) \
                .eq("id", rows[0]["id"]).execute()
        else:
            sb.table("ma_provider_hints").insert({"provider": name, "count": 1}).execute()
    except Exception as e:
        print(f"[ma community] add_provider error: {e}")


def _ma_community_top_providers(limit: int = 60) -> list:
    """Return the top provider names from the community table, ordered by usage count."""
    try:
        rows = lib._sb().table("ma_provider_hints").select("provider") \
            .order("count", desc=True).limit(limit).execute().data or []
        return [r["provider"] for r in rows if r.get("provider")]
    except Exception as e:
        print(f"[ma community] top_providers error: {e}")
        return []


def _hint_to_query(provider: str) -> str:
    """Build a Gmail search query for a user-supplied provider name hint."""
    import re as _re
    safe = _re.sub(r'["\\\n\r]', '', provider.strip())[:60]
    # Broad: any email mentioning the provider — Groq decides relevance.
    # We avoid restricting to invoice/bill keywords because many provider emails
    # use different wording (e.g. "Your policy document", "Monthly summary").
    return f'"{safe}" newer_than:5y'


def _ma_gmail_scan_bg(device_id: str, access_token: str, refresh_token: str, from_number: str = "", provider_hints: list | None = None):
    """Background: scan Gmail for household accounts, extract, store as pending records."""
    token_row = {"device_id": device_id, "access_token": access_token, "refresh_token": refresh_token}
    at = _ma_gmail_get_token(token_row)
    if not at:
        # Token refresh failed fatally — auth_error already set in DB by _ma_gmail_get_token
        print(f"[ma gmail scan] aborting — no valid token for device={device_id!r}")
        return
    seen_providers = set()   # deduplicate by provider name (case-insensitive)
    seen_accounts  = set()   # secondary dedup by (provider, account_no)
    pending = []

    # Each entry: (confirmed_provider_name, query, fallback_type)
    # confirmed_provider: non-None for user hints — passed to Groq as a hint
    # fallback_type: used when Groq doesn't identify a type from the email
    # Personal hints (from user's saved accounts) + community top providers
    personal = list(provider_hints or [])
    community = _ma_community_top_providers(60)
    personal_lower = {h.lower() for h in personal}
    # Add community providers not already in personal hints and not in std domain list
    _std_domains = " ".join(q for _, q in _MA_GMAIL_QUERIES)
    for cp in community:
        if cp.lower() not in personal_lower and cp.lower() not in _std_domains.lower():
            personal.append(cp)
            personal_lower.add(cp.lower())
    hint_queries = [(h, _hint_to_query(h), "other") for h in personal]
    std_queries  = [(None, q, dt) for dt, q in _MA_GMAIL_QUERIES]
    all_queries  = std_queries + hint_queries

    for confirmed_provider, query, fallback_type in all_queries:
        is_hint = confirmed_provider is not None
        try:
            r = requests.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                params={"q": query, "maxResults": 25 if is_hint else 20},
                headers={"Authorization": f"Bearer {at}"},
                timeout=10,
            )
            if r.status_code != 200:
                print(f"[ma gmail scan] query failed {r.status_code}: {query[:60]}")
                continue
            msgs = r.json().get("messages", [])
            print(f"[ma gmail scan] query={query[:70]!r} found={len(msgs)} msgs hint={confirmed_provider!r}")
            hint_found = False
            for msg in msgs[:15 if is_hint else 10]:
                d = _ma_gmail_extract_email(at, msg["id"], confirmed_provider=confirmed_provider)
                if not d:
                    continue
                provider = (d.get("provider") or confirmed_provider or "").strip()
                account_no = _ma_gmail_clean_account(d.get("account_no", ""))
                if not provider:
                    continue
                provider_key = provider.lower()
                # Skip if we already have a record for this provider+account combo
                pair_key = (provider_key, account_no.lower())
                if pair_key in seen_accounts:
                    continue
                seen_accounts.add(pair_key)
                # Also skip if same provider with no account_no (avoids duplicate blank entries)
                if not account_no and provider_key in seen_providers:
                    continue
                seen_providers.add(provider_key)
                # Build ma_details-compatible record
                rec_type = d.get("type") or fallback_type
                rec_type_map = {
                    "energy": "energy", "broadband": "broadband", "mobile": "mobile",
                    "car_ins": "car_ins", "home_ins": "home_ins",
                    "life_ins": "life_ins", "council_tax": "council_tax", "water": "other", "other": "other",
                }
                rec_type = rec_type_map.get(rec_type, "other")
                data = {}
                field = "Council" if rec_type == "council_tax" else "Provider"
                data[field] = provider
                if account_no:
                    acc_field = "Account No" if rec_type not in ("car_ins","home_ins","life_ins") else "Policy No"
                    data[acc_field] = account_no
                if d.get("phone"):
                    data["Phone"] = d["phone"]
                if d.get("renewal_date"):
                    data["Renewal Date"] = d["renewal_date"]
                if d.get("price"):
                    data["Price"] = d["price"]
                from search import _MA_DETAIL_TYPES_MAP
                label = d.get("label") or _MA_DETAIL_TYPES_MAP.get(rec_type, rec_type)
                pending.append({"type": rec_type, "label": label, "data": data})
                _ma_community_add_provider(provider)  # grow community database
                if is_hint:
                    hint_found = True
            # If user confirmed this provider but extraction found nothing, add minimal placeholder
            if is_hint and not hint_found and confirmed_provider.lower() not in seen_providers:
                from search import _MA_DETAIL_TYPES_MAP
                pending.append({
                    "type": "other",
                    "label": _MA_DETAIL_TYPES_MAP.get("other", "Other"),
                    "data": {"Provider": confirmed_provider},
                })
                seen_providers.add(confirmed_provider.lower())
                print(f"[ma gmail scan] hint '{confirmed_provider}' — no extraction match, added minimal placeholder")
        except Exception as e:
            print(f"[ma gmail scan] query='{query[:60]}' fallback_type='{fallback_type}': {e}")

    # Get Gmail address
    gmail_email = ""
    try:
        r = requests.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/profile",
            headers={"Authorization": f"Bearer {at}"},
            timeout=8,
        )
        if r.status_code == 200:
            gmail_email = r.json().get("emailAddress", "")
    except Exception:
        pass

    from datetime import datetime as _dt
    # Always mark scan as done — use ISO timestamp (not "now()" which PostgREST won't evaluate)
    for _attempt in range(3):
        try:
            lib._sb().table("ma_gmail_tokens").update({
                "scan_status": "done",
                "pending":     pending,
                "email":       gmail_email,
                "last_scan":   _dt.utcnow().isoformat() + "Z",
            }).eq("device_id", device_id).execute()
            print(f"[ma gmail scan] completed: {len(pending)} records, email={gmail_email!r}")
            break
        except Exception as e:
            print(f"[ma gmail scan] db update attempt {_attempt+1} error: {e}")
            import time as _t; _t.sleep(1)

    # WhatsApp notification when scan finds something
    if pending:
        wa_to = _ma_gmail_wa_number(from_number or device_id)
        if wa_to:
            lines = []
            for r in pending[:6]:
                p = r.get("data", {}).get("Provider") or r.get("data", {}).get("Council") or ""
                lines.append(f"• {r['label']}" + (f" — {p}" if p else ""))
            msg = (
                f"📧 Found {len(pending)} account{'s' if len(pending) != 1 else ''} in your Gmail:\n\n"
                + "\n".join(lines)
                + "\n\nOpen Miru → My Area → My Details to review and import them."
            )
            _wa_send_proactive(wa_to, msg)


@app.route("/api/myarea/gmail/status")
def ma_gmail_status():
    device_id   = request.args.get("device_id", "").strip()
    from_number = request.args.get("from_number", "").strip()
    if not device_id and not from_number:
        return jsonify({"connected": False})
    try:
        sb  = lib._sb()
        sel = "device_id,email,scan_status,pending,last_scan,access_token,refresh_token"
        rows = sb.table("ma_gmail_tokens").select(sel).eq("device_id", device_id).execute().data if device_id else []
        if not rows and from_number:
            rows = sb.table("ma_gmail_tokens").select(sel).eq("from_number", from_number).execute().data
        if rows:
            row = rows[0]
            # A record exists but may have been disconnected (tokens cleared)
            has_tokens = bool(row.get("access_token") or row.get("refresh_token"))
            if not has_tokens:
                # Still surface auth_error status so UI can show reconnect warning
                if row.get("scan_status") == "auth_error":
                    return jsonify({"connected": False, "scan_status": "auth_error"})
                return jsonify({"connected": False})
            return jsonify({
                "connected":   True,
                "email":       row.get("email", ""),
                "scan_status": row.get("scan_status", ""),
                "pending":     row.get("pending", []),
                "last_scan":   row.get("last_scan", ""),
            })
    except Exception as e:
        print(f"[ma gmail status] {e}")
    return jsonify({"connected": False})


@app.route("/api/myarea/gmail/hints", methods=["GET","POST"])
def ma_gmail_hints():
    """GET: return provider hints. POST: save hints list."""
    device_id   = request.args.get("device_id", "").strip()
    from_number = request.args.get("from_number", "").strip()
    if not device_id and not from_number:
        return jsonify({"error": "device_id required"}), 400
    try:
        sb = lib._sb()
        # Find the token row by device_id first, then from_number fallback
        rows = sb.table("ma_gmail_tokens").select("device_id,provider_hints").eq("device_id", device_id).execute().data if device_id else []
        if not rows and from_number:
            rows = sb.table("ma_gmail_tokens").select("device_id,provider_hints").eq("from_number", from_number).execute().data
        row_device_id = rows[0]["device_id"] if rows else (device_id or from_number)
        q = sb.table("ma_gmail_tokens").eq("device_id", row_device_id)
        if request.method == "GET":
            hints = (rows[0].get("provider_hints") or []) if rows else []
            return jsonify({"hints": hints})
        else:
            hints = (request.json or {}).get("hints", [])
            hints = [h.strip() for h in hints if isinstance(h, str) and h.strip()][:20]
            q.update({"provider_hints": hints}).execute()
            # Log for community aggregation
            try:
                for h in hints:
                    lib._sb().table("ma_provider_hints").upsert(
                        {"provider": h.lower(), "count": 1}, on_conflict="provider"
                    ).execute()
            except Exception:
                pass
            return jsonify({"ok": True, "hints": hints})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/gmail/clear-pending", methods=["POST"])
def ma_gmail_clear_pending():
    device_id = request.args.get("device_id", "").strip()
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    try:
        lib._sb().table("ma_gmail_tokens").update({"pending": []}) \
            .eq("device_id", device_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/gmail/disconnect", methods=["POST"])
def ma_gmail_disconnect():
    """Clear auth tokens but keep provider_hints so they survive reconnect."""
    device_id   = request.args.get("device_id", "").strip()
    from_number = request.args.get("from_number", "").strip()
    if not device_id and not from_number:
        return jsonify({"error": "device_id or from_number required"}), 400
    try:
        sb = lib._sb()
        clear = {"access_token": None, "refresh_token": None,
                 "scan_status": "disconnected", "pending": [], "email": None}
        if device_id:
            sb.table("ma_gmail_tokens").update(clear).eq("device_id", device_id).execute()
        if from_number:
            sb.table("ma_gmail_tokens").update(clear).eq("from_number", from_number).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/gmail/rescan", methods=["POST"])
def ma_gmail_rescan():
    device_id   = request.args.get("device_id", "").strip()
    from_number = request.args.get("from_number", "").strip()
    if not device_id and not from_number:
        return jsonify({"error": "device_id required"}), 400
    try:
        rows = lib._sb().table("ma_gmail_tokens").select("*").eq("device_id", device_id).execute().data if device_id else []
        if not rows and from_number:
            rows = lib._sb().table("ma_gmail_tokens").select("*").eq("from_number", from_number).execute().data
        if not rows:
            return jsonify({"error": "not_connected"}), 401
        row = rows[0]
        row_device_id = row["device_id"]
        lib._sb().table("ma_gmail_tokens").update({"scan_status": "scanning"}) \
            .eq("device_id", row_device_id).execute()
        at = _ma_gmail_get_token(row)
        phone = from_number or row.get("from_number", "")
        # Merge saved providers from ma_details into hints so all known accounts are re-searched
        did_for_details = phone or row_device_id
        hints = _ma_gmail_sync_hints_from_details(did_for_details) or (row.get("provider_hints") or [])
        _did = row_device_id
        def _scan_safe():
            try:
                _ma_gmail_scan_bg(_did, at, row.get("refresh_token", ""), phone, provider_hints=hints)
            except Exception as _se:
                print(f"[ma gmail scan] thread crashed: {_se}")
                try:
                    lib._sb().table("ma_gmail_tokens").update({
                        "scan_status": "done", "pending": []
                    }).eq("device_id", _did).execute()
                except Exception:
                    pass
        import threading
        threading.Thread(target=_scan_safe, daemon=True).start()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/myarea/gmail/debug-queries")
def ma_gmail_debug_queries():
    """Dev tool: run each query and show message counts + subjects (no extraction)."""
    device_id = request.args.get("device_id", "").strip()
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    try:
        rows = lib._sb().table("ma_gmail_tokens").select("*").eq("device_id", device_id).execute().data
        if not rows:
            return jsonify({"error": "not_connected"}), 401
        row = rows[0]
        at = _ma_gmail_get_token(row)
        results = []
        for det_type, query in _MA_GMAIL_QUERIES:
            r = requests.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                params={"q": query, "maxResults": 5},
                headers={"Authorization": f"Bearer {at}"},
                timeout=10,
            )
            msgs = r.json().get("messages", []) if r.status_code == 200 else []
            subjects = []
            for m in msgs[:3]:
                mr = requests.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{m['id']}",
                    params={"format": "metadata", "metadataHeaders": ["Subject", "From"]},
                    headers={"Authorization": f"Bearer {at}"},
                    timeout=8,
                )
                if mr.status_code == 200:
                    hdrs = {h["name"]: h["value"] for h in mr.json().get("payload", {}).get("headers", [])}
                    subjects.append({"subject": hdrs.get("Subject",""), "from": hdrs.get("From","")})
            results.append({"type": det_type, "query": query[:80], "count": len(msgs), "samples": subjects})
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/myarea/pdf-extract", methods=["POST"])
def ma_pdf_extract():
    """Upload a bill PDF, extract text, return structured account details via Groq."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["file"]
    if not f.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDF files are supported"}), 400
    try:
        import fitz
        raw = f.read()
        pdf = fitz.open(stream=raw, filetype="pdf")
        text = "\n".join(page.get_text() for page in pdf).strip()
    except Exception as e:
        return jsonify({"error": f"Could not read PDF: {e}"}), 400
    if not text:
        return jsonify({"error": "PDF appears to be scanned/image-only — no text could be extracted"}), 400

    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return jsonify({"error": "Extraction service not configured"}), 503

    prompt = f"Filename: {f.filename}\n\n{text[:4000]}"
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": _MA_PDF_EXTRACT_SYSTEM},
                    {"role": "user",   "content": prompt},
                ],
                "max_tokens": 400,
                "temperature": 0.0,
                "response_format": {"type": "json_object"},
            },
            timeout=20,
        )
        if r.status_code != 200:
            return jsonify({"error": "Extraction failed"}), 502
        import json as _json
        d = _json.loads(r.json()["choices"][0]["message"]["content"])
        if d.get("skip"):
            return jsonify({"error": "No account details found in this PDF"}), 422
        # Clean account_no same as Gmail scan
        if d.get("account_no"):
            d["account_no"] = _ma_gmail_clean_account(d["account_no"]) or ""
        return jsonify(d)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _web_client_id():
    return os.environ.get("GMAIL_WEB_CLIENT_ID") or os.environ.get("GMAIL_CLIENT_ID", "")

def _web_client_secret():
    return os.environ.get("GMAIL_WEB_CLIENT_SECRET") or os.environ.get("GMAIL_CLIENT_SECRET", "")

def _school_oauth_url(profile_id: str) -> str:
    import urllib.parse
    params = {
        "client_id":     _web_client_id(),
        "redirect_uri":  _SCHOOL_OAUTH_REDIRECT,
        "response_type": "code",
        "scope":         _SCHOOL_OAUTH_SCOPES,
        "access_type":   "offline",
        "prompt":        "consent",
        "state":         profile_id,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)


@app.route("/school/oauth/callback")
def school_oauth_callback():
    code       = request.args.get("code", "")
    profile_id = request.args.get("state", "")
    error      = request.args.get("error", "")

    if error or not code or not profile_id:
        return redirect("/?screen=school&oauth_error=1")

    try:
        r = requests.post("https://oauth2.googleapis.com/token", data={
            "code":          code,
            "client_id":     _web_client_id(),
            "client_secret": _web_client_secret(),
            "redirect_uri":  _SCHOOL_OAUTH_REDIRECT,
            "grant_type":    "authorization_code",
        }, timeout=15)
        tokens = r.json()
    except Exception as e:
        print(f"[school oauth] token exchange error: {e}")
        return redirect("/?screen=school&oauth_error=2")

    refresh_token = tokens.get("refresh_token", "")
    if not refresh_token:
        print(f"[school oauth] no refresh_token in response: {tokens}")
        return redirect("/?screen=school&oauth_error=2")

    try:
        lib._sb().table("school_profiles").update(
            {"gmail_refresh_token": refresh_token, "gmail_token_error": False}
        ).eq("id", profile_id).execute()
    except Exception as e:
        print(f"[school oauth] db error: {e}")
        return redirect("/?screen=school&oauth_error=3")

    import threading
    threading.Thread(
        target=school_service.poll_all_profiles,
        kwargs={"days_back": 30, "force": False, "profile_ids": [profile_id], "skip_error_flag": True},
        daemon=True,
    ).start()

    # Redirect back to settings page with the WA number pre-filled so the user sees their events
    try:
        import urllib.parse as _up
        prof_row = lib._sb().table("school_profiles") \
            .select("from_number").eq("id", profile_id).execute().data
        wa_raw = (prof_row[0]["from_number"] if prof_row else "").replace("whatsapp:", "")
        if wa_raw:
            return redirect(f"/school/settings?wa={_up.quote(wa_raw)}&oauth=success")
    except Exception:
        pass
    return redirect("/?screen=school&oauth=success")


@app.route("/api/school/lookup")
def school_lookup():
    name = request.args.get("name", "").strip()
    if not name:
        return jsonify({})
    info = school_service._lookup_school(name)
    return jsonify(info)


@app.route("/api/school/signup", methods=["POST"])
def school_signup_api():
    import re as _re2
    try:
        data = request.get_json(force=True, silent=True) or {}

        wa_raw = data.get("wa_number", "").strip()
        child  = data.get("child_name", "").strip()
        school = data.get("school_name", "").strip()
        emails = data.get("sender_emails", [])

        if not wa_raw:
            return jsonify({"error": "WhatsApp number is required"}), 400
        if not child:
            return jsonify({"error": "Child name is required"}), 400
        if not school:
            return jsonify({"error": "School name is required"}), 400
        if not emails:
            return jsonify({"error": "At least one school email is required"}), 400

        digits = _re2.sub(r"[^\d+]", "", wa_raw)
        if not digits.startswith("+"):
            if digits.startswith("07"):
                digits = "+44" + digits[1:]
            elif not digits.startswith("44"):
                digits = "+" + digits
            else:
                digits = "+" + digits
        from_number = "whatsapp:" + digits

        result = lib._sb().table("school_profiles").insert({
            "from_number":    from_number,
            "child_name":     child,
            "school_name":    school,
            "class_name":     data.get("class_name", ""),
            "teacher_name":   data.get("teacher_name", ""),
            "year_group":     data.get("year_group", ""),
            "address":        data.get("address", ""),
            "phone":          data.get("phone", ""),
            "class_wa_group": data.get("class_wa_group", ""),
            "sender_emails":  emails,
        }).execute()
        profile_id = (result.data or [{}])[0].get("id", "")
        print(f"[school signup] created profile {profile_id} for {from_number}")

        oauth_url = _school_oauth_url(profile_id)
        return jsonify({"ok": True, "profile_id": profile_id, "oauth_url": oauth_url})

    except Exception as e:
        print(f"[school signup] unhandled error: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Signup failed: {e}"}), 500


@app.route("/api/school/diag")
def school_diag():
    """Admin diagnostic: show profile/token/event state without sending anything."""
    token = request.args.get("token", "")
    if token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "Unauthorized"}), 401
    try:
        profiles = lib._sb().table("school_profiles") \
            .select("id,from_number,child_name,school_name,sender_emails,gmail_refresh_token,active") \
            .execute().data or []
        # Check token validity without doing a full poll
        out = []
        for p in profiles:
            rtok = p.pop("gmail_refresh_token", None)
            token_status = "none"
            if rtok:
                try:
                    import requests as _rq
                    tr = _rq.post("https://oauth2.googleapis.com/token", data={
                        "client_id":     os.environ.get("GMAIL_WEB_CLIENT_ID") or os.environ.get("GMAIL_CLIENT_ID",""),
                        "client_secret": os.environ.get("GMAIL_WEB_CLIENT_SECRET") or os.environ.get("GMAIL_CLIENT_SECRET",""),
                        "refresh_token": rtok,
                        "grant_type":    "refresh_token",
                    }, timeout=8)
                    token_status = "valid" if "access_token" in tr.json() else f"error:{tr.json().get('error','?')}"
                except Exception as _te:
                    token_status = f"exception:{_te}"
            elif os.environ.get("GMAIL_REFRESH_TOKEN"):
                token_status = "env_var_fallback"
            recent = lib._sb().table("school_events").select("id,created_at,event_title") \
                .eq("profile_id", p["id"]).order("created_at", desc=True).limit(3).execute().data or []
            out.append({**p, "token_status": token_status,
                        "recent_events": [{"title": e["event_title"], "at": e["created_at"]} for e in recent]})
        env_token = bool(os.environ.get("GMAIL_REFRESH_TOKEN"))
        return jsonify({"profiles": out, "env_gmail_token": env_token,
                        "web_client_id": bool(os.environ.get("GMAIL_WEB_CLIENT_ID"))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/school/poll")
def school_poll():
    """Poll Gmail for new school emails and store events.
    Call every 6h via cron-job.org: GET /api/school/poll?token=YOUR_DIGEST_TOKEN
    """
    token = request.args.get("token", "")
    if not token or token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "Unauthorized"}), 401
    days_back = int(request.args.get("days_back", 14))
    force     = request.args.get("force", "").lower() in ("1", "true", "yes")
    import threading
    threading.Thread(
        target=school_service.poll_all_profiles,
        kwargs={"days_back": days_back, "force": force, "on_error": _school_gmail_token_alert},
        daemon=True,
    ).start()
    return jsonify({"status": "started", "days_back": days_back, "force": force})


_school_token_alert_sent: dict = {}  # from_number → last alert timestamp

def _school_gmail_token_alert(from_number: str, profiles: list):
    """Send a WhatsApp alert when Gmail token is revoked. Rate-limited to once per 24h."""
    import time
    now = time.time()
    last = _school_token_alert_sent.get(from_number, 0)
    if now - last < 86400:
        return
    _school_token_alert_sent[from_number] = now

    child_names = ", ".join(p["child_name"] for p in profiles if p.get("child_name")) or "your child"
    wa_number = from_number.replace("whatsapp:", "")
    msg = (
        f"⚠️ *Miru school comms paused*\n\n"
        f"Miru lost access to your Gmail and can no longer fetch school emails for {child_names}.\n\n"
        f"Tap below to reconnect (takes 30 seconds):\n"
        f"https://miru.humanagency.co/school/settings?wa={wa_number}"
    )
    try:
        twilio_client.messages.create(
            body=msg,
            from_=os.environ.get("TWILIO_WHATSAPP_NUMBER", ""),
            to=from_number,
        )
        print(f"[school] token alert sent to {from_number}")
    except Exception as e:
        print(f"[school] token alert send failed for {from_number}: {e}")


def _normalise_from_number(raw: str) -> str:
    """Convert user-entered number (+447...) to whatsapp:+447... stored format."""
    import re as _re
    digits = _re.sub(r"[^\d+]", "", raw.strip())
    if not digits.startswith("+"):
        if digits.startswith("07"):
            digits = "+44" + digits[1:]
        elif digits.startswith("44"):
            digits = "+" + digits
        else:
            digits = "+" + digits
    if digits.startswith("whatsapp:"):
        return digits
    return "whatsapp:" + digits


def _get_school_wa():
    """Return (from_number, err_response). Reads X-School-WA header, normalises
    to whatsapp:+... format, validates it exists in school_profiles."""
    raw = (request.headers.get("X-School-WA") or "").strip()
    if not raw:
        return None, (jsonify({"error": "WA number required", "auth": True}), 401)
    wa = _normalise_from_number(raw)
    try:
        rows = (lib._sb().table("school_profiles")
                .select("id").eq("from_number", wa).eq("active", True)
                .limit(1).execute().data or [])
    except Exception:
        rows = []
    if not rows:
        return None, (jsonify({"error": "No account found for this number", "auth": True}), 401)
    return wa, None


@app.route("/api/school/events")
def api_school_events():
    """Return school events for the requesting parent (scoped by WA number)."""
    wa, err = _get_school_wa()
    if err:
        return err
    days_ahead = int(request.args.get("days", 30))
    days_back  = int(request.args.get("back", 30))
    profile_id = request.args.get("profile_id", "")
    from datetime import date, timedelta
    past    = (date.today() - timedelta(days=days_back)).isoformat()
    horizon = (date.today() + timedelta(days=days_ahead)).isoformat()
    try:
        profiles_raw = (lib._sb().table("school_profiles")
                    .select("id,school_name,child_name,class_name,teacher_name,year_group,address,phone,class_wa_group,gmail_refresh_token,sender_emails")
                    .eq("from_number", wa).eq("active", True).execute().data or [])
        profiles = []
        for p in profiles_raw:
            gmail_connected = bool(p.pop("gmail_refresh_token", None))
            oauth_url = _school_oauth_url(p["id"]) if not gmail_connected else None
            p["gmail_connected"] = gmail_connected
            p["oauth_url"] = oauth_url
            profiles.append(p)
        allowed_ids = {p["id"] for p in profiles}
        if not allowed_ids:
            return jsonify({"events": [], "profiles": []})

        def _add_profile_filter(q):
            if profile_id and profile_id in allowed_ids:
                return q.eq("profile_id", profile_id)
            # Filter to only this parent's profiles
            return q.in_("profile_id", list(allowed_ids))

        dated = (_add_profile_filter(
                    lib._sb().table("school_events").select("*")
                    .gte("event_date", past).lte("event_date", horizon))
                 .order("event_date").execute().data or [])
        undated = (_add_profile_filter(
                    lib._sb().table("school_events").select("*")
                    .is_("event_date", "null").gte("created_at", past))
                   .order("created_at", desc=True).execute().data or [])
        all_events = dated + undated
        last_synced = None
        if all_events:
            last_synced = max((e.get("created_at") or "") for e in all_events) or None
        return jsonify({"events": all_events, "profiles": profiles, "last_synced": last_synced})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/school/dedup", methods=["POST"])
def api_school_dedup():
    """Remove fuzzy duplicate school events scoped to requesting parent's WA number."""
    wa, err = _get_school_wa()
    if err:
        return err
    import re as _re

    _stops = {"a","an","the","and","or","for","to","of","in","on","at","is","with",
              "about","from","your","session","drop","first","parent","support"}

    def _words(t):
        return [w for w in _re.sub(r"[^a-z0-9]", " ", (t or "").lower()).split()
                if len(w) > 2 and w not in _stops]

    def _overlap(a, b):
        wa2, wbArr = set(_words(a)), _words(b)
        if not wa2 and not wbArr:
            return 0
        common = sum(1 for w in wbArr if w in wa2)
        return common / max(len(wa2), len(wbArr), 1)

    def _richness(e):
        return (200 if e.get("action_needed") else 0) + len(e.get("description") or "") + len(e.get("action_needed") or "")

    try:
        profiles = (lib._sb().table("school_profiles")
                    .select("id").eq("from_number", wa).eq("active", True).execute().data or [])
        allowed_ids = [p["id"] for p in profiles]
        if not allowed_ids:
            return jsonify({"deleted": 0, "ids": []})
        all_events = (lib._sb().table("school_events").select("*")
                      .in_("profile_id", allowed_ids).execute().data or [])
        to_delete = []

        # Group by profile_id + event_date, then fuzzy-dedup within each group
        from collections import defaultdict
        groups = defaultdict(list)
        for e in all_events:
            key = (e.get("profile_id", ""), e.get("event_date") or "__undated__")
            groups[key].append(e)

        for (pid, dt), evts in groups.items():
            if dt == "__undated__" or len(evts) < 2:
                continue
            used = set()
            for i in range(len(evts)):
                if i in used:
                    continue
                cluster = [evts[i]]
                for j in range(i + 1, len(evts)):
                    if j in used:
                        continue
                    if _overlap(evts[i]["event_title"], evts[j]["event_title"]) >= 0.35:
                        cluster.append(evts[j])
                        used.add(j)
                if len(cluster) > 1:
                    cluster.sort(key=_richness, reverse=True)
                    for dup in cluster[1:]:
                        to_delete.append(dup["id"])
                used.add(i)

        if to_delete:
            for _id in to_delete:
                lib._sb().table("school_events").delete().eq("id", _id).execute()

        return jsonify({"deleted": len(to_delete), "ids": to_delete})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/api/school/digest")
def school_digest():
    """Send weekly school digest to all parents.
    Call Sunday ~18:00 via cron-job.org: GET /api/school/digest?token=YOUR_DIGEST_TOKEN
    """
    token = request.args.get("token", "")
    if not token or token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "Unauthorized"}), 401
    result = school_service.send_weekly_digest_all()
    return jsonify(result)


@app.route("/api/user-token")
def api_user_token():
    """Exchange a phone number for a stable per-user HMAC token.
    The token is used as X-Library-PIN for My Library and My Saves."""
    phone = request.args.get("phone", "").strip()
    if not phone:
        return jsonify({"error": "phone required"}), 400
    # Normalise: 07xxx → +447xxx
    phone = re.sub(r'\s+', '', phone)
    if re.match(r'^0\d{10}$', phone):
        phone = '+44' + phone[1:]
    token = _wa_user_token(phone)
    # Persist token→phone so _resolve_user_token survives Railway restarts
    # and works for users who have zero wa_saves rows
    try:
        lib._sb().table("ai_cache").upsert({
            "key": f"user_token:{token}",
            "data": {"phone": phone},
            "cached_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as _pe:
        print(f"[user-token] persist error: {_pe}")
    return jsonify({"token": token, "phone": phone})


@app.route("/api/my-fuel")
def api_my_fuel():
    """Return live prices for up to 3 saved stations identified by (lat, lon, brand)."""
    import json as _json
    try:
        saved = _json.loads(request.args.get("stations", "[]"))
    except Exception:
        return jsonify({"error": "invalid stations"}), 400

    all_st = get_stations()
    results = []
    for s in saved[:3]:
        slat = s.get("lat")
        slon = s.get("lon")
        sbrand = (s.get("brand") or "").lower()
        if slat is None or slon is None:
            results.append({**s, "found": False})
            continue
        best, best_dist = None, 0.3  # 300m match radius
        for st in all_st:
            d = haversine_km(slat, slon, st["lat"], st["lon"])
            if d < best_dist:
                if not sbrand or sbrand in st.get("brand", "").lower():
                    best_dist = d
                    best = st
        if best:
            results.append({
                "brand":   best.get("brand", s.get("brand")),
                "address": best.get("address", s.get("address", "")),
                "postcode":best.get("postcode", s.get("postcode", "")),
                "lat": slat, "lon": slon,
                "petrol":  best.get("petrol"),
                "diesel":  best.get("diesel"),
                "found":   True,
            })
        else:
            results.append({**s, "found": False})

    loaded_at = _station_cache.get("loaded_at", 0)
    updated = datetime.fromtimestamp(loaded_at).strftime("%H:%M") if loaded_at else "–"
    return jsonify({"stations": results, "updated": updated})


@app.route("/api/whatsapp-number")
def api_whatsapp_number():
    """Return the WhatsApp contact number for the bot (safe to expose publicly)."""
    raw = os.environ.get("TWILIO_WHATSAPP_FROM", "")
    # strip whatsapp: prefix if present; keep only digits and +
    number = re.sub(r"[^+\d]", "", raw.replace("whatsapp:", ""))
    return jsonify({"number": number})


@app.route("/api/wa-saves")
def api_wa_saves():
    """List saves. Admin PIN → all saves. User token → only their saves."""
    pin = (request.headers.get("X-Library-PIN") or
           request.headers.get("X-Admin-Password") or
           request.args.get("pin", ""))
    admin_pw = os.environ.get("ADMIN_PASSWORD", "")

    filter_number = None
    if admin_pw and pin == admin_pw:
        filter_number = None  # admin sees all
    elif pin:
        filter_number = _resolve_saves_token(pin)
        if not filter_number:
            return jsonify({"error": "Password required", "auth": True}), 401
    elif admin_pw:
        return jsonify({"error": "Password required", "auth": True}), 401
    # no ADMIN_PASSWORD set and no pin → open (dev mode)

    try:
        q = lib._sb().table("wa_saves").select(
            "id,title,url,summary,status,remind_day,created_at,image_url"
        )
        if filter_number:
            # DB stores "whatsapp:+44..." but web login resolves to "+44..." — match both
            clean = filter_number.replace("whatsapp:", "")
            q = q.in_("from_number", [clean, "whatsapp:" + clean])
        rows = q.order("created_at", desc=True).execute().data
        return jsonify({"saves": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/update", methods=["POST"])
def api_wa_saves_update():
    """Update triage status of a saved URL."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data = request.json or {}
    save_id = data.get("id")
    status = data.get("status")
    if status == "archive":
        status = "read"  # "archive" is the new UI label for read
    if not save_id or status not in ("read", "skip", "remind", "pending"):
        return jsonify({"error": "Invalid id or status"}), 400
    try:
        q = lib._sb().table("wa_saves").update({"status": status}).eq("id", save_id)
        if from_number:
            q = q.in_("from_number", _wa_number_variants(from_number))  # users can only update their own
        q.execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/delete", methods=["POST"])
def api_wa_saves_delete():
    """Delete a save. Users can only delete their own saves."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data = request.json or {}
    save_id = data.get("id")
    if not save_id:
        return jsonify({"error": "id required"}), 400
    try:
        q = lib._sb().table("wa_saves").delete().eq("id", save_id)
        if from_number:
            q = q.in_("from_number", _wa_number_variants(from_number))  # users can only delete their own
        q.execute()
        lib.saves_unsync(save_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/bulk-delete", methods=["POST"])
def api_wa_saves_bulk_delete():
    """Delete multiple saves by ID in one request."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data = request.json or {}
    ids = data.get("ids")
    if not ids or not isinstance(ids, list):
        return jsonify({"error": "ids array required"}), 400
    deleted = []
    errors = []
    for save_id in ids:
        try:
            q = lib._sb().table("wa_saves").delete().eq("id", save_id)
            if from_number:
                q = q.in_("from_number", _wa_number_variants(from_number))
            q.execute()
            lib.saves_unsync(save_id)
            deleted.append(save_id)
        except Exception as e:
            errors.append({"id": save_id, "error": str(e)})
    return jsonify({"deleted": deleted, "errors": errors})


@app.route("/api/admin/retitle-saves", methods=["POST"])
def api_admin_retitle_saves():
    """One-shot: re-derive specific titles for saves that still have generic titles."""
    key = request.headers.get("X-Admin-Key", "") or request.args.get("key", "")
    if not key or key != os.environ.get("ADMIN_KEY", "miru-admin-2026"):
        return jsonify({"error": "unauthorized"}), 401

    GENERIC = {"📷 Photo", "🍽️ Menu", "📢 Billboard/Ad", "🏪 Place",
               "🍷 Wine", "🪧 Sign", "📄 Document", "🎫 Event/Ticket", "🏷️ Brand"}
    EMOJI = {
        "📢 Billboard/Ad": "📢", "🏪 Place": "🏪", "🍷 Wine": "🍷",
        "📷 Photo": "📷", "🪧 Sign": "🪧", "📄 Document": "📄",
        "🎫 Event/Ticket": "🎫", "🏷️ Brand": "🏷️", "🍽️ Menu": "🍽️",
    }

    rows = lib._sb().table("wa_saves").select("id,title,summary,url").execute().data or []
    to_fix = [r for r in rows if (r.get("title") or "").strip() in GENERIC and (r.get("summary") or "").strip()]

    updated, skipped, failed = [], [], []
    for row in to_fix:
        try:
            old_title = (row.get("title") or "").strip()
            summary   = (row.get("summary") or "")[:600]
            emoji     = EMOJI.get(old_title, "")

            # Menus: restaurant name often appears as "🍽️ Name" in the summary body
            if old_title == "🍽️ Menu":
                for ln in summary.split("\n"):
                    ln = ln.strip()
                    if ln.startswith("🍽️ ") and len(ln) > 5:
                        name = ln[2:].strip()
                        if name and name.lower() not in ("menu", "restaurant"):
                            new_title = f"🍽️ {name} Menu"
                            lib._sb().table("wa_saves").update({"title": new_title}).eq("id", row["id"]).execute()
                            updated.append({"id": row["id"], "old": old_title, "new": new_title})
                        break
                else:
                    skipped.append(row["id"])
                continue

            # All others: ask Groq to name the specific thing
            prompt = (
                f"A saved item has this description:\n---\n{summary}\n---\n"
                f"Give me a short specific title (2-5 words) naming the actual brand, place, wine, "
                f"event, or product visible. No generic words like 'advertisement' or 'photo'. "
                f"Output ONLY the title text, nothing else."
            )
            result = _groq_chat(
                "You extract concise, specific titles from image descriptions.",
                [{"role": "user", "content": prompt}],
                max_tokens=25,
            )
            if result:
                result = result.strip().strip('"\'').strip()
            if not result or len(result) > 80 or result.lower() in ("unknown", "n/a", ""):
                skipped.append(row["id"])
                continue
            new_title = f"{emoji} {result}" if emoji else result
            if new_title == old_title:
                skipped.append(row["id"])
                continue
            lib._sb().table("wa_saves").update({"title": new_title}).eq("id", row["id"]).execute()
            updated.append({"id": row["id"], "old": old_title, "new": new_title})
        except Exception as e:
            failed.append({"id": row["id"], "error": str(e)})

    return jsonify({
        "scanned": len(to_fix),
        "updated": len(updated),
        "skipped": len(skipped),
        "failed":  len(failed),
        "items":   updated,
    })


@app.route("/api/wa-saves/enrich", methods=["POST"])
def api_wa_saves_enrich():
    """Look up place details for a photo/place save and update its summary."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data = request.json or {}
    save_id    = data.get("id", "")
    venue_name = data.get("venue", "").strip()
    lat        = data.get("lat")
    lon        = data.get("lon")
    if not save_id or not venue_name:
        return jsonify({"error": "id and venue required"}), 400
    fn = from_number
    # Fall back to stored GPS if frontend didn't send coordinates
    if lat is None or lon is None:
        if not fn:
            pin = request.headers.get("X-Library-PIN", "")
            fn = _resolve_user_token(pin)
        loc = fn and _USER_LAST_LOCATION.get(fn)
        if loc and (time.time() - loc.get("ts", 0)) < 7200:
            lat, lon = loc["lat"], loc["lon"]
    try:
        info = _lookup_venue(venue_name, lat=lat, lon=lon)
        parts = []
        if info.get("open_now") is not None:
            status = "🟢 Open now" if info["open_now"] else "🔴 Closed now"
            parts.append(f"{status}{' · ' + info['hours_today'] if info.get('hours_today') else ''}")
        elif info.get("hours_today"):
            parts.append(f"🕐 Today: {info['hours_today']}")
        if info.get("address"):
            parts.append(f"📍 {info['address'].split(',')[0]}")
        if info.get("rating"):
            parts.append(f"{'⭐'*int(round(info['rating']))} {info['rating']} ({info['rating_count']:,} reviews)")
        if info.get("phone"):
            parts.append(f"📞 {info['phone']}")
        if info.get("website"):
            parts.append(f"🌐 {info['website']}")
        new_summary = "\n".join(parts)
        update = {"summary": new_summary}
        if info.get("maps_url"):
            update["url"] = info["maps_url"]
        if info.get("name") and info["name"] != venue_name:
            update["title"] = f"🏪 {info['name']}"
        lib._sb().table("wa_saves").update(update).eq("id", save_id).execute()
        rows = lib._sb().table("wa_saves").select("*").eq("id", save_id).execute().data
        if rows:
            lib.saves_sync(rows[0])

        # Notify on WhatsApp
        if fn:
            display_name = update.get("title") or f"🏪 {venue_name}"
            msg = f"✅ Updated: {display_name}\n"
            if new_summary:
                msg += "\n" + new_summary
            _wa_send_proactive(fn, msg)

        return jsonify({"ok": True, "summary": new_summary, "title": update.get("title", ""), "url": update.get("url", "")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/update-location", methods=["POST"])
def api_wa_saves_update_location():
    """Edit the location (📍) embedded in a save's summary."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data = request.json or {}
    save_id  = data.get("id")
    location = data.get("location", "").strip()
    if not save_id:
        return jsonify({"error": "id required"}), 400
    try:
        q = lib._sb().table("wa_saves").select("summary,from_number").eq("id", save_id)
        rows = q.execute().data
        if not rows:
            return jsonify({"error": "save not found"}), 404
        row = rows[0]
        if from_number and row.get("from_number") != from_number:
            return jsonify({"error": "not your save"}), 403
        summary = row.get("summary") or ""
        import re
        if location:
            # Replace existing 📍 … on the META line, or append if missing
            if re.search(r"📍[^\n]*", summary):
                summary = re.sub(r"📍[^\n]*", f"📍 {location}", summary, count=1)
            elif summary.startswith("META:"):
                first_newline = summary.find("\n")
                if first_newline == -1:
                    summary += f" · 📍 {location}"
                else:
                    summary = summary[:first_newline] + f" · 📍 {location}" + summary[first_newline:]
            else:
                summary = f"📍 {location}\n" + summary
        else:
            # Blank location = remove it
            summary = re.sub(r"\s*·?\s*📍[^\n]*", "", summary)
        lib._sb().table("wa_saves").update({"summary": summary}).eq("id", save_id).execute()
        lib.saves_sync({**row, "id": save_id, "summary": summary})
        return jsonify({"ok": True, "summary": summary})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/ad-intel", methods=["POST"])
def api_wa_saves_ad_intel():
    """AI analysis of an ad/photo save: extract company, location, type and build relevant search links."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data = request.json or {}
    save_id = data.get("id", "")
    if not save_id:
        return jsonify({"error": "id required"}), 400

    rows = lib._sb().table("wa_saves").select("id,title,summary,image_url,url,from_number").eq("id", save_id).execute().data
    if not rows:
        return jsonify({"error": "save not found"}), 404
    save = rows[0]
    if from_number and save.get("from_number") != from_number:
        return jsonify({"error": "not your save"}), 403

    title     = (save.get("title") or "").strip()
    summary   = (save.get("summary") or "").strip()
    image_url = (save.get("image_url") or "").strip()

    if not title and not summary and not image_url:
        return jsonify({"error": "no content to analyse"}), 400

    json_schema = (
        '{"company":"company/brand name exactly as written, or null","ad_type":"real_estate|wine|car|vehicle|product|job|other",'
        '"website":"website domain if inferable e.g. rightmove.co.uk, else null",'
        '"postcode":"UK postcode if visible else null","area":"area or town if no postcode",'
        '"address":"full street address if visible else null",'
        '"price":"price with £ symbol e.g. £2785pcm or £450000 or £18.99, else null","bedrooms":null,'
        '"property_type":"house/flat/studio/commercial/land or null",'
        '"wine_name":"name of the wine if visible else null",'
        '"vintage":"year of the wine if visible else null",'
        '"grape":"grape variety if visible else null",'
        '"region":"wine region e.g. Burgundy, Rioja, Napa if visible else null",'
        '"registration":"vehicle registration plate exactly as shown, else null",'
        '"notes":"one sentence: key facts, anything notable visible in the image"}'
    )

    import json as _json, re as _re
    try:
        if image_url:
            # Vision model — send image URL directly
            groq_key = os.environ.get("GROQ_API_KEY", "")
            vision_messages = [{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": image_url}},
                {"type": "text", "text": (
                    f"Extract structured info from this image. Additional context — title: {title or 'none'}, summary: {summary[:400] or 'none'}.\n"
                    f"Reply with JSON only:\n{json_schema}"
                )},
            ]}]
            vr = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                json={"model": "meta-llama/llama-4-scout-17b-16e-instruct", "messages": vision_messages, "max_tokens": 500},
                timeout=20,
            )
            vr.raise_for_status()
            raw = vr.json()["choices"][0]["message"]["content"].strip()
        else:
            prompt = (
                "Extract structured info from this saved ad or listing.\n"
                f"Title: {title}\nContent: {summary[:1200]}\n\nReply with JSON only:\n{json_schema}"
            )
            raw = _groq_chat("You are a data extraction assistant. Reply with JSON only.", [{"role": "user", "content": prompt}], max_tokens=500, json_mode=True)
        raw = _re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()
        result = _json.loads(raw)
    except Exception as e:
        return jsonify({"error": f"AI failed: {e}"}), 500

    ad_type   = result.get("ad_type", "other")
    company   = (result.get("company")   or "").strip()
    website   = (result.get("website")   or "").strip().lower().lstrip("https://").lstrip("http://").rstrip("/")
    postcode  = (result.get("postcode")  or "").strip().upper()
    area      = (result.get("area")      or "").strip()
    address   = (result.get("address")   or "").strip()
    notes     = (result.get("notes")     or "").strip()
    beds      = result.get("bedrooms")
    prop_t    = result.get("property_type") or ""
    wine_name = (result.get("wine_name") or "").strip()
    vintage   = (result.get("vintage")   or "").strip()
    grape     = (result.get("grape")     or "").strip()
    region    = (result.get("region")    or "").strip()

    # Fix currency: Groq sometimes returns ¢ instead of £
    raw_price = str(result.get("price") or "").strip()
    for bad_cur in ["¢", "$", "€"]:  # ¢, $, €
        raw_price = raw_price.replace(bad_cur, "£")  # £
    if raw_price and raw_price[0].isdigit():
        raw_price = "£" + raw_price
    price = raw_price or None

    def _q(s): return str(s).replace(" ", "+").replace('"', "%22")

    loc = postcode or area
    links = []

    if ad_type == "real_estate":
        if company:
            if website and "." in website:
                links.append({"label": f"{company} website", "url": f"https://{website}"})
            else:
                ws_q = _q(f"{company} {loc} lettings agent website")
                links.append({"label": f"Find {company} website", "url": f"https://www.google.com/search?q={ws_q}"})
        if loc:
            pc_slug = loc.replace(" ", "%20")
            links.append({"label": "Rightmove", "url": f"https://www.rightmove.co.uk/property-to-rent/search.html?searchLocation={pc_slug}&useLocationIdentifier=true"})
            links.append({"label": "Zoopla", "url": f"https://www.zoopla.co.uk/to-rent/property/{loc.lower().replace(' ', '-')}/"})

    elif ad_type == "wine":
        vivino_q = _q(f"{wine_name} {company}".strip())
        links.append({"label": "Search Vivino", "url": f"https://www.vivino.com/search/wines?q={vivino_q}"})
        if company:
            links.append({"label": f"Find {company}", "url": f"https://www.google.com/search?q={_q(company + ' winery wine')}"})

    elif ad_type == "car":
        links.append({"label": "AutoTrader", "url": f"https://www.autotrader.co.uk/car-search?search_term={_q(company or notes or title)}"})
    elif ad_type == "job":
        links.append({"label": "Indeed", "url": f"https://uk.indeed.com/jobs?q={_q(company or title)}"})
    else:
        if company:
            links.append({"label": f"Find {company}", "url": f"https://www.google.com/search?q={_q(company + ' ' + loc)}"})

    # Auto-save: update title, location in summary, and persist website to url field
    import json as _json, re as _re
    try:
        if ad_type == "wine":
            parts = []
            if wine_name: parts.append(f"🍷 {wine_name}")
            if company:   parts.append(company)
            if vintage:   parts.append(vintage)
            if grape:     parts.append(grape)
            if price:     parts.append(price)
            new_title = " · ".join(parts) if parts else (title or "")
        else:
            parts = []
            if company: parts.append(company)
            if beds:    parts.append(f"{beds} bed")
            if prop_t:  parts.append(prop_t)
            if postcode: parts.append(postcode)
            elif area:   parts.append(area)
            if price:    parts.append(price)
            new_title = " · ".join(str(p) for p in parts) if parts else (title or "")

        loc_str = postcode or area
        old_summary = save.get("summary") or ""
        if loc_str:
            if _re.search(r"📍[^\n]*", old_summary):
                new_summary = _re.sub(r"📍[^\n]*", f"📍 {loc_str}", old_summary, count=1)
            elif old_summary.startswith("META:"):
                nl = old_summary.find("\n")
                insert = f" · 📍 {loc_str}"
                new_summary = (old_summary[:nl] + insert + old_summary[nl:]) if nl != -1 else (old_summary + insert)
            else:
                new_summary = f"META: 📍 {loc_str}\n" + old_summary
        else:
            new_summary = old_summary

        # Embed compact AI result so card can be re-rendered on next visit
        compact = {k: v for k, v in {
            "ad_type": ad_type, "company": company, "website": website,
            "postcode": postcode, "area": area, "address": address,
            "price": price, "bedrooms": beds, "property_type": prop_t,
            "wine_name": wine_name, "vintage": vintage, "grape": grape,
            "region": region, "notes": notes, "links": links,
        }.items() if v}
        airesult_line = "AIRESULT:" + _json.dumps(compact, separators=(',', ':'))
        summary_body = _re.sub(r"AIRESULT:[^\n]*\n?", "", new_summary).strip()
        final_summary = airesult_line + ("\n" + summary_body if summary_body else "")

        update_fields = {"summary": final_summary}
        if new_title and new_title != title:
            update_fields["title"] = new_title
        # Persist the company website so the save card shows a direct link permanently
        if website and "." in website and not (save.get("url") or "").startswith("http"):
            update_fields["url"] = f"https://{website}"
        lib._sb().table("wa_saves").update(update_fields).eq("id", save_id).execute()
    except Exception:
        pass

    return jsonify({
        "ok":            True,
        "company":       company,
        "website":       website,
        "ad_type":       ad_type,
        "postcode":      postcode,
        "area":          area,
        "address":       address,
        "price":         price,
        "bedrooms":      beds,
        "property_type": prop_t,
        "wine_name":     wine_name,
        "vintage":       vintage,
        "grape":         grape,
        "region":        region,
        "notes":         notes,
        "links":         links,
    })


@app.route("/api/wa-saves/rename", methods=["POST"])
def api_wa_saves_rename():
    """Update the title of a saved item."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data = request.json or {}
    save_id = data.get("id")
    title = (data.get("title") or "").strip()
    if not save_id or not title:
        return jsonify({"error": "id and title required"}), 400
    try:
        q = lib._sb().table("wa_saves").update({"title": title}).eq("id", save_id)
        if from_number:
            q = q.eq("from_number", from_number)
        q.execute()
        rows = lib._sb().table("wa_saves").select("*").eq("id", save_id).execute().data
        if rows:
            lib.saves_sync(rows[0])
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/search")
def api_wa_saves_search():
    """Search saves: Algolia first, Supabase fallback."""
    from_number_raw, err = _check_saves_pin()
    if err:
        return err
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"hits": []})
    from_number = from_number_raw or ""

    # Try Algolia
    try:
        hits = lib.saves_search(q, from_number=from_number)
        results = [
            {"id": h["objectID"], "title": h.get("title",""), "url": h.get("url",""),
             "summary": h.get("summary",""), "status": h.get("status","pending"),
             "created_at": h.get("created_at","")}
            for h in hits
        ]
        return jsonify({"hits": results})
    except Exception:
        pass

    # Supabase fallback: full-text search on fts generated column, then ilike
    try:
        sb = lib._sb()
        base = sb.table("wa_saves").select("id,title,url,summary,status,created_at,remind_day")
        if from_number:
            base = base.in_("from_number", _wa_number_variants(from_number))
        try:
            rows = base.text_search("fts", q, config="english").order("created_at", desc=True).limit(20).execute().data
        except Exception:
            # fts column not yet created — fall back to ilike on both fields
            def _qi(field):
                b = sb.table("wa_saves").select("id,title,url,summary,status,created_at,remind_day")
                if from_number:
                    b = b.in_("from_number", _wa_number_variants(from_number))
                return b.ilike(field, f"%{q}%").order("created_at", desc=True).limit(20).execute().data
            seen, rows = set(), []
            for row in _qi("title") + _qi("summary"):
                if row["id"] not in seen:
                    seen.add(row["id"])
                    rows.append(row)
        return jsonify({"hits": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/reindex", methods=["POST"])
def api_wa_saves_reindex():
    """Bulk-index all existing saves into Algolia (admin only)."""
    admin_pw = os.environ.get("ADMIN_PASSWORD", "")
    pin = request.headers.get("X-Admin-Password", "")
    if admin_pw and pin != admin_pw:
        return jsonify({"error": "Forbidden"}), 403
    try:
        rows = lib._sb().table("wa_saves").select("*").execute().data
        for row in rows:
            lib.saves_sync(row)
        return jsonify({"ok": True, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/scan-image", methods=["POST"])
def api_wa_saves_scan_image():
    """Accept a base64 image, extract structured info via Groq vision, return as a save card."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data   = request.json or {}
    b64    = data.get("image_b64", "")
    mime   = data.get("mime", "image/jpeg")
    if not b64:
        return jsonify({"error": "image_b64 required"}), 400

    prompt = (
        "Look at this image and extract any useful contact or business information. "
        "Return ONLY a JSON object with these fields (use null if not found):\n"
        '{"title": "business or person name", "phone": "phone number", '
        '"website": "website or URL", "address": "address if visible", '
        '"notes": "any other useful text (e.g. service offered, opening hours)", '
        '"category": "one of: contact, business, place, product, other"}\n'
        "Do not include any explanation outside the JSON."
    )
    try:
        raw = _groq_vision(b64, mime, prompt)
        # Parse JSON from response
        import re as _re
        m = _re.search(r'\{.*\}', raw, _re.DOTALL)
        if not m:
            return jsonify({"error": "Could not extract info from image"}), 422
        extracted = json.loads(m.group())
        return jsonify({"ok": True, "extracted": extracted})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wa-saves/add", methods=["POST"])
def api_wa_saves_add():
    """Manually save a URL from the web UI."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data  = request.json or {}
    url   = (data.get("url") or "").strip()
    text  = (data.get("text") or "").strip()
    title = (data.get("title") or "").strip()
    save_as = from_number or "web"

    if url and url.startswith("http"):
        result = _wa_save_url(save_as, url)
        return jsonify({"ok": True, "message": result})
    elif text:
        # Save plain-text card (e.g. from AI image scan)
        try:
            from supabase import create_client
            sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
            sb.table("wa_saves").insert({
                "from_number": save_as,
                "title":       title or text[:60],
                "summary":     text,
                "category":    "contact",
                "source":      "ai-scan",
            }).execute()
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Provide url or text"}), 400


def _normalize_gbook(item):
    vi = item.get("volumeInfo", {})
    isbns = [x["identifier"] for x in vi.get("industryIdentifiers", []) if x.get("type") in ("ISBN_13", "ISBN_10")]
    cover = ""
    if vi.get("imageLinks"):
        cover = vi["imageLinks"].get("thumbnail") or vi["imageLinks"].get("smallThumbnail") or ""
        if cover:
            cover = cover.replace("http://", "https://")
    year = None
    pd = vi.get("publishedDate", "")
    if pd and len(pd) >= 4:
        try: year = int(pd[:4])
        except ValueError: pass
    return {
        "key": item.get("id", ""),
        "title": vi.get("title", ""),
        "author_name": vi.get("authors", []),
        "isbn": isbns,
        "cover": cover,
        "first_publish_year": year,
    }



@app.route("/api/books")
def api_books():
    """Book search: Google Books (with API key) → Open Library fallback."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"docs": []})

    gbooks_key = os.environ.get("GOOGLE_BOOKS_API_KEY", "")

    # 1. Google Books with API key — fast (~300ms), reliable, rich data
    if gbooks_key:
        try:
            r = requests.get(
                "https://www.googleapis.com/books/v1/volumes",
                params={"q": q, "maxResults": 10, "printType": "books",
                        "orderBy": "relevance", "key": gbooks_key},
                timeout=6,
            )
            r.raise_for_status()
            items = r.json().get("items", [])
            if items:
                return jsonify({"docs": [_normalize_gbook(i) for i in items]})
        except Exception as e:
            print(f"[api/books] Google Books error: {e}")

    # 2. Open Library fallback — free, no key needed
    try:
        r = requests.get(
            "https://openlibrary.org/search.json",
            params={"q": q, "limit": 10, "fields": "key,title,author_name,isbn,cover_i,first_publish_year"},
            timeout=10,
        )
        r.raise_for_status()
        return jsonify({"docs": r.json().get("docs", [])})
    except Exception as e:
        print(f"[api/books] Open Library error: {e}")
        return jsonify({"error": str(e), "docs": []})


@app.route("/api/book/intel")
def api_book_intel():
    """Fetch rich book intel: cover, author bio, key themes, similar books."""
    title = request.args.get("title", "").strip()
    if not title or len(title) < 2:
        return jsonify({"error": "title required"}), 400

    cache_key = f"bookintel:{title.lower()}"
    cached = _BRAND_CACHE.get(cache_key)
    if cached and time.time() - cached["ts"] < 86400:
        return jsonify(cached["data"])

    result = {"title": title, "found": False}
    gbooks_key = os.environ.get("GOOGLE_BOOKS_API_KEY", "")

    # 1. Fetch from Google Books
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f'intitle:"{title}"', "maxResults": 1, "printType": "books",
                    **({"key": gbooks_key} if gbooks_key else {})},
            timeout=8,
        )
        if r.status_code == 200:
            items = r.json().get("items", [])
            if items:
                info = items[0].get("volumeInfo", {})
                result.update({
                    "found":       True,
                    "title":       info.get("title", title),
                    "subtitle":    info.get("subtitle", ""),
                    "authors":     info.get("authors", []),
                    "publisher":   info.get("publisher", ""),
                    "year":        (info.get("publishedDate", "") or "")[:4],
                    "description": info.get("description", "")[:600],
                    "categories":  info.get("categories", []),
                    "page_count":  info.get("pageCount"),
                    "cover":       (info.get("imageLinks", {}).get("thumbnail", "")
                                   or info.get("imageLinks", {}).get("smallThumbnail", "")),
                    "isbn":        next((i["identifier"] for i in info.get("industryIdentifiers", [])
                                       if i["type"] in ("ISBN_13","ISBN_10")), ""),
                    "google_id":   items[0].get("id", ""),
                    "preview_url": info.get("previewLink", ""),
                    "rating":      info.get("averageRating"),
                    "rating_count": info.get("ratingsCount"),
                })
    except Exception as _e:
        print(f"[book/intel] Google Books error: {_e}")

    # 2. Groq: key ideas + similar books
    if result.get("found") and os.environ.get("GROQ_API_KEY"):
        try:
            desc_ctx = result.get("description", "")[:400] or title
            prompt = (
                f'Book: "{result["title"]}" by {", ".join(result.get("authors",[]) or ["unknown"])}\n'
                f'Description: {desc_ctx}\n\n'
                'Return ONLY valid JSON (no markdown):\n'
                '{"key_ideas":["3-5 one-sentence key takeaways from the book"],'
                '"similar_books":[{"title":"","author":""}],'
                '"who_for":"one sentence describing who would benefit most from this book"}'
            )
            gr = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {os.environ['GROQ_API_KEY']}", "Content-Type": "application/json"},
                json={"model": "llama-3.1-8b-instant", "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 400, "temperature": 0.2},
                timeout=10,
            )
            if gr.status_code == 200:
                import json as _jj, re as _rr
                raw = gr.json()["choices"][0]["message"]["content"]
                m = _rr.search(r'\{.*\}', raw, _rr.DOTALL)
                if m:
                    ai = _jj.loads(m.group(0))
                    result["key_ideas"]    = ai.get("key_ideas", [])
                    result["similar_books"] = ai.get("similar_books", [])[:4]
                    result["who_for"]      = ai.get("who_for", "")
        except Exception as _e:
            print(f"[book/intel] groq error: {_e}")

    if result.get("found"):
        _BRAND_CACHE[cache_key] = {"ts": time.time(), "data": result}
    return jsonify(result)


@app.route("/api/book/isbn/<isbn>")
def api_book_isbn(isbn):
    """Fetch full book detail by ISBN via Google Books API."""
    isbn = isbn.strip()
    try:
        r = requests.get(
            "https://www.googleapis.com/books/v1/volumes",
            params={"q": f"isbn:{isbn}", "maxResults": 1},
            timeout=10,
        )
        items = r.json().get("items")
        if not items:
            return jsonify({"found": False})

        vi = items[0].get("volumeInfo", {})
        cover = ""
        if vi.get("imageLinks"):
            cover = vi["imageLinks"].get("thumbnail") or vi["imageLinks"].get("smallThumbnail") or ""
            if cover:
                cover = cover.replace("http://", "https://")

        all_isbns = [x["identifier"] for x in vi.get("industryIdentifiers", []) if x.get("type") in ("ISBN_13", "ISBN_10")]
        subjects = " · ".join(vi.get("categories", [])[:6])

        rating = vi.get("averageRating")
        community_rating = {"avg": round(float(rating), 1), "count": vi.get("ratingsCount", 0)} if rating else None

        return jsonify({
            "found":           True,
            "isbn":            (all_isbns[0] if all_isbns else isbn),
            "title":           vi.get("title", ""),
            "author":          ", ".join(vi.get("authors", [])) or "Unknown author",
            "cover":           cover,
            "description":     vi.get("description", ""),
            "subjects":        subjects,
            "communityRating": community_rating,
            "year":            vi.get("publishedDate", "")[:4] if vi.get("publishedDate") else "",
            "publishers":      vi.get("publisher", ""),
            "pageCount":       vi.get("pageCount"),
        })
    except Exception as e:
        return jsonify({"error": str(e), "found": False})


@app.route("/api/book/summary")
def api_book_summary():
    """Generate AI summary + reader perspectives for a book."""
    title       = request.args.get("title", "").strip()
    author      = request.args.get("author", "").strip()
    description = request.args.get("description", "").strip()
    subjects    = request.args.get("subjects", "").strip()
    rating      = request.args.get("rating", "").strip()
    rating_count = request.args.get("rating_count", "").strip()

    if not title:
        return jsonify({"error": "title required"}), 400

    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return jsonify({"error": "AI not available"}), 503

    rating_line = ""
    if rating:
        rating_line = f"Community rating: {rating}/5 from {rating_count} readers.\n"

    prompt = f"""Book: "{title}" by {author or "Unknown"}.
{rating_line}Subjects: {subjects or "not specified"}.
Description: {description[:500] if description else "not available"}.

Reply with a JSON object with these keys:
"summary": 2 short plain sentences — what the book is about. Simple language, no jargon.
"audience": 10 words max — who this is for. Start with "For...".
"verdict": 1 sentence — overall reader consensus.
"reviews": array of 3 strings, each max 20 words. One enthusiastic, one balanced, one critical. No names."""

    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 400,
                "temperature": 0.3,
                "response_format": {"type": "json_object"},
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()["choices"][0]["message"]["content"]
        return jsonify(json.loads(data))
    except Exception as e:
        print(f"[book/summary] error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/books/sync", methods=["POST"])
def api_books_sync():
    """Sync a user's book library against Supabase using phone as identity.
    Body: { phone, books: [{isbn, title, author, cover, description, year, rating, status, notes, added}] }
    Returns merged book list for that phone.
    """
    data  = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    if not phone:
        return jsonify({"error": "phone required"}), 400

    sb = lib._sb()
    local_books = data.get("books", [])

    # Upsert local books into wa_saves (url = book:{isbn})
    for bk in local_books:
        isbn  = (bk.get("isbn") or "").strip()
        title = (bk.get("title") or "Untitled").strip()
        if not isbn:
            continue
        url = f"book:{isbn}"
        existing = sb.table("wa_saves").select("id").eq("from_number", phone).eq("url", url).execute().data
        payload = {
            "from_number": phone,
            "url":         url,
            "title":       title,
            "summary":     json.dumps(bk),
            "status":      bk.get("status", "read"),
        }
        if existing:
            sb.table("wa_saves").update(payload).eq("id", existing[0]["id"]).execute()
        else:
            sb.table("wa_saves").insert(payload).execute()

    # Fetch all book saves for this phone
    rows = sb.table("wa_saves") \
        .select("id,url,title,summary,status,created_at") \
        .eq("from_number", phone) \
        .like("url", "book:%") \
        .order("created_at", desc=True) \
        .execute().data

    merged = []
    seen = set()
    for row in rows:
        try:
            bk = json.loads(row["summary"] or "{}")
        except Exception:
            bk = {}
        isbn = row["url"].replace("book:", "")
        if isbn in seen:
            continue
        seen.add(isbn)
        bk.setdefault("isbn",   isbn)
        bk.setdefault("title",  row["title"] or "")
        bk.setdefault("status", row["status"] or "read")
        bk["_id"] = row["id"]
        merged.append(bk)

    return jsonify({"books": merged})


@app.route("/api/books/save", methods=["POST"])
def api_books_save():
    """Save or update a single book for a phone number."""
    data  = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    bk    = data.get("book") or {}
    isbn  = (bk.get("isbn") or "").strip()
    if not phone or not isbn:
        return jsonify({"error": "phone and isbn required"}), 400

    sb  = lib._sb()
    url = f"book:{isbn}"
    existing = sb.table("wa_saves").select("id").eq("from_number", phone).eq("url", url).execute().data
    payload = {
        "from_number": phone,
        "url":         url,
        "title":       (bk.get("title") or "").strip(),
        "summary":     json.dumps(bk),
        "status":      bk.get("status", "read"),
    }
    if existing:
        sb.table("wa_saves").update(payload).eq("id", existing[0]["id"]).execute()
    else:
        sb.table("wa_saves").insert(payload).execute()

    lib.books_upsert(phone, bk)
    return jsonify({"ok": True})


@app.route("/api/books/delete", methods=["POST"])
def api_books_delete():
    """Delete a book from a phone's library by ISBN."""
    data  = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    isbn  = (data.get("isbn") or "").strip()
    if not phone or not isbn:
        return jsonify({"error": "phone and isbn required"}), 400
    lib._sb().table("wa_saves") \
        .delete() \
        .eq("from_number", phone) \
        .eq("url", f"book:{isbn}") \
        .execute()
    lib.books_delete(phone, isbn)
    return jsonify({"ok": True})


@app.route("/api/books/search")
def api_books_search():
    phone = (request.args.get("phone") or "").strip()
    q     = (request.args.get("q") or "").strip()
    if not phone or not q:
        return jsonify({"results": []})
    hits = lib.books_search(phone, q)
    if not hits:
        rows = lib._sb().table("wa_saves") \
            .select("url,title,summary") \
            .eq("from_number", phone) \
            .like("url", "book:%") \
            .order("created_at", desc=True) \
            .limit(500).execute().data
        q_words = [w.lower() for w in q.split() if len(w) > 1]
        for row in rows:
            try:
                bk = json.loads(row["summary"] or "{}")
            except Exception:
                bk = {}
            bk.setdefault("isbn", row["url"].replace("book:", ""))
            bk.setdefault("title", row["title"] or "")
            text = f"{bk.get('title','')} {bk.get('author','')}".lower()
            if any(w in text for w in q_words):
                hits.append(bk)
    return jsonify({"results": hits[:30]})


@app.route("/api/music/spotify-status")
def api_music_spotify_status():
    """Debug: check Spotify client credentials config and token fetch."""
    cid    = os.environ.get("SPOTIFY_CLIENT_ID", "")
    secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
    result = {
        "client_id_set":     bool(cid),
        "client_secret_set": bool(secret),
        "client_id_preview": cid[:8] + "…" if cid else "",
    }
    if cid and secret:
        try:
            token = _get_spotify_app_token()
            result["token_ok"] = bool(token)
            result["token_preview"] = token[:12] + "…" if token else ""
        except Exception as e:
            result["token_error"] = str(e)
    return jsonify(result)


@app.route("/api/music/spotify-raw")
def api_music_spotify_raw():
    """Debug: raw Spotify playlist response to diagnose chart failures."""
    try:
        token = _get_spotify_app_token()
        if not token:
            return jsonify({"error": "no token"})
        playlist_id = _SPOTIFY_PLAYLISTS.get("GB", _SPOTIFY_PLAYLISTS[""])
        r = requests.get(
            f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
            headers={"Authorization": f"Bearer {token}"},
            params={"limit": 3, "market": "GB"},
            timeout=10,
        )
        raw = r.json()
        items = raw.get("items", [])
        return jsonify({
            "status": r.status_code,
            "total": raw.get("total"),
            "item_count": len(items),
            "first_item": items[0] if items else None,
            "error": raw.get("error"),
        })
    except Exception as e:
        return jsonify({"exception": str(e)})


@app.route("/api/music/identify", methods=["POST"])
def api_music_identify():
    key = os.environ.get("RAPIDAPI_KEY", "")
    if not key:
        return jsonify({"error": "RAPIDAPI_KEY not configured"}), 503
    # Frontend sends raw PCM base64 as text/plain — pass straight to Shazam
    audio_b64 = request.get_data(as_text=True).strip()
    if not audio_b64:
        return jsonify({"error": "no audio"}), 400
    try:
        r = requests.post(
            "https://shazam.p.rapidapi.com/songs/detect",
            headers={
                "X-RapidAPI-Key":  key,
                "X-RapidAPI-Host": "shazam.p.rapidapi.com",
                "Content-Type":    "text/plain",
            },
            data=audio_b64,
            timeout=15,
        )
        print(f"[music/identify] status={r.status_code} body={r.text[:300]!r}")
        if not r.text.strip():
            return jsonify({"match": False})
        r.raise_for_status()
        data  = r.json()
        track = data.get("track", {})
        if not track:
            return jsonify({"match": False})
        images = track.get("images", {})
        spotify_uri = ""
        for p in (track.get("hub") or {}).get("providers", []):
            if "spotify" in p.get("caption", "").lower():
                for a in p.get("actions", []):
                    if a.get("uri", "").startswith("spotify:"):
                        spotify_uri = a["uri"]; break
        return jsonify({
            "match":   True,
            "title":   track.get("title", ""),
            "artist":  track.get("subtitle", ""),
            "cover":   images.get("coverarthq") or images.get("coverart", ""),
            "spotify": spotify_uri,
            "url":     track.get("url", ""),
        })
    except Exception as e:
        print(f"[music/identify] {e}")
        return jsonify({"error": str(e)}), 500


_spotify_app_token: str = ""
_spotify_app_token_expires: float = 0.0

_SPOTIFY_PLAYLISTS = {
    "GB": "37i9dQZEVXbLnolsZ8PSNw",   # Spotify UK Top 50
    "":   "37i9dQZEVXbMDoHDwVN2tF",   # Spotify Global Top 50
}

def _get_spotify_app_token() -> str:
    global _spotify_app_token, _spotify_app_token_expires
    if _spotify_app_token and time.time() < _spotify_app_token_expires:
        return _spotify_app_token
    cid = os.environ.get("SPOTIFY_CLIENT_ID", "")
    secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
    if not cid or not secret:
        return ""
    import base64 as _b64
    creds = _b64.b64encode(f"{cid}:{secret}".encode()).decode()
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {creds}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials"},
        timeout=10,
    )
    r.raise_for_status()
    d = r.json()
    _spotify_app_token = d["access_token"]
    _spotify_app_token_expires = time.time() + d.get("expires_in", 3600) - 60
    return _spotify_app_token


# music_saves table DDL (run once in Supabase):
# CREATE TABLE IF NOT EXISTS music_saves (
#   id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
#   phone       text NOT NULL,
#   title       text NOT NULL,
#   artist      text NOT NULL DEFAULT '',
#   album       text NOT NULL DEFAULT '',
#   year        text NOT NULL DEFAULT '',
#   genre       text NOT NULL DEFAULT '',
#   cover_url   text NOT NULL DEFAULT '',
#   spotify_url text NOT NULL DEFAULT '',
#   preview_url text NOT NULL DEFAULT '',
#   created_at  timestamptz NOT NULL DEFAULT now()
# );
# CREATE INDEX ON music_saves(phone, created_at DESC);

@app.route("/api/music/save", methods=["POST"])
def api_music_save():
    data = request.get_json(silent=True) or {}
    phone  = (data.get("phone")  or "").strip()
    title  = (data.get("title")  or "").strip()
    artist = (data.get("artist") or "").strip()
    if not phone or not title:
        return jsonify({"error": "phone and title required"}), 400
    try:
        existing = lib._sb().table("music_saves").select("id") \
            .eq("phone", phone).eq("title", title).eq("artist", artist).execute().data
        if existing:
            return jsonify({"ok": True, "id": existing[0]["id"], "already": True})
        row = lib._sb().table("music_saves").insert({
            "phone":       phone,
            "title":       title,
            "artist":      artist,
            "album":       data.get("album")       or "",
            "year":        data.get("year")        or "",
            "genre":       data.get("genre")       or "",
            "cover_url":   data.get("cover")       or "",
            "spotify_url": data.get("spotify")     or "",
            "preview_url": data.get("preview_url") or "",
        }).execute().data
        return jsonify({"ok": True, "id": row[0]["id"] if row else None})
    except Exception as e:
        print(f"[music/save] {e}")
        return jsonify({"error": "could not save"}), 500


@app.route("/api/music/saves")
def api_music_saves():
    phone = request.args.get("phone", "").strip()
    if not phone:
        return jsonify({"error": "phone required"}), 400
    try:
        rows = lib._sb().table("music_saves").select("*") \
            .eq("phone", phone).order("created_at", desc=True).limit(50).execute().data or []
        songs = [{
            "id":          r["id"],
            "title":       r.get("title")       or "",
            "artist":      r.get("artist")      or "",
            "cover":       r.get("cover_url")   or "",
            "url":         r.get("spotify_url") or "",
            "preview_url": r.get("preview_url") or "",
            "album":       r.get("album")       or "",
            "year":        r.get("year")        or "",
            "genre":       r.get("genre")       or "",
            "saved":       (r.get("created_at") or "")[:10],
        } for r in rows]
        return jsonify({"songs": songs})
    except Exception as e:
        print(f"[music/saves] {e}")
        return jsonify({"error": "could not load"}), 500


@app.route("/api/music/saves/<save_id>", methods=["PATCH"])
def api_music_save_patch(save_id):
    """Update an unmatched music save with correct iTunes metadata."""
    data = request.json or {}
    allowed = {"title", "artist", "album", "year", "genre", "cover_url", "preview_url", "spotify_url"}
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"error": "nothing to update"}), 400
    try:
        lib._sb().table("music_saves").update(updates).eq("id", save_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/music/save/<save_id>", methods=["DELETE"])
def api_music_save_delete(save_id):
    phone = request.args.get("phone", "").strip()
    if not phone:
        return jsonify({"error": "phone required"}), 400
    try:
        lib._sb().table("music_saves").delete().eq("id", save_id).eq("phone", phone).execute()
        return jsonify({"ok": True})
    except Exception as e:
        print(f"[music/save/delete] {e}")
        return jsonify({"error": "could not delete"}), 500


@app.route("/api/voice-query", methods=["POST"])
def api_voice_query():
    """Voice assistant: parse intent from speech transcript, return a speakable answer."""
    data = request.json or {}
    query    = (data.get("query") or "").strip()
    postcode = (data.get("postcode") or "").replace(" ", "").upper()
    phone    = (data.get("phone") or "").strip()

    if not query:
        return jsonify({"error": "No query provided"})

    if not os.environ.get("GROQ_API_KEY"):
        return jsonify({"error": "Voice assistant not configured (GROQ_API_KEY missing)"})

    # Resolve postcode from DB if not in localStorage (phone is the key)
    if not postcode and phone:
        postcode = _get_wa_home_postcode(phone) or ""

    # Step 1: parse intent
    intent_prompt = (
        f'Parse this voice query from a UK user and return JSON only.\n'
        f'Query: "{query}"\n'
        f'Return exactly: {{"intent":"train|fuel|weather|councillor|mp|general","from_station":"name or null","to_station":"name or null"}}'
    )
    try:
        raw = _groq_chat(
            "You are a query parser. Return only valid JSON, no explanation.",
            [{"role": "user", "content": intent_prompt}],
            max_tokens=80, json_mode=True
        )
        import json as _j
        intent = _j.loads(raw)
    except Exception as e:
        return jsonify({"error": f"Couldn't parse query: {e}"})

    intent_type = intent.get("intent", "general")

    # Step 2: execute
    if intent_type == "train":
        from_s = (intent.get("from_station") or "").strip()
        to_s   = (intent.get("to_station")   or "").strip()

        # No from station — find nearest from postcode
        if not from_s and postcode:
            try:
                base = request.host_url.rstrip("/")
                nr = requests.get(f"{base}/api/train/nearest-by-postcode?postcode={postcode}", timeout=8)
                nr_d = nr.json()
                from_s = nr_d.get("name") or ""
            except Exception:
                pass

        if not from_s:
            return jsonify({"answer": "Which station would you like departures from?", "intent": "train"})

        result = _wa_train_format(from_s, to_s)
        if not result:
            return jsonify({"answer": f"I couldn't find {from_s} station. Check the name and try again.", "intent": "train"})

        # Use Groq to turn raw departure data into a natural spoken sentence
        try:
            spoken = _groq_chat(
                "You are a friendly UK voice assistant. "
                "The user asked: \"" + query + "\". "
                "Below is live train departure data. "
                "Reply in ONE natural conversational sentence covering just the next train or two. "
                "Mention departure time, destination, and whether it's on time or delayed. "
                "No markdown, no lists, no asterisks, no emojis.",
                [{"role": "user", "content": result}],
                max_tokens=120
            )
            return jsonify({"answer": spoken.strip(), "intent": "train"})
        except Exception:
            # Fallback: basic cleanup
            import re as _re
            clean = _re.sub(r'[*_`]', '', result)
            clean = clean.replace("→", "to").replace("\n", ". ")
            clean = _re.sub(r'\s{2,}', ' ', clean).strip()
            return jsonify({"answer": clean, "intent": "train"})

    elif intent_type == "fuel":
        if not postcode:
            return jsonify({"answer": "I need your postcode to find fuel prices. Set it in your profile first.", "intent": "fuel"})
        try:
            base = request.host_url.rstrip("/")
            r = requests.get(f"{base}/api/search?postcode={postcode}&fuel=petrol&radius=3&mode=fuel", timeout=10)
            d = r.json()
            stations = d.get("stations") or d.get("results") or []
            if not stations:
                return jsonify({"answer": f"I couldn't find any fuel stations near you right now.", "intent": "fuel"})
            best  = stations[0]
            price = best.get("price") or best.get("petrol_price")
            name  = best.get("name") or best.get("brand") or "a nearby station"
            dist  = best.get("distance")
            dist_str = f" about {dist:.1f} miles away" if dist else " nearby"
            answer = f"The cheapest petrol near you is {price:.1f}p a litre at {name},{dist_str}."
            return jsonify({"answer": answer, "intent": "fuel"})
        except Exception as e:
            return jsonify({"answer": "I couldn't get fuel prices right now. Try again in a moment.", "intent": "fuel"})

    elif intent_type == "weather":
        if not postcode:
            return jsonify({"answer": "Set your postcode in My Area first, then I can give you a weather update.", "intent": "weather"})
        try:
            pc_r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=5)
            pc_d = pc_r.json()
            lat  = pc_d["result"]["latitude"]
            lng  = pc_d["result"]["longitude"]
            district = pc_d["result"].get("admin_district", postcode)
            wx_r = requests.get(
                f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lng}"
                "&current=temperature_2m,weather_code,apparent_temperature&timezone=auto",
                timeout=8
            )
            cur  = wx_r.json().get("current", {})
            temp = round(cur.get("temperature_2m", 0))
            feel = round(cur.get("apparent_temperature", temp))
            code = cur.get("weather_code", 0)
            descs = {0:"clear skies",1:"mostly clear",2:"partly cloudy",3:"overcast",
                     45:"foggy",51:"light drizzle",61:"light rain",63:"rain",65:"heavy rain",
                     80:"showers",81:"heavy showers",95:"thunderstorms"}
            desc = descs.get(code, "cloudy")
            feel_note = ""
            if feel <= 4:   feel_note = " Wrap up well, it feels really cold."
            elif feel <= 10: feel_note = " You'll want a jacket."
            elif feel >= 22: feel_note = " Quite warm — enjoy it!"
            answer = f"In {district} it's {temp} degrees with {desc}, feeling like {feel}.{feel_note}"
            return jsonify({"answer": answer, "intent": "weather"})
        except Exception as e:
            return jsonify({"answer": "I couldn't get the weather right now. Try again in a moment.", "intent": "weather"})

    elif intent_type in ("councillor", "mp"):
        if not postcode:
            return jsonify({"answer": "I couldn't find your postcode. Set it in My Area first.", "intent": intent_type})
        try:
            if intent_type == "mp":
                base = request.host_url.rstrip("/")
                r = requests.get(f"{base}/api/mp?postcode={postcode}", timeout=10)
                d = r.json()
                mp = d.get("mp") or {}
                name  = mp.get("name") or d.get("name") or ""
                party = mp.get("party") or d.get("party") or ""
                const = mp.get("constituency") or d.get("constituency") or ""
                if name:
                    answer = f"Your MP is {name} of {party}, representing {const}."
                else:
                    answer = "I couldn't find your MP details right now."
            else:
                result = _resolve_councillors(postcode)
                cllrs  = result.get("councillors") or []
                ward   = result.get("ward") or ""
                if not cllrs:
                    answer = f"I couldn't find councillor details for your area."
                elif len(cllrs) == 1:
                    c = cllrs[0]
                    answer = f"Your councillor for {ward} is {c.get('name','')}, {c.get('party','')}."
                else:
                    names = ", ".join(c.get("name","") for c in cllrs[:3])
                    answer = f"Your councillors for {ward} are {names}."
            return jsonify({"answer": answer, "intent": intent_type})
        except Exception as e:
            return jsonify({"answer": "I couldn't look up your local representatives right now.", "intent": intent_type})

    else:
        # General — Groq with full user context pulled from DB
        try:
            ctx_parts = []
            if postcode:
                ctx_parts.append(f"postcode {postcode}")
            # Pull stored My Area data (councillors, GPs, etc.) if available
            if postcode:
                try:
                    cllr_data = _resolve_councillors(postcode)
                    cllrs = cllr_data.get("councillors") or []
                    ward  = cllr_data.get("ward") or ""
                    if cllrs:
                        names = ", ".join(f"{c.get('name','')} ({c.get('party','')})" for c in cllrs[:3])
                        ctx_parts.append(f"local councillors for {ward}: {names}")
                except Exception:
                    pass
            ctx = "User context: " + "; ".join(ctx_parts) + ". " if ctx_parts else ""
            answer = _groq_chat(
                f"You are Miru, a helpful UK voice assistant. {ctx}"
                "You already know the user's location and stored details — never ask for them. "
                "Give a short, direct spoken answer in 1-2 sentences. No markdown, no bullet points, no emojis.",
                [{"role": "user", "content": query}],
                max_tokens=150
            )
            return jsonify({"answer": answer.strip(), "intent": "general"})
        except Exception:
            return jsonify({"answer": "I couldn't answer that right now. Try again.", "intent": "general"})


@app.route("/api/music/gigs")
def api_music_gigs():
    tm_key     = os.environ.get("TICKETMASTER_KEY", "")
    sk_key     = os.environ.get("SKIDDLE_KEY", "")
    postcode   = request.args.get("postcode", "").replace(" ", "").upper()
    if not postcode:
        return jsonify({"error": "postcode required"}), 400
    if not tm_key and not sk_key:
        return jsonify({"error": "Gigs not available — no API keys configured in Railway"})
    try:
        pc_r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=5)
        pc_d = pc_r.json()
        lat  = pc_d["result"]["latitude"]
        lng  = pc_d["result"]["longitude"]
    except Exception:
        return jsonify({"error": "Could not resolve postcode"}), 400

    import math as _math
    import datetime as _dt

    def _haversine(lat1, lng1, lat2, lng2):
        R = 6371
        dlat = _math.radians(lat2 - lat1)
        dlng = _math.radians(lng2 - lng1)
        a = _math.sin(dlat/2)**2 + _math.cos(_math.radians(lat1)) * _math.cos(_math.radians(lat2)) * _math.sin(dlng/2)**2
        return round(R * 2 * _math.asin(_math.sqrt(a)), 1)

    events = []
    seen   = set()
    today  = _dt.date.today().isoformat()

    # ── Ticketmaster ─────────────────────────────────────────────
    if tm_key:
        try:
            r = requests.get(
                "https://app.ticketmaster.com/discovery/v2/events.json",
                params={"apikey": tm_key, "latlong": f"{lat},{lng}", "radius": "20",
                        "unit": "miles", "classificationName": "music",
                        "startDateTime": today + "T00:00:00Z",
                        "sort": "date,asc", "size": "20"},
                timeout=10,
            )
            r.raise_for_status()
            for ev in (r.json().get("_embedded") or {}).get("events", []):
                venue    = ((ev.get("_embedded") or {}).get("venues") or [{}])[0]
                date_inf = ev.get("dates", {}).get("start", {})
                image    = next((i["url"] for i in (ev.get("images") or [])
                                 if i.get("ratio") == "16_9" and i.get("width", 0) >= 640), "")
                name = ev.get("name", "")
                date = date_inf.get("localDate", "")
                key_ = (name.lower(), date)
                if key_ in seen:
                    continue
                seen.add(key_)
                # Venue distance
                vloc = venue.get("location") or {}
                try:
                    vdist = _haversine(lat, lng, float(vloc["latitude"]), float(vloc["longitude"]))
                except Exception:
                    vdist = None
                price_ranges = ev.get("priceRanges") or []
                price = f"from £{price_ranges[0]['min']:.0f}" if price_ranges else ""
                events.append({"name": name, "date": date,
                               "time": date_inf.get("localTime", ""),
                               "venue": venue.get("name", ""),
                               "city":  (venue.get("city") or {}).get("name", ""),
                               "url":   ev.get("url", ""), "image": image,
                               "distance_km": vdist, "price": price})
        except Exception as e:
            print(f"[gigs/ticketmaster] {e}")

    # ── Skiddle ──────────────────────────────────────────────────
    if sk_key:
        try:
            r = requests.get(
                "https://www.skiddle.com/api/v1/events/search/",
                params={"api_key": sk_key, "latitude": lat, "longitude": lng,
                        "radius": 20, "order": "distance", "eventcode": "LIVE",
                        "minDate": today, "limit": 20},
                timeout=10,
            )
            r.raise_for_status()
            _MUSIC_CODES = {"LIVE", "FEST", "CLUB"}
            for ev in r.json().get("results", []):
                ev_type = (ev.get("EventCode") or ev.get("type") or "").upper()
                if ev_type and ev_type not in _MUSIC_CODES:
                    continue
                name  = ev.get("eventname", "") or ev.get("EventName", "")
                date  = ev.get("startdate", "")[:10] if ev.get("startdate") else ""
                if date and date < today:
                    continue
                key_ = (name.lower(), date)
                if key_ in seen:
                    continue
                seen.add(key_)
                vobj  = ev.get("venue") or {}
                try:
                    vdist = _haversine(lat, lng, float(vobj["latitude"]), float(vobj["longitude"]))
                except Exception:
                    vdist = None
                events.append({"name": name, "date": date,
                               "time": ev.get("starttime", ""),
                               "venue": vobj.get("name", ""),
                               "city":  vobj.get("town", ""),
                               "url":   ev.get("link", "") or f"https://www.skiddle.com/e/{ev.get('id','')}",
                               "image": ev.get("largeimageurl", "") or ev.get("imageurl", ""),
                               "distance_km": vdist})
        except Exception as e:
            print(f"[gigs/skiddle] {e}")

    # Sort: date first, then distance within the same day
    events.sort(key=lambda e: (e.get("date") or "9999", e.get("distance_km") or 999))
    return jsonify({"events": events[:25]})


@app.route("/api/music/charts")
def api_music_charts():
    country = request.args.get("country", "").upper()

    # ── Spotify Top 50 playlists (no market param — avoids 403) ──────────────
    playlist_id = _SPOTIFY_PLAYLISTS.get(country, _SPOTIFY_PLAYLISTS[""])
    try:
        token = _get_spotify_app_token()
        if token:
            r = requests.get(
                f"https://api.spotify.com/v1/playlists/{playlist_id}/items",
                headers={"Authorization": f"Bearer {token}"},
                params={"limit": 20, "fields": "items(track(id,name,artists,album(images),external_urls,duration_ms))"},
                timeout=10,
            )
            r.raise_for_status()
            tracks = []
            for i, item in enumerate(r.json().get("items", [])):
                t = item.get("track") or {}
                if not t or not t.get("name"):
                    continue
                images = t.get("album", {}).get("images", [])
                cover = images[0]["url"] if images else ""
                artists = ", ".join(a["name"] for a in t.get("artists", []))
                tracks.append({
                    "position":    i + 1,
                    "title":       t["name"],
                    "artist":      artists,
                    "cover":       cover,
                    "track_id":    t.get("id", ""),
                    "spotify_url": t.get("external_urls", {}).get("spotify", ""),
                })
            if tracks:
                return jsonify({"tracks": tracks, "source": "spotify"})
    except Exception as e:
        print(f"[music/charts/spotify] {e}")

    # ── iTunes fallback ───────────────────────────────────────────────────────
    feed_country = "gb" if country == "GB" else "us"
    try:
        r = requests.get(
            f"https://itunes.apple.com/{feed_country}/rss/topsongs/limit=20/json",
            timeout=10,
        )
        r.raise_for_status()
        entries = r.json()["feed"]["entry"]
        tracks = []
        for i, e in enumerate(entries):
            images = e.get("im:image", [])
            cover = images[2].get("label", "") if len(images) > 2 else (images[-1].get("label", "") if images else "")
            tracks.append({
                "position": i + 1,
                "title":    e.get("im:name", {}).get("label", ""),
                "artist":   e.get("im:artist", {}).get("label", ""),
                "cover":    cover,
            })
        return jsonify({"tracks": tracks, "source": "itunes"})
    except Exception as e:
        print(f"[music/charts/itunes] {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/spotify/config")
def api_spotify_config():
    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "")
    if not client_id:
        return jsonify({"available": False})
    # Use env var if set; otherwise derive from host header, forcing https for Railway
    redirect_uri = os.environ.get("SPOTIFY_REDIRECT_URI", "")
    if not redirect_uri:
        host = request.host  # e.g. miru.humanagency.co
        redirect_uri = f"https://{host}/spotify/callback"
    return jsonify({"available": True, "client_id": client_id, "redirect_uri": redirect_uri})


@app.route("/spotify/callback")
def spotify_callback():
    import json as _json
    code  = request.args.get("code", "")
    error = request.args.get("error", "")
    if error or not code:
        return (
            "<!doctype html><html><body><script>"
            "localStorage.removeItem('_sp_verifier');"
            "window.location='/?screen=music&sp_error=1';"
            "</script></body></html>"
        )
    safe_code = _json.dumps(code)
    return (
        f"<!doctype html><html><body><script>"
        f"localStorage.setItem('_sp_code',{safe_code});"
        f"window.location='/?screen=music&sp_connected=1';"
        f"</script></body></html>"
    )


@app.route("/api/tube/test")
def api_tube_test():
    """Test TfL tube integration — no auth needed. Pass ?postcode=SW1A1AA to test nearest-station."""
    results = {}
    try:
        results["status_all"] = get_tube_status()
    except Exception as e:
        results["status_all_error"] = str(e)
    try:
        results["status_victoria"] = get_tube_status("victoria")
    except Exception as e:
        results["status_victoria_error"] = str(e)
    try:
        results["journey_kgx_to_wat"] = get_tube_journey("Kings Cross", "Waterloo")
    except Exception as e:
        results["journey_error"] = str(e)
    try:
        results["command_parse_status"]  = handle_tube_command("tube status")
        results["command_parse_journey"] = handle_tube_command("tube Victoria to Waterloo")
    except Exception as e:
        results["command_parse_error"] = str(e)
    # Nearest station test — pass ?postcode=SW1A1AA
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if postcode:
        try:
            latlon = postcode_to_latlon(postcode)
            if latlon:
                lat, lon = latlon
                nid, nname, ndist = _nearest_tube_station(lat, lon)
                results["nearest_station"] = {"id": nid, "name": nname, "dist_m": ndist}
                if nid:
                    results["nearest_arrivals"] = get_tube_arrivals(nid, nname)
            else:
                results["nearest_station_error"] = "Postcode not found"
        except Exception as e:
            results["nearest_station_error"] = str(e)
    return jsonify(results)


@app.route("/api/tube/nearest")
def api_tube_nearest():
    """Find nearest tube stations by lat/lon (mirrors /api/train/nearest)."""
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lng", ""))
    except (TypeError, ValueError):
        return jsonify({"error": "lat/lng required"}), 400
    try:
        r = requests.get(
            "https://api.tfl.gov.uk/StopPoint",
            params={"lat": lat, "lon": lon, "stopTypes": "NaptanMetroStation,NaptanRailAccessArea",
                    "radius": 1500, "modes": "tube,dlr,elizabeth-line,overground", "returnLines": "false"},
            timeout=8,
        )
        r.raise_for_status()
        stops = r.json().get("stopPoints", [])
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    stations = [
        {"id": s["id"],
         "name": s["commonName"].replace(" Underground Station", ""),
         "distance_km": round(s.get("distance", 0) / 1000, 2)}
        for s in stops[:4]
    ]
    return jsonify({"stations": stations})


@app.route("/api/tube/nearest-by-postcode")
def api_tube_nearest_by_postcode():
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if not postcode:
        return jsonify({"error": "postcode required"}), 400
    latlon = postcode_to_latlon(postcode)
    if not latlon:
        return jsonify({"error": "Postcode not found"}), 404
    lat, lon = latlon
    nid, nname, ndist = _nearest_tube_station(lat, lon, radius_m=3000)
    if not nid:
        return jsonify({"error": "No tube station within 3km"}), 404
    dist_km = round((ndist or 0) / 1000, 2)
    return jsonify({"id": nid, "name": nname, "distance_km": dist_km, "lat": lat, "lon": lon})


@app.route("/api/tube/arrivals")
def api_tube_arrivals():
    naptan_id = request.args.get("id", "").strip()
    if not naptan_id:
        return jsonify({"error": "id required"}), 400
    try:
        r = requests.get(
            f"https://api.tfl.gov.uk/StopPoint/{naptan_id}/Arrivals",
            timeout=8,
        )
        r.raise_for_status()
        raw = r.json()
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    raw.sort(key=lambda a: a.get("timeToStation", 9999))
    seen, trains = set(), []
    for a in raw:
        line     = a.get("lineName", "")
        dest     = a.get("destinationName", "").replace(" Underground Station", "")
        platform = a.get("platformName", "")
        secs     = a.get("timeToStation", 0)
        mins     = secs // 60
        key      = (line, platform)
        if key in seen:
            continue
        seen.add(key)
        trains.append({
            "line": line,
            "destination": dest,
            "platform": platform,
            "minutes": mins,
            "due": mins < 1,
        })
        if len(trains) >= 10:
            break
    return jsonify({"trains": trains})


@app.route("/api/train/test")
def api_train_test():
    """Quick test — bypasses GPS, fetches WAT (Waterloo) directly."""
    rtt_token = os.environ.get("RTT_TOKEN", "")
    if not rtt_token:
        return jsonify({"error": "RTT_TOKEN not set"}), 503
    try:
        tr = requests.get("https://data.rtt.io/api/get_access_token",
            headers={"Authorization": f"Bearer {rtt_token}"}, timeout=10)
        return jsonify({"token_status": tr.status_code, "token_body": tr.text[:300]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Station data (static — no startup delay, no external API) ──
import difflib as _difflib
from uk_stations import UK_STATIONS as _STATION_CACHE
# ───────────────────────────────────────────────────────────────

@app.route("/api/train/nearest")
def api_train_nearest():
    lat = request.args.get("lat", "").strip()
    lng = request.args.get("lng", "").strip()
    if not lat or not lng:
        return jsonify({"error": "lat/lng required"}), 400
    try:
        user_lat, user_lon = float(lat), float(lng)
    except ValueError:
        return jsonify({"error": "invalid lat/lng"}), 400

    scored = sorted(
        [(haversine_km(user_lat, user_lon, s["lat"], s["lon"]), s)
         for s in _STATION_CACHE.values() if s.get("lat") and s.get("lon")],
        key=lambda x: x[0]
    )[:3]
    if not scored:
        return jsonify({"error": "No station found"}), 404
    stations = [
        {"name": s["name"], "crs": s["crs"], "distance_km": round(d, 2)}
        for d, s in scored
    ]
    return jsonify({"stations": stations})


@app.route("/api/train/nearest-by-postcode")
def api_train_nearest_by_postcode():
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if not postcode:
        return jsonify({"error": "postcode required"}), 400
    try:
        lat, lon = _latlon_for_postcode(postcode)
        if lat is None:
            return jsonify({"error": "Postcode not found"}), 404
        scored = []
        for s in _STATION_CACHE.values():
            if s.get("lat") and s.get("lon"):
                d = haversine_km(lat, lon, s["lat"], s["lon"])
                scored.append((d, s))
        scored.sort(key=lambda x: x[0])
        if not scored:
            return jsonify({"error": "No station found"}), 404
        stations = [
            {"name": s["name"], "crs": s["crs"],
             "lat": s["lat"], "lng": s["lon"],
             "distance_km": round(d, 2)}
            for d, s in scored[:2]
        ]
        return jsonify({"stations": stations, **stations[0]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/train/search")
def api_train_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "q required"}), 400

    if _STATION_CACHE:
        q_lower = q.lower()
        # Prefix match first, then contains
        matches = [s for k, s in _STATION_CACHE.items() if k.startswith(q_lower)]
        if not matches:
            matches = [s for k, s in _STATION_CACHE.items() if q_lower in k]

        seen, results = set(), []
        for s in sorted(matches, key=lambda x: x["name"]):
            if s["crs"] not in seen:
                seen.add(s["crs"])
                results.append(s)

        if results:
            return jsonify({"results": results[:8]})

        # No match — fuzzy suggestions ("Did you mean?")
        close = _difflib.get_close_matches(q_lower, list(_STATION_CACHE.keys()), n=3, cutoff=0.55)
        suggestions = [_STATION_CACHE[n] for n in close]
        return jsonify({"results": [], "suggestions": suggestions})

    # Cache not ready yet — fall back to Overpass
    try:
        import re
        safe_q = re.sub(r'[^\w\s\-]', '', q)
        ovr = requests.get(
            "https://overpass-api.de/api/interpreter",
            params={"data": f'[out:json][timeout:15];node["ref:crs"]["name"~"^{safe_q}",i];out 8 qt;'},
            headers={"User-Agent": "MiruApp/1.0 (miru.humanagency.co)"},
            timeout=15,
        )
        seen, results = set(), []
        for el in ovr.json().get("elements", []):
            tags = el.get("tags", {})
            crs  = (tags.get("ref:crs") or "").upper()
            name = tags.get("name", "")
            if crs and name and crs not in seen:
                seen.add(crs)
                results.append({"name": name, "crs": crs})
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tube/search")
def api_tube_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "q required"}), 400
    try:
        r = requests.get(
            f"https://api.tfl.gov.uk/StopPoint/Search/{requests.utils.quote(q)}",
            params={"modes": "tube,dlr,elizabeth-line,overground", "includeHubs": "false", "maxResults": "6"},
            timeout=8,
        )
        r.raise_for_status()
        matches = r.json().get("matches", [])
        results = [
            {"id": m["id"], "name": m["name"].replace(" Underground Station", "")}
            for m in matches
        ]
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 502


_rtt_access: dict = {"token": None, "exp": 0.0}
_rtt_departures_cache: dict = {}   # crs → (payload, ts), 30-second TTL
_RTT_DEPARTURES_TTL = 30

def _get_rtt_token() -> str:
    """Return a cached RTT access token, refreshing only when expired (4-min TTL)."""
    if _rtt_access["token"] and time.time() < _rtt_access["exp"]:
        return _rtt_access["token"]
    rtt_token = os.environ.get("RTT_TOKEN", "")
    if not rtt_token:
        raise RuntimeError("RTT_TOKEN not set")
    tr = requests.get(
        "https://data.rtt.io/api/get_access_token",
        headers={"Authorization": f"Bearer {rtt_token}"},
        timeout=10,
    )
    if not tr.text.strip():
        raise RuntimeError(f"Token exchange empty (HTTP {tr.status_code})")
    tr_data = tr.json()
    if "token" not in tr_data:
        raise RuntimeError(f"Token exchange failed: {tr_data}")
    _rtt_access["token"] = tr_data["token"]
    _rtt_access["exp"]   = time.time() + 240  # 4 minutes
    return _rtt_access["token"]


@app.route("/api/train/departures")
def api_train_departures():
    crs = request.args.get("crs", "").strip().upper()[:3]
    if not crs:
        return jsonify({"error": "crs required"}), 400

    # origin_crs: filter services by their true starting station (e.g. WAT)
    # calling_at: RTT param — plain CRS, no gb-nr: prefix
    origin_crs = request.args.get("origin_crs", "").strip().upper()[:3]
    calling_at = request.args.get("calling_at", "").strip().upper()[:3]

    if not os.environ.get("RTT_TOKEN"):
        return jsonify({"error": "Train API not configured — set RTT_TOKEN environment variable (free at api-portal.rtt.io)"}), 503

    # 30-second departures cache — include filters in key
    cache_key = (crs, calling_at, origin_crs)
    cached = _rtt_departures_cache.get(cache_key)
    if cached and time.time() - cached[1] < _RTT_DEPARTURES_TTL:
        return jsonify(cached[0])

    try:
        access = _get_rtt_token()

        rtt_params = {"code": f"gb-nr:{crs}"}
        if calling_at:
            rtt_params["calling_at"] = calling_at  # plain CRS, not gb-nr: prefixed
        r = requests.get(
            "https://data.rtt.io/rtt/location",
            headers={"Authorization": f"Bearer {access}"},
            params=rtt_params,
            timeout=12,
        )
        if not r.text.strip():
            return jsonify({"error": f"RTT location API returned empty response (HTTP {r.status_code})"}), 500
        data = r.json()
        if request.args.get("debug"):
            svc = (data.get("services") or [{}])[0]
            return jsonify({"raw_service": svc, "location": data.get("location")})
        services = data.get("services") or []

        def fmt_time(dt):
            """Parse HH:MM from ISO datetime, HH:MM, or HHMM."""
            if not dt:
                return ""
            s = str(dt).strip()
            if len(s) >= 16:          # ISO: 2026-05-07T14:05:00
                return s[11:16]
            if len(s) == 5 and s[2] == ":":  # already HH:MM
                return s
            if len(s) == 4 and s.isdigit():  # HHMM
                return s[:2] + ":" + s[2:]
            return s[:5] if len(s) >= 5 else s

        def mins_late(s, r):
            try:
                sh, sm = int(s[:2]), int(s[3:])
                rh, rm = int(r[:2]), int(r[3:])
                return (rh * 60 + rm) - (sh * 60 + sm)
            except Exception:
                return 0

        trains = []
        for s in services:
            # Filter by origin station if requested (e.g. office mode: only WAT-origin trains)
            if origin_crs:
                orig_list = s.get("origin") or []
                orig_crss = [(o.get("location") or {}).get("crs", "").upper() for o in orig_list]
                if origin_crs not in orig_crss:
                    continue

            td  = s.get("temporalData", {})
            dep = td.get("departure", {})
            dest_list = s.get("destination") or [{}]
            dest = (dest_list[0].get("location") or {}).get("description", "") if dest_list else ""
            sched_dt  = dep.get("scheduleAdvertised") or dep.get("scheduled") or ""
            real_dt   = (dep.get("realtimeForecast") or dep.get("forecast") or
                         dep.get("realtimeActual") or dep.get("actual") or "")
            cancelled = dep.get("isCancelled", False) or dep.get("cancelled", False)
            platform_raw = (s.get("locationMetadata") or {}).get("platform")
            if isinstance(platform_raw, dict):
                platform = str(platform_raw.get("display") or platform_raw.get("number") or "")
            else:
                platform = str(platform_raw) if platform_raw else ""
            sched = fmt_time(sched_dt)
            real  = fmt_time(real_dt)
            if cancelled:
                status = "Cancelled"
            elif real and real != sched:
                late = mins_late(sched, real)
                status = f"Exp {real}" + (f" (+{late} min)" if late > 0 else "")
            else:
                status = "On time"
            if not dest or not sched:
                continue
            trains.append({
                "scheduled":   sched,
                "expected":    status,
                "platform":    platform,
                "destination": dest,
                "cancelled":   bool(cancelled),
            })
        station_name = (data.get("location") or {}).get("description", crs)
        payload = {"station": station_name, "trains": trains}
        _rtt_departures_cache[cache_key] = (payload, time.time())
        return jsonify(payload)
    except Exception as e:
        print(f"[train/departures] error: {e}")
        _rtt_access["token"] = None  # force token refresh on next request
        return jsonify({"error": "Could not load departures — try again shortly"}), 500


@app.route("/static/miru-preview.png")
def miru_preview_image():
    """Return an SVG open-graph preview image for Miru."""
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" style="stop-color:#1e1b4b"/>
      <stop offset="100%" style="stop-color:#4c1d95"/>
    </linearGradient>
  </defs>
  <rect width="1200" height="630" fill="url(#bg)"/>
  <text x="600" y="220" font-family="system-ui,sans-serif" font-size="120" font-weight="900"
        fill="white" text-anchor="middle" letter-spacing="-4">Miru</text>
  <text x="600" y="310" font-family="system-ui,sans-serif" font-size="36" font-weight="500"
        fill="#c4b5fd" text-anchor="middle">Your everyday UK companion</text>
  <text x="600" y="390" font-family="system-ui,sans-serif" font-size="26" fill="#a78bfa" text-anchor="middle">
    ⛽ Fuel · 🏠 Area · 🔍 Local Services · 📌 Saves · 📚 Books
  </text>
  <text x="600" y="460" font-family="system-ui,sans-serif" font-size="22" fill="#7c3aed" text-anchor="middle">
    Also on WhatsApp · miru.humanagency.co
  </text>
</svg>"""
    from flask import Response
    return Response(svg, mimetype="image/svg+xml",
                    headers={"Cache-Control": "public, max-age=86400"})


# ── News area feeds lookup ────────────────────────────────────────────────────
_NEWS_AREA_FEEDS = {
    # England — counties
    "surrey":             [{"name":"BBC Surrey",          "url":"https://feeds.bbci.co.uk/news/england/surrey/rss.xml"},
                           {"name":"Get Surrey",          "url":"https://www.getsurrey.co.uk/news/?service=rss"}],
    "kent":               [{"name":"BBC Kent",            "url":"https://feeds.bbci.co.uk/news/england/kent/rss.xml"},
                           {"name":"Kent Online",         "url":"https://www.kentonline.co.uk/feed/"}],
    "east sussex":        [{"name":"BBC Sussex",          "url":"https://feeds.bbci.co.uk/news/england/sussex/rss.xml"},
                           {"name":"Sussex Live",         "url":"https://www.sussexlive.co.uk/news/?service=rss"}],
    "west sussex":        [{"name":"BBC Sussex",          "url":"https://feeds.bbci.co.uk/news/england/sussex/rss.xml"},
                           {"name":"Sussex Live",         "url":"https://www.sussexlive.co.uk/news/?service=rss"}],
    "hampshire":          [{"name":"BBC Hampshire",       "url":"https://feeds.bbci.co.uk/news/england/hampshire/rss.xml"},
                           {"name":"Hampshire Live",      "url":"https://www.hampshirelive.news/news/?service=rss"}],
    "berkshire":          [{"name":"BBC Berkshire",       "url":"https://feeds.bbci.co.uk/news/england/berkshire/rss.xml"},
                           {"name":"Berkshire Live",      "url":"https://www.berkshirelive.co.uk/news/?service=rss"}],
    "oxfordshire":        [{"name":"BBC Oxford",          "url":"https://feeds.bbci.co.uk/news/england/oxford/rss.xml"},
                           {"name":"Oxford Mail",         "url":"https://www.oxfordmail.co.uk/news/rss/"}],
    "essex":              [{"name":"BBC Essex",           "url":"https://feeds.bbci.co.uk/news/england/essex/rss.xml"},
                           {"name":"Essex Live",          "url":"https://www.essexlive.news/news/?service=rss"}],
    "hertfordshire":      [{"name":"BBC Three Counties",  "url":"https://feeds.bbci.co.uk/news/england/beds_bucks_herts/rss.xml"},
                           {"name":"Herts Live",          "url":"https://www.hertfordshiremercury.co.uk/news/rss/"}],
    "bedfordshire":       [{"name":"BBC Three Counties",  "url":"https://feeds.bbci.co.uk/news/england/beds_bucks_herts/rss.xml"}],
    "buckinghamshire":    [{"name":"BBC Three Counties",  "url":"https://feeds.bbci.co.uk/news/england/beds_bucks_herts/rss.xml"},
                           {"name":"Bucks Free Press",    "url":"https://www.bucksfreepress.co.uk/news/rss/"}],
    "cambridgeshire":     [{"name":"BBC Cambridgeshire",  "url":"https://feeds.bbci.co.uk/news/england/cambridgeshire/rss.xml"},
                           {"name":"Cambridge News",      "url":"https://www.cambridge-news.co.uk/news/?service=rss"}],
    "suffolk":            [{"name":"BBC Suffolk",         "url":"https://feeds.bbci.co.uk/news/england/suffolk/rss.xml"},
                           {"name":"East Anglian Daily",  "url":"https://www.eadt.co.uk/news/rss/"}],
    "norfolk":            [{"name":"BBC Norfolk",         "url":"https://feeds.bbci.co.uk/news/england/norfolk/rss.xml"},
                           {"name":"Norwich Evening News","url":"https://www.eveningnews24.co.uk/news/rss/"}],
    "gloucestershire":    [{"name":"BBC Gloucestershire", "url":"https://feeds.bbci.co.uk/news/england/gloucestershire/rss.xml"},
                           {"name":"Gloucestershire Live","url":"https://www.gloucestershirelive.co.uk/news/?service=rss"}],
    "wiltshire":          [{"name":"BBC Wiltshire",       "url":"https://feeds.bbci.co.uk/news/england/wiltshire/rss.xml"}],
    "somerset":           [{"name":"BBC Somerset",        "url":"https://feeds.bbci.co.uk/news/england/somerset/rss.xml"},
                           {"name":"Somerset Live",       "url":"https://www.somersetlive.co.uk/news/?service=rss"}],
    "devon":              [{"name":"BBC Devon",           "url":"https://feeds.bbci.co.uk/news/england/devon/rss.xml"},
                           {"name":"Devon Live",          "url":"https://www.devonlive.com/news/?service=rss"}],
    "cornwall":           [{"name":"BBC Cornwall",        "url":"https://feeds.bbci.co.uk/news/england/cornwall/rss.xml"},
                           {"name":"Cornwall Live",       "url":"https://www.cornwalllive.com/news/?service=rss"}],
    "dorset":             [{"name":"BBC Dorset",          "url":"https://feeds.bbci.co.uk/news/england/dorset/rss.xml"},
                           {"name":"Dorset Live",         "url":"https://www.dorsetlive.com/news/?service=rss"}],
    "leicestershire":     [{"name":"BBC Leicester",       "url":"https://feeds.bbci.co.uk/news/england/leicester/rss.xml"},
                           {"name":"Leicestershire Live", "url":"https://www.leicestermercury.co.uk/news/?service=rss"}],
    "nottinghamshire":    [{"name":"BBC Nottingham",      "url":"https://feeds.bbci.co.uk/news/england/nottingham/rss.xml"},
                           {"name":"Nottingham Post",     "url":"https://www.nottinghampost.com/news/?service=rss"}],
    "derbyshire":         [{"name":"BBC Derby",           "url":"https://feeds.bbci.co.uk/news/england/derbyshire/rss.xml"},
                           {"name":"Derbyshire Live",     "url":"https://www.derbytelegraph.co.uk/news/?service=rss"}],
    "lincolnshire":       [{"name":"BBC Lincolnshire",    "url":"https://feeds.bbci.co.uk/news/england/lincolnshire/rss.xml"},
                           {"name":"Lincolnshire Live",   "url":"https://www.lincolnshirelive.co.uk/news/?service=rss"}],
    "northamptonshire":   [{"name":"BBC Northampton",     "url":"https://feeds.bbci.co.uk/news/england/northampton/rss.xml"},
                           {"name":"Northants Live",      "url":"https://www.northamptonchron.co.uk/news/rss/"}],
    "staffordshire":      [{"name":"BBC Stoke",           "url":"https://feeds.bbci.co.uk/news/england/stoke_staffordshire/rss.xml"},
                           {"name":"Stoke-on-Trent Live", "url":"https://www.stokesentinel.co.uk/news/?service=rss"}],
    "shropshire":         [{"name":"BBC Shropshire",      "url":"https://feeds.bbci.co.uk/news/england/shropshire/rss.xml"},
                           {"name":"Shropshire Star",     "url":"https://www.shropshirestar.com/feed/"}],
    "worcestershire":     [{"name":"BBC Hereford & Worcs","url":"https://feeds.bbci.co.uk/news/england/hereford_worcester/rss.xml"}],
    "herefordshire":      [{"name":"BBC Hereford & Worcs","url":"https://feeds.bbci.co.uk/news/england/hereford_worcester/rss.xml"}],
    "west midlands":      [{"name":"BBC Birmingham",      "url":"https://feeds.bbci.co.uk/news/england/birmingham/rss.xml"},
                           {"name":"Birmingham Live",     "url":"https://www.birminghammail.co.uk/news/?service=rss"}],
    "warwickshire":       [{"name":"BBC Coventry",        "url":"https://feeds.bbci.co.uk/news/england/coventry/rss.xml"},
                           {"name":"Coventry Live",       "url":"https://www.coventrytelegraph.net/news/?service=rss"}],
    "greater manchester": [{"name":"BBC Manchester",      "url":"https://feeds.bbci.co.uk/news/england/manchester/rss.xml"},
                           {"name":"Manchester Evening News","url":"https://www.manchestereveningnews.co.uk/news/?service=rss"}],
    "merseyside":         [{"name":"BBC Merseyside",      "url":"https://feeds.bbci.co.uk/news/england/merseyside/rss.xml"},
                           {"name":"Liverpool Echo",      "url":"https://www.liverpoolecho.co.uk/news/?service=rss"}],
    "lancashire":         [{"name":"BBC Lancashire",      "url":"https://feeds.bbci.co.uk/news/england/lancashire/rss.xml"},
                           {"name":"Lancashire Telegraph","url":"https://www.lancashiretelegraph.co.uk/news/rss/"}],
    "cumbria":            [{"name":"BBC Cumbria",         "url":"https://feeds.bbci.co.uk/news/england/cumbria/rss.xml"}],
    "north yorkshire":    [{"name":"BBC North Yorkshire", "url":"https://feeds.bbci.co.uk/news/england/north_yorkshire/rss.xml"}],
    "west yorkshire":     [{"name":"BBC Leeds",           "url":"https://feeds.bbci.co.uk/news/england/leeds/rss.xml"},
                           {"name":"Yorkshire Evening Post","url":"https://www.yorkshireeveningpost.co.uk/news/rss/"}],
    "south yorkshire":    [{"name":"BBC Sheffield",       "url":"https://feeds.bbci.co.uk/news/england/south_yorkshire/rss.xml"},
                           {"name":"Sheffield Star",      "url":"https://www.thestar.co.uk/news/rss/"}],
    "east yorkshire":     [{"name":"BBC Humberside",      "url":"https://feeds.bbci.co.uk/news/england/humber/rss.xml"},
                           {"name":"Hull Live",           "url":"https://www.hulldailymail.co.uk/news/?service=rss"}],
    "tyne and wear":      [{"name":"BBC Newcastle",       "url":"https://feeds.bbci.co.uk/news/england/tyne/rss.xml"},
                           {"name":"Chronicle Live",      "url":"https://www.chroniclelive.co.uk/news/?service=rss"}],
    "county durham":      [{"name":"BBC Tees",            "url":"https://feeds.bbci.co.uk/news/england/tees/rss.xml"},
                           {"name":"Teesside Live",       "url":"https://www.gazettelive.co.uk/news/?service=rss"}],
    "northumberland":     [{"name":"BBC Newcastle",       "url":"https://feeds.bbci.co.uk/news/england/tyne/rss.xml"},
                           {"name":"Chronicle Live",      "url":"https://www.chroniclelive.co.uk/news/?service=rss"}],
    # Scotland
    "scotland":           [{"name":"BBC Scotland",        "url":"https://feeds.bbci.co.uk/news/scotland/rss.xml"},
                           {"name":"The Scotsman",        "url":"https://www.scotsman.com/news/rss.xml"},
                           {"name":"Herald Scotland",     "url":"https://www.heraldscotland.com/news/rss/"}],
    "city of edinburgh":  [{"name":"BBC Scotland",        "url":"https://feeds.bbci.co.uk/news/scotland/rss.xml"},
                           {"name":"Edinburgh Live",      "url":"https://www.edinburghlive.co.uk/news/?service=rss"}],
    "glasgow city":       [{"name":"BBC Scotland",        "url":"https://feeds.bbci.co.uk/news/scotland/rss.xml"},
                           {"name":"Glasgow Live",        "url":"https://www.glasgowlive.co.uk/news/?service=rss"}],
    # Wales
    "wales":              [{"name":"BBC Wales",           "url":"https://feeds.bbci.co.uk/news/wales/rss.xml"},
                           {"name":"Wales Online",        "url":"https://www.walesonline.co.uk/news/?service=rss"}],
    # Northern Ireland
    "northern ireland":   [{"name":"BBC Northern Ireland","url":"https://feeds.bbci.co.uk/news/northern_ireland/rss.xml"},
                           {"name":"Belfast Telegraph",   "url":"https://www.belfasttelegraph.co.uk/news/rss/"}],
    # London boroughs → London feed
    "greater london":     [{"name":"BBC London",          "url":"https://feeds.bbci.co.uk/news/england/london/rss.xml"},
                           {"name":"My London",           "url":"https://www.mylondon.news/news/?service=rss"}],
}
# Alias district names to county slugs for common areas
_NEWS_DISTRICT_MAP = {
    "mid sussex": "west sussex", "chichester": "west sussex",
    "worthing": "west sussex", "horsham": "west sussex",
    "brighton and hove": "east sussex", "eastbourne": "east sussex",
    "elmbridge": "surrey", "runnymede": "surrey", "waverley": "surrey",
    "guildford": "surrey", "mole valley": "surrey", "tandridge": "surrey",
    "maidstone": "kent", "sevenoaks": "kent", "tonbridge and malling": "kent",
    "tunbridge wells": "kent", "folkestone and hythe": "kent",
    "reading": "berkshire", "slough": "berkshire", "windsor and maidenhead": "berkshire",
    "city of bristol": "gloucestershire", "south gloucestershire": "gloucestershire",
    "city of edinburgh": "city of edinburgh",
    "glasgow city": "glasgow city",
}

def _get_area_feeds(county_key: str, district_key: str = "") -> list:
    """Return deduplicated list of {name, url} feeds for an area."""
    key = (county_key or "").lower().strip()
    dist = (district_key or "").lower().strip()
    # Try district alias first
    if dist in _NEWS_DISTRICT_MAP:
        key = _NEWS_DISTRICT_MAP[dist]
    elif key in _NEWS_DISTRICT_MAP:
        key = _NEWS_DISTRICT_MAP[key]
    feeds = _NEWS_AREA_FEEDS.get(key, [])
    # Deduplicate by URL
    seen, result = set(), []
    for f in feeds:
        if f["url"] not in seen:
            seen.add(f["url"]); result.append(f)
    return result


@app.route("/api/news/discover")
def api_news_discover():
    """Return suggested local news feeds for a postcode or area name."""
    import re as _re
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "q required"}), 400

    postcode_re = _re.compile(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}$', _re.I)
    area_label  = q
    county_key  = ""
    district_key = ""

    if postcode_re.match(q.replace(" ", "")):
        pc = q.replace(" ", "").upper()
        try:
            r = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=6)
            if r.status_code == 200:
                res = r.json().get("result", {})
                country  = res.get("country", "England")
                county   = (res.get("admin_county") or "").strip()
                district = (res.get("admin_district") or "").strip()
                area_label = county or district or q
                if country == "Scotland":
                    county_key = "scotland"
                    district_key = district.lower()
                elif country == "Wales":
                    county_key = "wales"
                elif country == "Northern Ireland":
                    county_key = "northern ireland"
                else:
                    county_key   = county.lower()
                    district_key = district.lower()
                    if not county_key:
                        county_key = district.lower()
        except Exception:
            pass
    else:
        # Free-text area name — normalise and match
        county_key = q.lower().strip()
        area_label = q.title()

    feeds = _get_area_feeds(county_key, district_key)
    if not feeds:
        return jsonify({"area": area_label, "feeds": [], "not_found": True})
    return jsonify({"area": area_label, "feeds": feeds})


@app.route("/api/news/fetch", methods=["POST"])
def api_news_fetch():
    import xml.etree.ElementTree as _ET
    import re as _re
    from urllib.parse import urljoin as _urljoin

    urls = (request.json or {}).get("urls", [])
    if not urls:
        return jsonify({"error": "No URLs provided"}), 400
    urls = urls[:10]

    def _strip_html(s):
        return _re.sub(r'<[^>]+>', '', s or "").strip()

    def _parse_one(url):
        hdrs = {"User-Agent": "Mozilla/5.0 (compatible; MiruBot/1.0)"}
        r = requests.get(url, timeout=10, headers=hdrs)
        r.raise_for_status()
        raw = r.content
        ct = r.headers.get("Content-Type", "")

        # If HTML page, discover RSS link
        text_peek = raw[:800].decode("utf-8", errors="replace")
        if "html" in ct.lower() and not any(tag in text_peek for tag in ("<rss", "<feed", "<?xml")):
            m = _re.search(
                r'<link[^>]+type=["\']application/(?:rss|atom)\+xml["\'][^>]+href=["\']([^"\']+)["\']'
                r'|<link[^>]+href=["\']([^"\']+)["\'][^>]+type=["\']application/(?:rss|atom)\+xml["\']',
                raw.decode("utf-8", errors="replace"), _re.I
            )
            if m:
                rss_url = m.group(1) or m.group(2)
                rss_url = _urljoin(r.url, rss_url)
                r2 = requests.get(rss_url, timeout=10, headers=hdrs)
                r2.raise_for_status()
                raw = r2.content
            else:
                return {"url": url, "source": url, "articles": [], "error": "No RSS feed found on this page"}

        root = _ET.fromstring(raw)
        articles = []
        source = url

        # RSS 2.0
        channel = root.find("channel")
        if channel is not None:
            source = (channel.findtext("title") or url).strip()
            for item in channel.findall("item")[:12]:
                title = _strip_html(item.findtext("title") or "")
                desc  = _strip_html(item.findtext("description") or "")[:180]
                link  = (item.findtext("link") or "").strip()
                pub   = (item.findtext("pubDate") or "").strip()
                if title:
                    articles.append({"title": title, "description": desc, "link": link, "published": pub})
        else:
            # Atom
            NS = "http://www.w3.org/2005/Atom"
            def _at(tag): return "{%s}%s" % (NS, tag)
            t = root.find(_at("title"))
            source = (t.text if t is not None else url or "").strip()
            for entry in root.findall(_at("entry"))[:12]:
                te = entry.find(_at("title"))
                title = _strip_html(te.text if te is not None else "")
                se = entry.find(_at("summary")) or entry.find(_at("content"))
                desc  = _strip_html(se.text if se is not None else "")[:180]
                le = entry.find(_at("link"))
                link  = (le.get("href", "") if le is not None else "")
                pe = entry.find(_at("published")) or entry.find(_at("updated"))
                pub = (pe.text if pe is not None else "").strip()
                if title:
                    articles.append({"title": title, "description": desc, "link": link, "published": pub})

        return {"url": url, "source": source, "articles": articles}

    feeds = []
    for u in urls:
        try:
            feeds.append(_parse_one(u))
        except Exception as exc:
            feeds.append({"url": u, "source": u, "articles": [], "error": str(exc)})

    return jsonify({"feeds": feeds})


@app.route("/ping")
def ping():
    return "OK", 200


# ── Space product ─────────────────────────────────────────────────────────────

_SPACE_CACHE: dict = {}  # key → (data, timestamp)

@app.route("/space")
def space_home():
    return render_template("space.html")

@app.route("/api/space/iss")
def api_space_iss():
    try:
        r = requests.get("https://api.wheretheiss.at/v1/satellites/25544", timeout=6)
        d = r.json()
        return jsonify({
            "lat":        round(d.get("latitude",  0), 4),
            "lon":        round(d.get("longitude", 0), 4),
            "altitude":   round(d.get("altitude",  0)),
            "velocity":   round(d.get("velocity",  0)),
            "visibility": d.get("visibility", ""),
            "timestamp":  d.get("timestamp"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/space/launches")
def api_space_launches():
    ck = "launches"
    if ck in _SPACE_CACHE and time.time() - _SPACE_CACHE[ck][1] < 1800:
        return jsonify(_SPACE_CACHE[ck][0])
    try:
        r = requests.get(
            "https://ll.thespacedevs.com/2.2.0/launch/upcoming/",
            params={"format": "json", "limit": 20, "mode": "list"},
            timeout=12,
            headers={"User-Agent": "space.humanagency.co/1.0"},
        )
        launches = []
        for l in r.json().get("results", []):
            location = l.get("location", "") or ""
            provider = l.get("lsp_name", "") or ""
            loc_lc   = location.lower()
            if "united kingdom" in loc_lc or ", uk" in loc_lc or "scotland" in loc_lc or "shetland" in loc_lc:
                country = "GBR"
            elif "french guiana" in loc_lc or "kourou" in loc_lc:
                country = "FRA"
            elif "russia" in loc_lc or "baikonur" in loc_lc or "plesetsk" in loc_lc or "vostochny" in loc_lc:
                country = "RUS"
            elif "china" in loc_lc or "jiuquan" in loc_lc or "xichang" in loc_lc or "taiyuan" in loc_lc or "wenchang" in loc_lc:
                country = "CHN"
            elif "japan" in loc_lc or "tanegashima" in loc_lc or "uchinoura" in loc_lc:
                country = "JPN"
            elif "india" in loc_lc or "sriharikota" in loc_lc:
                country = "IND"
            elif "new zealand" in loc_lc or "mahia" in loc_lc:
                country = "NZL"
            elif "usa" in loc_lc or "cape canaveral" in loc_lc or "kennedy" in loc_lc or "vandenberg" in loc_lc or "wallops" in loc_lc or "kwajalein" in loc_lc:
                country = "USA"
            else:
                country = ""
            launches.append({
                "name":        l.get("name", ""),
                "net":         l.get("net", ""),
                "location":    location,
                "country":     country,
                "provider":    provider,
                "status":      (l.get("status") or {}).get("name", ""),
                "description": (l.get("mission_type") or ""),
                "image":       l.get("image") or "",
            })
        out = {"launches": launches}
        _SPACE_CACHE[ck] = (out, time.time())
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/space/apod")
def api_space_apod():
    ck = "apod"
    if ck in _SPACE_CACHE and time.time() - _SPACE_CACHE[ck][1] < 3600:
        return jsonify(_SPACE_CACHE[ck][0])
    try:
        r = requests.get(
            "https://api.nasa.gov/planetary/apod",
            params={"api_key": os.environ.get("NASA_API_KEY", "DEMO_KEY")},
            timeout=8,
        )
        d = r.json()
        out = {
            "title":       d.get("title", ""),
            "explanation": d.get("explanation", ""),
            "url":         d.get("url", ""),
            "hdurl":       d.get("hdurl") or d.get("url", ""),
            "media_type":  d.get("media_type", "image"),
            "date":        d.get("date", ""),
            "copyright":   d.get("copyright", ""),
        }
        _SPACE_CACHE[ck] = (out, time.time())
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/space/news")
def api_space_news():
    ck = "news"
    if ck in _SPACE_CACHE and time.time() - _SPACE_CACHE[ck][1] < 1800:
        return jsonify(_SPACE_CACHE[ck][0])
    import xml.etree.ElementTree as ET
    feeds = [
        ("https://ukspaceagency.blog.gov.uk/feed/",                              "UK Space Agency"),
        ("https://www.esa.int/rssfeed/Our_Activities/Space_Engineering_Technology","ESA"),
        ("https://spacenews.com/feed/",                                           "SpaceNews"),
        ("https://spaceflightnow.com/feed/",                                      "Spaceflight Now"),
        ("https://www.nasaspaceflight.com/feed/",                                 "NASASpaceFlight"),
        ("https://www.nasa.gov/rss/dyn/breaking_news.rss",                        "NASA"),
    ]
    items = []
    for feed_url, source in feeds:
        try:
            r = requests.get(feed_url, timeout=7, headers={"User-Agent": "Mozilla/5.0"})
            root = ET.fromstring(r.content)
            channel = root.find("channel")
            if channel is None:
                continue
            for item in list(channel.findall("item"))[:2]:
                title = (item.findtext("title") or "").strip()
                link  = (item.findtext("link")  or "").strip()
                desc  = re.sub(r'<[^>]+>', '', item.findtext("description") or "")[:260].strip()
                pub   = (item.findtext("pubDate") or "").strip()
                if title and link:
                    items.append({"title": title, "summary": desc,
                                  "link": link, "published": pub, "source": source})
        except Exception as ex:
            print(f"[space news {source}] {ex}")
    out = {"items": items[:12]}
    _SPACE_CACHE[ck] = (out, time.time())
    return jsonify(out)


@app.route("/api/space/planets")
def api_space_planets():
    """Approximate Earth–planet distances using simplified Keplerian orbits (J2000 epoch)."""
    import math
    _PLANETS = {
        'Mercury': {'a': 0.387, 'T': 87.97,    'L0': 252.25, 'color': '#9e9e9e', 'diameter_km': 4_879,   'moons': 0,  'fact': 'Scorching days (430°C) and freezing nights (−180°C) — no atmosphere to regulate'},
        'Venus':   {'a': 0.723, 'T': 224.70,   'L0': 181.98, 'color': '#e8cda0', 'diameter_km': 12_104,  'moons': 0,  'fact': 'Hottest planet at 462°C — thick CO₂ atmosphere creates a runaway greenhouse effect'},
        'Mars':    {'a': 1.524, 'T': 686.97,   'L0': 355.45, 'color': '#c1440e', 'diameter_km': 6_779,   'moons': 2,  'fact': 'Home to Olympus Mons — the tallest volcano in the solar system at 22km high'},
        'Jupiter': {'a': 5.203, 'T': 4332.59,  'L0': 34.40,  'color': '#c88b3a', 'diameter_km': 139_820, 'moons': 95, 'fact': 'Its Great Red Spot is a storm bigger than Earth that has raged for 350+ years'},
        'Saturn':  {'a': 9.537, 'T': 10759.22, 'L0': 49.94,  'color': '#e4d191', 'diameter_km': 116_460, 'moons': 146,'fact': 'Its rings are mostly ice — spanning 282,000km but only 20m thick in places'},
        'Uranus':  {'a': 19.19, 'T': 30685.0,  'L0': 313.23, 'color': '#7de8e8', 'diameter_km': 50_724,  'moons': 28, 'fact': 'Rotates on its side with an axial tilt of 98° — thought to be from an ancient collision'},
        'Neptune': {'a': 30.07, 'T': 60190.0,  'L0': 304.88, 'color': '#4b70dd', 'diameter_km': 49_244,  'moons': 16, 'fact': 'Has the strongest winds in the solar system — up to 2,100 km/h'},
    }
    _EARTH = {'a': 1.0, 'T': 365.25, 'L0': 100.464}
    _J2000 = datetime(2000, 1, 1, 12)
    days = (datetime.utcnow() - _J2000).total_seconds() / 86400.0
    ea = math.radians((_EARTH['L0'] + 360.0 * days / _EARTH['T']) % 360)
    ex, ey = math.cos(ea), math.sin(ea)
    result = []
    for name, p in _PLANETS.items():
        pa = math.radians((p['L0'] + 360.0 * days / p['T']) % 360)
        px = p['a'] * math.cos(pa)
        py = p['a'] * math.sin(pa)
        dist_au = math.sqrt((px - ex)**2 + (py - ey)**2)
        dist_km = dist_au * 149_597_870.7
        result.append({
            'name':        name,
            'dist_au':     round(dist_au, 3),
            'dist_M_km':   round(dist_km / 1e6, 1),
            'light_min':   round(dist_au * 8.317, 1),
            'color':       p['color'],
            'diameter_km': p['diameter_km'],
            'moons':       p['moons'],
            'fact':        p['fact'],
            'helio_au':    p['a'],
        })
    return jsonify({'planets': result, 'ts': datetime.utcnow().isoformat()})


@app.route("/api/space/artemis")
def api_space_artemis():
    ck = "artemis"
    if ck in _SPACE_CACHE and time.time() - _SPACE_CACHE[ck][1] < 3600:
        return jsonify(_SPACE_CACHE[ck][0])
    try:
        r = requests.get(
            "https://images-api.nasa.gov/search",
            params={"q": "artemis orion crew 2024 2025", "media_type": "image",
                    "page_size": 9, "year_start": "2022"},
            timeout=8,
        )
        photos = []
        for item in r.json().get("collection", {}).get("items", [])[:6]:
            data  = (item.get("data") or [{}])[0]
            links = item.get("links") or []
            thumb = next((lk.get("href", "") for lk in links
                          if lk.get("render") == "image" or lk.get("rel") == "preview"), "")
            photos.append({
                "title": (data.get("title") or "")[:90],
                "desc":  re.sub(r'<[^>]+>', '', data.get("description") or "")[:200].strip(),
                "date":  (data.get("date_created") or "")[:10],
                "thumb": thumb,
            })
        out = {"photos": photos}
        _SPACE_CACHE[ck] = (out, time.time())
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e), "photos": []})


@app.route("/api/space/sky")
def api_space_sky():
    postcode = request.args.get("postcode", "").strip().upper().replace(" ", "")
    if not postcode:
        return jsonify({"error": "postcode required"}), 400
    try:
        r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=5)
        if r.status_code != 200:
            r2 = requests.get(f"https://api.postcodes.io/outcodes/{postcode}", timeout=5)
            if r2.status_code != 200:
                return jsonify({"error": "postcode not found"}), 404
            res = r2.json().get("result", {})
        else:
            res = r.json().get("result", {})
        lat, lon = res.get("latitude"), res.get("longitude")
        if not lat:
            return jsonify({"error": "no coords"}), 404
    except Exception:
        return jsonify({"error": "postcode lookup failed"}), 500
    try:
        r2 = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "current": "cloud_cover,weather_code,is_day,temperature_2m,wind_speed_10m",
                "daily":   "sunrise,sunset",
                "timezone": "Europe/London",
                "forecast_days": 1,
            },
            timeout=8,
        )
        d = r2.json()
        cur   = d.get("current", {})
        daily = d.get("daily", {})
        cloud   = cur.get("cloud_cover", 0)
        code    = cur.get("weather_code", 0)
        is_day  = cur.get("is_day", 1)
        temp    = cur.get("temperature_2m")
        wind    = cur.get("wind_speed_10m")
        sunrise = (daily.get("sunrise") or [""])[0]
        sunset  = (daily.get("sunset")  or [""])[0]
        _WMO = {
            0:"Clear sky", 1:"Mainly clear", 2:"Partly cloudy", 3:"Overcast",
            45:"Fog", 48:"Icy fog",
            51:"Light drizzle", 53:"Drizzle", 55:"Heavy drizzle",
            61:"Light rain", 63:"Rain", 65:"Heavy rain",
            71:"Light snow", 73:"Snow", 75:"Heavy snow",
            80:"Rain showers", 81:"Heavy showers", 82:"Violent showers",
            95:"Thunderstorm", 96:"Thunderstorm with hail",
        }
        _WMO_ICON = {
            0:"☀️" if is_day else "🌙", 1:"🌤️", 2:"⛅", 3:"☁️",
            45:"🌫️", 48:"🌫️",
            51:"🌦️", 53:"🌧️", 55:"🌧️",
            61:"🌧️", 63:"🌧️", 65:"🌧️",
            71:"🌨️", 73:"❄️", 75:"❄️",
            80:"🌦️", 81:"🌧️", 82:"⛈️",
            95:"⛈️", 96:"⛈️",
        }
        if cloud < 20:
            quality = "excellent"; qlabel = "Excellent for stargazing"; qicon = "⭐⭐⭐"
        elif cloud < 40:
            quality = "good";      qlabel = "Good — patches of cloud";  qicon = "⭐⭐"
        elif cloud < 70:
            quality = "fair";      qlabel = "Fair — mostly cloudy";     qicon = "⭐"
        else:
            quality = "poor";      qlabel = "Poor — heavy cloud cover"; qicon = "☁️"
        return jsonify({
            "lat": lat, "lon": lon,
            "cloud_pct": cloud,
            "weather_code": code,
            "weather_desc": _WMO.get(code, "Varied"),
            "weather_icon": _WMO_ICON.get(code, "🌡️"),
            "is_day": is_day,
            "temp_c": temp,
            "wind_kmh": wind,
            "sunrise": sunrise[11:16] if len(sunrise) > 10 else sunrise,
            "sunset":  sunset[11:16]  if len(sunset)  > 10 else sunset,
            "quality": quality,
            "quality_label": qlabel,
            "quality_icon": qicon,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Space Newsletter ──────────────────────────────────────────────────────────

_NL_COMPANIES = [
    {"name":"Surrey Satellite Technology (SSTL)", "loc":"Guildford, Surrey",  "url":"https://www.sstl.co.uk",         "desc":"World leader in small satellite manufacturing — 70+ satellites built since 1985. Part of the Airbus group. Employs 700+ people in Surrey and has shaped the global small-sat industry."},
    {"name":"Orbex",                              "loc":"Forres, Scotland",    "url":"https://orbex.space",            "desc":"Building Prime — a small orbital rocket designed to launch from Scottish soil. One of the most exciting UK startups, aiming to put the UK on the orbital launch map for the first time."},
    {"name":"Astroscale UK",                      "loc":"Harwell, Oxfordshire","url":"https://astroscale.com",         "desc":"Pioneering commercial debris removal and satellite life-extension. Ran the world's first commercial debris capture mission. A critical company for the long-term sustainability of space."},
    {"name":"OneWeb / Eutelsat",                  "loc":"London",              "url":"https://oneweb.net",             "desc":"Global LEO broadband constellation with 650+ satellites. Part UK-government owned. Aims to connect the unconnected — schools, ships, remote communities — with high-speed internet from orbit."},
    {"name":"Spire Global",                       "loc":"Glasgow",             "url":"https://spire.com",             "desc":"110+ nanosatellites providing maritime tracking, weather data, and RF spectrum analysis globally. A shining example of Scotland's growing small-sat cluster."},
    {"name":"Reaction Engines",                   "loc":"Culham, Oxfordshire", "url":"https://www.reactionengines.co.uk","desc":"Developing SABRE — a hybrid air-breathing rocket engine that could halve the cost of getting to orbit. A genuinely revolutionary British engineering project backed by BAE Systems and ESA."},
    {"name":"Open Cosmos",                        "loc":"Harwell, Oxfordshire","url":"https://opencosmos.com",        "desc":"End-to-end small satellite missions — hardware, software and operations — for commercial and government customers. Making space accessible to organisations that couldn't previously afford it."},
    {"name":"SEN",                                "loc":"London",              "url":"https://sen.com",               "desc":"British space media company streaming live 4K Earth footage from cameras on the ISS. The live stream you can watch on space.humanagency.co right now is powered by SEN."},
    {"name":"Goonhilly Earth Station",            "loc":"Cornwall",            "url":"https://goonhilly.org",         "desc":"Deep space communications facility in Cornwall tracking Moon and Mars missions. A piece of Cold War infrastructure reinvented as a commercial ground station for the new space era."},
    {"name":"Satellite Applications Catapult",    "loc":"Harwell, Oxfordshire","url":"https://sa.catapult.org.uk",   "desc":"Government-backed innovation centre connecting satellite data to UK businesses and public services — from flood mapping to precision agriculture. The bridge between space and the economy."},
    {"name":"Skyrora",                            "loc":"Edinburgh",           "url":"https://www.skyrora.com",       "desc":"Developing Skyrora XL for UK orbital launch. Also produces test vehicles for propulsion R&D. Part of Scotland's growing launch ecosystem alongside Orbex."},
    {"name":"BAE Systems Space",                  "loc":"Various UK",          "url":"https://www.baesystems.com",    "desc":"Space situational awareness, electronic intelligence and responsive space programmes for UK and allied governments. One of the largest employers in UK space."},
]

_NL_CAREERS = [
    {"icon":"🎓", "step":"Study the right subjects",    "body":"Physics, maths, engineering, and computer science are core — but business, law, policy and geography all have strong space applications. The industry needs far more people than it has rocket scientists."},
    {"icon":"🔭", "step":"Get involved early",           "body":"UK Space Design Competition, British Astronomy & Astrophysics Olympiad (BAAO), or a local astronomy club. Doing counts more than knowing — and it makes your CV stand out."},
    {"icon":"🏢", "step":"Target your first employer",   "body":"SSTL (Guildford), Orbex (Scotland), Airbus (Stevenage), Open Cosmos and the Satellite Applications Catapult all run graduate programmes. Apply early — cohorts are small."},
    {"icon":"🌐", "step":"Use the right job portals",    "body":"jobs.ukspace.org (UKSA), Space Talent, LinkedIn filtered to 'space + UK'. ESA runs UK-facing graduate traineeship programmes too. Don't just search 'aerospace' — 'satellite' and 'earth observation' find different roles."},
    {"icon":"📡", "step":"Build with what you have",    "body":"Universities hold CubeSat records. Raspberry Pi and software-defined radio skills translate directly to space work. Open-source ground station tools are genuinely visible to hiring managers in this industry."},
    {"icon":"🤝", "step":"Network — the industry is tiny","body":"UK Space Conference (annual), NewSpace People events, UKSEDS (students & grads). The UK space sector employs 48,800 people. Compared to finance or tech, it's a village. People remember faces and names."},
]

_NL_FACTS = [
    {"term":"Where does oxygen come from on the ISS?",     "def":"The ISS splits water into hydrogen and oxygen via electrolysis, powered by its solar panels. Solid fuel 'oxygen candles' serve as backup. CO₂ is scrubbed out continuously. The ISS carries 90 days of emergency reserves."},
    {"term":"How do toilets work in space?",               "def":"Like vacuum cleaners. A suction fan pulls liquid waste into a sealed container; solid waste goes into a bag. Astronauts need foot restraints and thigh bars to stay in position. The ISS toilet cost $19 million and took years to get right."},
    {"term":"Is astronaut urine really recycled into drinking water?", "def":"Yes — and it's cleaner than most tap water. The Water Recovery System distils urine using centrifugal force, filters it, and UV-treats it. Recovery rates have reached 98%. NASA's line: 'Yesterday's coffee is tomorrow's coffee.'"},
    {"term":"What is the ISS, actually?",                  "def":"A football-pitch-sized laboratory at 408 km altitude, travelling at 28,000 km/h. Built by 15 countries between 1998 and 2011. Permanently crewed since 2000. It has hosted 273 people from 21 countries. The UK contributes through ESA."},
    {"term":"Why don't astronauts use bread in space?",    "def":"Crumbs float, get inhaled, clog equipment, and can short-circuit electronics. Tortillas replaced bread in the 1980s. They're flat, seal tightly, produce no crumbs, and stay fresh longer in the sealed ISS environment."},
    {"term":"How do astronauts sleep on the ISS?",         "def":"In small soundproofed sleeping bags attached to the wall of a phone-booth-sized cabin. There's no 'up' or 'down', so any orientation works. The ISS sees 16 sunrises every 24 hours — lighting is controlled to maintain sleep rhythms."},
    {"term":"What happened to Columbia in 2003?",          "def":"A piece of foam broke off during launch and punched a hole in the heat shield. On reentry at 7 km/s, superheated gas entered the breach and destroyed the vehicle. All 7 crew were killed. It grounded the Shuttle for 2 years and reshaped NASA's safety culture."},
    {"term":"How far away is the Moon, really?",           "def":"About 384,000 km — which sounds precise until you know it varies from 356,000 km (perigee) to 406,000 km (apogee) as the Moon's orbit wobbles. Apollo 11 took 3 days to get there. Light makes the trip in 1.3 seconds."},
    {"term":"What is microgravity — is it zero gravity?",  "def":"No. At 408 km, Earth's gravity is still 88% as strong as on the surface. What makes things 'float' is that the ISS is in constant freefall — it's moving sideways so fast that it falls around the Earth rather than into it. Everything inside falls together."},
    {"term":"What does SpaceX's Falcon 9 reuse mean?",     "def":"Before SpaceX, rocket first stages were thrown away after each launch — like burning the plane after every flight. Falcon 9's first stage lands itself on a drone ship or back at the launch site. It's been reused up to 20 times per booster, slashing the cost per kilogram to orbit."},
]

def _fetch_nl_launches(upcoming=True):
    """Fetch upcoming or previous launches from Launch Library 2."""
    endpoint = "upcoming" if upcoming else "previous"
    try:
        r = requests.get(
            f"https://ll.thespacedevs.com/2.2.0/launch/{endpoint}/",
            params={"format": "json", "limit": 10, "mode": "list"},
            timeout=12,
            headers={"User-Agent": "space.humanagency.co/1.0"},
        )
        out = []
        for l in r.json().get("results", []):
            out.append({
                "name":         l.get("name", ""),
                "net":          l.get("net", ""),
                "provider":     l.get("lsp_name", "") or "",
                "location":     l.get("location", "") or "",
                "status":       (l.get("status") or {}).get("name", ""),
                "mission_type": l.get("mission_type", "") or "",
            })
        return out
    except Exception as e:
        print(f"[nl_launches] {e}")
        return []

def _render_nl_launch_rows(launches, limit=6):
    rows = ""
    for l in launches[:limit]:
        net = l.get("net", "")
        try:
            import datetime as _dt
            dt = _dt.datetime.fromisoformat(net.replace("Z", "+00:00"))
            date_str = dt.strftime("%a %d %b, %H:%M UTC")
        except Exception:
            date_str = net
        status = l.get("status", "")
        icon = "✅" if "success" in status.lower() else ("❌" if "fail" in status.lower() else "🚀")
        rows += (
            f'<tr><td style="padding:10px 0;border-bottom:1px solid #1e1e42">'
            f'<div style="font-size:14px;font-weight:700;color:#dde0f5">{icon} {l["name"]}</div>'
            f'<div style="font-size:12px;color:#818cf8;margin-top:2px">{l["provider"]} · {date_str}</div>'
            f'<div style="font-size:11px;color:#6b6b99;margin-top:1px">📍 {l["location"]}</div>'
            f'</td></tr>'
        )
    if not rows:
        rows = '<tr><td style="color:#6b6b99;font-size:13px;padding:10px 0">No launches in feed.</td></tr>'
    return rows

def _render_space_newsletter_html(upcoming, previous, company, career, fact, iss_data, week_str):
    upcoming_rows  = _render_nl_launch_rows(upcoming)
    previous_rows  = _render_nl_launch_rows(previous)

    iss_block = ""
    if iss_data and iss_data.get("lat") is not None:
        lat, lon = iss_data["lat"], iss_data["lon"]
        ns = "N" if lat >= 0 else "S"
        ew = "E" if lon >= 0 else "W"
        iss_block = (
            f'<div style="background:#13132a;border-radius:10px;padding:14px;margin-bottom:12px">'
            f'<div style="font-size:12px;color:#818cf8;font-weight:700;margin-bottom:4px">🛸 Current ISS position</div>'
            f'<div style="font-size:13px;color:#dde0f5;font-weight:700">{abs(lat):.1f}°{ns}, {abs(lon):.1f}°{ew}</div>'
            f'<div style="font-size:11px;color:#6b6b99;margin-top:2px">~408 km altitude · ~27,600 km/h · orbiting every 92 min</div>'
            f'</div>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>🛸 Space Digest — {week_str}</title></head>
<body style="background:#05050f;color:#dde0f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;margin:0 auto;padding:24px 20px">

<!-- Header -->
<div style="text-align:center;padding:28px 0 24px;border-bottom:1px solid #1e1e42;margin-bottom:28px">
  <div style="font-size:36px;margin-bottom:10px">🛸</div>
  <div style="font-size:26px;font-weight:900;letter-spacing:-1px;color:#a5b4fc">Space Digest</div>
  <div style="font-size:13px;color:#6b6b99;margin-top:6px">Week of {week_str} · <a href="https://miru.humanagency.co/space" style="color:#818cf8;text-decoration:none">space.humanagency.co</a></div>
</div>

<!-- ISS + SEN -->
<div style="margin-bottom:32px">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#6b6b99;margin-bottom:14px">🔴 ISS &amp; Live Earth</div>
  {iss_block}
  <div style="font-size:13px;color:#6b6b99;line-height:1.7">
    Right now, <strong style="color:#dde0f5">7 people</strong> are living and working aboard the International Space Station — conducting microgravity science, maintaining life systems, and watching the Earth pass below every 92 minutes.
    <br><br>
    British company <strong style="color:#dde0f5">SEN</strong> has 4K cameras mounted on the ISS exterior, streaming live video of Earth from orbit.
    <br><br>
    <a href="https://sen.com/live" style="display:inline-block;background:linear-gradient(135deg,#0ea5e9,#2563eb);color:#fff;text-decoration:none;border-radius:20px;padding:9px 18px;font-size:12px;font-weight:700">📺 Watch Earth live — SEN →</a>
    &nbsp;
    <a href="https://spotthestation.nasa.gov/" style="display:inline-block;background:#13132a;border:1px solid #1e1e42;color:#818cf8;text-decoration:none;border-radius:20px;padding:9px 18px;font-size:12px;font-weight:700">Spot the ISS →</a>
  </div>
</div>

<!-- Last week's launches -->
<div style="margin-bottom:32px">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#6b6b99;margin-bottom:14px">📅 What Launched Last Week</div>
  <table style="width:100%;border-collapse:collapse">{previous_rows}</table>
  <div style="margin-top:12px;font-size:11px;color:#6b6b99">Data: Launch Library 2 · Includes all orbital and suborbital attempts worldwide</div>
</div>

<!-- Upcoming launches -->
<div style="margin-bottom:32px">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#6b6b99;margin-bottom:14px">🚀 Coming Up</div>
  <table style="width:100%;border-collapse:collapse">{upcoming_rows}</table>
  <div style="margin-top:14px">
    <a href="https://miru.humanagency.co/space" style="color:#818cf8;font-size:12px;text-decoration:none;font-weight:600">See all upcoming launches on the live dashboard →</a>
  </div>
</div>

<!-- Fact of the week -->
<div style="background:#0d0d20;border:1px solid #2e2e58;border-radius:14px;padding:20px;margin-bottom:28px">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#818cf8;margin-bottom:12px">💡 Space 101 — Fact of the Week</div>
  <div style="font-size:15px;font-weight:800;color:#dde0f5;margin-bottom:10px;line-height:1.3">{fact["term"]}</div>
  <div style="font-size:13px;color:#6b6b99;line-height:1.7">{fact["def"]}</div>
</div>

<!-- UK Company Spotlight -->
<div style="background:#0d0d20;border:1px solid #2e2e58;border-radius:14px;padding:20px;margin-bottom:28px">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#34d399;margin-bottom:12px">🇬🇧 UK Company Spotlight</div>
  <div style="font-size:16px;font-weight:800;color:#dde0f5;margin-bottom:4px">{company["name"]}</div>
  <div style="font-size:12px;color:#6b6b99;margin-bottom:12px">📍 {company["loc"]}</div>
  <div style="font-size:13px;color:#6b6b99;line-height:1.7;margin-bottom:14px">{company["desc"]}</div>
  <a href="{company["url"]}" style="color:#34d399;font-size:12px;text-decoration:none;font-weight:700">{company["url"].replace("https://","").replace("http://","").rstrip("/")} →</a>
</div>

<!-- Careers -->
<div style="background:#0d0d20;border:1px solid #2e2e58;border-radius:14px;padding:20px;margin-bottom:28px">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#f59e0b;margin-bottom:12px">🎓 Get Into Space</div>
  <div style="font-size:15px;font-weight:800;color:#dde0f5;margin-bottom:10px">{career["icon"]} {career["step"]}</div>
  <div style="font-size:13px;color:#6b6b99;line-height:1.7;margin-bottom:14px">{career["body"]}</div>
  <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:4px">
    <a href="https://jobs.ukspace.org" style="font-size:11px;color:#f59e0b;text-decoration:none;border:1px solid #f59e0b44;border-radius:20px;padding:4px 12px;font-weight:600">jobs.ukspace.org</a>
    <a href="https://spacetalent.org" style="font-size:11px;color:#f59e0b;text-decoration:none;border:1px solid #f59e0b44;border-radius:20px;padding:4px 12px;font-weight:600">Space Talent</a>
    <a href="https://ukseds.org" style="font-size:11px;color:#f59e0b;text-decoration:none;border:1px solid #f59e0b44;border-radius:20px;padding:4px 12px;font-weight:600">UKSEDS</a>
  </div>
</div>

<!-- Footer -->
<div style="text-align:center;padding-top:24px;border-top:1px solid #1e1e42;font-size:11px;color:#2e2e58;line-height:1.8">
  <div>🛸 <a href="https://miru.humanagency.co/space" style="color:#818cf8;text-decoration:none">space.humanagency.co</a> · humanagency.co</div>
  <div>You signed up for the UK Space Digest at space.humanagency.co</div>
</div>

</body>
</html>"""

@app.route("/api/space/newsletter/generate", methods=["GET", "POST"])
def api_space_newsletter_generate():
    import datetime
    token = request.args.get("token") or (request.get_json(silent=True) or {}).get("token", "")
    if token != os.environ.get("SPACE_NL_TOKEN", "space-digest-2026"):
        return jsonify({"error": "Unauthorized"}), 401
    try:
        today     = datetime.date.today()
        week_num  = today.isocalendar()[1]
        week_str  = today.strftime("%d %b %Y")

        upcoming = _fetch_nl_launches(upcoming=True)
        previous = _fetch_nl_launches(upcoming=False)

        company = _NL_COMPANIES[week_num % len(_NL_COMPANIES)]
        career  = _NL_CAREERS[week_num % len(_NL_CAREERS)]
        fact    = _NL_FACTS[week_num % len(_NL_FACTS)]

        iss_data = None
        try:
            ri = requests.get("http://api.open-notify.org/iss-now.json", timeout=5)
            pos = ri.json().get("iss_position", {})
            iss_data = {"lat": float(pos.get("latitude", 0)), "lon": float(pos.get("longitude", 0))}
        except Exception:
            pass

        html    = _render_space_newsletter_html(upcoming, previous, company, career, fact, iss_data, week_str)
        subject = f"🛸 Space Digest — {week_str}"

        # Store in Supabase (space_newsletter table)
        try:
            lib._sb().table("space_newsletter").insert({
                "week_of":      str(today),
                "subject":      subject,
                "html_content": html,
                "sent":         False,
            }).execute()
        except Exception as db_err:
            print(f"[space_nl] DB insert failed (table may not exist yet): {db_err}")

        # Also save launches to space_launches table
        try:
            all_launches = upcoming + previous
            for l in all_launches:
                lib._sb().table("space_launches").upsert({
                    "name":         l["name"],
                    "provider":     l["provider"],
                    "location":     l["location"],
                    "net":          l["net"] or None,
                    "status":       l["status"],
                    "mission_type": l["mission_type"],
                    "fetched_at":   datetime.datetime.utcnow().isoformat(),
                }, on_conflict="name,net").execute()
        except Exception as db_err:
            print(f"[space_nl] launches DB insert failed: {db_err}")

        return jsonify({"ok": True, "subject": subject, "upcoming": len(upcoming), "previous": len(previous)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/space/newsletter/latest")
def api_space_newsletter_latest():
    try:
        rows = lib._sb().table("space_newsletter").select("id,week_of,subject,created_at,sent").order("created_at", desc=True).limit(5).execute().data or []
        return jsonify({"editions": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/space/newsletter")
def space_newsletter_preview():
    try:
        rows = lib._sb().table("space_newsletter").select("html_content,subject,week_of").order("created_at", desc=True).limit(1).execute().data or []
        if rows and rows[0].get("html_content"):
            return rows[0]["html_content"]
        return "<p style='font-family:sans-serif;padding:40px;color:#666'>No newsletter generated yet. Hit <code>/api/space/newsletter/generate?token=space-digest-2026</code> to generate one.</p>", 404
    except Exception as e:
        return f"<p style='font-family:sans-serif;padding:40px;color:red'>Error: {e}</p>", 500

# ── AI Newsletter ─────────────────────────────────────────────────────────────

_AI_NL_CONCEPTS = [
    {"term": "LLM", "full": "Large Language Model", "definition": "The technology behind ChatGPT, Claude and Gemini. Trained on vast amounts of text to predict the next word — billions of times — until it can write, reason and converse fluently. It's not looking things up; it learned patterns so deeply that it can generate plausible, useful text from scratch."},
    {"term": "RAG", "full": "Retrieval-Augmented Generation", "definition": "A way to ground AI answers in real, up-to-date information. Instead of relying only on what the model learned during training, it first searches a database or document store, retrieves the relevant bits, then generates an answer using those as context. This is how AI assistants can reference your company's own documents."},
    {"term": "Hallucination", "full": "When AI Gets It Wrong", "definition": "AI systems sometimes state things that are completely false — with total confidence. It happens because the model generates plausible-sounding text, not necessarily true text. The fix: treat AI like a brilliant but overconfident colleague. Useful for drafts and ideas. Always verify facts that matter."},
    {"term": "AI Agent", "full": "AI That Takes Action", "definition": "Until recently, AI mostly answered questions. Agents take action. Give an agent a goal — 'research competitors and write a report' — and it will browse the web, read pages, take notes, and draft the report itself. This is the next big shift: AI as a capable junior colleague that can actually get things done."},
    {"term": "Prompt", "full": "How You Talk to AI", "definition": "A prompt is what you type to an AI. The way you phrase a request makes an enormous difference to the output quality. Specificity wins: tell the AI who the answer is for, what format you want, what to avoid, and what you already know. 'Summarise this for a non-technical CFO in 3 bullets' beats 'summarise this' every time."},
    {"term": "Fine-tuning", "full": "Teaching AI Your Domain", "definition": "Taking a general-purpose model and training it further on your specific data — medical records, legal contracts, customer service logs — so it becomes a specialist. Like hiring a smart generalist and giving them six months of deep sector experience. The result is a model that speaks your language."},
    {"term": "Transformer", "full": "The Architecture Behind Everything", "definition": "The neural network design that powers virtually all modern AI. Introduced in a 2017 Google paper called 'Attention Is All You Need.' The 'T' in GPT, the 'T' in BERT. Transformers use 'attention' — a mechanism that lets the model weigh how important each word is relative to every other word in the input."},
    {"term": "RLHF", "full": "Reinforcement Learning from Human Feedback", "definition": "The key technique that makes LLMs helpful rather than chaotic. Human raters score model outputs — 'this response is helpful,' 'this one is harmful' — and the model trains to produce what humans prefer. It's how ChatGPT went from raw text predictor to something that actually follows instructions."},
    {"term": "Chain of Thought", "full": "Getting AI to Show Its Working", "definition": "A prompting technique: ask the model to 'think step by step' before giving an answer. It dramatically improves accuracy on maths, logic and reasoning tasks. The model isn't smarter — it's just forced to break the problem down before committing to an answer, which catches many errors."},
    {"term": "Mixture of Experts", "full": "How GPT-4 Is So Large Yet So Fast", "definition": "An architecture where a massive model is actually made up of many specialist sub-models — 'experts.' For each input, only the most relevant experts are activated. This lets you build enormously capable models while keeping the compute cost manageable. It's how GPT-4 reportedly has 1.8 trillion parameters but doesn't cost a fortune to run."},
    {"term": "Multimodal AI", "full": "AI That Sees, Hears and Reads", "definition": "AI that can process multiple types of input — text, images, audio, video — rather than just one. GPT-4o, Gemini and Claude are all multimodal: show them a photo and ask a question, paste a chart and ask for analysis, or speak to them directly. This is what makes AI feel genuinely versatile rather than a clever search box."},
    {"term": "On-Device AI", "full": "AI Without the Cloud", "definition": "AI that runs entirely on your phone or laptop, without sending data to a server. Apple Intelligence, Google's Gemini Nano, and Meta's small Llama models are examples. Faster responses, full privacy — your data never leaves your device. The tradeoff: smaller models, less capable than the cloud giants."},
]

_AI_NL_TOOLS = [
    {"name": "Claude", "maker": "Anthropic", "url": "https://claude.ai", "category": "AI Assistant", "desc": "Anthropic's AI assistant, built with safety as a first principle using Constitutional AI. Known for nuanced long-form writing, careful reasoning, and admitting uncertainty. Handles 200,000+ token contexts — you can paste an entire book and ask questions. Strong for analysis, drafting, and coding."},
    {"name": "ChatGPT", "maker": "OpenAI", "url": "https://chat.openai.com", "category": "AI Assistant", "desc": "The product that launched the generative AI era in November 2022 — 100 million users in 2 months. Powered by GPT-4o, which understands text, images and audio. Has a huge plugin/tool ecosystem, web browsing, image generation (DALL-E) and code execution built in. The Swiss Army knife of AI tools."},
    {"name": "Perplexity", "maker": "Perplexity AI", "url": "https://perplexity.ai", "category": "AI Search", "desc": "An AI search engine that answers questions with cited, real-time sources — unlike ChatGPT which relies on training data. Ask it anything and it searches, reads and summarises relevant pages with footnotes. Particularly good for research, fact-checking, and staying current. Faster and more accurate than Googling and reading articles yourself."},
    {"name": "Cursor", "maker": "Anysphere", "url": "https://cursor.sh", "category": "AI Coding", "desc": "The AI code editor that's taken the developer world by storm. Built on VS Code but deeply integrated with Claude and GPT-4. You can describe a feature in plain English and it writes the code, spots bugs before you see them, and edits multiple files simultaneously. Millions of developers have switched from traditional editors."},
    {"name": "Notebook LM", "maker": "Google", "url": "https://notebooklm.google.com", "category": "Research", "desc": "Upload your own documents — PDFs, articles, meeting notes — and have an AI that has read all of them answer your questions. No hallucination risk on things it hasn't read because it only draws on what you gave it. Also generates audio summaries you can listen to. Excellent for researchers, analysts, and anyone drowning in documents."},
    {"name": "Gamma", "maker": "Gamma", "url": "https://gamma.app", "category": "Presentations", "desc": "Type a topic or paste an outline and get a full presentation in seconds — designed slides, clean layout, relevant structure. Not a replacement for a designer, but a way to go from blank page to first draft in under a minute. Saves the three hours you'd spend arguing with PowerPoint to get something that looks decent."},
    {"name": "ElevenLabs", "maker": "ElevenLabs", "url": "https://elevenlabs.io", "category": "Voice AI", "desc": "The most convincing AI voice synthesis available. Can clone a voice from a short audio sample, create characters with distinct voices, and generate narration that's nearly indistinguishable from a professional voiceover. Used by podcasters, game developers, audiobook creators and content makers worldwide."},
    {"name": "Midjourney", "maker": "Midjourney", "url": "https://midjourney.com", "category": "Image Generation", "desc": "The image generator that changed what people thought AI could do. Type a description and get a photorealistic or artistic image in seconds. The current gold standard for quality, particularly for illustrations, concept art and creative work. Still requires a Discord account but the results justify the friction."},
    {"name": "Otter.ai", "maker": "Otter AI", "url": "https://otter.ai", "category": "Transcription", "desc": "Joins your Zoom/Teams/Meet calls and transcribes in real time, identifying who said what. At the end of any meeting you get a full transcript, summary, and action items automatically. The 20-minute post-meeting write-up that everyone dreads now takes 30 seconds to review. Genuinely changes how meetings work."},
    {"name": "Runway", "maker": "Runway ML", "url": "https://runwayml.com", "category": "Video AI", "desc": "Text-to-video generation that's actually usable. Describe a scene and get a short video clip; extend existing footage; remove backgrounds in real time. Used by film studios including Marvel to accelerate post-production. The most credible answer to 'what does AI do to video production?'"},
]

_AI_NL_PEOPLE = [
    {"name": "Geoffrey Hinton", "role": "Godfather of Deep Learning · Turing Award 2018", "bio": "Spent decades championing neural networks when everyone thought they were a dead end. He was proved spectacularly right. In 2023, he left Google specifically so he could speak freely about AI risks — a rare move from someone who built the technology. His warnings carry weight because of what he built.", "quote": "I console myself with the normal excuse: if I hadn't done it, someone else would have.", "url": "https://en.wikipedia.org/wiki/Geoffrey_Hinton"},
    {"name": "Andrej Karpathy", "role": "Former OpenAI & Tesla · AI Educator", "bio": "Led AI at Tesla (Autopilot) and was a founding OpenAI member. Now the most popular AI educator in the world — his YouTube tutorials on building neural networks from scratch have been watched by millions. If you want to truly understand how LLMs work, his 'Neural Networks: Zero to Hero' series is the starting point.", "quote": "The hottest new programming language is English.", "url": "https://karpathy.ai"},
    {"name": "Demis Hassabis", "role": "CEO · Google DeepMind · Nobel Prize 2024", "bio": "Co-founded DeepMind with a mission to 'solve intelligence, then use it to solve everything else.' Built AlphaGo, which beat the world Go champion — considered far harder than chess — then AlphaFold, which cracked protein folding, a 50-year biology problem. Won the Nobel Prize in Chemistry in 2024. The only person to win the Nobel and design a world-beating game AI.", "quote": "Science is the engine of prosperity. AI is the most powerful tool we have to accelerate it.", "url": "https://en.wikipedia.org/wiki/Demis_Hassabis"},
    {"name": "Fei-Fei Li", "role": "Co-Director · Stanford HAI", "bio": "Created ImageNet — 14 million labelled images that became the benchmark that sparked the deep learning revolution. When she released the dataset in 2009, computers could barely recognise objects. Within 5 years they were beating humans. She also advocates for AI that serves humanity broadly, not just tech companies.", "quote": "AI is neither good nor evil. It's a tool that amplifies human intent.", "url": "https://en.wikipedia.org/wiki/Fei-Fei_Li"},
    {"name": "Sam Altman", "role": "CEO · OpenAI", "bio": "Turned OpenAI from a research lab into the company that brought AI to hundreds of millions of people with ChatGPT. Simultaneously believes he's building potentially the most dangerous technology ever made, and that it's still the right thing to do. The most consequential product launch since the smartphone — 100 million users in 2 months.", "quote": "AI will be the greatest tool humanity has ever built.", "url": "https://en.wikipedia.org/wiki/Sam_Altman"},
    {"name": "Yann LeCun", "role": "Chief AI Scientist · Meta · Turing Award 2018", "bio": "Invented convolutional neural networks (CNNs) — the technology that made computers able to see. Every photo tag on Facebook, every face unlocking your phone, every medical scan uses CNNs. Now a vocal sceptic of current AI safety concerns and a proponent of open-source AI. Argues the current LLM approach is fundamentally limited.", "quote": "Our intelligence is what makes us human, and AI is an extension of that quality.", "url": "https://en.wikipedia.org/wiki/Yann_LeCun"},
    {"name": "Yoshua Bengio", "role": "Founder · Mila · Turing Award 2018", "bio": "One of the three scientists who made deep learning work, he helped establish Montreal as a world AI hub. Unlike some peers, Bengio now spends much of his time warning about AI risks and advocating for safety research — signed a letter calling for a pause on the most powerful AI training.", "quote": "AI is not good or evil in itself. It's a tool, like fire. The question is how we use it.", "url": "https://en.wikipedia.org/wiki/Yoshua_Bengio"},
    {"name": "Dario Amodei", "role": "CEO · Anthropic", "bio": "Left OpenAI in 2021 with his sister Daniela to found Anthropic, after growing concerns about AI safety. Created Claude — an AI designed from the ground up to be safe, honest and helpful. Pioneer of Constitutional AI, a method that trains the model to follow principles rather than just optimising for approval. One of the most thoughtful voices on both the capability and danger of AI.", "quote": "We are building one of the most transformative and potentially dangerous technologies in history.", "url": "https://en.wikipedia.org/wiki/Dario_Amodei"},
]

_AI_NL_PROMPTS = [
    {"title": "Rewrite anything for your audience", "prompt": "Rewrite this [document/email/report] for [audience description]. Keep all the key information but adjust the tone, vocabulary and examples so it lands for someone who [describe what they know/care about]. Here is the original:\n\n[paste text]", "why": "The most common AI mistake is forgetting to specify the audience. This one change transforms generic output into something that actually works. Use it for proposals, comms, and anything you need multiple versions of."},
    {"title": "Turn a meeting into action", "prompt": "Here is a transcript/notes from a meeting:\n\n[paste transcript or notes]\n\nExtract: (1) key decisions made, (2) action items with owner and deadline if mentioned, (3) open questions that need answers, (4) a 3-sentence summary I could send to someone who wasn't there.", "why": "Paste this into Claude or ChatGPT immediately after a meeting. Turns 60 minutes of conversation into a clear record in under 10 seconds. Saves the post-meeting write-up that everyone dreads."},
    {"title": "Pressure-test your own argument", "prompt": "I am about to [propose / present / argue for] the following:\n\n[describe your position or plan]\n\nPlease play devil's advocate. What are the strongest objections? What assumptions am I making that could be wrong? What would a sceptic say? Don't hold back.", "why": "Most people use AI to confirm their thinking. This flips it. Use before a big presentation, pitch, or decision to find the holes in your argument before someone else does."},
    {"title": "Explain anything in plain English", "prompt": "Explain [technical concept / report / document] as if you're talking to a smart, curious person who has no background in [field]. Use a real-world analogy. Avoid jargon. Then give me 3 questions a non-expert might ask about it.", "why": "Works for understanding dense reports, briefing non-technical stakeholders, or preparing to explain something in a meeting. The 'analogy' instruction is the key — it forces the AI to connect the concept to something real."},
    {"title": "Generate a week of content", "prompt": "I need 5 social media posts for [platform: LinkedIn / Instagram / X] about [topic]. I am a [role] targeting [audience]. Tone: [professional / conversational / direct]. Each post should be different in format — one question, one tip, one story, one statistic, one opinion. Keep each under [word count].", "why": "Most people use AI for one post at a time. Batching a week's worth in one prompt, with format constraints, gets you varied content that doesn't feel repetitive. Adjust the format list to whatever works for your platform."},
    {"title": "Decode any document before you sign", "prompt": "Here is a [contract / terms / agreement]. Read it and tell me: (1) what I am committing to, (2) any clauses that are unusual or unfair, (3) what happens if things go wrong, (4) the 3 questions I should ask before signing. Plain English only.\n\n[paste document]", "why": "Legal documents are written for lawyers. This prompt makes them readable. Don't use it as legal advice — use it to know which questions to ask your actual solicitor, or to decide if you even need one."},
    {"title": "Build a study guide for anything", "prompt": "I want to understand [topic] deeply. I have [X hours/weeks] to learn it. I have [describe current knowledge level]. Create a structured learning plan: what to study in what order, the best free resources for each section, and 10 questions I should be able to answer when I'm done.", "why": "AI is an extraordinary tutor when you point it at a specific goal. This prompt turns 'I want to learn X' into an actual plan with sequenced content. Works for career pivots, exam prep, or just getting to grips with something your job now requires."},
    {"title": "Write a first draft, then make it yours", "prompt": "Write a first draft of [email / report / proposal / bio] with the following brief:\n- Purpose: [what it needs to achieve]\n- Audience: [who will read it]\n- Key points to cover: [list them]\n- Tone: [formal / warm / direct]\n- Length: approximately [X words]\n\nThen list 3 things I should consider changing to make it feel more personal.", "why": "The blank page is the hardest part. This generates a solid draft fast, and the follow-up question ('what should I change') actively invites you to edit rather than just accept it. The best AI output is always what you do with the draft."},
]

_AI_NL_DUMMIES = [
    {"q": "What actually is AI?", "a": "Software that learned from millions of examples instead of following a hand-written rulebook. Show it enough cat photos and it learns to spot cats — no programmer needed. It's very good at pattern matching and completely hopeless at common sense."},
    {"q": "Why does ChatGPT sound confident even when it's wrong?", "a": "Because it's predicting likely words, not looking up facts. It's an incredibly sophisticated autocomplete — it generates whatever text sounds most plausible. Plausible isn't the same as true. Always check anything important."},
    {"q": "Is AI going to take my job?", "a": "Some tasks, yes. Whole jobs, rarely overnight. AI is better thought of as a very fast junior assistant — it handles the repetitive bits while you handle the judgement, context, and relationships. The jobs most at risk are ones that are almost entirely routine text or data work."},
    {"q": "What's the difference between ChatGPT, Claude, and Gemini?", "a": "They're all large language models — the same basic technology. ChatGPT (OpenAI) has the biggest ecosystem and brand recognition. Claude (Anthropic) is often considered strongest at long documents and nuanced writing. Gemini (Google) is built into Google's products — Search, Docs, Gmail. Try all three on the same task and pick the one that works best for you."},
    {"q": "Do I need to know how to code to use AI?", "a": "No. The whole point is that you just talk to it in plain English. You do need to be specific — vague requests get vague answers. But writing a good prompt is more about being clear than being technical. If you can write a decent email, you can use AI effectively."},
    {"q": "Is my data private when I use ChatGPT or Claude?", "a": "By default, conversations may be used to train models — so don't paste in personal data, passwords, client details, or anything confidential. Both OpenAI and Anthropic have paid plans and enterprise options with stronger privacy guarantees. When in doubt, imagine your IT department can read every message."},
]

def _render_ai_newsletter_html(concept, tool, person, prompt_item, dummy, week_str):
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>🤖 AI Digest — {week_str}</title></head>
<body style="background:#f8fafc;color:#1e293b;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif;max-width:600px;margin:0 auto;padding:24px 20px">

<!-- Header -->
<div style="text-align:center;padding:32px 0 28px;border-bottom:2px solid #e2e8f0;margin-bottom:32px">
  <div style="font-size:36px;margin-bottom:10px">🤖</div>
  <div style="font-size:26px;font-weight:900;letter-spacing:-1px;background:linear-gradient(135deg,#4f46e5,#7c3aed);-webkit-background-clip:text;-webkit-text-fill-color:transparent">AI Digest</div>
  <div style="font-size:13px;color:#64748b;margin-top:6px">Week of {week_str} · <a href="https://ai.humanagency.co" style="color:#4f46e5;text-decoration:none">ai.humanagency.co</a></div>
  <div style="font-size:12px;color:#94a3b8;margin-top:4px">What changed in AI, what it means for you — no hype, no jargon</div>
</div>

<!-- Concept of the Week -->
<div style="background:linear-gradient(135deg,#eef2ff,#f5f3ff);border:1px solid #c7d2fe;border-radius:16px;padding:22px;margin-bottom:24px">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#6366f1;margin-bottom:10px">🧠 Concept of the Week</div>
  <div style="display:flex;align-items:baseline;gap:10px;margin-bottom:8px;flex-wrap:wrap">
    <span style="font-size:22px;font-weight:900;color:#312e81">{concept["term"]}</span>
    <span style="font-size:13px;color:#6366f1;font-weight:600">{concept["full"]}</span>
  </div>
  <div style="font-size:14px;color:#1e293b;line-height:1.75">{concept["definition"]}</div>
  <div style="margin-top:14px">
    <a href="https://ai.humanagency.co#jargon" style="font-size:11px;color:#6366f1;text-decoration:none;font-weight:700;border:1px solid #c7d2fe;border-radius:20px;padding:5px 14px">See full jargon decoder →</a>
  </div>
</div>

<!-- Tool Spotlight -->
<div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:16px;padding:22px;margin-bottom:24px;box-shadow:0 1px 3px rgba(0,0,0,.06)">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#7c3aed;margin-bottom:10px">🛠 Tool Spotlight</div>
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
    <div style="background:linear-gradient(135deg,#4f46e5,#7c3aed);color:#fff;width:40px;height:40px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:900;flex-shrink:0">{tool["name"][0]}</div>
    <div>
      <div style="font-size:17px;font-weight:800;color:#1e293b">{tool["name"]}</div>
      <div style="font-size:12px;color:#94a3b8">{tool["maker"]} · {tool["category"]}</div>
    </div>
  </div>
  <div style="font-size:13px;color:#475569;line-height:1.75;margin-bottom:14px">{tool["desc"]}</div>
  <a href="{tool["url"]}" style="display:inline-block;background:linear-gradient(135deg,#4f46e5,#7c3aed);color:#fff;text-decoration:none;border-radius:20px;padding:9px 18px;font-size:12px;font-weight:700">Try {tool["name"]} →</a>
</div>

<!-- Try It Yourself (Prompt) -->
<div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:16px;padding:22px;margin-bottom:24px;box-shadow:0 1px 3px rgba(0,0,0,.06)">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#0891b2;margin-bottom:10px">⚡ Try It Yourself</div>
  <div style="font-size:16px;font-weight:800;color:#1e293b;margin-bottom:8px">{prompt_item["title"]}</div>
  <div style="background:#f1f5f9;border-left:3px solid #4f46e5;border-radius:0 8px 8px 0;padding:14px 16px;margin-bottom:12px">
    <div style="font-size:12px;font-weight:700;color:#94a3b8;margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em">Copy this prompt →</div>
    <div style="font-size:13px;color:#1e293b;line-height:1.7;white-space:pre-wrap;font-family:monospace">{prompt_item["prompt"]}</div>
  </div>
  <div style="font-size:12px;color:#64748b;line-height:1.6"><strong style="color:#475569">Why it works:</strong> {prompt_item["why"]}</div>
</div>

<!-- Who to Follow -->
<div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:16px;padding:22px;margin-bottom:24px;box-shadow:0 1px 3px rgba(0,0,0,.06)">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#059669;margin-bottom:14px">👤 Person Behind the AI</div>
  <div style="display:flex;align-items:center;gap:14px;margin-bottom:14px">
    <div style="background:linear-gradient(135deg,#10b981,#059669);color:#fff;width:44px;height:44px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:900;flex-shrink:0">{person["name"][0]}</div>
    <div>
      <div style="font-size:16px;font-weight:800;color:#1e293b">{person["name"]}</div>
      <div style="font-size:12px;color:#94a3b8;margin-top:2px">{person["role"]}</div>
    </div>
  </div>
  <div style="font-size:13px;color:#475569;line-height:1.75;margin-bottom:12px">{person["bio"]}</div>
  <div style="background:#f0fdf4;border-radius:10px;padding:12px 14px;margin-bottom:14px">
    <div style="font-size:13px;color:#166534;font-style:italic;line-height:1.6">"{person["quote"]}"</div>
  </div>
  <a href="{person["url"]}" style="font-size:12px;color:#059669;text-decoration:none;font-weight:700">Read more →</a>
</div>

<!-- Plain English -->
<div style="background:linear-gradient(135deg,#fefce8,#fffbeb);border:1px solid #fde68a;border-radius:16px;padding:22px;margin-bottom:24px">
  <div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#d97706;margin-bottom:12px">🙋 Plain English Q&amp;A</div>
  <div style="font-size:15px;font-weight:800;color:#1e293b;margin-bottom:10px">{dummy["q"]}</div>
  <div style="font-size:13px;color:#78350f;line-height:1.75">{dummy["a"]}</div>
</div>

<!-- CTA -->
<div style="text-align:center;background:linear-gradient(135deg,#4f46e5,#7c3aed);border-radius:16px;padding:28px 20px;margin-bottom:28px">
  <div style="font-size:18px;font-weight:900;color:#fff;margin-bottom:8px">Explore the full AI guide</div>
  <div style="font-size:13px;color:#c7d2fe;margin-bottom:18px">Jargon decoder · Use cases by role · People who built it · Articles worth reading</div>
  <a href="https://ai.humanagency.co" style="display:inline-block;background:#fff;color:#4f46e5;text-decoration:none;border-radius:20px;padding:10px 24px;font-size:13px;font-weight:800">Open ai.humanagency.co →</a>
</div>

<!-- Footer -->
<div style="text-align:center;padding-top:20px;border-top:1px solid #e2e8f0;font-size:11px;color:#94a3b8;line-height:1.9">
  <div>🤖 <a href="https://ai.humanagency.co" style="color:#4f46e5;text-decoration:none">ai.humanagency.co</a> · <a href="https://humanagency.co" style="color:#4f46e5;text-decoration:none">humanagency.co</a></div>
  <div>You signed up for the AI Digest at ai.humanagency.co</div>
</div>

</body>
</html>"""


@app.route("/api/ai/newsletter/generate", methods=["GET", "POST"])
def api_ai_newsletter_generate():
    import datetime
    token = request.args.get("token") or (request.get_json(silent=True) or {}).get("token", "")
    if token != os.environ.get("AI_NL_TOKEN", "ai-digest-2026"):
        return jsonify({"error": "Unauthorized"}), 401
    try:
        today    = datetime.date.today()
        week_num = today.isocalendar()[1]
        week_str = today.strftime("%d %b %Y")

        concept = _AI_NL_CONCEPTS[week_num % len(_AI_NL_CONCEPTS)]
        tool    = _AI_NL_TOOLS[week_num % len(_AI_NL_TOOLS)]
        person  = _AI_NL_PEOPLE[week_num % len(_AI_NL_PEOPLE)]
        prompt_item = _AI_NL_PROMPTS[week_num % len(_AI_NL_PROMPTS)]
        dummy   = _AI_NL_DUMMIES[week_num % len(_AI_NL_DUMMIES)]

        html    = _render_ai_newsletter_html(concept, tool, person, prompt_item, dummy, week_str)
        subject = f"🤖 AI Digest — {week_str}"

        try:
            lib._sb().table("ai_newsletter").insert({
                "week_of":      str(today),
                "subject":      subject,
                "html_content": html,
                "sent":         False,
            }).execute()
        except Exception as db_err:
            print(f"[ai_nl] DB insert failed (table may not exist yet): {db_err}")

        return jsonify({"ok": True, "subject": subject, "concept": concept["term"], "tool": tool["name"], "person": person["name"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ai/newsletter/latest")
def api_ai_newsletter_latest():
    try:
        rows = lib._sb().table("ai_newsletter").select("id,week_of,subject,created_at,sent").order("created_at", desc=True).limit(5).execute().data or []
        return jsonify({"editions": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/ai/newsletter")
def ai_newsletter_preview():
    try:
        rows = lib._sb().table("ai_newsletter").select("html_content,subject,week_of").order("created_at", desc=True).limit(1).execute().data or []
        if rows and rows[0].get("html_content"):
            return rows[0]["html_content"]
        return "<p style='font-family:sans-serif;padding:40px;color:#666'>No newsletter generated yet. Hit <code>/api/ai/newsletter/generate?token=ai-digest-2026</code> to generate one.</p>", 404
    except Exception as e:
        return f"<p style='font-family:sans-serif;padding:40px;color:red'>Error: {e}</p>", 500


# ── PM Intel ──────────────────────────────────────────────────────────────────

@app.route("/pm")
def pm_home():
    return render_template("pm.html")


def _sb_pm():
    from supabase import create_client
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


@app.route("/api/pm/projects", methods=["GET", "POST"])
def api_pm_projects():
    try:
        sb = _sb_pm()
        if request.method == "POST":
            data = request.json or {}
            name = (data.get("name") or "").strip()
            if not name:
                return jsonify({"error": "Name required"}), 400
            row = sb.table("pm_projects").insert({
                "name": name,
                "proj_type": data.get("proj_type", "IT Transformation"),
                "phase": int(data.get("phase", 1)),
                "description": data.get("description", ""),
            }).execute()
            return jsonify({"ok": True, "project": row.data[0]})
        rows = sb.table("pm_projects").select("*").order("created_at", desc=True).execute()
        return jsonify(rows.data or [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pm/projects/<project_id>", methods=["PATCH", "DELETE"])
def api_pm_project(project_id):
    try:
        sb = _sb_pm()
        if request.method == "DELETE":
            sb.table("pm_docs").delete().eq("project_id", project_id).execute()
            sb.table("pm_projects").delete().eq("id", project_id).execute()
            return jsonify({"ok": True})
        sb.table("pm_projects").update(request.json or {}).eq("id", project_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pm/docs", methods=["GET", "POST"])
def api_pm_docs():
    try:
        sb = _sb_pm()
        if request.method == "POST":
            data = request.json or {}
            row = sb.table("pm_docs").insert({
                "project_id": data.get("project_id"),
                "phase": int(data.get("phase", 1)),
                "doc_type": data.get("doc_type", "note"),
                "title": data.get("title", ""),
                "content": data.get("content", ""),
                "ai_summary": data.get("ai_summary", ""),
                "status": data.get("status", "draft"),
            }).execute()
            return jsonify({"ok": True, "doc": row.data[0]})
        project_id = request.args.get("project_id")
        q = sb.table("pm_docs").select("*").order("created_at", desc=True)
        if project_id:
            q = q.eq("project_id", project_id)
        return jsonify(q.execute().data or [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pm/docs/<doc_id>", methods=["PATCH", "DELETE"])
def api_pm_doc(doc_id):
    try:
        sb = _sb_pm()
        if request.method == "DELETE":
            sb.table("pm_docs").delete().eq("id", doc_id).execute()
            return jsonify({"ok": True})
        sb.table("pm_docs").update(request.json or {}).eq("id", doc_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pm/analyse", methods=["POST"])
def api_pm_analyse():
    data = request.json or {}
    action = data.get("action", "summarise")
    content = (data.get("content") or "").strip()
    context = (data.get("context") or "").strip()
    if not content:
        return jsonify({"error": "Content required"}), 400
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return jsonify({"error": "AI not configured"}), 500

    PROMPTS = {
        "boscard": (
            "You are a senior programme manager. Generate a complete BOSCARD from the brief below.\n"
            "Output clean markdown with these sections:\n"
            "## Benefits\nWhat value does this deliver to the business?\n"
            "## Objectives\n3-5 SMART, measurable objectives\n"
            "## Scope\n**In scope:** ...\n**Out of scope:** ...\n"
            "## Constraints\nTime, budget, resource, regulatory limits\n"
            "## Assumptions\nKey assumptions being made\n"
            "## Risks\nTop 5 risks with (High/Med/Low) probability and impact\n"
            "## Dependencies\nInternal and external dependencies\n\nBrief:\n"
        ),
        "raid": (
            "You are a programme manager. Extract ALL RAID items from the text below.\n"
            "Output clean markdown tables:\n"
            "## Risks\n| ID | Description | Prob | Impact | Owner | Mitigation |\n|---|---|---|---|---|---|\n"
            "## Assumptions\n| ID | Description | Owner | Validation |\n|---|---|---|---|\n"
            "## Issues\n| ID | Description | Priority | Owner | Resolution |\n|---|---|---|---|---|\n"
            "## Dependencies\n| ID | Description | Type | Owner | Due |\n|---|---|---|---|---|\n\nText:\n"
        ),
        "status": (
            "You are a programme manager. Write a formal Weekly Status Report from the notes below.\n"
            "Format:\n**Week ending:** [date if mentioned, else 'w/e [today]']\n"
            "**Overall RAG:** 🟢 Green / 🟡 Amber / 🔴 Red — [one-line reason]\n"
            "**Executive Summary:** [2-3 sentences]\n\n"
            "**Accomplishments this week:**\n- [bullet]\n\n"
            "**Planned next week:**\n- [bullet]\n\n"
            "**Risks & Issues:**\n| Item | RAG | Owner | Action |\n|---|---|---|---|\n\n"
            "**Decisions required:**\n- [bullet or 'None this week']\n\nNotes:\n"
        ),
        "pir": (
            "You are a programme manager writing a Post-Implementation Review.\n"
            "Structure the input into:\n"
            "## Executive Summary\n"
            "## Objectives vs Actuals\n| Objective | Target | Achieved | RAG |\n|---|---|---|---|\n"
            "## What Went Well\n- [bullets]\n"
            "## Areas for Improvement\n- [bullets]\n"
            "## Recommendations for Future Projects\n- [bullets]\n"
            "## Benefits Realised\n\nInput:\n"
        ),
        "tsa": (
            "You are an IT separation programme manager reviewing a TSA document.\n"
            "Produce:\n## TSA Summary\nServices, exit dates, SLAs\n"
            "## Open Actions\n| Action | Owner | Due | Status |\n|---|---|---|---|\n"
            "## Exit Risk Areas\nServices at risk of delayed exit\n"
            "## Recommendations\nNext steps and critical path items\n\nContent:\n"
        ),
        "summarise": (
            "Summarise the following document for a senior executive. Output:\n"
            "**One-line summary** (max 20 words)\n"
            "**Key Points** (5 bullets max)\n"
            "**Decisions / Actions needed** (if any)\n"
            "**RAG Status** (Green/Amber/Red with reason, if inferable)\n\nDocument:\n"
        ),
        "raci": (
            "You are a programme manager. Generate a RACI matrix from the content below.\n"
            "List all activities/deliverables as rows. List stakeholder roles as columns.\n"
            "Use R=Responsible, A=Accountable, C=Consulted, I=Informed.\n"
            "Output as a markdown table.\n\nContent:\n"
        ),
    }

    prompt_text = PROMPTS.get(action, PROMPTS["summarise"])
    if context:
        prompt_text = f"Project context: {context}\n\n" + prompt_text

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt_text + content}],
                "temperature": 0.25,
                "max_tokens": 2000,
            },
            timeout=35,
        )
        if r.status_code != 200:
            return jsonify({"error": f"AI error {r.status_code}"}), 500
        result = r.json()["choices"][0]["message"]["content"].strip()
        return jsonify({"result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pm/intake", methods=["POST"])
def api_pm_intake():
    data = request.json or {}
    problem = (data.get("problem") or "").strip()
    if not problem:
        return jsonify({"error": "Problem description required"}), 400
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return jsonify({"error": "AI not configured"}), 500

    prompt = (
        "You are a senior programme manager with deep experience in IT projects "
        "(SAP, ERP, migrations, de-mergers, digital builds, consumer goods industry).\n\n"
        "A user has described a problem or idea. Your job:\n"
        "1. Assess feasibility: green = clear & worth doing, amber = viable but needs clarity, red = too vague\n"
        "2. Generate a full BOSCARD\n"
        "3. Suggest project type and starting phase (1=Initiate, 2=Plan & Design, 3=Execute, 4=Test, 5=Transition, 6=Close)\n\n"
        "Return ONLY valid JSON, no extra text:\n"
        "{\n"
        '  "feasibility": "green|amber|red",\n'
        '  "feasibility_reason": "2 sentence explanation",\n'
        '  "clarifying_questions": ["question if amber/red, else empty array"],\n'
        '  "project_name": "short descriptive project name",\n'
        '  "project_type": "one of: IT Transformation / Data / Cloud Migration / ERP Implementation / De-merger / Separation / Digital / App Build / Infrastructure Refresh / Process Improvement / Programme Management",\n'
        '  "suggested_phase": 1,\n'
        '  "boscard": "## Benefits\\n...\\n## Objectives\\n...\\n## Scope\\n**In scope:**...\\n**Out of scope:**...\\n## Constraints\\n...\\n## Assumptions\\n...\\n## Risks\\n...\\n## Dependencies\\n..."\n'
        "}\n\n"
        "Problem description:\n"
    )

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt + problem}],
                "temperature": 0.2,
                "max_tokens": 2500,
            },
            timeout=40,
        )
        if r.status_code != 200:
            return jsonify({"error": f"AI error {r.status_code}"}), 500
        raw = r.json()["choices"][0]["message"]["content"].strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        import json as _json
        parsed = _json.loads(raw)
        return jsonify(parsed)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nFuelWatch UK — SMS Service")
    print("=" * 40)
    print("Pre-loading station data...")
    get_stations()
    print("\nService running on http://localhost:5000")
    print("Webhook endpoint: http://localhost:5000/sms")
    print("\nTo expose publicly, run in another terminal:")
    print("  ngrok http 5000")
    print("\nThen set your Twilio webhook to:")
    print("  https://YOUR-NGROK-URL/sms\n")
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=False, host="0.0.0.0", port=port)
