#!/usr/bin/env python3
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

import os
import re
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
from search import postcode_to_latlon, fetch_all_stations, haversine_km, fetch_nearby_amenities

app = Flask(__name__)

# ── Cache stations in memory (refresh every 30 min) ───────────────────────────
import time
_station_cache = {"data": [], "loaded_at": 0}
CACHE_TTL = 1800  # 30 minutes

def get_stations():
    now = time.time()
    if not _station_cache["data"] or (now - _station_cache["loaded_at"]) > CACHE_TTL:
        _station_cache["data"] = fetch_all_stations()
        _station_cache["loaded_at"] = now
    return _station_cache["data"]


# ── SMS Parser ────────────────────────────────────────────────────────────────

def parse_sms(body: str):
    """
    Parse incoming SMS into (postcode, fuel, radius_miles).
    Examples:
      "KT16 0DA"           -> ("KT160DA", "petrol", 5.0)
      "KT160DA diesel"     -> ("KT160DA", "diesel", 5.0)
      "KT16 0DA petrol 10" -> ("KT160DA", "petrol", 10.0)
    """
    body = body.strip().upper()

    # Extract UK postcode (handles spaced and non-spaced)
    postcode_match = re.search(
        r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body
    )
    if not postcode_match:
        return None, None, None

    postcode = postcode_match.group(1).replace(" ", "")

    fuel = "diesel" if "DIESEL" in body else "petrol"

    radius_match = re.search(r'\b(\d+)\s*(?:MILE|MI|MILES)?\b', body.replace(postcode, ""))
    radius = float(radius_match.group(1)) if radius_match else 5.0
    radius = min(max(radius, 1), 20)  # clamp between 1–20 miles

    return postcode, fuel, radius


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

def get_weather(lat: float, lon: float) -> str:
    """Fetch current weather from Open-Meteo (free, no API key)."""
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,weathercode,windspeed_10m"
            f"&timezone=Europe/London"
        )
        r = requests.get(url, timeout=5)
        c = r.json()["current"]
        temp    = round(c["temperature_2m"])
        code    = c["weathercode"]
        wind    = round(c["windspeed_10m"])
        desc    = WEATHER_CODES.get(code, "")
        return f"{temp}°C {desc}, Wind {wind}km/h"
    except Exception:
        return ""


# ── Search & Format ───────────────────────────────────────────────────────────

def search_and_format(postcode: str, fuel: str, radius_miles: float) -> str:
    """Run search and format result as a concise SMS reply."""

    latlon = postcode_to_latlon(postcode)
    if not latlon:
        return f"Sorry, couldn't find postcode {postcode}. Please check and try again."

    lat, lon = latlon
    radius_km = radius_miles * 1.60934
    stations = get_stations()

    nearby = []
    for s in stations:
        price = s.get(fuel)
        if not price or price <= 0:
            continue
        dist_km = haversine_km(lat, lon, s["lat"], s["lon"])
        if dist_km <= radius_km:
            nearby.append({**s, "dist_mi": dist_km / 1.60934, "price": price})

    if not nearby:
        return (
            f"No {fuel} stations found within {radius_miles:.0f} miles of {postcode}.\n"
            f"Try: {postcode} {fuel} 10"
        )

    nearby.sort(key=lambda x: (x["price"], x["dist_mi"]))
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
    lines += [
        f"-- {fuel_label} near {postcode} --",
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
        for s in amenities["supermarkets"][:3]:
            lines.append(f"  {s['name']}{s['rating']} ({s['dist_mi']:.1f}mi)")
    if amenities["cafes"]:
        lines += ["", "-- Coffee --"]
        for c in amenities["cafes"][:3]:
            lines.append(f"  {c['name']}{c['rating']} ({c['dist_mi']:.1f}mi)")

    return "\n".join(l for l in lines if l is not None)


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

    postcode, fuel, radius = parse_sms(body)

    if not postcode:
        resp.message(
            "FuelWatch UK\nCouldn't read that postcode.\n"
            "Try: KT16 0DA\nOr: KT16 0DA diesel 10"
        )
        return str(resp)

    reply = search_and_format(postcode, fuel, radius)
    resp.message(reply)
    return str(resp)


@app.route("/health")
def health():
    stations = get_stations()
    return {"status": "ok", "stations_loaded": len(stations)}


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
