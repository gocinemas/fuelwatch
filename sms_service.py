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

import io
import json
import os
import re
import time
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from flask import Flask, request, send_file
from twilio.twiml.messaging_response import MessagingResponse
from search import postcode_to_latlon, fetch_all_stations, haversine_km, fetch_nearby_amenities

app = Flask(__name__)

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
_station_cache = {"data": [], "loaded_at": 0}
CACHE_TTL = 1800  # 30 minutes

def get_stations():
    now = time.time()
    if not _station_cache["data"] or (now - _station_cache["loaded_at"]) > CACHE_TTL:
        _station_cache["data"] = fetch_all_stations()
        _station_cache["loaded_at"] = now
        log_national_snapshot(_station_cache["data"])
    return _station_cache["data"]


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

def search_and_format(postcode: str, fuel: str, radius_miles: float, retailer: str = None) -> str:
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
        if retailer and retailer.lower() not in s.get("brand", "").lower():
            continue
        dist_km = haversine_km(lat, lon, s["lat"], s["lon"])
        if dist_km <= radius_km:
            nearby.append({**s, "dist_mi": dist_km / 1.60934, "price": price})

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


@app.route("/health")
def health():
    stations = get_stations()
    return {"status": "ok", "stations_loaded": len(stations)}


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
