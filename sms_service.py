#!/usr/bin/env python3
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
                    fetch_company_info, fetch_brand_data)
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
    return render_template("index.html", prefill_company=None, prefill_doc=None)

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

@app.route("/<company_slug>")
def company_page(company_slug):
    return render_template("index.html", prefill_company=company_slug.replace("-", " "), prefill_doc=None)


# ── Library API ───────────────────────────────────────────────────────────────

def _check_library_pin():
    """Return 401 if wrong password, else None. Uses ADMIN_PASSWORD env var."""
    pw = os.environ.get("ADMIN_PASSWORD", "")
    if not pw:
        return None  # no password set — open access
    supplied = (request.headers.get("X-Library-PIN")
                or request.headers.get("X-Admin-Password")
                or request.args.get("pin", "")
                or request.args.get("pw", ""))
    if supplied != pw:
        return jsonify({"error": "Password required", "auth": True}), 401
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
        # Dev mode — open access, try to resolve user token anyway
        from_number = _resolve_user_token(supplied) if supplied else None
        return from_number, None
    if supplied == admin_pw:
        return None, None  # admin, unrestricted
    if supplied:
        from_number = _resolve_user_token(supplied)
        if from_number:
            return from_number, None  # valid user token
    return None, (jsonify({"error": "Password required", "auth": True}), 401)


@app.route("/api/library/documents")
def api_library_list():
    err = _check_library_pin()
    if err: return err
    try:
        docs = lib.list_documents()
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
        else:
            text = request.form.get("text", "").strip()
            doc_type = "note"

        if not text:
            return jsonify({"error": "No content found"}), 400

        doc = lib.upload_document(title, text, doc_type, page_count)
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
    """Slower endpoint: schools, pubs, cafes from Overpass (cached 1hr)."""
    result = _resolve_postcode(request.args.get("postcode", ""))
    if not result:
        return jsonify({"error": "Postcode not found."}), 404
    postcode, lat, lon, pc_fmt = result
    analytics.log_search("area", postcode, request.remote_addr, request.user_agent.string)
    local = fetch_local_amenities(lat, lon, 5.0, 5.0)
    return jsonify({
        "schools":      {"schools": local.get("schools", []), "universities": local.get("universities", [])},
        "pubs":         local.get("pubs", []),
        "cafes":        local.get("cafes", []),
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
            _KAGI_CAT_IDS = {c["categoryId"]: c["id"] for c in cats}
            _KAGI_CAT_IDS_TS = _time.time()
            print(f"[kagi] refreshed {len(_KAGI_CAT_IDS)} category IDs")
        except Exception as e:
            print(f"[kagi] category lookup failed: {e}")
    return _KAGI_CAT_IDS.get(slug, "")

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
        if not cat_id:
            raise ValueError(f"No category ID found for slug '{slug}'")
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


@app.route("/api/brand")
def api_brand():
    name = request.args.get("name", "").strip()
    if not name or len(name) < 2:
        return jsonify({"error": "Brand name required"}), 400
    analytics.log_search("brand", name, request.remote_addr, request.user_agent.string)
    return jsonify(fetch_brand_data(name))


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
            headers={"User-Agent": "Miru/1.0"},
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
            headers={"User-Agent": "Miru/1.0"},
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
                    by_gss[gss] = {
                        "ward": row.get("post_label", "").strip(),
                        "council": row.get("organisation_name", "").strip().strip('"'),
                        "election_date": row.get("election_date", "").strip(),
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
        return _cors(jsonify({"profiles": [
            {
                "id":            p["id"],
                "child_name":    p.get("child_name", ""),
                "school_name":   p.get("school_name", ""),
                "class_name":    p.get("class_name", ""),
                "teacher_name":  p.get("teacher_name", ""),
                "sender_emails": p.get("sender_emails") or [],
            }
            for p in profiles
        ]}))
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
            kwargs={"days_back": 3, "force": True, "profile_ids": profile_ids},
            daemon=True,
        ).start()
        return _cors(jsonify({"ok": True, "started": True}))
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
            timeout=10, headers={"User-Agent": "Miru/1.0"},
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
                headers={"User-Agent": "Miru/1.0"},
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
                r2 = requests.get(url, params={"format": "json"}, timeout=6, headers={"User-Agent": "Miru/1.0"})
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
            headers={"User-Agent": "Miru/1.0"},
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
            headers={"User-Agent": "Miru/1.0"},
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
                        headers={"User-Agent": "Miru/1.0"},
                    )
                    if pr.status_code == 200:
                        pd = pr.json()
                        email = pd.get("email", "") or ""
                        for ident in (pd.get("identifiers") or []):
                            if ident.get("value_type") == "email" and not email:
                                email = ident.get("value", "")
                        photo_url = ((pd.get("image") or {}).get("image_url") or "")
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
                timeout=6, headers={"User-Agent": "Miru/1.0"},
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
    email = phone = website = twitter = ""
    try:
        cr = requests.get(
            f"https://members-api.parliament.uk/api/Members/{mp_id}/Contact",
            headers={"Accept": "application/json", "User-Agent": "Miru/1.0"},
            timeout=8,
        )
        if cr.ok:
            for c in cr.json().get("value", []):
                if not email   and c.get("email"):                              email   = c["email"]
                if not phone   and c.get("phone"):                              phone   = c["phone"]
                if not website and (c.get("line1") or "").startswith("http"):   website = c["line1"]
                if not twitter and "twitter" in (c.get("type") or "").lower():  twitter = c.get("line1", "").lstrip("@")
    except Exception:
        pass
    return {"email": email, "phone": phone, "website": website, "twitter": twitter}


def _seed_mps_to_db() -> int:
    """Paginate Parliament Members API and bulk-insert all MPs into Supabase. Returns count."""
    rows = []
    skip = 0
    while True:
        try:
            r = requests.get(
                "https://members-api.parliament.uk/api/Members/Search",
                params={"House": 1, "IsCurrentMember": True, "take": 20, "skip": skip},
                headers={"Accept": "application/json", "User-Agent": "Miru/1.0"},
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

        # Step 3: fetch contacts in background if not yet stored (avoids blocking gunicorn worker)
        if not row.get("contacts_fetched"):
            mp_id   = row["mp_id"]
            con_key = constituency.lower()
            def _bg_contacts():
                contacts = _fetch_contacts(mp_id)
                try:
                    lib._sb().table("mps").update({**contacts, "contacts_fetched": True}) \
                        .eq("constituency", con_key).execute()
                    with _mp_mem_lock:
                        _mp_mem[con_key] = {**_mp_mem.get(con_key, row), **contacts, "contacts_fetched": True}
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
                    timeout=6, headers={"User-Agent": "Miru/1.0"},
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
                headers={"User-Agent": "Miru/1.0"},
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
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

# Simple 30-minute in-memory cache for Overpass results keyed by postcode
_services_cache: dict = {}
_SERVICES_TTL = 21600  # 6 hours — hospitals/supermarkets/police don't move

def _overpass_mirrors(query):
    """POST to Overpass with mirror fallback. Use for hospitals/supermarkets."""
    for url in _OVERPASS_URLS:
        try:
            r = requests.post(url, data={"data": query}, timeout=18)
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
            tags.get("contact:phone") or tags.get("contact:telephone") or "").strip()

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

def _fetch_hospitals(lat, lon):
    items = _places_nearby(lat, lon, "hospital", 15000, 4)
    # strip place_id from output
    for h in items:
        h.pop("place_id", None)
        h["phone"] = ""
    return items

def _fetch_supermarkets_overpass(lat, lon, radius_m=5000):
    query = f"""
[out:json][timeout:25];
(
  node["shop"="supermarket"](around:{radius_m},{lat},{lon});
  way["shop"="supermarket"](around:{radius_m},{lat},{lon});
  node["shop"="grocery"](around:{radius_m},{lat},{lon});
  way["shop"="grocery"](around:{radius_m},{lat},{lon});
);
out center;
"""
    try:
        elements = _overpass_mirrors(query)
        items = []
        for el in elements:
            tags = el.get("tags", {})
            name = tags.get("name") or tags.get("brand") or ""
            if not name:
                continue
            elat, elon = _el_coords(el)
            if not elat or not elon:
                continue
            dist = round(haversine_km(lat, lon, elat, elon), 2)
            address = _el_address(tags)
            items.append({"name": name, "address": address, "distance_km": dist})
        items.sort(key=lambda x: x["distance_km"])
        return items[:10]
    except Exception as e:
        print(f"[overpass supermarkets] {e}")
        return []

def _fetch_supermarkets(lat, lon):
    items = _places_nearby(lat, lon, "supermarket", 10000, 10)
    for s in items:
        s.pop("place_id", None)
    if not items:
        items = _fetch_supermarkets_overpass(lat, lon)
    return items


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
    if not postcode:
        return jsonify({"error": "Postcode required"}), 400
    try:
        cache_key = f"shops:{postcode}"
        hit = _services_cache.get(cache_key)
        if hit and time.time() - hit["ts"] < _SERVICES_TTL:
            return jsonify(hit["data"])
        lat, lon = _latlon_for_postcode(postcode)
        if lat is None:
            return jsonify({"error": "Postcode not found"}), 404
        supermarkets = _fetch_supermarkets(lat, lon)
        result = {"supermarkets": supermarkets}
        if supermarkets:  # only cache if we got results
            _services_cache[cache_key] = {"data": result, "ts": time.time()}
        return jsonify(result)
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
            headers={"User-Agent": "Miru/1.0"},
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
        "dentist", "pharmacy", "post_office", "townhall", "social_facility",
        "food_bank", "police", "fire_station", "leisure_centre",
        "veterinary", "bank",
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
    """Stable per-user token derived from phone number + server secret."""
    secret = os.environ.get("DIGEST_TOKEN", "miru-secret")
    token = hmac.new(secret.encode(), from_number.encode(), hashlib.sha256).hexdigest()[:20]
    _TOKEN_TO_NUMBER[token] = from_number
    return token

def _resolve_user_token(token: str):
    """Return from_number for a user token, or None. Populates cache from DB on cold start."""
    if token in _TOKEN_TO_NUMBER:
        return _TOKEN_TO_NUMBER[token]
    try:
        rows = lib._sb().table("wa_saves").select("from_number").execute().data
        seen = set()
        for row in rows:
            n = row.get("from_number", "")
            if n and n not in seen:
                seen.add(n)
                _wa_user_token(n)  # populates _TOKEN_TO_NUMBER
        return _TOKEN_TO_NUMBER.get(token)
    except Exception:
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

    def _bg_safe():
        try:
            _bg()
        except Exception as _bge:
            app.logger.error(f"[vision] background thread crashed: {_bge}", exc_info=True)
    threading.Thread(target=_bg_safe, daemon=True).start()
    return "📷 Got it — reading your photo now…"


def _wa_send_proactive(to: str, body: str) -> None:
    """Send an outbound WhatsApp message via Twilio (fire-and-forget)."""
    try:
        from twilio.rest import Client as _TC
        sid   = os.environ.get("TWILIO_ACCOUNT_SID", "")
        token = os.environ.get("TWILIO_AUTH_TOKEN", "")
        frm   = os.environ.get("TWILIO_WHATSAPP_FROM", "")
        if sid and token and frm:
            _TC(sid, token).messages.create(
                body=body, from_=f"whatsapp:{frm}", to=to
            )
    except Exception:
        pass


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
    "gp":         ["doctor", "gp", "surgery", "medical centre", "health centre"],
    "doctor":     ["doctor", "gp", "surgery", "medical"],
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
        if not places:
            return f"No local services found near {display}. Try a different postcode or area."

        # Apply service filter if provided
        sf = service_filter.lower().strip()
        if sf:
            # Find synonyms for the filter keyword
            filter_words = None
            for key, synonyms in _SERVICE_SYNONYMS.items():
                if key in sf:
                    filter_words = synonyms
                    break
            if filter_words is None:
                filter_words = [w for w in sf.split() if len(w) > 2]

            if filter_words:
                filtered = [
                    p for p in places
                    if any(fw in p.get("name", "").lower() or fw in p.get("category", "").lower()
                           for fw in filter_words)
                ]
                if filtered:
                    places = filtered
                # else fall through to show everything

        from itertools import groupby
        header = f"🏛️ {sf.title() if sf else 'Local services'} near {display}\n" if sf else f"🏛️ Local services near {display}\n"
        lines = [header]
        count = 0
        for cat, group in groupby(places, key=lambda p: p["category"]):
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


def _split_product_postcode(body: str):
    """Return (product_name, postcode_or_None) from a freeform message."""
    m = re.search(r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body.upper())
    if not m:
        return body.strip(), None
    postcode = m.group(1).replace(" ", "")
    remaining = (body[:m.start()] + " " + body[m.end():]).strip()
    return remaining, postcode


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
    lines.append("🔗 miru.humanagency.co")
    return "\n".join(lines)


_HELP_MSG = (
    "Miru 🇬🇧 — your UK assistant\n"
    "\n"
    "⛽ *Fuel Prices*\n"
    "Send your postcode:\n"
    "  e.g. KT1 2BA\n"
    "  e.g. petrol KT1 2BA\n"
    "\n"
    "🗳️ *Elections & Voting*\n"
    "Send vote + your postcode:\n"
    "  e.g. vote KT1 2BA\n"
    "\n"
    "🏛️ *Local Services*\n"
    "Send places + postcode or service:\n"
    "  e.g. places KT1 2BA\n"
    "  e.g. GP near KT1 2BA\n"
    "\n"
    "🔖 *Save for Later*\n"
    "Send any article URL to save it.\n"
    "Then:\n"
    "  LIST — see pending saves\n"
    "  1 READ · 2 SKIP · 3 REMIND Monday\n"
    "  (You'll also get a daily digest)\n"
    "\n"
    "Reply *HELP* anytime for this menu"
)

_GREETING_WORDS = {"hi", "hello", "hey", "start", "help", "menu", "miru", "join"}


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
            _USER_LAST_LOCATION[from_number] = {
                "lat": float(_lat), "lon": float(_lon), "ts": time.time()
            }
            resp.message("📍 Got your location — I'll use it to add context when you send me a photo. It expires in 2 hours.")
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
        resp.message(_HELP_MSG)
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
        resp.message("FuelWatch UK 🇬🇧\nCouldn't read that.\nFor fuel: SW1A 1AA\nFor prices: Heinz Baked Beans")
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


@app.route("/api/wa-digest")
def wa_digest():
    """Send weekly digest of pending saves to each user via WhatsApp.
    Trigger weekly via cron-job.org: GET /api/wa-digest?token=YOUR_DIGEST_TOKEN
    """
    token = request.args.get("token", "")
    if not token or token != os.environ.get("DIGEST_TOKEN", ""):
        return jsonify({"error": "Unauthorized"}), 401

    try:
        # Include pending + remind (overdue reminders resurface in digest)
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
    sent = 0

    for from_number, saves in by_user.items():
        # Cap at 9 for numbered triage replies (1–9)
        batch = saves[:9]
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

    return jsonify({"sent": sent, "total_users": len(by_user)})


# ── School web signup ──────────────────────────────────────────────────────────

@app.route("/school/signup")
def school_signup_page():
    prefill_wa = request.args.get("wa", "")
    return render_template("school_signup.html", prefill_wa=prefill_wa)


_SCHOOL_OAUTH_REDIRECT = "https://miru.humanagency.co/school/oauth/callback"
_SCHOOL_OAUTH_SCOPES   = "https://www.googleapis.com/auth/gmail.readonly"

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
            {"gmail_refresh_token": refresh_token}
        ).eq("id", profile_id).execute()
    except Exception as e:
        print(f"[school oauth] db error: {e}")
        return redirect("/?screen=school&oauth_error=3")

    import threading
    threading.Thread(
        target=school_service.poll_all_profiles,
        kwargs={"days_back": 30, "force": False, "profile_ids": [profile_id]},
        daemon=True,
    ).start()

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
        kwargs={"days_back": days_back, "force": force},
        daemon=True,
    ).start()
    return jsonify({"status": "started", "days_back": days_back, "force": force})


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
        filter_number = _resolve_user_token(pin)
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
            q = q.eq("from_number", filter_number)
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
            q = q.eq("from_number", from_number)  # users can only update their own
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
            q = q.eq("from_number", from_number)  # users can only delete their own
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
                q = q.eq("from_number", from_number)
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
            base = base.eq("from_number", from_number)
        try:
            rows = base.text_search("fts", q, config="english").order("created_at", desc=True).limit(20).execute().data
        except Exception:
            # fts column not yet created — fall back to ilike on both fields
            def _qi(field):
                b = sb.table("wa_saves").select("id,title,url,summary,status,created_at,remind_day")
                if from_number:
                    b = b.eq("from_number", from_number)
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


@app.route("/api/wa-saves/add", methods=["POST"])
def api_wa_saves_add():
    """Manually save a URL from the web UI."""
    from_number, err = _check_saves_pin()
    if err:
        return err
    data = request.json or {}
    url = (data.get("url") or "").strip()
    if not url or not url.startswith("http"):
        return jsonify({"error": "Valid URL required"}), 400
    # Use user's from_number if known, otherwise tag as "web"
    save_as = from_number or "web"
    result = _wa_save_url(save_as, url)
    return jsonify({"ok": True, "message": result})


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

    return jsonify({"ok": True})


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

    best, best_dist = None, float("inf")
    for s in _STATION_CACHE.values():
        if s.get("lat") and s.get("lon"):
            d = (s["lat"] - user_lat) ** 2 + (s["lon"] - user_lon) ** 2
            if d < best_dist:
                best_dist, best = d, s
    if best:
        return jsonify({"name": best["name"], "crs": best["crs"], "lat": best["lat"], "lng": best["lon"]})
    return jsonify({"error": "No station found"}), 404


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

    if not os.environ.get("RTT_TOKEN"):
        return jsonify({"error": "Train API not configured — set RTT_TOKEN environment variable (free at api-portal.rtt.io)"}), 503

    # 30-second departures cache — train data doesn't change faster than this
    cached = _rtt_departures_cache.get(crs)
    if cached and time.time() - cached[1] < _RTT_DEPARTURES_TTL:
        return jsonify(cached[0])

    try:
        access = _get_rtt_token()

        r = requests.get(
            "https://data.rtt.io/rtt/location",
            headers={"Authorization": f"Bearer {access}"},
            params={"code": f"gb-nr:{crs}"},
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
        _rtt_departures_cache[crs] = (payload, time.time())
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
        text = r.text
        ct = r.headers.get("Content-Type", "")

        # If HTML page, discover RSS link
        if "html" in ct.lower() and not any(tag in text[:800] for tag in ("<rss", "<feed", "<?xml")):
            m = _re.search(
                r'<link[^>]+type=["\']application/(?:rss|atom)\+xml["\'][^>]+href=["\']([^"\']+)["\']'
                r'|<link[^>]+href=["\']([^"\']+)["\'][^>]+type=["\']application/(?:rss|atom)\+xml["\']',
                text, _re.I
            )
            if m:
                rss_url = m.group(1) or m.group(2)
                rss_url = _urljoin(r.url, rss_url)
                r2 = requests.get(rss_url, timeout=10, headers=hdrs)
                r2.raise_for_status()
                text = r2.text
            else:
                return {"url": url, "source": url, "articles": [], "error": "No RSS feed found on this page"}

        root = _ET.fromstring(text.encode("utf-8"))
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
