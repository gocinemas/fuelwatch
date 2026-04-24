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
from flask import Flask, request, send_file, render_template, jsonify, Response
from twilio.twiml.messaging_response import MessagingResponse
from search import (postcode_to_latlon, fetch_all_stations, haversine_km,
                    fetch_nearby_amenities, fetch_nearby_schools,
                    fetch_nearby_pubs, fetch_house_prices, fetch_local_amenities,
                    fetch_company_info, fetch_brand_data)
import analytics
import library as lib

app = Flask(__name__)

# Initialise analytics DB on startup
analytics.init_db()

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


@app.route("/")
def index():
    return render_template("index.html", prefill_company=None, prefill_doc=None)

@app.route("/doc/<share_id>")
def doc_page(share_id):
    return render_template("index.html", prefill_company=None, prefill_doc=share_id)

@app.route("/<company_slug>")
def company_page(company_slug):
    return render_template("index.html", prefill_company=company_slug.replace("-", " "), prefill_doc=None)


# ── Library API ───────────────────────────────────────────────────────────────

def _check_library_pin():
    """Return 401 response if PIN is wrong, else None."""
    pin = os.environ.get("LIBRARY_PIN", "")
    if not pin:
        return None  # no PIN set — open access
    supplied = request.headers.get("X-Library-PIN") or request.args.get("pin", "")
    if supplied != pin:
        return jsonify({"error": "PIN required", "auth": True}), 401
    return None


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
    text = chunks[0]["content"] if chunks else (doc.get("text_content") or "")[:800]
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

        # Use chunks already fetched by get_document — no extra DB call.
        # Score by keyword overlap, fall back to first N chunks, then to text_content.
        doc_chunks = doc.get("chunks", [])
        if doc_chunks:
            q_words = {w for w in question.lower().split() if len(w) > 2}
            if q_words:
                doc_chunks = sorted(doc_chunks,
                    key=lambda c: -sum(1 for w in q_words if w in c.get("content", "").lower()))
            _complex = any(w in question.lower() for w in ("compare", "summar", "all", "every", "list", "overview", "explain"))
            top_n = 6 if _complex else 3
            context = "\n\n---\n\n".join(c["content"] for c in doc_chunks[:top_n])
        else:
            context = (doc.get("text_content") or "")[:4000]

        system = (
            f'You are a helpful assistant. Answer questions ONLY based on the document "{doc["title"]}".\n\n'
            f'Document content:\n{context}\n\n'
            f'Answer concisely. If the answer is not in the document, say so briefly.'
        )
        messages = history[-6:] + [{"role": "user", "content": question}]
        answer = _groq_chat(system, messages, max_tokens=600)
        return jsonify({"answer": answer})
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
        answer = _groq_chat(system, messages, max_tokens=700)
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
    result = _resolve_postcode(request.args.get("postcode", ""))
    if not result:
        return jsonify({"error": "Postcode not found. Please check and try again."}), 404
    postcode, lat, lon, pc_fmt = result
    analytics.log_search("fuel", postcode, request.remote_addr, request.user_agent.string)

    fuel   = request.args.get("fuel", "petrol").lower()
    radius = float(request.args.get("radius", 5))
    if fuel not in ("petrol", "diesel"): fuel = "petrol"
    radius = min(max(radius, 1), 20)
    radius_km = radius * 1.60934
    stations = get_stations()

    nearby = []
    for s in stations:
        price = s.get(fuel)
        if not price or price <= 0: continue
        dist_km = haversine_km(lat, lon, s["lat"], s["lon"])
        if dist_km <= radius_km:
            nearby.append({**s, "dist_mi": round(dist_km / 1.60934, 2), "price": price})
    nearby.sort(key=lambda x: (x["price"], x["dist_mi"]))
    avg = round(sum(s["price"] for s in nearby) / len(nearby), 1) if nearby else 0

    weather = get_weather(lat, lon)
    rightmove_url = f"https://www.rightmove.co.uk/house-prices/{pc_fmt.lower().replace(' ', '-')}.html"

    return jsonify({
        "postcode": postcode, "pc_fmt": pc_fmt,
        "fuel": fuel, "radius": radius,
        "weather": weather,
        "stations": nearby[:10], "avg_price": avg,
        "rightmove_url": rightmove_url,
        "timestamp": datetime.now().isoformat(),
    })


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
    local = fetch_local_amenities(lat, lon, 3.0, 1.5)
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
                "url":      articles[0].get("link", "") if articles else "",
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


@app.route("/api/brand")
def api_brand():
    name = request.args.get("name", "").strip()
    if not name or len(name) < 2:
        return jsonify({"error": "Brand name required"}), 400
    analytics.log_search("brand", name, request.remote_addr, request.user_agent.string)
    return jsonify(fetch_brand_data(name))


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


def _groq_chat(system, messages, max_tokens=600, json_mode=False):
    """Call Groq API (OpenAI-compatible). Returns reply text."""
    body = {
        "model": "llama-3.3-70b-versatile",
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
    if not company:
        return jsonify({"error": "Company name required"}), 400

    try:
        from search import _fetch_news
        results_news = _fetch_news(
            company,
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
            max_tokens=600,
            json_mode=True,
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


@app.route("/api/product")
def api_product():
    import requests as _req, json as _json
    barcode = request.args.get("barcode", "").strip()
    name    = request.args.get("name", "").strip()
    debug   = request.args.get("debug") == "1"

    product = None
    _errors = []

    OFF_HEADERS = {
        "User-Agent": "MiruApp/1.0 (https://miru.humanagency.co; contact@humanagency.co)",
        "Accept": "application/json",
    }

    # --- barcode lookup via Open Food Facts ---
    if barcode:
        try:
            r = _req.get(f"https://world.openfoodfacts.org/api/v2/product/{barcode}.json",
                         timeout=10, headers=OFF_HEADERS)
            d = r.json()
            if d.get("status") == 1:
                p = d["product"]
                cats = p.get("categories", "")
                cat  = cats.split(",")[0].strip() if cats else ""
                product = {
                    "name":     p.get("product_name_en") or p.get("product_name", ""),
                    "brand":    p.get("brands", ""),
                    "category": cat,
                    "image":    p.get("image_url", ""),
                    "barcode":  barcode,
                }
            else:
                _errors.append(f"OFF barcode status={d.get('status')} http={r.status_code}")
        except Exception as e:
            _errors.append(f"OFF barcode exc: {e}")
            print(f"[product] barcode lookup error: {e}")

    # --- name search via Open Food Facts v2 search ---
    if not product and name:
        try:
            r = _req.get("https://world.openfoodfacts.org/cgi/search.pl",
                         params={"search_terms": name, "json": 1, "page_size": 5,
                                 "lc": "en", "action": "process"},
                         timeout=10, headers=OFF_HEADERS)
            _errors.append(f"OFF search http={r.status_code} len={len(r.text)}")
            products = r.json().get("products", [])
            if products:
                p = products[0]
                cats = p.get("categories", "")
                cat  = cats.split(",")[0].strip() if cats else ""
                product = {
                    "name":     p.get("product_name_en") or p.get("product_name", name),
                    "brand":    p.get("brands", ""),
                    "category": cat,
                    "image":    p.get("image_url", ""),
                }
            else:
                _errors.append(f"OFF search returned 0 products")
        except Exception as e:
            _errors.append(f"OFF search exc: {e}")
            print(f"[product] name search error: {e}")

    # --- AI alternatives via Groq ---
    alternatives = []
    search_term = (product["name"] if product else name) or barcode
    if search_term:
        try:
            from groq import Groq as _Groq
            gc = _Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
            brand_ctx = f" by {product['brand']}" if product and product.get("brand") else ""
            prompt = (
                f"Product: \"{search_term}\"{brand_ctx}.\n"
                "Suggest 3 alternative products available in UK supermarkets (Tesco, Sainsbury's, ASDA, Lidl, Aldi). "
                "For each give name, estimated UK price, and a one-line reason. "
                'Return ONLY a JSON array: [{"name":"...","price":"£X.XX","reason":"..."}]'
            )
            resp = gc.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=400, temperature=0.4,
            )
            raw = resp.choices[0].message.content.strip()
            _errors.append(f"groq_raw: {raw[:300]}")
            start, end = raw.find("["), raw.rfind("]")
            if start != -1 and end != -1:
                alternatives = _json.loads(raw[start:end+1])
            else:
                _errors.append(f"no JSON array found in groq response")
        except Exception as e:
            _errors.append(f"alternatives exc: {e}")
            print(f"[product] alternatives error: {e}")

    out = {"product": product, "alternatives": alternatives, "query": search_term}
    if debug:
        out["_errors"] = _errors
    return jsonify(out)


@app.route("/whatsapp", methods=["POST"])
def whatsapp_reply():
    body        = request.form.get("Body", "").strip()
    from_number = request.form.get("From", "unknown")
    print(f"WhatsApp from {from_number}: {body}")

    resp = MessagingResponse()

    if not body:
        resp.message("FuelWatch UK\nText your postcode to get fuel prices.\nExample: KT16 0DA\nOr: KT16 0DA diesel 10")
        return str(resp)

    postcode, fuel, radius, retailer = parse_sms(body)

    if not postcode:
        resp.message("FuelWatch UK\nCouldn't read that postcode.\nTry: KT16 0DA\nOr: KT16 0DA diesel 10")
        return str(resp)

    reply = search_and_format(postcode, fuel, radius, retailer)
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
