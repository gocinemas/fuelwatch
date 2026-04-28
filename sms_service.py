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


def whatsapp_search_and_format(postcode: str, fuel: str, radius_miles: float, retailer: str = None) -> str:
    """Lean WhatsApp reply — fuel prices + weather only, no amenities fetch."""
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
              "", "Reply with postcode [diesel] [radius]"]
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
    "E07000212": "west-surrey",  # Runnymede
    "E07000209": "west-surrey",  # Guildford
    "E07000214": "west-surrey",  # Waverley
    "E07000215": "west-surrey",  # Woking
    "E07000210": "east-surrey",  # Mole Valley
    "E07000211": "east-surrey",  # Reigate and Banstead
    "E07000213": "east-surrey",  # Tandridge
    "E07000216": "east-surrey",  # Elmbridge
    "E07000217": "east-surrey",  # Surrey Heath
    "E07000207": "east-surrey",  # Epsom and Ewell
}
# New council slug → human-readable predecessor area list (for "no past results" message)
_COUNCIL_PREDECESSORS = {
    "west-surrey": ["Runnymede", "Guildford", "Waverley", "Woking"],
    "east-surrey": ["Mole Valley", "Reigate & Banstead", "Tandridge", "Elmbridge", "Epsom & Ewell"],
}
# Old county GSS → county-level council slug
_COUNTY_TO_COUNCIL_SLUG = {
    "E10000030": "surrey",
    "E10000012": "essex",
    "E10000020": "norfolk",
    "E10000029": "suffolk",
    "E10000013": "gloucestershire",
}
# Unitary authority GSS → council slug
_UA_TO_COUNCIL_SLUG = {
    "E06000042": "milton-keynes",
    "E06000030": "swindon",
    "E06000034": "thurrock",
}


_DC_RESULTS_CACHE: dict = {}

def _ward_to_slug(name: str) -> str:
    import re
    s = name.lower()
    s = re.sub(r"['’‘`]", "", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", "-", s.strip())

def _fetch_dc_results(council_slug: str, ward_name: str, election_date: str) -> list:
    """Fetch results for a ward from Democracy Club API. Returns [] if not available."""
    cache_key = f"dcr:{council_slug}:{ward_name}:{election_date}"
    if cache_key in _DC_RESULTS_CACHE:
        return _DC_RESULTS_CACHE[cache_key]
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
        # Pick best matching ward ballot
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
        results = []
        for c in candidacies:
            res = c.get("result") or {}
            results.append({
                "name":    c.get("person", {}).get("name", ""),
                "party":   c.get("party_name", ""),
                "elected": c.get("elected") or res.get("elected", False),
                "votes":   res.get("num_ballots"),
            })
        results.sort(key=lambda x: -(x["votes"] or 0))
        if results:
            _DC_RESULTS_CACHE[cache_key] = results
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
            if gss:
                if gss not in by_gss:
                    by_gss[gss] = {
                        "ward": row.get("post_label", "").strip(),
                        "council": row.get("organisation_name", "").strip().strip('"'),
                        "election_date": row.get("election_date", "").strip(),
                        "candidates": [],
                    }
                by_gss[gss]["candidates"].append(candidate)
            else:
                # Reorganised councils: parse council/ward slug from ballot_paper_id
                # e.g. local.west-surrey.chertsey.2026-05-07
                bp = row.get("ballot_paper_id", "")
                parts = bp.split(".")
                if len(parts) >= 4:
                    council_slug = parts[1]
                    ward_slug = parts[2]
                    by_slug.setdefault(council_slug, {})
                    if ward_slug not in by_slug[council_slug]:
                        by_slug[council_slug][ward_slug] = {
                            "ward": row.get("post_label", "").strip().strip('"'),
                            "council": row.get("organisation_name", "").strip().strip('"'),
                            "election_date": row.get("election_date", "").strip(),
                            "candidates": [],
                        }
                    by_slug[council_slug][ward_slug]["candidates"].append(candidate)
    return {"by_gss": by_gss, "by_slug": by_slug}


def _best_ward_match(ward_name: str, wards: dict) -> str | None:
    """Token-overlap match; weighted by token length so specific place names win ties."""
    tokens = set(re.sub(r"[^a-z0-9 ]", "", ward_name.lower()).split()) - {"and", "the", ""}
    best_slug, best_score = None, 0
    for slug, data in wards.items():
        wt = set(re.sub(r"[^a-z0-9 ]", "", data["ward"].lower()).split()) - {"and", "the", ""}
        st = set(slug.replace("-", " ").split())
        matched = tokens & (wt | st)
        score = sum(len(t) for t in matched)
        if score > best_score:
            best_score = score
            best_slug = slug
    return best_slug if best_score > 0 else None


_ELECTIONS_DATA = None

def _get_elections():
    global _ELECTIONS_DATA
    if _ELECTIONS_DATA is None:
        _ELECTIONS_DATA = _load_elections_csv()
    return _ELECTIONS_DATA


@app.route("/api/elections")
def api_elections():
    postcode = request.args.get("postcode", "").strip().replace(" ", "").upper()
    if not postcode:
        return jsonify({"error": "Postcode required"}), 400
    try:
        r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=6)
        if r.status_code != 200:
            return jsonify({"error": f"Could not find postcode {postcode}"}), 404
        result = r.json().get("result", {})
        codes        = result.get("codes", {})
        ward_gss     = codes.get("admin_ward", "")
        ward_name    = result.get("admin_ward", "")
        district     = result.get("admin_district", "")
        district_code = codes.get("admin_district", "")
        county_code  = codes.get("admin_county", "")
        ua_code      = codes.get("admin_district", "")  # unitaries use district code
        postcode_fmt = f"{postcode[:-3]} {postcode[-3:]}"

        elections = _get_elections()
        ward_data = elections["by_gss"].get(ward_gss, {})
        council_slug = None

        # Fallback: reorganised/merged council lookup by ward name token matching
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

        candidates = sorted(
            ward_data.get("candidates", []),
            key=lambda c: (c["party"], c["name"])
        )

        is_new_council = bool(council_slug and council_slug in _COUNCIL_PREDECESSORS)
        predecessors   = _COUNCIL_PREDECESSORS.get(council_slug or "", [])
        dc_results_url = (
            None if is_new_council else
            f"https://candidates.democracyclub.org.uk/elections/local.{district.lower().replace(' ','-')}.2022-05-05/"
        )

        # Fetch polling station + past/live results in parallel
        effective_council = (council_slug or district).lower().replace(" ", "-")
        effective_ward    = ward_data.get("ward") or ward_name
        election_date_str = ward_data.get("election_date", "2026-05-01")
        import datetime as _dt
        election_happened = _dt.date.today() >= _dt.date.fromisoformat(election_date_str)

        with ThreadPoolExecutor(max_workers=3) as ex:
            f_ps   = ex.submit(_fetch_polling_station, postcode)
            f_2022 = ex.submit(_fetch_dc_results, effective_council, effective_ward, "2022-05-05") \
                     if not is_new_council else None
            f_live = ex.submit(_fetch_dc_results, effective_council, effective_ward, election_date_str) \
                     if election_happened else None
            polling_station = f_ps.result()
            past_results    = f_2022.result() if f_2022 else []
            live_results    = f_live.result() if f_live else []

        county_name = result.get("admin_county") or ""

        return jsonify({
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
        })
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
    ])
    # Pubs/food at 5km — rural areas (e.g. Longcross) have pubs spread 3-5km out
    food_types = "cafe|restaurant|fast_food|pub|bar|fuel"
    food_radius = 5000
    leisure_types = "sports_centre|swimming_pool|fitness_centre|park|playground|attraction"
    query = f"""[out:json][timeout:25];
(
  node["amenity"~"^({services_types})$"](around:{radius},{lat},{lon});
  way["amenity"~"^({services_types})$"](around:{radius},{lat},{lon});
  node["amenity"~"^({food_types})$"](around:{food_radius},{lat},{lon});
  way["amenity"~"^({food_types})$"](around:{food_radius},{lat},{lon});
  node["leisure"~"^({leisure_types})$"](around:{radius},{lat},{lon});
  way["leisure"~"^({leisure_types})$"](around:{radius},{lat},{lon});
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

        kind = tags.get("amenity") or tags.get("leisure", "")
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
                "fields":   "name,rating,user_ratings_total,reviews,url",
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
            "reviews":       reviews,
        })
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
        img = Image.open(io.BytesIO(f.read())).convert("RGB")
        results = _pyzbar.decode(img)
        if results:
            return jsonify({"barcode": results[0].data.decode("utf-8"), "format": results[0].type})
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


_FUEL_WORDS = {"petrol", "diesel", "unleaded", "mile", "miles", "mi"} | {r.lower() for r in KNOWN_RETAILERS}
_ELECTION_WORDS = {"vote", "voting", "election", "elections", "candidate", "candidates",
                   "polling", "ballot", "stand", "standing", "mp", "councillor"}
_PLACES_WORDS = {"places", "services", "local", "near", "nearby", "around",
                 "library", "gp", "doctor", "pharmacy", "dentist", "leisure",
                 "gym", "pool", "community", "postoffice", "council", "park"}


def whatsapp_places_format(q: str) -> str:
    """Format local places info for WhatsApp reply."""
    try:
        geo = _geocode_place(q)
        if not geo or geo[0] is None:
            return f"Couldn't find '{q}'. Try a postcode or town name — e.g. places KT1 2BA"
        lat, lon, display = geo[0], geo[1], geo[2]
        places = _overpass_places(lat, lon)
        if not places:
            return f"No local services found near {display}. Try a different postcode or area."

        # Group by category, pick top 1-2 per category, cap total at 12 entries
        from itertools import groupby
        lines = [f"🏛️ Local services near {display}\n"]
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
                    # Simplify OSM hours for SMS
                    import re as _re
                    hours_short = _re.sub(r'[A-Z][a-z]-[A-Z][a-z]', lambda m: m.group(), hours)
                    line += f"\n  🕐 {hours_short}"
                if phone:
                    line += f"\n  📞 {phone}"
                lines.append(line)
                count += 1

        lines.append(f"\n📍 Within 900m · miru.app")
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
        )
    except Exception as e:
        return f"Sorry, couldn't load election info. Try miru.app instead."

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
    return "\n".join(lines)


@app.route("/whatsapp", methods=["POST"])
def whatsapp_reply():
    body        = request.form.get("Body", "").strip()
    from_number = request.form.get("From", "unknown")
    print(f"WhatsApp from {from_number}: {body}")

    resp = MessagingResponse()

    if not body:
        resp.message(
            "Miru 🇬🇧\n"
            "⛽ Fuel prices: SW1A 1AA\n"
            "🛒 Grocery prices: Heinz Beans\n"
            "🗳️ Elections: vote SW1A 1AA\n"
            "🏛️ Local services: places SW1A 1AA"
        )
        return str(resp)

    postcode, fuel, radius, retailer = parse_sms(body)
    body_words = set(body.lower().split())

    # ── Places query ──────────────────────────────────────────────────────────
    if body_words & _PLACES_WORDS:
        # Extract postcode or strip trigger word to get place name
        pc_m = re.search(r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body.upper())
        if pc_m:
            places_q = pc_m.group(1).strip()
        else:
            places_q = re.sub(
                r'\b(?:places|services|local|near|nearby|around)\b', '', body, flags=re.I
            ).strip()
        if places_q:
            cache_key = f"places_wa:{places_q.lower()}"
            cached = _WA_CACHE.get(cache_key)
            if cached and (time.time() - cached[0]) < _WA_CACHE_TTL:
                resp.message(cached[1])
                return str(resp)
            reply = whatsapp_places_format(places_q)
            _WA_CACHE[cache_key] = (time.time(), reply)
            resp.message(reply)
            return str(resp)
        else:
            resp.message("Please include a postcode or place name, e.g.:\nplaces KT1 2BA")
            return str(resp)

    # ── Elections query ────────────────────────────────────────────────────────
    if body_words & _ELECTION_WORDS:
        pc_m = re.search(r'([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})', body.upper())
        elec_postcode = pc_m.group(1).replace(" ", "") if pc_m else None
        if elec_postcode:
            cache_key = f"elections:{elec_postcode}"
            cached = _WA_CACHE.get(cache_key)
            if cached and (time.time() - cached[0]) < _WA_CACHE_TTL:
                resp.message(cached[1])
                return str(resp)
            reply = whatsapp_elections_format(elec_postcode)
            _WA_CACHE[cache_key] = (time.time(), reply)
            resp.message(reply)
            return str(resp)
        else:
            resp.message("Please include your postcode, e.g.:\nvote KT16 0DA")
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
        _get_elections_data()
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


@app.route("/api/books")
def api_books():
    """Proxy Open Library book search — avoids client-side CORS issues."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"docs": []})
    try:
        r = requests.get(
            "https://openlibrary.org/search.json",
            params={"q": q, "limit": 8, "fields": "key,title,author_name,isbn,cover_i,first_publish_year"},
            timeout=10,
        )
        data = r.json()
        return jsonify({"docs": data.get("docs", [])})
    except Exception as e:
        return jsonify({"error": str(e), "docs": []})


@app.route("/api/book/isbn/<isbn>")
def api_book_isbn(isbn):
    """Fetch full book detail (description, rating, subjects) by ISBN via Open Library."""
    isbn = isbn.strip()
    try:
        r = requests.get(
            f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data",
            timeout=10,
        )
        d = r.json()
        bk = d.get(f"ISBN:{isbn}")
        if not bk:
            return jsonify({"found": False})

        cover = ""
        if bk.get("cover"):
            cover = bk["cover"].get("medium") or bk["cover"].get("small") or ""

        description = ""
        community_rating = None
        raw_subjects = bk.get("subjects", [])
        subjects = " · ".join(
            (s.get("name") if isinstance(s, dict) else s) for s in raw_subjects[:6]
        )

        work_key = (bk.get("works") or [{}])[0].get("key")
        if work_key:
            try:
                wr = requests.get(f"https://openlibrary.org{work_key}.json", timeout=6)
                wd = wr.json()
                desc = wd.get("description", "")
                description = desc if isinstance(desc, str) else (desc.get("value", "") if isinstance(desc, dict) else "")
                if not subjects and wd.get("subjects"):
                    subjects = " · ".join(str(s) for s in wd["subjects"][:6])
            except Exception:
                pass
            try:
                rr = requests.get(f"https://openlibrary.org{work_key}/ratings.json", timeout=6)
                rd = rr.json()
                avg = rd.get("summary", {}).get("average")
                if avg:
                    community_rating = {"avg": round(float(avg), 1), "count": rd["summary"].get("count", 0)}
            except Exception:
                pass

        if not description:
            notes = bk.get("notes", "")
            description = notes if isinstance(notes, str) else (notes.get("value", "") if isinstance(notes, dict) else "")

        return jsonify({
            "found":           True,
            "isbn":            isbn,
            "title":           bk.get("title", ""),
            "author":          ", ".join(a.get("name", "") for a in bk.get("authors", [])) or "Unknown author",
            "cover":           cover,
            "description":     description,
            "subjects":        subjects,
            "communityRating": community_rating,
            "year":            bk.get("publish_date", ""),
            "publishers":      ", ".join(p.get("name", "") for p in bk.get("publishers", [])),
        })
    except Exception as e:
        return jsonify({"error": str(e), "found": False})


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
