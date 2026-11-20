"""
Bin collection lookup for any UK postcode, using each council's own checker.

  GET /api/bins/lookup?postcode=KT16 0DA   -> council, and either the address list or the council's link
  GET /api/bins/dates?postcode=..&uprn=..  -> collection dates, where a council connector supports it

Steps:
  1. postcodes.io gives the council for the postcode (any UK postcode).
  2. If we have a connector for that council, use its own checker's steps.
  3. Otherwise return the council's own bin page link.
Results are cached for 24 hours. Nothing about the person is stored.
"""
import os
import re
import time
import uuid
from urllib.parse import quote

import requests
from flask import jsonify, request

CACHE_SECONDS = 24 * 3600
# Off by default. Only turned on after WhenIsBins agree to commercial use (see hello@whenisbins.com).
WIB_ENABLED = os.environ.get("BINS_WHENISBINS_ENABLED", "") == "1"
WIB_BASE = "https://whenisbins.com/v1"
WIB_AGENT = "factory-humanagency"
_cache = {}
UA = "Factory-bin-lookup/1.0 (humanagency.co)"


def _cached(key, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]
    value = fn()
    _cache[key] = (time.time(), value)
    return value


def resolve_council(postcode):
    pc = re.sub(r"\s+", "", postcode or "").upper()
    def fetch():
        r = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=10)
        if r.status_code != 200:
            return None
        res = r.json().get("result") or {}
        return {"name": res.get("admin_district"), "code": (res.get("codes") or {}).get("admin_district")}
    return _cached("council:" + pc, fetch)


# ── Connectors: one per council, using that council's own checker ──

def _runnymede_addresses(postcode):
    url = "https://www.runnymede.gov.uk/homepage/150/check-your-bin-collection-day"
    r = requests.get(url, params={"postcode": postcode, "submit": "Find"},
                     headers={"User-Agent": UA}, timeout=20)
    html = r.text
    start = html.find('id="address"')
    if start < 0:
        return []
    block = html[start:html.find("</select>", start)]
    opts = re.findall(r'<option\s+value="(\d+)"\s*>\s*([^<]+?)\s*</option>', block, flags=re.S)
    return [{"id": v, "label": re.sub(r"\s+", " ", l).strip()} for v, l in opts]


def _runnymede_dates(uprn):
    """Same request the council's page makes when an address is chosen."""
    from datetime import datetime
    url = "https://www.runnymede.gov.uk/homepage/150/check-your-bin-collection-day"
    r = requests.get(url, params={"address": uprn, "submit": "Find"},
                     headers={"User-Agent": UA}, timeout=20)
    html = r.text
    rows = re.findall(r"<tr>\s*<td>(?:<span[^>]*></span>)?\s*([^<]+?)\s*</td>\s*<td>\s*([^<]+?)\s*</td>", html, flags=re.S)
    collections = []
    for service, when in rows:
        m = re.search(r"(\d{1,2} \w+ \d{4})", when)
        if not m:
            continue
        try:
            iso = datetime.strptime(m.group(1), "%d %B %Y").date().isoformat()
        except ValueError:
            continue
        collections.append({"bin": service.strip(), "date": iso})
    return {"collections": collections,
            "source": "Runnymede Borough Council bin collection checker",
            "checked": datetime.now().date().isoformat()}


def _wib_addresses(postcode):
    r = requests.get(f"{WIB_BASE}/addresses", params={"postcode": postcode},
                     headers={"X-Agent": WIB_AGENT, "User-Agent": UA}, timeout=20)
    if r.status_code != 200:
        return None, None
    d = r.json()
    council = (d.get("council") or {}).get("name")
    addresses = [{"id": c["id"], "label": c["label"]} for c in (d.get("candidates") or [])]
    return council, addresses


def _wib_dates(postcode, property_id):
    """Creates one lookup, waits for it to finish, and returns collections with the council's source link."""
    from datetime import datetime
    headers = {"X-Agent": WIB_AGENT, "User-Agent": UA, "Idempotency-Key": str(uuid.uuid4())}
    r = requests.post(f"{WIB_BASE}/lookups", json={"postcode": postcode, "property_id": property_id},
                      headers=headers, timeout=30)
    if r.status_code not in (200, 201):
        return None
    lk = r.json()
    lookup_id = lk.get("id")
    deadline = time.time() + 120
    while lk.get("status") in ("queued", "running", "partial") and time.time() < deadline:
        time.sleep(5)
        g = requests.get(f"{WIB_BASE}/lookups/{lookup_id}", headers=headers, timeout=20)
        if g.status_code != 200:
            break
        lk = g.json()
    res = lk.get("result") or {}
    if lk.get("status") != "done" or not res:
        return None
    collections = []
    for c in res.get("collections", []):
        for d in c.get("dates", []):
            collections.append({"bin": c.get("name"), "date": d})
    collections.sort(key=lambda x: x["date"])
    council = res.get("council") or {}
    return {"collections": collections,
            "source": (council.get("name") or "Council") + " (via WhenIsBins)",
            "council_page": council.get("lookup_url") or "",  # fallback set in the route
            "checked": datetime.now().date().isoformat()}


CONNECTORS = {
    "E07000212": {  # Runnymede
        "name": "Runnymede",
        "addresses": _runnymede_addresses,
        "dates": _runnymede_dates,
        "page": "https://www.runnymede.gov.uk/homepage/150/check-your-bin-collection-day",
    },
}


def _gov_link(postcode):
    return "https://www.gov.uk/find-local-council?postcode=" + quote(postcode)


def lookup(postcode):
    if WIB_ENABLED:
        name, addresses = _cached("wib-addr:" + postcode, lambda: _wib_addresses(postcode))
        if addresses:
            return {"postcode": postcode, "council": name, "status": "addresses",
                    "addresses": addresses, "council_page": _gov_link(postcode)}, 200
    council = resolve_council(postcode)
    if not council or not council.get("code"):
        return {"error": "We could not find that postcode."}, 404
    conn = CONNECTORS.get(council["code"])
    if conn:
        addresses = _cached("addr:" + postcode, lambda: conn["addresses"](postcode))
        if addresses:
            return {"postcode": postcode, "council": council["name"], "status": "addresses",
                    "addresses": addresses, "council_page": conn["page"]}, 200
    return {"postcode": postcode, "council": council["name"], "status": "link",
            "council_page": (conn or {}).get("page") or _gov_link(postcode),
            "message": f"Check {council['name']} council's own bin checker for your collection days."}, 200


def register_bins_lookup(app):
    @app.route("/api/bins/lookup")
    def api_bins_lookup():
        pc = (request.args.get("postcode") or "").strip()
        if not pc:
            return jsonify({"error": "Enter a postcode."}), 400
        body, code = lookup(pc)
        return jsonify(body), code

    @app.route("/api/bins/dates")
    def api_bins_dates():
        pc = (request.args.get("postcode") or "").strip()
        uprn = (request.args.get("uprn") or "").strip()
        if WIB_ENABLED and uprn.startswith("p:") and pc:
            out = _cached("wib-dates:" + uprn, lambda: _wib_dates(pc, uprn))
            if out:
                out["council_page"] = out.get("council_page") or _gov_link(pc)
                return jsonify(out), 200
            return jsonify({"error": "Collection days are not available for this address right now.",
                            "council_page": _gov_link(pc)}), 501
        if not pc or not uprn.isdigit():
            return jsonify({"error": "Choose your address first."}), 400
        council = resolve_council(pc) or {}
        conn = CONNECTORS.get(council.get("code"))
        if not conn or not conn.get("dates"):
            return jsonify({"error": "Dates for this council are not connected yet.",
                            "council_page": (conn or {}).get("page") or _gov_link(pc)}), 501
        return jsonify(conn["dates"](uprn)), 200
