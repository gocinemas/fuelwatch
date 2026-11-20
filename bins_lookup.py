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
import re
import time
from urllib.parse import quote

import requests
from flask import jsonify, request

CACHE_SECONDS = 24 * 3600
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


CONNECTORS = {
    "E07000212": {  # Runnymede
        "name": "Runnymede",
        "addresses": _runnymede_addresses,
        "dates": None,  # not built yet: needs the council's dates step
        "page": "https://www.runnymede.gov.uk/homepage/150/check-your-bin-collection-day",
    },
}


def _gov_link(postcode):
    return "https://www.gov.uk/find-local-council?postcode=" + quote(postcode)


def lookup(postcode):
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
        if not pc or not uprn.isdigit():
            return jsonify({"error": "Choose your address first."}), 400
        council = resolve_council(pc) or {}
        conn = CONNECTORS.get(council.get("code"))
        if not conn or not conn.get("dates"):
            return jsonify({"error": "Dates for this council are not connected yet.",
                            "council_page": (conn or {}).get("page") or _gov_link(pc)}), 501
        return jsonify(conn["dates"](uprn)), 200
