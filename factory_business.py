"""'Is your business easy to find?' for the Factory page (humanagency.co/factory).

Step 1, instant and anonymous: business name + postcode (+ website) -> Google Maps listing, rating,
reviews, website checks, social links, a score and the top fixes.
Step 2, only if they want help: they leave WhatsApp or email -> saved, and the owner is told.

  POST /api/factory/business-find    {business, postcode, lat?, lng?} -> {candidates: [{id, name, address}]}  (pick yours)
  POST /api/factory/business-scan    {business, postcode, website, place_id?}  -> {google, website, score, out_of, fixes}
  POST /api/factory/business-check   {business, postcode, website, contact, name, scan}
  GET  /factory/requests?key=...     list of requests (key = CRON_SECRET); &done=<id> marks one done

Saved in ma_details, type="factory_biz_check" (no new table).
Google lookups use GOOGLE_PLACES_KEY (or GOOGLE_API_KEY), capped per day by FACTORY_GOOGLE_DAILY_CAP
(default 100), so a busy day can't run up a bill.
Notifications (optional, set on Railway):
  OWNER_WHATSAPP  e.g. +447700900123   new requests sent here on WhatsApp
  OWNER_EMAIL     new requests emailed here (uses RESEND_API_KEY)
"""
import html
import ipaddress
import json
import os
import re
import socket
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from flask import Response, jsonify, request

TYPE = "factory_biz_check"
FACTORY_ORIGIN = "https://humanagency.co"
SOCIAL = {
    "instagram": r"instagram\.com/",
    "facebook": r"facebook\.com/|fb\.com/",
    "tiktok": r"tiktok\.com/@",
    "x": r"(?:twitter|x)\.com/",
    "linkedin": r"linkedin\.com/",
    "youtube": r"youtube\.com/",
    "google": r"g\.page/|maps\.app\.goo\.gl/|google\.[a-z.]+/maps",
}
_recent = {}   # ip -> [timestamps], simple limit so the form can't be used to spam


def _sb():
    import library
    return library._sb()


def _public_host(host):
    """True only if every address the host resolves to is public (no localhost or internal network)."""
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        return False
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            return False
    return True


AI_BOTS = {"GPTBot": "ChatGPT", "OAI-SearchBot": "ChatGPT search", "ChatGPT-User": "ChatGPT",
           "ClaudeBot": "Claude", "anthropic-ai": "Claude", "Claude-Web": "Claude",
           "PerplexityBot": "Perplexity", "Google-Extended": "Google Gemini"}
BUSINESS_TYPES = re.compile(r"LocalBusiness|Organization|Store|Restaurant|Cafe|Bakery|Shop|Salon|Service|Hotel|BarOrPub|Dentist|"
                            r"Physician|Plumber|Electrician|Contractor|Professional|FoodEstablishment|Brewery|Winery|Florist|Gym|"
                            r"HealthAndBeauty|AutoRepair|Library|School|Bookstore|Corporation", re.I)


def _fetch(url, limit=600_000):
    """GET a public URL, following up to 3 redirects by hand and checking every host. Returns (url, status, text)."""
    for _ in range(4):
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.hostname or not _public_host(p.hostname):
            raise ValueError("not public")
        r = requests.get(url, timeout=6, allow_redirects=False, stream=True,
                         headers={"User-Agent": "Mozilla/5.0 (compatible; HumanAgencyCheck/1.0)"})
        if r.is_redirect or r.status_code in (301, 302, 303, 307, 308):
            url = urljoin(url, r.headers.get("Location", ""))
            r.close()
            continue
        body = r.raw.read(limit, decode_content=True).decode(r.encoding or "utf-8", "ignore")
        r.close()
        return url, r.status_code, body
    raise ValueError("too many redirects")


def _ai_blocked(robots):
    """Names of AI assistants a robots.txt shuts out of the whole site."""
    blocked, agents, in_rules = set(), [], False
    for raw in (robots or "").splitlines():
        line = raw.split("#", 1)[0].strip()
        if ":" not in line:
            continue
        k, v = [x.strip() for x in line.split(":", 1)]
        k = k.lower()
        if k == "user-agent":
            if in_rules:
                agents, in_rules = [], False
            agents.append(v)
        elif k in ("disallow", "allow"):
            in_rules = True
            if k == "disallow" and v == "/":
                for a in agents:
                    if a == "*":
                        blocked.update(AI_BOTS.keys())
                    for bot in AI_BOTS:
                        if a.lower() == bot.lower():
                            blocked.add(bot)
    return sorted({AI_BOTS[b] for b in blocked})


def check_website(raw):
    """Quick public checks of a website. Never follows redirects to non-public hosts."""
    url = (raw or "").strip()
    if not url:
        return None
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    out = {"url": url, "loads": False, "secure": False, "phone_friendly": False, "title": "", "social": [],
           "llms_txt": False, "ai_blocked": [], "schema": False}
    try:
        try:
            url, status, body = _fetch(url)
        except ValueError:
            out["error"] = "That address can't be checked."
            return out
        out["url"] = url
        out["loads"] = status < 400
        out["secure"] = url.lower().startswith("https://")
        out["phone_friendly"] = bool(re.search(r'<meta[^>]+name=["\']viewport["\']', body, re.I))
        m = re.search(r"<title[^>]*>(.*?)</title>", body, re.I | re.S)
        out["title"] = html.unescape(re.sub(r"\s+", " ", m.group(1)).strip())[:120] if m else ""
        hrefs = " ".join(re.findall(r'href=["\']([^"\']+)["\']', body, re.I)).lower()
        out["social"] = [k for k, pat in SOCIAL.items() if re.search(pat, hrefs)]
        # Can AI assistants read and understand this business?
        out["schema"] = any(BUSINESS_TYPES.search(block) for block in
                            re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', body, re.I | re.S))
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        try:
            _, st, txt = _fetch(base + "/llms.txt", 40_000)
            out["llms_txt"] = st == 200 and len(txt.strip()) > 20 and not txt.lstrip().startswith("<")
        except Exception:
            pass
        try:
            _, st, txt = _fetch(base + "/robots.txt", 60_000)
            if st == 200 and not txt.lstrip().startswith("<"):
                out["ai_blocked"] = _ai_blocked(txt)
        except Exception:
            pass
    except Exception as e:
        out["error"] = "The website didn't respond."
        print(f"[biz-check] {url}: {str(e)[:120]}")
    return out


_gcount = {"day": "", "n": 0}


PLACE_FIELDS = ["displayName", "formattedAddress", "websiteUri", "rating", "userRatingCount", "googleMapsUri", "businessStatus",
                "regularOpeningHours.weekdayDescriptions", "nationalPhoneNumber", "pureServiceAreaBusiness",
                "primaryType"]
PLACE_ID = re.compile(r"^[A-Za-z0-9_-]{10,300}$")


def _gkey_ok():
    """The Places key, or None when it isn't set or today's cap is used up (counts one call)."""
    key = os.environ.get("GOOGLE_PLACES_KEY") or os.environ.get("GOOGLE_API_KEY", "")
    if not key:
        return None, None
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _gcount["day"] != day:
        _gcount.update(day=day, n=0)
    if _gcount["n"] >= int(os.environ.get("FACTORY_GOOGLE_DAILY_CAP", "100") or 100):
        return None, {"skipped": "daily limit"}
    _gcount["n"] += 1
    return key, None


def google_candidates(business, postcode="", lat=None, lng=None):
    """Up to 5 Google Maps listings matching the name near the postcode (or the visitor's location),
    so the owner can pick theirs instead of us guessing. Cheap fields only."""
    key, skip = _gkey_ok()
    if not key:
        return skip or {"skipped": "not set up"}
    body = {"textQuery": f"{business} {postcode}".strip(), "regionCode": "GB", "maxResultCount": 5}
    if lat is not None and lng is not None:
        body["locationBias"] = {"circle": {"center": {"latitude": lat, "longitude": lng}, "radius": 3000.0}}
    try:
        r = requests.post("https://places.googleapis.com/v1/places:searchText", timeout=8, json=body,
                          headers={"X-Goog-Api-Key": key, "Content-Type": "application/json",
                                   "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress"})
        places = (r.json() or {}).get("places") or []
    except Exception as e:
        print(f"[biz-find] google: {str(e)[:120]}")
        return {"skipped": "error"}
    return [{"id": p.get("id"), "name": (p.get("displayName") or {}).get("text", ""), "address": p.get("formattedAddress", "")}
            for p in places if p.get("id")]


def _listing(p, name):
    return {"found": True, "name": name, "address": p.get("formattedAddress", ""),
            "rating": p.get("rating"), "reviews": p.get("userRatingCount") or 0,
            "website": p.get("websiteUri", ""), "maps_url": p.get("googleMapsUri", ""),
            "phone": bool(p.get("nationalPhoneNumber")), "hours": bool((p.get("regularOpeningHours") or {}).get("weekdayDescriptions")),
            "open": p.get("businessStatus", "") in ("", "OPERATIONAL"),
            # No premises customers visit (mobile hairdresser, plumber, cleaner): Google lists the area served instead
            "mobile": bool(p.get("pureServiceAreaBusiness")),
            # Google's main category (bakery, hair_care, plumber...), so the page can talk about what matters to that kind of business
            "kind": p.get("primaryType", "")}


def google_listing(business, postcode, place_id=""):
    """The business's Google Maps listing. With place_id (picked by the owner) it's exact; otherwise one search
    by name and postcode, counted only if the name looks like theirs. None if not set up."""
    key, skip = _gkey_ok()
    if not key:
        return skip
    try:
        if place_id:
            r = requests.get(f"https://places.googleapis.com/v1/places/{place_id}", timeout=8,
                             headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": ",".join(PLACE_FIELDS)})
            p = r.json() or {}
            if r.status_code != 200 or not p.get("displayName"):
                return {"found": False}
            return dict(_listing(p, (p.get("displayName") or {}).get("text", "")), picked=True)
        r = requests.post("https://places.googleapis.com/v1/places:searchText", timeout=8,
                          headers={"X-Goog-Api-Key": key, "Content-Type": "application/json",
                                   "X-Goog-FieldMask": ",".join("places." + f for f in PLACE_FIELDS)},
                          json={"textQuery": f"{business} {postcode}".strip(), "regionCode": "GB", "maxResultCount": 1})
        places = (r.json() or {}).get("places") or []
    except Exception as e:
        print(f"[biz-scan] google: {str(e)[:120]}")
        return None
    if not places:
        return {"found": False}
    p = places[0]
    name = (p.get("displayName") or {}).get("text", "")
    # Only count it if the name looks like theirs (Google returns the nearest match even when it isn't)
    words = [w for w in re.findall(r"[a-z0-9]+", business.lower()) if len(w) > 2 and w not in ("the", "and", "ltd", "limited")]
    if words and not any(w in name.lower() for w in words):
        return {"found": False, "nearest": name}
    return _listing(p, name)


MOBILE_WORDS = re.compile(r"\bmobile\b|\bat home\b|\bcomes? to you\b", re.I)


def is_mobile(g, business=""):
    """True for a business that goes to its customers rather than having premises they visit."""
    return bool(g and g.get("mobile")) or bool(MOBILE_WORDS.search(business or ""))


def score_and_fixes(g, w, mobile=False):
    """Plain-English score out of 8 and the fixes that matter most, most important first.
    mobile: the business goes to its customers, so no till, no shop hours, no map to a front door."""
    checks, fixes = [], []
    def add(ok, fix, weight):
        checks.append(bool(ok))
        if not ok:
            fixes.append((weight, fix))
    on_google = bool(g and g.get("found"))
    if g is not None and not g.get("skipped"):
        add(on_google, "Claim your free Google Business Profile. It's what puts you on Google Maps and in local Google search.", 10)
    if on_google:
        n = g.get("reviews") or 0
        add(n >= 20, f"Get more Google reviews (you have {n}). Send happy customers your review link by text or WhatsApp straight after each visit." if mobile
            else f"Get more Google reviews (you have {n}). A QR code by the till is an easy way to ask happy customers.", 8)
        add(g.get("hours"), "Add the days and hours you work to Google, and the areas you cover, so people know when you can come to them." if mobile
            else "Add your opening hours to Google, so people don't turn up when you're closed.", 7)
        add(g.get("website"), "Add your website to your Google listing.", 6)
    has_site = bool(w and w.get("loads"))
    add(has_site, "Get a simple one-page website: what you do, prices, the areas you cover and a link to book." if mobile
        else "Get a simple one-page website: what you do, opening hours, a map and how to contact you.", 9)
    if has_site:
        add(w.get("phone_friendly"), "Make your website work on phones. Many people will look you up on one.", 7)
        add(w.get("secure"), "Switch your website to https. Browsers mark sites without it as \"Not secure\".", 5)
        soc = w.get("social") or []
        add(any(s in soc for s in ("instagram", "facebook", "tiktok")), "Link your Instagram, Facebook or TikTok from your website, so people can see what you're up to.", 4)
        # Can AI assistants (ChatGPT, Claude, Copilot, Gemini) find and describe you?
        blocked = w.get("ai_blocked") or []
        add(not blocked, f"Your website tells AI assistants ({', '.join(blocked)}) not to read it. Unless that's on purpose, allow them, or they can't use your site when answering questions.", 8)
        add(w.get("schema"), "Add your business details in a standard format (schema.org), so search engines and AI tools can read your name, the areas you cover, hours and what you do." if mobile
            else "Add your business details in a standard format (schema.org), so search engines and AI tools can read your name, address, hours and what you do.", 6)
        # llms.txt is reported but not scored: AI tools don't reliably read it yet.
    fixes.sort(key=lambda x: -x[0])
    return sum(checks), len(checks), [f for _, f in fixes[:3]]


def _notify(rec):
    pc = rec.get("postcode", "")
    w = rec.get("website_check") or {}
    wants = {"fix": "fixes from the business check", "ai-setup": "help setting up AI", "ai-visibility": "help getting found by AI assistants"}
    lines = [f"🏪 New request: {rec['business']} ({pc}), wants {wants.get(rec.get('interest'), rec.get('interest') or 'help')}",
             f"Contact: {rec['contact']}" + (f" ({rec['name']})" if rec.get("name") else "")]
    if w:
        lines.append(f"Website: {w.get('url')} · loads {'yes' if w.get('loads') else 'no'} · "
                     f"secure {'yes' if w.get('secure') else 'no'} · phone {'yes' if w.get('phone_friendly') else 'no'} · "
                     f"social: {', '.join(w.get('social') or []) or 'none found'}")
    else:
        lines.append("No website given.")
    g = (rec.get("scan") or {}).get("google") or {}
    if g.get("found"):
        lines.append(f"Google: {g.get('name')} · {g.get('rating') or '-'}★ from {g.get('reviews') or 0} reviews · {g.get('maps_url', '')}")
    elif g:
        lines.append("Google: not found on Google Maps")
    sc = rec.get("scan") or {}
    if sc.get("out_of"):
        lines.append(f"Score: {sc.get('score')}/{sc.get('out_of')}. Fixes: " + " | ".join(sc.get("fixes") or []))
    text = "\n".join(lines)
    wa = os.environ.get("OWNER_WHATSAPP", "").strip()
    if wa and _send_wa:
        try:
            _send_wa("whatsapp:" + wa, text + "\n\nAll requests: miru.humanagency.co/factory/requests")
        except Exception as e:
            print(f"[biz-check] WhatsApp notify failed: {e}")
    to, key = os.environ.get("OWNER_EMAIL", "").strip(), os.environ.get("RESEND_API_KEY", "")
    if to and key:
        try:
            requests.post("https://api.resend.com/emails",
                          headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                          json={"from": "Factory <reports@mekalav.com>", "to": [to],
                                "subject": f"Business check request: {rec['business']}",
                                "html": "<pre style='font:14px/1.5 sans-serif'>" + html.escape(text) + "</pre>"},
                          timeout=10)
        except Exception as e:
            print(f"[biz-check] email notify failed: {e}")


_send_wa = None


def _scan_line(sc):
    if not sc:
        return ""
    e = html.escape
    g = sc.get("google") or {}
    gl = (f"Google: <a target=_blank href='{e(g.get('maps_url', ''))}'>{e(g.get('name', ''))}</a> · {e(str(g.get('rating') or '-'))}★ from {e(str(g.get('reviews') or 0))} reviews"
          if g.get("found") else ("Google: not on Google Maps" if g else ""))
    fx = "".join(f"<li>{e(f)}</li>" for f in sc.get("fixes") or [])
    return f"{gl}<br>Score {e(str(sc.get('score')))}/{e(str(sc.get('out_of')))}<ol style='margin:4px 0 0'>{fx}</ol>"


def _cors(resp):
    if request.path in ("/api/factory/business-check", "/api/factory/business-scan", "/api/factory/business-find") and request.headers.get("Origin", "") == FACTORY_ORIGIN:
        resp.headers["Access-Control-Allow-Origin"] = FACTORY_ORIGIN
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Vary"] = "Origin"
    return resp


def register_factory_business(app, send_wa=None):
    global _send_wa
    _send_wa = send_wa
    app.after_request(_cors)

    def _limited(bucket, n):
        ip = (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip()
        now = time.time()
        k = bucket + ":" + ip
        _recent[k] = [t for t in _recent.get(k, []) if now - t < 3600]
        if len(_recent[k]) >= n:
            return True
        _recent[k].append(now)
        return False

    @app.route("/api/factory/business-find", methods=["POST"])
    def factory_business_find():
        """Which listing is theirs? Up to 5 nearby matches to pick from before the check runs."""
        b = request.get_json(silent=True) or {}
        business = str(b.get("business") or "").strip()[:100]
        postcode = str(b.get("postcode") or "").strip().upper()[:10]
        try:
            lat, lng = float(b["lat"]), float(b["lng"])
            if not (49 < lat < 61 and -9 < lng < 3):
                lat = lng = None
        except (KeyError, TypeError, ValueError):
            lat = lng = None
        if len(business) < 2:
            return jsonify({"error": "Tell us the business name."}), 400
        if _limited("find", 30):
            return jsonify({"error": "That's a lot of searches. Try again in an hour."}), 429
        c = google_candidates(business, postcode, lat, lng)
        return jsonify({"candidates": c if isinstance(c, list) else [], "skipped": None if isinstance(c, list) else c.get("skipped")})

    @app.route("/api/factory/business-scan", methods=["POST"])
    def factory_business_scan():
        b = request.get_json(silent=True) or {}
        business = str(b.get("business") or "").strip()[:100]
        postcode = str(b.get("postcode") or "").strip().upper()[:10]
        website = str(b.get("website") or "").strip()[:200]
        place_id = str(b.get("place_id") or "").strip()
        place_id = place_id if PLACE_ID.match(place_id) else ""
        if len(business) < 2:
            return jsonify({"error": "Tell us the business name."}), 400
        if _limited("scan", 10):
            return jsonify({"error": "That's a lot of checks. Try again in an hour."}), 429
        g = google_listing(business, postcode, place_id)
        if not website and g and g.get("found") and g.get("website"):
            website = g["website"]
        w = check_website(website) if website else None
        mobile = is_mobile(g, business)
        score, out_of, fixes = score_and_fixes(g, w, mobile)
        try:
            _sb().table("ma_details").insert({"device_id": "scan:" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6],
                                              "type": "factory_biz_scan", "label": business[:60],
                                              "data": {"business": business, "postcode": postcode, "score": score, "out_of": out_of,
                                                       "on_google": bool(g and g.get("found")), "has_site": bool(w and w.get("loads")),
                                                       "created": datetime.now(timezone.utc).isoformat(timespec="seconds")}}).execute()
        except Exception as e:
            print(f"[biz-scan] log failed: {e}")
        return jsonify({"google": g, "website": w, "score": score, "out_of": out_of, "fixes": fixes, "mobile": mobile})

    @app.route("/api/factory/business-check", methods=["POST"])
    def factory_business_check():
        b = request.get_json(silent=True) or {}
        business = str(b.get("business") or "").strip()[:100]
        postcode = str(b.get("postcode") or "").strip().upper()[:10]
        contact = str(b.get("contact") or "").strip()[:120]
        name = str(b.get("name") or "").strip()[:60]
        website = str(b.get("website") or "").strip()[:200]
        if len(business) < 2:
            return jsonify({"error": "Tell us the business name."}), 400
        if not (re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", contact) or len(re.sub(r"\D", "", contact)) >= 10):
            return jsonify({"error": "Add a mobile number or email so we can send your report."}), 400
        if _limited("check", 5):
            return jsonify({"error": "Thanks, we've got your requests. Try again in an hour."}), 429
        scan = b.get("scan") if isinstance(b.get("scan"), dict) else None
        check = (scan or {}).get("website") or (check_website(website) if website else None)
        if scan:
            scan = {"google": scan.get("google"), "score": scan.get("score"), "out_of": scan.get("out_of"),
                    "fixes": [str(f)[:240] for f in (scan.get("fixes") or [])[:3]]}
        interest = str(b.get("interest") or "fix")[:20]
        rec = {"business": business, "postcode": postcode, "contact": contact, "name": name, "interest": interest,
               "website": website, "website_check": check, "scan": scan, "status": "new",
               "created": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        rid = "biz:" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
        try:
            _sb().table("ma_details").insert({"device_id": rid, "type": TYPE, "label": business[:60], "data": rec}).execute()
        except Exception as e:
            print(f"[biz-check] save failed: {e}")
            return jsonify({"error": "Couldn't save that right now. Please try again."}), 500
        _notify(rec)
        return jsonify({"ok": True, "website_check": check})

    @app.route("/factory/requests", methods=["GET"])
    def factory_requests_page():
        secret = os.environ.get("CRON_SECRET", "")
        if not secret or request.args.get("key", "") != secret:
            return jsonify({"error": "Unauthorized"}), 401
        done = request.args.get("done", "")
        rows = _sb().table("ma_details").select("id,device_id,data").eq("type", TYPE).execute().data or []
        if done:
            for r in rows:
                if r["device_id"] == done:
                    d = dict(r["data"] or {}); d["status"] = "done"
                    _sb().table("ma_details").update({"data": d}).eq("id", r["id"]).execute()
                    r["data"] = d
        rows.sort(key=lambda r: r["device_id"], reverse=True)
        e = html.escape
        yes = lambda v: "✅" if v else "❌"
        items = []
        for r in rows:
            d = r.get("data") or {}
            w = d.get("website_check") or {}
            q = f"{d.get('business', '')} {d.get('postcode', '')}"
            web = (f"<a href='{e(w.get('url', ''))}' target=_blank>{e(w.get('url', ''))}</a> · loads {yes(w.get('loads'))} secure {yes(w.get('secure'))} "
                   f"phone {yes(w.get('phone_friendly'))} · social: {e(', '.join(w.get('social') or []) or 'none found')}"
                   f"<br>AI: llms.txt {yes(w.get('llms_txt'))} schema {yes(w.get('schema'))} · blocks: {e(', '.join(w.get('ai_blocked') or []) or 'none')}") if w else "No website given"
            items.append(f"""<div class="r {'done' if d.get('status') == 'done' else ''}"><b>{e(d.get('business', ''))}</b> <small>{e(d.get('postcode', ''))} · {e(d.get('created', '')[:16].replace('T', ' '))}</small>
<div>Wants: <b>{e({"fix": "fixes", "ai-setup": "AI setup help", "ai-visibility": "AI visibility"}.get(d.get('interest') or 'fix', d.get('interest') or ''))}</b></div>
<div>Contact: {e(d.get('contact', ''))}{(' (' + e(d['name']) + ')') if d.get('name') else ''}</div><div>{web}</div>
<div>{_scan_line(d.get('scan'))}</div>
<div class="a"><a target=_blank href="https://www.google.com/search?q={e(requests.utils.quote(q))}">Google</a>
<a target=_blank href="https://www.google.com/maps/search/{e(requests.utils.quote(q))}">Maps</a>
<a target=_blank href="https://www.instagram.com/explore/search/keyword/?q={e(requests.utils.quote(d.get('business', '')))}">Instagram</a>
<a target=_blank href="https://www.tiktok.com/search?q={e(requests.utils.quote(q))}">TikTok</a>
{'' if d.get('status') == 'done' else f"<a href='?key={e(secret)}&done={e(r['device_id'])}'>Mark done</a>"}</div></div>""")
        page = f"""<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Business check requests</title>
<style>body{{font:15px/1.45 system-ui,sans-serif;max-width:760px;margin:30px auto;padding:0 16px;background:#fff8f1;color:#1d1630}}
.r{{background:#fff;border:2px solid #1d1630;border-radius:14px;padding:12px 14px;margin:0 0 12px}}.r.done{{opacity:.5}}
small{{color:#6c6485}}.a a{{margin-right:12px;font-weight:700;color:#7c4dff}}</style>
<h1>Business check requests</h1><p>{len(rows)} so far, {sum(1 for r in rows if (r.get('data') or {}).get('status') != 'done')} to do.</p>{''.join(items) or '<p>None yet.</p>'}"""
        return Response(page, mimetype="text/html")
