"""'Is your business easy to find?' for the Factory page (humanagency.co/factory).

A local business owner gives their business name, postcode, website (optional) and a contact.
We run a quick check of the website straight away, save the request, and tell the owner of
Human Agency so they can send a fuller report by hand within 24 hours.

  POST /api/factory/business-check   {business, postcode, website, contact, name}
  GET  /factory/requests?key=...     list of requests (key = CRON_SECRET); &done=<id> marks one done

Saved in ma_details, type="factory_biz_check" (no new table).
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


def check_website(raw):
    """Quick public checks of a website. Never follows redirects to non-public hosts."""
    url = (raw or "").strip()
    if not url:
        return None
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    out = {"url": url, "loads": False, "secure": False, "phone_friendly": False, "title": "", "social": []}
    try:
        for _ in range(4):   # follow up to 3 redirects by hand, checking each host
            p = urlparse(url)
            if p.scheme not in ("http", "https") or not p.hostname or not _public_host(p.hostname):
                out["error"] = "That address can't be checked."
                return out
            r = requests.get(url, timeout=6, allow_redirects=False, stream=True,
                             headers={"User-Agent": "Mozilla/5.0 (compatible; HumanAgencyCheck/1.0)"})
            if r.is_redirect or r.status_code in (301, 302, 303, 307, 308):
                url = urljoin(url, r.headers.get("Location", ""))
                r.close()
                continue
            break
        body = r.raw.read(600_000, decode_content=True).decode(r.encoding or "utf-8", "ignore")
        r.close()
        out["url"] = url
        out["loads"] = r.status_code < 400
        out["secure"] = url.lower().startswith("https://")
        out["phone_friendly"] = bool(re.search(r'<meta[^>]+name=["\']viewport["\']', body, re.I))
        m = re.search(r"<title[^>]*>(.*?)</title>", body, re.I | re.S)
        out["title"] = html.unescape(re.sub(r"\s+", " ", m.group(1)).strip())[:120] if m else ""
        hrefs = " ".join(re.findall(r'href=["\']([^"\']+)["\']', body, re.I)).lower()
        out["social"] = [k for k, pat in SOCIAL.items() if re.search(pat, hrefs)]
    except Exception as e:
        out["error"] = "The website didn't respond."
        print(f"[biz-check] {url}: {str(e)[:120]}")
    return out


def _notify(rec):
    pc = rec.get("postcode", "")
    w = rec.get("website_check") or {}
    lines = [f"🏪 New business check: {rec['business']} ({pc})",
             f"Contact: {rec['contact']}" + (f" ({rec['name']})" if rec.get("name") else "")]
    if w:
        lines.append(f"Website: {w.get('url')} · loads {'yes' if w.get('loads') else 'no'} · "
                     f"secure {'yes' if w.get('secure') else 'no'} · phone {'yes' if w.get('phone_friendly') else 'no'} · "
                     f"social: {', '.join(w.get('social') or []) or 'none found'}")
    else:
        lines.append("No website given.")
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


def _cors(resp):
    if request.path == "/api/factory/business-check" and request.headers.get("Origin", "") == FACTORY_ORIGIN:
        resp.headers["Access-Control-Allow-Origin"] = FACTORY_ORIGIN
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Vary"] = "Origin"
    return resp


def register_factory_business(app, send_wa=None):
    global _send_wa
    _send_wa = send_wa
    app.after_request(_cors)

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
        ip = (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip()
        now = time.time()
        _recent[ip] = [t for t in _recent.get(ip, []) if now - t < 3600]
        if len(_recent[ip]) >= 5:
            return jsonify({"error": "Thanks, we've got your requests. Try again in an hour."}), 429
        _recent[ip].append(now)
        check = check_website(website) if website else None
        rec = {"business": business, "postcode": postcode, "contact": contact, "name": name,
               "website": website, "website_check": check, "status": "new",
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
                   f"phone {yes(w.get('phone_friendly'))} · social: {e(', '.join(w.get('social') or []) or 'none found')}") if w else "No website given"
            items.append(f"""<div class="r {'done' if d.get('status') == 'done' else ''}"><b>{e(d.get('business', ''))}</b> <small>{e(d.get('postcode', ''))} · {e(d.get('created', '')[:16].replace('T', ' '))}</small>
<div>Contact: {e(d.get('contact', ''))}{(' (' + e(d['name']) + ')') if d.get('name') else ''}</div><div>{web}</div>
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
