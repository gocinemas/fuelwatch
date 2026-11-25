"""Local leads: draft a first message to a local business, written to fit the channel.

  POST /api/factory/lead-intro
       {lead: {name, type, town, rating, reviews, site, gaps[]}, channel, me, offer, note, tweak, draft}
       -> {message, subject}

channel: instagram | facebook | whatsapp | email | call | comment
note:    something the sender noticed about them (an award, a post), used as the opener
tweak:   "shorter" | "warmer" | "bolder" | "" to rework the previous draft
Assisted, not automatic: it only drafts. The sender reads, edits and sends it themselves.
Uses ANTHROPIC_API_KEY.

  GET /api/factory/company-type?name=&pc=  -> {status: company | not_found | unknown, ...}
Sole traders and some partnerships count as individuals under PECR, so cold emails, texts and DMs to them
need consent. Local leads uses this to suggest a call or a public comment instead. 30 drafts an hour per visitor, LEAD_INTRO_DAILY_CAP a day (default 150).
"""
import json
import os
import re
import time
from datetime import datetime, timezone

from flask import jsonify, request

from factory_business import FACTORY_ORIGIN

PATH = "/api/factory/lead-intro"
MODEL = os.getenv("LEAD_INTRO_MODEL", "claude-sonnet-5-5")
DAILY_CAP = int(os.getenv("LEAD_INTRO_DAILY_CAP", "150") or 150)

CHANNELS = {
    "instagram": "an Instagram DM. 40 to 70 words. No subject line. Casual and friendly, like a real person in the DMs, no sign-off block",
    "facebook": "a Facebook Messenger message to their business page. 40 to 70 words. No subject line. Friendly and casual",
    "whatsapp": "a WhatsApp message. 40 to 70 words. No subject line. Friendly, easy to reply to with one line",
    "email": "a short email. 80 to 120 words, plus a subject line under 8 words that's specific to them, not salesy",
    "call": "notes for a short phone call, written as what to say out loud: a 2-sentence opener saying who you are and why you're calling, "
            "the one thing you noticed, and a question. 60 to 90 words. No subject line. Natural spoken English",
    "comment": "a public comment on one of their social media posts. 15 to 35 words. A genuine, specific compliment or observation about their work "
               "or the post the sender noted. No pitch, no link, no mention of what the sender sells or of the free check. No subject line",
}
TWEAKS = {
    "shorter": "Make it noticeably shorter, keeping the specific detail and the ask.",
    "warmer": "Make it warmer and more personal, less businesslike, still short.",
    "bolder": "Make it more direct and confident about the problem and the fix, still friendly, never pushy.",
}

RULES = """You help a local consultant write a first message to a small local business. They will read, edit and send it themselves.
Write like a real person from the area, not a marketer:
- Plain British English, contractions, short sentences. No em dashes or en dashes. No emoji unless the channel is Instagram, and then at most one.
- Never use: "I hope this finds you well", "I came across", "reach out", "touch base", "leverage", "elevate", "game-changer", "unlock", "boost your online presence", "digital footprint", "solutions".
- Open with something specific about them: the note from the sender if there is one, otherwise what they do and where. Be genuine, not gushing.
- Mention ONE or TWO concrete things from the check, and say in plain words why it matters for THIS kind of business
  (e.g. for a mobile hairdresser, bookings over DM eat into time with clients; for a plumber, people pick from Google Maps reviews in an emergency).
- Offer something small and easy to say yes to (a quick free look, a 15-minute chat, fixing one thing). One question at the end they can answer in a line.
- Use ONLY the facts given. Never invent numbers, awards, competitors or results. If a fact is missing, leave it out.
- Don't claim to be a company or agency. Say "I". Mention being local if a town is given.
Reply with JSON only: {"subject": "...or empty", "message": "..."}"""

_recent, _day = {}, {"date": "", "count": 0}
CH_PATH = "/api/factory/company-type"
_ch_cache = {}
_SUFFIX = re.compile(r"\b(ltd|limited|llp|plc|the|and|co|uk)\b")


def _norm(name):
    return re.sub(r"\s+", " ", _SUFFIX.sub(" ", re.sub(r"[^a-z0-9 ]", " ", str(name or "").lower().replace("&", " and ")))).strip()


def company_type(name, postcode=""):
    """Is this business a registered company? Uses Companies House search (COMPANIES_HOUSE_API_KEY).
    company: an active company with the same name (and, if we have both, the same postcode area)
    not_found: no active company under this name, so it may well be a sole trader or partnership
    unknown: no key, or Companies House couldn't be reached"""
    key = os.getenv("COMPANIES_HOUSE_API_KEY", "")
    if not key:
        return {"status": "unknown"}
    ck = (_norm(name), (postcode or "").upper().split(" ")[0])
    hit = _ch_cache.get(ck)
    if hit and time.time() - hit[0] < 7 * 86400:
        return hit[1]
    import requests
    try:
        r = requests.get("https://api.company-information.service.gov.uk/search/companies", params={"q": name, "items_per_page": 10},
                         auth=(key, ""), timeout=8)
        if r.status_code != 200:
            return {"status": "unknown"}
        items = r.json().get("items") or []
    except Exception as e:
        print(f"[company-type] {e}")
        return {"status": "unknown"}
    want, area = _norm(name), re.sub(r"\d.*$", "", ck[1])
    out = {"status": "not_found"}
    for it in items:
        if it.get("company_status") != "active" or _norm(it.get("title")) != want:
            continue
        pc = str((it.get("address") or {}).get("postal_code") or "").upper()
        if area and pc and not pc.startswith(area):
            continue
        out = {"status": "company", "name": it.get("title"), "number": it.get("company_number"), "type": it.get("company_type"),
               "url": "https://find-and-update.company-information.service.gov.uk/company/" + str(it.get("company_number"))}
        break
    _ch_cache[ck] = (time.time(), out)
    return out


def _cors(resp):
    if request.path in (PATH, CH_PATH) and request.headers.get("Origin", "") == FACTORY_ORIGIN:
        resp.headers["Access-Control-Allow-Origin"] = FACTORY_ORIGIN
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Vary"] = "Origin"
    return resp


def _clean(v, n):
    return re.sub(r"\s+", " ", str(v or "")).strip()[:n]


def build_prompt(lead, channel, me, offer, note, tweak, draft):
    gaps = [_clean(g[0] if isinstance(g, list) else g, 140) for g in (lead.get("gaps") or [])][:4]
    facts = {
        "business": _clean(lead.get("name"), 80), "type": _clean(lead.get("type"), 40), "town": _clean(lead.get("town"), 40),
        "google_rating": lead.get("rating") or None, "google_reviews": lead.get("reviews") if lead.get("reviews") not in ("", None) else None,
        "has_website": bool(lead.get("site")), "what_the_free_check_found": gaps,
    }
    lines = [f"Write {CHANNELS[channel]}.",
             f"About the business (facts from a free check): {json.dumps(facts)}",
             f"Sender's name: {me or '(not given, sign off without a name)'}",
             f"What the sender helps with: {offer or 'helping local businesses get found online and use AI for everyday admin'}",
             "The sender can also point them to a free self-check at humanagency.co/factory if it fits naturally (optional)."]
    if note:
        lines.append(f"Something the sender noticed about them (use it as the opener): {note}")
    if tweak and draft:
        lines.append(f"Here is the previous draft:\n{draft}\n{TWEAKS[tweak]}")
    return "\n".join(lines)


def draft_message(prompt):
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    r = client.messages.create(model=MODEL, max_tokens=600, system=RULES, messages=[{"role": "user", "content": prompt}])
    text = "".join(getattr(b, "text", "") for b in r.content)
    m = re.search(r"\{.*\}", text, re.S)
    d = json.loads(m.group(0)) if m else {"subject": "", "message": text.strip()}
    msg = re.sub(r"\s*[—–]\s*", ", ", str(d.get("message") or "")).strip()   # no em or en dashes, ever
    return {"message": msg, "subject": _clean(d.get("subject"), 80)}


def register_factory_intro(app):
    app.after_request(_cors)

    @app.route(CH_PATH, methods=["GET"])
    def factory_company_type():
        name = _clean(request.args.get("name"), 100)
        if len(name) < 2:
            return jsonify({"error": "Which business?"}), 400
        return jsonify(company_type(name, _clean(request.args.get("pc"), 10)))

    @app.route(PATH, methods=["POST", "OPTIONS"])
    def factory_lead_intro():
        if request.method == "OPTIONS":
            return ("", 204)
        b = request.get_json(silent=True) or {}
        lead = b.get("lead") if isinstance(b.get("lead"), dict) else {}
        channel = b.get("channel") if b.get("channel") in CHANNELS else "instagram"
        tweak = b.get("tweak") if b.get("tweak") in TWEAKS else ""
        if len(_clean(lead.get("name"), 80)) < 2:
            return jsonify({"error": "Which business is this for?"}), 400
        if not os.getenv("ANTHROPIC_API_KEY"):
            return jsonify({"error": "Drafting isn't switched on yet."}), 503
        ip = (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip()
        now = time.time()
        _recent[ip] = [t for t in _recent.get(ip, []) if now - t < 3600]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if _day["date"] != today:
            _day.update(date=today, count=0)
        if len(_recent[ip]) >= 30 or _day["count"] >= DAILY_CAP:
            return jsonify({"error": "That's a lot of drafts. Try again in a while."}), 429
        _recent[ip].append(now)
        _day["count"] += 1
        prompt = build_prompt(lead, channel, _clean(b.get("me"), 40), _clean(b.get("offer"), 160), _clean(b.get("note"), 200),
                              tweak, str(b.get("draft") or "")[:1500])
        try:
            return jsonify(draft_message(prompt))
        except Exception as e:
            print(f"[lead-intro] failed: {e}")
            return jsonify({"error": "Couldn't write a draft just now."}), 502
