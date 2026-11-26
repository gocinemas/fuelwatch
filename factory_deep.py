"""Detailed website report for businesses that are already easy to find.

  POST /api/factory/business-report  {url, business, kind, address, phone}
       -> {url, sections: [{title, items: [{ok, label, detail}]}], fixes: [..], score, out_of, ms, kb}

Reads the home page and, if there is one, the contact page (2 fetches, public hosts only, via _fetch).
No AI calls, so it costs nothing to run. Checks are plain, explainable rules: what a visitor or a search
engine can actually find, compared with the business's own Google listing where we have it.
"""
import html
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

from flask import jsonify, request

from factory_business import BUSINESS_TYPES, FACTORY_ORIGIN, SOCIAL, _fetch

PATH = "/api/factory/business-report"
DAYS = re.compile(r"\b(mon|tue|wed|thu|fri|sat|sun)[a-z]*\b", re.I)
POSTCODE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b", re.I)

# What a visitor of this kind of business looks for, and how to spot it on the page
KIND_NEEDS = [
    (r"bakery|cake|cafe|coffee|dessert|ice_cream|sandwich", "Menu or what you sell, with prices", r"\bmenu\b|£\s?\d", "Order or pre-order link", r"\border|pre-?order|click\s*(&|and)\s*collect|cake"),
    (r"restaurant|takeaway|meal_|pizza|fish|chip|kebab|curry|burger|food", "Menu with prices", r"\bmenu\b|£\s?\d", "Allergen information", r"allergen|gluten|nut"),
    (r"hair|barber|beauty|nail|salon|spa|lash|brow|massage", "Price list", r"price|£\s?\d", "Online booking", r"\bbook|appointment|fresha|booksy|treatwell|setmore"),
    (r"plumb|electric|roof|contractor|builder|locksmith|painter|decorat|heating|clean|garage|mechanic|car_repair", "Areas you cover", r"\bareas?\b|cover|serving", "Quote request", r"quote|estimate|enquir"),
    (r"dentist|dental|physio|doctor|clinic|optic|chiropract|osteo|veterinar|vet|health|therap", "Prices or fees", r"\bfees?\b|price|£\s?\d", "How to book", r"\bbook|appointment|register"),
    (r"pub|bar|inn|brewery", "What's on (events)", r"what'?s on|events?|quiz|live music", "Table booking", r"\bbook|reserv"),
]


def _text(body):
    t = re.sub(r"<(script|style|noscript|svg)[^>]*>.*?</\1>", " ", body, flags=re.S | re.I)
    return html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", t)))


def _digits(s):
    d = re.sub(r"\D", "", s or "")
    return "0" + d[2:] if d.startswith("44") else d


def build_report(url, kind="", address="", phone="", business=""):
    t0 = time.time()
    final, status, body = _fetch(url)
    ms = int((time.time() - t0) * 1000)
    if status >= 400:
        raise ValueError("The website didn't load.")
    kb = round(len(body.encode("utf-8", "ignore")) / 1024)
    text = _text(body)
    low = text.lower()
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', body, re.I)
    hl = " ".join(hrefs).lower()

    # The contact page often holds the address, hours and phone: read it too, if there is one
    contact = next((h for h in hrefs if re.search(r"contact|find-us|visit|location", h, re.I)), "")
    if contact:
        try:
            cu = urljoin(final, contact)
            if urlparse(cu).hostname == urlparse(final).hostname:
                _, st, cb = _fetch(cu, 300_000)
                if st < 400:
                    ct = _text(cb)
                    text, low = text + " " + ct, (text + " " + ct).lower()
                    hl += " " + " ".join(re.findall(r'href=["\']([^"\']+)["\']', cb, re.I)).lower()
        except Exception:
            pass

    def item(ok, label, detail, fix=None, weight=1):
        return {"ok": bool(ok), "label": label, "detail": detail, "fix": None if ok else fix, "w": weight}

    # 1. Can people find the basics?
    pc = POSTCODE.search(address or "")
    pc_on = bool(pc) and (pc.group(1) + pc.group(2)).lower() in re.sub(r"\s", "", low)
    ph = _digits(phone)
    ph_on = bool(ph) and ph[-9:] in re.sub(r"\D", "", text)
    tel = "tel:" in hl
    basics = [
        item(pc_on, "Address on the site", "Your postcode appears on the site." if pc_on else "We couldn't find your postcode on the home or contact page.",
             "Put your full address, with postcode, on the home or contact page so people and maps agree on where you are.", 2) if pc else None,
        item(ph_on, "Phone number matches Google", "The number on Google is on your site too." if ph_on else "The number on your Google listing isn't on the site, or it's different.",
             "Use the same phone number on your site as on Google. Mismatches confuse customers and search engines.", 2) if ph else None,
        item(tel, "Tap to call on phones", "Your number is a tap-to-call link." if tel else "The phone number isn't tappable on a mobile.",
             "Make your phone number a tap-to-call link, so mobile visitors can ring in one tap."),
        item(bool(DAYS.search(text)) and re.search(r"open|hours|\d{1,2}(:\d\d)?\s?(am|pm)", low), "Opening hours on the site",
             "Your opening hours are on the site." if DAYS.search(text) else "We couldn't see opening hours.",
             "Show your opening hours on the site, matching Google, so people don't have to look elsewhere."),
    ]

    # 2. Does the site turn visitors into customers?
    conv = []
    for pat, l1, r1, l2, r2 in KIND_NEEDS:
        if re.search(pat, kind or business or "", re.I):
            conv += [item(re.search(r1, low), l1, f"Found on the site." if re.search(r1, low) else "We couldn't find this on the home or contact page.",
                          f"Add a clear {l1.lower()} where people can see it from the home page.", 2),
                     item(re.search(r2, low + " " + hl), l2, f"Found on the site." if re.search(r2, low + " " + hl) else "We couldn't find this.",
                          f"Add a clear {l2.lower()} so people can act straight away instead of calling.", 2)]
            break
    if not conv:
        cta = re.search(r"\b(book|order|quote|enquir|contact|get in touch|call us)\b", low)
        conv.append(item(cta, "A clear next step", "There's a clear next step (book, order, contact)." if cta else "No obvious next step on the home page.",
                         "Give visitors one clear next step on the home page: book, order, get a quote or contact you.", 2))
    reviews = re.search(r"review|testimonial|what (our )?customers say|★|stars?\b", low)
    conv.append(item(reviews, "Reviews on the site", "You show reviews or testimonials." if reviews else "Your good reviews aren't on the site.",
                     "Show a few of your best Google reviews on the site. They're the strongest reason to choose you."))
    social = [k for k, p in SOCIAL.items() if re.search(p, hl)]
    conv.append(item(social, "Links to your social media", ", ".join(s.title() for s in social) + " linked." if social else "No Instagram, Facebook or TikTok links.",
                     "Link your Instagram, Facebook or TikTok so people can see what you're up to."))

    # 3. Is it in good shape?
    imgs = re.findall(r"<img\b[^>]*>", body, re.I)
    no_alt = [i for i in imgs if not re.search(r'\balt=["\'][^"\']+', i, re.I)]
    years = [int(y) for y in re.findall(r"(?:©|&copy;|copyright)\s*(?:\d{4}\s*[-–]\s*)?(20\d\d)", body, re.I)]
    this_year = datetime.now(timezone.utc).year
    stale = bool(years) and max(years) < this_year - 1
    mixed = re.search(r'(src|href)=["\']http://', body, re.I) and final.startswith("https://")
    shape = [
        item(ms < 2500, "Loads quickly", f"The home page answered in {ms / 1000:.1f} seconds.",
             "Speed the site up (smaller images, fewer plugins). Slow pages lose mobile visitors."),
        item(kb < 1500, "Page isn't too heavy", f"The home page is about {kb} KB of code.", "The page is heavy. Trim unused plugins and scripts."),
        item(re.search(r'<meta[^>]+name=["\']viewport', body, re.I), "Works on phones", "Set up for phone screens.", "Make the site work on phones: most local searches happen there.", 2),
        item(final.startswith("https://") and not mixed, "Secure", "Uses https throughout." if final.startswith("https://") and not mixed else "Not fully secure.",
             "Make sure every page and image loads over https, so browsers don't warn people."),
        item(not stale, "Looks up to date", f"Copyright year {max(years)}." if years else "No out-of-date copyright year found.",
             f"The footer says © {max(years) if years else ''}. Update it: an old year makes a site look abandoned."),
        item(not imgs or len(no_alt) <= len(imgs) // 3, "Pictures described", f"{len(imgs) - len(no_alt)} of {len(imgs)} pictures have descriptions.",
             "Add short descriptions (alt text) to your pictures. It helps search engines and people using screen readers."),
        item(re.search(r"privacy", hl + " " + low), "Privacy policy", "There's a privacy policy." if re.search(r"privacy", hl + " " + low) else "No privacy policy link found.",
             "Add a privacy policy page if you collect any details (forms, bookings, newsletters)."),
    ]

    # 4. Can Google and AI tools read it?
    m = re.search(r"<title[^>]*>(.*?)</title>", body, re.I | re.S)
    title = html.unescape(re.sub(r"\s+", " ", m.group(1)).strip()) if m else ""
    d = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)', body, re.I) or \
        re.search(r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+name=["\']description', body, re.I)
    desc = html.unescape(d.group(1).strip()) if d else ""
    h1 = re.findall(r"<h1\b", body, re.I)
    schema = any(BUSINESS_TYPES.search(b) for b in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', body, re.I | re.S))
    town = (address.split(",")[-2].strip() if address.count(",") >= 2 else "")
    town_word = re.sub(r"\s*[A-Z]{1,2}\d.*$", "", town).strip()
    read = [
        item(10 <= len(title) <= 65, "Page title", f"\"{title[:70]}\"" if title else "No page title.",
             "Give the home page a clear title of up to about 60 characters: what you do and where, e.g. \"Bakery in Virginia Water | The Jolly Baker\".", 2),
        item(town_word and town_word.lower() in (title + " " + desc).lower(), "Your town in the title or description",
             f"Mentions {town_word}." if town_word and town_word.lower() in (title + " " + desc).lower() else f"Doesn't mention {town_word or 'your town'}.",
             f"Mention {town_word or 'your town'} in the page title or description: it's what people type into Google.") if town_word else None,
        item(50 <= len(desc) <= 170, "Search description", f"\"{desc[:90]}…\"" if len(desc) > 90 else (f"\"{desc}\"" if desc else "No description set."),
             "Write a one-line description (about 150 characters) of what you do and where. Google often shows it under your name."),
        item(len(h1) == 1, "One main heading", "One main heading (h1)." if len(h1) == 1 else (f"{len(h1)} main headings (h1)." if h1 else "No main heading (h1)."),
             "Give the home page one main heading saying what you do."),
        item(schema, "Business details for search and AI", "Your business details are in a standard format (schema.org)." if schema else "Not set up.",
             "Add your business details in a standard format (schema.org) so search engines and AI tools can read your name, address, hours and what you do.", 2),
    ]

    sections = [{"title": t, "items": [i for i in its if i]} for t, its in
                (("Can people find the basics?", basics), ("Does it turn visitors into customers?", conv),
                 ("Is the site in good shape?", shape), ("Can Google and AI tools read it?", read))]
    allitems = [i for s in sections for i in s["items"]]
    score = sum(i["w"] for i in allitems if i["ok"])
    out_of = sum(i["w"] for i in allitems)
    fixes = [i["fix"] for i in sorted((i for i in allitems if not i["ok"]), key=lambda i: -i["w"])][:5]
    for i in allitems:
        i.pop("w", None)
    return {"url": final, "ms": ms, "kb": kb, "score": score, "out_of": out_of, "sections": sections, "fixes": fixes,
            "checked": datetime.now(timezone.utc).isoformat(timespec="seconds")}


_recent = {}


def _cors(resp):
    if request.path == PATH and request.headers.get("Origin", "") == FACTORY_ORIGIN:
        resp.headers["Access-Control-Allow-Origin"] = FACTORY_ORIGIN
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Vary"] = "Origin"
    return resp


def register_factory_deep(app):
    app.after_request(_cors)

    @app.route(PATH, methods=["POST"])
    def factory_business_report():
        b = request.get_json(silent=True) or {}
        url = str(b.get("url") or "").strip()[:300]
        if not re.match(r"^https?://", url, re.I):
            return jsonify({"error": "Which website?"}), 400
        ip = (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip()
        now = time.time()
        _recent[ip] = [t for t in _recent.get(ip, []) if now - t < 3600]
        if len(_recent[ip]) >= 10:
            return jsonify({"error": "That's a lot of reports. Try again in an hour."}), 429
        _recent[ip].append(now)
        try:
            return jsonify(build_report(url, str(b.get("kind") or "")[:40], str(b.get("address") or "")[:200],
                                        str(b.get("phone") or "")[:30], str(b.get("business") or "")[:100]))
        except ValueError as e:
            return jsonify({"error": str(e) if str(e) != "not public" else "That address can't be checked."}), 400
        except Exception as e:
            print(f"[biz-report] {url}: {str(e)[:120]}")
            return jsonify({"error": "The website didn't respond."}), 502
