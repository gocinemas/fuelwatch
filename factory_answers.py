"""AI Answer Audit for Intel: what do AI assistants tell shoppers about a product, and is it right?

  POST /api/factory/answer-audit   {brand, product, market, url}
       -> {score, capped, categories[], answers[], tools[], brand_source_share, checked_against, ran_at}

How it works:
  1. Reads the brand's own product page (url) as the source of truth. Without it nothing can be graded.
  2. Asks each AI tool we hold a key for six shopper questions, one per category shoppers said they'd ask
     (AuraVeo / OnePoll, 4,000 UK & US adults, Oct 2026): how to use it, features, ingredients,
     troubleshooting, usage tips, warranty.
  3. A judge (Claude) grades every answer against the product page only. Questions the page doesn't
     cover are marked "can't check" and left out of the score, rather than guessed.
  4. Score 0-100, categories weighted by how many shoppers want to ask them. A safety fail
     (allergens, storage, safe use) caps the score at 50.

Tools, each switched on by its key on Railway (missing key = tool skipped):
  ANTHROPIC_API_KEY   Claude, with web search          (also the judge, so required)
  OPENAI_API_KEY      ChatGPT, with web search
  PERPLEXITY_API_KEY  Perplexity (sonar)
  GEMINI_API_KEY      Gemini, with Google Search grounding
Costs are capped: 3 audits per hour per visitor, ANSWER_AUDIT_DAILY_CAP a day in total (default 20),
and the same product is answered from a 24-hour cache.
Each audit is saved in ma_details, type="factory_answer_audit", so results can be compared month to month.
"""
import html
import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from flask import jsonify, request

from factory_business import FACTORY_ORIGIN, _fetch

TYPE = "factory_answer_audit"
PATH = "/api/factory/answer-audit"
ANSWER_MODEL = os.getenv("ANSWER_AUDIT_MODEL", "claude-sonnet-5-5")
JUDGE_MODEL = os.getenv("ANSWER_AUDIT_JUDGE_MODEL", "claude-sonnet-5-5")
DAILY_CAP = int(os.getenv("ANSWER_AUDIT_DAILY_CAP", "20") or 20)
CACHE_SECONDS = 24 * 3600

# Share of shoppers who'd ask each kind of question (AuraVeo / OnePoll, Oct 2026). Used as weights.
CATEGORIES = [
    ("use", "How to use it", 30, "How do I use {p}?"),
    ("features", "Product features", 23, "What are the main features of {p}, and what sizes or versions does it come in?"),
    ("ingredients", "Ingredients / materials", 23, "What's in {p}? Does it contain common allergens like nuts, gluten or dairy?"),
    ("trouble", "Troubleshooting", 21, "How should I store {p}, and what should I do if something seems wrong with it?"),
    ("tips", "Usage tips", 17, "Any tips to get the most out of {p}?"),
    ("warranty", "Warranty / support", 17, "What do I do if {p} is faulty or I want a refund? How do I contact {b}?"),
]
SAFETY = {"ingredients", "trouble"}
POINTS = {"correct": 100, "partly": 60, "vague": 30, "out_of_date": 20, "wrong": 0, "safety_fail": 0}
GRADE_LABEL = {"correct": "Correct", "partly": "Partly right", "vague": "Vague", "out_of_date": "Out of date",
               "wrong": "Wrong", "safety_fail": "Safety fail", "cant_check": "Can't check", "no_answer": "No answer"}

_recent = {}      # ip -> [timestamps]
_day = {"date": "", "count": 0}
_cache = {}       # key -> (time, result)


def _sb():
    import library
    return library._sb()


def _page_text(raw_html, limit=14000):
    """Readable text of a product page: drops scripts, styles and tags, keeps structured data."""
    jsonld = " ".join(re.findall(r"<script[^>]+application/ld\+json[^>]*>(.*?)</script>", raw_html, re.S | re.I))[:4000]
    t = re.sub(r"<(script|style|noscript|svg)[^>]*>.*?</\1>", " ", raw_html, flags=re.S | re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(re.sub(r"\s+", " ", t)).strip()
    return (t[:limit - len(jsonld)] + ("\n\nStructured data: " + jsonld if jsonld else "")).strip()


def _shopper(market):
    return (f"You are a general AI assistant talking to an ordinary shopper in the {market}. "
            "Answer their question helpfully and briefly, in under 120 words, the way you normally would.")


# ── The AI tools ─────────────────────────────────────────────────────────────

def ask_claude(q, market):
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    kw = dict(model=ANSWER_MODEL, max_tokens=700, system=_shopper(market), messages=[{"role": "user", "content": q}])
    try:
        r = client.messages.create(tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 2}], **kw)
    except Exception as e:
        print(f"[answer-audit] Claude web search unavailable, answering without it: {e}")
        r = client.messages.create(**kw)
    text, links = [], []
    for block in r.content:
        if getattr(block, "type", "") == "text":
            text.append(block.text)
            for c in getattr(block, "citations", None) or []:
                if getattr(c, "url", None):
                    links.append(c.url)
        elif getattr(block, "type", "") == "web_search_tool_result":
            for item in getattr(block, "content", None) or []:
                if getattr(item, "url", None):
                    links.append(item.url)
    return "".join(text).strip(), links


def ask_chatgpt(q, market):
    r = requests.post("https://api.openai.com/v1/responses", timeout=60,
                      headers={"Authorization": "Bearer " + os.getenv("OPENAI_API_KEY", "")},
                      json={"model": os.getenv("ANSWER_AUDIT_OPENAI_MODEL", "gpt-5-mini"), "tools": [{"type": "web_search"}],
                            "instructions": _shopper(market), "input": q})
    r.raise_for_status()
    d = r.json()
    text, links = [], []
    for item in d.get("output", []):
        for c in item.get("content", []) or []:
            if c.get("type") == "output_text":
                text.append(c.get("text", ""))
                links += [a.get("url") for a in c.get("annotations", []) or [] if a.get("url")]
    return "".join(text).strip(), links


def ask_perplexity(q, market):
    r = requests.post("https://api.perplexity.ai/chat/completions", timeout=60,
                      headers={"Authorization": "Bearer " + os.getenv("PERPLEXITY_API_KEY", "")},
                      json={"model": os.getenv("ANSWER_AUDIT_PERPLEXITY_MODEL", "sonar"),
                            "messages": [{"role": "system", "content": _shopper(market)}, {"role": "user", "content": q}]})
    r.raise_for_status()
    d = r.json()
    return d["choices"][0]["message"]["content"].strip(), list(d.get("citations") or [])


def ask_gemini(q, market):
    model = os.getenv("ANSWER_AUDIT_GEMINI_MODEL", "gemini-2.5-flash")
    r = requests.post(f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent", timeout=60,
                      headers={"x-goog-api-key": os.getenv("GEMINI_API_KEY", "")},
                      json={"systemInstruction": {"parts": [{"text": _shopper(market)}]},
                            "contents": [{"parts": [{"text": q}]}], "tools": [{"google_search": {}}]})
    r.raise_for_status()
    cand = (r.json().get("candidates") or [{}])[0]
    text = "".join(p.get("text", "") for p in (cand.get("content") or {}).get("parts", []))
    links = [c.get("web", {}).get("uri") for c in (cand.get("groundingMetadata") or {}).get("groundingChunks", []) or []]
    return text.strip(), [u for u in links if u]


TOOLS = [("claude", "Claude", "ANTHROPIC_API_KEY", ask_claude), ("chatgpt", "ChatGPT", "OPENAI_API_KEY", ask_chatgpt),
         ("perplexity", "Perplexity", "PERPLEXITY_API_KEY", ask_perplexity), ("gemini", "Gemini", "GEMINI_API_KEY", ask_gemini)]


def tools_on():
    return [(k, name, fn) for k, name, env, fn in TOOLS if os.getenv(env)]


# ── Grading ──────────────────────────────────────────────────────────────────

JUDGE_RULES = """You check what AI assistants told shoppers about a product against the brand's own product page.
Use ONLY the product page below as the truth. Never use your own knowledge of the product.
Grade every answer with exactly one of:
  correct      matches the page
  partly       right as far as it goes, but misses something the page says that a shopper would want
  vague        generic, nothing specific to this product
  out_of_date  states something the page shows has changed (old recipe, size, pack, price)
  wrong        contradicts the page
  safety_fail  wrong about allergens, ingredients that matter for safety, storage, or safe use
  cant_check   the page doesn't cover what the answer claims, so it can't be checked
  no_answer    the assistant didn't answer
If an answer is mostly unverifiable but contains one claim the page contradicts, grade it by that claim.
Reply with JSON only: {"grades": [{"id": "...", "grade": "...", "note": "one short plain sentence: what's wrong or missing, quoting the page where useful"}]}"""


def judge(page, product, qa):
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    items = "\n\n".join(f'[{x["id"]}] Question: {x["question"]}\nAnswer: {x["answer"] or "(no answer)"}' for x in qa)
    r = client.messages.create(model=JUDGE_MODEL, max_tokens=4000, system=JUDGE_RULES, messages=[{"role": "user", "content":
        f"Product: {product}\n\n<product_page>\n{page}\n</product_page>\n\nAnswers to grade:\n\n{items}"}])
    text = "".join(getattr(b, "text", "") for b in r.content)
    m = re.search(r"\{.*\}", text, re.S)
    grades = (json.loads(m.group(0)) if m else {}).get("grades", [])
    return {g.get("id"): g for g in grades if g.get("grade") in GRADE_LABEL}


def score(answers):
    """Overall 0-100, categories weighted by shopper demand. Unchecked answers are left out."""
    cats, total, weight = [], 0, 0
    for key, label, w, _ in CATEGORIES:
        pts = [POINTS[a["grade"]] for a in answers if a["category"] == key and a["grade"] in POINTS]
        no_ans = [a for a in answers if a["category"] == key and a["grade"] == "no_answer"]
        pts += [0] * len(no_ans)
        cat = {"key": key, "label": label, "weight": w, "score": round(sum(pts) / len(pts)) if pts else None,
               "safety_fail": any(a["grade"] == "safety_fail" for a in answers if a["category"] == key)}
        cats.append(cat)
        if pts:
            total += cat["score"] * w
            weight += w
    overall = round(total / weight) if weight else None
    capped = any(c["safety_fail"] for c in cats)
    if capped and overall is not None:
        overall = min(overall, 50)
    return overall, capped, cats


def brand_source_share(answers, domain):
    with_links = [a for a in answers if a.get("links")]
    if not with_links or not domain:
        return None
    hits = [a for a in with_links if any(domain in (urlparse(u).hostname or "") for u in a["links"])]
    return round(100 * len(hits) / len(with_links))


def run_audit(brand, product, market, url):
    final_url, status, raw = _fetch(url)
    if status >= 400:
        raise ValueError("We couldn't open that product page.")
    page = _page_text(raw)
    if len(page) < 300:
        raise ValueError("That page didn't have enough product detail to check answers against.")
    tools = tools_on()
    jobs = []
    for key, label, w, tmpl in CATEGORIES:
        q = tmpl.format(p=product, b=brand)
        for tkey, tname, fn in tools:
            jobs.append({"id": f"{key}-{tkey}", "category": key, "tool": tkey, "tool_name": tname, "question": q, "fn": fn})

    def one(job):
        try:
            text, links = job["fn"](job["question"], market)
            return text, links[:8], None
        except Exception as e:
            print(f"[answer-audit] {job['tool']} failed: {e}")
            return "", [], "error"

    with ThreadPoolExecutor(max_workers=6) as pool:
        for job, (text, links, err) in zip(jobs, pool.map(one, jobs)):
            job.update(answer=text, links=links, error=err)
    grades = judge(page, product, [j for j in jobs if not j["error"]])
    answers = []
    for j in jobs:
        g = grades.get(j["id"], {})
        grade = "no_answer" if j["error"] else g.get("grade", "cant_check")
        answers.append({"id": j["id"], "category": j["category"], "tool": j["tool_name"], "question": j["question"],
                        "answer": j["answer"], "links": j["links"], "grade": grade, "grade_label": GRADE_LABEL[grade],
                        "note": g.get("note", "The assistant couldn't be reached." if j["error"] else "")})
    overall, capped, cats = score(answers)
    domain = (urlparse(final_url).hostname or "").removeprefix("www.")
    return {"brand": brand, "product": product, "market": market, "checked_against": final_url,
            "tools": [name for _, name, _ in tools], "score": overall, "capped": capped, "categories": cats,
            "answers": answers, "brand_source_share": brand_source_share(answers, domain),
            "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}


# ── Route ────────────────────────────────────────────────────────────────────

def _cors(resp):
    if request.path == PATH and request.headers.get("Origin", "") == FACTORY_ORIGIN:
        resp.headers["Access-Control-Allow-Origin"] = FACTORY_ORIGIN
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Vary"] = "Origin"
    return resp


def register_factory_answers(app):
    app.after_request(_cors)

    @app.route(PATH, methods=["POST", "OPTIONS"])
    def factory_answer_audit():
        if request.method == "OPTIONS":
            return ("", 204)
        b = request.get_json(silent=True) or {}
        brand = str(b.get("brand") or "").strip()[:80]
        product = str(b.get("product") or "").strip()[:120]
        market = "US" if str(b.get("market") or "").upper() == "US" else "UK"
        url = str(b.get("url") or "").strip()[:400]
        if len(brand) < 2 or len(product) < 2:
            return jsonify({"error": "Tell us the brand and the product."}), 400
        if not url.startswith(("http://", "https://")):
            return jsonify({"error": "Add the link to the product's page on the brand's own website. We check every answer against it."}), 400
        if not os.getenv("ANTHROPIC_API_KEY"):
            return jsonify({"error": "The answer check isn't switched on yet."}), 503
        key = "|".join(x.lower() for x in (brand, product, market, url))
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < CACHE_SECONDS and not b.get("fresh"):
            return jsonify(dict(hit[1], cached=True))
        ip = (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip()
        now = time.time()
        _recent[ip] = [t for t in _recent.get(ip, []) if now - t < 3600]
        if len(_recent[ip]) >= 3:
            return jsonify({"error": "That's a lot of checks. Try again in an hour."}), 429
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if _day["date"] != today:
            _day.update(date=today, count=0)
        if _day["count"] >= DAILY_CAP:
            return jsonify({"error": "We've run all of today's checks. Try again tomorrow."}), 429
        _recent[ip].append(now)
        _day["count"] += 1
        try:
            result = run_audit(brand, product, market, url)
        except ValueError as e:
            msg = str(e) if str(e) not in ("not public", "too many redirects") else "We couldn't open that product page."
            return jsonify({"error": msg}), 400
        except Exception as e:
            print(f"[answer-audit] failed: {e}")
            return jsonify({"error": "The check didn't finish. Try again in a minute."}), 502
        _cache[key] = (time.time(), result)
        try:
            _sb().table("ma_details").insert({"device_id": "audit:" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6],
                                              "type": TYPE, "label": f"{brand} / {product}"[:60], "data": result}).execute()
        except Exception as e:
            print(f"[answer-audit] save failed: {e}")
        return jsonify(result)
