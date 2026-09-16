"""
Company Data Populator — background enrichment agent for company_details.

Given a company name, gathers:
  - description, industry, website, headquarters, founded_year, employee_count
  - social links (twitter/linkedin/instagram/facebook)
  - key facts
using Wikipedia as a free grounding source and Claude for structuring/summarizing,
then upserts the result into the `company_details` Supabase table
(schema: migrations/010_create_company_details.sql).

Entry points:
  - populate_company_data(name, slug)  — called as a fire-and-forget background
    thread from company_intelligence_routes.py, so the HTTP response never
    blocks on enrichment.
  - populate_stale_companies(limit)    — sweep job for a cron endpoint
    (see /api/cron/refresh-companies in sms_service.py) or the Railway
    `worker` dyno, mirroring school_service.poll_all_profiles().
  - `python company_data_populator.py "Ikea"` — manual/CLI run.

Never raises out to its caller — all failures are caught, logged, and recorded
on the row as status='failed' so the page can show a friendly error instead of
hanging in "pending" forever.
"""

import os
import re
import json
import traceback
from datetime import datetime, timezone, timedelta

import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# How long a "ready" profile is considered fresh before a background refresh
# is triggered again on next view.
STALE_AFTER_DAYS = 30

CLAUDE_MODEL = "claude-sonnet-4-6"

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
_HTTP_HEADERS = {"User-Agent": "Miru/1.0 (company intelligence enrichment; contact: mekala@gmail.com)"}

_sb_client = None


def _sb():
    global _sb_client
    if _sb_client is None:
        _sb_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb_client


def slugify(name: str) -> str:
    """Turn 'Ikea' / 'IKEA UK' into a stable, URL + lookup-safe slug."""
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unknown"


def _fetch_wikipedia_summary(company_name: str):
    """
    Best-effort grounding fact source so Claude isn't enriching from memory
    alone. Any failure here is non-fatal — enrichment falls back to Claude
    alone.
    """
    try:
        search_params = {
            "action": "query",
            "list": "search",
            "srsearch": company_name,
            "format": "json",
            "srlimit": 1,
        }
        r = requests.get(WIKIPEDIA_API, params=search_params, headers=_HTTP_HEADERS, timeout=6)
        r.raise_for_status()
        hits = r.json().get("query", {}).get("search", [])
        if not hits:
            return None
        title = hits[0]["title"]

        extract_params = {
            "action": "query",
            "prop": "extracts|pageimages|info",
            "exintro": True,
            "explaintext": True,
            "titles": title,
            "format": "json",
            "inprop": "url",
            "piprop": "original",
        }
        r2 = requests.get(WIKIPEDIA_API, params=extract_params, headers=_HTTP_HEADERS, timeout=6)
        r2.raise_for_status()
        pages = r2.json().get("query", {}).get("pages", {})
        page = next(iter(pages.values()), {})
        if not page.get("extract"):
            return None
        return {
            "title": page.get("title"),
            "extract": page.get("extract"),
            "wikipedia_url": page.get("fullurl"),
            "image": (page.get("original") or {}).get("source"),
        }
    except Exception as e:
        print(f"[company_populator] wikipedia lookup failed for '{company_name}': {e}")
        return None


def _parse_json_block(text: str):
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(json)?", "", text).rsplit("```", 1)[0].strip()
    return json.loads(text)


def _enrich_with_claude(company_name: str, wiki_context):
    """
    Ask Claude to structure everything into the shape company_details wants.
    Returns None (not raises) if ANTHROPIC_API_KEY isn't configured or the
    call/parse fails — callers must tolerate a partial/empty result.
    """
    if not ANTHROPIC_API_KEY:
        print("[company_populator] ANTHROPIC_API_KEY not set — skipping Claude enrichment")
        return None

    context_block = ""
    if wiki_context and wiki_context.get("extract"):
        context_block = (
            "\n\nBackground (from Wikipedia — verify/refine, don't just copy verbatim):\n"
            + wiki_context["extract"][:2000]
        )

    prompt = f"""You are a company research analyst. Produce a concise, factual profile for the company "{company_name}".{context_block}

Return ONLY valid JSON (no markdown fences, no commentary) with this exact shape:
{{
  "description": "2-3 sentence plain-English summary of what the company does",
  "industry": "primary industry / sector",
  "website": "https://... official website, best guess if unknown, else null",
  "headquarters": "city, country",
  "founded_year": 1943,
  "employee_count": "e.g. '10,000-50,000' or null if unknown",
  "social_links": {{"twitter": "https://... or null", "linkedin": "https://... or null", "instagram": "https://... or null", "facebook": "https://... or null"}},
  "key_facts": ["short factual bullet 1", "short factual bullet 2", "short factual bullet 3", "short factual bullet 4"],
  "confidence_score": 0.0
}}

Use null for any field you are not reasonably confident about — never invent URLs or numbers. confidence_score is a float 0-1 reflecting your overall confidence in this profile's accuracy."""

    try:
        from anthropic import Anthropic

        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(
            block.text for block in resp.content if getattr(block, "type", None) == "text"
        )
        return _parse_json_block(text)
    except Exception as e:
        print(f"[company_populator] Claude enrichment failed for '{company_name}': {e}")
        traceback.print_exc()
        return None


def populate_company_data(company_name: str, slug: str = None, requested_by: str = None):
    """
    Main entry point for the background agent.

    Safe to call from a daemon thread or a worker process — never raises;
    on failure it records status='failed' + fetch_error on the row so the
    page can surface a friendly message instead of spinning forever.
    """
    if not company_name or not company_name.strip():
        return None

    slug = slug or slugify(company_name)
    sb = _sb()

    try:
        sb.table("company_details").update({"status": "enriching"}).eq("slug", slug).execute()
    except Exception as e:
        print(f"[company_populator] could not mark '{slug}' as enriching: {e}")

    try:
        wiki = _fetch_wikipedia_summary(company_name)
        enriched = _enrich_with_claude(company_name, wiki) or {}

        description = enriched.get("description")
        if not description and wiki and wiki.get("extract"):
            description = wiki["extract"][:500]

        record = {
            "company_name": (company_name or slug).strip().title(),
            "slug": slug,
            "description": description,
            "industry": enriched.get("industry"),
            "website": enriched.get("website"),
            "logo_url": (wiki or {}).get("image"),
            "headquarters": enriched.get("headquarters"),
            "founded_year": enriched.get("founded_year"),
            "employee_count": enriched.get("employee_count"),
            "social_links": enriched.get("social_links") or {},
            "key_facts": enriched.get("key_facts") or [],
            "sources": [u for u in [(wiki or {}).get("wikipedia_url")] if u],
            "status": "ready",
            "confidence_score": enriched.get("confidence_score"),
            "fetch_error": None,
            "last_enriched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if requested_by:
            record["requested_by"] = requested_by

        sb.table("company_details").upsert(record, on_conflict="slug").execute()
        print(f"[company_populator] enriched '{company_name}' (slug={slug})")
        return record

    except Exception as e:
        print(f"[company_populator] enrichment failed for '{company_name}': {e}")
        traceback.print_exc()
        try:
            sb.table("company_details").update(
                {
                    "status": "failed",
                    "fetch_error": str(e)[:500],
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            ).eq("slug", slug).execute()
        except Exception:
            pass
        return None


def populate_stale_companies(limit: int = 20):
    """
    Sweep job: retries anything pending/failed, plus anything 'ready' but
    older than STALE_AFTER_DAYS. Intended for the Railway `worker` dyno
    (brand_worker.py-style) or the /api/cron/refresh-companies endpoint.
    """
    sb = _sb()
    results = []

    # 1. Never-completed rows (pending from a page view, or a previous failure)
    res = (
        sb.table("company_details")
        .select("company_name,slug,status")
        .in_("status", ["pending", "failed"])
        .limit(limit)
        .execute()
    )
    todo = list(res.data or [])

    # 2. Stale "ready" rows, oldest first, filling up to `limit`
    if len(todo) < limit:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=STALE_AFTER_DAYS)).isoformat()
        res2 = (
            sb.table("company_details")
            .select("company_name,slug,status,last_enriched_at")
            .eq("status", "ready")
            .lt("last_enriched_at", cutoff)
            .order("last_enriched_at")
            .limit(limit - len(todo))
            .execute()
        )
        todo.extend(res2.data or [])

    for row in todo:
        results.append(populate_company_data(row["company_name"], row["slug"]))

    print(f"[company_populator] sweep processed {len(results)} companies")
    return results


if __name__ == "__main__":
    import sys

    name = " ".join(sys.argv[1:]) or "Ikea"
    print(f"Populating company data for: {name}")
    out = populate_company_data(name)
    print(json.dumps(out, indent=2, default=str))
