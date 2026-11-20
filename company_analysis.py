"""
AI analysis for the Intel company page.

  GET /api/company/<name>/analysis

Writes "AI use" and a SWOT from what Miru already holds for the company:
its stored description and industry, plus live job-advert signals.
Every item must quote the input it rests on. Output is labelled as AI
analysis on the page; it is an estimate, not a checked fact.

Cached in memory for 24 hours so repeat views cost nothing.
"""
import json
import os
import re
import time

from flask import jsonify, request

from company_intelligence_routes import _ensure_row, slugify

ANALYSIS_MODEL = "claude-sonnet-4-6"
CACHE_SECONDS = 24 * 3600
_cache = {}  # slug -> (timestamp, payload)

PROMPT = """You are writing an AI-analysis note for a company research page.
Use ONLY the facts below. Do not add figures, rankings, dates or claims that are not in the facts.
Every item must include "evidence": a short quote or paraphrase of the fact it rests on.

COMPANY: {name}
INDUSTRY: {industry}
DESCRIPTION (AI summary, unchecked): {description}
HIRING (job adverts found, {roles} roles): {hiring}

Return only JSON in this shape:
{{
  "ai_use": [{{"point": "...", "evidence": "..."}}],
  "swot": {{
    "strengths": [{{"point": "...", "evidence": "..."}}],
    "weaknesses": [{{"point": "...", "evidence": "..."}}],
    "opportunities": [{{"point": "...", "evidence": "..."}}],
    "threats": [{{"point": "...", "evidence": "..."}}]
  }}
}}
Give 2 to 4 items per list. If the facts do not support a list, return an empty list for it.
"ai_use" = how the company appears to use or invest in AI, based only on the facts."""


def _parse_json(text):
    m = re.search(r"\{[\s\S]*\}", text or "")
    return json.loads(m.group(0)) if m else None


def _hiring_summary(name):
    try:
        from hiring_signals_agent import fetch_hiring_signals_live
        d = fetch_hiring_signals_live(name) or {}
    except Exception:
        return "", 0
    o = d.get("overview") or {}
    roles = o.get("total_open_roles") or 0
    signals = "; ".join(g.get("title", "") + ": " + g.get("description", "")
                        for g in (d.get("growth_signals") or []))
    return signals or "no clear signals", roles


def build_analysis(name):
    row, _ = _ensure_row(name, requested_by=None)
    if not row or row.get("status") != "ready":
        return None, "Analysis is available once the company has been researched."
    hiring, roles = _hiring_summary(row.get("display_name") or name)
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        resp = client.messages.create(
            model=ANALYSIS_MODEL,
            max_tokens=1200,
            messages=[{"role": "user", "content": PROMPT.format(
                name=row.get("display_name", name),
                industry=row.get("industry") or "not stated",
                description=(row.get("description") or "not stated")[:1500],
                roles=roles,
                hiring=hiring or "none found",
            )}],
        )
        text = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        data = _parse_json(text)
    except Exception as e:
        print(f"[company_analysis] failed for '{name}': {e}")
        return None, "Analysis is not available right now."
    if not data:
        return None, "Analysis is not available right now."
    data["label"] = "AI analysis. An estimate based on the facts shown, not a checked fact."
    data["model"] = ANALYSIS_MODEL
    return data, None


def register_company_analysis(app):
    @app.route("/api/company/<company_name>/analysis")
    def api_company_analysis(company_name):
        name = company_name.replace("-", " ").replace("_", " ").strip()
        slug = slugify(name)
        hit = _cache.get(slug)
        if hit and time.time() - hit[0] < CACHE_SECONDS:
            return jsonify(hit[1])
        data, err = build_analysis(name)
        if err:
            return jsonify({"error": err}), 202
        _cache[slug] = (time.time(), data)
        return jsonify(data)
