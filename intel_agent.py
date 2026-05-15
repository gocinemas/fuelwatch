"""
Intel Research Agent
Agentic company research using LLM tool_use.
Uses Groq (llama-3.3-70b-versatile) by default.
Upgrade: set ANTHROPIC_API_KEY to use Claude instead.
"""

import os
import json
import re
import requests
from search import fetch_company_info, _fetch_news

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.3-70b-versatile"

# ── Tool schemas (OpenAI-compatible, works with both Groq and Claude) ────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_company_profile",
            "description": (
                "Fetch a comprehensive profile for a company: Wikipedia summary, recent news, "
                "share price, job listings, and hiring signals. Call this first for any company."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "The company name, e.g. 'Gymshark' or 'Unilever'"
                    }
                },
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_news_by_topic",
            "description": (
                "Search recent news for a company filtered by a specific topic. "
                "Use after get_company_profile to dig into strategy, leadership, financials, or AI moves."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {"type": "string", "description": "The company name"},
                    "topic": {
                        "type": "string",
                        "description": (
                            "Topic to filter news by, e.g. "
                            "'CEO leadership change', 'AI strategy', 'acquisition merger', "
                            "'layoffs redundancy', 'earnings results'"
                        )
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max articles to return (default 5)",
                        "default": 5
                    }
                },
                "required": ["company_name", "topic"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_companies_house",
            "description": (
                "Look up a UK company on Companies House: registered name, company number, "
                "status (active/dissolved), type, SIC codes, incorporation date. "
                "Use for any UK company to verify legal standing."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "The company name to search on Companies House"
                    }
                },
                "required": ["company_name"]
            }
        }
    }
]

# ── System prompt ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert company research analyst producing briefs for a strategic audience:
investors, potential partners, and senior hires. Be specific, direct, and commercially sharp.

Research process (always follow this order):
1. Call get_company_profile — baseline data: description, financials, hiring signals, news
2. Call get_news_by_topic with topic "strategy OR leadership OR acquisition OR partnership"
3. If company appears UK-based, call get_companies_house to verify legal status
4. If there are interesting signals (AI moves, restructure, new CEO), call get_news_by_topic again
   with a more specific topic to get detail

After gathering data, produce your final output as a JSON object with these exact fields:
{
  "headline": "One sharp sentence capturing the company's current moment",
  "overview": "2-3 sentences: what they do, market position, current momentum",
  "financials": "Revenue, market cap, share price, profitability — only what is known",
  "strategy": "Key strategic moves, direction, acquisitions, partnerships from news",
  "leadership": "CEO and key leadership signals, recent changes",
  "hiring_signals": "What their open roles reveal about their direction (use job data)",
  "risks": ["Risk 1", "Risk 2", "Risk 3"],
  "opportunity_angle": "Why this company is interesting right now — the outsider's take",
  "confidence": "high | medium | low — based on data quality available"
}

If specific data is absent, say 'Not publicly available' rather than guessing.
Output ONLY the JSON — no preamble, no markdown fences."""

# ── Tool implementations ─────────────────────────────────────────────────────

def _tool_get_company_profile(company_name: str) -> str:
    try:
        data = fetch_company_info(company_name)
        if not data:
            return json.dumps({"error": f"No profile found for {company_name}"})
        out = {
            "company":         data.get("name") or company_name,
            "description":     data.get("description", ""),
            "founded":         data.get("founded", ""),
            "headquarters":    data.get("headquarters") or data.get("country", ""),
            "employees":       data.get("employees", ""),
            "revenue":         data.get("revenue", ""),
            "share_price":     data.get("share_price", ""),
            "market_cap":      data.get("market_cap", ""),
            "industry":        data.get("industry") or data.get("sector", ""),
            "parent_company":  data.get("parent", ""),
            "recent_news":     [
                {"title": n.get("title"), "date": n.get("date"), "source": n.get("source")}
                for n in (data.get("news") or [])[:6]
            ],
            "job_count":       len(data.get("jobs") or []),
            "hiring_signals":  data.get("hiring_signals", {}),
            "top_jobs":        [j.get("title") for j in (data.get("jobs") or [])[:8] if j.get("title")],
            "wikipedia_url":   data.get("wikipedia_url", ""),
        }
        return json.dumps(out, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


def _tool_get_news_by_topic(company_name: str, topic: str, limit: int = 5) -> str:
    try:
        articles = _fetch_news(company_name, topic, limit=limit)
        if not articles:
            return json.dumps({"articles": [], "note": f"No news found for '{company_name}' + '{topic}'"})
        return json.dumps({
            "articles": [
                {"title": a.get("title"), "date": a.get("date"), "source": a.get("source")}
                for a in articles
            ]
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


def _tool_get_companies_house(company_name: str) -> str:
    ch_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
    if ch_key:
        try:
            r = requests.get(
                "https://api.company-information.service.gov.uk/search/companies",
                params={"q": company_name, "items_per_page": 3},
                auth=(ch_key, ""),
                timeout=8
            )
            if r.status_code == 200:
                items = r.json().get("items") or []
                if items:
                    best = items[0]
                    cn   = best.get("company_number", "")
                    # Fetch full detail
                    detail = {}
                    if cn:
                        dr = requests.get(
                            f"https://api.company-information.service.gov.uk/company/{cn}",
                            auth=(ch_key, ""), timeout=8
                        )
                        if dr.status_code == 200:
                            detail = dr.json()
                    return json.dumps({
                        "company_name":       detail.get("company_name") or best.get("title", ""),
                        "company_number":     cn,
                        "status":             detail.get("company_status") or best.get("company_status", ""),
                        "company_type":       detail.get("type") or best.get("company_type", ""),
                        "incorporated":       detail.get("date_of_creation", ""),
                        "registered_address": detail.get("registered_office_address", {}),
                        "sic_codes":          detail.get("sic_codes", []),
                        "accounts_next_due":  (detail.get("accounts") or {}).get("next_due", ""),
                        "ch_url": f"https://find-and-update.company-information.service.gov.uk/company/{cn}",
                    }, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # No key — use public autocomplete (name + number + status only)
    try:
        r = requests.get(
            "https://autocomplete.companieshouse.gov.uk/autocompletion",
            params={"term": company_name, "type": "companies", "datarestrictions": "active"},
            timeout=8,
            headers={"User-Agent": "MiruIntelAgent/1.0"}
        )
        if r.status_code == 200:
            items = r.json().get("Items") or []
            if items:
                best = items[0]
                cn   = best.get("ID", "")
                return json.dumps({
                    "company_name":   best.get("Title", ""),
                    "company_number": cn,
                    "company_type":   best.get("Type", ""),
                    "status":         "Active",
                    "ch_url": f"https://find-and-update.company-information.service.gov.uk/company/{cn}",
                    "note": "Basic lookup (set COMPANIES_HOUSE_API_KEY for full filings data)"
                })
        return json.dumps({"note": "Company not found on Companies House — may not be UK-registered"})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ── Tool dispatcher ──────────────────────────────────────────────────────────

def _dispatch(tool_name: str, args: dict) -> str:
    if tool_name == "get_company_profile":
        return _tool_get_company_profile(args.get("company_name", ""))
    if tool_name == "get_news_by_topic":
        return _tool_get_news_by_topic(
            args.get("company_name", ""),
            args.get("topic", ""),
            int(args.get("limit", 5))
        )
    if tool_name == "get_companies_house":
        return _tool_get_companies_house(args.get("company_name", ""))
    return json.dumps({"error": f"Unknown tool: {tool_name}"})


# ── Agent loop ───────────────────────────────────────────────────────────────

def run_research_agent(company_name: str, max_iterations: int = 6) -> dict:
    """
    Run the Intel Research Agent for a given company.
    Returns a structured research brief as a dict.
    Logs each tool call to 'steps' for transparency.
    """
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return {"error": "GROQ_API_KEY not set", "company": company_name}

    messages = [
        {"role": "user", "content": f"Research this company for me: {company_name}"}
    ]
    steps = []  # what the agent did — returned to caller for UI

    for iteration in range(max_iterations):
        payload = {
            "model":       GROQ_MODEL,
            "messages":    [{"role": "system", "content": SYSTEM_PROMPT}] + messages,
            "tools":       TOOLS,
            "tool_choice": "auto",
            "temperature": 0.1,
            "max_tokens":  2000,
        }

        try:
            r = requests.post(
                GROQ_API_URL,
                headers={
                    "Authorization": f"Bearer {groq_key}",
                    "Content-Type":  "application/json"
                },
                json=payload,
                timeout=40,
            )
            r.raise_for_status()
            response = r.json()
        except Exception as e:
            return {"error": f"LLM call failed: {e}", "company": company_name, "steps": steps}

        choice = response.get("choices", [{}])[0]
        msg    = choice.get("message", {})
        reason = choice.get("finish_reason", "")

        messages.append(msg)

        # Agent wants to call tools
        if reason == "tool_calls" and msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                fn_name = tc["function"]["name"]
                try:
                    fn_args = json.loads(tc["function"]["arguments"])
                except Exception:
                    fn_args = {}

                step = {"tool": fn_name, "args": fn_args}
                tool_result = _dispatch(fn_name, fn_args)
                step["result_preview"] = tool_result[:200]  # truncate for logging
                steps.append(step)

                messages.append({
                    "role":        "tool",
                    "tool_call_id": tc["id"],
                    "content":     tool_result,
                })
            continue  # next iteration

        # Agent finished — parse JSON brief
        content = (msg.get("content") or "").strip()
        try:
            clean = re.sub(r"```(?:json)?\s*", "", content).strip().rstrip("`").strip()
            brief = json.loads(clean)
        except Exception:
            # Couldn't parse JSON — return raw
            brief = {"overview": content, "raw": True}

        brief["company"]  = company_name
        brief["steps"]    = steps
        brief["model"]    = GROQ_MODEL
        return brief

    return {
        "error":   "Agent reached max iterations without completing",
        "company": company_name,
        "steps":   steps
    }
