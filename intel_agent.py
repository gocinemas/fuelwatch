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
from search import _fetch_news

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.3-70b-versatile"

# ── Tool schemas (OpenAI-compatible, works with both Groq and Claude) ────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_news_by_topic",
            "description": (
                "Search recent news for a company filtered by a specific topic. "
                "Call this 2-3 times with different topics to build a complete picture."
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
                            "'layoffs redundancy', 'earnings results', 'new product launch', "
                            "'campaign marketing', 'partnership deal'"
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
    }
]

# ── System prompt ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert company research analyst producing briefs for a strategic audience:
investors, potential partners, and senior hires. Be specific, direct, and commercially sharp.

The user is already looking at a full company profile on screen (description, financials, news feed, job listings).
Your job is to surface what the profile screen does NOT show: strategic signals, leadership moves,
competitive positioning, and what this company's current moment means for someone deciding to invest,
partner, hire, or compete with them.

Research process:
1. Call get_news_by_topic with topic "strategy acquisition partnership restructure" — strategic moves
2. Call get_news_by_topic with topic "CEO leadership hiring layoffs" — people signals
3. If interesting signals appear (AI pivot, new CEO, major deal), call get_news_by_topic again
   with a sharper topic to get detail (e.g. "AI investment product launch", "merger acquisition target")

After gathering data, produce your final output as a JSON object with these exact fields:
{
  "headline": "One sharp sentence capturing the company's current moment",
  "strategy": "Key strategic moves from news — acquisitions, pivots, partnerships, new markets",
  "leadership": "CEO and key leadership signals, recent changes, what they signal",
  "hiring_signals": "What the direction of hiring reveals — which teams are growing, what capabilities they're building",
  "ai_focus": "What AI bets this company is making — pilots, investments, named tools, stated ambitions, or absence of AI signals",
  "risks": ["Risk 1", "Risk 2", "Risk 3"],
  "opportunity_angle": "Why this company is interesting right now — the outsider's take for an investor or partner",
  "confidence": "high | medium | low — based on recency and quality of news data"
}

If specific data is absent, say 'Not publicly available' rather than guessing.
Output ONLY the JSON — no preamble, no markdown fences."""

# ── Tool implementations ─────────────────────────────────────────────────────

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


# ── Tool dispatcher ──────────────────────────────────────────────────────────

def _dispatch(tool_name: str, args: dict) -> str:
    if tool_name == "get_news_by_topic":
        return _tool_get_news_by_topic(
            args.get("company_name", ""),
            args.get("topic", ""),
            int(args.get("limit", 5))
        )
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
