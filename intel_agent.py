"""
Intel Research Agent
Agentic company research using LLM tool_use.
Primary: Groq (llama-3.3-70b-versatile) with up to 3 retries on 429/5xx.
Fallback: Claude (claude-haiku-4-5-20251001) when ANTHROPIC_API_KEY is set.
"""

import os
import json
import re
import time
import requests
from search import _fetch_news

GROQ_API_URL     = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL       = "llama-3.3-70b-versatile"
TOGETHER_API_URL = "https://api.together.xyz/v1/chat/completions"
TOGETHER_MODEL   = "meta-llama/Llama-3.3-70B-Instruct-Turbo"
CLAUDE_MODEL     = "claude-haiku-4-5-20251001"
GROQ_MAX_RETRIES = 2

# ── Tool schemas ─────────────────────────────────────────────────────────────

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

# Claude uses input_schema instead of parameters
CLAUDE_TOOLS = [
    {
        "name": t["function"]["name"],
        "description": t["function"]["description"],
        "input_schema": t["function"]["parameters"],
    }
    for t in TOOLS
]

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert company research analyst producing briefs for a strategic audience:
investors, potential partners, and senior hires. Be specific, direct, and commercially sharp.

The user is already looking at a full company profile on screen (description, financials, news feed, job listings).
Your job is to surface what the profile screen does NOT show: strategic signals, leadership moves,
competitive positioning, and what this company's current moment means for someone deciding to invest,
partner, hire, or compete with them.

Research process (all 3 calls are mandatory):
1. Call get_news_by_topic with topic "strategy acquisition partnership restructure" — strategic moves
2. Call get_news_by_topic with topic "CEO leadership hiring layoffs" — people signals
3. Call get_news_by_topic with topic "artificial intelligence AI machine learning automation technology" — AI signals
   This call is ALWAYS required. Use it to populate ai_focus with specific AI bets, tool names, pilots, or investments.
   If the news returns nothing AI-related, state "No public AI initiatives found" in ai_focus — do not say 'Not publicly available'.

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

If specific data is absent for most fields, say 'Not publicly available'. For ai_focus specifically, always write something based on your search — either what AI moves were found, or "No public AI initiatives found in recent news."
Output ONLY the JSON — no preamble, no markdown fences."""

# ── Tool implementations ──────────────────────────────────────────────────────

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


def _dispatch(tool_name: str, args: dict) -> str:
    if tool_name == "get_news_by_topic":
        return _tool_get_news_by_topic(
            args.get("company_name", ""),
            args.get("topic", ""),
            int(args.get("limit", 5))
        )
    return json.dumps({"error": f"Unknown tool: {tool_name}"})


def _parse_brief(content: str) -> dict:
    clean = re.sub(r"```(?:json)?\s*", "", content).strip().rstrip("`").strip()
    try:
        return json.loads(clean)
    except Exception:
        return {"overview": content, "raw": True}


# ── Generic OpenAI-compat call with retry ─────────────────────────────────────

def _openai_compat_call(url: str, key: str, payload: dict, max_retries: int = 3) -> dict:
    last_err = None
    for attempt in range(max_retries):
        try:
            r = requests.post(
                url,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
                timeout=45,
            )
            if r.status_code == 429:
                wait = (2 ** attempt) * 1
                time.sleep(wait)
                last_err = f"HTTP 429 (attempt {attempt + 1})"
                continue
            if r.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    last_err = f"HTTP {r.status_code} (attempt {attempt + 1})"
                    continue
            r.raise_for_status()
            return r.json()
        except requests.Timeout:
            last_err = f"Timeout (attempt {attempt + 1})"
            if attempt < max_retries - 1:
                continue
    raise Exception(f"API call failed after {max_retries} attempts: {last_err}")


# ── Groq: single call with retry on 429/5xx ───────────────────────────────────

def _groq_call(payload: dict, groq_key: str) -> dict:
    try:
        return _openai_compat_call(GROQ_API_URL, groq_key, payload, GROQ_MAX_RETRIES)
    except Exception as e:
        raise Exception(f"Groq failed: {e}")


def _agent_loop_groq(company_name: str, max_iterations: int = 6) -> dict:
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        raise Exception("GROQ_API_KEY not set")

    messages = [{"role": "user", "content": f"Research this company for me: {company_name}"}]
    steps = []

    for _ in range(max_iterations):
        payload = {
            "model":       GROQ_MODEL,
            "messages":    [{"role": "system", "content": SYSTEM_PROMPT}] + messages,
            "tools":       TOOLS,
            "tool_choice": "auto",
            "temperature": 0.1,
            "max_tokens":  2000,
        }
        response = _groq_call(payload, groq_key)

        choice = response.get("choices", [{}])[0]
        msg    = choice.get("message", {})
        reason = choice.get("finish_reason", "")
        messages.append(msg)

        if reason == "tool_calls" and msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                fn_name = tc["function"]["name"]
                try:
                    fn_args = json.loads(tc["function"]["arguments"])
                except Exception:
                    fn_args = {}
                step = {"tool": fn_name, "args": fn_args}
                tool_result = _dispatch(fn_name, fn_args)
                step["result_preview"] = tool_result[:200]
                steps.append(step)
                messages.append({
                    "role":         "tool",
                    "tool_call_id": tc["id"],
                    "content":      tool_result,
                })
            continue

        content = (msg.get("content") or "").strip()
        brief = _parse_brief(content)
        brief.update({"company": company_name, "steps": steps, "model": GROQ_MODEL})
        return brief

    raise Exception("Groq agent: max iterations reached without completing")


# ── Together.ai: llama fallback (same model, different host) ──────────────────

def _agent_loop_together(company_name: str, max_iterations: int = 6) -> dict:
    together_key = os.environ.get("TOGETHER_API_KEY", "")
    if not together_key:
        raise Exception("TOGETHER_API_KEY not set")

    messages = [{"role": "user", "content": f"Research this company for me: {company_name}"}]
    steps = []

    for _ in range(max_iterations):
        payload = {
            "model":       TOGETHER_MODEL,
            "messages":    [{"role": "system", "content": SYSTEM_PROMPT}] + messages,
            "tools":       TOOLS,
            "tool_choice": "auto",
            "temperature": 0.1,
            "max_tokens":  2000,
        }
        response = _openai_compat_call(TOGETHER_API_URL, together_key, payload)

        choice = response.get("choices", [{}])[0]
        msg    = choice.get("message", {})
        reason = choice.get("finish_reason", "")
        messages.append(msg)

        if reason == "tool_calls" and msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                fn_name = tc["function"]["name"]
                try:
                    fn_args = json.loads(tc["function"]["arguments"])
                except Exception:
                    fn_args = {}
                step = {"tool": fn_name, "args": fn_args}
                tool_result = _dispatch(fn_name, fn_args)
                step["result_preview"] = tool_result[:200]
                steps.append(step)
                messages.append({
                    "role":         "tool",
                    "tool_call_id": tc["id"],
                    "content":      tool_result,
                })
            continue

        content = (msg.get("content") or "").strip()
        brief = _parse_brief(content)
        brief.update({"company": company_name, "steps": steps, "model": f"{TOGETHER_MODEL} (together fallback)"})
        return brief

    raise Exception("Together agent: max iterations reached without completing")


# ── Claude: fallback agent loop ───────────────────────────────────────────────

def _agent_loop_claude(company_name: str, max_iterations: int = 6) -> dict:
    try:
        import anthropic as _anthropic
    except ImportError:
        raise Exception("anthropic package not installed — add it to requirements.txt")

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise Exception("ANTHROPIC_API_KEY not set in environment")

    client = _anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": f"Research this company for me: {company_name}"}]
    steps = []

    for _ in range(max_iterations):
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2000,
            system=SYSTEM_PROMPT,
            tools=CLAUDE_TOOLS,
            messages=messages,
        )

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    step = {"tool": block.name, "args": block.input}
                    tool_result = _dispatch(block.name, block.input)
                    step["result_preview"] = tool_result[:200]
                    steps.append(step)
                    tool_results.append({
                        "type":         "tool_result",
                        "tool_use_id":  block.id,
                        "content":      tool_result,
                    })
            messages.append({"role": "user", "content": tool_results})
            continue

        content = "".join(
            getattr(block, "text", "") for block in response.content
        ).strip()
        brief = _parse_brief(content)
        brief.update({"company": company_name, "steps": steps, "model": f"{CLAUDE_MODEL} (fallback)"})
        return brief

    raise Exception("Claude agent: max iterations reached without completing")


# ── Main entry point ──────────────────────────────────────────────────────────

def run_research_agent(company_name: str, max_iterations: int = 6) -> dict:
    """
    Try Groq → Together.ai → Claude haiku, returning the first success.
    """
    errors = []

    try:
        return _agent_loop_groq(company_name, max_iterations)
    except Exception as e:
        errors.append(f"Groq: {e}")

    if os.environ.get("TOGETHER_API_KEY"):
        try:
            result = _agent_loop_together(company_name, max_iterations)
            result["groq_error"] = errors[0]
            return result
        except Exception as e:
            errors.append(f"Together: {e}")

    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            result = _agent_loop_claude(company_name, max_iterations)
            result["prior_errors"] = errors
            return result
        except Exception as e:
            errors.append(f"Claude: {e}")

    return {"error": f"All providers failed. {' | '.join(errors)}", "company": company_name}
