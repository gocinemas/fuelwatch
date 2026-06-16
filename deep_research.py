#!/usr/bin/env python3
"""
Deep Research Agent - Phase 2
Fetches EDGAR filings, recent news, and company strategy
Uses agentic AI to analyze and summarize
"""

import requests
import os
from datetime import datetime, timedelta

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
SEC_API = "https://data.sec.gov/api/xbrl"
NEWS_API = "https://newsapi.org/v2"

def _fetch_sec_filings(company_name: str, cik: str = None) -> dict:
    """Fetch recent SEC filings (10-K, 10-Q, 8-K) for a company."""
    try:
        # If no CIK, try to look it up
        if not cik:
            r = requests.get(
                "https://www.sec.gov/files/company_tickers.json",
                timeout=5,
                headers={"User-Agent": "MiruIntel/1.0"}
            )
            if r.status_code == 200:
                data = r.json()
                for ticker_data in data.values():
                    if ticker_data.get("title", "").lower() == company_name.lower():
                        cik = str(ticker_data.get("cik_str", "")).zfill(10)
                        break

        if not cik:
            return {"error": "CIK not found"}

        # Fetch recent filings
        filings_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(filings_url, timeout=10, headers={"User-Agent": "MiruIntel/1.0"})

        if r.status_code != 200:
            return {"error": "Could not fetch filings"}

        data = r.json()
        filings = data.get("filings", {}).get("recent", {})

        # Extract recent 10-K, 10-Q, 8-K
        recent = {
            "10-K": None,  # Annual report
            "10-Q": None,  # Quarterly report
            "8-K": None,   # Current events
        }

        for form_type, accession_nums, dates in zip(
            filings.get("form", []),
            filings.get("accessionNumber", []),
            filings.get("filingDate", [])
        ):
            if form_type in recent and recent[form_type] is None:
                recent[form_type] = {
                    "type": form_type,
                    "date": dates,
                    "accession": accession_nums,
                }

        return recent
    except Exception as e:
        print(f"[sec_filings] error: {e}")
        return {}

def _fetch_recent_news(company_name: str, days: int = 30) -> list:
    """Fetch recent news about the company."""
    try:
        if not NEWS_API:
            return []

        date_from = (datetime.now() - timedelta(days=days)).isoformat()

        r = requests.get(
            f"{NEWS_API}/everything",
            params={
                "q": company_name,
                "sortBy": "publishedAt",
                "language": "en",
                "apiKey": os.environ.get("NEWS_API_KEY", ""),
            },
            timeout=10
        )

        if r.status_code == 200:
            articles = r.json().get("articles", [])[:10]  # Top 10
            return [
                {
                    "title": a.get("title"),
                    "source": a.get("source", {}).get("name"),
                    "date": a.get("publishedAt"),
                    "url": a.get("url"),
                }
                for a in articles
            ]
    except Exception as e:
        print(f"[news] error: {e}")

    return []

def deep_research(company_name: str) -> dict:
    """
    Run deep research on a company using agentic AI.

    Fetches:
    - Recent SEC filings (strategy, financial performance)
    - Recent news (recent events, acquisitions, launches)
    - Uses AI to extract and summarize key insights
    """

    if not GROQ_API_KEY:
        return {"error": "Groq API key not configured"}

    print(f"[deep_research] Starting for {company_name}")

    # Fetch data sources
    filings = _fetch_sec_filings(company_name)
    news = _fetch_recent_news(company_name)

    # Prepare context for AI
    context = f"""
You are an expert business analyst. Analyze the following information about {company_name} and provide:

1. **Strategic Direction** - What is the company's strategy? Where are they headed?
2. **Recent Moves** - What major initiatives, acquisitions, or launches happened recently?
3. **Growth Drivers** - What's driving revenue/profit growth?
4. **Key Risks** - What are the main risks to the business?
5. **Financial Health** - Is the company growing or declining?

Recent News:
{chr(10).join(f"- {n['title']} ({n['date'][:10]})" for n in news[:5])}

Recent Filings Available:
{str(filings)}

Provide a concise analysis (2-3 sentences per section) that would be useful for a VP making strategic decisions.
"""

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a business intelligence analyst for VPs and CMOs."
                    },
                    {
                        "role": "user",
                        "content": context
                    }
                ],
                "max_tokens": 1000,
                "temperature": 0.7,
            },
            timeout=30
        )

        if r.status_code == 200:
            analysis = r.json()["choices"][0]["message"]["content"]
            return {
                "company": company_name,
                "analysis": analysis,
                "sources": {
                    "recent_filings": len([f for f in filings.values() if f]),
                    "recent_news": len(news),
                },
                "timestamp": datetime.now().isoformat(),
            }
        else:
            return {"error": f"Groq API error: {r.status_code}"}

    except Exception as e:
        print(f"[deep_research] error: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    # Test
    result = deep_research("Apple")
    print(result)
