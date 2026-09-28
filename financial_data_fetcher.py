"""
Financial data fetcher - enriches company data with revenue, employees, etc.
Uses free public sources: Wikipedia, SEC filings, and AI parsing.
"""

import os
import requests
import re
from datetime import datetime

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

def fetch_financial_data(company_name: str, wiki_extract: str = None) -> dict:
    """
    Fetch financial data (revenue, employees) from available sources.
    Uses Wikipedia extract + AI parsing if available.
    """
    data = {}
    
    # Try to extract from Wikipedia text if provided
    if wiki_extract:
        data = _parse_financial_from_text(company_name, wiki_extract)
    
    # Try SEC Edgar for US public companies
    if not data.get("revenue"):
        data.update(_fetch_sec_basic(company_name) or {})
    
    return data

def _parse_financial_from_text(company_name: str, text: str) -> dict:
    """Parse financial data from Wikipedia or other text using AI"""
    if not GROQ_API_KEY or not text:
        return {}
    
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_API_KEY)
        
        prompt = f"""From this text about {company_name}, extract:
1. Revenue (annual in billions USD)
2. Number of employees
3. Founded year (if not already known)
4. Headquarters

Text: {text[:1000]}

Return as JSON only: {{"revenue_billions": null, "employees": null, "founded_year": null, "headquarters": null}}
Only include fields you find explicit numbers for."""
        
        response = client.messages.create(
            model="mixtral-8x7b-32768",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=200
        )
        
        text = response.choices[0].message.content
        import json
        try:
            result = json.loads(text)
            return {k: v for k, v in result.items() if v is not None}
        except:
            pass
    except Exception as e:
        print(f"[financial_data] AI parsing failed: {e}")
    
    return {}

def _fetch_sec_basic(company_name: str) -> dict:
    """Fetch basic data from SEC Edgar for US public companies"""
    try:
        # Search SEC Edgar for company
        params = {
            "action": "cik_lookup",
            "company": company_name,
            "type": "company",
            "dateb": "",
            "owner": "exclude",
            "count": 1,
            "output": "json"
        }
        
        url = "https://data.sec.gov/submissions/cik_lookup.json"
        r = requests.get(url, params=params, timeout=5)
        r.raise_for_status()
        
        # This is a public endpoint but returns limited data
        # For real financial data, would need to parse actual filings
        
        return {}
    except Exception as e:
        print(f"[financial_data] SEC Edgar fetch failed: {e}")
        return {}

if __name__ == "__main__":
    result = fetch_financial_data("Unilever", 
        "Unilever PLC is a British multinational consumer packaged goods company headquartered in London, England. "
        "It was founded in 1930. The company has over 130,000 employees and operates in 190 countries with revenue "
        "exceeding £50 billion.")
    print(result)
