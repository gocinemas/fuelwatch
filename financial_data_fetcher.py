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
    """Parse financial data from Wikipedia or other text"""
    data = {}

    if not text:
        return _get_fallback_data(company_name)

    # Try regex patterns first for explicit numbers
    # Revenue patterns: "$XX billion", "£XX billion", "revenue of $X"
    revenue_patterns = [
        r'revenue[^\d]*(\d+\.?\d*)\s*billion',
        r'\$(\d+\.?\d*)\s*billion.*revenue',
        r'revenue.*\$(\d+\.?\d*)\s*b',
        r'(\d+\.?\d*)\s*billion.*revenue',
    ]

    for pattern in revenue_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                data["revenue_billions"] = float(match.group(1))
                break
            except:
                pass

    # Employee patterns: "XX,000 employees", "XX thousand employees"
    employee_patterns = [
        r'([\d,]+)\s*employees',
        r'employ[^s]*:\s*(\d+(?:,\d{3})*)',
        r'workforce.*?([\d,]+)',
    ]

    for pattern in employee_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                emp_str = match.group(1).replace(",", "")
                data["employees"] = int(emp_str)
                break
            except:
                pass

    # If regex found something, return it
    if data:
        return data

    # Try AI parsing if Groq available
    if GROQ_API_KEY:
        try:
            from groq import Groq
            client = Groq(api_key=GROQ_API_KEY)

            prompt = f"""Extract financial data for {company_name}:
Text: {text[:1500]}

Find exact numbers only for:
1. Annual revenue (in billions USD)
2. Total employees
3. Founded year
4. Headquarters location

Return JSON: {{"revenue_billions": null, "employees": null}}"""

            response = client.messages.create(
                model="mixtral-8x7b-32768",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=200
            )

            import json
            try:
                result = json.loads(response.choices[0].message.content)
                return {k: v for k, v in result.items() if v is not None}
            except:
                pass
        except Exception as e:
            print(f"[financial_data] AI parsing failed: {e}")

    # Fallback to known data for major companies
    return _get_fallback_data(company_name)

def _get_fallback_data(company_name: str) -> dict:
    """Fallback financial data for major known companies"""
    # Known major companies' financial data (latest available)
    known_data = {
        "unilever": {"revenue_billions": 60.1, "employees": 128000},
        "reckitt": {"revenue_billions": 16.8, "employees": 21000},
        "henkel": {"revenue_billions": 21.7, "employees": 53000},
        "nestlé": {"revenue_billions": 95.7, "employees": 301000},
        "procter & gamble": {"revenue_billions": 80.0, "employees": 105000},
        "coca-cola": {"revenue_billions": 43.0, "employees": 200000},
        "pepsico": {"revenue_billions": 91.5, "employees": 309000},
        "microsoft": {"revenue_billions": 198.3, "employees": 220000},
        "apple": {"revenue_billions": 383.3, "employees": 161000},
        "amazon": {"revenue_billions": 575.0, "employees": 1500000},
        "google": {"revenue_billions": 307.4, "employees": 190234},
        "meta": {"revenue_billions": 114.9, "employees": 67317},
    }

    normalized = company_name.lower().strip()
    return known_data.get(normalized, {})

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
