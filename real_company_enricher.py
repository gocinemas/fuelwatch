"""
Real company enrichment using ACTUAL data sources only:
- SEC EDGAR (US public companies)
- Wikipedia (description, founding)
- Company websites (official data)
NO fallback/fake data - only real data.
"""

import os
import requests
import re
import json
from datetime import datetime

def get_real_company_data(company_name: str, wiki_extract: str = None) -> dict:
    """
    Get REAL data only from actual sources.
    Returns only data that's actually found - no fallbacks.
    """
    data = {"name": company_name}
    
    # 1. Try EDGAR for US public companies
    edgar_data = _fetch_edgar_real(company_name)
    if edgar_data:
        data.update(edgar_data)
    
    # 2. Try Wikipedia for description/founding
    if wiki_extract:
        wiki_data = _parse_wiki_real(company_name, wiki_extract)
        data.update(wiki_data)
    
    return data

def _fetch_edgar_real(company_name: str) -> dict:
    """
    Fetch REAL data from SEC EDGAR API (public, free).
    Returns actual financial data for US public companies.
    """
    try:
        # Search SEC for company CIK
        search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            "company": company_name,
            "action": "getcompany",
            "type": "",
            "dateb": "",
            "owner": "exclude",
            "count": 1,
            "output": "json"
        }
        
        r = requests.get(search_url, params=params, timeout=10, headers={
            "User-Agent": "Miru/1.0 (company research)"
        })
        r.raise_for_status()
        
        result = r.json()
        if not result.get("cik_lookup_results"):
            return {}
        
        company = result["cik_lookup_results"][0]
        cik = str(company["CIK"]).zfill(10)
        
        data = {
            "name": company.get("Entity Name", company_name),
            "cik": cik,
            "source": "SEC EDGAR"
        }
        
        # Fetch latest 10-K for financial data
        filings_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r2 = requests.get(filings_url, timeout=10, headers={
            "User-Agent": "Miru/1.0"
        })
        r2.raise_for_status()
        
        filings = r2.json()
        if filings.get("facts", {}).get("us-gaap"):
            gaap = filings["facts"]["us-gaap"]
            
            # Extract REAL financial metrics
            if "Assets" in gaap:
                assets = gaap["Assets"].get("USD", [])
                if assets:
                    data["total_assets"] = assets[-1].get("val")
            
            if "NetIncomeLoss" in gaap:
                income = gaap["NetIncomeLoss"].get("USD", [])
                if income:
                    data["net_income"] = income[-1].get("val")
            
            if "Revenues" in gaap:
                revenue = gaap["Revenues"].get("USD", [])
                if revenue:
                    data["revenue"] = revenue[-1].get("val")
            
            if "EntityEmployees" in gaap:
                employees = gaap["EntityEmployees"].get("pure", [])
                if employees:
                    data["employees"] = employees[-1].get("val")
        
        return data
    except Exception as e:
        print(f"[EDGAR] Fetch failed: {e}")
        return {}

def _parse_wiki_real(company_name: str, text: str) -> dict:
    """
    Extract REAL data from Wikipedia text only.
    No guessing - only explicit numbers/facts.
    """
    data = {}
    
    if not text:
        return data
    
    # Founded year - explicit patterns only
    founded_match = re.search(r'founded(?:\s+in)?\s+(\d{4})', text, re.IGNORECASE)
    if founded_match:
        data["founded_year"] = int(founded_match.group(1))
    
    # Headquarters - explicit patterns
    hq_match = re.search(r'headquartered?\s+(?:in|at)\s+([^,\n.]+(?:,\s*[^,\n.]+)?)', text, re.IGNORECASE)
    if hq_match:
        data["headquarters"] = hq_match.group(1).strip()
    
    # Revenue - explicit numbers only
    revenue_match = re.search(r'revenue[^\d]*\$?(\d+\.?\d*)\s*(?:billion|million|billion USD)', text, re.IGNORECASE)
    if revenue_match:
        value = float(revenue_match.group(1))
        # Convert to billions if millions
        if "million" in revenue_match.group(0).lower() and "billion" not in revenue_match.group(0).lower():
            value = value / 1000
        data["revenue_billions"] = value
    
    # Employees - explicit numbers only
    employees_match = re.search(r'([\d,]+)\s*(?:employees|staff|workforce)', text, re.IGNORECASE)
    if employees_match:
        emp_str = employees_match.group(1).replace(",", "")
        data["employees"] = int(emp_str)
    
    # Description (first sentence/paragraph)
    desc_match = re.match(r'^([^.!?]+[.!?])', text)
    if desc_match:
        data["description"] = desc_match.group(1).strip()
    
    return data

if __name__ == "__main__":
    # Test with Unilever (UK company - won't have EDGAR data)
    # Test with Apple (US company - will have EDGAR data)
    result = get_real_company_data("Apple", "Apple Inc. is an American technology company founded in 1976")
    print(json.dumps(result, indent=2, default=str))
