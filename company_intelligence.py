"""
Company Intelligence Fetcher
Fetches real company data from OpenCorporates, Crunchbase, EDGAR
Uses fallback chain: OpenCorporates → Crunchbase → EDGAR
Caches results in Supabase to avoid re-fetching
"""

import requests
import json
import os
import re
from datetime import datetime

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

# Known company CIKs (SEC identifiers) - build this over time
KNOWN_CIKS = {
    "Nike": "0000320187",
    "Adidas": "0001018724",
    "Coca-Cola": "0000021344",
    "Pepsi": "0000077476",
    "Apple": "0000320193",
    "Microsoft": "0000789019",
    "Amazon": "0001018724",
    "Google": "0001652044",
    "Meta": "0001326801",
    "Tesla": "0001318605",
    "Netflix": "0001564408",
    "Walmart": "0000104169",
    "Target": "0000027674",
    "Best Buy": "0000764478",
    "Unilever": "0000884996",
    "Nestlé": "0000912057",
}

def _fetch_opencorporates(company_name: str) -> dict:
    """Fetch from OpenCorporates API (free, international)."""
    try:
        r = requests.get(
            "https://api.opencorporates.com/v0.4/companies/search",
            params={"q": company_name, "jurisdiction_code": "us", "per_page": 1},
            timeout=8,
            headers={"User-Agent": "Miru/1.0"}
        )
        if r.status_code != 200:
            return {}

        data = r.json()
        companies = data.get("companies", [])
        if not companies:
            return {}

        comp = companies[0].get("company", {})
        return {
            "name": comp.get("name", ""),
            "founded_year": comp.get("incorporation_date", "")[:4] if comp.get("incorporation_date") else "",
            "hq": {
                "city": comp.get("registered_address_city", ""),
                "country": comp.get("registered_address_country_code", "")
            },
            "industry": comp.get("company_type", ""),
            "company_number": comp.get("company_number", ""),
            "source": "OpenCorporates"
        }
    except Exception as e:
        print(f"[opencorporates] {e}")
        return {}


def _fetch_crunchbase(company_name: str) -> dict:
    """Fetch from Crunchbase free tier (API key required)."""
    api_key = os.environ.get("CRUNCHBASE_API_KEY", "")
    if not api_key:
        return {}

    try:
        r = requests.post(
            "https://api.crunchbase.com/api/v4/autocompletes",
            json={"query": company_name, "limit": 1},
            headers={"User-Agent": "Miru/1.0", "X-CB-User-Key": api_key},
            timeout=8
        )
        if r.status_code != 200:
            return {}

        data = r.json()
        entities = data.get("entities", [])
        if not entities:
            return {}

        entity = entities[0]
        return {
            "name": entity.get("name", ""),
            "founded_year": entity.get("founded_year", ""),
            "hq": {
                "city": entity.get("city", ""),
                "country": entity.get("country", "")
            },
            "industry": entity.get("primary_category", ""),
            "employees": entity.get("num_employees", ""),
            "source": "Crunchbase"
        }
    except Exception as e:
        print(f"[crunchbase] {e}")
        return {}


def _get_cik_from_db(company_name: str) -> str:
    """Fetch CIK from database cache."""
    try:
        import library as lib
        sb = lib._sb()
        result = sb.table("cik_lookup").select("cik").eq("company_name_lower", company_name.lower()).limit(1).execute().data
        if result:
            return result[0].get("cik", "")
    except:
        pass
    return ""


def _save_cik_to_db(company_name: str, cik: str):
    """Save CIK to database cache for future lookups."""
    try:
        import library as lib
        sb = lib._sb()
        sb.table("cik_lookup").upsert({
            "company_name": company_name,
            "company_name_lower": company_name.lower(),
            "cik": cik,
            "cached_at": datetime.now().isoformat()
        }).execute()
    except:
        pass


def _fetch_edgar(company_name: str) -> dict:
    """Fetch from SEC EDGAR via data.sec.gov API (free, US public companies only)."""
    try:
        # Step 1: Look up CIK - first from DB cache, then hardcoded, then return empty
        cik = _get_cik_from_db(company_name)
        if not cik:
            for known_name, known_cik in KNOWN_CIKS.items():
                if company_name.lower() == known_name.lower() or company_name.lower() in known_name.lower():
                    cik = known_cik
                    _save_cik_to_db(company_name, cik)
                    break

        if not cik:
            return {}

        # Step 2: Fetch company data from data.sec.gov
        r = requests.get(
            f"https://data.sec.gov/submissions/CIK{int(cik):0>10}.json",
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0"}
        )

        if r.status_code != 200:
            return {}

        comp_data = r.json()

        # Extract company information
        result = {
            "name": comp_data.get("name", ""),
            "cik": cik,
            "ticker": comp_data.get("tickers", [""])[0] if comp_data.get("tickers") else "",
            "hq": {
                "city": comp_data.get("addresses", {}).get("business", {}).get("city", ""),
                "state": comp_data.get("addresses", {}).get("business", {}).get("stateOrCountry", ""),
                "country": "United States"
            },
            "industry": comp_data.get("sicDescription", ""),
            "source": "EDGAR"
        }

        return {k: v for k, v in result.items() if v}  # Remove empty fields

    except Exception as e:
        print(f"[edgar] {e}")
        return {}


def _groq_parse_company_data(raw_data: dict, company_name: str) -> dict:
    """Use Groq to parse/structure company data from multiple sources."""
    if not GROQ_API_KEY:
        return raw_data

    try:
        prompt = f"""Given this company data, structure it as JSON. Fill in missing fields with empty strings.

Company name: {company_name}
Data: {json.dumps(raw_data)}

Return ONLY this JSON structure, no markdown:
{{
  "name": "company name",
  "founded_year": "YYYY",
  "founders": ["name1", "name2"],
  "hq": {{"city": "city", "country": "country"}},
  "industry": "industry",
  "employees": "number or empty",
  "leadership": [{{"name": "name", "title": "CEO"}}]
}}"""

        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "max_tokens": 300
            },
            timeout=6
        )

        if r.status_code != 200:
            return raw_data

        content = r.json()["choices"][0]["message"]["content"].strip()
        m = re.search(r'\{.*\}', content, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except:
                return raw_data
    except Exception as e:
        print(f"[groq_parse] {e}")

    return raw_data


def fetch_company_intelligence(company_name: str) -> dict:
    """
    Main orchestrator: try all sources in order, cache result.
    Returns comprehensive company intelligence.
    """
    if not company_name or len(company_name) < 2:
        return {}

    cache_key = f"company:{company_name.lower()}"

    # Try cache first
    try:
        import library as lib
        sb = lib._sb()
        cached = sb.table("ai_cache").select("data").eq("key", cache_key).limit(1).execute().data
        if cached:
            return cached[0].get("data", {})
    except:
        pass

    # Fallback chain
    result = {}

    # Try 1: OpenCorporates (international, free)
    print(f"[intelligence] Fetching {company_name} from OpenCorporates...")
    oc_data = _fetch_opencorporates(company_name)
    if oc_data:
        result.update(oc_data)
        print(f"[intelligence] Got data from OpenCorporates")

    # Try 2: Crunchbase (if API key available)
    if not result or not result.get("founded_year"):
        print(f"[intelligence] Fetching {company_name} from Crunchbase...")
        cb_data = _fetch_crunchbase(company_name)
        if cb_data:
            result.update(cb_data)
            print(f"[intelligence] Got data from Crunchbase")

    # Try 3: EDGAR (US public companies)
    if not result or not result.get("founded_year"):
        print(f"[intelligence] Fetching {company_name} from EDGAR...")
        edgar_data = _fetch_edgar(company_name)
        if edgar_data:
            result.update(edgar_data)
            print(f"[intelligence] Got data from EDGAR")

    if not result:
        print(f"[intelligence] No data found for {company_name}")
        return {}

    # Parse with Groq if needed
    if not all(result.get(k) for k in ["founded_year", "hq"]):
        result = _groq_parse_company_data(result, company_name)

    # Cache result
    try:
        import library as lib
        sb = lib._sb()
        sb.table("ai_cache").upsert({"key": cache_key, "data": result, "cached_at": datetime.now().isoformat()}).execute()
    except:
        pass

    print(f"[intelligence] {company_name}: {result}")
    return result
