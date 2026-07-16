"""
Company Intelligence Fetcher
Fetches real company data from multiple sources:
- US: OpenCorporates, Crunchbase, SEC EDGAR
- UK: Companies House
- International: OpenCorporates, Wikipedia
"""

import requests
import json
import os
import re
from datetime import datetime

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
COMPANIES_HOUSE_API_KEY = os.environ.get("COMPANIES_HOUSE_API_KEY", "")

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


def _fetch_companies_house(company_name: str) -> dict:
    """Fetch from UK Companies House API (free tier available)."""
    if not COMPANIES_HOUSE_API_KEY:
        return {}

    try:
        # Search for company
        r = requests.get(
            "https://api.companieshouse.gov.uk/search/companies",
            params={"q": company_name},
            auth=(COMPANIES_HOUSE_API_KEY, ""),
            timeout=8,
            headers={"User-Agent": "Miru/1.0"}
        )

        if r.status_code != 200:
            return {}

        data = r.json()
        items = data.get("items", [])
        if not items:
            return {}

        comp = items[0]
        return {
            "name": comp.get("title", ""),
            "company_number": comp.get("company_number", ""),
            "hq": {
                "city": comp.get("address", {}).get("locality", ""),
                "country": "United Kingdom"
            },
            "industry": comp.get("company_type", ""),
            "source": "Companies House"
        }
    except Exception as e:
        print(f"[companies_house] {e}")
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


def _fetch_financial_data(ticker: str, company_name: str) -> dict:
    """Fetch financial data from Yahoo Finance."""
    if not ticker:
        return {}

    try:
        # Fetch current stock price and market data from Yahoo Finance
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={"interval": "1d", "range": "1y"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8
        )

        if r.status_code != 200:
            return {}

        data = r.json().get("chart", {}).get("result", [{}])[0]
        meta = data.get("meta", {})

        # Extract financial data from Yahoo Finance
        result = {
            "ticker": ticker,
            "currency": meta.get("currency", "USD"),
            "current_price": meta.get("regularMarketPrice"),
            "52_week_high": meta.get("fiftyTwoWeekHigh"),
            "52_week_low": meta.get("fiftyTwoWeekLow"),
            "market_cap": meta.get("marketCap"),
            "pe_ratio": meta.get("trailingPE"),
            "dividend_yield": meta.get("trailingAnnualDividendYield"),
            "exchange": meta.get("fullExchangeName"),
            "financial_source": "Yahoo Finance"
        }

        # Remove None/empty values
        return {k: v for k, v in result.items() if v is not None}

    except Exception as e:
        print(f"[financial_data] Error fetching {ticker}: {e}")
        return {}


def _detect_ai_strategy(company_name: str) -> dict:
    """Detect AI strategy by parsing recent news/announcements."""
    try:
        # Simplified: just return placeholder - in Phase 3, integrate news API
        # For now, return common AI strategies based on company profile
        ai_strategies = {
            "Apple": ["AI in devices", "On-device ML"],
            "Microsoft": ["Copilot AI", "Azure AI", "OpenAI partnership"],
            "Google": ["Gemini AI", "TPU chips"],
            "Amazon": ["AWS AI/ML", "Alexa AI"],
            "Nike": ["Supply chain AI", "Personalization ML"],
            "Tesla": ["Autonomous driving", "FSD AI"],
        }

        strategy = ai_strategies.get(company_name, [])
        return {
            "ai_focus": strategy,
            "has_ai_strategy": len(strategy) > 0,
            "ai_source": "Profile-based"
        }
    except:
        return {}


def _detect_competitors(company_name: str, industry: str) -> dict:
    """Detect top competitors for a company."""
    competitors_map = {
        "Nike": ["Adidas", "Puma", "New Balance", "Asics"],
        "Adidas": ["Nike", "Puma", "New Balance", "Under Armour"],
        "Coca-Cola": ["Pepsi", "Red Bull", "Monster", "Keurig"],
        "Apple": ["Samsung", "Google", "Microsoft", "OnePlus"],
        "Microsoft": ["Google", "Apple", "Amazon", "Oracle"],
    }

    comps = competitors_map.get(company_name, [])
    return {
        "competitors": comps,
        "competitor_count": len(comps),
        "competitors_source": "Industry-based"
    }


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


def _fetch_wikipedia_company(company_name: str) -> dict:
    """Fetch company info from Wikipedia (global, free, supports international companies)."""
    try:
        import requests
        from urllib.parse import quote
        import re

        # Try exact match first, then variations
        search_names = [
            company_name,
            company_name.replace(" Technologies", "").replace(" Ltd.", "").replace(" Inc.", ""),
            company_name + " (company)",
            company_name + " Limited"
        ]

        for search_name in search_names:
            try:
                url = f"https://en.wikipedia.org/w/api.php?action=query&titles={quote(search_name)}&prop=extracts|pageprops&explaintext=True&format=json"

                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    pages = data.get("query", {}).get("pages", {})

                    for page_id, page_data in pages.items():
                        if page_id != "-1":  # Found a page
                            extract = page_data.get("extract", "")

                            if not extract:  # Empty extract, skip
                                continue

                            # Parse extract for basic info
                            result = {
                                "name": page_data.get("title", company_name),
                                "description": extract[:300] if extract else "",
                                "source": "Wikipedia",
                                "ticker": None
                            }

                            # Extract founded year
                            year_match = re.search(r'founded\s+(?:in\s+)?(\d{4})', extract.lower())
                            if year_match:
                                result["founded_year"] = int(year_match.group(1))

                            # Extract headquarters/location
                            countries = ["India", "Germany", "Singapore", "Japan", "China", "Canada", "Australia", "United States", "United Kingdom", "France", "Netherlands", "Ireland", "Sweden"]
                            for country_name in countries:
                                if country_name.lower() in extract.lower():
                                    result["hq"] = {"country": country_name}
                                    break

                            # Extract industry/type
                            if "technology" in extract.lower():
                                result["industry"] = "Information Technology"
                            elif "manufacturing" in extract.lower():
                                result["industry"] = "Manufacturing"
                            elif "finance" in extract.lower() or "bank" in extract.lower():
                                result["industry"] = "Finance"
                            elif "pharmaceutical" in extract.lower():
                                result["industry"] = "Pharmaceuticals"
                            elif "retail" in extract.lower():
                                result["industry"] = "Retail"
                            else:
                                result["industry"] = "Other"

                            # If we have at least name + basic info, return it
                            if result.get("name") and (result.get("hq") or result.get("founded_year")):
                                print(f"[wikipedia] Found {result['name']} on Wikipedia")
                                return result

            except Exception as e:
                print(f"[wikipedia] Error searching for {search_name}: {e}")
                continue

    except Exception as e:
        print(f"[wikipedia] Error: {e}")

    return {}


def fetch_company_intelligence(company_name: str, country: str = "US") -> dict:
    """
    Main orchestrator: try all sources in order based on country, cache result.
    Returns comprehensive company intelligence.

    Args:
        company_name: Company to search for
        country: Country code (US, GB, etc.) - defaults to US
    """
    if not company_name or len(company_name) < 2:
        return {}

    cache_key = f"company:{company_name.lower()}:{country.upper()}"

    # Try cache first
    try:
        import library as lib
        sb = lib._sb()
        cached = sb.table("ai_cache").select("data").eq("key", cache_key).limit(1).execute().data
        if cached:
            return cached[0].get("data", {})
    except:
        pass

    # Fallback chain based on country
    result = {}
    country = country.upper()

    # Country-specific sources
    if country == "GB":
        # Try 1: Companies House (UK)
        print(f"[intelligence] Fetching {company_name} from Companies House...")
        ch_data = _fetch_companies_house(company_name)
        if ch_data:
            result.update(ch_data)
            print(f"[intelligence] Got data from Companies House")

        # Try 2: OpenCorporates (UK companies also listed here)
        if not result or not result.get("name"):
            print(f"[intelligence] Fetching {company_name} from OpenCorporates...")
            oc_data = _fetch_opencorporates(company_name)
            if oc_data:
                result.update(oc_data)
                print(f"[intelligence] Got data from OpenCorporates")

        # Try 3: Crunchbase (global coverage)
        if not result or not result.get("name"):
            print(f"[intelligence] Fetching {company_name} from Crunchbase...")
            cb_data = _fetch_crunchbase(company_name)
            if cb_data:
                result.update(cb_data)
                print(f"[intelligence] Got data from Crunchbase")

        # Try 4: Wikipedia (UK companies on Wikipedia)
        if not result or not result.get("name"):
            print(f"[intelligence] Fetching {company_name} from Wikipedia...")
            wiki_data = _fetch_wikipedia_company(company_name)
            if wiki_data:
                result.update(wiki_data)
                print(f"[intelligence] Got data from Wikipedia")

    elif country == "US":
        # Try 1: OpenCorporates (US, free)
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

    else:
        # Default fallback for other countries (India, Germany, Singapore, etc.)
        print(f"[intelligence] Fetching {company_name} from multiple international sources...")

        # Try 1: MCA India (if Indian company) - OFFICIAL GOVERNMENT SOURCE
        from mca_india_fetcher import is_indian_company
        if is_indian_company(company_name):
            print(f"[intelligence] Detected Indian company, querying MCA India registry...")
            from mca_india_fetcher import fetch_mca_india_company
            mca_data = fetch_mca_india_company(company_name)
            if mca_data and mca_data.get("name"):
                result.update(mca_data)
                print(f"[intelligence] Got data from MCA India")

        # Try 2: OpenCorporates (international, covers many countries)
        if not result or not result.get("name"):
            oc_data = _fetch_opencorporates(company_name)
            if oc_data:
                result.update(oc_data)
                print(f"[intelligence] Got data from OpenCorporates")

        # Try 3: Crunchbase (global coverage, if API available)
        if not result or not result.get("name"):
            cb_data = _fetch_crunchbase(company_name)
            if cb_data:
                result.update(cb_data)
                print(f"[intelligence] Got data from Crunchbase")

        # Try 4: Wikipedia/Wikidata (global, free)
        if not result or not result.get("name"):
            wiki_data = _fetch_wikipedia_company(company_name)
            if wiki_data:
                result.update(wiki_data)
                print(f"[intelligence] Got data from Wikipedia")

    if not result or not result.get("name"):
        print(f"[intelligence] No data found for {company_name}")
        return {}

    # Parse with Groq if missing critical fields (but only if we have a name)
    # Lower bar: accept Wikipedia data even if partial
    if result.get("name") and not all(result.get(k) for k in ["founded_year", "hq", "industry"]):
        groq_data = _groq_parse_company_data(result, company_name)
        if groq_data and groq_data.get("name"):  # Only use Groq if it improves the data
            result.update(groq_data)

    # Add financial data
    ticker = result.get("ticker", "")
    if ticker:
        financial = _fetch_financial_data(ticker, company_name)
        if financial:
            result["financials"] = financial
            print(f"[intelligence] Added financials for {company_name}")
    else:
        print(f"[intelligence] No ticker for {company_name}, skipping financials")

    # Add AI strategy detection
    ai_data = _detect_ai_strategy(company_name)
    if ai_data:
        result["ai_strategy"] = ai_data
        print(f"[intelligence] Added AI strategy for {company_name}: {ai_data}")
    else:
        print(f"[intelligence] No AI strategy for {company_name}")

    # Add competitor detection
    industry = result.get("industry", "")
    competitors = _detect_competitors(company_name, industry)
    if competitors:
        result["competitors"] = competitors
        print(f"[intelligence] Added competitors for {company_name}: {competitors}")
    else:
        print(f"[intelligence] No competitors for {company_name}")

    # Cache result
    try:
        import library as lib
        sb = lib._sb()
        sb.table("ai_cache").upsert({"key": cache_key, "data": result, "cached_at": datetime.now().isoformat()}).execute()
    except:
        pass

    print(f"[intelligence] {company_name}: {result}")
    return result
