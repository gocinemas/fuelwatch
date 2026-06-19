"""
Brand Data Enricher

Fetches real brand fundamentals from free sources:
1. Wikidata API - founding year, HQ, website, founders
2. SEC Edgar - revenue for public parent companies
3. Groq - synthesize, verify, fill gaps

No Wikipedia as primary source (unreliable for brand data).
"""

import requests
import json
import re
from datetime import datetime
import os

GROQ_API_KEY = os.getenv('GROQ_API_KEY')
try:
    import groq as groq_module
    groq_client = groq_module.Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
except:
    groq_client = None

def fetch_wikidata_brand(brand_name: str) -> dict:
    """
    Fetch brand fundamentals from Wikidata using SPARQL API.
    Returns: {founding_year, headquarters, website, founders, industry}
    """
    try:
        # SPARQL query to find brand by label (also search for brand articles, companies)
        sparql_query = f"""
        SELECT ?item ?itemLabel ?founded ?foundedLabel ?hq ?hqLabel ?website ?industry ?industryLabel ?creator ?creatorLabel
        WHERE {{
            ?item rdfs:label "{brand_name}"@en .
            {{ ?item wdt:P31 wd:Q431289 }} UNION {{ ?item wdt:P31 wd:Q6881115 }} UNION {{ ?item wdt:P31 wd:Q783794 }} .
            OPTIONAL {{ ?item wdt:P571 ?founded . }}  # inception/founding date
            OPTIONAL {{ ?item wdt:P159 ?hq . }}  # headquarters location
            OPTIONAL {{ ?item wdt:P856 ?website . }}  # official website
            OPTIONAL {{ ?item wdt:P452 ?industry . }}  # industry
            OPTIONAL {{ ?item wdt:P170 ?creator . }}  # creator/manufacturer
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        LIMIT 1
        """

        url = "https://query.wikidata.org/sparql"
        headers = {"User-Agent": "Miru/1.0 (brand enrichment)"}
        params = {"query": sparql_query, "format": "json"}

        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data.get("results", {}).get("bindings"):
            return {}

        result = data["results"]["bindings"][0]

        # Extract founding year
        founding_year = None
        if "founded" in result:
            founded_val = result["founded"]["value"]
            # Parse ISO date format (YYYY-MM-DD)
            founding_year = founded_val.split("-")[0] if "-" in founded_val else founded_val

        # Extract HQ location
        hq = None
        if "hqLabel" in result:
            hq = result["hqLabel"]["value"]

        # Extract website
        website = None
        if "website" in result:
            website = result["website"]["value"]
            # Clean URL
            if website.startswith("http"):
                website = website.replace("https://", "").replace("http://", "").rstrip("/")

        return {
            "founding_year": founding_year,
            "headquarters": hq,
            "website": website,
            "industry": result.get("industryLabel", {}).get("value"),
            "source": "wikidata",
            "confidence": 0.85
        }

    except Exception as e:
        print(f"[enricher] Wikidata fetch failed for '{brand_name}': {e}")
        return {}


def fetch_sec_edgar_financials(company_name: str, parent_company: str = None) -> dict:
    """
    Fetch financials from SEC Edgar for public companies.
    Returns: {revenue, net_income, market_cap, year}
    """
    try:
        # Try parent company first if provided
        search_name = parent_company or company_name

        # SEC Edgar company search
        url = "https://data.sec.gov/submissions/CIK0000000789.json"  # Example CIK

        # This is simplified - real implementation would:
        # 1. Search SEC for company CIK
        # 2. Fetch 10-K filings
        # 3. Extract financial data

        # For now, return empty - requires proper SEC integration
        return {}

    except Exception as e:
        print(f"[enricher] SEC Edgar fetch failed for '{company_name}': {e}")
        return {}


def synthesize_with_groq(brand_name: str, parent_company: str, wikidata: dict, sec: dict) -> dict:
    """
    Use Groq to synthesize brand data, verify consistency, fill gaps.
    """
    if not groq_client:
        return {}

    try:
        context = f"""
Brand: {brand_name}
Parent Company: {parent_company}

Wikidata findings:
- Founding Year: {wikidata.get('founding_year', 'N/A')}
- Headquarters: {wikidata.get('headquarters', 'N/A')}
- Website: {wikidata.get('website', 'N/A')}
- Industry: {wikidata.get('industry', 'N/A')}

SEC Edgar findings (if parent company public):
- Revenue: {sec.get('revenue', 'N/A')}
- Net Income: {sec.get('net_income', 'N/A')}

Task:
1. Verify consistency across sources
2. Fill any missing fields using reasoning about the brand
3. Generate confidence scores (0-100) for each field
4. Flag any inconsistencies

Return ONLY valid JSON with fields: founding_year, headquarters, website, industry, revenue, confidence_scores
"""

        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": context}],
            temperature=0.3,
            max_tokens=500,
            timeout=5
        )

        result_text = response.choices[0].message.content.strip()

        # Try to parse JSON from response
        try:
            return json.loads(result_text)
        except:
            # Extract JSON if embedded in text
            json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(0))
            return {}

    except Exception as e:
        print(f"[enricher] Groq synthesis failed: {e}")
        return {}


def enrich_brand_data(brand_name: str, parent_company: str = None) -> dict:
    """
    Complete brand data enrichment pipeline.

    Returns:
    {
        brand: brand_name,
        parent_company: parent_company,
        founding_year: "YYYY",
        headquarters: "City, Country",
        website: "brand.com",
        industry: "Cosmetics",
        revenue: "$XXX Million",
        confidence_scores: {...},
        sources: ["wikidata", "sec_edgar", "groq"],
        timestamp: "ISO datetime"
    }
    """

    print(f"[enricher] Starting enrichment for {brand_name} (parent: {parent_company})")

    # Step 1: Fetch from Wikidata
    wikidata = fetch_wikidata_brand(brand_name)
    print(f"[enricher] Wikidata: {bool(wikidata)}")

    # Step 2: Fetch from SEC Edgar if parent company provided
    sec = fetch_sec_edgar_financials(brand_name, parent_company)
    print(f"[enricher] SEC Edgar: {bool(sec)}")

    # Step 3: Synthesize with Groq
    synthesized = synthesize_with_groq(brand_name, parent_company or "", wikidata, sec)
    print(f"[enricher] Groq synthesis: {bool(synthesized)}")

    # Step 4: Merge results
    result = {
        "brand": brand_name,
        "parent_company": parent_company,
        "founding_year": synthesized.get("founding_year") or wikidata.get("founding_year"),
        "headquarters": synthesized.get("headquarters") or wikidata.get("headquarters"),
        "website": synthesized.get("website") or wikidata.get("website"),
        "industry": synthesized.get("industry") or wikidata.get("industry"),
        "revenue": synthesized.get("revenue") or sec.get("revenue"),
        "confidence_scores": synthesized.get("confidence_scores", {}),
        "sources": ["wikidata"] + (["sec_edgar"] if sec else []) + (["groq"] if synthesized else []),
        "timestamp": datetime.now().isoformat()
    }

    print(f"[enricher] Enrichment complete for {brand_name}")
    return result


if __name__ == "__main__":
    # Test
    data = enrich_brand_data("Olay", "Procter & Gamble")
    print(json.dumps(data, indent=2))
