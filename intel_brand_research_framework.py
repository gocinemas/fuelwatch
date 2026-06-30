"""
Intel Brand Research Framework v1
=====================================
Systematically research and populate 93 brands with REAL, TRACEABLE data.

Data Sourcing Priority:
1. Official company sources (10-K, annual reports, investor.brand.com)
2. Government records (SEC EDGAR, Companies House, regulatory filings)
3. Academic/institutional (World Bank, IMF, national statistics)
4. Verified industry (Statista, Eurostat, market research firms)
5. Official brand channels (website, social media official counts)
6. NO speculation, estimates, or AI-generated numbers

Confidence Scoring:
- SEC/10-K filing: 95%
- Companies House/Official annual report: 90%
- Yahoo Finance/Official investor relations: 85%
- Wikipedia/News sources: 70%
- Estimates/Secondary sources: 60%
- Missing: 0%

Output: JSON with source tracking for every field
"""

import json
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os

# Data Sources by Category
DATA_SOURCES = {
    "founding_year": {
        "primary": ["Wikipedia", "Wikidata", "Official company history"],
        "confidence": 85,
    },
    "headquarters": {
        "primary": ["SEC Edgar", "Companies House", "Official website"],
        "confidence": 90,
    },
    "revenue_2025": {
        "primary": ["SEC Edgar 10-K", "Companies House accounts", "Official investor relations"],
        "confidence": 95,
    },
    "market_cap": {
        "primary": ["Yahoo Finance", "Google Finance", "Official investor relations"],
        "confidence": 85,
    },
    "profit_margin": {
        "primary": ["SEC Edgar", "Companies House annual accounts"],
        "confidence": 90,
    },
    "employees": {
        "primary": ["SEC Edgar", "LinkedIn company page", "Official reports"],
        "confidence": 85,
    },
    "top_products": {
        "primary": ["Official website", "Retailer data (Amazon, boots.com, etc)"],
        "confidence": 85,
    },
    "pricing": {
        "primary": ["Official brand website", "Major retailers", "Price comparison sites"],
        "confidence": 85,
    },
    "competitors": {
        "primary": ["Industry reports", "Statista", "Yahoo Finance competitors"],
        "confidence": 75,
    },
    "social_followers": {
        "primary": ["Official @brand social media counts (real-time)"],
        "confidence": 95,
    },
    "market_share": {
        "primary": ["Statista reports", "Industry research", "Yahoo Finance"],
        "confidence": 75,
    },
}

class BrandResearchTracker:
    """Track source attribution for each data point."""

    def __init__(self, brand_name: str):
        self.brand_name = brand_name
        self.research_log = {
            "brand_name": brand_name,
            "research_date": datetime.now().isoformat(),
            "fields": {}
        }

    def add_field(self, field_name: str, value: any, source: str, source_url: str,
                  confidence: int, notes: str = ""):
        """Record a data field with source tracking."""
        self.research_log["fields"][field_name] = {
            "value": value,
            "source": source,
            "source_url": source_url,
            "confidence": confidence,
            "notes": notes,
            "captured_at": datetime.now().isoformat()
        }

    def to_dict(self) -> Dict:
        """Export research log as dictionary."""
        return self.research_log

    def to_json(self) -> str:
        """Export research log as JSON."""
        return json.dumps(self.research_log, indent=2)


class BrandDataCollector:
    """Collect brand data from verified sources."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Intel Brand Research/1.0 (data.humanagency.co)"
        })

    def fetch_sec_edgar_company(self, ticker: str) -> Dict:
        """
        Fetch company data from SEC Edgar API.
        Returns: {company_name, cik, founded_year, state, revenue, market_cap, employees}
        """
        try:
            # SEC Edgar Company Facts API
            url = f"https://data.sec.gov/submissions/CIK{ticker}.json"
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            company_name = data.get("entityName", "")
            cik = data.get("cik_str", "")

            return {
                "company_name": company_name,
                "cik": cik,
                "source": "SEC Edgar API",
                "source_url": url,
                "confidence": 95
            }
        except Exception as e:
            print(f"[SEC Edgar] Failed to fetch {ticker}: {e}")
            return {}

    def fetch_yahoo_finance_quote(self, ticker: str) -> Dict:
        """
        Fetch stock quote and company info from Yahoo Finance.
        Returns: {market_cap, price, currency, sector}
        """
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            resp = self.session.get(url, params={"interval": "1d", "range": "1d"}, timeout=10)
            resp.raise_for_status()

            data = resp.json()
            meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})

            return {
                "market_cap": meta.get("marketCap"),
                "price": meta.get("regularMarketPrice"),
                "currency": meta.get("currency"),
                "source": "Yahoo Finance",
                "source_url": f"https://finance.yahoo.com/quote/{ticker}",
                "confidence": 85
            }
        except Exception as e:
            print(f"[Yahoo Finance] Failed to fetch {ticker}: {e}")
            return {}

    def fetch_wikidata_company(self, company_name: str) -> Dict:
        """
        Fetch company fundamentals from Wikidata.
        Returns: {founded_year, headquarters, website, description}
        """
        try:
            sparql_query = f"""
            SELECT ?item ?label ?founded ?hq ?hqLabel ?website ?inception ?inceptionLabel
            WHERE {{
                ?item rdfs:label "{company_name}"@en .
                {{ ?item wdt:P31 wd:Q783794 }} UNION {{ ?item wdt:P31 wd:Q156 }} .
                ?item rdfs:label ?label .
                OPTIONAL {{ ?item wdt:P571 ?founded . }}
                OPTIONAL {{ ?item wdt:P159 ?hq . ?hq rdfs:label ?hqLabel . }}
                OPTIONAL {{ ?item wdt:P856 ?website . }}
                OPTIONAL {{ ?item wdt:P580 ?inception . ?inception rdfs:label ?inceptionLabel . }}
                FILTER (LANG(?label) = "en")
            }}
            LIMIT 1
            """

            url = "https://query.wikidata.org/sparql"
            resp = self.session.get(url, params={"query": sparql_query, "format": "json"}, timeout=10)
            resp.raise_for_status()

            data = resp.json()
            results = data.get("results", {}).get("bindings", [])

            if not results:
                return {}

            result = results[0]

            founded_year = None
            if "founded" in result:
                founded_val = result["founded"]["value"]
                founded_year = int(founded_val.split("-")[0])

            headquarters = None
            if "hqLabel" in result:
                headquarters = result["hqLabel"]["value"]

            website = None
            if "website" in result:
                website = result["website"]["value"]
                website = website.replace("https://", "").replace("http://", "").rstrip("/")

            return {
                "founded_year": founded_year,
                "headquarters": headquarters,
                "website": website,
                "source": "Wikidata",
                "source_url": f"https://www.wikidata.org/wiki/{result['item']['value'].split('/')[-1]}",
                "confidence": 85
            }
        except Exception as e:
            print(f"[Wikidata] Failed to fetch {company_name}: {e}")
            return {}

    def fetch_official_website_info(self, website_url: str) -> Dict:
        """
        Fetch basic info from official website (title, meta description).
        Returns: {description, founded_year_guess}
        """
        try:
            if not website_url.startswith("http"):
                website_url = f"https://{website_url}"

            resp = self.session.get(website_url, timeout=10)
            resp.raise_for_status()

            # Extract meta description
            import re
            meta_desc_match = re.search(r'<meta\s+name="description"\s+content="([^"]+)"', resp.text)
            description = meta_desc_match.group(1) if meta_desc_match else ""

            return {
                "description": description,
                "source": "Official website",
                "source_url": website_url,
                "confidence": 80
            }
        except Exception as e:
            print(f"[Website] Failed to fetch {website_url}: {e}")
            return {}


class BrandResearchPlan:
    """Generate research plan for a batch of brands."""

    def __init__(self, brands: List[str]):
        self.brands = brands
        self.research_plan = []

    def categorize_brands(self) -> Dict[str, List[str]]:
        """Categorize brands by type for targeted research."""
        categories = {
            "technology": ["Apple", "Microsoft", "Google", "Amazon", "Tesla", "Meta", "NVIDIA", "Intel"],
            "beverages": ["Coca-Cola", "PepsiCo", "Red Bull", "Starbucks", "Monster", "Tropicana"],
            "fashion": ["Nike", "Adidas", "Zara", "H&M", "LVMH", "Gucci", "Prada"],
            "FMCG": ["Nestlé", "Unilever", "Procter & Gamble", "Colgate-Palmolive"],
            "retail": ["Walmart", "Amazon", "Alibaba", "Tesco", "Costco"],
            "automotive": ["Tesla", "Toyota", "BMW", "Mercedes-Benz", "Volkswagen"],
            "pharma": ["Pfizer", "Moderna", "J&J", "Roche", "Novartis"],
            "other": []
        }

        # Re-categorize based on actual brands provided
        categorized = {cat: [] for cat in categories.keys()}
        for brand in self.brands:
            found = False
            for cat, brand_list in categories.items():
                if cat == "other":
                    continue
                if any(b.lower() in brand.lower() or brand.lower() in b.lower() for b in brand_list):
                    categorized[cat].append(brand)
                    found = True
                    break
            if not found:
                categorized["other"].append(brand)

        return categorized

    def generate_sources_by_category(self) -> Dict[str, Dict]:
        """Generate data sources strategy by brand category."""
        return {
            "technology": {
                "financials": ["SEC Edgar 10-K", "Yahoo Finance"],
                "products": ["Official product pages", "Tech review sites"],
                "market_data": ["Yahoo Finance", "Google Finance"],
                "social": ["Official @company Twitter, Instagram, YouTube"]
            },
            "beverages": {
                "financials": ["SEC Edgar 10-K", "Official investor relations"],
                "products": ["Brand website", "Amazon, major retailers"],
                "pricing": ["Official website", "Tesco.com, Sainsbury's, Asda"],
                "market_data": ["Statista beverage market reports"]
            },
            "fashion": {
                "financials": ["SEC Edgar 10-K", "Company House accounts"],
                "products": ["Brand websites", "ASOS, John Lewis"],
                "pricing": ["Official website", "Retail partners"],
                "market_data": ["Fashion industry reports", "Statista"]
            },
            "other": {
                "financials": ["SEC Edgar 10-K", "Companies House", "Official reports"],
                "products": ["Official website", "Major retailers"],
                "market_data": ["Industry-specific databases"]
            }
        }


def generate_research_roadmap(brands_list: List[str]) -> Dict:
    """
    Generate a systematic research roadmap for all brands.
    Output: Research plan with source priorities, effort estimates, dependencies.
    """
    plan = BrandResearchPlan(brands_list)
    categorized = plan.categorize_brands()
    sources_by_cat = plan.generate_sources_by_category()

    roadmap = {
        "total_brands": len(brands_list),
        "research_date": datetime.now().isoformat(),
        "categories": categorized,
        "data_sources_by_category": sources_by_cat,
        "priority_sequence": [],
        "effort_estimates": {},
        "quality_gates": {
            "all_fields_traceable": "Every numeric value must have source_url",
            "confidence_scores": "Source type determines confidence (95% SEC, 85% Wikipedia, 70% news, 60% estimates)",
            "missing_data_marked": "Mark as 'Not Available - Source Not Found' (not estimated)",
            "no_fabrication": "Only use real, published data with verifiable sources"
        }
    }

    # Assign priority based on market impact
    priority_scoring = {
        "technology": 10,
        "beverages": 9,
        "fashion": 8,
        "FMCG": 8,
        "retail": 9,
        "automotive": 8,
        "pharma": 9,
        "other": 5
    }

    for category, brand_list in categorized.items():
        priority = priority_scoring.get(category, 5)
        for brand in brand_list:
            roadmap["priority_sequence"].append({
                "brand": brand,
                "category": category,
                "priority": priority,
                "effort_hours": 2,  # Per brand research time
                "key_sources": sources_by_cat.get(category, {})
            })

    # Sort by priority (descending)
    roadmap["priority_sequence"].sort(key=lambda x: x["priority"], reverse=True)

    return roadmap


# Export for use in other scripts
if __name__ == "__main__":
    print("Intel Brand Research Framework Ready")
    print("- BrandResearchTracker: Track sources for each field")
    print("- BrandDataCollector: Gather data from verified sources")
    print("- BrandResearchPlan: Plan research strategy by category")
    print("- generate_research_roadmap(): Create systematic roadmap")
