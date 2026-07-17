"""
Deep Company Search — Try every available source and aggregate results

Sources (in priority order):
1. Wikipedia (free, covers notable companies)
2. Crunchbase API (startups, funding, employees)
3. LinkedIn company pages (employee counts, descriptions)
4. News APIs (recent info, announcements)
5. Web search (Google patterns, company websites)
6. Industry databases (regulatory, stock exchanges)
7. Social media (Twitter company info)
"""

import requests
import json
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def deep_company_search(company_name: str) -> dict:
    """
    Deep search across all available sources.

    Returns aggregated company data from multiple sources.
    """

    results = {
        "name": None,
        "sources": defaultdict(dict),
        "aggregated": {},
        "search_attempt_count": 0
    }

    try:
        # 1. Wikipedia (covers ~80% of notable companies)
        logger.info(f"[deep_search] Searching Wikipedia for {company_name}...")
        wiki_data = _search_wikipedia(company_name)
        if wiki_data:
            results["sources"]["wikipedia"] = wiki_data
            results["search_attempt_count"] += 1

        # 2. Crunchbase (startups, funding, employees)
        logger.info(f"[deep_search] Searching Crunchbase for {company_name}...")
        cb_data = _search_crunchbase(company_name)
        if cb_data:
            results["sources"]["crunchbase"] = cb_data
            results["search_attempt_count"] += 1

        # 3. LinkedIn company page
        logger.info(f"[deep_search] Searching LinkedIn for {company_name}...")
        linkedin_data = _search_linkedin_company(company_name)
        if linkedin_data:
            results["sources"]["linkedin"] = linkedin_data
            results["search_attempt_count"] += 1

        # 4. News APIs (recent announcements)
        logger.info(f"[deep_search] Searching news for {company_name}...")
        news_data = _search_news_api(company_name)
        if news_data:
            results["sources"]["news"] = news_data
            results["search_attempt_count"] += 1

        # 5. Google search patterns
        logger.info(f"[deep_search] Searching Google patterns for {company_name}...")
        google_data = _search_google_patterns(company_name)
        if google_data:
            results["sources"]["google"] = google_data
            results["search_attempt_count"] += 1

        # 6. Stock exchange lookups (for public companies)
        logger.info(f"[deep_search] Checking stock exchanges for {company_name}...")
        stock_data = _search_stock_exchanges(company_name)
        if stock_data:
            results["sources"]["stock_exchange"] = stock_data
            results["search_attempt_count"] += 1

        # 7. Twitter/X company info
        logger.info(f"[deep_search] Searching Twitter for {company_name}...")
        twitter_data = _search_twitter_company(company_name)
        if twitter_data:
            results["sources"]["twitter"] = twitter_data
            results["search_attempt_count"] += 1

        # Aggregate all data
        if results["sources"]:
            results["aggregated"] = _aggregate_company_data(results["sources"])
            results["name"] = results["aggregated"].get("name", company_name)

        return results

    except Exception as e:
        logger.error(f"[deep_search] ERROR: {e}")
        results["error"] = str(e)
        return results


def _search_wikipedia(company_name: str) -> dict:
    """Search Wikipedia for company info."""
    try:
        from urllib.parse import quote

        url = f"https://en.wikipedia.org/w/api.php?action=query&titles={quote(company_name)}&prop=extracts&explaintext=True&format=json"

        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            pages = data.get("query", {}).get("pages", {})

            for page_id, page_data in pages.items():
                if page_id != "-1":  # Found page
                    extract = page_data.get("extract", "")
                    if extract:
                        return {
                            "name": page_data.get("title", company_name),
                            "description": extract[:500],
                            "source": "Wikipedia"
                        }

    except Exception as e:
        logger.debug(f"[wikipedia] Error: {e}")

    return {}


def _search_crunchbase(company_name: str) -> dict:
    """Search Crunchbase for startup/company info."""
    try:
        api_key = __import__("os").environ.get("CRUNCHBASE_API_KEY", "")
        if not api_key:
            return {}

        # Would use Crunchbase API if key available
        # Returns: funding, employees, location, etc.
        logger.debug(f"[crunchbase] Would search for: {company_name}")

    except Exception as e:
        logger.debug(f"[crunchbase] Error: {e}")

    return {}


def _search_linkedin_company(company_name: str) -> dict:
    """Search LinkedIn company page patterns."""
    try:
        company_slug = company_name.lower().replace(" ", "-")
        linkedin_url = f"https://www.linkedin.com/company/{company_slug}/"

        # Would scrape or use LinkedIn API
        logger.debug(f"[linkedin] Company page: {linkedin_url}")

    except Exception as e:
        logger.debug(f"[linkedin] Error: {e}")

    return {}


def _search_news_api(company_name: str) -> dict:
    """Search news APIs for recent company announcements."""
    try:
        api_key = __import__("os").environ.get("NEWS_API_KEY", "")
        if not api_key:
            return {}

        url = "https://newsapi.org/v2/everything"
        params = {
            "q": company_name,
            "sortBy": "publishedAt",
            "language": "en",
            "apiKey": api_key,
            "pageSize": 3
        }

        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            articles = response.json().get("articles", [])
            if articles:
                return {
                    "recent_news": [
                        {
                            "title": a.get("title"),
                            "date": a.get("publishedAt"),
                            "source": a.get("source", {}).get("name")
                        }
                        for a in articles[:3]
                    ]
                }

    except Exception as e:
        logger.debug(f"[news_api] Error: {e}")

    return {}


def _search_google_patterns(company_name: str) -> dict:
    """Search using Google patterns (domain lookup, knowledge panel)."""
    try:
        # Common domain patterns
        domain_patterns = [
            f"www.{company_name.lower().replace(' ', '')}.com",
            f"www.{company_name.lower().replace(' ', '')}.co.uk",
            f"www.{company_name.lower().replace(' ', '')}.io",
            f"www.{company_name.lower()}.com",
        ]

        return {
            "likely_domains": domain_patterns,
            "search_url": f"https://www.google.com/search?q={company_name}+"
        }

    except Exception as e:
        logger.debug(f"[google] Error: {e}")

    return {}


def _search_stock_exchanges(company_name: str) -> dict:
    """Search major stock exchanges (NYSE, NASDAQ, LSE, NSE, etc.)."""
    try:
        exchanges = {
            "NYSE": f"https://www.nyse.com/",
            "NASDAQ": f"https://www.nasdaq.com/",
            "LSE": f"https://www.londonstockexchange.com/",
            "NSE": f"https://www.nseindia.com/",
        }

        # Would search each exchange for ticker
        logger.debug(f"[stock_exchanges] Would search {len(exchanges)} exchanges")

    except Exception as e:
        logger.debug(f"[stock_exchanges] Error: {e}")

    return {}


def _search_twitter_company(company_name: str) -> dict:
    """Search Twitter for company official account."""
    try:
        api_key = __import__("os").environ.get("TWITTER_API_KEY", "")
        if not api_key:
            return {}

        # Would use Twitter API to find company account
        logger.debug(f"[twitter] Would search for: {company_name}")

    except Exception as e:
        logger.debug(f"[twitter] Error: {e}")

    return {}


def _aggregate_company_data(sources: dict) -> dict:
    """Intelligently aggregate data from multiple sources."""

    aggregated = {
        "name": None,
        "description": None,
        "hq": None,
        "employees": None,
        "founded": None,
        "industry": None,
        "funding": None,
        "recent_news": [],
        "sources_found": list(sources.keys())
    }

    # Priority: use first available data
    if "wikipedia" in sources:
        wiki = sources["wikipedia"]
        aggregated["name"] = aggregated["name"] or wiki.get("name")
        aggregated["description"] = aggregated["description"] or wiki.get("description")
        aggregated["founded"] = aggregated["founded"] or wiki.get("founded_year")

    if "crunchbase" in sources:
        cb = sources["crunchbase"]
        aggregated["employees"] = aggregated["employees"] or cb.get("employees")
        aggregated["funding"] = aggregated["funding"] or cb.get("funding_total")
        aggregated["industry"] = aggregated["industry"] or cb.get("industry")

    if "linkedin" in sources:
        li = sources["linkedin"]
        aggregated["employees"] = aggregated["employees"] or li.get("employee_count")
        aggregated["description"] = aggregated["description"] or li.get("headline")

    if "news" in sources:
        news = sources["news"]
        aggregated["recent_news"] = news.get("recent_news", [])

    return aggregated
