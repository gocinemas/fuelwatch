"""
Brand Expansion Intelligence — Real-time brand market positioning

Fetches brand data from:
1. Market databases (brand positioning, pricing, distribution)
2. Social signals (mentions, sentiment, trend data)
3. Expansion signals (new markets, SKUs, partnerships)
4. Competitor tracking (direct brand competitors, market share)
5. Supply chain signals (vendor changes, manufacturing capacity)

Mirrors company_intelligence.py but adapted for consumer brands.
"""

import requests
import json
import os
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def fetch_brand_intelligence(brand_name: str, market: str = "GB") -> dict:
    """
    Fetch comprehensive brand intelligence.

    Args:
        brand_name: Brand to search (e.g., "Nike", "Zara", "Glossier")
        market: Market code (GB, US, IN) — where brand is positioned

    Returns:
        {
            "name": "Nike",
            "market": "GB",
            "positioning": {...},
            "market_size": {...},
            "expansion_signals": [...],
            "competitor_brands": [...],
            "pricing": {...},
            "distribution": {...},
            "social_signals": {...},
            "supply_chain": {...}
        }
    """

    result = {
        "name": brand_name,
        "market": market.upper(),
        "positioning": {},
        "market_size": {},
        "expansion_signals": [],
        "competitor_brands": [],
        "pricing": {},
        "distribution": {},
        "social_signals": {},
        "supply_chain": {},
        "data_sources": []
    }

    try:
        # 1. Fetch brand positioning data
        positioning = _fetch_brand_positioning(brand_name, market)
        if positioning:
            result["positioning"] = positioning
            result["data_sources"].append("Wikidata/Wikipedia")

        # 2. Fetch market size & opportunity
        market_data = _fetch_market_data(brand_name, market)
        if market_data:
            result["market_size"] = market_data
            result["data_sources"].append("World Bank/Trading Economics")

        # 3. Fetch expansion signals (new products, geographies, partnerships)
        expansion = _fetch_expansion_signals(brand_name, market)
        if expansion:
            result["expansion_signals"] = expansion
            result["data_sources"].append("News APIs/Press Releases")

        # 4. Fetch competitor brands
        competitors = _fetch_competitor_brands(brand_name)
        if competitors:
            result["competitor_brands"] = competitors
            result["data_sources"].append("Market Research")

        # 5. Fetch pricing data (D2C catalog tracking)
        pricing = _fetch_pricing_data(brand_name, market)
        if pricing:
            result["pricing"] = pricing
            result["data_sources"].append("E-commerce APIs")

        # 6. Fetch distribution info
        distribution = _fetch_distribution(brand_name, market)
        if distribution:
            result["distribution"] = distribution
            result["data_sources"].append("Retail Tracking")

        # 7. Fetch social signals (Reddit, Twitter, TikTok mentions)
        social = _fetch_social_signals(brand_name)
        if social:
            result["social_signals"] = social
            result["data_sources"].append("Social Media APIs")

        # 8. Supply chain signals
        supply_chain = _fetch_supply_chain(brand_name)
        if supply_chain:
            result["supply_chain"] = supply_chain
            result["data_sources"].append("Supply Chain Data")

        # Generate summary
        result["summary"] = _generate_brand_summary(result)

        return result

    except Exception as e:
        logger.error(f"[brand_intelligence] ERROR: {e}", exc_info=True)
        result["error"] = str(e)
        return result


def _fetch_brand_positioning(brand_name: str, market: str) -> dict:
    """Fetch brand positioning info from Wikipedia/Wikidata."""
    try:
        from urllib.parse import quote

        url = f"https://en.wikipedia.org/w/api.php?action=query&titles={quote(brand_name)}&prop=extracts&explaintext=True&format=json"
        response = requests.get(url, timeout=5)

        if response.status_code == 200:
            data = response.json()
            pages = data.get("query", {}).get("pages", {})

            for page_id, page_data in pages.items():
                if page_id != "-1":
                    extract = page_data.get("extract", "")
                    return {
                        "name": page_data.get("title", brand_name),
                        "description": extract[:300] if extract else "",
                        "source": "Wikipedia"
                    }

    except Exception as e:
        logger.debug(f"[brand_positioning] Error: {e}")

    return {}


def _fetch_market_data(brand_name: str, market: str) -> dict:
    """Fetch market size and opportunity data."""
    try:
        # Placeholder: Would integrate with World Bank, Trading Economics APIs
        # For now, return structure
        market_info = {
            "market_code": market,
            "market_size_gbp": "TBD",
            "growth_rate": "TBD",
            "category": "Consumer/Retail",
            "purchasing_power_index": "TBD"
        }

        return market_info

    except Exception as e:
        logger.debug(f"[market_data] Error: {e}")

    return {}


def _fetch_expansion_signals(brand_name: str, market: str) -> list:
    """Fetch new product launches, market entries, partnerships."""
    signals = []

    try:
        # Try news APIs for brand expansion announcements
        api_key = os.environ.get("NEWS_API_KEY", "")

        if api_key:
            keywords = [
                f"{brand_name} launches",
                f"{brand_name} new",
                f"{brand_name} expands",
                f"{brand_name} partnership",
                f"{brand_name} acquisition",
            ]

            for keyword in keywords:
                try:
                    url = "https://newsapi.org/v2/everything"
                    params = {
                        "q": keyword,
                        "sortBy": "publishedAt",
                        "language": "en",
                        "apiKey": api_key,
                        "pageSize": 3
                    }

                    response = requests.get(url, params=params, timeout=5)
                    if response.status_code == 200:
                        articles = response.json().get("articles", [])
                        for article in articles[:1]:  # Take top 1 per keyword
                            signals.append({
                                "title": article.get("title", ""),
                                "date": article.get("publishedAt", ""),
                                "source": article.get("source", {}).get("name", ""),
                                "type": _classify_expansion(article.get("title", ""))
                            })

                except Exception as e:
                    logger.debug(f"[expansion] Error: {e}")

    except Exception as e:
        logger.debug(f"[expansion_signals] Error: {e}")

    return signals


def _classify_expansion(title: str) -> str:
    """Classify type of expansion from title."""
    title_lower = title.lower()
    if "launch" in title_lower or "new" in title_lower:
        return "Product Launch"
    elif "expand" in title_lower or "market" in title_lower:
        return "Market Expansion"
    elif "partner" in title_lower or "collaboration" in title_lower:
        return "Partnership"
    elif "acquire" in title_lower or "acquisition" in title_lower:
        return "Acquisition"
    return "Announcement"


def _fetch_competitor_brands(brand_name: str) -> list:
    """Fetch direct competitor brands in same category."""
    competitors = []

    # Placeholder: Would use market research APIs
    # For now, return empty — can populate with manual data or APIs

    return competitors


def _fetch_pricing_data(brand_name: str, market: str) -> dict:
    """Fetch current pricing, SKU catalog, price changes."""
    try:
        # Placeholder: Would integrate with e-commerce APIs (Shopify, Amazon, etc.)
        # For SMB/D2C brands, track SKU prices and trends

        pricing_info = {
            "currency": "GBP" if market == "GB" else "USD",
            "price_range": "TBD",
            "average_price": "TBD",
            "price_changes_3m": [],
            "skus_tracked": 0,
            "top_products": []
        }

        return pricing_info

    except Exception as e:
        logger.debug(f"[pricing] Error: {e}")

    return {}


def _fetch_distribution(brand_name: str, market: str) -> dict:
    """Fetch where brand is distributed (online, retail, channels)."""
    try:
        distribution_info = {
            "channels": ["Online", "Retail"],
            "retailers": [],
            "online_presence": {
                "website": f"https://www.{brand_name.lower().replace(' ', '')}.com",
                "shopify": False,
                "amazon": False,
                "direct_d2c": True
            },
            "market_reach": "TBD"
        }

        return distribution_info

    except Exception as e:
        logger.debug(f"[distribution] Error: {e}")

    return {}


def _fetch_social_signals(brand_name: str) -> dict:
    """Fetch social media sentiment, mentions, trends."""
    try:
        social_info = {
            "reddit_mentions": [],
            "twitter_sentiment": "TBD",
            "tiktok_presence": False,
            "instagram_followers": "TBD",
            "overall_sentiment": "TBD",
            "trending": False
        }

        return social_info

    except Exception as e:
        logger.debug(f"[social_signals] Error: {e}")

    return {}


def _fetch_supply_chain(brand_name: str) -> dict:
    """Fetch supply chain info, vendor changes, capacity."""
    try:
        supply_chain_info = {
            "manufacturing_locations": [],
            "key_vendors": [],
            "capacity_signals": [],
            "compliance_status": "TBD",
            "sustainability_score": "TBD"
        }

        return supply_chain_info

    except Exception as e:
        logger.debug(f"[supply_chain] Error: {e}")

    return {}


def _generate_brand_summary(data: dict) -> str:
    """Generate human-readable brand summary."""
    name = data.get("name", "N/A")
    market = data.get("market", "Global")

    summary_parts = [
        f"**Brand:** {name} ({market})",
    ]

    if data.get("positioning", {}).get("description"):
        summary_parts.append(f"**Positioning:** {data['positioning']['description'][:150]}...")

    if data.get("expansion_signals"):
        summary_parts.append(f"**Recent Signals:** {len(data['expansion_signals'])} announcements")

    if data.get("social_signals"):
        summary_parts.append(f"**Social:** {data['social_signals'].get('overall_sentiment', 'TBD')}")

    return "\n".join(summary_parts)
