"""
Phase 1 Data Collector
Fetches real brand data from free/open sources:
- Wikipedia API (fundamentals, description, competitors)
- World Bank PPP indices
- Web scraping (competitor pricing)
- Reddit sentiment analysis (positioning)
"""

import requests
import json
from datetime import datetime
import re
from concurrent.futures import ThreadPoolExecutor

class Phase1DataCollector:
    def __init__(self, brand_name: str, category: str, market_country: str, market_iso: str):
        self.brand_name = brand_name
        self.category = category
        self.market_country = market_country
        self.market_iso = market_iso

        # PPP indices (World Bank 2024 estimates)
        self.ppp_indices = {
            'GB': 1.0,   # Reference (GBP)
            'US': 1.0,   # Reference (USD)
            'IN': 0.25,  # India PPP is ~4x lower purchasing power
            'BR': 0.42,  # Brazil
            'ID': 0.24,  # Indonesia
            'CN': 0.35,  # China
            'MX': 0.45,  # Mexico
        }

        self.sources_used = []

    def fetch_wikipedia_fundamentals(self) -> dict:
        """
        Fetch brand fundamentals from Wikipedia.
        Returns: {founded_year, headquarters_city, headquarters_country, website}
        """
        try:
            # Wikipedia API
            url = "https://en.wikipedia.org/w/api.php"
            params = {
                "action": "query",
                "titles": self.brand_name,
                "prop": "extracts|info",
                "explaintext": True,
                "format": "json"
            }

            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()

            pages = data.get("query", {}).get("pages", {})
            page = next(iter(pages.values())) if pages else {}

            if page.get("missing"):
                print(f"[collector] {self.brand_name} not found on Wikipedia")
                return {}

            extract = page.get("extract", "")

            result = {
                "description": extract[:200] if extract else "",  # First 200 chars
                "source": "Wikipedia"
            }

            # Extract founded year using regex
            founded_match = re.search(r"founded in (\d{4})|established in (\d{4})", extract, re.IGNORECASE)
            if founded_match:
                result["founded_year"] = int(founded_match.group(1) or founded_match.group(2))

            # Extract headquarters
            hq_match = re.search(r"headquartered in ([^.,]+(?:,[^.,]+)?)", extract, re.IGNORECASE)
            if hq_match:
                hq = hq_match.group(1).strip()
                result["headquarters"] = hq

            self.sources_used.append("Wikipedia")
            return result

        except Exception as e:
            print(f"[collector] Wikipedia fetch failed for {self.brand_name}: {e}")
            return {}

    def fetch_world_bank_ppp(self) -> dict:
        """
        Get PPP index for this market from World Bank.
        Returns: {ppp_index, ppp_source}
        """
        try:
            # World Bank World Development Indicators API
            url = f"https://api.worldbank.org/v2/country/{self.market_iso}/indicator/NY.GDP.PCAP.PP.CD"
            params = {"format": "json", "per_page": 1}

            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()

            if len(data) > 1 and data[1]:
                latest = data[1][0]  # Most recent year
                gdp_ppp_percapita = latest.get("value")
                if gdp_ppp_percapita:
                    # Normalize to reference (US = $63,543 per capita PPP)
                    ppp_index = float(gdp_ppp_percapita) / 63543
                    self.sources_used.append("World Bank")
                    return {
                        "ppp_index": round(ppp_index, 2),
                        "ppp_source": "World Bank 2024"
                    }

            return {}
        except Exception as e:
            print(f"[collector] World Bank PPP fetch failed: {e}")
            # Fallback to hardcoded indices
            return {
                "ppp_index": self.ppp_indices.get(self.market_iso, 0.5),
                "ppp_source": "Hardcoded estimate"
            }

    def fetch_competitor_pricing(self, competitor_list: list) -> dict:
        """
        Fetch competitor prices for this market.
        Simple web scraping approach (limited).
        Returns: {competitor_name: price_local, ...}
        """
        # Placeholder: In production, would scrape Amazon, Tesco, Nykaa, etc.
        # For now, return empty to avoid rate limiting
        return {}

    def fetch_reddit_sentiment(self) -> dict:
        """
        Analyze Reddit sentiment for brand positioning.
        Simple keyword search approach.
        Returns: {brand_mentions, sentiment_summary, positioning_clues}
        """
        try:
            # Use Pushshift Reddit API (free, read-only)
            url = "https://api.pushshift.io/reddit/search/submission"
            params = {
                "q": f"{self.brand_name} {self.category}",
                "subreddit": "skincare,marketing,branding",
                "size": 10,
                "sort": "desc"
            }

            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()

            posts = data.get("data", [])
            if posts:
                self.sources_used.append("Reddit")
                return {
                    "reddit_mentions": len(posts),
                    "reddit_source": "Pushshift API"
                }

            return {}
        except Exception as e:
            print(f"[collector] Reddit sentiment fetch failed: {e}")
            return {}

    def estimate_target_segment(self) -> dict:
        """
        Estimate target segment based on brand + market + category.
        Uses heuristics and category knowledge.
        Returns: {target_demographic, target_income_tier, segment_size_millions}
        """

        segment_templates = {
            ("skincare", "GB"): {
                "demographic": "Women 30-60, affluent + middle-income",
                "income_tier": "upper-middle to affluent",
                "size_millions": 8.5
            },
            ("skincare", "US"): {
                "demographic": "Women 25-60, middle to affluent income",
                "income_tier": "middle to affluent",
                "size_millions": 45
            },
            ("skincare", "IN"): {
                "demographic": "Women 25-50, urban, upper-middle income",
                "income_tier": "upper-middle to affluent",
                "size_millions": 30  # Indian affluent women
            },
            ("beverages", "GB"): {
                "demographic": "Ages 10-40, all income levels",
                "income_tier": "mass-market",
                "size_millions": 25
            },
            ("beverages", "IN"): {
                "demographic": "Ages 15-35, urban, middle-income+",
                "income_tier": "middle to affluent",
                "size_millions": 150  # Large youth demographic
            },
        }

        key = (self.category, self.market_iso)
        template = segment_templates.get(key, {
            "demographic": "General population",
            "income_tier": "mass-market",
            "size_millions": 50
        })

        return {
            "target_demographic": template["demographic"],
            "target_income_tier": template["income_tier"],
            "segment_size_millions": template["size_millions"]
        }

    def collect_all(self) -> dict:
        """
        Run full data collection pipeline.
        Returns: Complete Phase 1 data object for this brand-market.
        """
        print(f"\n[collector] Starting Phase 1 data collection: {self.brand_name} ({self.market_country})")

        result = {
            "brand_name": self.brand_name,
            "category": self.category,
            "market_country": self.market_country,
            "market_iso_code": self.market_iso,
        }

        # 1. Wikipedia fundamentals
        wiki_data = self.fetch_wikipedia_fundamentals()
        result.update(wiki_data)

        # 2. World Bank PPP
        ppp_data = self.fetch_world_bank_ppp()
        result.update(ppp_data)

        # 3. Competitor pricing (placeholder)
        # comp_pricing = self.fetch_competitor_pricing([...])

        # 4. Reddit sentiment
        reddit_data = self.fetch_reddit_sentiment()
        result.update(reddit_data)

        # 5. Target segment estimation
        segment_data = self.estimate_target_segment()
        result.update(segment_data)

        # Metadata
        result["sources_used"] = self.sources_used
        result["confidence_score"] = min(len(self.sources_used) * 25, 100)  # 25 points per source
        result["data_completeness"] = self._calculate_completeness(result)
        result["last_verified_date"] = datetime.now().isoformat()

        print(f"[collector] Complete. Sources: {', '.join(self.sources_used)}. Confidence: {result['confidence_score']}%")
        return result

    def _calculate_completeness(self, data: dict) -> int:
        """Calculate how complete the data is (0-100)."""
        required_fields = [
            "founded_year", "headquarters", "target_demographic",
            "ppp_index", "description"
        ]
        present = sum(1 for field in required_fields if data.get(field))
        return int((present / len(required_fields)) * 100)


def collect_batch(brands: list) -> list:
    """
    Collect data for multiple brands in parallel.

    Args: brands = [
        {"name": "Olay", "category": "skincare", "markets": ["GB", "US", "IN"]},
        ...
    ]
    """
    all_results = []

    for brand_info in brands:
        name = brand_info["name"]
        category = brand_info["category"]

        for market_iso in brand_info["markets"]:
            # Map ISO to country name
            iso_to_country = {
                "GB": "UK", "US": "USA", "IN": "India",
                "BR": "Brazil", "ID": "Indonesia"
            }
            country = iso_to_country.get(market_iso, market_iso)

            collector = Phase1DataCollector(name, category, country, market_iso)
            result = collector.collect_all()
            all_results.append(result)

    return all_results


if __name__ == "__main__":
    # Test with pilot brands
    pilot_brands = [
        {
            "name": "Olay",
            "category": "skincare",
            "markets": ["GB", "US", "IN"]
        },
        {
            "name": "Red Bull",
            "category": "beverages",
            "markets": ["GB", "US", "IN"]
        },
        {
            "name": "Coca-Cola",
            "category": "beverages",
            "markets": ["GB", "US", "IN"]
        },
    ]

    results = collect_batch(pilot_brands)

    print("\n" + "="*80)
    print("PHASE 1 DATA COLLECTION RESULTS")
    print("="*80)

    for result in results:
        print(f"\n✓ {result['brand_name']} ({result['market_country']})")
        print(f"  Founded: {result.get('founded_year', 'N/A')}")
        print(f"  PPP Index: {result.get('ppp_index', 'N/A')}")
        print(f"  Segment: {result.get('target_demographic', 'N/A')}")
        print(f"  Segment Size: {result.get('segment_size_millions', 'N/A')}M")
        print(f"  Completeness: {result.get('data_completeness', 0)}%")
        print(f"  Sources: {', '.join(result.get('sources_used', []))}")
