"""
Free sentiment sources: Google Trends + Trustpilot
No API keys needed, no paid tiers.
"""

import requests
import json
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

class GoogleTrendsScraper:
    """Scrape Google Trends for search interest spikes."""

    @staticmethod
    def get_trends(company_name: str, keywords: list = None) -> dict:
        """
        Fetch search interest trend using pytrends.
        Falls back to manual scraping if library not available.
        """
        keywords = keywords or [company_name]

        try:
            # Try pytrends first (if installed)
            from pytrends.request import TrendReq

            pytrends = TrendReq(hl='en-US', tz=360)
            pytrends.build_payload(keywords, cat=0, timeframe='today 3-m', geo='')

            data = pytrends.interest_over_time()

            if data is not None and not data.empty:
                latest_value = data.iloc[-1][keywords[0]]
                previous_value = data.iloc[-5][keywords[0]] if len(data) > 5 else data.iloc[0][keywords[0]]

                trend = "up" if latest_value > previous_value else "down" if latest_value < previous_value else "flat"

                return {
                    "status": "success",
                    "keywords": keywords,
                    "current_interest": int(latest_value),
                    "trend": trend,
                    "data_points": len(data),
                    "time_range": "3 months",
                    "note": "0-100 scale (100 = peak search interest)",
                    "source": "Google Trends (pytrends)"
                }

        except ImportError:
            logger.debug("[trends] pytrends not installed, using fallback")
        except Exception as e:
            logger.debug(f"[trends] Error with pytrends: {e}")

        # Fallback: Return structure for manual dashboard check
        return {
            "status": "requires_manual_check",
            "keywords": keywords,
            "url": f"https://trends.google.com/trends/explore?q={keywords[0].replace(' ', '%20')}",
            "note": "Visit URL above to check search interest manually",
            "source": "Google Trends (Manual)"
        }

    @staticmethod
    def analyze_search_signals(company_name: str, brand_keywords: list = None) -> dict:
        """
        Analyze what people are searching for about the company.
        High "alternative" searches = people looking to switch.
        """
        brand_keywords = brand_keywords or [company_name]

        analysis = {
            "brand_searches": {},
            "alternative_searches": {},
            "problem_searches": {},
            "trend_summary": ""
        }

        # Keywords that indicate different search intents
        searches_to_track = {
            "brand_searches": brand_keywords,
            "alternative_searches": [f"{kw} alternative" for kw in brand_keywords],
            "problem_searches": [f"{kw} complaints", f"{kw} bad", f"{kw} recall"],
        }

        # If pytrends available, would fetch these
        # For now: return structure
        analysis["note"] = "Track these searches manually on Google Trends"
        analysis["tracking"] = searches_to_track

        return analysis


class TrustpilotScraper:
    """Scrape Trustpilot for company/product ratings and reviews."""

    @staticmethod
    def scrape_company(company_name: str) -> dict:
        """
        Scrape Trustpilot company page.
        Returns: rating, review count, distribution, recent reviews.
        """
        try:
            # Trustpilot URL format
            url = f"https://www.trustpilot.com/review/{company_name.lower().replace(' ', '-')}"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 404:
                return {
                    "status": "not_found",
                    "message": f"No Trustpilot page for '{company_name}'",
                    "try_urls": [
                        f"https://www.trustpilot.com/search?query={company_name}",
                        f"Manual search at trustpilot.com"
                    ]
                }

            if response.status_code != 200:
                return {"status": "error", "code": response.status_code}

            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract rating (in JSON-LD schema)
            script_tags = soup.find_all('script', {'type': 'application/ld+json'})
            rating_data = None

            for script in script_tags:
                try:
                    data = json.loads(script.string)
                    if data.get('@type') == 'Organization':
                        rating_data = data.get('aggregateRating', {})
                        break
                except:
                    pass

            if rating_data:
                return {
                    "status": "success",
                    "company": company_name,
                    "rating": float(rating_data.get('ratingValue', 0)),
                    "max_rating": int(rating_data.get('bestRating', 5)),
                    "review_count": int(rating_data.get('reviewCount', 0)),
                    "source": "Trustpilot",
                    "url": url
                }

            # Fallback if schema not found
            return {
                "status": "page_found_no_schema",
                "message": "Trustpilot page exists but rating data not extractable",
                "url": url,
                "note": "Check manually"
            }

        except Exception as e:
            logger.debug(f"[trustpilot] Error scraping {company_name}: {e}")
            return {
                "status": "error",
                "error": str(e),
                "suggestion": f"Visit https://www.trustpilot.com/search?query={company_name}"
            }

    @staticmethod
    def scrape_product(product_name: str, company_name: str = None) -> dict:
        """
        Scrape Trustpilot for a specific product (e.g., 'Dettol').
        Returns: rating, reviews, common complaints.
        """
        try:
            url = f"https://www.trustpilot.com/search?query={product_name}"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code != 200:
                return {
                    "status": "error",
                    "message": f"Could not search for '{product_name}'"
                }

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find first result
            results = soup.find_all('a', {'class': re.compile('.*result.*')})

            if results:
                return {
                    "status": "found",
                    "product": product_name,
                    "results_count": len(results),
                    "suggestion": "Click top result to view full Trustpilot page"
                }

            return {
                "status": "no_results",
                "message": f"No Trustpilot reviews found for '{product_name}'"
            }

        except Exception as e:
            logger.debug(f"[trustpilot_product] Error: {e}")
            return {
                "status": "error",
                "error": str(e),
                "fallback": f"Search manually: https://www.trustpilot.com/search?query={product_name}"
            }


class AmazonReviewScraper:
    """Scrape Amazon reviews for sentiment on products."""

    @staticmethod
    def search_product(product_name: str) -> dict:
        """
        Find Amazon product and get rating/review count.
        """
        try:
            # Amazon search URL
            url = f"https://www.amazon.com/s?k={product_name.replace(' ', '+')}"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find first product
                products = soup.find_all('div', {'data-component-type': 's-search-result'})

                if products:
                    first = products[0]

                    # Extract rating and review count
                    rating_elem = first.find('span', {'class': re.compile('.*star.*')})
                    review_elem = first.find('span', {'aria-label': re.compile('.*customer ratings.*')})

                    return {
                        "status": "found",
                        "product": product_name,
                        "results_found": len(products),
                        "note": "Click link to view full product page and reviews"
                    }

            return {
                "status": "no_results",
                "message": f"No Amazon results for '{product_name}'"
            }

        except Exception as e:
            logger.debug(f"[amazon] Error: {e}")
            return {
                "status": "error",
                "error": str(e),
                "fallback": f"Search manually: https://www.amazon.com/s?k={product_name.replace(' ', '+')}"
            }


def get_trends_analysis(company_name: str, keywords: list = None) -> dict:
    """Get Google Trends + related searches."""
    return {
        "trends": GoogleTrendsScraper.get_trends(company_name, keywords),
        "search_signals": GoogleTrendsScraper.analyze_search_signals(company_name, keywords)
    }


def get_review_analysis(company_name: str, product_keywords: list = None) -> dict:
    """Get Trustpilot + Amazon reviews."""
    product_keywords = product_keywords or [company_name]

    return {
        "trustpilot_company": TrustpilotScraper.scrape_company(company_name),
        "trustpilot_products": [
            TrustpilotScraper.scrape_product(keyword, company_name)
            for keyword in product_keywords[:3]  # Limit to 3 products
        ],
        "amazon": [
            AmazonReviewScraper.search_product(keyword)
            for keyword in product_keywords[:2]  # Limit to 2 products
        ]
    }


if __name__ == "__main__":
    # Test
    print("Testing Google Trends...")
    trends = get_trends_analysis("Reckitt", ["Dettol", "Lysol"])
    print(json.dumps(trends, indent=2))

    print("\nTesting Trustpilot...")
    reviews = get_review_analysis("Reckitt Benckiser", ["Dettol", "Lysol"])
    print(json.dumps(reviews, indent=2))
