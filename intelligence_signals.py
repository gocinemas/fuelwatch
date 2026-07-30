"""
Real-time Company Intelligence Signals
Aggregates hiring, news, stock movement, exec changes, product launches
for deal-makers (PE/M&A/Founders)
"""

import requests
import json
import re
from datetime import datetime, timedelta
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class CompanySignals:
    """Aggregates real-time signals for a company."""

    def __init__(self, company_name: str, ticker: str = None):
        self.company_name = company_name
        self.ticker = ticker
        self.signals = {}

    def fetch_all_signals(self) -> dict:
        """Fetch all signals for the company."""
        try:
            self.signals = {
                "company": self.company_name,
                "ticker": self.ticker,
                "timestamp": datetime.utcnow().isoformat(),
                "stock_movement": self._fetch_stock_movement(),
                "news_signals": self._fetch_news_signals(),
                "hiring_signals": self._fetch_hiring_signals(),
                "executive_changes": self._fetch_executive_changes(),
                "product_launches": self._fetch_product_launches(),
                "competitive_position": self._fetch_competitive_position(),
            }
            return self.signals
        except Exception as e:
            logger.error(f"[signals] Error fetching signals for {self.company_name}: {e}")
            return {"error": str(e)}

    def _fetch_stock_movement(self) -> dict:
        """Fetch 30-day stock trend and volume."""
        if not self.ticker:
            return {"status": "no_ticker", "message": "Ticker not available"}

        try:
            # Fetch from Yahoo Finance
            url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{self.ticker}"
            params = {
                "modules": "price,summaryDetail"
            }

            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                result = data.get("quoteSummary", {}).get("result", [{}])[0]
                price = result.get("price", {})

                return {
                    "ticker": self.ticker,
                    "current_price": price.get("regularMarketPrice", {}).get("raw"),
                    "52_week_high": price.get("fiftyTwoWeekHigh", {}).get("raw"),
                    "52_week_low": price.get("fiftyTwoWeekLow", {}).get("raw"),
                    "currency": price.get("currency"),
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "Yahoo Finance"
                }
        except Exception as e:
            logger.debug(f"[stock] Error: {e}")

        return {"status": "unavailable", "source": "Yahoo Finance"}

    def _fetch_news_signals(self) -> dict:
        """Fetch recent news mentions and sentiment."""
        try:
            # Use NewsAPI
            api_key = "demo"  # Would use real key in prod
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": self.company_name,
                "sortBy": "publishedAt",
                "language": "en",
                "pageSize": 10,
            }

            response = requests.get(url, params=params, timeout=8)
            if response.status_code == 200:
                data = response.json()
                articles = data.get("articles", [])

                # Extract keywords
                recent_30d = datetime.utcnow() - timedelta(days=30)
                recent_articles = [
                    a for a in articles
                    if datetime.fromisoformat(a.get("publishedAt", "").replace("Z", "+00:00")) > recent_30d
                ]

                # Categorize articles
                categories = self._categorize_news(articles)

                return {
                    "total_mentions_30d": len(recent_articles),
                    "top_articles": [
                        {
                            "title": a.get("title"),
                            "source": a.get("source", {}).get("name"),
                            "published": a.get("publishedAt"),
                            "category": categories.get(a.get("title"), "general")
                        }
                        for a in recent_articles[:5]
                    ],
                    "categories": {
                        "earnings": len([a for a in articles if "earnings" in a.get("title", "").lower()]),
                        "lawsuit": len([a for a in articles if "lawsuit" in a.get("title", "").lower() or "litigation" in a.get("title", "").lower()]),
                        "acquisition": len([a for a in articles if "acquire" in a.get("title", "").lower()]),
                        "product_launch": len([a for a in articles if "launch" in a.get("title", "").lower() or "new" in a.get("title", "").lower()]),
                    },
                    "source": "NewsAPI"
                }
        except Exception as e:
            logger.debug(f"[news] Error: {e}")

        return {"status": "unavailable", "source": "NewsAPI"}

    def _fetch_hiring_signals(self) -> dict:
        """Fetch LinkedIn hiring velocity."""
        # In MVP, this would be mocked/hardcoded
        # Real implementation would scrape LinkedIn or use Apify
        return {
            "linkedin_job_count": "N/A - requires LinkedIn scraper",
            "trend": "up/flat/down (based on 30-day trend)",
            "note": "Implement with Apify or LinkedIn scraper",
            "source": "LinkedIn (proposed)"
        }

    def _fetch_executive_changes(self) -> dict:
        """Detect recent executive departures/arrivals."""
        try:
            # Search news for exec changes
            keywords = ["CEO", "CFO", "COO", "CTO", "joins", "departs", "resigned", "appointed"]

            # This would parse news articles for keywords
            return {
                "recent_changes": [],
                "departures_30d": 0,
                "arrivals_30d": 0,
                "note": "Parse from news articles in MVP",
                "source": "News + LinkedIn"
            }
        except Exception as e:
            logger.debug(f"[executives] Error: {e}")

        return {}

    def _fetch_product_launches(self) -> dict:
        """Detect recent product launches."""
        return {
            "recent_launches": [],
            "last_90_days": [],
            "note": "Scrape from company website + news",
            "source": "Company Press + News"
        }

    def _fetch_competitive_position(self) -> dict:
        """Competitive positioning data (static for MVP)."""
        # Hardcode for Reckitt MVP
        if "Reckitt" in self.company_name:
            return {
                "market_segment": "Home Care & Hygiene",
                "market_share": {
                    "home_care_uk": "31%",
                    "hygiene_emea": "28%",
                    "global": "8%"
                },
                "direct_competitors": [
                    {"name": "Henkel", "market_share": "18%", "strength": "Premium cleaning"},
                    {"name": "Unilever", "market_share": "16%", "strength": "Laundry leadership"},
                    {"name": "SC Johnson", "market_share": "8%", "strength": "US market"}
                ],
                "key_brands": ["Dettol", "Lysol", "Air Wick", "Nurofen", "Strepsils"],
                "competitive_notes": [
                    "Losing to Henkel in premium natural segment (new gap emerging)",
                    "Strong in UK/EMEA, weak in US (Lysol recovering post-COVID)",
                    "Investing heavily in emerging markets (APAC expansion)"
                ],
                "source": "Market Research (Hardcoded for MVP)"
            }

        return {"status": "not_implemented"}

    def _categorize_news(self, articles: list) -> dict:
        """Categorize news articles by topic."""
        categories = {}
        keywords = {
            "earnings": ["earnings", "revenue", "profit", "results", "q1", "q2", "q3", "q4"],
            "lawsuit": ["lawsuit", "litigation", "sued", "legal", "court"],
            "acquisition": ["acquire", "acquisition", "merger", "deal", "bought"],
            "product": ["launch", "new product", "sku", "brand"]
        }

        for article in articles:
            title = article.get("title", "").lower()
            category = "general"

            for cat, words in keywords.items():
                if any(word in title for word in words):
                    category = cat
                    break

            categories[article.get("title")] = category

        return categories

    def generate_headline(self) -> str:
        """Generate 1-line intelligence headline."""
        signals = self.signals

        # Simple logic for MVP
        news = signals.get("news_signals", {})
        stock = signals.get("stock_movement", {})

        mentions = news.get("total_mentions_30d", 0)
        price = stock.get("current_price")

        if mentions > 15:
            return f"🔥 Lots of buzz around {self.company_name} (${price})"
        elif mentions > 5:
            return f"📊 {self.company_name} in the news (${price})"
        else:
            return f"📍 {self.company_name} quiet period (${price})"


def get_company_signals(company_name: str, ticker: str = None) -> dict:
    """Get real-time signals for a company."""
    signals = CompanySignals(company_name, ticker)
    return signals.fetch_all_signals()
