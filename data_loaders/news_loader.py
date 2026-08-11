"""
NewsAPI Loader - Real 2026 News Data

Fetches real news articles from NewsAPI for companies.
Shows latest 2026 coverage with dates and sources.
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class NewsAPILoader:
    """Load real 2026 news articles from NewsAPI."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("NEWSAPI_KEY")
        self.base_url = "https://newsapi.org/v2"
        self.session = requests.Session()

    def get_company_news(self, company_name: str, days_back: int = 30) -> Dict:
        """
        Fetch real news articles for a company from 2026.

        Returns:
            {
                "company": "Reckitt",
                "articles": [
                    {
                        "title": "Reckitt expands APAC operations",
                        "source": "Reuters",
                        "date": "2026-08-11",
                        "url": "https://...",
                        "summary": "..."
                    },
                    ...
                ],
                "total_articles": 12,
                "last_updated": "2026-08-11T14:30:00Z",
                "date_range": "last 30 days (2026)",
                "data_quality": "LIVE"
            }
        """

        if not self.api_key:
            logger.warning("No NewsAPI key set")
            return None

        try:
            # Get news from last N days
            from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

            url = f"{self.base_url}/everything"
            params = {
                "q": company_name,
                "from": from_date,
                "sortBy": "publishedAt",
                "language": "en",
                "apiKey": self.api_key
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Extract articles
            articles = []
            for article in data.get("articles", [])[:5]:  # Top 5 articles
                articles.append({
                    "title": article.get("title"),
                    "source": article.get("source", {}).get("name"),
                    "date": article.get("publishedAt", "").split("T")[0],
                    "url": article.get("url"),
                    "summary": article.get("description")
                })

            logger.info(f"Fetched {len(articles)} news articles for {company_name}")

            return {
                "company": company_name,
                "articles": articles,
                "total_articles": len(articles),
                "last_updated": datetime.utcnow().isoformat() + "Z",
                "date_range": f"last {days_back} days (2026)",
                "source": "NewsAPI",
                "data_quality": "LIVE"
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching news for {company_name}: {e}")
            return None


def load_2026_news_data(company_list: List[str], api_key: Optional[str] = None) -> Dict:
    """
    Load real 2026 news for all companies.

    Returns:
        {
            "Reckitt": {
                "articles": [...],
                "total_articles": 12,
                "date_range": "last 30 days (2026)"
            },
            ...
        }
    """

    loader = NewsAPILoader(api_key)
    results = {}

    for company in company_list:
        news = loader.get_company_news(company)

        if news:
            results[company] = news
            print(f"✅ {company}: {news.get('total_articles')} news articles (2026)")
        else:
            print(f"⚠️  {company}: Unable to fetch news (API unavailable)")

    return results


if __name__ == "__main__":
    import os

    companies = ["Reckitt", "Unilever", "Apple", "Microsoft"]

    print("📰 Fetching real 2026 news from NewsAPI...")
    api_key = os.environ.get("NEWSAPI_KEY")

    if not api_key:
        print("⚠️  NEWSAPI_KEY not set. To use this loader:")
        print("   1. Sign up at https://newsapi.org/")
        print("   2. Get API key from your account")
        print("   3. Set: export NEWSAPI_KEY=<your-key>")

    data = load_2026_news_data(companies, api_key)

    print("\n✅ 2026 News Data (LIVE):")
    print(json.dumps(data, indent=2))
