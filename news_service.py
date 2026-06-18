"""
Real news integration via NewsAPI.org
Fetches actual brand news articles instead of synthetic data
"""

import requests
from datetime import datetime, timedelta

NEWSAPI_KEY = "33adec4e7113463586eb63afefa3b131"
NEWSAPI_URL = "https://newsapi.org/v2/everything"

def fetch_brand_news(brand_name, days_back=30):
    """
    Fetch real news articles for a brand from NewsAPI.org
    Returns up to 5 most recent, relevant articles
    """
    try:
        # Search for brand news from last 30 days
        from_date = (datetime.now() - timedelta(days=days_back)).isoformat()

        params = {
            "q": brand_name,
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": 5,
            "from": from_date,
            "apiKey": NEWSAPI_KEY
        }

        response = requests.get(NEWSAPI_URL, params=params, timeout=5)

        if response.status_code != 200:
            return []

        data = response.json()
        articles = data.get("articles", [])

        # Format articles for display
        formatted_news = []
        for article in articles[:5]:  # Limit to 5
            formatted_news.append({
                "title": article.get("title", ""),
                "source": article.get("source", {}).get("name", "Unknown"),
                "published_date": article.get("publishedAt", ""),
                "category": extract_category(article.get("description", "")),
                "url": article.get("url", ""),
                "description": article.get("description", ""),
                "image": article.get("urlToImage", "")
            })

        return formatted_news

    except Exception as e:
        print(f"Error fetching news for {brand_name}: {e}")
        return []

def extract_category(description):
    """
    Infer news category from description
    """
    if not description:
        return "News"

    desc_lower = description.lower()

    if any(word in desc_lower for word in ["profit", "revenue", "earnings", "financial", "growth"]):
        return "Financial"
    elif any(word in desc_lower for word in ["product", "launch", "new", "release"]):
        return "Product"
    elif any(word in desc_lower for word in ["market", "competitor", "competition"]):
        return "Market Position"
    elif any(word in desc_lower for word in ["sustain", "environment", "green", "eco"]):
        return "Sustainability"
    elif any(word in desc_lower for word in ["ai", "artificial", "intelligence", "tech", "digital"]):
        return "Innovation"
    else:
        return "News"
