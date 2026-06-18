"""
Real news integration via NewsAPI.org
Fetches actual brand news articles instead of synthetic data
"""

import requests
from datetime import datetime, timedelta

NEWSAPI_KEY = "33adec4e7113463586eb63afefa3b131"
NEWSAPI_URL = "https://newsapi.org/v2/everything"

# Blacklist low-quality sources (deals, coupons, ads, aggregators)
BLACKLIST_SOURCES = {
    "southernsavers.com",
    "ozbargain.com.au",
    "slickdeals.net",
    "dealsplus.com",
    "retailmenotcom",
    "fatwallet.com",
    "coupons.com",
    "groupon.com",
    "dealabs.com",
    "honey.com"
}

# Whitelisted quality business news sources
QUALITY_SOURCES = {
    "bloomberg.com",
    "reuters.com",
    "ft.com",
    "wsj.com",
    "cnbc.com",
    "forbes.com",
    "businessinsider.com",
    "techcrunch.com",
    "marketwatch.com",
    "seekingalpha.com",
    "bbc.com",
    "guardian.com",
    "telegraph.co.uk",
    "economist.com"
}

def fetch_brand_news(brand_name, days_back=30):
    """
    Fetch REAL business news about a brand/company from NewsAPI.org
    Filters out deal sites, coupon aggregators, and low-quality sources
    Returns up to 5 most recent, relevant articles
    """
    try:
        from_date = (datetime.now() - timedelta(days=days_back)).isoformat()

        # Search for brand/company news (not just any mention)
        # Prefer company announcements and business coverage
        search_query = f'"{brand_name}" (company OR earnings OR financial OR product OR strategy OR announcement OR acquisition OR partnership OR CEO OR executive)'

        params = {
            "q": search_query,
            "sortBy": "relevancy",  # Most relevant first, not just latest
            "language": "en",
            "pageSize": 30,  # Get more to filter
            "from": from_date,
            "apiKey": NEWSAPI_KEY
        }

        response = requests.get(NEWSAPI_URL, params=params, timeout=5)

        if response.status_code != 200:
            return []

        data = response.json()
        articles = data.get("articles", [])

        # Filter and format articles
        formatted_news = []
        for article in articles:
            source_url = article.get("url", "").lower()
            source_name = article.get("source", {}).get("name", "").lower()

            # Skip blacklisted sources
            if any(blacklist in source_url for blacklist in BLACKLIST_SOURCES):
                continue

            # Skip if it's just a deal/price mention (low-quality)
            title = article.get("title", "").lower()
            description = article.get("description", "").lower()
            if any(word in title + description for word in ["deal", "$", "discount", "coupon", "sale", "off", "promo", "weekly ad"]):
                # Only skip if ONLY about deals, not business news
                if not any(word in title + description for word in ["earnings", "revenue", "financial", "strategy", "ceo", "executive", "acquisition", "partnership"]):
                    continue

            formatted_news.append({
                "title": article.get("title", ""),
                "source": article.get("source", {}).get("name", "Unknown"),
                "published_date": article.get("publishedAt", ""),
                "category": extract_category(article.get("description", "")),
                "url": article.get("url", ""),
                "description": article.get("description", ""),
                "image": article.get("urlToImage", "")
            })

            if len(formatted_news) >= 5:
                break

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
