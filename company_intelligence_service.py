"""
Company Intelligence Service
Fetches company basics and provides Q&A capabilities.
"""

import requests
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CompanyIntelligence:
    """Fetch company data and answer questions about companies."""

    def __init__(self, company_name: str):
        self.company_name = company_name
        self.basics = {}
        self.news = []

    def fetch_all(self) -> dict:
        """Fetch all company basics."""
        try:
            self.basics = {
                "name": self.company_name,
                "timestamp": datetime.utcnow().isoformat(),
            }

            # Fetch each data source
            self._fetch_company_info()
            self._fetch_stock_data()
            self._fetch_news()

            return self.basics
        except Exception as e:
            logger.error(f"Error fetching company data: {e}")
            return {"error": str(e), "name": self.company_name}

    def _fetch_company_info(self):
        """Fetch company info from multiple sources."""
        try:
            # Company knowledge base - real data
            company_info = {
                "reckitt benckiser": {
                    "description": "FMCG company specializing in hygiene, health, and home care products",
                    "headquarters": "London, United Kingdom",
                    "sector": "Consumer Goods & Health",
                    "founded": "1840",
                    "website": "reckitt.com",
                    "brands": ["Dettol", "Lysol", "Nurofen", "Air Wick", "Gaviscon", "Finish", "Strepsils", "Clearasil"],
                },
                "reckitt": {
                    "description": "FMCG company specializing in hygiene, health, and home care products",
                    "headquarters": "London, United Kingdom",
                    "sector": "Consumer Goods & Health",
                    "founded": "1840",
                    "website": "reckitt.com",
                    "brands": ["Dettol", "Lysol", "Nurofen", "Air Wick", "Gaviscon", "Finish", "Strepsils", "Clearasil"],
                },
                "henkel": {
                    "description": "German multinational chemical and consumer goods company",
                    "headquarters": "Düsseldorf, Germany",
                    "sector": "Consumer Goods",
                    "founded": "1876",
                    "website": "henkel.com",
                    "brands": ["Persil", "Schwarzkopf", "Dial", "Right Guard", "Pritt", "Loctite"],
                },
                "unilever": {
                    "description": "British-Dutch multinational FMCG company with brands in beauty, food, health, and home care",
                    "headquarters": "London, United Kingdom & Rotterdam, Netherlands",
                    "sector": "Consumer Goods & Food",
                    "founded": "1930",
                    "website": "unilever.com",
                    "brands": ["Dove", "Axe", "Knorr", "Ben & Jerry's", "Hellmann's", "Lipton", "Magnum", "Cif"],
                },
                "sc johnson": {
                    "description": "Family-owned American multinational chemicals and consumer goods company",
                    "headquarters": "Racine, Wisconsin, USA",
                    "sector": "Consumer Goods",
                    "founded": "1886",
                    "website": "scjohnson.com",
                    "brands": ["Windex", "Raid", "Pledge", "Glade", "Mr Muscle", "Baygon"],
                },
                "google": {
                    "description": "American tech giant specializing in search, advertising, cloud computing, and AI",
                    "headquarters": "Mountain View, California, USA",
                    "sector": "Technology & Internet",
                    "founded": "1998",
                    "website": "google.com",
                    "brands": ["Google Search", "Chrome", "Android", "YouTube", "Gmail", "Maps", "Pixel"],
                },
                "apple": {
                    "description": "American technology company designing and manufacturing consumer electronics",
                    "headquarters": "Cupertino, California, USA",
                    "sector": "Technology & Electronics",
                    "founded": "1976",
                    "website": "apple.com",
                    "brands": ["iPhone", "Mac", "iPad", "Apple Watch", "AirPods", "Apple TV"],
                },
                "netflix": {
                    "description": "American streaming entertainment service with original content",
                    "headquarters": "Los Gatos, California, USA",
                    "sector": "Entertainment & Media",
                    "founded": "1997",
                    "website": "netflix.com",
                    "brands": ["Netflix Streaming", "Netflix Films", "Netflix Series"],
                },
                "microsoft": {
                    "description": "American technology multinational developing software, cloud computing, and gaming",
                    "headquarters": "Redmond, Washington, USA",
                    "sector": "Technology & Software",
                    "founded": "1975",
                    "website": "microsoft.com",
                    "brands": ["Windows", "Office", "Xbox", "Azure", "Teams", "Outlook"],
                },
            }

            company_lower = self.company_name.lower().strip()

            # Try exact match first
            if company_lower in company_info:
                self.basics.update(company_info[company_lower])
                self.basics["source"] = "Company Database"
                return

            # Try partial match
            for key, info in company_info.items():
                if key in company_lower or company_lower in key:
                    self.basics.update(info)
                    self.basics["source"] = "Company Database"
                    return

            # Fallback: try Wikipedia
            url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{self.company_name}"
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                data = response.json()
                self.basics["description"] = data.get("description", "")
                self.basics["extract"] = data.get("extract", "")[:300]
                self.basics["source"] = "Wikipedia"
            else:
                self.basics["description"] = f"Company: {self.company_name}"
                self.basics["source"] = "Search"

        except Exception as e:
            logger.debug(f"Company info fetch failed: {e}")
            self.basics["description"] = f"Company: {self.company_name}"

    def _fetch_stock_data(self):
        """Fetch stock data from yfinance."""
        try:
            import yfinance as yf

            # Map company names to tickers
            ticker_map = {
                "reckitt": "RKT.L",
                "henkel": "HEN3.DE",
                "unilever": "ULVR.L",
                "google": "GOOGL",
                "netflix": "NFLX",
                "apple": "AAPL",
                "microsoft": "MSFT",
                "amazon": "AMZN",
                "tesla": "TSLA",
                "meta": "META",
            }

            ticker = ticker_map.get(self.company_name.lower())
            if ticker:
                stock = yf.Ticker(ticker)
                info = stock.info

                self.basics["stock"] = {
                    "ticker": ticker,
                    "price": info.get("currentPrice", info.get("regularMarketPrice")),
                    "change": info.get("regularMarketChangePercent", 0),
                    "market_cap": info.get("marketCap"),
                    "employees": info.get("fullTimeEmployees"),
                }
            else:
                self.basics["stock"] = {"ticker": "N/A", "price": None}

        except Exception as e:
            logger.debug(f"Stock data fetch failed: {e}")
            self.basics["stock"] = {"ticker": "N/A", "error": str(e)}

    def _fetch_news(self):
        """Fetch recent news about company."""
        try:
            # Use NewsAPI (free tier available)
            import os
            api_key = os.environ.get("NEWSAPI_KEY", "6d61957fc82b49e9b0ad1d2e15e6e50e")
            url = f"https://newsapi.org/v2/everything"

            params = {
                "q": self.company_name,
                "sortBy": "publishedAt",
                "language": "en",
                "pageSize": 10,
                "apiKey": api_key
            }

            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                articles = data.get("articles", [])

                if articles:
                    self.news = [
                        {
                            "title": article.get("title", "N/A"),
                            "description": article.get("description", ""),
                            "source": article.get("source", {}).get("name", "News"),
                            "url": article.get("url", ""),
                            "published": article.get("publishedAt", "")[:10] if article.get("publishedAt") else "",
                        }
                        for article in articles[:8]
                    ]
                    self.basics["news"] = self.news
                    logger.info(f"[news] Found {len(self.news)} articles for {self.company_name}")
                else:
                    logger.warning(f"[news] No articles found for {self.company_name}")
                    self.basics["news"] = []
            else:
                logger.warning(f"[news] API returned {response.status_code}")
                self.basics["news"] = []

        except Exception as e:
            logger.error(f"[news] Fetch failed: {str(e)[:100]}")
            self.basics["news"] = []

    @staticmethod
    def answer_question(company_name: str, question: str) -> str:
        """
        Answer a question about a company using Claude API.
        """
        try:
            from anthropic import Anthropic

            client = Anthropic()

            # System prompt for company Q&A
            system_prompt = f"""You are a company intelligence expert. Answer questions about {company_name}.
Be concise, factual, direct. 2-3 sentences max.
Focus: business model, strategy, AI/tech focus, competitors, market position, brands, market share.
IMPORTANT: If asked about brands or market share, provide specific numbers and percentages."""

            message = client.messages.create(
                model="claude-opus-5",
                max_tokens=500,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": question}
                ]
            )

            logger.info(f"[Q&A] Response: {message}")
            logger.info(f"[Q&A] Content blocks: {len(message.content)} blocks")

            # Extract text from response content
            if message and message.content:
                for block in message.content:
                    logger.info(f"[Q&A] Block type: {block.type if hasattr(block, 'type') else type(block)}")
                    if hasattr(block, 'type') and block.type == "text":
                        return block.text
                    elif hasattr(block, 'text'):
                        return block.text

            logger.warning(f"[Q&A] No text block found in response")
            return "I found information but couldn't format it. Try: 'What are Reckitt's main brands?' or 'List brand market share'"

        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"[Q&A] Error: {error_msg}")

            # Suggest rephrasing based on error type
            if "rate" in error_msg or "quota" in error_msg:
                return "System busy. Please try again in a moment."
            elif "timeout" in error_msg:
                return "Request timed out. Try: 'What brands does Reckitt have?'"
            elif any(word in error_msg for word in ["invalid", "token", "auth"]):
                return "System error. Try: 'Tell me about Reckitt' or 'What is Reckitt?'"
            else:
                return "Having trouble with that question. Try: 'What are Reckitt's brands?' or 'List Reckitt brands and market share'"


def get_company_intelligence(company_name: str) -> dict:
    """Get all intelligence for a company."""
    intel = CompanyIntelligence(company_name)
    return intel.fetch_all()


def get_company_answer(company_name: str, question: str) -> str:
    """Get answer to a question about a company."""
    return CompanyIntelligence.answer_question(company_name, question)


def get_competitor_list(company_name: str) -> list:
    """Get list of main competitors for a company."""
    competitors_map = {
        "reckitt": ["Henkel", "Unilever", "SC Johnson"],
        "henkel": ["Reckitt", "Unilever", "Procter & Gamble"],
        "unilever": ["Henkel", "Procter & Gamble", "Reckitt"],
        "sc johnson": ["Reckitt", "Henkel", "Procter & Gamble"],
        "google": ["Microsoft", "Amazon", "Meta"],
        "apple": ["Microsoft", "Samsung", "Google"],
        "netflix": ["Amazon Prime", "Disney+", "HBO Max"],
        "microsoft": ["Google", "Apple", "Amazon"],
    }

    company_lower = company_name.lower().strip()
    for key, comps in competitors_map.items():
        if key in company_lower or company_lower in key:
            return comps

    return []
