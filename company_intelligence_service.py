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
        """Fetch company info from Wikipedia."""
        try:
            # Try Wikipedia API for company info
            url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{self.company_name}"
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                data = response.json()
                self.basics["description"] = data.get("description", "")
                self.basics["extract"] = data.get("extract", "")[:200]  # First 200 chars
                self.basics["source"] = "Wikipedia"
            else:
                self.basics["description"] = f"Company: {self.company_name}"
                self.basics["source"] = "Search"

        except Exception as e:
            logger.debug(f"Wikipedia fetch failed: {e}")
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
            api_key = "6d61957fc82b49e9b0ad1d2e15e6e50e"  # Free tier key for demo
            url = f"https://newsapi.org/v2/everything"

            params = {
                "q": self.company_name,
                "sortBy": "publishedAt",
                "language": "en",
                "pageSize": 5,
                "apiKey": api_key
            }

            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                self.news = [
                    {
                        "title": article["title"],
                        "description": article["description"],
                        "source": article["source"]["name"],
                        "url": article["url"],
                        "published": article["publishedAt"][:10],
                    }
                    for article in data.get("articles", [])[:5]
                ]
                self.basics["news"] = self.news
            else:
                self.basics["news"] = []

        except Exception as e:
            logger.debug(f"News fetch failed: {e}")
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
            system_prompt = f"""You are a company intelligence expert.
Answer questions about {company_name} based on public information you know.
Be concise, factual, and direct. If you don't have specific data, say so.
Focus on: business model, strategy, market position, growth, challenges, AI/tech focus."""

            message = client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=300,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": question}
                ]
            )

            return message.content[0].text

        except Exception as e:
            logger.error(f"Claude API error: {e}")
            return f"Unable to answer question: {str(e)}"


def get_company_intelligence(company_name: str) -> dict:
    """Get all intelligence for a company."""
    intel = CompanyIntelligence(company_name)
    return intel.fetch_all()


def get_company_answer(company_name: str, question: str) -> str:
    """Get answer to a question about a company."""
    return CompanyIntelligence.answer_question(company_name, question)
