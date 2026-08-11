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
            self._fetch_trends()  # NEW: Fetch financial history for trends

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
                "gsk": {
                    "description": "Global pharmaceutical and healthcare company specializing in vaccines, oncology, and specialty medicines",
                    "headquarters": "London, United Kingdom",
                    "sector": "Pharmaceutical & Healthcare",
                    "founded": "2000",
                    "website": "gsk.com",
                    "brands": ["Avandia", "Avodart", "Cervarix", "Dyrenium", "Flonase", "Polarimine", "Trizivir"],
                },
                "nestlé": {
                    "description": "World's largest packaged food and beverage company with brands across coffee, petcare, nutrition, and food",
                    "headquarters": "Vevey, Switzerland",
                    "sector": "Food & Beverage",
                    "founded": "1866",
                    "website": "nestle.com",
                    "brands": ["Nescafé", "Purina", "KitKat", "Nespresso", "Maggi", "Aero", "Smarties", "Milky Bar"],
                },
                "nestle": {
                    "description": "World's largest packaged food and beverage company with brands across coffee, petcare, nutrition, and food",
                    "headquarters": "Vevey, Switzerland",
                    "sector": "Food & Beverage",
                    "founded": "1866",
                    "website": "nestle.com",
                    "brands": ["Nescafé", "Purina", "KitKat", "Nespresso", "Maggi", "Aero", "Smarties", "Milky Bar"],
                },
                "procter & gamble": {
                    "description": "American multinational consumer goods company with focus on beauty, health, fabric, and home care",
                    "headquarters": "Cincinnati, Ohio, USA",
                    "sector": "Consumer Goods",
                    "founded": "1837",
                    "website": "pg.com",
                    "brands": ["Tide", "Gillette", "Olay", "Pampers", "Ariel", "Duracell", "Vicks", "Pantene"],
                },
                "procter and gamble": {
                    "description": "American multinational consumer goods company with focus on beauty, health, fabric, and home care",
                    "headquarters": "Cincinnati, Ohio, USA",
                    "sector": "Consumer Goods",
                    "founded": "1837",
                    "website": "pg.com",
                    "brands": ["Tide", "Gillette", "Olay", "Pampers", "Ariel", "Duracell", "Vicks", "Pantene"],
                },
                "pfizer": {
                    "description": "American multinational pharmaceutical corporation specializing in vaccines, oncology, and primary care",
                    "headquarters": "New York City, USA",
                    "sector": "Pharmaceutical",
                    "founded": "1849",
                    "website": "pfizer.com",
                    "brands": ["Viagra", "Lipitor", "Lyrica", "Eliquis", "Xeljanz", "Prevnar", "Comirnaty"],
                },
                "moderna": {
                    "description": "American biotechnology company pioneering mRNA vaccine platform",
                    "headquarters": "Cambridge, Massachusetts, USA",
                    "sector": "Biotechnology & Pharmaceuticals",
                    "founded": "2010",
                    "website": "modernatx.com",
                    "brands": ["Spikevax", "Moderna mRNA Platform", "Seasonal Boosters"],
                },
                "johnson & johnson": {
                    "description": "American multinational healthcare conglomerate with pharma, medical devices, and consumer health",
                    "headquarters": "New Brunswick, New Jersey, USA",
                    "sector": "Pharmaceutical & Healthcare",
                    "founded": "1886",
                    "website": "jnj.com",
                    "brands": ["Tylenol", "Listerine", "Neutrogena", "Acuvue", "DePuy", "Ethicon", "Janssen"],
                },
                "amazon": {
                    "description": "American multinational technology company with ecommerce, cloud computing, and digital services",
                    "headquarters": "Seattle, Washington, USA",
                    "sector": "Technology & Ecommerce",
                    "founded": "1994",
                    "website": "amazon.com",
                    "brands": ["Amazon.com", "AWS", "Prime Video", "Whole Foods", "Ring", "Alexa"],
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
        """Fetch stock data from yfinance including valuation metrics."""
        try:
            import yfinance as yf

            # Map company names to tickers
            ticker_map = {
                "reckitt": "RKT.L",
                "henkel": "HEN3.DE",
                "unilever": "ULVR.L",
                "gsk": "GSK.L",
                "google": "GOOGL",
                "netflix": "NFLX",
                "apple": "AAPL",
                "microsoft": "MSFT",
                "amazon": "AMZN",
                "tesla": "TSLA",
                "meta": "META",
                "nestlé": "NSRGY",
                "nestle": "NSRGY",
                "procter & gamble": "PG",
                "procter and gamble": "PG",
                "pfizer": "PFE",
                "moderna": "MRNA",
                "johnson & johnson": "JNJ",
                "johnson and johnson": "JNJ",
                "s.c. johnson": "SC",
            }

            ticker = ticker_map.get(self.company_name.lower())
            if ticker:
                stock = yf.Ticker(ticker)
                info = stock.info

                market_cap = info.get("marketCap")
                pe_ratio = info.get("trailingPE")
                dividend_yield = info.get("dividendYield")
                week_52_high = info.get("fiftyTwoWeekHigh")
                week_52_low = info.get("fiftyTwoWeekLow")

                self.basics["stock"] = {
                    "ticker": ticker,
                    "price": info.get("currentPrice", info.get("regularMarketPrice")),
                    "change": info.get("regularMarketChangePercent", 0),
                    "market_cap": market_cap,
                    "market_cap_billions": round(market_cap / 1e9, 1) if market_cap else None,
                    "pe_ratio": round(pe_ratio, 1) if pe_ratio else None,
                    "dividend_yield": round(dividend_yield * 100, 2) if dividend_yield else None,
                    "week_52_high": round(week_52_high, 2) if week_52_high else None,
                    "week_52_low": round(week_52_low, 2) if week_52_low else None,
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

    def _fetch_trends(self):
        """Fetch financial trends from Supabase database."""
        try:
            from supabase import create_client
            import os

            # Initialize empty trends
            self.basics["trends"] = {"revenue": {}, "margin": {}, "employees": {}}
            self.basics["financials"] = {}

            try:
                db = create_client(
                    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
                    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
                )
            except Exception as db_error:
                logger.warning(f"[trends] Database connection failed: {str(db_error)[:100]}")
                return

            # Fetch all financial records for this company
            company_lower = self.company_name.lower().strip()
            try:
                result = db.table("company_financials").select("*").eq("company_name", company_lower).order("period", desc=False).execute()
            except Exception as query_error:
                logger.warning(f"[trends] Query failed for {company_lower}: {str(query_error)[:100]}")
                return

            if result.data and len(result.data) > 0:
                # Build trends dict organized by metric
                trends = {
                    "revenue": {},
                    "margin": {},
                    "employees": {}
                }

                for record in result.data:
                    try:
                        period = str(record.get("period", ""))
                        revenue = record.get("revenue_millions")
                        margin = record.get("operating_margin_pct")
                        employees = record.get("employees")

                        if period and revenue:
                            trends["revenue"][period] = revenue
                        if period and margin:
                            trends["margin"][period] = margin
                        if period and employees:
                            trends["employees"][period] = employees
                    except Exception as record_error:
                        logger.debug(f"[trends] Error processing record: {record_error}")
                        continue

                # Also fetch latest financials for display
                try:
                    latest = max(result.data, key=lambda x: str(x.get("period", "")))
                    self.basics["financials"] = {
                        "revenue_millions": latest.get("revenue_millions"),
                        "operating_margin_pct": latest.get("operating_margin_pct"),
                        "employees": latest.get("employees"),
                        "revenue_growth_pct": latest.get("revenue_growth_pct"),
                        "period": latest.get("period")
                    }
                except Exception as latest_error:
                    logger.debug(f"[trends] Error processing latest: {latest_error}")

                self.basics["trends"] = trends
                logger.info(f"[trends] Fetched {len(result.data)} periods for {self.company_name}")
            else:
                logger.debug(f"[trends] No financial data found for {self.company_name}")

        except Exception as e:
            logger.error(f"[trends] Unexpected error: {str(e)[:100]}")
            self.basics["trends"] = {"revenue": {}, "margin": {}, "employees": {}}

    @staticmethod
    def answer_question(company_name: str, question: str) -> str:
        """
        Answer a question about a company using Groq API (fast + cheap).
        Falls back to local database for direct questions.
        """
        try:
            import os
            import requests

            # Check for direct competitor question
            q_lower = question.lower()
            if any(word in q_lower for word in ["competitor", "rivals", "competition", "competitors"]):
                competitors = get_competitor_list(company_name)
                if competitors:
                    comp_str = ", ".join(competitors)
                    return f"Main competitors for {company_name}: {comp_str}. These companies compete directly in the same market segments."
                else:
                    return f"Competitors for {company_name} not yet mapped. Try asking about their brands or strategy instead."

            # Check for brand question (but not if asking about market share, revenue, etc.)
            if any(word in q_lower for word in ["what brands", "what are your brands", "brand", "product line", "list your brands"]):
                if not any(word in q_lower for word in ["market", "share", "revenue", "price"]):
                    intel = CompanyIntelligence(company_name)
                    intel.fetch_all()
                    if intel.basics.get("brands"):
                        brands = ", ".join(intel.basics["brands"])
                        return f"{company_name}'s main brands: {brands}"
                    else:
                        return f"Brand information for {company_name} not available. Try: 'Tell me about {company_name}'"

            # Check for strategy/acquisition questions (use database first, then Groq)
            if any(word in q_lower for word in ["strategy", "acquisition", "acquire", "growth", "focus"]):
                try:
                    from .sms_service import get_supabase
                    supabase = get_supabase()

                    # Get M&A deals to infer strategy
                    deals_response = supabase.table("company_deals").select("*").eq("company_name", company_name).order("year", desc=True).limit(5).execute()
                    deals = deals_response.data if deals_response.data else []

                    # Get financials to infer growth strategy
                    financials_response = supabase.table("company_financials").select("*").eq("company_name", company_name).order("year", desc=True).limit(5).execute()
                    financials = financials_response.data if financials_response.data else []

                    if deals or financials:
                        # Build strategy insight from M&A and financials
                        strategy_parts = []

                        # M&A pattern analysis
                        if deals:
                            deal_types = {}
                            for deal in deals:
                                dtype = deal.get("deal_type", "").title()
                                deal_types[dtype] = deal_types.get(dtype, 0) + 1
                            deal_summary = ", ".join([f"{count} {dtype}s" for dtype, count in deal_types.items()])
                            strategy_parts.append(f"Recent M&A: {deal_summary}")

                        # Growth pattern
                        if financials and len(financials) >= 2:
                            latest = financials[0]
                            prev = financials[1]
                            revenue_latest = latest.get("revenue", 0)
                            revenue_prev = prev.get("revenue", 0)
                            if revenue_latest and revenue_prev and revenue_prev > 0:
                                growth = ((revenue_latest - revenue_prev) / revenue_prev) * 100
                                if growth > 5:
                                    strategy_parts.append(f"Strong organic growth ({growth:.1f}% YoY)")
                                elif growth < -2:
                                    strategy_parts.append(f"Restructuring phase ({growth:.1f}% YoY)")

                        if strategy_parts:
                            return f"{company_name} strategy: {' | '.join(strategy_parts)}"
                except Exception as db_err:
                    logger.warning(f"[Q&A] Strategy database fallback failed: {db_err}")

            groq_api_key = os.environ.get("GROQ_API_KEY")
            if not groq_api_key:
                logger.error("[Q&A] GROQ_API_KEY not set")
                return "System error: API key missing. Try: 'What are their brands?' or 'Who are their competitors?'"

            # System prompt for company Q&A
            system_prompt = f"""You are a company intelligence expert. Answer questions about {company_name}.
Be concise, factual, direct. 2-3 sentences max.
Focus: business model, strategy, AI/tech focus, competitors, market position, brands, market share.
IMPORTANT: If asked about brands or market share, provide specific numbers and percentages."""

            # Call Groq API
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {groq_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "mixtral-8x7b-32768",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": question}
                    ],
                    "max_tokens": 500,
                    "temperature": 0.7
                },
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                if data.get("choices") and len(data["choices"]) > 0:
                    answer = data["choices"][0].get("message", {}).get("content", "")
                    if answer and answer.strip():
                        logger.info(f"[Q&A] Groq response for {company_name}: {answer[:100]}...")
                        return answer.strip()

            logger.warning(f"[Q&A] Groq returned empty or error: {response.status_code}")
            return "Try asking: 'Who are their competitors?', 'What are their brands?', or 'Tell me about their strategy'"

        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"[Q&A] Groq error: {error_msg}")

            # Suggest rephrasing based on error type
            if "rate" in error_msg or "quota" in error_msg:
                return "System busy. Try: 'Who are their competitors?' or 'What brands do they have?'"
            elif "timeout" in error_msg:
                return "Request timed out. Try: 'Tell me about their brands' or 'Who is their competition?'"
            elif any(word in error_msg for word in ["invalid", "token", "auth", "api"]):
                return "System error. Try asking about competitors or brands."
            else:
                return "Having trouble with that question. Try: 'Who are their competitors?' or 'What brands do they have?'"


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
        "gsk": ["Pfizer", "Moderna", "Johnson & Johnson"],
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
