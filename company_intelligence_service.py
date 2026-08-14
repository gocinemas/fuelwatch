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
                "exxonmobil": {
                    "description": "American multinational oil and gas corporation with operations in upstream, downstream, and chemical segments",
                    "headquarters": "Spring, Texas, USA",
                    "sector": "Energy & Oil/Gas",
                    "founded": "1870",
                    "website": "exxonmobil.com",
                    "brands": ["Exxon", "Mobil", "Esso"],
                },
                "chevron": {
                    "description": "American multinational energy corporation with oil and gas exploration, production, and refining operations",
                    "headquarters": "San Ramon, California, USA",
                    "sector": "Energy & Oil/Gas",
                    "founded": "1879",
                    "website": "chevron.com",
                    "brands": ["Chevron", "Texaco", "Caltex"],
                },
                "shell": {
                    "description": "British-Dutch multinational oil and gas company with integrated energy operations",
                    "headquarters": "The Hague, Netherlands",
                    "sector": "Energy & Oil/Gas",
                    "founded": "1907",
                    "website": "shell.com",
                    "brands": ["Shell", "Pennzoil"],
                },
                "airbnb": {
                    "description": "American online hospitality service enabling peer-to-peer accommodation rental",
                    "headquarters": "San Francisco, California, USA",
                    "sector": "Travel & Hospitality",
                    "founded": "2008",
                    "website": "airbnb.com",
                    "brands": ["Airbnb"],
                },
                "uber": {
                    "description": "American ride-hailing and food delivery technology platform",
                    "headquarters": "San Francisco, California, USA",
                    "sector": "Technology & Transportation",
                    "founded": "2009",
                    "website": "uber.com",
                    "brands": ["Uber", "Uber Eats", "Uber Freight"],
                },
                "starbucks": {
                    "description": "American multinational coffee company operating 35K+ locations globally",
                    "headquarters": "Seattle, Washington, USA",
                    "sector": "Food & Beverage",
                    "founded": "1971",
                    "website": "starbucks.com",
                    "brands": ["Starbucks", "Teavana", "Evolution Fresh"],
                },
                "coca-cola": {
                    "description": "American multinational beverage company with 500+ brands in over 200 countries",
                    "headquarters": "Atlanta, Georgia, USA",
                    "sector": "Food & Beverage",
                    "founded": "1886",
                    "website": "coca-cola.com",
                    "brands": ["Coca-Cola", "Sprite", "Fanta", "Minute Maid", "Dasani", "Costa Coffee"],
                },
                "tesla": {
                    "description": "American electric vehicle and clean energy company with automotive and energy storage products",
                    "headquarters": "Austin, Texas, USA",
                    "sector": "Automotive & Energy",
                    "founded": "2003",
                    "website": "tesla.com",
                    "brands": ["Tesla", "Powerwall", "Supercharger"],
                },
                "lvmh": {
                    "description": "French multinational luxury goods conglomerate with 100+ brands across fashion, leather, jewelry, and watches",
                    "headquarters": "Paris, France",
                    "sector": "Luxury & Fashion",
                    "founded": "1987",
                    "website": "lvmh.com",
                    "brands": ["Louis Vuitton", "Dior", "Fendi", "Céline", "Givenchy", "Celine", "Loro Piana"],
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
                "exxonmobil": "XOM",
                "chevron": "CVX",
                "shell": "SHEL",
                "airbnb": "ABNB",
                "uber": "UBER",
                "starbucks": "SBUX",
                "coca-cola": "KO",
                "lvmh": "LVMH.PA",
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

                # Yahoo Finance returns dividend_yield as decimal (0.0267 = 2.67%), so just format it
                dividend_yield_pct = round(dividend_yield * 100, 2) if dividend_yield and dividend_yield < 1 else round(dividend_yield, 2) if dividend_yield else None

                self.basics["stock"] = {
                    "ticker": ticker,
                    "price": info.get("currentPrice", info.get("regularMarketPrice")),
                    "change": info.get("regularMarketChangePercent", 0),
                    "market_cap": market_cap,
                    "market_cap_billions": round(market_cap / 1e9, 1) if market_cap else None,
                    "pe_ratio": round(pe_ratio, 1) if pe_ratio else None,
                    "dividend_yield": dividend_yield_pct,
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
        Answer a question about a company using intelligent intent detection + layered fallback.
        1. Classify question intent (competitor, brands, strategy, financial, etc.)
        2. Try database handlers for that intent
        3. Fall back to Groq API if needed
        """
        try:
            import os
            import requests
            import library as lib
            from qa_intent import detect_intent, get_answer_strategy
            from qa_handlers import DatabaseHandlers

            # Step 1: Detect intent
            intent = detect_intent(question)
            logger.info(f"[Q&A] Question intent: {intent}")

            # Step 2: Get fallback strategy for this intent
            strategy = get_answer_strategy(intent)

            # Step 3: Try handlers in order
            supabase = lib._sb()
            for source, handler_name in strategy:
                if source == "database":
                    # Try database handler
                    handler = getattr(DatabaseHandlers, handler_name, None)
                    if handler:
                        try:
                            answer = handler(company_name, supabase)
                            if answer:
                                logger.info(f"[Q&A] Handler '{handler_name}' returned answer")
                                return answer
                        except Exception as handler_err:
                            logger.warning(
                                f"[Q&A] Handler '{handler_name}' failed: {handler_err}"
                            )
                            continue

                elif source == "groq":
                    # Fall back to Groq API
                    groq_api_key = os.environ.get("GROQ_API_KEY")
                    if not groq_api_key:
                        logger.error("[Q&A] GROQ_API_KEY not set")
                        continue

                    system_prompt = f"""You are a company intelligence expert. Answer about {company_name}.
Be concise, factual, direct. 2-3 sentences max.
Focus: business model, strategy, competitors, market position, brands, growth."""

                    try:
                        response = requests.post(
                            "https://api.groq.com/openai/v1/chat/completions",
                            headers={
                                "Authorization": f"Bearer {groq_api_key}",
                                "Content-Type": "application/json",
                            },
                            json={
                                "model": "mixtral-8x7b-32768",
                                "messages": [
                                    {"role": "system", "content": system_prompt},
                                    {"role": "user", "content": question},
                                ],
                                "max_tokens": 500,
                                "temperature": 0.7,
                            },
                            timeout=10,
                        )

                        if response.status_code == 200:
                            data = response.json()
                            if data.get("choices") and len(data["choices"]) > 0:
                                answer = (
                                    data["choices"][0]
                                    .get("message", {})
                                    .get("content", "")
                                )
                                if answer and answer.strip():
                                    logger.info(f"[Q&A] Groq returned answer")
                                    return answer.strip()

                        logger.warning(
                            f"[Q&A] Groq returned empty: {response.status_code}"
                        )
                    except requests.Timeout:
                        logger.warning("[Q&A] Groq timeout")
                    except Exception as groq_err:
                        logger.warning(f"[Q&A] Groq failed: {groq_err}")

            # All handlers failed
            return "Unable to answer that question. Try: 'Who are their competitors?', 'What brands do they own?', or 'Tell me about their strategy'"

        except Exception as e:
            logger.error(f"[Q&A] Unexpected error: {e}")
            return "Having trouble with that question. Try asking about competitors or brands."


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
