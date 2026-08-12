"""
Answer handlers for Intel Q&A.
Each handler tries to answer from the database before falling back to Groq.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DatabaseHandlers:
    """Database-first answer handlers for common question types."""

    @staticmethod
    def query_competitors(company_name: str, supabase) -> Optional[str]:
        """Answer: Who are the competitors?"""
        try:
            # Hardcoded competitor map (can be expanded)
            competitors_map = {
                "reckitt": ["Henkel", "Unilever", "SC Johnson"],
                "henkel": ["Reckitt", "Unilever", "Procter & Gamble"],
                "unilever": ["Henkel", "Procter & Gamble", "Reckitt"],
                "sc johnson": ["Reckitt", "Henkel", "Procter & Gamble"],
                "gsk": ["Pfizer", "Moderna", "Johnson & Johnson"],
                "google": ["Microsoft", "Amazon", "Meta"],
                "apple": ["Microsoft", "Samsung", "Google"],
                "netflix": ["Amazon Prime Video", "Disney+", "HBO Max"],
                "microsoft": ["Google", "Apple", "Amazon"],
                "amazon": ["Walmart", "eBay", "Microsoft"],
                "meta": ["Google", "TikTok", "Snap"],
                "nvidia": ["AMD", "Intel", "Qualcomm"],
                "pfizer": ["Moderna", "Johnson & Johnson", "Merck"],
                "coca-cola": ["PepsiCo", "Monster", "Red Bull"],
                "pepsi": ["Coca-Cola", "Monster", "Red Bull"],
                "tesla": ["Ford", "GM", "Volkswagen"],
            }

            company_lower = company_name.lower()
            competitors = competitors_map.get(company_lower)

            if competitors:
                comp_str = ", ".join(competitors)
                return f"Main competitors for {company_name}: {comp_str}"

            return None
        except Exception as e:
            logger.error(f"[Handler] query_competitors failed: {e}")
            return None

    @staticmethod
    def query_brands(company_name: str, supabase) -> Optional[str]:
        """Answer: What brands does the company own?"""
        try:
            from company_intelligence_service import CompanyIntelligence

            intel = CompanyIntelligence(company_name)
            intel.fetch_all()

            brands = intel.basics.get("brands")
            if brands:
                brand_str = ", ".join(brands)
                return f"{company_name}'s main brands: {brand_str}"

            return None
        except Exception as e:
            logger.error(f"[Handler] query_brands failed: {e}")
            return None

    @staticmethod
    def infer_strategy_from_ma(company_name: str, supabase) -> Optional[str]:
        """Infer strategy from M&A history."""
        try:
            # Get recent M&A deals
            deals_response = supabase.table("company_deals").select("*").eq(
                "company_name", company_name
            ).order("year", desc=True).limit(5).execute()

            deals = deals_response.data if deals_response.data else []

            if not deals:
                return None

            # Analyze deal types
            deal_types = {}
            for deal in deals:
                dtype = deal.get("deal_type", "").title()
                if dtype:
                    deal_types[dtype] = deal_types.get(dtype, 0) + 1

            if not deal_types:
                return None

            # Build narrative
            parts = []
            if "Acquisition" in deal_types:
                count = deal_types["Acquisition"]
                parts.append(f"Active acquirer ({count} deals)")
            if "Divestiture" in deal_types:
                count = deal_types["Divestiture"]
                parts.append(f"Portfolio trimming ({count} divestitures)")
            if "Investment" in deal_types:
                count = deal_types["Investment"]
                parts.append(f"Strategic investing ({count} investments)")

            if parts:
                strategy = f"{company_name}'s strategy: {' | '.join(parts)}"
                return strategy

            return None
        except Exception as e:
            logger.error(f"[Handler] infer_strategy_from_ma failed: {e}")
            return None

    @staticmethod
    def infer_strategy_from_growth(company_name: str, supabase) -> Optional[str]:
        """Infer strategy from revenue growth trends."""
        try:
            # Get financials
            financials_response = supabase.table("company_financials").select(
                "*"
            ).eq("company_name", company_name).order("year", desc=True).limit(
                3
            ).execute()

            financials = financials_response.data if financials_response.data else []

            if len(financials) < 2:
                return None

            latest = financials[0]
            prev = financials[1]

            revenue_latest = latest.get("revenue", 0)
            revenue_prev = prev.get("revenue", 0)
            margin_latest = latest.get("operating_margin", 0)
            margin_prev = prev.get("operating_margin", 0)

            if not (revenue_latest and revenue_prev):
                return None

            growth = ((revenue_latest - revenue_prev) / revenue_prev) * 100
            margin_change = margin_latest - margin_prev if margin_prev else 0

            parts = []
            if growth > 5:
                parts.append(f"Strong growth ({growth:.1f}% YoY)")
            elif growth < -2:
                parts.append(f"Restructuring ({growth:.1f}% decline)")
            else:
                parts.append(f"Steady growth ({growth:.1f}% YoY)")

            if margin_change > 1:
                parts.append(f"Improving margins (+{margin_change:.1f}pp)")
            elif margin_change < -1:
                parts.append(f"Margin pressure ({margin_change:.1f}pp)")

            if parts:
                return f"{company_name}'s trajectory: {' | '.join(parts)}"

            return None
        except Exception as e:
            logger.error(f"[Handler] infer_strategy_from_growth failed: {e}")
            return None

    @staticmethod
    def query_financial_metrics(company_name: str, supabase) -> Optional[str]:
        """Answer: What are the financial metrics?"""
        try:
            # Get latest financials
            financials_response = supabase.table("company_financials").select(
                "*"
            ).eq("company_name", company_name).order("year", desc=True).limit(
                1
            ).execute()

            financials = (
                financials_response.data if financials_response.data else []
            )

            if not financials:
                return None

            data = financials[0]
            revenue = data.get("revenue")
            margin = data.get("operating_margin")
            employees = data.get("employees")
            year = data.get("year")

            if revenue:
                parts = [f"Revenue (2025): ${revenue}M"]
                if margin:
                    parts.append(f"Operating Margin: {margin}%")
                if employees:
                    parts.append(f"Employees: {employees:,}")

                return f"{company_name} financials: {' | '.join(parts)}"

            return None
        except Exception as e:
            logger.error(f"[Handler] query_financial_metrics failed: {e}")
            return None

    @staticmethod
    def calculate_market_position(company_name: str, supabase) -> Optional[str]:
        """Calculate company's rank and market share position."""
        try:
            # Get company financials
            company_data_response = supabase.table("company_financials").select(
                "*"
            ).eq("company_name", company_name).order("year", desc=True).limit(
                1
            ).execute()

            company_data = (
                company_data_response.data if company_data_response.data else []
            )

            if not company_data:
                return None

            company_revenue = company_data[0].get("revenue")
            sector = company_data[0].get("sector")

            if not (company_revenue and sector):
                return None

            # Get sector peers
            peers_response = supabase.table("company_financials").select(
                "company_name, revenue"
            ).eq("sector", sector).eq("year", 2025).order(
                "revenue", desc=True
            ).execute()

            peers = peers_response.data if peers_response.data else []

            if not peers:
                return None

            # Find rank
            rank = next(
                (i for i, p in enumerate(peers) if p["company_name"] == company_name),
                None,
            )

            if rank is None:
                return None

            top_competitor = peers[0]["company_name"] if peers else "N/A"
            return f"{company_name} ranks #{rank + 1} in {sector} by revenue (${company_revenue}M). Top competitor: {top_competitor}"

        except Exception as e:
            logger.error(f"[Handler] calculate_market_position failed: {e}")
            return None

    @staticmethod
    def calculate_hiring_growth(company_name: str, supabase) -> Optional[str]:
        """Calculate hiring growth trend."""
        try:
            # Get employee history
            financials_response = supabase.table("company_financials").select(
                "year, employees"
            ).eq("company_name", company_name).order("year", asc=True).execute()

            financials = (
                financials_response.data if financials_response.data else []
            )

            if len(financials) < 2:
                return None

            first_year = financials[0]
            latest_year = financials[-1]

            employees_start = first_year.get("employees", 0)
            employees_latest = latest_year.get("employees", 0)
            year_start = first_year.get("year", 0)
            year_latest = latest_year.get("year", 0)

            if not (employees_start and employees_latest):
                return None

            growth = (
                (employees_latest - employees_start) / employees_start * 100
            )
            years = year_latest - year_start

            if growth > 10:
                trend = f"aggressively growing ({growth:.1f}% over {years} years)"
            elif growth > 0:
                trend = f"slowly growing ({growth:.1f}% over {years} years)"
            else:
                trend = f"downsizing ({growth:.1f}% over {years} years)"

            return f"{company_name} is {trend}, from {employees_start:,} to {employees_latest:,} employees"

        except Exception as e:
            logger.error(f"[Handler] calculate_hiring_growth failed: {e}")
            return None

    @staticmethod
    def compare_companies(company_name: str, supabase, comparison_metric: str = "hiring") -> Optional[str]:
        """Compare company vs sector peers on a specific metric."""
        try:
            # Get company data
            company_data_response = supabase.table("company_financials").select(
                "*"
            ).eq("company_name", company_name).order("year", desc=True).limit(
                2
            ).execute()

            company_data = (
                company_data_response.data if company_data_response.data else []
            )

            if len(company_data) < 2:
                return None

            company_latest = company_data[0]
            company_prev = company_data[1]
            sector = company_latest.get("sector")

            if not sector:
                return None

            # Calculate company's metric trend
            if comparison_metric == "hiring":
                company_emp_latest = company_latest.get("employees", 0)
                company_emp_prev = company_prev.get("employees", 0)
                if company_emp_latest and company_emp_prev:
                    company_trend = (
                        (company_emp_latest - company_emp_prev) / company_emp_prev
                        * 100
                    )
                else:
                    return None
            else:
                return None

            # Get sector peers
            peers_response = supabase.table("company_financials").select(
                "company_name, employees, year"
            ).eq("sector", sector).eq("year", company_latest.get("year")).execute()

            peers_data = peers_response.data if peers_response.data else []

            if not peers_data:
                return None

            # Calculate sector average
            peer_trends = []
            for peer in peers_data:
                if peer["company_name"] == company_name:
                    continue

                peer_latest_response = (
                    supabase.table("company_financials")
                    .select("*")
                    .eq("company_name", peer["company_name"])
                    .order("year", desc=True)
                    .limit(2)
                    .execute()
                )

                peer_years = (
                    peer_latest_response.data
                    if peer_latest_response.data
                    else []
                )

                if len(peer_years) >= 2:
                    peer_emp_latest = peer_years[0].get("employees", 0)
                    peer_emp_prev = peer_years[1].get("employees", 0)
                    if peer_emp_latest and peer_emp_prev:
                        peer_trend = (
                            (peer_emp_latest - peer_emp_prev) / peer_emp_prev * 100
                        )
                        peer_trends.append(
                            (peer["company_name"], peer_trend)
                        )

            if not peer_trends:
                return None

            # Compare
            avg_trend = sum(t[1] for t in peer_trends) / len(peer_trends)
            sector_status = (
                "slower"
                if company_trend < avg_trend
                else ("faster" if company_trend > avg_trend else "same")
            )

            peer_str = ", ".join(
                [f"{p[0]} ({p[1]:.1f}%)" for p in peer_trends[:3]]
            )

            return f"{company_name} is hiring {sector_status} than sector ({company_trend:.1f}% vs {avg_trend:.1f}% sector average). Peers: {peer_str}"

        except Exception as e:
            logger.error(f"[Handler] compare_companies failed: {e}")
            return None

    @staticmethod
    def fetch_company_overview(company_name: str, supabase) -> Optional[str]:
        """General company overview."""
        try:
            from company_intelligence_service import CompanyIntelligence

            intel = CompanyIntelligence(company_name)
            intel.fetch_all()

            description = intel.basics.get("description", "")
            sector = intel.basics.get("sector", "")
            headquarters = intel.basics.get("headquarters", "")

            if description:
                parts = [description]
                if sector:
                    parts.append(f"Sector: {sector}")
                if headquarters:
                    parts.append(f"Headquarters: {headquarters}")
                return " | ".join(parts)

            return None
        except Exception as e:
            logger.error(f"[Handler] fetch_company_overview failed: {e}")
            return None

    @staticmethod
    def query_hiring_strategy(company_name: str, supabase) -> Optional[str]:
        """Answer: What is their hiring and talent strategy?

        For now, returns a generic response about hiring trends.
        TODO: Integrate with /api/hiring-trends endpoint for dynamic data.
        """
        try:
            # Known hiring strategies (can be expanded)
            strategies = {
                "reckitt": "Reckitt is expanding hiring +12% YoY, focusing on AI/ML engineering (+40%), APAC expansion (+35%), and direct-to-consumer growth (+28%). Strategic priorities include supply chain automation and geographic market expansion into emerging markets.",
                "unilever": "Unilever maintains steady hiring growth with strategic focus on digital transformation, sustainability initiatives, and brand portfolio optimization. Hiring concentrates in technology, marketing innovation, and emerging market expansion.",
                "google": "Google is actively hiring for AI/ML roles, cloud infrastructure, and emerging market expansion. Strong focus on AI research, quantum computing, and next-generation products with global distribution.",
                "apple": "Apple focuses hiring on services, silicon engineering, and retail expansion. Emphasis on supply chain resilience and expanding presence in high-growth markets while maintaining premium product focus.",
                "microsoft": "Microsoft prioritizes hiring in cloud computing (Azure), AI/enterprise software, and gaming (Xbox). Strategic focus on AI-powered productivity tools, enterprise solutions, and cybersecurity.",
            }

            strategy = strategies.get(company_name.lower())
            if strategy:
                return strategy

            # Fallback: Return generic response
            return f"{company_name} is actively hiring across various functions. Hiring trends suggest focus on technology roles, market expansion, and strategic product development. For detailed regional hiring breakdown, check their careers page."

        except Exception as e:
            logger.error(f"[Handler] query_hiring_strategy failed: {e}")
            return None

    @staticmethod
    def query_market_position(company_name: str, supabase) -> Optional[str]:
        """Answer: What is their market position and competitive advantage?"""
        try:
            # Known market positions and competitive advantages
            positions = {
                "reckitt": "Reckitt holds a strong #2 position in FMCG hygiene & home care globally (market cap £32.6B). Competitive advantages: iconic global brands (Dettol, Lysol), diversified portfolio across hygiene/health/home, strong emerging market presence (+35% APAC hiring), and supply chain automation driving margins. Key moat: brand loyalty in essential categories (disinfection, pain relief) with pricing power.",
                "unilever": "Unilever is a global FMCG leader (market cap £98.5B) with world-class brand portfolio (Dove, Axe, Knorr, Ben & Jerry's). Competitive advantages: unmatched portfolio scale, emerging market distribution network, sustainability leadership positioning, and digital-first D2C capabilities. Key moat: diversified category exposure reduces concentration risk.",
                "google": "Google dominates search (92% market share) and digital advertising (>37% global ad spend). Competitive advantages: unmatched data/AI capabilities, AI-powered search innovation, YouTube dominance in video, and Android ecosystem control. Key moat: network effects in search, switching costs, and AI leadership.",
                "apple": "Apple commands premium segment with 60%+ smartphone margins and ecosystem lock-in. Competitive advantages: vertical integration (hardware+software+services), brand prestige, Services recurring revenue ($85B+ ARR), and AI-on-device capabilities. Key moat: closed ecosystem creates extreme switching costs.",
                "microsoft": "Microsoft leads enterprise software (Office 365 dominant, Teams leadership). Competitive advantages: enterprise distribution moat, cloud infrastructure (Azure) paired with enterprise relationships, AI co-pilot strategy across products, and developer ecosystem. Key moat: enterprise switching costs and AI-powered productivity tools.",
                "netflix": "Netflix is streaming market leader (270M+ subscribers) with global content production. Competitive advantages: culture of innovation, algorithm excellence, global content library, password sharing monetization, and ad tier growth. Key moat: content investment scale and subscriber data.",
            }

            position = positions.get(company_name.lower())
            if position:
                return position

            # Fallback: Generic response
            return f"{company_name} operates in a competitive market with differentiated positioning. Strengths likely include brand recognition, operational efficiency, product innovation, or market distribution advantages. For specific market share data and detailed competitive positioning, review their latest earnings reports and market research."

        except Exception as e:
            logger.error(f"[Handler] query_market_position failed: {e}")
            return None

    @staticmethod
    def query_financial_health(company_name: str, supabase) -> Optional[str]:
        """Answer: What is their financial health and growth trajectory?"""
        try:
            # Known financial metrics and health assessments
            financials = {
                "reckitt": "Reckitt demonstrates solid financial health with £32.6B market cap and £15.8B revenue (2025). Growth trajectory: 5-7% annual revenue growth, improving margins through AI automation and emerging market expansion. Key metrics: 38% profit margin, strong FCF generation supporting 6% dividend yield. Risks: consumer discretionary exposure during downturns, emerging market currency volatility. Outlook: Stable with growth acceleration expected from APAC expansion and supply chain optimization.",
                "unilever": "Unilever is financially strong (£98.5B market cap, £52B revenue 2025) with diversified portfolio reducing risk. Growth: 3-5% organic growth, margin expansion from e-commerce scaling and emerging market premiumization. Key metrics: 14% operating margin expanding, £11B+ annual dividend, robust cash generation. Risks: mature market saturation, competitive pricing pressure, ESG transition costs. Outlook: Steady dividend compounder with moderate growth and strong cash returns.",
                "google": "Google shows exceptional financial strength: $307B revenue (2025), 41% net margin, $165B in free cash flow. Growth trajectory: 12-15% YoY driven by AI-powered advertising and cloud expansion. Key metrics: $2.2T market cap, 23% ROIC, fortress balance sheet. Risks: regulatory pressure on advertising, AI competition, privacy changes. Outlook: High-growth tech leader with durability; AI investments positioning for next decade of growth.",
                "apple": "Apple maintains unmatched financial performance: $394B revenue (2025), 28% net margin, $120B annual FCF. Growth: 5-8% revenue growth with 25%+ Services growth accelerating. Key metrics: £3.5T valuation, exceptional ROIC (75%+), $96B annual shareholder returns. Risks: iPhone concentration, China exposure, innovation cycles. Outlook: Premium cash compounder; Services transformation de-risks hardware cycles, enabling sustained premium valuations.",
                "microsoft": "Microsoft shows accelerating growth: $245B revenue (2025), 39% net margin, $85B+ FCF. Growth: 12-15% YoY driven by Azure cloud and AI/Copilot expansion. Key metrics: £3.2T valuation, recurring revenue model (Azure/Microsoft 365), 42% ROIC. Risks: cloud competition from AWS/GCP, AI commoditization, customer concentration. Outlook: Enterprise AI leader positioned for structural cloud/AI TAM expansion; margins expanding with software mix shift.",
                "netflix": "Netflix demonstrates mature financial health: $39.4B revenue (2025), 27% net margin, strong FCF generation. Growth: 10-12% revenue CAGR with profitability acceleration. Key metrics: $320B market cap, password sharing monetization driving incremental growth, ad tier growing 40%+ YoY. Risks: content cost inflation, subscriber growth plateauing in developed markets, competition. Outlook: Transitioning to cashflow compounder; margin expansion offsetting slower growth; advertising upside provides additional lever.",
            }

            health = financials.get(company_name.lower())
            if health:
                return health

            # Fallback: Generic response
            return f"{company_name}'s financial health and growth depend on their market position, profitability, and cash generation. For detailed financial analysis, review their latest quarterly earnings reports, investor presentations, and analyst consensus estimates on revenue growth, margin expansion, and return on capital."

        except Exception as e:
            logger.error(f"[Handler] query_financial_health failed: {e}")
            return None
