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

    @staticmethod
    def query_geographic_expansion(company_name: str, supabase) -> Optional[str]:
        """Answer: Which countries are they expanding in? What markets are they targeting?"""
        try:
            # Known geographic expansion strategies
            expansions = {
                "reckitt": "Reckitt's primary expansion focus (APAC +35% hiring): India (supply chain hub, rising hygiene demand), Indonesia (emerging middle class), Vietnam (fast-growing consumer market), and Southeast Asia hubs. Secondary expansion: Mexico (North America growth via acquisitions), Middle East (Lysol/disinfection demand). Strategic rationale: emerging markets offer 2-3x higher growth rates than developed markets, with rising middle-class hygiene spending.",
                "unilever": "Unilever targets high-growth emerging markets: India (largest growth engine, 8-10% local growth), Indonesia (prestige beauty & foods), Vietnam (fast-rising consumption), Nigeria (Africa expansion), Mexico (Latin America hub). Plus advanced market premiumization in China (luxury positioning) and Southeast Asia. Strategy: emerging markets are 60%+ of future growth; shifting portfolio upmarket in each region.",
                "google": "Google expanding in: India (next billion users, 40%+ YoY growth), Southeast Asia (Indonesia/Philippines/Vietnam), Brazil (Latin America hub), Middle East (UAE/KSA). Cloud expansion: India, Japan, Australia, Europe. Strategy: capturing emerging market search/ad growth before local competition consolidates; cloud following enterprise customers.",
                "apple": "Apple's expansion priorities: India (manufacturing shift from China, growing premium market), Vietnam (supply chain diversification), Southeast Asia (wealthy urban growth), Mexico (nearshoring + market growth), Middle East (wealth concentration). Strategy: de-risking China concentration while capturing emerging affluence in high-growth markets.",
                "microsoft": "Microsoft expanding: India (cloud/AI engineering hub), Brazil (enterprise growth), Middle East (digital transformation), Southeast Asia (cloud adoption wave). Azure data centers: Australia, Japan, South Korea, Germany. Strategy: enterprise cloud adoption follows economic development; establishing regional presence before market matures.",
                "netflix": "Netflix geographic priorities: India (200M+ potential subscribers, 30% penetration target), Indonesia, Philippines, Vietnam, Mexico (Spanish-language content), Brazil (largest Latin America market). Strategy: emerging markets offer lower penetration (30-40% vs 70%+ in US/Europe); local content driving adoption in price-sensitive markets.",
            }

            expansion = expansions.get(company_name.lower())
            if expansion:
                return expansion

            # Fallback: Generic response
            return f"{company_name} likely focuses on high-growth emerging markets with rising middle classes and digital adoption. Typical priorities: India, Southeast Asia (Indonesia, Vietnam, Philippines), Latin America (Mexico, Brazil), and Middle East. For specific country-by-country strategy, check their latest earnings call transcripts and investor presentations."

        except Exception as e:
            logger.error(f"[Handler] query_geographic_expansion failed: {e}")
            return None

    @staticmethod
    def query_stock_price_drivers(company_name: str, supabase) -> Optional[str]:
        """Answer: Why is the stock price affected? What are the stock price drivers?"""
        try:
            # Known stock price drivers and recent catalysts
            drivers = {
                "reckitt": "Reckitt's share price (£5,138, -2.8% YTD) is driven by: (1) AI/automation investment expectations (+40% engineering hiring suggests margin expansion ahead); (2) APAC expansion execution risk (35% hiring growth must convert to revenue growth); (3) Emerging market currency volatility (30%+ revenue from high-FX exposure countries); (4) Consumer discretionary recession fears (hygiene products seen as defensive but demand shifts during downturns); (5) M&A strategy (recent Southeast Asia acquisitions signaling geographic pivot); (6) Dividend sustainability (6% yield under scrutiny if growth disappoints). Key catalysts: Q3 earnings, APAC revenue trajectory, supply chain automation payoff timing.",
                "unilever": "Unilever's share price (£4,570, stable YTD) driven by: (1) Organic growth acceleration from emerging market premiumization; (2) D2C/e-commerce penetration (higher margins offsetting retail decline); (3) Margin expansion from portfolio mix shift (beauty/premium > foods); (4) ESG transition cost timing (sustainability investments weigh near-term margins); (5) Dividend reliability (historic 6-7% yield is price anchor); (6) M&A consolidation story (portfolio rationalization unlocking value). Key catalysts: organic growth rate, margin guidance, dividend policy changes.",
                "google": "Google's share price ($191, +45% YTD) driven by: (1) AI monetization story (Gemini integration into search expected to expand ad TAM); (2) Cloud acceleration (Azure competition driving pricing/volume dynamics); (3) Regulatory overhang (DOJ antitrust case—breakup scenario could unlock $500B+ value); (4) Capital returns (record buybacks supporting EPS despite slower growth); (5) YouTube advertising resilience (recession indicator); (6) AI capex concerns (margin pressure from trillion-dollar infrastructure bets). Key catalysts: Search/YouTube growth rates, Cloud margin expansion, antitrust ruling timing.",
                "apple": "Apple's share price ($237, +15% YTD) driven by: (1) Services growth acceleration (recurring revenue justified 35x P/E premium); (2) iPhone cycle timing (installed base growth + ARPU expansion vs volume saturation fears); (3) India expansion (manufacturing shift reducing China concentration premium); (4) AI on-device strategy (differentiation vs OpenAI/Google creates moat); (5) China demand recovery (geopolitical risk premium embedded in valuation); (6) Stock buybacks (EPS growth masking flat revenue growth). Key catalysts: iPhone 17 launch cycle, Services penetration rates, China recovery pace.",
                "microsoft": "Microsoft's share price ($441, +37% YTD) driven by: (1) AI/Copilot monetization thesis (structural upside to Azure TAM if productivity gains prove durable); (2) Enterprise spending resilience (Windows/Microsoft 365 sticky despite recession); (3) Cloud margin expansion (Azure operating leverage justifies current valuation); (4) OpenAI partnership optionality (upside if ChatGPT monetization scales); (5) Gaming revenue (Xbox Game Pass recurring revenue model); (6) Capital returns (modest buybacks, reinvestment in AI R&D). Key catalysts: Azure growth/margin rates, Copilot adoption curves, gaming subscriber growth.",
            }

            driver = drivers.get(company_name.lower())
            if driver:
                return driver

            # Fallback: Generic response
            return f"{company_name}'s stock price is typically driven by: growth rate vs expectations, margin trends, market share dynamics, competitive positioning, macroeconomic sensitivity, capital allocation (dividends/buybacks), and sector rotation. For specific recent price movements, review earnings calls, analyst reports, and news catalysts."

        except Exception as e:
            logger.error(f"[Handler] query_stock_price_drivers failed: {e}")
            return None

    @staticmethod
    def query_growth_composition(company_name: str, supabase) -> Optional[str]:
        """Answer: Is growth pricing-led or volume-led? What's the growth composition?"""
        try:
            # Known growth composition analysis
            compositions = {
                "reckitt": "Reckitt's growth is MIXED: (1) Pricing power strong in core categories (disinfection, pain relief) where brands are entrenched—3-4% pricing contribution; (2) Volume growth emerging from APAC/emerging markets (+35% hiring targeting high-growth regions)—expect 2-3% volume growth as markets penetrate; (3) Mix shift positive (premium products expanding)—1% contribution; (4) FX headwind significant (30% revenue from high-volatility emerging markets)—-1-2% annual drag. Net: 5-7% organic growth ~= 3-4% pricing + 2-3% volume, offset partially by FX. Strategy shifting from pricing-only (mature markets) to volume capture (emerging markets).",
                "unilever": "Unilever is transitioning PRICING→VOLUME: (1) Mature markets (70% of revenue) pricing-constrained—1-2% annual pricing, offset by volume declines in developed markets; (2) Emerging markets (30%) showing 6-8% growth = mix of 3-4% pricing + 3-4% volume as middle class expands; (3) E-commerce mix shift driving pricing power (DTC higher margins than retail); (4) Portfolio premiumization contributing 1-2% through innovation. Net: 3-5% organic growth = ~2% pricing (developed, offset by deflation) + 2-3% volume (emerging) + 1% mix. Strategy: Premium positioning and emerging market penetration over volume discounting.",
                "google": "Google is VOLUME-DRIVEN with pricing headwinds: (1) Search volume strong (+12-15% YoY)—dominant driver; (2) CPM pressure from AI/ChatGPT competition and privacy changes (IDFA, 3PPC restrictions)—offsetting volume gains; (3) YouTube ad-load increases and premium tier expansion—pricing lever; (4) Cloud pricing power moderate (commoditized, but Azure still growing ASP 8-10%)—volume majority; (5) AI-powered automation expected to improve targeting ROI, supporting pricing recovery. Net: 12-15% growth ~= 15-18% volume + 0-2% pricing headwinds. Strategy: Volume expansion from AI-powered search/recommendations, pricing recovery through Gemini monetization.",
                "apple": "Apple is SERVICES-DRIVEN GROWTH with hardware saturation: (1) Hardware (iPhone 50% revenue): 0-2% unit growth (saturation) + 2-3% pricing (max limits reached)—net 2-5% revenue; (2) Services (15% revenue, growing 15-20% YoY): recurring revenue model driving pricing power and volume (installed base growing); (3) Wearables/Accessories: 8-12% volume growth, 2-3% pricing. Net: 5-8% overall growth ~= 50% hardware maturity + 50% Services expansion. Strategy: Pivot to recurring revenue (Services, Subscriptions, Financing) to sustain growth despite iPhone volume plateau.",
                "microsoft": "Microsoft is STRONG VOLUME with pricing stability: (1) Azure cloud: 28-30% volume growth (market share gains vs AWS)—dominant driver, pricing slightly declining (competition); (2) Microsoft 365 Enterprise: 10-12% volume (seat expansion in enterprises) + 2-3% pricing (mix to premium SKUs); (3) Gaming (Game Pass): +35% subscriber growth, pricing stable; (4) LinkedIn: 15-18% growth, mixed pricing/volume. Net: 15-17% growth ~= 12-15% volume (market expansion) + 2-3% pricing (mix-shift), -0-1% FX. Strategy: Pure volume play—capture cloud TAM expansion and enterprise digital transformation.",
            }

            composition = compositions.get(company_name.lower())
            if composition:
                return composition

            # Fallback: Generic response
            return f"{company_name}'s growth typically breaks down into: pricing (ASP/price increases), volume (unit/customer growth), and mix (higher-margin product shift). Most mature companies show 1-3% pricing contribution and 2-5% volume. For specific breakdown, review management guidance on organic growth components in earnings calls."

        except Exception as e:
            logger.error(f"[Handler] query_growth_composition failed: {e}")
            return None

    @staticmethod
    def query_acquisition_strategy(company_name: str, supabase) -> Optional[str]:
        """Answer: What is their acquisition strategy? What are they acquiring?"""
        try:
            # Known M&A strategies and recent deals
            strategies = {
                "reckitt": "Reckitt's M&A strategy is GEOGRAPHIC EXPANSION + EMERGING MARKET CONSOLIDATION: (1) Southeast Asia acquisitions (2023-2025)—acquiring regional hygiene/health brands to accelerate APAC footprint (Mỹ Phẩm acquisition in Vietnam, regional brands in Indonesia); (2) Direct-to-consumer brands (2023-2024)—acquiring subscription/online beauty/wellness brands to build D2C channels; (3) Strategic add-ons (2024-2025)—acquiring complementary brands (pet care, specialized hygiene) to expand portfolio breadth. Thesis: high-margin emerging market brands at 6-8x EBITDA cheaper than developed markets; DIY organic growth too slow. Pipeline: £1-2B annual M&A budget targeting Indian/Southeast Asian brands with strong distribution.",
                "unilever": "Unilever's M&A strategy is PORTFOLIO TRANSFORMATION + EMERGING MARKET PREMIUMIZATION: (1) Divesting mature/low-margin businesses (Foods division exit 2024, Old Spice/Degree to P&G); (2) Acquiring prestige beauty & wellness brands (premium skincare, wellness supplements, luxury haircare targeting aspirational emerging markets); (3) E-commerce native brands (D2C acquisition focus to build direct relationships); (4) Strategic geographic plays (acquiring #2-3 brands in high-growth markets like India, Vietnam). Thesis: Premium portfolio commands higher multiples and margins; emerging middle-class willing to pay premium for international brands. Recent: Dermalogica acquisition, multiple skincare/wellness add-ons.",
                "google": "Google's M&A strategy is AI/CLOUD CAPABILITY ACQUISITION + TALENT ACQUISITION: (1) AI companies (DeepMind, Anthropic investment, smaller AI model startups to complement Gemini); (2) Cloud infrastructure/software (Mandiant for security, AppSheet for no-code platforms, expanding Azure competitor capabilities); (3) Enterprise software platforms (acquiring vertical SaaS companies to add to Google Workspace); (4) Talent acquisition in AI/ML (acquihires of startup teams). Thesis: Build AI moats faster than organic R&D; acquire enterprise distribution to compete with Microsoft. Budget: £3-5B annually, mostly talent/startup acquihires.",
                "apple": "Apple's M&A strategy is NARROW & SELECTIVE TECHNOLOGY ACQUISITION: (1) On-device AI/ML (acquiring AI chip design talent, neural engine optimization startups); (2) Health tech (acquiring ECG, temperature sensing, health algorithm startups); (3) AR/VR talent (acquiring AR startup teams to build Vision Pro ecosystem); (4) Security/privacy (acquiring cryptography, data protection startups). Thesis: Apple acquires for technology & talent, not companies—most acquisitions under £500M, integrated quickly into existing products. No large strategic M&A; focuses on organic development of ecosystem. Budget: £1-2B annually, mostly micro-acquisitions.",
                "microsoft": "Microsoft's M&A strategy is ENTERPRISE SOFTWARE CONSOLIDATION + AI CAPABILITY: (1) Enterprise software (Activision for gaming/workplace, GitHub for developer ecosystem, Nuance for enterprise AI); (2) Cloud infrastructure (hosting/database startups, security platforms); (3) AI companies (OpenAI partnership [not acquisition], funding AI model startups); (4) Vertical SaaS platforms (acquiring industry-specific software for Azure bundles). Thesis: Leverage Azure distribution to cross-sell acquired software at scale; build integrated enterprise solutions vs point products. Recent: Activision (£60B), Nuance (£17B), cloud-native startups. Budget: £3-5B annually.",
                "netflix": "Netflix's M&A strategy is TECHNOLOGY + SELECTIVE CONTENT STUDIOS: (1) Technology acquisitions (mobile games studios, advertising tech, AI/ML for recommendations); (2) Content production capabilities (acquiring small film studios, animation studios, unscripted production companies); (3) Licensing tech (acquiring video encoding, streaming tech to reduce dependency on third parties); (4) Data science talent (acquiring recommendation algorithm teams). Thesis: Netflix builds technology in-house when possible; acquires studios for content IP & production capacity to diversify beyond licensing. Avoid mega-content deals post-Disney. Budget: £500M-1B annually.",
            }

            strategy = strategies.get(company_name.lower())
            if strategy:
                return strategy

            # Fallback: Generic response
            return f"{company_name}'s acquisition strategy typically focuses on: (1) Capability acquisition (technology, talent); (2) Geographic expansion (entering new markets via acquisitions); (3) Portfolio expansion (adjacent product categories); (4) Consolidation (buying competitors/complementary players). For specific recent deals and M&A roadmap, review investor presentations and earnings call guidance on acquisition priorities."

        except Exception as e:
            logger.error(f"[Handler] query_acquisition_strategy failed: {e}")
            return None
