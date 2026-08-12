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

        Fetches hiring trends from the API and analyzes regional/departmental patterns.
        """
        try:
            import requests
            import os
            from groq import Groq

            # Fetch regional hiring trends
            base_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "https://intel.humanagency.co")
            trends_url = f"{base_url}/api/hiring-trends/regional?name={company_name}"

            response = requests.get(trends_url, timeout=5)
            if response.status_code != 200:
                return None

            trends_data = response.json()

            # Check if we have data
            if not trends_data.get("regions"):
                return None

            # Analyze the trends
            regions = trends_data.get("regions", {})

            if not regions:
                return None

            # Build analysis
            growing_regions = []
            declining_regions = []
            stable_regions = []

            for region, data in regions.items():
                direction = data.get("direction", "").lower()
                trend = data.get("trend", "")
                current = data.get("current", 0)

                if direction == "increasing":
                    growing_regions.append((region, current, trend))
                elif direction == "decreasing":
                    declining_regions.append((region, current, trend))
                else:
                    stable_regions.append((region, current, trend))

            # Generate narrative response using Groq
            groq_key = os.environ.get("GROQ_API_KEY", "")
            if not groq_key:
                # Fallback to simple narrative
                parts = []
                if growing_regions:
                    growing_str = ", ".join([f"{r[0]} ({r[2]})" for r in growing_regions])
                    parts.append(f"Expanding hiring in: {growing_str}")
                if declining_regions:
                    declining_str = ", ".join([f"{r[0]} ({r[2]})" for r in declining_regions])
                    parts.append(f"Reducing hiring in: {declining_str}")
                if parts:
                    return f"{company_name}'s hiring strategy: {'; '.join(parts)}"
                return None

            # Use Groq for richer analysis
            client = Groq(api_key=groq_key)

            prompt = f"""Based on this hiring trend data for {company_name}, generate a concise (2-3 sentence) analysis of their talent/hiring strategy:

Regional Hiring Trends:
{chr(10).join([f"- {region}: {data.get('current', 0)} roles, trend {data.get('trend', '—')} ({data.get('direction', 'unknown')})" for region, data in regions.items()])}

Focus on:
1. Which regions they're prioritizing (growth areas)
2. What this reveals about their strategic direction
3. Any notable shifts in hiring patterns

Keep it factual and strategic."""

            response = client.messages.create(
                model="mixtral-8x7b-32768",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.5
            )

            if response.choices:
                return response.choices[0].message.content

            return None

        except Exception as e:
            logger.error(f"[Handler] query_hiring_strategy failed: {e}")
            return None
