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
