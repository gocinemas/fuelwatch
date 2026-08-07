"""
Company Comparison Service
Side-by-side comparison of two companies across key metrics.
"""

import logging
import os
from company_intelligence_service import CompanyIntelligence

logger = logging.getLogger(__name__)


class CompanyComparisonService:
    """Compare two companies side-by-side."""

    def __init__(self):
        self.db = None

    def set_db(self, supabase_client):
        """Set Supabase client."""
        self.db = supabase_client

    def compare(self, *companies) -> dict:
        """
        Compare 2-4 companies across financials, market position, and signals.
        """
        try:
            if len(companies) < 2 or len(companies) > 4:
                return {"error": "Provide 2-4 companies to compare"}

            # Fetch all companies
            company_data = {}
            for company_name in companies:
                intel = CompanyIntelligence(company_name)
                data = intel.fetch_all()
                financials = self._get_latest_financials(company_name)

                company_data[company_name.lower()] = {
                    "name": company_name,
                    "description": data.get("description"),
                    "sector": data.get("sector"),
                    "headquarters": data.get("headquarters"),
                    "brands": data.get("brands", [])[:5],
                    "stock": data.get("stock", {}),
                    "financials": financials,
                }

            # Build response
            result = {
                "companies": company_data,
                "comparison": self._calculate_multi_diffs(company_data),
            }

            return result

        except Exception as e:
            logger.error(f"[compare] Error: {e}")
            return {"error": str(e)}

    def _get_latest_financials(self, company_name: str) -> dict:
        """Get latest financial data from Supabase."""
        try:
            if not self.db:
                logger.warning(f"[compare] DB not initialized")
                return {}

            # Query company_financials table
            result = self.db.table("company_financials").select("*").eq(
                "company_name", company_name.lower()
            ).order("period", desc=True).limit(1).execute()

            if result.data and len(result.data) > 0:
                row = result.data[0]
                return {
                    "period": row.get("period"),
                    "revenue_millions": row.get("revenue_millions"),
                    "gross_margin_pct": row.get("gross_margin_pct"),
                    "operating_margin_pct": row.get("operating_margin_pct"),
                    "employees": row.get("employees"),
                    "revenue_growth_pct": row.get("revenue_growth_pct"),
                }
            return {}
        except Exception as e:
            logger.warning(f"[compare] Financial lookup failed for {company_name}: {e}")
            return {}

    def _calculate_diffs(self, fin1: dict, fin2: dict) -> dict:
        """Calculate differences between two companies."""
        if not fin1 or not fin2:
            return {}

        rev1 = fin1.get("revenue_millions", 0)
        rev2 = fin2.get("revenue_millions", 0)
        growth1 = fin1.get("revenue_growth_pct", 0)
        growth2 = fin2.get("revenue_growth_pct", 0)
        margin1 = fin1.get("operating_margin_pct", 0)
        margin2 = fin2.get("operating_margin_pct", 0)
        emp1 = fin1.get("employees", 0)
        emp2 = fin2.get("employees", 0)

        return {
            "revenue_diff_millions": rev1 - rev2,
            "revenue_diff_pct": round((rev1 - rev2) / rev2 * 100, 1) if rev2 else 0,
            "growth_diff_pct": round(growth1 - growth2, 1),
            "margin_diff_pct": round(margin1 - margin2, 1),
            "employee_diff": emp1 - emp2,
            "larger_by_revenue": "company1" if rev1 > rev2 else "company2",
            "faster_growth": "company1" if growth1 > growth2 else "company2",
            "higher_margin": "company1" if margin1 > margin2 else "company2",
        }

    def _calculate_multi_diffs(self, company_data: dict) -> dict:
        """Calculate rankings for 2-4 companies."""
        companies = list(company_data.items())
        metrics = {}

        # Extract metrics for ranking
        fin_data = [comp[1].get("financials", {}) for comp in companies]

        # Revenue ranking
        revenues = [(i, comp[1].get("financials", {}).get("revenue_millions", 0)) for i, comp in enumerate(companies)]
        revenues.sort(key=lambda x: x[1], reverse=True)
        metrics["largest_by_revenue"] = companies[revenues[0][0]][1]["name"]
        metrics["revenues"] = {companies[i][1]["name"]: rev for i, rev in revenues}

        # Growth ranking
        growths = [(i, comp[1].get("financials", {}).get("revenue_growth_pct", 0)) for i, comp in enumerate(companies)]
        growths.sort(key=lambda x: x[1], reverse=True)
        metrics["fastest_growth"] = companies[growths[0][0]][1]["name"]

        # Margin ranking
        margins = [(i, comp[1].get("financials", {}).get("operating_margin_pct", 0)) for i, comp in enumerate(companies)]
        margins.sort(key=lambda x: x[1], reverse=True)
        metrics["highest_margin"] = companies[margins[0][0]][1]["name"]

        # Employees ranking
        emps = [(i, comp[1].get("financials", {}).get("employees", 0)) for i, comp in enumerate(companies)]
        emps.sort(key=lambda x: x[1], reverse=True)
        metrics["largest_by_employees"] = companies[emps[0][0]][1]["name"]

        return metrics


# Global instance
comparison_service = CompanyComparisonService()
