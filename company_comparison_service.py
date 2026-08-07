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

    def compare(self, company1: str, company2: str) -> dict:
        """
        Compare two companies across financials, market position, and signals.
        """
        try:
            # Fetch both companies
            intel1 = CompanyIntelligence(company1)
            intel2 = CompanyIntelligence(company2)

            data1 = intel1.fetch_all()
            data2 = intel2.fetch_all()

            # Fetch financial history
            financials1 = self._get_latest_financials(company1)
            financials2 = self._get_latest_financials(company2)

            # Build comparison
            return {
                "company1": {
                    "name": company1,
                    "description": data1.get("description"),
                    "sector": data1.get("sector"),
                    "headquarters": data1.get("headquarters"),
                    "brands": data1.get("brands", [])[:5],
                    "stock": data1.get("stock", {}),
                    "financials": financials1,
                },
                "company2": {
                    "name": company2,
                    "description": data2.get("description"),
                    "sector": data2.get("sector"),
                    "headquarters": data2.get("headquarters"),
                    "brands": data2.get("brands", [])[:5],
                    "stock": data2.get("stock", {}),
                    "financials": financials2,
                },
                "comparison": self._calculate_diffs(financials1, financials2),
            }

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


# Global instance
comparison_service = CompanyComparisonService()
