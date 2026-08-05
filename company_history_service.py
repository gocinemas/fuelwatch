"""
Company financial history and trend tracking.
Stores and analyzes historical metrics for trend analysis.
"""

import logging
from datetime import datetime, timedelta
from typing import dict, list
import json

logger = logging.getLogger(__name__)


class CompanyHistoryTracker:
    """Track historical financials and market trends."""

    def __init__(self):
        self.db = None  # Will be injected from Flask app

    def set_db(self, supabase_client):
        """Inject Supabase client."""
        self.db = supabase_client

    def add_financials(self, company_name: str, period: str, data: dict) -> bool:
        """
        Store historical financial data.

        Args:
            company_name: Company name
            period: "Q1 2026", "2025", etc.
            data: {
                'revenue_millions': 2500,
                'gross_margin_pct': 48.5,
                'operating_margin_pct': 22.3,
                'employees': 12000,
                'revenue_growth_pct': 8.5,
                'source': 'manual'
            }
        """
        try:
            if not self.db:
                logger.warning("Database not initialized")
                return False

            payload = {
                "company_name": company_name.lower(),
                "period": period,
                "revenue_millions": data.get('revenue_millions'),
                "gross_margin_pct": data.get('gross_margin_pct'),
                "operating_margin_pct": data.get('operating_margin_pct'),
                "net_margin_pct": data.get('net_margin_pct'),
                "employees": data.get('employees'),
                "revenue_growth_pct": data.get('revenue_growth_pct'),
                "source": data.get('source', 'manual'),
            }

            result = self.db.table('company_financials').insert(payload).execute()
            logger.info(f"[history] Added financials for {company_name} {period}")
            return True
        except Exception as e:
            logger.error(f"[history] Failed to add financials: {e}")
            return False

    def get_financial_history(self, company_name: str, periods: int = 8) -> list:
        """
        Get historical financials for a company (last N periods).
        Returns sorted by period descending (most recent first).
        """
        try:
            if not self.db:
                return []

            result = self.db.table('company_financials').select(
                'period, revenue_millions, gross_margin_pct, operating_margin_pct, employees, revenue_growth_pct'
            ).eq('company_name', company_name.lower()).order('period', desc=True).limit(periods).execute()

            return result.data if result.data else []
        except Exception as e:
            logger.error(f"[history] Failed to fetch financials: {e}")
            return []

    def add_deal(self, company_name: str, deal_data: dict) -> bool:
        """
        Store M&A/deal activity.

        Args:
            company_name: Company name
            deal_data: {
                'deal_type': 'acquisition',  # 'acquisition', 'acquired_by', 'investment'
                'target_company': 'AcmeCorp',
                'amount_millions': 500,
                'announcement_date': '2026-01-15',
                'description': 'Strategic acquisition in health tech'
            }
        """
        try:
            if not self.db:
                logger.warning("Database not initialized")
                return False

            payload = {
                "company_name": company_name.lower(),
                "deal_type": deal_data.get('deal_type'),
                "target_company": deal_data.get('target_company'),
                "investor_company": deal_data.get('investor_company'),
                "amount_millions": deal_data.get('amount_millions'),
                "announcement_date": deal_data.get('announcement_date'),
                "completion_date": deal_data.get('completion_date'),
                "description": deal_data.get('description'),
                "source": deal_data.get('source', 'manual'),
            }

            result = self.db.table('company_deals').insert(payload).execute()
            logger.info(f"[deals] Added deal for {company_name}")
            return True
        except Exception as e:
            logger.error(f"[deals] Failed to add deal: {e}")
            return False

    def get_deals(self, company_name: str, limit: int = 10) -> list:
        """Get recent M&A/deal activity."""
        try:
            if not self.db:
                return []

            result = self.db.table('company_deals').select(
                'deal_type, target_company, investor_company, amount_millions, announcement_date, description'
            ).eq('company_name', company_name.lower()).order('announcement_date', desc=True).limit(limit).execute()

            return result.data if result.data else []
        except Exception as e:
            logger.error(f"[deals] Failed to fetch deals: {e}")
            return []

    def add_market_trend(self, company_name: str, category: str, trend_data: dict) -> bool:
        """
        Store market trends (market share, TAM, growth).

        Args:
            company_name: Company name
            category: 'market_share', 'tam', 'category_growth', 'regional_growth'
            trend_data: {
                'metric_name': 'disinfectant_market_share',
                'value_pct': 42.5,
                'period': 'Q1 2026',
                'source': 'manual'
            }
        """
        try:
            if not self.db:
                logger.warning("Database not initialized")
                return False

            payload = {
                "company_name": company_name.lower(),
                "category": category,
                "metric_name": trend_data.get('metric_name'),
                "value_pct": trend_data.get('value_pct'),
                "period": trend_data.get('period'),
                "source": trend_data.get('source', 'manual'),
            }

            result = self.db.table('company_market_trends').insert(payload).execute()
            logger.info(f"[trends] Added trend for {company_name}: {category}")
            return True
        except Exception as e:
            logger.error(f"[trends] Failed to add trend: {e}")
            return False

    def get_market_trends(self, company_name: str, category: str = None) -> list:
        """Get market trends (market share, TAM, growth)."""
        try:
            if not self.db:
                return []

            query = self.db.table('company_market_trends').select(
                'category, metric_name, value_pct, period'
            ).eq('company_name', company_name.lower())

            if category:
                query = query.eq('category', category)

            result = query.order('period', desc=True).limit(20).execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"[trends] Failed to fetch trends: {e}")
            return []

    def calculate_growth_rates(self, company_name: str) -> dict:
        """Calculate YoY revenue growth, employee growth, margin trends."""
        try:
            history = self.get_financial_history(company_name, periods=8)
            if len(history) < 2:
                return {}

            latest = history[0]
            prior_year = next((h for h in history if h['period'].split()[-1] == str(int(latest['period'].split()[-1]) - 1)), None)

            if not prior_year:
                return {}

            return {
                'revenue_growth_yoy': self._calc_growth(prior_year.get('revenue_millions'), latest.get('revenue_millions')),
                'employee_growth_yoy': self._calc_growth(prior_year.get('employees'), latest.get('employees')),
                'margin_trend': latest.get('operating_margin_pct', 0) - prior_year.get('operating_margin_pct', 0),
                'latest_revenue': latest.get('revenue_millions'),
                'latest_employees': latest.get('employees'),
            }
        except Exception as e:
            logger.error(f"[trends] Failed to calculate growth: {e}")
            return {}

    @staticmethod
    def _calc_growth(prior, latest):
        """Calculate percentage growth."""
        if not prior or prior == 0:
            return 0
        return round(((latest - prior) / prior) * 100, 1)


# Initialize global instance
history_tracker = CompanyHistoryTracker()
