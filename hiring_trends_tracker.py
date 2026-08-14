"""
Hiring Trends Tracker
Records daily hiring snapshots and detects regional hiring trends.
"""

import os
import json
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from supabase import create_client
from hiring_signals_fetcher import HiringSignalsFetcher

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


def _get_supabase():
    """Get Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Supabase credentials not configured")
        return None
    return create_client(SUPABASE_URL, SUPABASE_KEY)


class HiringTrendsTracker:
    """Track hiring trends over time by region and department."""

    def __init__(self):
        self.sb = _get_supabase()
        self.fetcher = HiringSignalsFetcher()

    def take_daily_snapshot(self, company_name: str) -> dict:
        """
        Take a daily snapshot of hiring data for a company.
        Stores in hiring_snapshots table.

        Returns:
            {
                "company_name": "Reckitt",
                "snapshot_date": "2026-08-12",
                "regions_snapshot": {
                    "Europe": 45,
                    "North America": 32,
                    "Asia": 18
                },
                "departments_snapshot": {
                    "ENGINEERING": 20,
                    "AI/ML": 8,
                    "SALES": 6
                },
                "total_openings": 95
            }
        """
        try:
            if not self.sb:
                logger.error(f"[snapshot] Supabase not configured")
                return {}

            # Fetch current hiring data
            hiring_data = self.fetcher.fetch_hiring_signals(company_name)

            if not hiring_data or not hiring_data.get("sample_roles"):
                logger.warning(f"[snapshot] No hiring data for {company_name}")
                return {}

            # Parse sample roles to extract regions and departments
            regions_snapshot = defaultdict(int)
            departments_snapshot = defaultdict(int)

            for role in hiring_data.get("sample_roles", []):
                region = self._extract_region(role.get("location", ""))
                dept = role.get("department", "Other")

                regions_snapshot[region] += 1
                departments_snapshot[dept] += 1

            # Also count from top_departments if available
            for dept_data in hiring_data.get("top_departments", []):
                dept = dept_data.get("department", "Other")
                count = dept_data.get("open_count", 0)
                departments_snapshot[dept] = count  # Use actual count

            total_openings = hiring_data.get("overview", {}).get("total_open_roles", 0)
            snapshot_date = datetime.utcnow().date().isoformat()

            # Store each region/department combo as a record
            records = []
            for region, count in regions_snapshot.items():
                records.append({
                    "company_name": company_name,
                    "snapshot_date": snapshot_date,
                    "region": region,
                    "department": None,
                    "open_roles": count
                })

            for dept, count in departments_snapshot.items():
                records.append({
                    "company_name": company_name,
                    "snapshot_date": snapshot_date,
                    "region": None,
                    "department": dept,
                    "open_roles": count
                })

            # Insert or update records
            if records:
                response = self.sb.table("hiring_snapshots").upsert(records).execute()
                logger.info(f"[snapshot] Stored {len(records)} hiring snapshots for {company_name}")

            return {
                "company_name": company_name,
                "snapshot_date": snapshot_date,
                "regions_snapshot": dict(regions_snapshot),
                "departments_snapshot": dict(departments_snapshot),
                "total_openings": total_openings
            }

        except Exception as e:
            logger.error(f"[snapshot] Error taking snapshot for {company_name}: {e}")
            return {}

    def get_hiring_trend(self, company_name: str, region: str = None, days: int = 30) -> dict:
        """
        Get hiring trend for a company over N days.

        Returns:
            {
                "company_name": "Reckitt",
                "region": "Europe",
                "current_openings": 45,
                "previous_openings": 38,
                "trend": "↑ +18%",
                "trend_direction": "increasing",
                "change_count": 7,
                "history": [
                    {"date": "2026-08-01", "openings": 38},
                    ...
                ]
            }
        """
        try:
            if not self.sb:
                logger.error("[trend] Supabase not configured")
                return {}

            cutoff_date = (datetime.utcnow().date() - timedelta(days=days)).isoformat()
            today = datetime.utcnow().date().isoformat()

            # Query snapshots
            query = self.sb.table("hiring_snapshots").select(
                "snapshot_date, open_roles"
            ).eq("company_name", company_name).gte(
                "snapshot_date", cutoff_date
            ).lte(
                "snapshot_date", today
            )

            if region:
                query = query.eq("region", region)
            else:
                query = query.is_("region", "is.null")  # Only region-level data

            response = query.order("snapshot_date").execute()
            snapshots = response.data or []

            if not snapshots:
                logger.warning(f"[trend] No trend data for {company_name} in {region}")
                return {
                    "company_name": company_name,
                    "region": region or "All",
                    "current_openings": 0,
                    "previous_openings": 0,
                    "trend": "No data",
                    "trend_direction": "unknown",
                    "history": []
                }

            # Group by date
            history = {}
            for snap in snapshots:
                date = snap.get("snapshot_date")
                count = snap.get("open_roles", 0)
                if date not in history:
                    history[date] = 0
                history[date] += count

            # Sort by date
            sorted_dates = sorted(history.keys())
            current_openings = history[sorted_dates[-1]] if sorted_dates else 0
            previous_openings = history[sorted_dates[0]] if sorted_dates else 0

            # Calculate trend
            change = current_openings - previous_openings
            change_pct = round((change / previous_openings * 100)) if previous_openings > 0 else 0

            if change > 0:
                trend = f"↑ +{change_pct}%"
                trend_direction = "increasing"
            elif change < 0:
                trend = f"↓ {change_pct}%"
                trend_direction = "decreasing"
            else:
                trend = "→ Stable"
                trend_direction = "stable"

            # Build history list
            history_list = [
                {"date": date, "openings": history[date]}
                for date in sorted_dates
            ]

            return {
                "company_name": company_name,
                "region": region or "All",
                "current_openings": current_openings,
                "previous_openings": previous_openings,
                "trend": trend,
                "trend_direction": trend_direction,
                "change_count": change,
                "period_days": days,
                "history": history_list
            }

        except Exception as e:
            logger.error(f"[trend] Error getting trend for {company_name}: {e}")
            return {}

    def calculate_retention_metrics(self, company_name: str) -> dict:
        """
        Calculate employee retention metrics.
        Uses employee count changes and hiring data to estimate retention.
        Provides sensible defaults when financial data is missing.

        Returns:
            {
                "retention_rate": 88,  # percentage
                "turnover_rate": 12,   # percentage
                "retention_trend": "stable",  # stable, improving, declining
                "based_on": "hiring_and_employee_data"
            }
        """
        try:
            if not self.sb:
                logger.error("[retention] Supabase not configured")
                return {
                    "retention_rate": 85,
                    "turnover_rate": 15,
                    "retention_trend": "stable",
                    "based_on": "industry_average"
                }

            # Get latest financial data for employee count
            fin_response = self.sb.table("company_financials").select(
                "period, employees"
            ).eq("company_name", company_name).order(
                "period", desc=True
            ).limit(3).execute()

            financials = fin_response.data or []

            # If we have at least 2 years of data, calculate actual retention
            if len(financials) >= 2:
                # Employee count change over periods
                latest_employees = financials[0].get("employees", 0)
                previous_employees = financials[1].get("employees", 0)
                employee_growth = ((latest_employees - previous_employees) / previous_employees * 100) if previous_employees > 0 else 0

                # Get hiring trend for same period
                hiring_trend = self.get_hiring_trend(company_name, days=365)
                hiring_growth = hiring_trend.get("trend_direction") == "increasing"

                # Estimate retention based on hiring vs employee growth
                if hiring_growth and employee_growth < 5:
                    # Hiring aggressively but employees not growing much = replacing people
                    retention_rate = max(70, 100 - abs(employee_growth) - 20)
                    retention_trend = "declining"
                elif not hiring_growth and employee_growth > 0:
                    # Minimal hiring, employees still growing = high retention
                    retention_rate = min(95, 85 + employee_growth)
                    retention_trend = "improving"
                else:
                    # Balanced scenario
                    retention_rate = 85
                    retention_trend = "stable"

                retention_rate = max(60, min(98, int(retention_rate)))  # Clamp 60-98%
                turnover_rate = 100 - retention_rate

                return {
                    "retention_rate": retention_rate,
                    "turnover_rate": turnover_rate,
                    "retention_trend": retention_trend,
                    "employee_growth_pct": round(employee_growth, 1),
                    "based_on": "financial_hiring_analysis"
                }
            else:
                # No financial data, use industry defaults
                logger.info(f"[retention] No financial data for {company_name}, using industry average")
                return {
                    "retention_rate": 85,  # Industry average for tech/consumer companies
                    "turnover_rate": 15,
                    "retention_trend": "stable",
                    "based_on": "industry_average"
                }

        except Exception as e:
            logger.error(f"[retention] Error calculating retention for {company_name}: {e}")
            # Return industry average as fallback
            return {
                "retention_rate": 85,
                "turnover_rate": 15,
                "retention_trend": "stable",
                "based_on": "industry_average"
            }

    def get_regional_trends(self, company_name: str, days: int = 30) -> dict:
        """
        Get hiring trends broken down by region.

        Returns:
            {
                "company_name": "Reckitt",
                "regions": {
                    "Europe": {
                        "current": 45,
                        "trend": "↑ +18%",
                        "direction": "increasing"
                    },
                    "North America": {
                        "current": 32,
                        "trend": "↓ -10%",
                        "direction": "decreasing"
                    }
                }
            }
        """
        try:
            if not self.sb:
                logger.error("[regional] Supabase not configured")
                return {}

            cutoff_date = (datetime.utcnow().date() - timedelta(days=days)).isoformat()
            today = datetime.utcnow().date().isoformat()

            # Get distinct regions
            response = self.sb.table("hiring_snapshots").select(
                "region"
            ).eq("company_name", company_name).not_.is_(
                "region", "is.null"
            ).gte(
                "snapshot_date", cutoff_date
            ).execute()

            regions = list(set([r.get("region") for r in response.data or [] if r.get("region")]))

            regional_trends = {}
            for region in regions:
                trend = self.get_hiring_trend(company_name, region=region, days=days)
                if trend:
                    regional_trends[region] = {
                        "current": trend.get("current_openings", 0),
                        "previous": trend.get("previous_openings", 0),
                        "trend": trend.get("trend", "—"),
                        "direction": trend.get("trend_direction", "unknown")
                    }

            return {
                "company_name": company_name,
                "period_days": days,
                "regions": regional_trends
            }

        except Exception as e:
            logger.error(f"[regional] Error getting regional trends: {e}")
            return {}

    def _extract_region(self, location: str) -> str:
        """Extract region from location string."""
        location_lower = location.lower()

        regions = {
            "North America": ["usa", "us", "canada", "california", "new york", "toronto"],
            "Europe": ["uk", "london", "berlin", "paris", "europe"],
            "Asia": ["india", "singapore", "tokyo", "hong kong", "asia"],
            "Remote": ["remote", "worldwide", "anywhere"]
        }

        for region, keywords in regions.items():
            if any(kw in location_lower for kw in keywords):
                return region

        return "Various"


# Standalone functions for Flask routes
def take_snapshot(company_name: str) -> dict:
    """Take a hiring snapshot for a company."""
    tracker = HiringTrendsTracker()
    return tracker.take_daily_snapshot(company_name)


def get_trend(company_name: str, region: str = None, days: int = 30) -> dict:
    """Get hiring trend for a company."""
    tracker = HiringTrendsTracker()
    return tracker.get_hiring_trend(company_name, region=region, days=days)


def get_regional_trends_data(company_name: str, days: int = 30) -> dict:
    """Get regional hiring trends for a company."""
    tracker = HiringTrendsTracker()
    return tracker.get_regional_trends(company_name, days=days)


def get_retention_metrics(company_name: str) -> dict:
    """Get employee retention metrics for a company."""
    tracker = HiringTrendsTracker()
    return tracker.calculate_retention_metrics(company_name)
