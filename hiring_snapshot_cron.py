"""
Daily Hiring Snapshot Cron Job
Runs at 6 AM UTC daily to record hiring snapshots for tracked companies.
"""

import os
import logging
from datetime import datetime
from hiring_trends_tracker import HiringTrendsTracker

logger = logging.getLogger(__name__)

# List of companies to track
TRACKED_COMPANIES = [
    "Apple",
    "Google",
    "Microsoft",
    "Amazon",
    "Meta",
    "Tesla",
    "Netflix",
    "Reckitt",
    "Unilever",
    "Nike",
    "Adidas",
    "Monzo",
    "Wise",
    "Revolut",
]


def run_daily_snapshots():
    """
    Take hiring snapshots for all tracked companies.
    Called daily at 6 AM UTC.
    """
    logger.info(f"[cron] Starting daily hiring snapshots at {datetime.utcnow()}")

    tracker = HiringTrendsTracker()
    results = {}

    for company in TRACKED_COMPANIES:
        try:
            snapshot = tracker.take_daily_snapshot(company)
            results[company] = {
                "status": "success",
                "total_openings": snapshot.get("total_openings", 0),
                "regions": len(snapshot.get("regions_snapshot", {})),
                "departments": len(snapshot.get("departments_snapshot", {}))
            }
            logger.info(f"[cron] ✓ {company}: {snapshot.get('total_openings', 0)} openings")
        except Exception as e:
            results[company] = {
                "status": "error",
                "error": str(e)
            }
            logger.error(f"[cron] ✗ {company}: {e}")

    logger.info(f"[cron] Daily snapshots complete: {len([r for r in results.values() if r.get('status') == 'success'])}/{len(TRACKED_COMPANIES)} succeeded")

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "completed": len([r for r in results.values() if r.get('status') == 'success']),
        "total": len(TRACKED_COMPANIES),
        "results": results
    }


def add_company_to_tracking(company_name: str):
    """Add a new company to the daily tracking list."""
    if company_name not in TRACKED_COMPANIES:
        TRACKED_COMPANIES.append(company_name)
        logger.info(f"[tracking] Added {company_name} to daily snapshots")
    return {
        "company": company_name,
        "tracked_companies": len(TRACKED_COMPANIES),
        "message": f"{company_name} will be tracked starting next daily run"
    }


# For testing: can be called directly
if __name__ == "__main__":
    result = run_daily_snapshots()
    print(result)
