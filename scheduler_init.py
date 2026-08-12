"""
Scheduler initialization for Flask app.
Sets up daily hiring snapshot cron job at 6 AM UTC.
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

scheduler = None


def init_scheduler(app):
    """Initialize the scheduler and register jobs."""
    global scheduler

    try:
        # Create background scheduler
        scheduler = BackgroundScheduler()

        # Register the daily hiring snapshot job
        # Runs every day at 6:00 AM UTC
        from hiring_snapshot_cron import run_daily_snapshots

        scheduler.add_job(
            run_daily_snapshots,
            trigger=CronTrigger(hour=6, minute=0, timezone='UTC'),
            id='daily_hiring_snapshots',
            name='Daily Hiring Snapshots',
            replace_existing=True,
            max_instances=1  # Only one instance at a time
        )

        # Start the scheduler
        if not scheduler.running:
            scheduler.start()
            logger.info("[scheduler] Daily hiring snapshot job scheduled for 6:00 AM UTC")

        # Log all scheduled jobs
        for job in scheduler.get_jobs():
            logger.info(f"[scheduler] Registered: {job.id} -> {job.name} at {job.trigger}")

        return True

    except ImportError:
        logger.warning("[scheduler] APScheduler not installed. Install with: pip install apscheduler")
        return False
    except Exception as e:
        logger.error(f"[scheduler] Error initializing scheduler: {e}")
        return False


def stop_scheduler():
    """Stop the scheduler (called on app shutdown)."""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("[scheduler] Scheduler stopped")


# Test function to manually trigger snapshot
def trigger_snapshot_now(company_name: str):
    """Manually trigger a hiring snapshot for testing."""
    try:
        from hiring_snapshot_cron import run_daily_snapshots
        result = run_daily_snapshots()
        return result
    except Exception as e:
        logger.error(f"[scheduler] Error triggering snapshot: {e}")
        return {"error": str(e)}
