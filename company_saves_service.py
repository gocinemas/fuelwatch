"""
Company Saves Service
Manages user's saved/bookmarked companies for quick access.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CompanySavesService:
    """Manage saved companies for users."""

    def __init__(self):
        self.db = None

    def set_db(self, supabase_client):
        """Inject Supabase client."""
        self.db = supabase_client

    def save_company(self, user_id: str, company_name: str) -> bool:
        """Save a company to user's list."""
        try:
            if not self.db:
                return False

            result = self.db.table("company_saves").insert({
                "user_id": user_id.lower(),
                "company_name": company_name.lower(),
            }).execute()

            if result.data:
                logger.info(f"[saves] Saved {company_name} for {user_id}")
                return True
            return False

        except Exception as e:
            logger.warning(f"[saves] Already saved or error: {e}")
            return True  # Idempotent - if already exists, still success

    def unsave_company(self, user_id: str, company_name: str) -> bool:
        """Remove a company from user's list."""
        try:
            if not self.db:
                return False

            result = self.db.table("company_saves").delete().eq(
                "user_id", user_id.lower()
            ).eq("company_name", company_name.lower()).execute()

            logger.info(f"[saves] Unsaved {company_name} for {user_id}")
            return True

        except Exception as e:
            logger.error(f"[saves] Unsave error: {e}")
            return False

    def get_saves(self, user_id: str) -> list:
        """Get all saved companies for a user."""
        try:
            if not self.db:
                return []

            result = self.db.table("company_saves").select("*").eq(
                "user_id", user_id.lower()
            ).order("saved_at", desc=True).execute()

            if result.data:
                return [{"company": row["company_name"], "saved_at": row["saved_at"]}
                        for row in result.data]
            return []

        except Exception as e:
            logger.error(f"[saves] Get error: {e}")
            return []

    def is_saved(self, user_id: str, company_name: str) -> bool:
        """Check if company is saved by user."""
        try:
            if not self.db:
                return False

            result = self.db.table("company_saves").select("id").eq(
                "user_id", user_id.lower()
            ).eq("company_name", company_name.lower()).execute()

            return len(result.data) > 0

        except Exception as e:
            logger.error(f"[saves] Is saved check error: {e}")
            return False


# Global instance
company_saves_service = CompanySavesService()
