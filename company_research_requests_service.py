"""
Company Research Requests Service
Manage user requests to research companies.
"""

import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ResearchRequestService:
    """Handle company research requests from users."""

    def __init__(self):
        self.db = None

    def set_db(self, supabase_client):
        """Inject Supabase client."""
        self.db = supabase_client

    def request_research(self, company_name: str, requested_by: str = None, notes: str = None) -> dict:
        """
        Submit a research request for a company.

        Args:
            company_name: Company to research
            requested_by: User email (optional)
            notes: Additional notes (optional)

        Returns: {
            'success': bool,
            'id': request_id,
            'message': str,
            'company': company_name
        }
        """
        try:
            if not self.db:
                logger.warning("Database not initialized")
                return {"success": False, "message": "Database error"}

            # Check if request already exists
            existing = self.db.table("company_research_requests").select("id").eq(
                "company_name", company_name.lower()
            ).eq("status", "pending").execute()

            if existing.data and len(existing.data) > 0:
                return {
                    "success": True,
                    "id": existing.data[0]["id"],
                    "message": f"Research for {company_name} already requested!",
                    "company": company_name,
                    "status": "already_requested"
                }

            # Create new request
            payload = {
                "company_name": company_name.lower(),
                "requested_by": requested_by or "anonymous",
                "notes": notes,
                "status": "pending",
            }

            result = self.db.table("company_research_requests").insert(payload).execute()

            if result.data:
                request_id = result.data[0]["id"]
                logger.info(f"[research_request] Created request #{request_id} for {company_name}")
                return {
                    "success": True,
                    "id": request_id,
                    "message": f"✅ Research request submitted for {company_name}! We'll add this soon.",
                    "company": company_name,
                    "status": "submitted"
                }
            else:
                return {"success": False, "message": "Failed to create request"}

        except Exception as e:
            logger.error(f"[research_request] Error: {e}")
            return {"success": False, "message": str(e)}

    def get_pending_requests(self) -> list:
        """Get all pending research requests for admin dashboard."""
        try:
            if not self.db:
                return []

            result = self.db.table("company_research_requests").select(
                "id, company_name, requested_by, notes, created_at"
            ).eq("status", "pending").order("created_at", desc=True).execute()

            return result.data if result.data else []
        except Exception as e:
            logger.error(f"[research_request] Failed to fetch requests: {e}")
            return []

    def get_all_requests(self, limit: int = 50) -> list:
        """Get all research requests (admin)."""
        try:
            if not self.db:
                return []

            result = self.db.table("company_research_requests").select(
                "*"
            ).order("created_at", desc=True).limit(limit).execute()

            return result.data if result.data else []
        except Exception as e:
            logger.error(f"[research_request] Failed to fetch all requests: {e}")
            return []

    def mark_completed(self, request_id: int) -> bool:
        """Mark a research request as completed."""
        try:
            if not self.db:
                return False

            result = self.db.table("company_research_requests").update({
                "status": "completed",
                "completed_at": datetime.utcnow().isoformat(),
            }).eq("id", request_id).execute()

            if result.data:
                logger.info(f"[research_request] Marked #{request_id} as completed")
                return True
            return False
        except Exception as e:
            logger.error(f"[research_request] Failed to mark completed: {e}")
            return False

    def mark_researching(self, request_id: int) -> bool:
        """Mark a research request as currently being researched."""
        try:
            if not self.db:
                return False

            result = self.db.table("company_research_requests").update({
                "status": "researching"
            }).eq("id", request_id).execute()

            if result.data:
                logger.info(f"[research_request] Marked #{request_id} as researching")
                return True
            return False
        except Exception as e:
            logger.error(f"[research_request] Failed to mark researching: {e}")
            return False


# Initialize global instance
research_request_service = ResearchRequestService()
