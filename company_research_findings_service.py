"""
Research Findings Service
Manages hybrid AI + human research data combining auto-gathered and admin-verified findings.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ResearchFindingsService:
    """Store and merge auto-gathered data + admin edits."""

    def __init__(self):
        self.db = None

    def set_db(self, supabase_client):
        """Inject Supabase client."""
        self.db = supabase_client

    def create_findings(self, company_name: str, request_id: int = None) -> dict:
        """Create empty findings record for a company research."""
        try:
            if not self.db:
                return {"error": "Database not initialized"}

            payload = {
                "company_name": company_name.lower(),
                "request_id": request_id,
                "agent_status": "pending"
            }

            result = self.db.table("company_research_findings").insert(payload).execute()

            if result.data:
                logger.info(f"[findings] Created for {company_name}")
                return result.data[0]
            return {"error": "Failed to create findings"}

        except Exception as e:
            logger.error(f"[findings] Create error: {e}")
            return {"error": str(e)}

    def save_agent_findings(self, company_name: str, agent_data: dict) -> bool:
        """Save auto-gathered data from research agent."""
        try:
            if not self.db:
                return False

            payload = {
                "auto_description": agent_data.get("description"),
                "auto_market_position": agent_data.get("market_position"),
                "auto_risks": agent_data.get("risks"),
                "auto_opportunities": agent_data.get("opportunities"),
                "auto_brands": agent_data.get("brands"),
                "auto_financials": agent_data.get("financials"),
                "agent_status": "completed",
                "updated_at": datetime.utcnow().isoformat()
            }

            result = self.db.table("company_research_findings").update(payload).eq(
                "company_name", company_name.lower()
            ).execute()

            if result.data:
                logger.info(f"[findings] Saved agent data for {company_name}")
                return True
            return False

        except Exception as e:
            logger.error(f"[findings] Save agent error: {e}")
            return False

    def get_findings(self, company_name: str) -> dict:
        """Get all findings (auto + admin) for a company."""
        try:
            if not self.db:
                return {}

            result = self.db.table("company_research_findings").select("*").eq(
                "company_name", company_name.lower()
            ).execute()

            if result.data and len(result.data) > 0:
                return result.data[0]
            return {}

        except Exception as e:
            logger.error(f"[findings] Get error: {e}")
            return {}

    def save_admin_edits(self, company_name: str, admin_data: dict) -> bool:
        """Save admin edits/additions to findings."""
        try:
            if not self.db:
                return False

            payload = {
                "admin_description": admin_data.get("description"),
                "admin_market_position": admin_data.get("market_position"),
                "admin_risks": admin_data.get("risks"),
                "admin_opportunities": admin_data.get("opportunities"),
                "admin_brands": admin_data.get("brands"),
                "admin_financials": admin_data.get("financials"),
                "admin_verified": admin_data.get("verified", False),
                "admin_notes": admin_data.get("notes"),
                "updated_at": datetime.utcnow().isoformat()
            }

            result = self.db.table("company_research_findings").update(payload).eq(
                "company_name", company_name.lower()
            ).execute()

            if result.data:
                logger.info(f"[findings] Saved admin edits for {company_name}")
                return True
            return False

        except Exception as e:
            logger.error(f"[findings] Save admin error: {e}")
            return False

    def merge_findings(self, company_name: str) -> dict:
        """
        Merge auto-gathered + admin data (admin input takes priority).
        Returns combined dataset ready for database storage.
        """
        findings = self.get_findings(company_name)

        if not findings:
            return {}

        # Admin data overrides auto data
        merged = {
            "description": findings.get("admin_description") or findings.get("auto_description"),
            "market_position": findings.get("admin_market_position") or findings.get("auto_market_position"),
            "risks": findings.get("admin_risks") or findings.get("auto_risks", []),
            "opportunities": findings.get("admin_opportunities") or findings.get("auto_opportunities", []),
            "brands": findings.get("admin_brands") or findings.get("auto_brands", []),
            "financials": findings.get("admin_financials") or findings.get("auto_financials", {}),
            "admin_verified": findings.get("admin_verified", False),
            "admin_notes": findings.get("admin_notes"),
        }

        return merged


# Initialize global instance
research_findings_service = ResearchFindingsService()
