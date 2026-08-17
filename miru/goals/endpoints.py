"""
Flask endpoints for family goals (Phase 2).
Add these routes to sms_service.py after app definition.

Routes:
- GET /api/goals/<household_id> — list all active goals
- POST /api/goals — create new goal
- POST /api/goals/<goal_id>/update — log progress to a goal
- GET /api/goals/<goal_id>/progress — get current progress
"""

from flask import request, jsonify
from datetime import date, timedelta
import library as lib


def register_goals_endpoints(app):
    """Register all goals endpoints to the Flask app."""

    @app.route("/api/goals/<household_id>", methods=["GET"])
    def list_goals(household_id):
        """
        List all active goals for a household.
        GET /api/goals/<household_id>
        """
        try:
            goals = lib._sb().table("family_goals").select("*") \
                .eq("household_id", household_id) \
                .eq("status", "active") \
                .order("created_at", desc=False) \
                .execute().data or []

            # Enrich with progress
            for goal in goals:
                goal_id = goal.get("id")
                progress = lib._sb().table("goal_progress").select("value") \
                    .eq("goal_id", goal_id) \
                    .order("updated_at", desc=True) \
                    .limit(1) \
                    .execute().data or []

                goal["current_value"] = progress[0]["value"] if progress else 0

            return jsonify({"status": "ok", "goals": goals})

        except Exception as e:
            return jsonify({"error": str(e), "status": "error"}), 500

    @app.route("/api/goals", methods=["POST"])
    def create_goal():
        """
        Create a new family goal.
        POST /api/goals
        {
            "household_id": "44712345678",
            "title": "Takeaways under £100",
            "target_value": 10000,      // pence
            "goal_type": "spend_reduction",
            "period_days": 7,           // week
            "created_by_wa": "whatsapp:+44..."
        }
        """
        try:
            data = request.get_json() or {}
            household_id = data.get("household_id")
            title = data.get("title")
            target_value = int(data.get("target_value", 0))
            goal_type = data.get("goal_type", "spend_reduction")
            period_days = int(data.get("period_days", 7))
            created_by_wa = data.get("created_by_wa")

            if not all([household_id, title, target_value, created_by_wa]):
                return jsonify({"error": "Missing required fields"}), 400

            # Calculate period
            today = date.today()
            period_start = today - timedelta(days=today.weekday())  # Monday
            period_end = period_start + timedelta(days=period_days - 1)

            goal = lib._sb().table("family_goals").insert({
                "household_id": household_id,
                "goal_type": goal_type,
                "title": title,
                "target_value": target_value,
                "target_unit": "gbp",
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                "created_by_wa": created_by_wa
            }).execute()

            return jsonify({
                "status": "ok",
                "goal_id": goal.data[0]["id"] if goal.data else None
            })

        except Exception as e:
            return jsonify({"error": str(e), "status": "error"}), 500

    @app.route("/api/goals/<goal_id>/update", methods=["POST"])
    def update_goal_progress(goal_id):
        """
        Log progress to a goal (user spent £X on this category).
        Called when processing receipts to auto-update goals.
        POST /api/goals/<goal_id>/update
        {
            "value": 5000,      // pence, current week total
            "source": "spend_log"
        }
        """
        try:
            data = request.get_json() or {}
            value = float(data.get("value", 0))
            source = data.get("source", "spend_log")

            # Update goal's current_value
            lib._sb().table("family_goals").update({
                "current_value": value,
                "updated_at": "now()"
            }).eq("id", goal_id).execute()

            # Log progress entry
            lib._sb().table("goal_progress").insert({
                "goal_id": goal_id,
                "value": value,
                "source": source
            }).execute()

            return jsonify({"status": "ok"})

        except Exception as e:
            return jsonify({"error": str(e), "status": "error"}), 500

    @app.route("/api/goals/<goal_id>/progress", methods=["GET"])
    def get_goal_progress(goal_id):
        """
        Get current progress on a specific goal.
        GET /api/goals/<goal_id>/progress
        """
        try:
            goal = lib._sb().table("family_goals").select("*") \
                .eq("id", goal_id) \
                .maybe_single().execute()

            if not goal or not goal.data:
                return jsonify({"error": "Goal not found"}), 404

            goal = goal.data
            current = goal.get("current_value", 0) or 0
            target = goal.get("target_value", 0) or 0

            progress_pct = 0
            if target > 0:
                progress_pct = min(100, int(current / target * 100))

            remaining = max(0, target - current)

            return jsonify({
                "status": "ok",
                "goal_id": goal_id,
                "title": goal.get("title"),
                "target_value": target,
                "current_value": current,
                "remaining": remaining,
                "progress_percent": progress_pct,
                "period_start": goal.get("period_start"),
                "period_end": goal.get("period_end"),
                "period_end_date": goal.get("period_end")
            })

        except Exception as e:
            return jsonify({"error": str(e), "status": "error"}), 500

    @app.route("/api/goals/<goal_id>/complete", methods=["POST"])
    def complete_goal(goal_id):
        """
        Mark a goal as achieved or missed.
        POST /api/goals/<goal_id>/complete
        {
            "status": "achieved" | "missed"
        }
        """
        try:
            data = request.get_json() or {}
            status = data.get("status", "achieved")

            if status not in ["achieved", "missed"]:
                return jsonify({"error": "Invalid status"}), 400

            lib._sb().table("family_goals").update({
                "status": status,
                "updated_at": "now()"
            }).eq("id", goal_id).execute()

            return jsonify({"status": "ok", "goal_status": status})

        except Exception as e:
            return jsonify({"error": str(e), "status": "error"}), 500
