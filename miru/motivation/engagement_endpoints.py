"""
Flask endpoints for Phase 2 engagement loops (weekly wins digest, goals
progress, social proof, badges). Add to sms_service.py after app definition,
same pattern as register_motivation_endpoints / register_goals_endpoints.

Routes:
- GET  /api/v2/engagement-summary?wa=... — everything the "Wins & Goals" card needs
- POST /api/v2/goals — create a personal goal (wraps family_goals, household_id = wa)
"""

from flask import request, jsonify
from datetime import date, timedelta

import library as lib
from miru.motivation.engagement import (
    build_engagement_summary,
    resolve_wa,
    _household_id,
)


def register_engagement_endpoints(app):
    """Register engagement endpoints to the Flask app."""

    @app.route("/api/v2/engagement-summary", methods=["GET"])
    def v2_engagement_summary():
        raw = request.args.get("wa") or request.args.get("token") or ""
        wa = resolve_wa(raw)
        if not wa:
            return jsonify({"status": "error", "error": "Missing or unresolvable wa"}), 400
        try:
            return jsonify(build_engagement_summary(wa))
        except Exception as e:
            app.logger.error(f"[engagement-summary] {e}")
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/api/v2/goals", methods=["POST"])
    def v2_create_goal():
        """
        Create a personal goal from the web app.
        POST /api/v2/goals { wa, title, target_value (pounds), goal_type?, period_days? }
        Mirrors miru/goals/endpoints.py::create_goal but keyed off wa instead
        of a caller-supplied household_id, and takes pounds (web-friendly)
        rather than pence.
        """
        try:
            data = request.get_json(silent=True) or {}
            wa = resolve_wa(data.get("wa") or "")
            title = (data.get("title") or "").strip()
            target_gbp = data.get("target_value")
            goal_type = data.get("goal_type", "spend_reduction")
            period_days = int(data.get("period_days", 7))

            if not wa or not title or not target_gbp:
                return jsonify({"status": "error", "error": "Missing wa, title, or target_value"}), 400

            target_pence = int(round(float(target_gbp) * 100))
            household_id = _household_id(wa)

            # Ensure a household_members row exists (family_goals FKs to it),
            # same self-as-admin bootstrap miru/goals/handlers.py uses.
            try:
                hh = lib._sb().table("household_members").select("household_id") \
                    .eq("household_id", household_id).maybe_single().execute()
                if not hh or not hh.data:
                    lib._sb().table("household_members").insert({
                        "household_id": household_id, "wa": wa,
                        "display_name": "You", "role": "admin",
                    }).execute()
            except Exception:
                pass

            today = date.today()
            period_start = today
            period_end = today + timedelta(days=max(1, period_days) - 1)

            goal = lib._sb().table("family_goals").insert({
                "household_id": household_id,
                "goal_type": goal_type,
                "title": title,
                "target_value": target_pence,
                "target_unit": "gbp",
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                "created_by_wa": wa,
                "status": "active",
            }).execute()

            return jsonify({
                "status": "ok",
                "goal_id": goal.data[0]["id"] if goal.data else None,
            })

        except Exception as e:
            app.logger.error(f"[v2-create-goal] {e}")
            return jsonify({"status": "error", "error": str(e)}), 500
