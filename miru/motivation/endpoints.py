"""
Flask endpoints for motivation features (Phase 1b).
Add these routes to sms_service.py after the app definition.

Routes:
- POST /api/motivation/prefs — toggle feature flags
- POST /api/motivation/prefs/target — set weekly spending target
- GET /api/cron/weekly-savings — Sunday 18:00 cron job (send weekly summaries)
- (Modified) GET /api/fuel/check-drops — add savings_events logging + increment total_saved
"""

from flask import request, jsonify
from datetime import datetime, date, timedelta
import lib
from weekly_savings_summary import get_weekly_savings, format_weekly_message
from miru.motivation import nudges


def _wa_send_proactive(to: str, body: str):
    """Send outbound WhatsApp message."""
    from twilio.rest import Client
    import os

    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    twilio_whatsapp = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")

    if not account_sid or not auth_token:
        print(f"[_wa_send_proactive] Missing Twilio creds")
        return False

    try:
        client = Client(account_sid, auth_token)
        message = client.messages.create(
            from_=twilio_whatsapp,
            body=body,
            to=to
        )
        print(f"[_wa_send_proactive] Sent to {to}: {message.sid}")
        return True
    except Exception as e:
        print(f"[_wa_send_proactive] Error: {e}")
        return False


def register_motivation_endpoints(app):
    """Register all motivation endpoints to the Flask app."""

    @app.route("/api/motivation/prefs", methods=["POST"])
    def motivation_prefs():
        """
        Toggle motivation feature flags.
        POST /api/motivation/prefs
        {
            "wa": "whatsapp:+44...",
            "feature": "price_alerts" | "weekly_summary" | "sustainability" | etc,
            "enabled": true|false
        }
        """
        try:
            data = request.get_json() or {}
            wa = data.get("wa") or request.form.get("wa")
            feature = data.get("feature") or request.form.get("feature")
            enabled = data.get("enabled", True)

            if not wa or not feature:
                return jsonify({"error": "Missing wa or feature"}), 400

            # Map feature name to column
            feature_map = {
                "price_alerts": "price_alerts_enabled",
                "weekly_summary": "weekly_summary_enabled",
                "sustainability": "sustainability_enabled",
                "family_goals": "family_goals_enabled",
                "social_proof": "social_proof_enabled",
                "time_saved": "time_saved_enabled",
            }

            if feature not in feature_map:
                return jsonify({"error": "Unknown feature"}), 400

            column = feature_map[feature]
            lib._sb().table("motivation_prefs").upsert({
                "wa": wa,
                column: enabled
            }, on_conflict="wa").execute()

            return jsonify({"status": "ok", "wa": wa, "feature": feature, "enabled": enabled})

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/motivation/prefs/target", methods=["POST"])
    def set_weekly_target():
        """
        Set weekly spending target.
        POST /api/motivation/prefs/target
        {
            "wa": "whatsapp:+44...",
            "target_pence": 18000  // £180.00
        }
        """
        try:
            data = request.get_json() or {}
            wa = data.get("wa") or request.form.get("wa")
            target_pence = int(data.get("target_pence", 0))

            if not wa or target_pence <= 0:
                return jsonify({"error": "Missing wa or invalid target"}), 400

            lib._sb().table("motivation_prefs").upsert({
                "wa": wa,
                "weekly_target_pence": target_pence,
                "weekly_target_set_date": date.today().isoformat()
            }, on_conflict="wa").execute()

            return jsonify({"status": "ok", "target_pence": target_pence})

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/cron/weekly-savings", methods=["GET"])
    def cron_weekly_savings():
        """
        Cron job: Send weekly savings summaries (Sunday 18:00).
        GET /api/cron/weekly-savings?token=DIGEST_TOKEN

        Fetches all users with weekly_summary_enabled, computes summaries, sends WhatsApp.
        Also inserts savings_events rows for the week.
        """
        import os
        token = request.args.get("token", "")
        expected_token = os.environ.get("CRON_TOKEN", "miru-digest-2026")

        if token != expected_token:
            return jsonify({"error": "Invalid token"}), 403

        try:
            # Get all users with weekly_summary_enabled
            users = lib._sb().table("motivation_prefs").select("wa") \
                .eq("weekly_summary_enabled", True) \
                .execute().data or []

            sent = 0
            errors = 0

            for user_row in users:
                wa = user_row.get("wa")
                if not wa:
                    continue

                try:
                    # Get savings for the week
                    savings = get_weekly_savings(wa)

                    if savings.get('status') != 'ok':
                        continue

                    # Format message
                    msg = format_weekly_message(savings)
                    if not msg:
                        continue

                    # Send WhatsApp
                    if _wa_send_proactive(wa, msg):
                        sent += 1

                        # Log event
                        variance = savings.get('week_variance_pence', 0)
                        lib._sb().table("savings_events").insert({
                            "wa": wa,
                            "event_type": "underspend_week",
                            "amount_pence": abs(variance),
                            "description": f"Weekly summary: spent £{savings['total_spent_pence']/100:.2f}"
                        }).execute()
                    else:
                        errors += 1

                except Exception as e:
                    print(f"[weekly-savings] Error for {wa}: {e}")
                    errors += 1

            return jsonify({
                "status": "ok",
                "sent": sent,
                "errors": errors,
                "total_users": len(users)
            })

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/motivation/savings-summary/<from_number>", methods=["GET"])
    def get_savings_summary(from_number):
        """
        Get weekly savings summary for a specific user.
        Used by brief integration.
        GET /api/motivation/savings-summary/whatsapp:+44...
        """
        try:
            savings = get_weekly_savings(from_number)
            return jsonify(savings)
        except Exception as e:
            return jsonify({"error": str(e), "status": "error"}), 500
