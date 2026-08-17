"""
WhatsApp handlers for motivation features (Phase 1b).
Called from sms_service._whatsapp_reply_inner() for keywords: price alert, alerts off, beat.
"""

import lib
from miru.motivation import nudges


def handle_price_alert_setup(from_number: str, body: str) -> str:
    """
    Handle "price alert" / "alert me" keyword.
    Parses postcode and opts user into price alerts.
    """
    import re

    body_lower = body.lower().strip()

    # Try to extract postcode from the message
    # Match patterns: "price alert KT16 0HY" or "alert me KT16 0HY" or just "price alert"
    postcode_match = re.search(r'([A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})', body, re.IGNORECASE)
    postcode = None

    if postcode_match:
        postcode = postcode_match.group(1).replace(' ', '').upper()
    else:
        # Fall back to saved home postcode
        try:
            pc_row = lib._sb().table("my_area_places").select("postcode") \
                .eq("from_number", from_number).eq("category", "_home") \
                .maybe_single().execute()
            if pc_row and pc_row.data:
                postcode = pc_row.data["postcode"].upper().replace(' ', '')
        except Exception:
            pass

    if not postcode:
        return "⛽ *Fuel Price Alerts*\n\nWhat's your postcode?\n(e.g. *KT16 0HY*)\n\nI'll notify you when fuel prices drop."

    # Normalize postcode (remove spaces)
    postcode = postcode.replace(' ', '').upper()

    try:
        # Enable price alerts in motivation_prefs
        lib._sb().table("motivation_prefs").upsert({
            "wa": from_number,
            "price_alerts_enabled": True
        }, on_conflict="wa").execute()

        # Check if fuel alert already exists for this postcode
        existing = lib._sb().table("fuel_alerts").select("id") \
            .eq("wa", from_number).eq("postcode", postcode) \
            .maybe_single().execute()

        if existing and existing.data:
            return f"⛽ Already tracking fuel in {postcode}.\n\nYou'll get an alert when prices drop.\nReply *alerts off* to pause."
        else:
            # Create fuel alert record
            lib._sb().table("fuel_alerts").insert({
                "wa": from_number,
                "postcode": postcode,
                "fuel_type": "Unleaded",  # Default
                "last_price": None,
                "threshold_drop": 1.0,
                "total_saved_pence": 0
            }).execute()

            return f"⛽ *Fuel alerts set!*\n\n{postcode}\nUnleaded petrol\n\nI'll ping you when prices drop. Reply *alerts off* to pause."

    except Exception as e:
        return f"❌ Couldn't set up alerts: {e}"


def handle_alerts_off(from_number: str) -> str:
    """Handle "alerts off" keyword — disable price alerts."""
    try:
        lib._sb().table("motivation_prefs").upsert({
            "wa": from_number,
            "price_alerts_enabled": False
        }, on_conflict="wa").execute()

        return "⏸ Fuel alerts paused.\n\nReply *price alert* anytime to restart."
    except Exception as e:
        return f"❌ Error: {e}"


def handle_beat_target(from_number: str) -> str:
    """
    Handle "beat" keyword — set weekly spending target.
    User is committing to beat last week's spend.
    """
    try:
        # Get last week's spend
        from weekly_savings_summary import get_weekly_savings
        savings = get_weekly_savings(from_number)

        if savings.get('status') != 'ok':
            return "📊 I don't have enough spend data yet to set a target.\n\nLog some receipts first (send screenshots), then reply *beat*."

        last_week = savings.get('last_week_pence', 0)
        if last_week == 0:
            return "📊 No spend data from last week.\n\nLog receipts first, then reply *beat* next week."

        # Target: 5% reduction from last week
        target = int(last_week * 0.95)
        target_gbp = target / 100

        # Store target in motivation_prefs as JSON
        try:
            prefs_row = lib._sb().table("motivation_prefs").select("*") \
                .eq("wa", from_number).maybe_single().execute()
            prefs = prefs_row.data or {}
        except Exception:
            prefs = {}

        # Save weekly target
        prefs['weekly_target_pence'] = target
        prefs['weekly_target_set_date'] = lib.datetime.date.today().isoformat()

        lib._sb().table("motivation_prefs").upsert({
            "wa": from_number,
            **{k: v for k, v in prefs.items() if k in [
                'price_alerts_enabled', 'weekly_summary_enabled',
                'sustainability_enabled', 'family_goals_enabled',
                'social_proof_enabled', 'time_saved_enabled', 'is_internal_tester',
                'weekly_target_pence', 'weekly_target_set_date'
            ]}
        }, on_conflict="wa").execute()

        return f"🎯 *New weekly target: £{target_gbp:.2f}*\n\n(↓5% from last week's £{last_week/100:.2f})\n\nI'll track your progress and celebrate when you beat it! 🎉"

    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"❌ Couldn't set target: {e}"


def handle_weekly_summary_toggle(from_number: str, enable: bool = True) -> str:
    """
    Handle opt-in/opt-out for weekly summary messages.
    """
    try:
        lib._sb().table("motivation_prefs").upsert({
            "wa": from_number,
            "weekly_summary_enabled": enable
        }, on_conflict="wa").execute()

        if enable:
            return "📊 Weekly summaries enabled! You'll get a message every Sunday at 6pm."
        else:
            return "📊 Weekly summaries disabled. Reply *weekly* to turn them back on."

    except Exception as e:
        return f"❌ Error: {e}"
