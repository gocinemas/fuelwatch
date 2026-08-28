"""
Motivation nudges and copy for Miru Phase 2+.
Central place for celebration/streak copy so tone stays consistent.

Rules (from MIRU_STEERING.md):
- After 9pm: never suggest going out or buying
- Priority scores: School 90, Commute 85, Spend 60
- Motivation scores: ~55 (savings), ~50 (sustainability), ~35 (time saved)
"""

import re
from datetime import datetime, time as datetime_time


def _send_whatsapp_message(wa: str, body: str) -> bool:
    """
    Send an outbound WhatsApp message via Twilio.
    Self-contained (no import from sms_service) to avoid circular imports —
    mirrors miru/motivation/endpoints.py::_wa_send_proactive.
    """
    from twilio.rest import Client
    import os

    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    twilio_whatsapp = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")

    if not account_sid or not auth_token:
        print("[nudges] Missing Twilio creds, cannot send celebration")
        return False

    try:
        client = Client(account_sid, auth_token)
        message = client.messages.create(from_=twilio_whatsapp, body=body, to=wa)
        print(f"[nudges] Celebration sent to {wa}: {message.sid}")
        return True
    except Exception as e:
        print(f"[nudges] Error sending celebration to {wa}: {e}")
        return False


def _log_achievement(wa: str, event: dict, msg: str) -> None:
    """
    Log a celebrated win to savings_events, for later gamification
    (streaks, badges, leaderboards) and for the Monday brief callback.
    """
    try:
        import library as lib

        event_type = event.get('type') or 'celebration'
        value = event.get('value', 0)

        row = {
            "wa": wa,
            "event_type": "weekly_savings_win" if event_type == "weekly_savings" else event_type,
            "description": msg,
        }

        if event_type == 'sustainability_action':
            row["co2_grams"] = value
        else:
            row["amount_pence"] = value

        lib._sb().table("savings_events").insert(row).execute()
    except Exception as e:
        print(f"[nudges] Error logging achievement for {wa}: {e}")


def celebrate(event: dict, wa: str = None) -> str:
    """
    Generate celebration copy for a win event.

    event = {
        'type': 'fuel_drop' | 'beat_target' | 'sustainability_action' | 'weekly_savings',
        'value': amount in pence (or grams CO2e for sustainability_action),
        'description': str
    }

    If `wa` (a WhatsApp number, e.g. "whatsapp:+44...") is provided, the
    celebration is also SENT via Twilio and LOGGED to savings_events for
    later gamification. Without `wa`, this is a pure copy generator.

    Returns celebration message string or None.
    """
    event_type = event.get('type')
    value = event.get('value', 0)

    msg = None

    if event_type == 'fuel_drop':
        # Fuel price drop
        value_gbp = value / 100
        if value_gbp < 0.5:
            msg = f"⛽ Fuel down {value_gbp:.2f}p/L"
        elif value_gbp < 2:
            msg = f"⛽ Nice! Fuel down £{value_gbp:.2f}"
        else:
            msg = f"⛽ Great! Fuel dropped £{value_gbp:.2f}"

    elif event_type == 'beat_target':
        # User beat their weekly target
        value_gbp = value / 100
        msg = f"🎉 You beat your target by £{value_gbp:.2f}!"

    elif event_type == 'weekly_savings':
        # Sunday cron: user spent significantly less than last week
        value_gbp = value / 100
        msg = f"🎉 You saved £{value_gbp:.2f} this week! Keep it up."

    elif event_type == 'sustainability_action':
        # Sustainability win (TGTG, local shop, etc.)
        co2_kg = value / 1000  # value in grams
        msg = f"🌱 Nice! That saved ~{co2_kg:.1f}kg CO2e"

    if msg and wa:
        _send_whatsapp_message(wa, msg)
        _log_achievement(wa, event, msg)

    return msg


def nudge_cta(feature: str) -> str:
    """
    Generate reply-keyword footer and CTA for a feature.

    feature = 'price_alert' | 'weekly_summary' | 'goal'

    Returns footer string or None.
    """
    if feature == 'price_alert':
        return "Reply STOP to pause price alerts anytime."
    elif feature == 'weekly_summary':
        return "Reply BEAT to set next week's target lower."
    elif feature == 'goal':
        return "Reply DONE if you've hit it early!"
    return None


def priority_score(event_type: str) -> int:
    """
    Priority score for brief ranking.

    School 90, Commute 85, Spend 60, Savings 55, Sustainability 50, Weather 40, Time 35.

    Used to decide which card appears first in brief narrative.
    """
    scores = {
        'school': 90,
        'commute': 85,
        'spend': 60,
        'savings': 55,
        'sustainability': 50,
        'weather': 40,
        'time_saved': 35,
    }
    return scores.get(event_type, 30)


def should_suppress_cta(current_time: datetime = None) -> bool:
    """
    Check if we should suppress action CTAs based on time of day.

    After 21:00 (9pm), don't suggest going out/buying/setting goals.
    Before 05:00, also suppress.

    Returns True if CTA should be suppressed.
    """
    if current_time is None:
        current_time = datetime.now()

    hour = current_time.hour
    # Suppress 21:00-05:00
    return hour >= 21 or hour < 5


def should_celebrate(current_time: datetime = None) -> bool:
    """
    Celebration messages are OK any time, even at night (they're not CTAs to act).

    Returns True if celebration is OK to send.
    """
    # Always OK to celebrate wins, any time
    return True


def format_price_drop_notification(
    fuel_type: str,
    postcode: str,
    new_price_ppl: float,
    old_price_ppl: float,
    drop_ppl: float,
    total_saved_pence: int,
    cheapest_merchant: str = None,
    distance_miles: float = None
) -> str:
    """
    Format a price drop alert for WhatsApp.

    Example:
    ⛽ Fuel price drop!
    Petrol in KT16 0DA is now 138.9p/litre
    (was 142.9p — down 4.0p)

    Cheapest: Tesco · High St · 0.8mi away
    Save ~£2.20 on a full tank
    💰 Miru has saved you £31.40 so far this year

    🔗 miru.humanagency.co
    """
    # Tank size estimate for savings: 55L
    tank_savings = (drop_ppl * 55) / 100

    msg = f"""⛽ Fuel price drop!
{fuel_type} in {postcode} is now {new_price_ppl:.1f}p/litre
(was {old_price_ppl:.1f}p — down {drop_ppl:.1f}p)"""

    if cheapest_merchant:
        location = f"{cheapest_merchant}"
        if distance_miles:
            location += f" · {distance_miles:.1f}mi away"
        msg += f"\n\nCheapest: {location}"

    msg += f"\nSave ~£{tank_savings:.2f} on a full tank"
    msg += f"\n💰 Miru has saved you £{total_saved_pence/100:.2f} so far this year"
    msg += f"\n\n🔗 miru.humanagency.co"

    return msg


def format_weekly_summary_with_goal(
    total_spent_pence: int,
    last_week_pence: int,
    variance_pence: int,
    fuel_saved_pence: int,
    receipt_count: int,
    top_category: str,
    current_target_pence: int = None
) -> str:
    """
    Format weekly savings summary for WhatsApp.

    If current_target_pence provided, check if user beat it.
    """
    total_gbp = total_spent_pence / 100
    variance_gbp = abs(variance_pence) / 100
    fuel_gbp = fuel_saved_pence / 100
    target_gbp = current_target_pence / 100 if current_target_pence else None

    variance_dir = "↓" if variance_pence < 0 else ("↑" if variance_pence > 0 else "→")
    trend = "🟢" if variance_pence < 0 else ("🔴" if variance_pence > 0 else "⚪")

    week_dates = "11–17 Aug"  # TODO: compute from actual dates

    msg = f"""📊 YOUR WEEK WITH MIRU
{week_dates}

💰 Spent: £{total_gbp:.2f} ({variance_dir}£{variance_gbp:.2f} vs last week)
⛽ Fuel alerts saved you £{fuel_gbp:.2f}
🧾 {receipt_count} receipts logged, mostly {top_category.lower()}"""

    # If target provided, compare
    if target_gbp:
        if total_spent_pence <= current_target_pence:
            msg += f"\n\n✅ You beat your £{target_gbp:.2f} target!"
        else:
            overage = (total_spent_pence - current_target_pence) / 100
            msg += f"\n\n📊 Target: £{target_gbp:.2f} | Actual: £{total_gbp:.2f} (+£{overage:.2f})"
    else:
        msg += f"\n\nYou're spending {('less' if variance_pence < 0 else 'more')} than last week. {trend}"

    msg += f"\n\nReply BEAT to set next week's target lower."

    return msg
