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


def celebrate(event: dict) -> str:
    """
    Generate celebration copy for a win event.

    event = {
        'type': 'fuel_drop' | 'beat_target' | 'sustainability_action',
        'value': amount in pence,
        'description': str
    }

    Returns celebration message string or None.
    """
    event_type = event.get('type')
    value = event.get('value', 0)

    if event_type == 'fuel_drop':
        # Fuel price drop
        value_gbp = value / 100
        if value_gbp < 0.5:
            return f"⛽ Fuel down {value_gbp:.2f}p/L"
        elif value_gbp < 2:
            return f"⛽ Nice! Fuel down £{value_gbp:.2f}"
        else:
            return f"⛽ Great! Fuel dropped £{value_gbp:.2f}"

    elif event_type == 'beat_target':
        # User beat their weekly target
        value_gbp = value / 100
        return f"🎉 You beat your target by £{value_gbp:.2f}!"

    elif event_type == 'sustainability_action':
        # Sustainability win (TGTG, local shop, etc.)
        co2_kg = value / 1000  # value in grams
        return f"🌱 Nice! That saved ~{co2_kg:.1f}kg CO2e"

    return None


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
