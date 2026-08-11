"""
Budget Tracker — Calculate spending pace and send warnings
"""
from datetime import datetime, timedelta
from supabase import create_client
import os

def get_budget_status(phone: str) -> dict:
    """
    Get user's budget status for current month

    Returns:
        {
            'current_spend': 250,
            'days_into_month': 10,
            'projected_total': 750,
            'average_monthly': 700,
            'status': 'on_pace' | 'overspending' | 'underspending',
            'pace_warning': 'You\'re on pace to spend £750 (avg: £700)',
            'days_remaining': 21
        }
    """
    try:
        supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

        # Get current month's spending
        now = datetime.now()
        month_start = now.replace(day=1)

        data, _ = supabase.table("receipts").select("total").where(
            f"phone = '{phone}' AND shop_date >= '{month_start.isoformat()}' AND shop_date < '{now.isoformat()}'"
        ).execute()

        current_spend = sum(float(r.get('total', 0)) for r in (data[1] if data else []))

        # Get average monthly spend (last 3 months)
        three_months_ago = now - timedelta(days=90)
        historical, _ = supabase.table("receipts").select("total").where(
            f"phone = '{phone}' AND shop_date >= '{three_months_ago.isoformat()}'"
        ).execute()

        all_spend = [float(r.get('total', 0)) for r in (historical[1] if historical else [])]
        average_monthly = sum(all_spend) / 3 if all_spend else 0

        # Calculate pace
        days_into_month = now.day
        days_in_month = 30  # Simplified
        days_remaining = days_in_month - days_into_month

        daily_pace = current_spend / days_into_month if days_into_month > 0 else 0
        projected_total = daily_pace * days_in_month

        # Determine status
        if projected_total > average_monthly * 1.2:
            status = "overspending"
        elif projected_total < average_monthly * 0.8:
            status = "underspending"
        else:
            status = "on_pace"

        return {
            'current_spend': round(current_spend, 2),
            'days_into_month': days_into_month,
            'projected_total': round(projected_total, 2),
            'average_monthly': round(average_monthly, 2),
            'status': status,
            'pace_warning': f"💰 You're on pace to spend £{projected_total:.0f} this month (your avg: £{average_monthly:.0f})",
            'days_remaining': days_remaining
        }
    except Exception as e:
        print(f"[budget_tracker] Error: {e}")
        return None


def format_budget_report(budget_status: dict) -> str:
    """Format budget status for WhatsApp"""
    if not budget_status:
        return "Couldn't calculate budget status"

    emoji = "✅" if budget_status['status'] == "on_pace" else "⚠️" if budget_status['status'] == "overspending" else "🟢"

    text = f"{emoji} BUDGET CHECK\n\n"
    text += f"Spent so far: £{budget_status['current_spend']}\n"
    text += f"Days into month: {budget_status['days_into_month']}\n"
    text += f"{budget_status['pace_warning']}\n"
    text += f"Days remaining: {budget_status['days_remaining']}\n\n"

    if budget_status['status'] == "overspending":
        text += f"⚠️ On track to overspend by £{budget_status['projected_total'] - budget_status['average_monthly']:.0f}"
    elif budget_status['status'] == "underspending":
        text += f"🟢 On track to spend £{budget_status['average_monthly'] - budget_status['projected_total']:.0f} less"
    else:
        text += f"✅ On pace with your average spending"

    return text
