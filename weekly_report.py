"""
Weekly Report Generator — Send Monday morning spend summaries
"""
from datetime import datetime, timedelta
from supabase import create_client
import os

def get_weekly_report(phone: str) -> dict:
    """
    Get user's spending for the past week

    Returns:
        {
            'week_total': 340,
            'week_start': '2026-08-04',
            'week_end': '2026-08-10',
            'daily_breakdown': [
                {'day': 'Mon', 'amount': 45, 'transactions': 2},
                ...
            ],
            'category_breakdown': {
                'Groceries': 120,
                'Coffee': 45,
                'Transport': 75
            },
            'vs_last_week': -15,  # negative = spent less
            'trending': 'up' | 'down' | 'stable'
        }
    """
    try:
        supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

        # Get week dates (Mon-Sun)
        today = datetime.now()
        monday = today - timedelta(days=today.weekday())
        sunday = monday + timedelta(days=6)

        # Get this week's spend
        data, _ = supabase.table("receipts").select("total,shop_date,merchant").where(
            f"phone = '{phone}' AND shop_date >= '{monday.isoformat()}' AND shop_date <= '{sunday.isoformat()}'"
        ).execute()

        transactions = data[1] if data and len(data) > 1 else []
        week_total = sum(float(t.get('total', 0)) for t in transactions)

        # Daily breakdown
        daily = {}
        for t in transactions:
            day = datetime.fromisoformat(t['shop_date']).strftime('%a')
            if day not in daily:
                daily[day] = {'amount': 0, 'count': 0}
            daily[day]['amount'] += float(t.get('total', 0))
            daily[day]['count'] += 1

        daily_breakdown = [
            {'day': day, 'amount': round(daily[day]['amount'], 2), 'transactions': daily[day]['count']}
            for day in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            if day in daily
        ]

        # Category breakdown (simplified)
        categories = {}
        for t in transactions:
            merchant = t.get('merchant', 'Other').split()[0]  # First word
            if merchant not in categories:
                categories[merchant] = 0
            categories[merchant] += float(t.get('total', 0))

        # Last week comparison
        last_monday = monday - timedelta(days=7)
        last_sunday = last_monday + timedelta(days=6)

        last_week_data, _ = supabase.table("receipts").select("total").where(
            f"phone = '{phone}' AND shop_date >= '{last_monday.isoformat()}' AND shop_date <= '{last_sunday.isoformat()}'"
        ).execute()

        last_week_trans = last_week_data[1] if last_week_data and len(last_week_data) > 1 else []
        last_week_total = sum(float(t.get('total', 0)) for t in last_week_trans)
        vs_last_week = round(week_total - last_week_total, 2)

        # Trend
        if vs_last_week > 20:
            trending = "up"
        elif vs_last_week < -20:
            trending = "down"
        else:
            trending = "stable"

        return {
            'week_total': round(week_total, 2),
            'week_start': monday.strftime('%Y-%m-%d'),
            'week_end': sunday.strftime('%Y-%m-%d'),
            'daily_breakdown': daily_breakdown,
            'category_breakdown': {k: round(v, 2) for k, v in sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]},
            'vs_last_week': vs_last_week,
            'trending': trending,
            'transaction_count': len(transactions)
        }
    except Exception as e:
        print(f"[weekly_report] Error: {e}")
        return None


def format_weekly_report(report: dict) -> str:
    """Format weekly report for WhatsApp"""
    if not report:
        return "Couldn't generate weekly report"

    trend_emoji = "📈" if report['trending'] == "up" else "📉" if report['trending'] == "down" else "➡️"
    vs_emoji = "⬆️" if report['vs_last_week'] > 0 else "⬇️" if report['vs_last_week'] < 0 else "➡️"

    text = f"📊 WEEKLY SPEND REPORT\n"
    text += f"{report['week_start']} → {report['week_end']}\n\n"

    text += f"💰 TOTAL: £{report['week_total']}\n"
    text += f"{vs_emoji} vs last week: £{abs(report['vs_last_week'])} {'more' if report['vs_last_week'] > 0 else 'less'}\n"
    text += f"{trend_emoji} Trend: {report['trending'].upper()}\n\n"

    text += "📅 DAILY BREAKDOWN:\n"
    for day in report['daily_breakdown']:
        text += f"  {day['day']}: £{day['amount']} ({day['transactions']} txns)\n"

    text += "\n🏪 TOP CATEGORIES:\n"
    for cat, amount in list(report['category_breakdown'].items())[:4]:
        pct = (amount / report['week_total'] * 100) if report['week_total'] > 0 else 0
        text += f"  {cat}: £{amount} ({pct:.0f}%)\n"

    text += f"\n✨ {report['transaction_count']} transactions this week"

    return text
