"""
Weekly savings summary for Miru motivation layer.
Built on proven _v2_fetch_spend path, not the broken budget_tracker/weekly_report.

Computes:
- Total spend this week
- Total spend last week
- Spend variance (up/down)
- Fuel alerts savings (cumulative from fuel_alerts.total_saved_pence)
- Receipt count
- Top category
"""

import re
from datetime import date, timedelta
import lib


def get_weekly_savings(from_number: str) -> dict:
    """
    Compute weekly savings for a WhatsApp number.

    Returns {
        'total_spent_pence': int,
        'last_week_pence': int,
        'week_variance_pence': int,
        'fuel_saved_pence': int,
        'receipt_count': int,
        'top_category': str,
        'period_end_date': 'YYYY-MM-DD',
        'status': 'ok' | 'no_data'
    }
    """
    try:
        today = date.today()

        # This week: Monday start to today
        # Sunday end: today (Friday/Saturday/Sunday might be partial)
        # Calculate: 7 days back from today at 00:00
        week_start = (today - timedelta(days=today.weekday())).isoformat()  # Monday
        week_end = today.isoformat()  # Today

        # Last week: 14 days back to 8 days back
        last_week_start = (today - timedelta(days=today.weekday() + 7)).isoformat()  # Previous Monday
        last_week_end = (today - timedelta(days=today.weekday())).isoformat()  # This Monday (start of current week)

        sb = lib._sb()

        # Fetch this week's receipts
        this_week_rows = sb.table("wa_saves").select("summary,title,category,id,created_at") \
            .eq("from_number", from_number) \
            .gte("created_at", week_start) \
            .lt("created_at", week_end) \
            .ilike("title", "🧾%") \
            .execute().data or []

        # Fetch last week's receipts for comparison
        last_week_rows = sb.table("wa_saves").select("summary,title,category,id,created_at") \
            .eq("from_number", from_number) \
            .gte("created_at", last_week_start) \
            .lt("created_at", last_week_end) \
            .ilike("title", "🧾%") \
            .execute().data or []

        # Helper: deduplicate and sum receipts for a list
        def sum_receipts(rows):
            """Deduplicate and sum receipt amounts."""
            seen_by_key = {}
            total = 0.0
            count = 0
            breakdown = {}

            for r in rows:
                created = r.get("created_at", "")
                date_key = created[:10]  # YYYY-MM-DD

                # Extract amount
                m = re.search(r'£([\d,]+\.?\d*)', r.get("summary", "") + r.get("title", ""))
                if not m:
                    continue

                try:
                    amount = float(m.group(1).replace(",", ""))
                except ValueError:
                    continue

                # Deduplicate by date + amount
                key = (date_key, round(amount, 2))
                if key in seen_by_key:
                    continue

                seen_by_key[key] = True
                merchant = (r.get("title") or "").replace("🧾", "").strip()

                # Skip online orders
                if merchant.startswith("Online:"):
                    continue

                total += amount
                count += 1

                # Category breakdown (use stored category or auto-detect)
                summary = r.get("summary") or ""
                # Import the category detection (assumes it exists in sms_service)
                # For now, use raw category or fallback to "Other"
                cat = (r.get("category") or "").strip() or "Other"
                breakdown[cat] = breakdown.get(cat, 0) + amount

            return int(total * 100), count, breakdown  # Return pence, count, breakdown

        # Sum this week and last week
        this_week_pence, this_week_count, this_week_cat = sum_receipts(this_week_rows)
        last_week_pence, last_week_count, last_week_cat = sum_receipts(last_week_rows)

        # Variance
        variance = this_week_pence - last_week_pence
        variance_direction = "↓" if variance < 0 else ("↑" if variance > 0 else "→")

        # Fetch fuel alerts savings (cumulative)
        fuel_alerts = sb.table("fuel_alerts") \
            .select("total_saved_pence") \
            .eq("wa", from_number) \
            .execute().data or []

        fuel_saved = sum(int(f.get("total_saved_pence", 0)) for f in fuel_alerts)

        # Top category this week
        top_cat = max(this_week_cat.items(), key=lambda x: x[1])[0] if this_week_cat else "Other"

        # Determine status: "no_data" if no receipts this week
        status = "ok" if this_week_count > 0 else "no_data"

        return {
            'total_spent_pence': this_week_pence,
            'last_week_pence': last_week_pence,
            'week_variance_pence': variance,
            'variance_direction': variance_direction,
            'fuel_saved_pence': fuel_saved,
            'receipt_count': this_week_count,
            'top_category': top_cat,
            'period_start_date': week_start,
            'period_end_date': week_end,
            'status': status
        }

    except Exception as e:
        import logging
        logging.error(f"[weekly_savings] Error computing for {from_number}: {e}")
        return {'status': 'error', 'error': str(e)}


def format_weekly_message(savings_data: dict) -> str:
    """
    Format weekly savings summary for WhatsApp.

    Example:
    📊 YOUR WEEK WITH MIRU
    11–17 Aug

    💰 Spent: £186 (↓£24 vs last week)
    ⛽ Fuel alerts saved you £8.40
    🧾 12 receipts logged, mostly groceries

    You're spending less than 6 of your last 8 weeks. 🟢

    Reply BEAT to set next week's target lower.
    """
    if savings_data.get('status') != 'ok':
        return None  # Don't send a message if no data

    total_pence = savings_data['total_spent_pence']
    last_week_pence = savings_data['last_week_pence']
    variance = savings_data['week_variance_pence']
    variance_dir = savings_data['variance_direction']
    fuel_saved = savings_data['fuel_saved_pence']
    receipt_count = savings_data['receipt_count']
    top_cat = savings_data['top_category']

    # Format amounts as GBP
    total_gbp = total_pence / 100
    variance_gbp = abs(variance) / 100
    fuel_gbp = fuel_saved / 100

    # Determine trend indicator
    if variance < 0:
        trend = "🟢"  # Spending down
    elif variance > 0:
        trend = "🔴"  # Spending up
    else:
        trend = "⚪"  # Same

    # Build message
    week_dates = "11–17 Aug"  # TODO: compute actual dates from savings_data period

    msg = f"""📊 YOUR WEEK WITH MIRU
{week_dates}

💰 Spent: £{total_gbp:.2f} ({variance_dir}£{variance_gbp:.2f} vs last week)
⛽ Fuel alerts saved you £{fuel_gbp:.2f}
🧾 {receipt_count} receipts logged, mostly {top_cat.lower()}

You're spending {('less' if variance < 0 else 'more')} than last week. {trend}

Reply BEAT to set next week's target lower.
"""

    return msg.strip()
