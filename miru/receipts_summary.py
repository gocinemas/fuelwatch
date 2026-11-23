"""
Weekly and monthly spend summaries built from saved receipts (wa_saves, 🧾 rows).

Used by:
- the weekly WhatsApp digest (appended to the weekly savings message)
- the web app (GET /api/v2/receipts/summary)
- scripts/receipt_summary.py (preview in the terminal)

Read-only: nothing here sends messages or writes to the database.
"""
from __future__ import annotations

import collections
import re
from datetime import date, timedelta

# Miru's category labels -> a few buckets people recognise
BUCKETS = {
    "Groceries": "Groceries",
    "Fuel": "Fuel & transport",
    "Transport": "Fuel & transport",
    "Dining": "Eating out & coffee",
    "Eating out": "Eating out & coffee",
    "Coffee & Lunch": "Eating out & coffee",
    "Takeaway": "Eating out & coffee",
    "Fast Food": "Eating out & coffee",
}

EATING_OUT = "Eating out & coffee"
FUEL = "Fuel & transport"


def bucket_for(category: str | None) -> str:
    return BUCKETS.get(category or "", "Other")


def parse_total(summary: str | None) -> float | None:
    m = re.search(r"Total: £([\d.,]+)", summary or "")
    return float(m.group(1).replace(",", "")) if m else None


def _has_date(summary: str | None) -> bool:
    return "date not on receipt" not in (summary or "")


def load_receipts(lib, from_number: str) -> list[dict]:
    """Receipts for one user, normalised. `lib` is the library module (for _sb())."""
    plain = from_number.replace("whatsapp:", "").strip()
    rows = (lib._sb().table("wa_saves")
            .select("title,summary,category,created_at,from_number")
            .ilike("title", "🧾%")
            .limit(2000)
            .execute().data or [])
    out = []
    for r in rows:
        if plain not in (r.get("from_number") or ""):
            continue
        total = parse_total(r.get("summary"))
        if total is None:
            continue
        out.append({
            "date": date.fromisoformat((r.get("created_at") or "")[:10]),
            "shop": r["title"].replace("🧾 ", "").split(" (")[0].strip(),
            "bucket": bucket_for(r.get("category")),
            "total": total,
            "dated": _has_date(r.get("summary")),
        })
    return out


def week_bounds(today: date):
    """Last 7 days ending today, and the 7 days before that."""
    this_start = today - timedelta(days=6)
    prev_end = this_start - timedelta(days=1)
    return this_start, today, prev_end - timedelta(days=6), prev_end


def month_bounds(year: int, month: int):
    start = date(year, month, 1)
    nxt = date(year + (month == 12), (month % 12) + 1, 1)
    return start, nxt - timedelta(days=1)


def _period_stats(receipts, start, end):
    # Receipts with no date on the receipt are left out of every period until a date is added;
    # otherwise they'd be counted in whichever week they were saved.
    inside = [r for r in receipts if r["dated"] and start <= r["date"] <= end]
    total = sum(r["total"] for r in inside)
    buckets = collections.defaultdict(float)
    shops = collections.defaultdict(float)
    for r in inside:
        buckets[r["bucket"]] += r["total"]
        shops[r["shop"]] += r["total"]
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "total": round(total, 2),
        "count": len(inside),
        "undated": sum(1 for r in receipts if not r["dated"]),
        "buckets": {k: round(v, 2) for k, v in sorted(buckets.items(), key=lambda x: -x[1])},
        "top_shops": [{"shop": s, "total": round(a, 2)}
                      for s, a in sorted(shops.items(), key=lambda x: -x[1])[:3]],
    }


def _actions(stats: dict, prev_total: float | None) -> list[str]:
    """Up to three things the user can do, based on this period's numbers."""
    acts = []
    buckets = stats["buckets"]
    total = stats["total"]
    if total <= 0:
        return ["Log a receipt to start tracking. Send a photo or forward an email receipt."]

    eating = buckets.get(EATING_OUT, 0)
    if eating >= 50:
        acts.append(f"Eating out and coffee came to £{eating:,.2f}. Set a limit of £{round(eating * 0.8 / 5) * 5:,.0f} for next period.")

    fuel = buckets.get(FUEL, 0)
    if fuel >= 40:
        acts.append(f"Fuel and transport: £{fuel:,.2f}. Check today's cheapest station near you before you fill up.")

    groceries_share = buckets.get("Groceries", 0) / total if total else 0
    if groceries_share >= 0.7:
        shops = [s["shop"] for s in stats["top_shops"]]
        if any("Costco" in s for s in shops):
            acts.append("Most of your spend is groceries and Costco is a top shop. Check the £65 membership is still paying for itself.")
        else:
            acts.append("Groceries are most of your spend. Compare your top two supermarkets for the same basket.")

    if prev_total and prev_total > 0:
        change = (total - prev_total) / prev_total
        if change >= 0.2 and len(acts) < 3:
            acts.append(f"Up {change * 100:.0f}% on the last period. Look at the top shop first.")
        elif change <= -0.2 and len(acts) < 3:
            acts.append(f"Down {abs(change) * 100:.0f}% on the last period. Keep it going.")

    if stats["undated"] and len(acts) < 3:
        acts.append(f"{stats['undated']} receipt(s) have no date. Add dates so they count in the right period.")
    return acts[:3]


def build_summary(lib, from_number: str, period: str, today: date | None = None, month: str | None = None) -> dict:
    """
    period: "week" (last 7 days) or "month" (a calendar month, default this month).
    Returns totals, categories, top shops, comparison with the previous period, and actions.
    """
    today = today or date.today()
    receipts = load_receipts(lib, from_number)

    if period == "week":
        start, end, p_start, p_end = week_bounds(today)
        label = f"Your week: {start:%d %b} to {end:%d %b %Y}"
    elif period == "month":
        if month:
            y, m = map(int, month.split("-"))
        else:
            y, m = today.year, today.month
        start, end = month_bounds(y, m)
        p_start, p_end = month_bounds(y - (m == 1), 12 if m == 1 else m - 1)
        label = f"Your month: {start:%B %Y}"
        if (y, m) == (today.year, today.month):
            # Month still in progress: compare the same number of days from last month
            end = today
            label += " (so far)"
            p_end = min(p_end, p_start + timedelta(days=(today - start).days))
    else:
        raise ValueError("period must be 'week' or 'month'")

    stats = _period_stats(receipts, start, end)
    prev = _period_stats(receipts, p_start, p_end)
    prev_total = prev["total"] if prev["count"] else None
    change_pct = round((stats["total"] - prev_total) / prev_total * 100) if prev_total else None
    actions = _actions(stats, prev_total)

    stats.update({
        "period": period,
        "label": label,
        "previous_total": prev_total,
        "change_pct": change_pct,
        "actions": actions,
    })
    stats["text"] = format_text(stats)
    return stats


def digest_text(lib, from_number: str, today: date | None = None) -> str:
    """
    Text to append to the weekly WhatsApp digest: last 7 days, plus last month in the
    first week of the month. Returns "" if there are no receipts to report.
    """
    today = today or date.today()
    parts = []
    week = build_summary(lib, from_number, "week", today=today)
    if week["count"]:
        parts.append(week["text"])
    if today.day <= 7:
        last_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        month = build_summary(lib, from_number, "month", today=today, month=last_month)
        if month["count"]:
            parts.append(month["text"])
    return "\n\n".join(parts)


def format_text(s: dict) -> str:
    """WhatsApp-friendly text."""
    lines = [f"🧾 {s['label']}", f"Total logged: £{s['total']:,.2f} across {s['count']} receipts"]
    if s["change_pct"] is not None:
        arrow = "Up" if s["change_pct"] >= 0 else "Down"
        lines.append(f"{arrow} {abs(s['change_pct'])}% on the previous {s['period']} (£{s['previous_total']:,.2f})")
    if s["buckets"]:
        lines += ["", "Where it went:"]
        for name, amt in s["buckets"].items():
            share = amt / s["total"] * 100 if s["total"] else 0
            lines.append(f"• {name}: £{amt:,.2f} ({share:.0f}%)")
    if s["top_shops"]:
        lines += ["", "Top shops: " + ", ".join(f"{t['shop']} £{t['total']:,.2f}" for t in s["top_shops"])]
    if s["actions"]:
        lines += ["", "Try this:"] + [f"• {a}" for a in s["actions"]]
    return "\n".join(lines)
