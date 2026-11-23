"""
Weekly and monthly spend summaries built from saved receipts (wa_saves, 🧾 rows).

Used by:
- the weekly WhatsApp digest (appended to the weekly savings message)
- the web app (GET /api/v2/receipts/summary, and the Receipts screen card)
- scripts/receipt_summary.py (preview in the terminal)

Budgets are stored per user in ma_details (type "receipt_budgets"), so no schema change is needed.
Read-only except set_budget().
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
BUDGET_ALERT_PCT = 80


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


# ── Budgets ───────────────────────────────────────────────────────────────

def get_budgets(lib, from_number: str) -> dict:
    """{bucket: monthly limit in £}. Empty if none set."""
    rows = (lib._sb().table("ma_details").select("data")
            .eq("device_id", from_number).eq("type", "receipt_budgets")
            .limit(1).execute().data or [])
    return dict(rows[0]["data"]) if rows and isinstance(rows[0].get("data"), dict) else {}


def set_budget(lib, from_number: str, bucket: str, amount: float) -> dict:
    """Set or clear (amount <= 0) one category's monthly budget."""
    if bucket not in set(BUCKETS.values()) | {"Other"}:
        raise ValueError(f"unknown bucket: {bucket}")
    budgets = get_budgets(lib, from_number)
    if amount and amount > 0:
        budgets[bucket] = round(float(amount), 2)
    else:
        budgets.pop(bucket, None)
    existing = (lib._sb().table("ma_details").select("device_id")
                .eq("device_id", from_number).eq("type", "receipt_budgets")
                .limit(1).execute().data or [])
    if existing:
        lib._sb().table("ma_details").update({"data": budgets}) \
            .eq("device_id", from_number).eq("type", "receipt_budgets").execute()
    else:
        lib._sb().table("ma_details").insert({
            "device_id": from_number, "type": "receipt_budgets", "data": budgets,
        }).execute()
    return budgets


# ── Summaries ─────────────────────────────────────────────────────────────

def week_bounds(today: date):
    """Last 7 days ending today, and the 7 days before that."""
    this_start = today - timedelta(days=6)
    prev_end = this_start - timedelta(days=1)
    return this_start, today, prev_end - timedelta(days=6), prev_end


def month_bounds(year: int, month: int):
    start = date(year, month, 1)
    nxt = date(year + (month == 12), (month % 12) + 1, 1)
    return start, nxt - timedelta(days=1)


def find_duplicates(inside: list[dict]) -> int:
    """Extra copies of the same receipt: same shop, date and total."""
    seen = collections.Counter((r["shop"], r["date"], round(r["total"], 2)) for r in inside)
    return sum(n - 1 for n in seen.values() if n > 1)


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
        "duplicates": find_duplicates(inside),
        "buckets": {k: round(v, 2) for k, v in sorted(buckets.items(), key=lambda x: -x[1])},
        "top_shops": [{"shop": s, "total": round(a, 2)}
                      for s, a in sorted(shops.items(), key=lambda x: -x[1])[:3]],
    }


def _budget_status(stats: dict, budgets: dict, scale: float) -> list[dict]:
    """
    scale: fraction of the month this period covers (1.0 for a full month).
    For a week, budgets are pro-rated, so a week's limit is budget * 7 / days in month.
    """
    out = []
    for bucket, limit in budgets.items():
        spent = stats["buckets"].get(bucket, 0.0)
        allowed = round(limit * scale, 2)
        pct = round(spent / allowed * 100) if allowed else 0
        out.append({"bucket": bucket, "spent": spent, "limit": allowed, "pct": pct})
    return out


def _actions(stats: dict, prev_total: float | None, budget_rows: list[dict]) -> list[str]:
    """Up to three things the user can do, based on this period's numbers."""
    acts = []
    buckets = stats["buckets"]
    total = stats["total"]
    if total <= 0 and not stats["undated"]:
        return ["Log a receipt to start tracking. Send a photo or forward an email receipt."]

    for row in sorted(budget_rows, key=lambda r: -r["pct"]):
        if row["pct"] >= BUDGET_ALERT_PCT:
            acts.append(f"{row['bucket']} is at {row['pct']}% of its £{row['limit']:,.0f} budget "
                        f"(£{row['spent']:,.2f} spent).")
            break

    eating = buckets.get(EATING_OUT, 0)
    if eating >= 50 and not any(a.startswith(EATING_OUT) for a in acts):
        acts.append(f"Eating out and coffee came to £{eating:,.2f}. Set a limit of £{round(eating * 0.8 / 5) * 5:,.0f} for next period.")

    fuel = buckets.get(FUEL, 0)
    if fuel >= 40:
        acts.append(f"Fuel and transport: £{fuel:,.2f}. Check today's cheapest station near you before you fill up.")

    if stats["duplicates"]:
        acts.append(f"{stats['duplicates']} receipt(s) look like duplicates (same shop, date and total). Check them in Receipts.")

    groceries_share = buckets.get("Groceries", 0) / total if total else 0
    if groceries_share >= 0.7 and len(acts) < 3:
        shops = [s["shop"] for s in stats["top_shops"]]
        if any("Costco" in s for s in shops):
            acts.append("Most of your spend is groceries and Costco is a top shop. Check the £65 membership is still paying for itself.")
        else:
            acts.append("Groceries are most of your spend. Compare your top two supermarkets for the same basket.")

    if prev_total and prev_total > 0 and total:
        change = (total - prev_total) / prev_total
        if change >= 0.2 and len(acts) < 3:
            acts.append(f"Up {change * 100:.0f}% on the last period. Look at the top shop first.")
        elif change <= -0.2 and len(acts) < 3:
            acts.append(f"Down {abs(change) * 100:.0f}% on the last period. Keep it going.")

    if stats["undated"] and len(acts) < 3:
        acts.append(f"{stats['undated']} receipt(s) have no date, so they're not counted yet. Add dates to include them.")
    return acts[:3]


def build_summary(lib, from_number: str, period: str, today: date | None = None, month: str | None = None) -> dict:
    """
    period: "week" (last 7 days) or "month" (a calendar month, default this month).
    Returns totals, categories, top shops, comparison with the previous period, budgets and actions.
    """
    today = today or date.today()
    receipts = load_receipts(lib, from_number)
    budgets = get_budgets(lib, from_number)

    if period == "week":
        start, end, p_start, p_end = week_bounds(today)
        label = f"Your week: {start:%d %b} to {end:%d %b %Y}"
        days_in_month = month_bounds(end.year, end.month)[1].day
        scale = 7 / days_in_month
    elif period == "month":
        if month:
            y, m = map(int, month.split("-"))
        else:
            y, m = today.year, today.month
        start, end = month_bounds(y, m)
        p_start, p_end = month_bounds(y - (m == 1), 12 if m == 1 else m - 1)
        label = f"Your month: {start:%B %Y}"
        scale = 1.0
        if (y, m) == (today.year, today.month):
            # Month still in progress: compare the same number of days from last month
            end = today
            label += " (so far)"
            p_end = min(p_end, p_start + timedelta(days=(today - start).days))
            scale = (today - start).days + 1
            scale = scale / month_bounds(y, m)[1].day
    else:
        raise ValueError("period must be 'week' or 'month'")

    stats = _period_stats(receipts, start, end)
    prev = _period_stats(receipts, p_start, p_end)
    prev_total = prev["total"] if prev["count"] else None
    change_pct = round((stats["total"] - prev_total) / prev_total * 100) if prev_total else None
    budget_rows = _budget_status(stats, budgets, scale) if budgets else []
    actions = _actions(stats, prev_total, budget_rows)

    stats.update({
        "period": period,
        "label": label,
        "previous_total": prev_total,
        "change_pct": change_pct,
        "budgets": budget_rows,
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
