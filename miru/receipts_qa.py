"""
Answer simple spending questions on WhatsApp from saved receipts.

Examples it understands:
  "how much did I spend on coffee in October"
  "how much on groceries this month"
  "what did I spend at Tesco last month"
  "how much did I spend on fuel"

Anything that isn't clearly a spending question returns None, so the normal WhatsApp
handling carries on. Read-only.
"""
from __future__ import annotations

import re
from datetime import date, timedelta

from miru.receipts_summary import BUCKETS, load_receipts, month_bounds, week_bounds

# Only questions that use a spend word, so ordinary "how much" questions (prices etc.) are not caught
QUESTION = re.compile(r"\b(how\s+much|what)\b.*\b(spend|spent|spending)\b", re.IGNORECASE)

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
MONTHS.update({"jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
               "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12})

# Words people use -> the bucket they mean
TOPIC_WORDS = {
    "groceries": "Groceries", "grocery": "Groceries", "food shop": "Groceries", "shopping": "Groceries",
    "fuel": "Fuel & transport", "petrol": "Fuel & transport", "diesel": "Fuel & transport", "transport": "Fuel & transport",
    "coffee": "Eating out & coffee", "takeaway": "Eating out & coffee", "eating out": "Eating out & coffee",
    "lunch": "Eating out & coffee", "restaurant": "Eating out & coffee",
}


def _parse_period(text: str, today: date):
    """Returns (start, end, label)."""
    t = text.lower()
    if "last week" in t or "this week" in t:
        if "last week" in t:
            end = today - timedelta(days=today.weekday() + 1)
            return end - timedelta(days=6), end, "last week"
        start, end, _, _ = week_bounds(today)
        return start, end, "the last 7 days"
    if "last month" in t:
        y, m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
        start, end = month_bounds(y, m)
        return start, end, start.strftime("%B %Y")
    for name, num in MONTHS.items():
        if re.search(rf"\b{name}\b", t):
            year_m = re.search(rf"\b{name}\b\s+(\d{{4}})", t)
            y = int(year_m.group(1)) if year_m else (today.year if num <= today.month else today.year - 1)
            start, end = month_bounds(y, num)
            if start > today:
                return None
            if (y, num) == (today.year, today.month):
                return start, today, start.strftime("%B %Y") + " so far"
            return start, end, start.strftime("%B %Y")
    start, end = month_bounds(today.year, today.month)
    return start, today, start.strftime("%B %Y") + " so far"


def _parse_filter(text: str, receipts: list[dict]):
    """Returns (predicate, label). Predicate is None for 'everything'."""
    t = text.lower()
    for word, bucket in TOPIC_WORDS.items():
        if re.search(rf"\b{re.escape(word)}\b", t):
            return (lambda r, b=bucket: r["bucket"] == b), word
    # Match a shop by its first word, so "Costco" finds "Costco Wholesale"
    firsts = {}
    for r in receipts:
        if r["shop"]:
            firsts.setdefault(r["shop"].split()[0].lower(), r["shop"].split()[0])
    for first_lower, first in sorted(firsts.items(), key=lambda x: -len(x[0])):
        if len(first_lower) >= 3 and re.search(rf"\b{re.escape(first_lower)}\b", t):
            return (lambda r, f=first_lower: r["shop"].split()[0].lower() == f), first
    return None, None


def answer_spend_question(lib, from_number: str, text: str, today: date | None = None) -> str | None:
    if not text or not QUESTION.search(text):
        return None
    today = today or date.today()
    receipts = load_receipts(lib, from_number)
    dated = [r for r in receipts if r["dated"]]

    period = _parse_period(text, today)
    if period is None:
        return "That month hasn't happened yet. Try a month that has passed."
    start, end, label = period
    pred, what = _parse_filter(text, dated)

    inside = [r for r in dated if start <= r["date"] <= end and (pred is None or pred(r))]
    if not inside:
        target = f" on {what}" if what else ""
        return f"No receipts logged{target} for {label} yet."

    total = sum(r["total"] for r in inside)
    target = f" on {what}" if what else ""
    lines = [f"You spent £{total:,.2f}{target} in {label}, across {len(inside)} receipt{'s' if len(inside) != 1 else ''}."]
    if pred is None:
        shops = collections_top(inside)
        if shops:
            lines.append("Top shops: " + ", ".join(f"{s} £{a:,.2f}" for s, a in shops))
    undated = sum(1 for r in receipts if not r["dated"])
    if undated:
        lines.append(f"{undated} receipt(s) have no date, so they aren't included.")
    return "\n".join(lines)


def collections_top(rows: list[dict], n: int = 3):
    totals = {}
    for r in rows:
        totals[r["shop"]] = totals.get(r["shop"], 0) + r["total"]
    return [(s, round(a, 2)) for s, a in sorted(totals.items(), key=lambda x: -x[1])[:n]]
