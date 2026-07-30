"""
Miru Intelligence Engine - Solid, Thorough User Insights

Generates comprehensive spending analysis, patterns, and actionable insights
using consistent global constants and UK-centric logic.
"""

import json
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
from collections import Counter, defaultdict
import logging

from constants import (
    TIMEZONE, CURRENCY_SYMBOL, DATE_FORMAT_DISPLAY,
    SPEND_CATEGORIES, now_london, today_london,
    DAILY_SPEND_WARNING, WEEKLY_SPEND_WARNING, CAFE_VISIT_FREQUENCY,
    MAX_RECENT_ITEMS, MAX_TOP_ITEMS, DECIMAL_PLACES_CURRENCY
)

logger = logging.getLogger(__name__)


def _receipt_category(merchant: str, summary: str = "") -> str:
    """Categorize merchant into spend category. Checks merchant name AND receipt items."""
    m = (merchant or "").lower().strip()
    s = (summary or "").lower().strip()
    if not m:
        return "Other"

    # IMPORTANT: Check specific fuel before generic grocery stores!
    # (so "Tesco Petrol" doesn't get caught by "tesco" → Groceries)

    # Parking
    if any(k in m for k in ["parking", "ncp", "q-park", "apcoa", "justpark", "ringgo"]):
        return "Parking"

    # Fuel — MUST BE BEFORE generic store checks
    if any(k in m for k in ["petrol", "fuel", "shell", "bp", "esso", "tesco petrol", "asda fuel", "filling station"]):
        return "Fuel"

    # Groceries (specific supermarket chains) — AFTER fuel to avoid false positives
    if any(k in m for k in ["tesco", "sainsbury", "asda", "morrisons", "waitrose", "aldi", "lidl", "co-op"]):
        return "Groceries"

    # Coffee & Lunch (specific chains)
    if any(k in m for k in ["pret", "starbucks", "costa", "greggs", "leon", "itsu"]):
        return "Coffee & Lunch"

    # Dining (restaurants, pubs, Italian, French, etc)
    dining_keywords = ["nando", "wagamama", "pizza express", "zizzi", "restaurant", "pub ", "cafe", "bistro", "trattoria"]
    if any(k in m for k in dining_keywords):
        return "Dining"

    # Takeaway
    if any(k in m for k in ["takeaway", "curry", "indian", "chinese", "thai"]):
        return "Takeaway"

    # Transport
    if any(k in m for k in ["national express", "stagecoach", "uber", "taxi", "train", "railway"]):
        return "Transport"

    # Shopping
    if any(k in m for k in ["amazon", "ebay", "john lewis", "next", "next plc"]):
        return "Shopping"

    # Entertainment
    if any(k in m for k in ["cinema", "netflix", "spotify", "ticket", "concert"]):
        return "Entertainment"

    # Health
    if any(k in m for k in ["boots", "pharmacy", "doctor", "gp", "dentist", "gym"]):
        return "Health"

    # Check receipt items for dining indicators (pasta, wine, appetizers, desserts, main courses)
    if s:
        dining_items = ["pasta", "penne", "arrabbiata", "risotto", "pizza", "steak", "salmon", "lamb", "wine", "cocktail", "mocktail", "appetizer", "starter", "main course", "tiramisu", "dessert", "pint", "lager", "ale", "beer"]
        if any(item in s for item in dining_items):
            return "Dining"

        # Grocery items
        grocery_items = ["milk", "bread", "eggs", "butter", "cheese", "vegetables", "fruit", "cereal", "rice", "pasta boxes"]
        if any(item in s for item in grocery_items):
            return "Groceries"

    return "Other"


class MiruIntelligence:
    """Main intelligence engine for comprehensive spending insights."""

    def __init__(self, sb=None):
        """Initialize with optional Supabase connection."""
        self.sb = sb
        self.logger = logging.getLogger(__name__)

    def get_full_intelligence(self, from_number: str, sb) -> Dict[str, Any]:
        """
        Main entry point: aggregate all user data and generate insights.

        Args:
            from_number: WhatsApp format phone (whatsapp:+447911...)
            sb: Supabase client instance

        Returns:
            {"success": True, "insights": {...}} or {"success": False, "error": "..."}
        """
        try:
            self.sb = sb
            phone = from_number.replace("whatsapp:", "").strip()

            if not self.sb:
                return {"success": False, "error": "No database connection"}

            # Calculate week boundaries (Mon-Sun in London time)
            today = today_london()
            days_since_monday = today.weekday()
            week_start = today - timedelta(days=days_since_monday)
            week_end = week_start + timedelta(days=6)

            # Fetch this week's spend data
            this_week_spend = self._analyze_spend(phone, week_start, week_end)
            last_week_spend = self._analyze_spend(phone, week_start - timedelta(days=7), week_start - timedelta(days=1))

            # Generate insights
            insights = self._generate_insights(this_week_spend, last_week_spend, phone)

            # Calculate contextual data for intelligent brief
            days_since_fuel = self._calculate_days_since_fuel(phone)

            return {
                "success": True,
                "insights": insights,
                "data": {
                    "this_week": this_week_spend,
                    "last_week": last_week_spend,
                },
                "data_summary": {
                    "days_since_fuel": days_since_fuel,
                },
                "timestamp": now_london().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"Intelligence generation failed: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _analyze_spend(self, phone: str, start_date: date, end_date: date) -> Dict[str, Any]:
        """Analyze spending for a date range."""
        try:
            # Query wa_saves receipts
            receipts = self.sb.table("wa_saves").select("summary,title,created_at") \
                .eq("from_number", f"whatsapp:{phone}") \
                .gte("created_at", start_date.isoformat()) \
                .lte("created_at", (end_date + timedelta(days=1)).isoformat()) \
                .execute().data or []

            # Filter to receipts only (title starts with 🧾)
            receipt_rows = [r for r in receipts if (r.get("title") or "").startswith("🧾")]

            spend_total = 0.0
            by_category = defaultdict(lambda: {"total": 0.0, "count": 0})
            by_merchant = defaultdict(lambda: {"total": 0.0, "count": 0})

            # Parse receipt amounts
            for r in receipt_rows:
                amount = self._extract_amount(r.get("summary", ""))
                if amount > 0:
                    merchant = (r.get("title") or "").replace("🧾", "").strip() or "Unknown"
                    summary = r.get("summary", "")
                    category = _receipt_category(merchant, summary)

                    spend_total += amount
                    by_category[category]["total"] += amount
                    by_category[category]["count"] += 1
                    by_merchant[merchant]["total"] += amount
                    by_merchant[merchant]["count"] += 1

            return {
                "period": f"{start_date.strftime('%d %b')} — {end_date.strftime('%d %b')}",
                "total": round(spend_total, DECIMAL_PLACES_CURRENCY),
                "transaction_count": len(receipt_rows),
                "by_category": dict(sorted(by_category.items(), key=lambda x: x[1]["total"], reverse=True)),
                "by_merchant": dict(sorted(by_merchant.items(), key=lambda x: x[1]["total"], reverse=True)[:MAX_TOP_ITEMS]),
            }

        except Exception as e:
            self.logger.error(f"Spend analysis failed: {e}")
            return {"period": "—", "total": 0, "transaction_count": 0, "by_category": {}, "by_merchant": {}}

    def _extract_amount(self, summary: str) -> float:
        """Extract £ amount from receipt summary."""
        import re
        patterns = [
            r'Total due:\s*£([\d,]+\.?\d*)',
            r'Total amount:\s*£([\d,]+\.?\d*)',
            r'Total:\s*£([\d,]+\.?\d*)',
            r'Total\s+£([\d,]+\.?\d*)',
        ]
        for pattern in patterns:
            m = re.search(pattern, summary)
            if m:
                try:
                    return float(m.group(1).replace(",", ""))
                except (ValueError, IndexError):
                    pass
        return 0.0

    def _calculate_days_since_fuel(self, phone: str) -> int:
        """Calculate days since last fuel purchase. Returns -1 if no fuel purchase found."""
        try:
            today = today_london()
            # Query receipts to find last fuel purchase
            receipts = self.sb.table("wa_saves").select("title,created_at,summary") \
                .eq("from_number", f"whatsapp:{phone}") \
                .order("created_at", desc=True) \
                .limit(100) \
                .execute().data or []

            for r in receipts:
                title = (r.get("title") or "").lower()
                summary = (r.get("summary") or "").lower()
                # Check if receipt is from fuel station
                if any(fuel_name in title + summary for fuel_name in ["shell", "bp", "esso", "tesco petrol", "asda fuel", "fuel", "petrol"]):
                    try:
                        receipt_date = date.fromisoformat(r.get("created_at", "")[:10])
                        days_since = (today - receipt_date).days
                        return days_since
                    except (ValueError, TypeError):
                        continue
            return -1  # No fuel purchase found
        except Exception as e:
            self.logger.error(f"Fuel calculation failed: {e}")
            return -1

    def _generate_insights(self, this_week: Dict, last_week: Dict, phone: str) -> str:
        """Generate actionable, human-readable insights."""
        insights = []

        # Spending change
        spend_diff = this_week["total"] - last_week["total"]
        if last_week["total"] > 0:
            pct_change = (spend_diff / last_week["total"]) * 100
        else:
            pct_change = 0

        if spend_diff < 0:
            insights.append(f"✅ Down {CURRENCY_SYMBOL}{abs(spend_diff):.2f} ({abs(int(pct_change))}%) vs last week")
        elif spend_diff > 0:
            insights.append(f"⚠️ Up {CURRENCY_SYMBOL}{spend_diff:.2f} (+{int(pct_change)}%) vs last week")

        # High spend warning
        if this_week["total"] > WEEKLY_SPEND_WARNING:
            insights.append(f"⚠️ Weekly spend {CURRENCY_SYMBOL}{this_week['total']:.2f} above {CURRENCY_SYMBOL}{WEEKLY_SPEND_WARNING}")

        # Top category
        if this_week["by_category"]:
            top_cat = sorted(this_week["by_category"].items(), key=lambda x: x[1]["total"], reverse=True)[0]
            cat_name, cat_data = top_cat
            icon = SPEND_CATEGORIES.get(cat_name, {}).get("icon", "💳")
            pct = (cat_data["total"] / this_week["total"] * 100) if this_week["total"] > 0 else 0
            insights.append(f"{icon} {cat_name}: {CURRENCY_SYMBOL}{cat_data['total']:.2f} ({int(pct)}% of spend)")

        # Top merchant
        if this_week["by_merchant"]:
            top_merchant = list(this_week["by_merchant"].items())[0]
            merchant_name, merchant_data = top_merchant
            insights.append(f"🏪 Most spent at: {merchant_name} ({merchant_data['count']} times)")

        return " • ".join(insights) if insights else "No spending data this week"
