"""
Auto-link spend to family goals.
When a receipt is logged, update matching household goals.

Called from receipt handlers:
  link_receipt_to_goals(from_number, amount_pence, category)
"""

import library as lib
from datetime import date, timedelta
import re


def link_receipt_to_goals(from_number: str, amount_pence: int, category: str = None):
    """
    When a user logs a receipt, find and update matching household goals.

    Called after wa_saves.insert() with receipt data.

    Args:
        from_number: WhatsApp number (e.g. 'whatsapp:+44...')
        amount_pence: Receipt amount in pence
        category: Merchant category (e.g., 'Coffee', 'Groceries', 'Takeaway')
    """
    try:
        if not amount_pence or amount_pence <= 0:
            return  # Ignore zero/negative amounts

        # Get household ID from user's phone
        household_id = from_number.replace("whatsapp:", "").replace("+", "")

        # Fetch all ACTIVE goals for this household in current week
        today = date.today()
        week_start = (today - timedelta(days=today.weekday())).isoformat()
        week_end = today.isoformat()

        goals = lib._sb().table("family_goals").select("*") \
            .eq("household_id", household_id) \
            .eq("status", "active") \
            .gte("period_start", week_start) \
            .lte("period_end", week_end) \
            .execute().data or []

        if not goals:
            return  # No active goals this week

        # Match goals to receipt category
        # Simple matching: if goal title contains category keyword, it's a match
        matched_goals = []

        for goal in goals:
            title_lower = (goal.get("title") or "").lower()
            category_lower = (category or "").lower()

            # Direct category match (e.g., "Coffee" in "set goal Coffee under £30")
            if category_lower and category_lower in title_lower:
                matched_goals.append(goal)
            # Keyword matching (e.g., "takeaway" in goal title, receipt is "Deliveroo")
            elif _category_matches(title_lower, category_lower):
                matched_goals.append(goal)

        # For each matched goal, update its progress
        for goal in matched_goals:
            goal_id = goal.get("id")
            current_value = goal.get("current_value", 0) or 0
            new_value = current_value + amount_pence

            try:
                # Update goal's current_value
                lib._sb().table("family_goals").update({
                    "current_value": new_value,
                    "updated_at": "now()"
                }).eq("id", goal_id).execute()

                # Log progress entry
                lib._sb().table("goal_progress").insert({
                    "goal_id": goal_id,
                    "value": new_value,
                    "source": "spend_log"
                }).execute()

                print(f"[goals] Linked receipt £{amount_pence/100:.2f} ({category}) to goal {goal_id}")

            except Exception as log_err:
                print(f"[goals] Error updating goal {goal_id}: {log_err}")

    except Exception as e:
        print(f"[goals] link_receipt_to_goals error: {e}")


def _category_matches(goal_title: str, receipt_category: str) -> bool:
    """
    Smart category matching for goals.
    Handles synonyms and related categories.
    """
    # Map of category aliases
    aliases = {
        "coffee": ["coffee", "cafe", "café", "caffeine", "drink", "beverage"],
        "takeaway": ["takeaway", "takeout", "delivery", "deliveroo", "uber eats", "just eat", "food delivery", "restaurant", "kebab", "pizza", "burger"],
        "groceries": ["groceries", "grocery", "tesco", "asda", "sainsbury", "sainsburys", "morrisons", "waitrose", "ocado", "food shop"],
        "fuel": ["fuel", "petrol", "diesel", "gas", "bp", "shell", "tesco fuel", "sainsbury fuel"],
        "entertainment": ["cinema", "movie", "theater", "theatre", "concert", "show", "entertainment"],
        "transport": ["transport", "uber", "taxi", "train", "bus", "parking", "petrol"],
    }

    # Check if receipt category matches any alias in goal title
    for category_group, keywords in aliases.items():
        if receipt_category in keywords:
            # Check if any keyword from this group is in goal title
            if any(kw in goal_title for kw in keywords):
                return True

    return False
