"""
WhatsApp handlers for family goals (Phase 2).
Called from sms_service._whatsapp_reply_inner() for keywords: set goal, goals, add member.
"""

import library as lib
from datetime import datetime, date, timedelta
import json


def handle_set_family_goal(from_number: str, body: str) -> str:
    """
    Handle "set goal" keyword to create a new family goal.
    Examples:
      "set goal Takeaways under £100"
      "set goal Coffee under £30"
      "set goal Save £50 on fuel"
    """
    try:
        # Extract goal description from "set goal X"
        lower = body.lower().strip()
        if lower.startswith("set goal "):
            goal_desc = body[9:].strip()  # Remove "set goal "
        else:
            return "📋 Usage: *set goal Takeaways under £100*\n\nTell me your goal and I'll track it for your household."

        # Determine goal type and target from description
        # Simple heuristic: look for "under £X" or "over £X" patterns
        import re

        # Match currency pattern: "under £100" or "save £50"
        gbp_match = re.search(r'(?:under|over|save|spend)\s*£([\d.]+)', goal_desc, re.IGNORECASE)
        if not gbp_match:
            return f"📋 I need a target amount, like: *set goal Takeaways under £100*"

        target_value = float(gbp_match.group(1))
        target_pence = int(target_value * 100)
        goal_type = "spend_reduction"  # For Phase 2, only spend goals

        # Get user's household ID (use phone number as household ID if new)
        # This links all family members under one household
        household_id = from_number.replace("whatsapp:", "").replace("+", "")

        try:
            # Check if household exists
            hh = lib._sb().table("household_members").select("household_id") \
                .eq("household_id", household_id) \
                .maybe_single().execute()

            if not hh or not hh.data:
                # Create household entry for this user as admin
                lib._sb().table("household_members").insert({
                    "household_id": household_id,
                    "wa": from_number,
                    "display_name": "You",
                    "role": "admin"
                }).execute()
        except Exception as e:
            pass  # Household might already exist

        # Calculate period: this week (Mon-Sun)
        today = date.today()
        period_start = today - timedelta(days=today.weekday())  # Monday
        period_end = period_start + timedelta(days=6)  # Sunday

        # Create goal
        goal = lib._sb().table("family_goals").insert({
            "household_id": household_id,
            "goal_type": goal_type,
            "title": goal_desc,
            "target_value": target_pence,
            "target_unit": "gbp",
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "created_by_wa": from_number,
            "status": "active"
        }).execute()

        # Get the inserted goal ID
        goal_id = goal.data[0]['id'] if goal.data else "unknown"

        return f"""🎯 *Goal Set!*

{goal_desc}
Period: {period_start.strftime('%d/%m/%y')} – {period_end.strftime('%d/%m/%y')}
Target: £{target_value:.2f}

I'll track progress and let you know how close you are. 📊

Reply *goals* to see all your family goals."""

    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"❌ Couldn't set goal: {e}"


def handle_list_goals(from_number: str) -> str:
    """
    Handle "goals" keyword to list all active goals for the household.
    Shows: goal title, progress, time remaining, status.
    """
    try:
        # Get household ID from user's phone
        household_id = from_number.replace("whatsapp:", "").replace("+", "")

        # Fetch all active goals for this household
        goals = lib._sb().table("family_goals").select("*") \
            .eq("household_id", household_id) \
            .eq("status", "active") \
            .order("created_at", desc=False) \
            .execute().data or []

        if not goals:
            return "📋 No active family goals yet.\n\nReply *set goal Takeaways under £100* to create one."

        # Build message with all goals
        msg = "🎯 *YOUR FAMILY GOALS*\n\n"

        for i, goal in enumerate(goals, 1):
            title = goal.get("title", "")
            target = int(goal.get("target_value", 0)) / 100
            current = int(goal.get("current_value", 0)) / 100
            period_end = goal.get("period_end", "")

            # Calculate progress percentage
            progress_pct = 0
            if target > 0:
                progress_pct = min(100, int(current / target * 100))

            # Progress bar
            bar_filled = int(progress_pct / 10)
            bar = "█" * bar_filled + "░" * (10 - bar_filled)

            # Remaining amount
            remaining = max(0, target - current)

            msg += f"{i}. {title}\n"
            msg += f"   Target: £{target:.2f} | Current: £{current:.2f}\n"
            msg += f"   [{bar}] {progress_pct}%\n"
            msg += f"   Remaining: £{remaining:.2f}\n\n"

        msg += "Reply *set goal X under £Y* to add another."

        return msg.strip()

    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"❌ Error fetching goals: {e}"


def handle_add_household_member(from_number: str, body: str) -> str:
    """
    Handle "add member" keyword to link another family member to this household.
    Example: "add member +44712345678 Riaan"
    """
    try:
        # Extract phone and name from "add member +44712345678 Riaan"
        import re

        match = re.search(r'add\s+member\s+(\+?[\d\s]+)\s+(.+)', body, re.IGNORECASE)
        if not match:
            return "📱 Usage: *add member +44712345678 Riaan*\n\nThis links them to your family goals."

        wa_raw = match.group(1).strip().replace(" ", "")
        if not wa_raw.startswith("+"):
            wa_raw = "+" + wa_raw

        display_name = match.group(2).strip()

        # Format as WhatsApp number
        member_wa = f"whatsapp:{wa_raw}"

        # Get household ID
        household_id = from_number.replace("whatsapp:", "").replace("+", "")

        # Check if already a member
        existing = lib._sb().table("household_members").select("wa") \
            .eq("household_id", household_id) \
            .eq("wa", member_wa) \
            .maybe_single().execute()

        if existing and existing.data:
            return f"👤 {display_name} is already in your household."

        # Add member
        lib._sb().table("household_members").insert({
            "household_id": household_id,
            "wa": member_wa,
            "display_name": display_name,
            "role": "member"
        }).execute()

        return f"✅ *{display_name} added to household!*\n\nTey can now see and contribute to your family goals. 👨‍👩‍👧"

    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"❌ Error adding member: {e}"
