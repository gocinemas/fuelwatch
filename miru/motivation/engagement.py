"""
Engagement loops (Phase 2, Aug 2026): weekly wins digest data, goal progress,
social proof, and achievement badges.

Builds entirely on existing motivation-layer tables — savings_events,
motivation_prefs, fuel_alerts (Phase 3), family_goals/goal_progress (Phase 4) —
plus two new ones added in migrations/phase6_engagement_badges.sql:
  - weekly_win_events  (one row per user per week; written by the Sunday cron,
                         gives the streak counter and days-on-budget durable history)
  - user_achievements  (unlocked badges; reconciled on every read)

No parallel "user_goals" table — family_goals already models a goal exactly as
described (target/current/period/status), keyed by household_id. For a solo
user (not in an explicit household), household_id is just their own normalized
WhatsApp number, same convention miru/goals/handlers.py already uses.
"""

import re
from datetime import date, timedelta

import library as lib

_DEFAULT_DAILY_TARGET_PENCE = 3000  # £30 — mirrors sms_service._SPEND_ALERT_DEFAULT_DAILY_PENCE

ACHIEVEMENTS = {
    "first_week":    {"label": "First Week",    "emoji": "🌱", "desc": "Logged your first week with Miru"},
    "budget_master":  {"label": "Budget Master", "emoji": "🏆", "desc": "5-week on-budget streak"},
    "goal_getter":    {"label": "Goal Getter",   "emoji": "🎯", "desc": "Hit a savings goal"},
    "big_saver":      {"label": "Big Saver",     "emoji": "💰", "desc": "£100+ saved with Miru"},
    "fuel_saver":     {"label": "Fuel Saver",    "emoji": "⛽", "desc": "£20+ saved via fuel alerts"},
}


def _household_id(wa: str) -> str:
    """Same normalization miru/goals/handlers.py uses to key family_goals."""
    return (wa or "").replace("whatsapp:", "").replace("+", "")


def resolve_wa(token: str) -> str:
    """
    Accept either a raw phone number (+44... or 44...) or an already-prefixed
    'whatsapp:+44...' and return the canonical 'whatsapp:+44...' form that
    wa_saves / savings_events / motivation_prefs / fuel_alerts are all keyed
    by. Mirrors sms_service._v2_resolve's phone-normalization branch — kept
    as a local copy (rather than imported) to avoid a circular import, since
    sms_service imports this module's endpoints at startup.
    """
    token = (token or "").strip()
    if not token:
        return ""
    if token.startswith("whatsapp:"):
        return token
    if token.startswith("+") or (token.isdigit() and len(token) >= 10):
        if not token.startswith("+"):
            token = "+" + token
        return "whatsapp:" + token
    return ""  # HMAC saves-tokens aren't resolvable here without sms_service's user-token lookup


def _daily_breakdown(wa: str, week_start_iso: str, week_end_iso: str) -> dict:
    """{'YYYY-MM-DD': pounds_spent} for this user's receipts this week, deduped."""
    try:
        rows = lib._sb().table("wa_saves").select("summary,title,created_at") \
            .eq("from_number", wa) \
            .gte("created_at", week_start_iso).lt("created_at", week_end_iso) \
            .ilike("title", "🧾%").execute().data or []
    except Exception:
        return {}

    seen = set()
    daily = {}
    for r in rows:
        created = r.get("created_at", "")
        date_key = created[:10]
        m = re.search(r'£([\d,]+\.?\d*)', (r.get("summary") or "") + (r.get("title") or ""))
        if not m:
            continue
        try:
            amount = float(m.group(1).replace(",", ""))
        except ValueError:
            continue
        merchant = (r.get("title") or "").replace("🧾", "").strip()
        if merchant.startswith("Online:"):
            continue
        key = (date_key, round(amount, 2))
        if key in seen:
            continue
        seen.add(key)
        daily[date_key] = daily.get(date_key, 0.0) + amount
    return daily


def compute_days_on_budget(wa: str, week_start_iso: str, week_end_iso: str):
    """Returns (days_on_budget, days_elapsed, daily_breakdown_dict)."""
    if not week_start_iso:
        return 0, 0, {}

    daily_target_pence = _DEFAULT_DAILY_TARGET_PENCE
    try:
        row = lib._sb().table("motivation_prefs").select("daily_target_pence,weekly_target_pence") \
            .eq("wa", wa).maybe_single().execute()
        prefs = (row.data if row else {}) or {}
        if prefs.get("daily_target_pence"):
            daily_target_pence = int(prefs["daily_target_pence"])
        elif prefs.get("weekly_target_pence"):
            daily_target_pence = int(prefs["weekly_target_pence"]) // 7
    except Exception:
        pass

    daily = _daily_breakdown(wa, week_start_iso, week_end_iso)

    start = date.fromisoformat(week_start_iso)
    end_cap = min(date.fromisoformat(week_end_iso), date.today())
    days_elapsed = max(1, (end_cap - start).days + 1)

    on_budget_days = 0
    for i in range(days_elapsed):
        d = (start + timedelta(days=i)).isoformat()
        if int(daily.get(d, 0.0) * 100) <= daily_target_pence:
            on_budget_days += 1

    return on_budget_days, days_elapsed, daily


def _pick_tip(daily: dict, top_category: str) -> str:
    if daily:
        worst_day, worst_amt = max(daily.items(), key=lambda kv: kv[1])
        if worst_amt > 0:
            weekday = date.fromisoformat(worst_day).strftime("%A")
            return f"Your {weekday} spend (£{worst_amt:.2f}) was your highest this week. Worth a look?"
    if top_category and top_category != "Other":
        return f"{top_category} was your biggest category this week — worth keeping an eye on."
    return "Keep logging receipts on WhatsApp to build your weekly picture."


def build_weekly_wins(wa: str) -> dict:
    """Digest data for the web card and the Sunday WhatsApp message."""
    from weekly_savings_summary import get_weekly_savings

    savings = get_weekly_savings(wa)
    if savings.get("status") == "error":
        return {"status": "error"}

    week_start = savings.get("period_start_date")
    week_end = savings.get("period_end_date")
    days_on_budget, days_elapsed, daily = compute_days_on_budget(wa, week_start, week_end)

    streak = 0
    try:
        prev = lib._sb().table("weekly_win_events").select("streak_count") \
            .eq("wa", wa).order("period_start", desc=True).limit(1) \
            .execute().data or []
        if prev:
            streak = int(prev[0].get("streak_count") or 0)
    except Exception:
        pass

    variance = savings.get("week_variance_pence", 0)

    return {
        "status": "ok" if savings.get("status") == "ok" else "no_data",
        "spent_pence": savings.get("total_spent_pence", 0),
        "last_week_pence": savings.get("last_week_pence", 0),
        "saved_vs_last_week_pence": max(0, -variance),
        "overspent_vs_last_week_pence": max(0, variance),
        "days_on_budget": days_on_budget,
        "days_elapsed": days_elapsed,
        "fuel_saved_pence": savings.get("fuel_saved_pence", 0),
        "receipt_count": savings.get("receipt_count", 0),
        "streak_weeks": streak,
        "tip": _pick_tip(daily, savings.get("top_category")),
        "period_start": week_start,
        "period_end": week_end,
    }


def record_weekly_win_event(wa: str, savings: dict) -> dict:
    """
    Called by the Sunday digest cron only (one authoritative write per user
    per week). Computes on_budget + streak from the previous row and upserts
    into weekly_win_events. Returns the row written.
    """
    week_start = savings.get("period_start_date")
    week_end = savings.get("period_end_date")
    if not week_start:
        return {}

    days_on_budget, _, _ = compute_days_on_budget(wa, week_start, week_end)
    variance = savings.get("week_variance_pence", 0)

    on_budget = variance <= 0
    try:
        prefs = lib._sb().table("motivation_prefs").select("weekly_target_pence") \
            .eq("wa", wa).maybe_single().execute()
        target = ((prefs.data if prefs else {}) or {}).get("weekly_target_pence")
        if target:
            on_budget = savings.get("total_spent_pence", 0) <= target
    except Exception:
        pass

    prev_streak = 0
    try:
        prev = lib._sb().table("weekly_win_events").select("streak_count,period_start") \
            .eq("wa", wa).lt("period_start", week_start) \
            .order("period_start", desc=True).limit(1).execute().data or []
        if prev:
            prev_streak = int(prev[0].get("streak_count") or 0)
    except Exception:
        pass

    new_streak = prev_streak + 1 if on_budget else 0

    row = {
        "wa": wa,
        "period_start": week_start,
        "period_end": week_end,
        "spend_pence": savings.get("total_spent_pence", 0),
        "savings_amount_pence": max(0, -variance),
        "on_budget": on_budget,
        "days_on_budget": days_on_budget,
        "streak_count": new_streak,
    }
    try:
        lib._sb().table("weekly_win_events").upsert(row, on_conflict="wa,period_start").execute()
    except Exception as e:
        print(f"[engagement] record_weekly_win_event failed for {wa}: {e}")
    return row


def build_goals_progress(wa: str) -> dict:
    household_id = _household_id(wa)
    try:
        goals = lib._sb().table("family_goals").select("*") \
            .eq("household_id", household_id).eq("status", "active") \
            .order("created_at", desc=False).execute().data or []
    except Exception as e:
        return {"status": "error", "goals": [], "error": str(e)}

    out = []
    for g in goals:
        target = float(g.get("target_value") or 0)
        current = float(g.get("current_value") or 0)
        pct = min(100, int(current / target * 100)) if target > 0 else 0
        out.append({
            "id": g.get("id"),
            "title": g.get("title"),
            "goal_type": g.get("goal_type"),
            "target_value": target,
            "current_value": current,
            "target_unit": g.get("target_unit", "gbp"),
            "progress_percent": pct,
            "remaining": max(0, target - current),
            "period_start": g.get("period_start"),
            "period_end": g.get("period_end"),
        })
    return {"status": "ok", "goals": out}


def build_social_proof(wa: str) -> dict:
    """
    Percentile vs. every other user's spend this week (anonymized — only
    counts and an aggregate average are ever returned, never other users'
    identities or amounts). Needs at least 5 users with receipts this week
    to be meaningful; otherwise returns insufficient_data.
    """
    today = date.today()
    week_start = (today - timedelta(days=today.weekday())).isoformat()
    week_end = today.isoformat()

    try:
        rows = lib._sb().table("wa_saves").select("from_number,summary,title,created_at") \
            .gte("created_at", week_start).lt("created_at", week_end) \
            .ilike("title", "🧾%").execute().data or []
    except Exception:
        return {"status": "error"}

    per_user = {}
    seen = set()
    for r in rows:
        fn = r.get("from_number")
        if not fn:
            continue
        date_key = (r.get("created_at") or "")[:10]
        m = re.search(r'£([\d,]+\.?\d*)', (r.get("summary") or "") + (r.get("title") or ""))
        if not m:
            continue
        try:
            amount = float(m.group(1).replace(",", ""))
        except ValueError:
            continue
        merchant = (r.get("title") or "").replace("🧾", "").strip()
        if merchant.startswith("Online:"):
            continue
        key = (fn, date_key, round(amount, 2))
        if key in seen:
            continue
        seen.add(key)
        per_user[fn] = per_user.get(fn, 0.0) + amount

    users_compared = len(per_user)
    if wa not in per_user or users_compared < 5:
        return {"status": "insufficient_data", "users_compared": users_compared}

    user_spend = per_user[wa]
    spends = list(per_user.values())
    spent_more_count = sum(1 for v in spends if v > user_spend)
    percentile = int(round(spent_more_count / users_compared * 100))
    avg_spend = sum(spends) / users_compared

    if percentile >= 80:
        tier_label = "You're in the top 20% of savers this week"
    elif percentile >= 60:
        tier_label = "You're in the top 40% of savers this week"
    elif percentile >= 40:
        tier_label = "You're spending about average this week"
    else:
        tier_label = None  # don't guilt-trip — stay quiet in the bottom half

    return {
        "status": "ok",
        "percentile": percentile,
        "users_compared": users_compared,
        "user_spend_pence": int(round(user_spend * 100)),
        "avg_spend_pence": int(round(avg_spend * 100)),
        "tier_label": tier_label,
    }


def _lifetime_savings_pence(wa: str) -> int:
    total = 0
    try:
        events = lib._sb().table("savings_events").select("amount_pence") \
            .eq("wa", wa).execute().data or []
        total += sum(int(e.get("amount_pence") or 0) for e in events)
    except Exception:
        pass
    try:
        fuel = lib._sb().table("fuel_alerts").select("total_saved_pence") \
            .eq("wa", wa).execute().data or []
        total += sum(int(f.get("total_saved_pence") or 0) for f in fuel)
    except Exception:
        pass
    return total


def check_and_unlock_achievements(wa: str, weekly_wins: dict, goals: dict) -> dict:
    """Reconciles unlocked badges against current data. Safe to call on every page load."""
    try:
        existing_rows = lib._sb().table("user_achievements").select("achievement_type,unlocked_date") \
            .eq("wa", wa).execute().data or []
    except Exception:
        existing_rows = []
    existing = {r["achievement_type"] for r in existing_rows}

    to_unlock = []
    if weekly_wins.get("status") == "ok" and "first_week" not in existing:
        to_unlock.append("first_week")
    if weekly_wins.get("streak_weeks", 0) >= 5 and "budget_master" not in existing:
        to_unlock.append("budget_master")
    if any(g.get("progress_percent", 0) >= 100 for g in goals.get("goals", [])) and "goal_getter" not in existing:
        to_unlock.append("goal_getter")
    if _lifetime_savings_pence(wa) >= 10000 and "big_saver" not in existing:
        to_unlock.append("big_saver")
    if weekly_wins.get("fuel_saved_pence", 0) >= 2000 and "fuel_saver" not in existing:
        to_unlock.append("fuel_saver")

    newly_unlocked = []
    for t in to_unlock:
        try:
            lib._sb().table("user_achievements").insert({"wa": wa, "achievement_type": t}).execute()
            newly_unlocked.append(t)
            existing_rows.append({"achievement_type": t, "unlocked_date": date.today().isoformat()})
        except Exception:
            pass  # already unlocked (race) or table not migrated yet

    badges = [
        {**ACHIEVEMENTS[r["achievement_type"]], "type": r["achievement_type"], "unlocked_date": r.get("unlocked_date")}
        for r in existing_rows if r.get("achievement_type") in ACHIEVEMENTS
    ]
    return {"badges": badges, "newly_unlocked": newly_unlocked}


def build_engagement_summary(wa: str) -> dict:
    """Everything the web 'Wins & Goals' card needs, in one call."""
    weekly_wins = build_weekly_wins(wa)
    goals = build_goals_progress(wa)
    social_proof = build_social_proof(wa)
    ach = check_and_unlock_achievements(wa, weekly_wins, goals)

    return {
        "status": "ok",
        "weekly_wins": weekly_wins,
        "goals": goals.get("goals", []),
        "social_proof": social_proof,
        "badges": ach["badges"],
        "newly_unlocked": ach["newly_unlocked"],
    }
