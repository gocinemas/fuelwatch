"""
Module Integrations - Add intelligence insights to existing Miru screens
"""

def enhance_your_week_with_insights(week_data: dict, insights: dict) -> dict:
    """
    Enhance Your Week module with intelligence-driven insights.
    Adds forecasts, anomalies, and recommendations.
    """

    if not insights.get("insights"):
        return week_data

    insights_obj = insights["insights"]

    # Add spend forecast to Your Week
    if insights_obj.get("forecast"):
        week_data["forecast"] = {
            "next_week_spend": insights_obj["forecast"].get("next_week_spend"),
            "action_items": insights_obj["forecast"].get("action_items", [])
        }

    # Add anomalies
    if insights_obj.get("anomalies"):
        week_data["anomalies"] = insights_obj["anomalies"]

    # Add recommendations
    if insights_obj.get("recommendations"):
        week_data["recommendations"] = insights_obj["recommendations"][:3]

    # Add spend trend
    if insights_obj.get("spend"):
        week_data["spend_insight"] = insights_obj["spend"]

    return week_data


def enhance_receipts_with_insights(receipts_data: dict, insights: dict) -> dict:
    """
    Enhance Receipts module with spend intelligence.
    Adds trend analysis, forecasts, and savings opportunities.
    """

    if not insights.get("insights"):
        return receipts_data

    insights_obj = insights["insights"]

    # Add spend intelligence
    if insights_obj.get("spend"):
        receipts_data["spend_insight"] = insights_obj["spend"]

    # Add location intelligence (where to save money)
    if insights_obj.get("location"):
        receipts_data["location_insight"] = insights_obj["location"]

    # Add top 3 savings opportunities
    if insights_obj.get("recommendations"):
        savings_recs = [r for r in insights_obj["recommendations"] if "save" in r.lower()]
        receipts_data["savings_tips"] = savings_recs[:3]

    return receipts_data


def enhance_fuel_with_insights(fuel_data: dict, insights: dict) -> dict:
    """
    Enhance Fuel module with price intelligence.
    Adds trend analysis, refill forecasting, and cost optimization.
    """

    if not insights.get("insights"):
        return fuel_data

    insights_obj = insights["insights"]

    # Add fuel intelligence
    if insights_obj.get("fuel"):
        fuel_data["intelligence"] = insights_obj["fuel"]

    # Add action items
    if insights_obj.get("forecast"):
        fuel_data["action_items"] = [
            a for a in insights_obj["forecast"].get("action_items", [])
            if "fuel" in a.lower() or "refill" in a.lower()
        ]

    return fuel_data


def get_smart_notifications(insights: dict) -> list:
    """
    Extract actionable notifications from insights.
    Returns list of {title, message, action, priority}
    """

    if not insights.get("insights"):
        return []

    insights_obj = insights["insights"]
    notifications = []

    # Fuel refill notification
    if insights_obj.get("fuel"):
        fuel = insights_obj["fuel"]
        if fuel.get("next_fill_days") and fuel["next_fill_days"] <= 3:
            notifications.append({
                "title": "⛽ Refill Soon",
                "message": f"You'll need fuel in {fuel['next_fill_days']} days. Prices are {fuel.get('price_trend', 'stable')}.",
                "action": "showScreen('fuel')",
                "priority": "high"
            })

        if fuel.get("price_trend") == "down":
            notifications.append({
                "title": "⛽ Prices Down",
                "message": f"Fuel prices dropped {fuel.get('percent_change', 0)}% since your last fill. Good time to fill up!",
                "action": "showScreen('fuel')",
                "priority": "medium"
            })

    # Spend alert notification
    if insights_obj.get("spend"):
        spend = insights_obj["spend"]
        if spend.get("trend") == "up":
            notifications.append({
                "title": "💳 Spending Up",
                "message": f"Your spending is up {spend.get('vs_normal', '0')}% this week.",
                "action": "showScreen('receipts')",
                "priority": "medium"
            })

    # Anomaly notification
    if insights_obj.get("anomalies"):
        for anomaly in insights_obj["anomalies"][:2]:
            notifications.append({
                "title": "⚠️ Unusual Pattern",
                "message": anomaly,
                "action": "showScreen('intelligence')",
                "priority": "low"
            })

    # Action items (top priority)
    if insights_obj.get("recommendations"):
        for rec in insights_obj["recommendations"][:1]:  # Top recommendation only
            notifications.append({
                "title": "💡 Recommendation",
                "message": rec,
                "action": "showScreen('intelligence')",
                "priority": "high"
            })

    # Sort by priority
    priority_order = {"high": 0, "medium": 1, "low": 2}
    notifications.sort(key=lambda x: priority_order.get(x.get("priority"), 3))

    return notifications[:5]  # Return top 5 notifications
