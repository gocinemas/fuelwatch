"""
STRONG BRIEF ENGINE — Predictive Intelligence for the next 2-4 hours

Combines:
1. Clash detection (conflicts between events/weather/traffic)
2. Predictive alerts (what's coming that matters)
3. Opportunity spotting (deals, places, moments)
4. Micro-decisions (actionable advice: "leave now?", "worth it?")
5. Time optimization (fastest route given constraints)

Output: One unified STRONG BRIEF with proactive warnings + opportunities.
"""
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any

class StrongBriefEngine:
    def __init__(self):
        self.now = datetime.now()
        self.lookahead_hours = 3  # Predict next 3 hours

    def build_strong_brief(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build a unified strong brief with:
        - Clashes: conflicts that need attention
        - Predictions: what's coming
        - Opportunities: moments worth seizing
        - Decisions: actionable advice
        """
        clashes = self._detect_clashes(context)
        predictions = self._forecast_coming_hours(context)
        opportunities = self._find_opportunities(context)
        decisions = self._make_micro_decisions(context, clashes, predictions)

        # Rank by importance
        alerts = self._rank_alerts(clashes, predictions, opportunities)

        return {
            "brief_type": "STRONG",
            "timestamp": self.now.isoformat(),
            "alerts": alerts,
            "clashes": clashes,
            "predictions": predictions,
            "opportunities": opportunities,
            "decisions": decisions,
            "narrative": self._generate_narrative(alerts, decisions)
        }

    def _detect_clashes(self, ctx: Dict) -> List[Dict]:
        """Detect conflicts: rain + event, traffic + deadline, etc."""
        clashes = []

        # CLASH 1: Weather + School timing
        weather = ctx.get("weather", {})
        school_events = ctx.get("school_events", [])
        if weather.get("rain_prob", 0) > 60:
            for evt in school_events:
                evt_time = evt.get("time")  # e.g., "15:15"
                if evt_time and self._is_within_hours(evt_time, hours=2):
                    clashes.append({
                        "type": "weather_event",
                        "severity": "high",
                        "description": f"☔ Rain ({weather.get('rain_prob')}%) during {evt.get('activity')} at {evt_time}",
                        "action": "Bring umbrella + allow extra time"
                    })

        # CLASH 2: Traffic + Deadline
        commute = ctx.get("commute", {})
        travel_time_mins = commute.get("travel_time_mins", 0)
        leave_deadline = commute.get("must_leave_by")
        if travel_time_mins > 25:  # Heavy traffic
            clashes.append({
                "type": "traffic_deadline",
                "severity": "high",
                "description": f"🚗 Heavy traffic ({travel_time_mins} min) — leave earlier",
                "action": f"Depart by {self._subtract_minutes(leave_deadline, 10)}"
            })

        # CLASH 3: Fuel low + price spike
        fuel = ctx.get("fuel", {})
        if fuel.get("current_price", 0) > fuel.get("avg_price", 0) + 5:
            clashes.append({
                "type": "fuel_price",
                "severity": "medium",
                "description": f"⛽ Prices high (+{fuel.get('current_price') - fuel.get('avg_price')}p)",
                "action": "Wait 24h if possible, or use cheapest nearby"
            })

        return clashes

    def _forecast_coming_hours(self, ctx: Dict) -> List[Dict]:
        """Predict what matters in next 2-4 hours."""
        predictions = []

        # PREDICTION 1: Next event
        school = ctx.get("school_events", [])
        if school:
            next_evt = school[0]
            predictions.append({
                "time": next_evt.get("time"),
                "type": "event",
                "description": f"🏫 {next_evt.get('activity')} at {next_evt.get('time')}",
                "prep_time_mins": self._calc_prep_time(next_evt)
            })

        # PREDICTION 2: Weather change
        weather = ctx.get("weather", {})
        if weather.get("rain_prob", 0) > 40:
            predictions.append({
                "time": weather.get("rain_time", "unknown"),
                "type": "weather",
                "description": f"🌧️ Rain expected at {weather.get('rain_time')}",
                "impact": "Allow extra travel time"
            })

        # PREDICTION 3: Fuel price trend
        fuel = ctx.get("fuel", {})
        if fuel.get("trend") == "dropping":
            predictions.append({
                "type": "price_trend",
                "description": "⬇️ Fuel prices dropping — wait if you can",
                "savings_potential": "£1-2 per tank"
            })

        return predictions

    def _find_opportunities(self, ctx: Dict) -> List[Dict]:
        """Find serendipitous moments: deals, places, activities."""
        opportunities = []

        # OPPORTUNITY 1: Nearby discount
        nearby = ctx.get("nearby_places", [])
        for place in nearby:
            if place.get("discount"):
                opportunities.append({
                    "type": "deal",
                    "place": place.get("name"),
                    "distance_mi": place.get("distance_mi"),
                    "discount": place.get("discount"),
                    "description": f"✨ {place.get('name')} has {place.get('discount')} off",
                    "time_fits": self._check_time_fit(place, ctx)
                })

        # OPPORTUNITY 2: Trending local activity
        trends = ctx.get("local_trends", [])
        for trend in trends[:2]:
            opportunities.append({
                "type": "activity",
                "description": f"🎬 {trend.get('activity')} trending near you",
                "time": trend.get("time"),
                "distance_mi": trend.get("distance_mi")
            })

        # OPPORTUNITY 3: Personal pattern insight
        spend = ctx.get("spend", {})
        if spend.get("vs_average") and "below" in spend.get("vs_average", "").lower():
            opportunities.append({
                "type": "insight",
                "description": f"💡 Your spend is {spend.get('vs_average')} — budget room for treats?",
                "action": "Enjoy something nice guilt-free"
            })

        return opportunities

    def _make_micro_decisions(self, ctx: Dict, clashes: List, predictions: List) -> Dict[str, Any]:
        """Generate actionable micro-decisions: 'Should I leave now?' 'Worth the trip?'"""
        decisions = {}

        # DECISION 1: Leave timing
        if clashes and any(c["type"] == "traffic_deadline" for c in clashes):
            decisions["leave_now"] = {
                "recommendation": "YES, leave 10 mins early",
                "reason": "Heavy traffic + school pickup",
                "time": "NOW or 2:45pm latest"
            }
        else:
            decisions["leave_now"] = {
                "recommendation": "No rush, leave by 3pm",
                "reason": "Traffic normal, 15min buffer"
            }

        # DECISION 2: Is the trip worth it?
        nearby = ctx.get("nearby_places", [])
        discount_places = [p for p in nearby if p.get("discount")]
        if discount_places:
            best = max(discount_places, key=lambda x: float(x.get("discount", "0%").split()[0].rstrip("%")))
            decisions["worth_detour"] = {
                "recommendation": "YES",
                "place": best.get("name"),
                "savings": best.get("discount"),
                "time_cost_mins": best.get("extra_time_mins", 5)
            }

        # DECISION 3: Fuel now or later?
        fuel = ctx.get("fuel", {})
        if fuel.get("current_price", 0) > fuel.get("avg_price", 0) + 5:
            decisions["fuel_timing"] = {
                "recommendation": "WAIT 24h if possible",
                "reason": f"Prices high (+{fuel.get('current_price') - fuel.get('avg_price')}p)",
                "savings_potential": "£1-2"
            }
        else:
            decisions["fuel_timing"] = {
                "recommendation": "Buy now if needed",
                "cheapest": fuel.get("cheapest_nearby_brand"),
                "price": fuel.get("cheapest_price")
            }

        return decisions

    def _rank_alerts(self, clashes: List, predictions: List, opportunities: List) -> List[Dict]:
        """Rank all alerts by importance: clashes first, then predictions, then opportunities."""
        alerts = []

        # High severity clashes first
        for clash in sorted(clashes, key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x.get("severity", "low"), 3)):
            alerts.append({
                "emoji": "⚠️",
                "priority": "critical",
                "text": clash.get("description"),
                "action": clash.get("action")
            })

        # Then predictions
        for pred in predictions[:2]:
            alerts.append({
                "emoji": {"event": "🏫", "weather": "🌧️", "price_trend": "⬇️"}.get(pred.get("type"), "📌"),
                "priority": "high",
                "text": pred.get("description"),
                "action": pred.get("impact") or pred.get("prep_time_mins")
            })

        # Then opportunities
        for opp in opportunities[:2]:
            alerts.append({
                "emoji": "✨",
                "priority": "medium",
                "text": opp.get("description"),
                "action": opp.get("action", opp.get("time_fits"))
            })

        return alerts

    def _generate_narrative(self, alerts: List, decisions: Dict) -> str:
        """Generate human-friendly narrative from alerts + decisions."""
        if not alerts:
            return "All clear for the next few hours. Enjoy your time!"

        lines = []

        # Critical alerts first
        critical = [a for a in alerts if a.get("priority") == "critical"]
        for alert in critical:
            lines.append(f"{alert.get('emoji')} {alert.get('text')} — {alert.get('action')}")

        # Decision advice
        if decisions.get("leave_now"):
            leave = decisions["leave_now"]
            lines.append(f"🚗 {leave.get('recommendation')} ({leave.get('reason')})")

        # Opportunity highlight
        opps = [a for a in alerts if a.get("priority") == "medium"]
        if opps:
            lines.append(f"\n💡 Worth noting: {opps[0].get('text')}")

        return "\n".join(lines)

    # ── Helper methods ────────────────────────────────────────────────

    def _is_within_hours(self, event_time_str: str, hours: int = 3) -> bool:
        """Check if event is within N hours."""
        try:
            h, m = map(int, event_time_str.split(":"))
            event = self.now.replace(hour=h, minute=m, second=0)
            return 0 <= (event - self.now).total_seconds() <= (hours * 3600)
        except:
            return False

    def _subtract_minutes(self, time_str: str, mins: int) -> str:
        """Subtract minutes from HH:MM string."""
        try:
            h, m = map(int, time_str.split(":"))
            t = self.now.replace(hour=h, minute=m) - timedelta(minutes=mins)
            return t.strftime("%H:%M")
        except:
            return time_str

    def _calc_prep_time(self, event: Dict) -> int:
        """Calculate prep time needed for event."""
        activity = event.get("activity", "").lower()
        if "pickup" in activity or "pickup" in activity:
            return 15
        elif "sport" in activity or "dance" in activity:
            return 30
        else:
            return 10

    def _check_time_fit(self, place: Dict, ctx: Dict) -> str:
        """Check if place visit fits in timeline."""
        # Simplified: just check if there's 30 min gap
        return "✓ Fits in schedule" if ctx.get("free_time_mins", 0) > 30 else "⏰ Tight timing"


# Test
if __name__ == "__main__":
    engine = StrongBriefEngine()

    test_ctx = {
        "weather": {"rain_prob": 75, "rain_time": "15:00"},
        "school_events": [{"time": "15:15", "activity": "Inaaya Violin", "location": "school"}],
        "commute": {"travel_time_mins": 28, "must_leave_by": "15:00"},
        "fuel": {"current_price": 162, "avg_price": 158, "trend": "stable"},
        "nearby_places": [
            {"name": "Costa", "distance_mi": 0.3, "discount": "20% off", "extra_time_mins": 2},
            {"name": "Waitrose", "distance_mi": 1.2, "discount": None}
        ],
        "local_trends": [{"activity": "Outdoor film night", "time": "19:00", "distance_mi": 1.5}],
        "spend": {"vs_average": "8% below average"},
        "free_time_mins": 45
    }

    result = engine.build_strong_brief(test_ctx)
    print(json.dumps(result, indent=2))
    print("\n" + "="*60)
    print("NARRATIVE:")
    print(result.get("narrative"))
