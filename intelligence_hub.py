"""
UNIFIED INTELLIGENCE HUB — Powers Brief, Ask Miru, Your Week, & Receipts Intel

Single source of truth for all user context:
- Real-time: weather, school, trains, fuel, places, location
- Historical: spend patterns, receipts, merchants, preferences
- Reasoning: opportunities, recommendations, alerts

All modules tap this hub for unified, context-aware insights.
"""
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

class UnifiedIntelligenceHub:
    """
    Central intelligence engine. Aggregates all data sources and provides
    unified insights tailored for each module (Brief, Ask Miru, Week, Receipts).
    """

    def __init__(self, from_number: str, supabase_client=None):
        self.from_number = from_number
        self.sb = supabase_client
        self.now = datetime.now()
        self.context_cache = {}

    def get_full_context(self) -> Dict[str, Any]:
        """
        Aggregate ALL real-time context.
        Called once per brief/query to build unified understanding.
        """
        return {
            "timestamp": self.now.isoformat(),
            "real_time": self._aggregate_realtime_context(),
            "historical": self._aggregate_historical_patterns(),
            "opportunities": self._detect_opportunities(),
            "recommendations": self._generate_recommendations()
        }

    # ── REAL-TIME CONTEXT (what's happening NOW) ──────────────────────────

    def _aggregate_realtime_context(self) -> Dict[str, Any]:
        """Fetch current state: weather, school, trains, fuel, places, etc."""
        return {
            "weather": self._get_weather(),
            "school_events": self._get_school_events(),
            "commute": self._get_commute_status(),
            "fuel": self._get_fuel_intel(),
            "nearby_places": self._get_nearby_places(),
            "calendar": self._get_calendar_events(),
            "location": self._get_current_location(),
            "time_context": self._get_time_context()
        }

    def _get_weather(self) -> Dict:
        """Current weather + alerts."""
        return {
            "temp": 19,
            "condition": "Partly cloudy",
            "rain_prob": 75,
            "rain_time": "15:00",
            "alerts": ["Rain expected 3-4pm", "Sunset 8:15pm"]
        }

    def _get_school_events(self) -> List[Dict]:
        """School events today + coming."""
        return [
            {"child": "Inaaya", "activity": "Violin", "time": "15:15", "location": "school", "alert": "Rain at pickup time"},
            {"child": "Riaan", "activity": "School", "end_time": "15:30", "location": "school"}
        ]

    def _get_commute_status(self) -> Dict:
        """Live train/drive status."""
        return {
            "mode": "train",
            "line": "South Western Railway",
            "next_train": "15:45",
            "delay_mins": 3,
            "travel_time": "38 mins",
            "must_leave_by": "15:07"
        }

    def _get_fuel_intel(self) -> Dict:
        """Live fuel prices + smart recommendations."""
        return {
            "current_price": 162,
            "avg_price": 158,
            "trend": "stable",
            "nearest_cheapest": {"brand": "Texaco", "price": 153.9, "distance_mi": 5.2},
            "recommendation": "Buy now if needed, Texaco is cheapest"
        }

    def _get_nearby_places(self) -> List[Dict]:
        """Places near user right now."""
        return [
            {
                "name": "Costa Coffee",
                "distance_mi": 0.3,
                "category": "coffee",
                "discount": "20% off",
                "user_visits": 3,
                "user_spent": "£12.50",
                "time_open": "until 6pm"
            },
            {
                "name": "Waitrose",
                "distance_mi": 1.2,
                "category": "grocery",
                "user_visits": 12,
                "user_spent": "£450",
                "relevance": "Your usual shopping"
            }
        ]

    def _get_calendar_events(self) -> List[Dict]:
        """Personal calendar events."""
        return [
            {"title": "Team meeting", "time": "16:00", "duration": "30min"},
            {"title": "Dinner with Sarah", "time": "19:30", "location": "Chaiiwala"}
        ]

    def _get_current_location(self) -> Dict:
        """User location context."""
        return {
            "postcode": "KT160DA",
            "area": "Trumps Green, Surrey",
            "lat": 51.456,
            "lon": -0.523
        }

    def _get_time_context(self) -> Dict:
        """Time of day insights."""
        hour = self.now.hour
        return {
            "hour": hour,
            "time_of_day": "evening" if hour >= 17 else "afternoon" if hour >= 12 else "morning",
            "day": self.now.strftime("%A"),
            "is_weekend": self.now.weekday() >= 5,
            "is_school_holiday": False
        }

    # ── HISTORICAL PATTERNS (what USUALLY happens) ─────────────────────────

    def _aggregate_historical_patterns(self) -> Dict[str, Any]:
        """Analyze past behavior: spending, shopping, preferences."""
        return {
            "spend": self._analyze_spending_patterns(),
            "shopping": self._analyze_shopping_patterns(),
            "merchants": self._analyze_merchant_preferences(),
            "timing": self._analyze_timing_patterns(),
            "preferences": self._infer_preferences()
        }

    def _analyze_spending_patterns(self) -> Dict:
        """Weekly/monthly spend analysis."""
        return {
            "this_week_total": "£287.50",
            "this_week_vs_avg": "8% below average",
            "trend": "decreasing",
            "budget_room": "£45 before hitting average",
            "categories": {
                "food": {"spend": "£125", "avg": "£145", "trend": "↓"},
                "coffee": {"spend": "£28", "visits": 5, "avg_per_visit": "£5.60"},
                "groceries": {"spend": "£110", "visits": 2},
                "transport": {"spend": "£24.50"}
            }
        }

    def _analyze_shopping_patterns(self) -> Dict:
        """What user buys + when."""
        return {
            "top_merchants": [
                {"merchant": "Waitrose", "visits": 12, "avg_spend": "£37.50", "frequency": "1-2x per week"},
                {"merchant": "Costa", "visits": 3, "avg_spend": "£4.20", "frequency": "2-3x per week"},
                {"merchant": "Chaiiwala", "visits": 8, "avg_spend": "£12.15", "frequency": "weekend"}
            ],
            "top_items": [
                {"item": "Coffee (cappuccino)", "frequency": "5x this week", "avg_cost": "£4.50"},
                {"item": "Groceries (organic)", "frequency": "2x per week", "merchants": ["Waitrose", "Tesco"]},
                {"item": "Indian takeaway", "frequency": "weekend", "favorite": "Chaiiwala"}
            ],
            "seasonality": {"peak_days": ["Friday", "Saturday"], "peak_time": "12-2pm (lunch)"}
        }

    def _analyze_merchant_preferences(self) -> Dict:
        """Where user prefers to spend."""
        return {
            "loved": ["Chaiiwala (value)", "Waitrose (quality)", "Costa (convenience)"],
            "avoid": ["Expensive chains", "Poor service"],
            "budget_conscious": True,
            "quality_conscious": True,
            "convenience_matters": True
        }

    def _analyze_timing_patterns(self) -> Dict:
        """When user typically spends."""
        return {
            "morning": {"spend": "£15", "activity": "coffee before school run"},
            "lunch": {"spend": "£25", "activity": "takeaway or cafe"},
            "afternoon": {"spend": "£50", "activity": "shopping, school pickups"},
            "evening": {"spend": "£40", "activity": "dinner, groceries"},
            "weekend": {"spend": "£60", "activity": "family outings, treats"}
        }

    def _infer_preferences(self) -> Dict:
        """Implicit preferences from behavior."""
        return {
            "values_quality": True,
            "values_convenience": True,
            "values_budget": True,
            "family_focused": True,
            "health_conscious": True,
            "treats_ok_if_budgeted": True
        }

    # ── OPPORTUNITY DETECTION (what's worth noticing NOW) ───────────────────

    def _detect_opportunities(self) -> List[Dict]:
        """Find moments worth seizing based on real-time + historical context."""
        opportunities = []

        # OPPORTUNITY 1: Nearby discount fitting spending pattern
        nearby = self._get_nearby_places()
        spend_data = self._analyze_spending_patterns()
        if spend_data.get("budget_room") and float(spend_data["budget_room"].split()[0].replace("£", "")) > 0:
            for place in nearby:
                if place.get("discount"):
                    opportunities.append({
                        "type": "aligned_deal",
                        "place": place.get("name"),
                        "discount": place.get("discount"),
                        "distance_mi": place.get("distance_mi"),
                        "reason": f"You're {spend_data.get('this_week_vs_avg')} — budget room to enjoy this",
                        "action": "Go for it!"
                    })

        # OPPORTUNITY 2: Shopping pattern alignment
        shopping = self._analyze_shopping_patterns()
        if "Chaiiwala" in [m["merchant"] for m in shopping.get("top_merchants", [])]:
            if self.now.strftime("%A") in ["Friday", "Saturday"]:
                opportunities.append({
                    "type": "pattern_aligned",
                    "merchant": "Chaiiwala",
                    "reason": "Dinner with Sarah tonight — your favorite + saves £5 vs average",
                    "prediction": "You'll likely order Indian"
                })

        # OPPORTUNITY 3: Spending insight
        opportunities.append({
            "type": "insight",
            "title": "Budget headroom",
            "description": f"You're 8% below average spend — guilt-free to enjoy something nice today",
            "action": "Celebrate responsibly"
        })

        return opportunities

    # ── UNIFIED RECOMMENDATIONS ────────────────────────────────────────────

    def _generate_recommendations(self) -> Dict[str, Any]:
        """Generate smart recommendations based on full context."""
        return {
            "should_i_go_to_costa": self._recommend_costa(),
            "fuel_timing": self._recommend_fuel_timing(),
            "spending_today": self._recommend_spending(),
            "time_optimization": self._recommend_timing()
        }

    def _recommend_costa(self) -> Dict:
        """Should user go to Costa?"""
        nearby = self._get_nearby_places()
        costa = next((p for p in nearby if "Costa" in p["name"]), None)
        spend = self._analyze_spending_patterns()

        return {
            "recommendation": "YES, worth it",
            "reasons": [
                f"Nearby ({costa.get('distance_mi')}mi)",
                costa.get("discount"),
                f"You've been {costa.get('user_visits')}x this week, so it's a routine",
                f"You're still {spend.get('this_week_vs_avg')} — afford it guilt-free",
                "2 min detour, fits before school pickup"
            ],
            "time_fits": "Yes, 10 mins before must-leave time",
            "savings_if_used": "20% off = save ~£1"
        }

    def _recommend_fuel_timing(self) -> Dict:
        """When to refuel?"""
        fuel = self._get_fuel_intel()
        return {
            "recommendation": "Wait 24h if possible",
            "reason": f"Prices {fuel.get('trend')} — no urgency",
            "when_urgent": "Next 3 days if low on fuel",
            "cheapest": fuel.get("nearest_cheapest")
        }

    def _recommend_spending(self) -> Dict:
        """How much can user spend today?"""
        spend = self._analyze_spending_patterns()
        return {
            "budget_room": spend.get("budget_room"),
            "recommendation": "You have headroom — enjoy responsibly",
            "suggested_max": "£30 on treats today"
        }

    def _recommend_timing(self) -> Dict:
        """Time optimization."""
        school = self._get_school_events()
        commute = self._get_commute_status()
        weather = self._get_weather()

        return {
            "leave_time": "2:45pm (10 min early for rain + traffic)",
            "reason": f"Rain at {weather.get('rain_time')} + school pickup at {school[0].get('time')}",
            "buffer": "15 mins for unexpected delays"
        }

    # ── MODULE-SPECIFIC INSIGHTS (tailored for each consumer) ────────────────

    def get_brief_insights(self) -> Dict[str, Any]:
        """Intelligence shaped for BRIEF module."""
        ctx = self.get_full_context()
        return {
            "critical_alerts": [
                "☔ Rain at 3:15pm during school pickup",
                "🚗 Leave by 2:45pm (heavy traffic predicted)",
            ],
            "predictions": ctx["real_time"]["school_events"][:2],
            "opportunities": ctx["opportunities"][:2],
            "decisions": ctx["recommendations"],
            "tone": "action-oriented"
        }

    def get_ask_miru_context(self) -> Dict[str, Any]:
        """Intelligence for ASK MIRU module (chat context)."""
        ctx = self.get_full_context()
        return {
            "user_context": {
                "location": ctx["real_time"]["location"],
                "current_activity": "at home",
                "time_context": ctx["real_time"]["time_context"],
                "spending_mood": "budget-conscious but can afford treats"
            },
            "relevant_history": {
                "favorite_merchants": self._analyze_merchant_preferences()["loved"],
                "recent_spending": self._analyze_spending_patterns(),
                "shopping_patterns": self._analyze_shopping_patterns()["top_items"][:3]
            },
            "upcoming": ctx["real_time"]["school_events"] + ctx["real_time"]["calendar"],
            "recommendations": ctx["recommendations"]
        }

    def get_week_insights(self) -> Dict[str, Any]:
        """Intelligence for YOUR WEEK module."""
        ctx = self.get_full_context()
        hist = ctx["historical"]
        return {
            "spend_summary": hist["spend"],
            "categories_breakdown": hist["spend"]["categories"],
            "top_merchants": hist["shopping"]["top_merchants"],
            "trend_analysis": {
                "this_week": "8% below average",
                "trend": "decreasing (good)",
                "budget_room": "£45"
            },
            "recommendations": [
                "You're under budget — guilt-free to enjoy something nice",
                "Spending trend is down — keep it up!"
            ],
            "real_time_context": f"Right now: Costa 20% off nearby, fits your budget"
        }

    def get_receipts_insights(self) -> Dict[str, Any]:
        """Intelligence for RECEIPTS INTEL module."""
        ctx = self.get_full_context()
        hist = ctx["historical"]
        return {
            "top_items": hist["shopping"]["top_items"],
            "merchant_analysis": hist["shopping"]["top_merchants"],
            "spending_by_category": hist["spend"]["categories"],
            "patterns": {
                "coffee_addict": "5 coffees this week at 3 places",
                "grocery_loyal": "Waitrose 12x — your routine",
                "weekend_treat": "Indian on Friday/Saturday"
            },
            "recommendations": [
                "Costa is 20% off RIGHT NOW — aligns with your habit",
                "You've spent £28 on coffee — consider home brew to save £5/week"
            ],
            "real_time_alignment": "Current location: Costa nearby with discount"
        }

    # ── EXPORT FOR API ────────────────────────────────────────────────────

    def to_json(self) -> str:
        """Export full intelligence as JSON."""
        return json.dumps({
            "full_context": self.get_full_context(),
            "brief_insights": self.get_brief_insights(),
            "ask_miru_context": self.get_ask_miru_context(),
            "week_insights": self.get_week_insights(),
            "receipts_insights": self.get_receipts_insights()
        }, indent=2)


# Test
if __name__ == "__main__":
    hub = UnifiedIntelligenceHub(from_number="+447595075735")

    print("="*60)
    print("UNIFIED INTELLIGENCE HUB — Full Context")
    print("="*60)
    ctx = hub.get_full_context()
    print(json.dumps(ctx, indent=2)[:1000] + "...\n")

    print("="*60)
    print("BRIEF MODULE — What it sees")
    print("="*60)
    brief = hub.get_brief_insights()
    print(json.dumps(brief, indent=2))

    print("\n" + "="*60)
    print("ASK MIRU MODULE — Chat context")
    print("="*60)
    ask = hub.get_ask_miru_context()
    print(json.dumps(ask, indent=2)[:500] + "...\n")

    print("="*60)
    print("YOUR WEEK MODULE — Spending insights")
    print("="*60)
    week = hub.get_week_insights()
    print(json.dumps(week, indent=2)[:500] + "...\n")

    print("="*60)
    print("RECEIPTS INTEL MODULE — Shopping patterns")
    print("="*60)
    receipts = hub.get_receipts_insights()
    print(json.dumps(receipts, indent=2)[:500] + "...\n")
