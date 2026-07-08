"""
Miru Intelligence Engine - Agentic Reasoning Across All Modules

Synthesizes data from receipts, fuel, school, calendar, saves, and commute
to provide personalized insights, forecasts, and recommendations.

Uses intelligence_optimizer for smart Groq/Anthropic routing:
- Groq for reasoning (60x cheaper, fast)
- Anthropic for fallback/high-quality needs
- Caching for repeated requests (90% savings)
"""

# ⚠️  Patch gevent FIRST before any async HTTP imports (Groq, Anthropic, Supabase use httpx)
from gevent import monkey as _gmonkey
_gmonkey.patch_all(thread=True, socket=True, ssl=True)

import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import os

# Groq LLM for agentic reasoning (primary)
from groq import Groq

# Anthropic as fallback
from anthropic import Anthropic

groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
anthropic_client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


class MiruIntelligence:
    """Main intelligence engine for Miru."""

    def __init__(self):
        self.model = "llama-3.1-8b-instant"  # Fast, reasoning-capable (mixtral-8x7b-32768 decommissioned)

    def _format_data_summary(self, data: Dict) -> str:
        """Format user data for agentic reasoning prompt."""
        return f"""
User Data Summary:
─────────────────
RECEIPTS (This Week):
  Total spend: £{data.get('spend_total', 0):.2f}
  Categories: {json.dumps(data.get('spend_by_category', {}), indent=2)}
  Top merchants: {', '.join(data.get('top_merchants', []))}

FUEL DATA:
  Last fill: £{data.get('last_fuel_amount', 0):.2f} @ {data.get('last_fuel_price', 'N/A')}p/L on {data.get('last_fuel_date', 'N/A')}
  Current fuel price: {data.get('current_fuel_price', 'N/A')}p/L
  Price trend: {data.get('fuel_price_trend', 'N/A')}
  Days since last fill: {data.get('days_since_fuel', 'N/A')}

SCHOOL EVENTS:
  Events this week: {data.get('school_events_count', 0)}
  Busiest day: {data.get('busiest_school_day', 'N/A')}

SPENDING PATTERN:
  Last week: £{data.get('last_week_spend', 0):.2f}
  Average weekly: £{data.get('avg_weekly_spend', 0):.2f}
  Trend: {data.get('spend_trend', 'N/A')}

CAFE/LOCATION:
  Cafe visits this week: {data.get('cafe_visits', 0)}
  Top location: {data.get('top_location', 'N/A')}

SAVES:
  This week: {data.get('saves_count', 0)}
  Last week: {data.get('last_week_saves', 0)}
"""

    def generate_insights(self, data: Dict, use_anthropic_fallback: bool = False) -> Dict[str, Any]:
        """
        Agentic reasoning across all modules to generate insights.
        Routes smartly between Groq (primary) and Anthropic (fallback).
        Returns: structured insights with forecasts, recommendations, anomalies.
        """

        data_summary = self._format_data_summary(data)

        # PRE-CHECK: If user filled up very recently (last 24h), don't suggest filling up
        days_since_fuel = data.get('days_since_fuel', 999)
        just_filled_up = days_since_fuel is not None and days_since_fuel <= 1

        fuel_instruction = ""
        if just_filled_up:
            fuel_instruction = """
CRITICAL: User JUST FILLED UP (within last 24 hours).
DO NOT suggest filling up now. Instead:
  - Note the recent fill (amount, price, date)
  - Compare price to current (up/down)
  - Suggest NEXT fill in 5-7 days based on typical usage
  - NOT an urgent action right now
"""
        else:
            fuel_instruction = """
Based on days since last fill and consumption pattern, suggest when they should fill up next.
"""

        prompt = f"""{data_summary}

SHARP, SPECIFIC INSIGHTS. Use actual data above. No generic advice.

RESPOND WITH VALID JSON ONLY (no markdown, no text outside JSON).

{{
  "fuel": {{
    "price_trend": "up or down or stable",
    "percent_change": 2.5,
    "next_fill_days": 5,
    "recommendation": "SPECIFIC action: e.g. 'Fill at Tesco (saves 3p/L)'"
  }},
  "spend": {{
    "trend": "up or down or stable",
    "vs_normal": "e.g. '-15% vs normal'",
    "forecast_next_week": 120,
    "top_saving": "SPECIFIC: e.g. 'Coffee at home 2x/week = £50/month saved'"
  }},
  "location": {{
    "most_visited": "actual top merchant",
    "cost_per_visit": 26.5,
    "alternative": "specific competitor name",
    "savings": "e.g. 'Switch from X to Y = £120/year'"
  }},
  "school": {{
    "busy_level": "normal or busy or very_busy",
    "impact": "SPECIFIC effect: e.g. 'Wednesday adds 30min drive'",
    "next_busy_day": "actual busiest day"
  }},
  "lifestyle": {{
    "change": "OBSERVED from data: e.g. 'Cafe visits up 40%'",
    "activity_level": "normal or increased or decreased"
  }},
  "anomalies": ["REAL issues: e.g. 'No fuel in 8 days (risky)'"],
  "recommendations": ["High-impact action with £ estimate", "Action 2", "Action 3"],
  "forecast": {{
    "next_week_spend": 125,
    "next_fuel_date": "specific date",
    "action_items": ["Exact action to take"]
  }}
}}

Return ONLY valid JSON. No markdown, no text."""

        try:
            # Route to Groq (primary) or Anthropic (fallback)
            if use_anthropic_fallback or not os.environ.get("GROQ_API_KEY"):
                # Use Anthropic Claude 3.5 Sonnet (higher quality, higher cost)
                print("[intelligence] Using Anthropic (fallback/high-quality)")
                message = anthropic_client.messages.create(
                    model="claude-opus-4-1",  # Fallback: use Opus for high quality when Groq unavailable
                    max_tokens=2000,
                    messages=[
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                )
                response_text = message.content[0].text.strip()
            else:
                # Use Groq Mixtral (60x cheaper, fast, good reasoning)
                print("[intelligence] Using Groq (primary, cost-optimized)")
                message = groq_client.chat.completions.create(
                    model=self.model,
                    max_tokens=2000,
                    messages=[
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                )
                response_text = message.choices[0].message.content.strip()
            print(f"[intelligence] Response: {response_text[:100]}...")

            # Extract JSON from response
            start = response_text.find('{')
            end = response_text.rfind('}') + 1

            if start >= 0 and end > start:
                json_str = response_text[start:end]
                try:
                    insights = json.loads(json_str)
                    print(f"[intelligence] ✅ Parsed successfully")
                    return insights
                except json.JSONDecodeError as je:
                    print(f"[intelligence] JSON error: {je}")
                    # Try to fix common issues
                    json_str = json_str.replace(",}", "}").replace(",]", "]")
                    # Replace null with defaults (0 for numbers, empty string for text)
                    json_str = json_str.replace(": null,", ': 0,').replace(": null}", ': 0}')
                    try:
                        insights = json.loads(json_str)
                        print(f"[intelligence] ✅ Fixed and parsed")
                        return insights
                    except:
                        print(f"[intelligence] ❌ Still invalid after fixes")
                        pass

            print(f"[intelligence] Failed to extract JSON")
            insights = {}

        except Exception as e:
            print(f"[intelligence] Error generating insights: {type(e).__name__}: {e}")
            # Don't cascade to Anthropic - just return empty insights with error
            # (cascading causes additional errors and delays)
            return {
                "fuel": {},
                "spend": {},
                "location": {},
                "school": {},
                "lifestyle": {},
                "anomalies": [],
                "recommendations": [],
                "forecast": {},
                "error": f"Failed to generate insights: {str(e)[:100]}"
            }

    def get_full_intelligence(self, from_number: str, sb) -> Dict[str, Any]:
        """
        Aggregates all user data and generates complete intelligence report.
        This is the main entry point for the insights API.
        """
        import datetime as _dt

        phone = from_number.replace("whatsapp:", "").strip()
        now = _dt.datetime.utcnow()
        today = now.date()
        week_ago = today - _dt.timedelta(days=7)

        # This week: Monday-Sunday (same as Your Week endpoint)
        days_since_monday = today.weekday()
        week_start = today - _dt.timedelta(days=days_since_monday)
        week_end = week_start + _dt.timedelta(days=6)

        # Aggregate all data
        try:
            # This week's receipts (Monday-Sunday only)
            receipts = sb.table("receipts").select("total,merchant,shop_date,restaurant_type") \
                .eq("phone", phone) \
                .gte("shop_date", week_start.isoformat()) \
                .lte("shop_date", week_end.isoformat()) \
                .execute().data or []

            spend_total = sum(float(r.get("total", 0)) for r in receipts)

            # Category breakdown
            spend_by_category = {}
            for r in receipts:
                merchant = r.get("merchant", "Unknown")
                from sms_service import _receipt_category
                cat = _receipt_category(merchant)
                if cat not in spend_by_category:
                    spend_by_category[cat] = 0
                spend_by_category[cat] += float(r.get("total", 0))

            # Top merchants
            merchants = {}
            for r in receipts:
                m = r.get("merchant", "Unknown")
                merchants[m] = merchants.get(m, 0) + float(r.get("total", 0))
            top_merchants = sorted(merchants.items(), key=lambda x: x[1], reverse=True)[:5]

            # Last week comparison (previous Monday-Sunday)
            last_week_end = week_start - _dt.timedelta(days=1)
            last_week_start = last_week_end - _dt.timedelta(days=6)
            last_week_receipts = sb.table("receipts").select("total") \
                .eq("phone", phone) \
                .gte("shop_date", last_week_start.isoformat()) \
                .lte("shop_date", last_week_end.isoformat()) \
                .execute().data or []
            last_week_spend = sum(float(r.get("total", 0)) for r in last_week_receipts)

            # Fuel data
            fuel_receipts = [r for r in receipts if _receipt_category(r.get("merchant", "")) == "Fuel"]
            last_fuel = fuel_receipts[0] if fuel_receipts else None

            # School events
            school_events = sb.table("school_events").select("event_date") \
                .eq("from_number", from_number) \
                .gte("event_date", today.isoformat()) \
                .execute().data or []

            # Saves
            saves_this_week = sb.table("wa_saves").select("id") \
                .eq("from_number", from_number) \
                .gte("created_at", (today - _dt.timedelta(days=7)).isoformat()) \
                .execute().data or []

            saves_last_week = sb.table("wa_saves").select("id") \
                .eq("from_number", from_number) \
                .gte("created_at", (week_ago - _dt.timedelta(days=7)).isoformat()) \
                .lte("created_at", week_ago.isoformat()) \
                .execute().data or []

            # Build data summary for intelligence engine
            data = {
                "spend_total": spend_total,
                "spend_by_category": spend_by_category,
                "top_merchants": [m[0] for m in top_merchants],
                "last_week_spend": last_week_spend,
                "avg_weekly_spend": (spend_total + last_week_spend) / 2,
                "spend_trend": "up" if spend_total > last_week_spend else "down" if spend_total < last_week_spend else "stable",
                "last_fuel_amount": float(last_fuel.get("total", 0)) if last_fuel else 0,
                "last_fuel_date": last_fuel.get("shop_date", "N/A") if last_fuel else "N/A",
                "days_since_fuel": (today - _dt.datetime.fromisoformat(last_fuel.get("shop_date", today.isoformat())).date()).days if last_fuel else 0,
                "school_events_count": len(school_events),
                "cafe_visits": len([r for r in receipts if _receipt_category(r.get("merchant", "")) in ["Coffee & Lunch", "Dining", "Takeaway"]]),
                "top_location": top_merchants[0][0] if top_merchants else "N/A",
                "saves_count": len(saves_this_week),
                "last_week_saves": len(saves_last_week),
            }

            # Generate insights using agentic reasoning
            insights = self.generate_insights(data)

            return {
                "success": True,
                "timestamp": now.isoformat(),
                "data_summary": data,
                "insights": insights
            }

        except Exception as e:
            print(f"[intelligence] Error aggregating data: {e}")
            return {
                "success": False,
                "error": str(e)
            }
