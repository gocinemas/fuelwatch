# Miru Intelligence Hub — Complete Agentic Architecture

**Status:** ✅ DEPLOYED  
**Model:** Groq Mixtral 8x7b (fast reasoning, $0.24 per MTok)  
**Launch Date:** 2 July 2026  

---

## What It Does

The Intelligence Hub is a **real-time agentic reasoning engine** that synthesizes data across all your Miru modules (receipts, fuel, school, calendar, saves) and generates **personalized insights, forecasts, and smart recommendations**.

### Core Insight Dimensions

1. **⛽ Fuel Intelligence**
   - Price trend (up/down/stable) with % change
   - Next refill forecast (days)
   - Best day/price to fill up
   - Cost optimization recommendations

2. **💳 Spend Intelligence**
   - Trend analysis (up/down/stable vs normal)
   - 4-week forecast with ASCII chart
   - Top 3 ways to save money
   - Category breakdown by spend

3. **📍 Location Intelligence**
   - Most visited places & cost per visit
   - Cheaper alternatives (save £X/month)
   - Neighborhood patterns

4. **🏫 School & Calendar Intelligence**
   - Busy level indicator (normal/busy/very busy)
   - Busiest day forecast
   - Impact on lifestyle & routine

5. **🎯 Lifestyle Patterns**
   - Habit changes detected (saves down, cafe visits up, etc.)
   - Activity level trend
   - Unusual patterns & alerts

6. **⚠️ Anomalies & Alerts**
   - Spending spikes detected
   - Missing patterns (e.g., no lunch when you usually eat out)
   - One-off unusual events

7. **⭐ Smart Recommendations**
   - Top 3 actionable items (save £X, switch to Y, schedule Z)
   - Optimization opportunities
   - Action items checklist

---

## Architecture

### Backend Stack

```
Flask (sms_service.py)
    ↓
/api/insights/* endpoints
    ↓
Intelligence Engine (intelligence_engine.py)
    ↓
Groq LLM (Mixtral 8x7b)
    ↓
Data Aggregation Layer
    ├── Receipts (spend by category/merchant)
    ├── Fuel (price history & trends)
    ├── School (events & busy days)
    ├── Saves (by type: books/shows/articles/music/places)
    ├── Calendar (commitments & patterns)
    └── Commute (location & frequency)
```

### API Endpoints

**1. `/api/insights/full` — Full Intelligence Report**
```
GET /api/insights/full?wa=<phone>

Response: {
  "success": true,
  "timestamp": "2026-07-02T21:30:00Z",
  "data_summary": {...},
  "insights": {
    "fuel": {...},
    "spend": {...},
    "location": {...},
    "school": {...},
    "lifestyle": {...},
    "anomalies": [...],
    "recommendations": [...],
    "forecast": {...}
  }
}
```

**2. `/api/insights/week` — Your Week Enhanced**
```
GET /api/insights/week?wa=<phone>
→ Your Week module with forecasts & anomalies
```

**3. `/api/insights/receipts` — Receipts with Spend Intel**
```
GET /api/insights/receipts?wa=<phone>
→ Receipts with trend analysis & savings tips
```

**4. `/api/insights/fuel` — Fuel with Price Intel**
```
GET /api/insights/fuel?wa=<phone>
→ Fuel module with refill forecasts & price trends
```

**5. `/api/insights/notifications` — Smart Alerts**
```
GET /api/insights/notifications?wa=<phone>

Response: {
  "notifications": [
    {
      "title": "⛽ Refill Soon",
      "message": "You'll need fuel in 3 days...",
      "action": "showScreen('fuel')",
      "priority": "high"
    },
    ...
  ]
}
```

### Frontend (index.html)

**Navigation:** 🧠 Intelligence button in sidebar (launches Intelligence Hub screen)

**Screen Sections:**
1. **4-Card Insights Grid** — Fuel | Spend | Lifestyle | Top Savings
2. **Fuel Details** — last fill, current price, best day, refill forecast
3. **Spend Analysis** — week comparison, 4-week forecast chart, category breakdown
4. **Location Patterns** — top places, cost per visit, cheaper alternatives
5. **School & Calendar** — busy level, busiest day, routine impact
6. **Action Items** — interactive checklist of things to do

**JavaScript Function:** `_intelligenceLoad()`
- Fetches `/api/insights/full?wa={phone}`
- Parses all insights
- Populates 40+ data fields
- Shows loading state & error handling

---

## Module Integration

### `module_integrations.py`

Helper functions to enhance existing modules:

```python
enhance_your_week_with_insights(week_data, intelligence)
  → Adds forecasts, anomalies, recommendations to Your Week

enhance_receipts_with_insights(receipts_data, intelligence)
  → Adds spend trends, savings tips, location intelligence

enhance_fuel_with_insights(fuel_data, intelligence)
  → Adds price trends, refill forecasts, action items

get_smart_notifications(intelligence)
  → Extracts actionable notifications prioritized by type
  → Returns: [{title, message, action, priority}, ...]
```

---

## Intelligence Engine (`intelligence_engine.py`)

### Class: `MiruIntelligence`

```python
class MiruIntelligence:
    model = "mixtral-8x7b-32768"  # Groq
    
    def get_full_intelligence(from_number, sb):
        """Main entry point. Aggregates all data & runs reasoning."""
        # 1. Fetch receipts (spend by category, top merchants)
        # 2. Fetch fuel data (price history, days since last fill)
        # 3. Fetch school events (count, busiest day)
        # 4. Fetch saves (by type, this week vs last week)
        # 5. Format data summary
        # 6. Call generate_insights() with Groq
        # 7. Return structured JSON
    
    def generate_insights(data):
        """Agentic reasoning using Groq LLM."""
        # Analyzes 7 dimensions (fuel, spend, location, school, lifestyle, anomalies, recommendations)
        # Returns: structured JSON with fuel/spend/location/school/lifestyle/anomalies/recommendations/forecast
```

### Cost Optimization

- **Model:** Groq Mixtral 8x7b-32768 ($0.24/MTok)
- **Max tokens:** 2000
- **Avg response:** ~1200 tokens = ~$0.0003 per call
- **Caching:** Results cached in browser for 5 minutes
- **Frequency:** On-demand (user clicks 🧠 button)

---

## Data Sources

| Module | Table | Query | Purpose |
|--------|-------|-------|---------|
| Receipts | `receipts` | Last 7 days | Spend trend, category breakdown |
| Fuel | `receipts` (filtered by category) | Last fill date/price | Price trends, refill forecast |
| School | `school_events` | Upcoming 7 days | Busy level, next busy day |
| Saves | `wa_saves` | This week vs last week | Lifestyle patterns |
| Calendar | `calendar` (if available) | Upcoming commitments | Routine impact |
| Location | `receipts` (by merchant) | Top merchants | Most visited places, cost analysis |

---

## Usage

### For Users

1. **Open Intelligence Hub**
   - Tap 🧠 button in sidebar
   - Wait for "Analyzing your data..." (5-10 seconds)

2. **Read Insights**
   - 4 cards at top: Fuel | Spend | Lifestyle | Savings
   - Sections below: detailed analysis, forecasts, action items

3. **Take Action**
   - Follow recommendations (refill fuel, switch stores, etc.)
   - Check action items checklist
   - Tap notifications to drill into modules

### For Developers

```python
# Get full intelligence
from intelligence_engine import MiruIntelligence
engine = MiruIntelligence()
result = engine.get_full_intelligence("whatsapp:447911123456", supabase_client)

# result.insights contains:
# - fuel: {price_trend, percent_change, next_fill_days, recommendation}
# - spend: {trend, vs_normal, forecast_next_week, top_saving}
# - location: {most_visited, cost_per_visit, alternative, savings}
# - school: {busy_level, impact, next_busy_day}
# - lifestyle: {change, activity_level}
# - anomalies: [...]
# - recommendations: [...]
# - forecast: {next_week_spend, next_fuel_date, action_items}
```

---

## Testing

**Integration test suite:** `test_intelligence_hub.py`

```bash
TEST_PHONE="whatsapp:447911123456" python3 test_intelligence_hub.py
```

Tests:
- ✅ HTML UI loads (screen-intelligence, sidebar button)
- ✅ Full intelligence endpoint works
- ✅ Module-specific endpoints (week, receipts, fuel)
- ✅ Notification generation
- ✅ Agentic reasoning (Groq LLM)

---

## Future Enhancements

**Phase 2 (Priority Order):**
1. Push notifications (WhatsApp alerts when fuel down 10%, spending 20% up, etc.)
2. Smart savings goals (track if recommendations worked)
3. Personalized timing (when to fill fuel based on your schedule)
4. Peer comparison (how your spending compares to similar households)
5. ML forecasting (replace Groq reasoning with trained models)

**Phase 3:**
- Historical trend analysis (6-month, 1-year patterns)
- Predictive alerts (fuel price about to spike, spend anomaly incoming)
- Integration with Twilio (WhatsApp briefing at optimal times)
- Export insights to calendar/email

---

## Monitoring

**Groq API Status**
- Check `GROQ_API_KEY` environment variable
- Monitor token usage in Groq console
- Log all errors to `/tmp/miru_intelligence.log`

**Miru Modules Dependency**
- Requires receipts data (at least 7 days of transactions)
- Requires fuel data (at least 1 fill recorded)
- Gracefully degrades if school/calendar data missing
- Anomaly detection skipped if insufficient historical data

---

## Related Files

- `intelligence_engine.py` — Core agentic reasoning engine
- `module_integrations.py` — Module enhancement functions
- `sms_service.py` — API endpoints (lines ~34550+)
- `templates/index.html` — UI screen & JavaScript
- `test_intelligence_hub.py` — Integration test suite
- `MIRU_STEERING.md` — Brief generation rules (separate system)

---

## Deployment Status

✅ **Deployed to Railway** — auto-deploys on `git push main`

**Endpoints live at:** `https://miru.humanagency.co/api/insights/*`

**Last updated:** 2 July 2026
