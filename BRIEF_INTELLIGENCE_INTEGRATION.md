# Brief × Intelligence Hub Integration

**Status:** ✅ LIVE  
**Date:** 3 July 2026  

---

## What Changed

The home **Brief** now pulls real-time agentic insights from the **Intelligence Hub** and weaves them into the personalized daily summary.

### Before (Smart Brief Only)
```
Morning: 🚂 Train 08:47 in 25min · 🏫 Riaan: PE class TODAY · 📌 Team standup
```

### After (Smart Brief + Intelligence)
```
Morning: ⛽ Fill up in 3 days — prices UP · 💳 Spending 📈 — save £15 at Tesco · 🏫 Riaan: PE class TODAY
```

---

## How It Works

### 1. Data Flow

```
/api/home/brief request
    ↓
Parallel fetch (~13 data sources):
  ├── fuel, weather, trains
  ├── school, calendar, deliveries
  ├── spend, saves, gmail
  ├── traffic (6-10am weekdays only)
  └── **NEW: intelligence (Groq agentic reasoning)**
    ↓
Build brief from:
  • Smart patterns (time-of-day, day-type)
  • Active trip, school events, commute
  • Spend anomalies
  • **NEW: Fuel refill forecasts**
  • **NEW: Spend trend + savings tips**
  • **NEW: Location recommendations**
  • **NEW: Lifestyle patterns**
  • **NEW: Anomaly alerts**
    ↓
Rank by priority (1-100 points)
    ↓
Return top 3 insights as brief
    ↓
Include full intelligence in response for frontend
```

### 2. Intelligence Signals in Brief

| Signal | Priority | Example |
|--------|----------|---------|
| ⛽ Fuel refill forecast | 85 | "Fill up in 3 days — prices UP" |
| 💳 Spend trend | 60 | "Spending 📈 — save £15 at Tesco" |
| 💰 Location savings | 50 | "Save £45/month by switching" |
| 🎯 Lifestyle changes | 35 | "Activity level down — try something new?" |
| ⚠️ Anomalies | 45 | "Unusual spending pattern detected" |

Traditional signals still have priority:
- 🛣️ Active trip: 100
- 🏫 School event (TODAY): 90
- 🚂 Train urgent: 100
- 🏫 School event (TOMORROW): 80

### 3. Response Structure

```json
{
  "brief": "Morning: ⛽ Fill up in 3 days — prices UP · 💳 Spending 📈 — save at Tesco",
  "context": {
    "fuel": {...},
    "weather": {...},
    "trains": {...},
    "school": {...},
    "intelligence": {           // <-- NEW
      "fuel": {
        "price_trend": "up",
        "percent_change": 3.5,
        "next_fill_days": 3,
        "recommendation": "Fill on Thursday"
      },
      "spend": {
        "trend": "up",
        "vs_normal": "+15%",
        "forecast_next_week": 250,
        "top_saving": "Switch to Tesco (save £15)"
      },
      "location": {
        "most_visited": "Waitrose",
        "cost_per_visit": 45,
        "alternative": "Tesco",
        "savings": "£45/month"
      },
      "anomalies": ["Unusual cafe spending pattern"],
      ...
    }
  },
  "intelligence": {             // <-- Also top-level for easy access
    "fuel": {...},
    "spend": {...},
    ...
  },
  "tod": "morning",
  "day_type": "midweek",
  ...
}
```

---

## Frontend Integration

The brief already displays in the home screen. Now it includes intelligence-driven insights.

**Example morning briefs by day/hour:**

| Time | Old Brief | New Brief (Intelligence) |
|------|-----------|-------------------------|
| **Mon 7am** | 🚂 Train 07:32 in 10min · 🏫 Maths TEST | ⛽ Fill up in 2 days (prices down 4%) · 🚂 Train 07:32 in 10min |
| **Tue 6pm** | 📌 Dinner with Sarah | 💳 Spending up 20% this week — try Tesco · 🏫 Inaaya dance 18:45 |
| **Wed 9am** | 🏫 School holidays start tomorrow | 🎯 Activity level down this week · ☀️ Good weather for parks |
| **Fri 5pm** | 🎉 Weekend! | 💰 Save £60/month switching cafes · 📌 Weekend plans ready |

---

## Time-of-Day Rules (From MIRU_STEERING.md)

Intelligence prioritization respects time of day:

### **6-10am (Morning Commute)**
Priority order:
1. 🚂 Trains (urgent)
2. ⛽ Fuel (refill forecast)
3. 🏫 School (today/tomorrow)
4. 💳 Spend (weekly alerts)

### **10am-5pm (Daytime)**
Priority order:
1. 📅 Calendar (meetings)
2. 💳 Spend (unusual patterns)
3. 📦 Deliveries (arriving today)
4. ⛽ Fuel (upcoming refill)

### **5-9pm (Evening)**
Priority order:
1. 🏫 School (upcoming events)
2. 💰 Location recommendations (where to eat)
3. 📌 Personal events
4. 🎯 Lifestyle insights

### **9pm+ (Night / At Home)**
Priority order:
1. ✅ No suggestions (respect rest)
2. 📚 Content saves only (shows, articles)
3. 📈 Historical insights (weekly summary)
4. 🛑 NO fuel/location/food suggestions

---

## Cost

**Intelligence fetch for brief:** ~$0.0005 per call (Groq Mixtral)

- Brief called: ~5-10 times/day per user (manual + WhatsApp)
- Cost: $0.003-0.006/day per user = $0.10-0.20/month
- Total Miru fleet: < $2/month for brief intelligence

**Negligible compared to:**
- Trains API calls ($0.10 per call, 5 calls/day)
- Weather API calls
- Gmail polling

---

## Configuration

### Enable/Disable Intelligence in Brief

To temporarily disable intelligence fetch (if Groq is down):

```python
# In /api/home/brief, change:
futures["intelligence"] = pool.submit(_get_brief_intelligence, from_number, lib._sb())

# To:
# futures["intelligence"] = pool.submit(_get_brief_intelligence, from_number, lib._sb())  # Disabled
ctx["intelligence"] = {}  # Empty intelligence
```

### Adjust Priority Scores

Edit `_build_super_smart_brief()` function:

```python
# Fuel refill forecast
if fuel.get("next_fill_days") and fuel["next_fill_days"] <= 3:
    insights.append(f"⛽ Fill up in {fuel['next_fill_days']} days — prices {fuel.get('price_trend', 'stable').upper()}")
    priority_score["fuel_refill"] = 85  # <-- Change priority here (0-100)
```

---

## What Intelligence Adds to Brief

### **Smart Forecasting**
- Fuel: "Fill up in 3 days when prices dip 4%"
- Spend: "Forecast shows £285 next week (up 15%)"

### **Personalized Recommendations**
- Location: "Save £45/month switching to Tesco"
- Lifestyle: "Activity level down — try something new?"

### **Anomaly Detection**
- Spending: "Unusual cafe spending this week (+£20)"
- Pattern: "Haven't visited your favorite coffee shop in 5 days"

### **Context Awareness**
- School busy: "Busiest school week — plan groceries now"
- Commute: "Trains typically 5min late Thursdays"

---

## Testing the Integration

### Manual Test
```bash
# Get brief with intelligence
curl "https://miru.humanagency.co/api/home/brief?token=YOUR_TOKEN"

# Check 'intelligence' field in response
# Should see: fuel, spend, location, school, lifestyle, anomalies, recommendations
```

### Expected Brief Evolution
1. **Without intelligence:** "Morning: 🚂 Train 07:32 · 🏫 School today"
2. **With intelligence:** "Morning: ⛽ Fill up in 2 days (prices UP) · 🏫 School today"

### Groq Timeout Handling
- If intelligence fetch times out (>2 sec):
  - Brief builds without intelligence
  - Falls back to smart brief (time-based patterns)
  - User still gets useful brief, no UI error
  - Log warns: "[brief] intelligence: timed out"

---

## Related Documentation

- `INTELLIGENCE_HUB.md` — Core intelligence engine
- `MIRU_STEERING.md` — Time-of-day brief rules
- `BUILD_SUMMARY_INTELLIGENCE_HUB.md` — Complete build overview

---

## Future Enhancements

**Phase 2:**
- WhatsApp push alerts when key intelligence changes
- Integrate forecasts into Your Week module
- Show intelligence trends in Receipts view

**Phase 3:**
- Learn user preferences for signal importance
- Suppress irrelevant insights (user disables "spend alerts")
- Personalize brief length by time of day

**Phase 4:**
- Multi-device sync (brief adapts to device type)
- Predictive escalation (alert user 6 hours before unusual pattern)
- Integration with calendar (schedule actions based on forecasts)

---

## Status

✅ **DEPLOYED & LIVE**  
✅ **Auto-deploys on git push main**  
✅ **Graceful degradation if Groq timeout**  
✅ **No UI changes needed**  

**User experience:** Brief automatically becomes smarter with zero friction.

---

**Last updated:** 3 July 2026
