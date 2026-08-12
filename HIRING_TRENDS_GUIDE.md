# Hiring Trends Tracking System — Complete Guide

## What's Built

A complete system to track hiring trends over time by region, showing which countries/regions companies are expanding or contracting hiring in.

### **Components**

| File | Purpose |
|------|---------|
| `hiring_trends_tracker.py` | Core tracking service — fetches, analyzes, and stores hiring data |
| `hiring_snapshot_cron.py` | Daily cron job — runs at 6 AM UTC to capture snapshots |
| `scheduler_init.py` | Flask scheduler — manages the daily job |
| `sms_service.py` | 3 new API endpoints added for querying trends |
| Database table: `hiring_snapshots` | Stores historical hiring data |

---

## API Endpoints

### **1. Get Hiring Trend (Overall or by Region)**
```
GET /api/hiring-trends?name=Reckitt&region=Europe&days=30
```

**Response:**
```json
{
  "company_name": "Reckitt",
  "region": "Europe",
  "current_openings": 45,
  "previous_openings": 38,
  "trend": "↑ +18%",
  "trend_direction": "increasing",
  "change_count": 7,
  "period_days": 30,
  "history": [
    {"date": "2026-08-01", "openings": 38},
    {"date": "2026-08-08", "openings": 41},
    {"date": "2026-08-12", "openings": 45}
  ]
}
```

**Query Params:**
- `name` (required) — Company name
- `region` (optional) — Filter by region: "North America", "Europe", "Asia", "Remote"
- `days` (optional) — Days to look back (default: 30)

---

### **2. Get Regional Breakdown**
```
GET /api/hiring-trends/regional?name=Reckitt&days=30
```

**Response:**
```json
{
  "company_name": "Reckitt",
  "period_days": 30,
  "regions": {
    "Europe": {
      "current": 45,
      "previous": 38,
      "trend": "↑ +18%",
      "direction": "increasing"
    },
    "North America": {
      "current": 32,
      "previous": 35,
      "trend": "↓ -8%",
      "direction": "decreasing"
    },
    "Asia": {
      "current": 18,
      "previous": 18,
      "trend": "→ Stable",
      "direction": "stable"
    }
  }
}
```

---

### **3. Manually Trigger Snapshot** (for testing)
```
POST /api/hiring-snapshot?name=Reckitt
```

**Response:**
```json
{
  "status": "snapshot_taken",
  "company": "Reckitt",
  "data": {
    "company_name": "Reckitt",
    "snapshot_date": "2026-08-12",
    "regions_snapshot": {
      "Europe": 45,
      "North America": 32,
      "Asia": 18
    },
    "departments_snapshot": {
      "ENGINEERING": 20,
      "AI/ML": 8,
      "SALES": 6
    },
    "total_openings": 95
  }
}
```

---

## How It Works

### **Daily Snapshot Flow**

1. **6:00 AM UTC** — Scheduler triggers `run_daily_snapshots()`
2. For each tracked company (Apple, Google, Reckitt, etc.):
   - Fetch current hiring data from LinkedIn, Indeed, Adzuna
   - Extract region and department info
   - Store snapshot in `hiring_snapshots` table
3. **Historical data** is now available for trend analysis

### **Trend Calculation**

Compares snapshots over time:
- **↑ Increasing** — More openings now than N days ago
- **↓ Decreasing** — Fewer openings now than N days ago
- **→ Stable** — Same number of openings

Example:
```
30 days ago: 38 openings (Europe)
Today:       45 openings (Europe)
Change:      +7 (+18%)
Trend:       ↑ Increasing
```

---

## Tracked Companies

By default, the system tracks:
- **Tech:** Apple, Google, Microsoft, Amazon, Meta, Tesla, Netflix
- **Consumer:** Reckitt, Unilever, Nike, Adidas
- **Startups:** Monzo, Wise, Revolut

**Add more companies:**
```python
from hiring_snapshot_cron import add_company_to_tracking

add_company_to_tracking("Spotify")
```

---

## Deployment Checklist

### **✅ Step 1: Database (DONE)**
```sql
CREATE TABLE hiring_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_name TEXT NOT NULL,
  snapshot_date DATE NOT NULL,
  region TEXT,
  department TEXT,
  open_roles INT DEFAULT 0,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(company_name, snapshot_date, region, department)
);

CREATE INDEX idx_hiring_date ON hiring_snapshots(company_name, snapshot_date);
```

### **✅ Step 2: Install APScheduler**
In Railway environment, add to dependencies:
```bash
pip install apscheduler
```

Or update `requirements.txt`:
```
apscheduler==3.10.4
```

### **✅ Step 3: Deploy to Railway**
```bash
git add .
git commit -m "Add hiring trends tracking system"
git push  # Auto-deploys to Railway
```

### **✅ Step 4: Verify Scheduler Started**
After deploy, check Railway logs:
```
[app] === SCHEDULER INIT COMPLETE: True ===
[scheduler] Daily hiring snapshot job scheduled for 6:00 AM UTC
[scheduler] Registered: daily_hiring_snapshots -> Daily Hiring Snapshots
```

### **✅ Step 5: Test Endpoints**

**Manually trigger a snapshot:**
```bash
curl -X POST "https://intel.humanagency.co/api/hiring-snapshot?name=Reckitt"
```

**Check trends:**
```bash
curl "https://intel.humanagency.co/api/hiring-trends?name=Reckitt&region=Europe"
```

**Regional breakdown:**
```bash
curl "https://intel.humanagency.co/api/hiring-trends/regional?name=Reckitt"
```

---

## Frontend Integration

Add to your Intel `/company` page to show hiring trends:

```javascript
// Fetch regional trends
const response = await fetch(`/api/hiring-trends/regional?name=${companyName}`);
const trends = await response.json();

// Display trends
trends.regions.forEach(([region, data]) => {
  console.log(`${region}: ${data.trend} (${data.direction})`);
});
```

**Example UI:**
```
Hiring Trends — Last 30 Days

Europe ........... ↑ +18% (45 openings)
North America ... ↓ -8% (32 openings)  
Asia ............ → Stable (18 openings)
```

---

## Troubleshooting

### **No data in trends endpoint**
- Wait for first daily snapshot (runs at 6 AM UTC)
- Or manually trigger: `POST /api/hiring-snapshot?name=Reckitt`

### **Scheduler not starting**
- Check APScheduler is installed
- See Railway logs: `[scheduler] Daily hiring snapshot job scheduled`

### **Database errors**
- Verify `hiring_snapshots` table exists
- Check Supabase connection: `SUPABASE_URL` and `SUPABASE_KEY` env vars

### **No job data fetched**
- Check job APIs are configured: Adzuna API key, LinkedIn access, etc.
- Look at `hiring_signals_fetcher.py` for data source issues

---

## Future Enhancements

- [ ] Visualize trends on Intel `/company` page (chart)
- [ ] Alert when hiring drops >20% in a region
- [ ] Compare hiring trends across competitors
- [ ] Department-level trends (not just regional)
- [ ] Sentiment analysis on hiring descriptions
- [ ] Predictive: forecast hiring based on trends

---

## Files Modified/Created

**New Files:**
- `hiring_trends_tracker.py` — Core tracking service
- `hiring_snapshot_cron.py` — Daily cron job
- `scheduler_init.py` — Scheduler initialization
- `HIRING_TRENDS_GUIDE.md` — This guide

**Modified Files:**
- `sms_service.py` — Added 3 API endpoints + scheduler init

**Database:**
- `hiring_snapshots` — New table for historical data

---

## Questions?

Check the code comments in:
- `hiring_trends_tracker.py` — Main tracking logic
- `hiring_snapshot_cron.py` — Job configuration
- `scheduler_init.py` — Scheduler setup
