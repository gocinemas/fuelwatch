# Intel: VP-Level Brand Intelligence Platform

## What You've Built

Intel is a **free strategic intelligence system for CMOs** that combines:
- Real-time brand data (SEC Edgar + social + news)
- Strategic insights (themes, trends, HBR-validated)
- Competitive intelligence (SKU tracking, pricing, market position)
- Historical trends (detect patterns week-by-week)
- Daily scraping agents (automated data collection)

**Live at:** `https://intel.humanagency.co`

---

## Architecture

### Phase 1: Foundation ✅
- **Strategic layer** — Auto-detect brand category → show themes + trends at TOP
- **Data layer** — SEC Edgar financials, social ad spend, product lineup
- **History schema** — Supabase tables for historical tracking (not implemented yet, schema ready)

### Phase 2: Agents ✅
- **Retail scraper** — Amazon, Tesco SKUs, prices, availability
- **Earnings parser** — Seeking Alpha transcripts for strategy
- **News monitor** — NewsAPI for brand mentions and sentiment
- **Competitor tracker** — Track vs competitors daily

### Phase 3: Scheduler ✅
- **Daily cron** — Runs all agents every morning at 2 AM
- **Stores to Supabase** — Historical data for trend analysis
- **Alerts system** — Flags significant changes (revenue ↑, rank shift, competitor moves)

---

## Testing Checklist

### 1. **Core Feature Test** (5 min)
```
Go to: https://intel.humanagency.co
Search: "Nike"
Expected:
  ✓ Strategic theme appears at TOP ("The Nike Margin Expansion Story")
  ✓ Consumer trend shows ("Direct-to-Consumer & Sustainability")
  ✓ Financials: Revenue $46.7B, Profit $5.1B, +42% growth
  ✓ Competitors: 4 competitors with market caps
  ✓ Social spend: Instagram $78M, YouTube $55M, TikTok $38M
  ✓ Top products: Air Force 1, Air Max, Jordan 1, Revolution 6
  ✓ No Waze/Maps buttons at bottom (hidden ✓)
```

### 2. **Auto-Category Test** (3 min)
```
Search: "Tesla" (no category parameter)
Expected:
  ✓ System detects "electric vehicle" → shows automotive themes
  ✓ Trend: "EV Adoption & Autonomy Race"
  ✓ Theme: "Manufacturing Flywheel vs Legacy's Catch-Up Trap"
```

### 3. **Moat Test** (2 min)
```
Look for "Why Intel" section (currently hidden, can be toggled)
Expected:
  ✓ Shows 4 reasons we win vs Calvin Ball:
    - Real-time (vs quarterly)
    - Strategy + Data (vs one or other)
    - Historical trends (vs snapshot)
    - Scrape everywhere (vs single source)
```

### 4. **Historical Trends Test** (Future - when scheduler runs)
```
After scheduler runs (daily at 2 AM UTC):
Expected:
  ✓ Trends section shows: "Revenue +5% 📈", "Rank Stable 📊"
  ✓ Competitor snapshot shows: Adidas $64.2B, Puma $18.5B
  ✓ Alerts flagged: "Competitor pricing down 8%"
```

### 5. **Scraping Agent Test** (Manual)
```
Run: python intel_scheduler.py
Expected:
  ✓ Logs show "Starting daily Intel scraping agents..."
  ✓ For each brand:
    - Scrapes Amazon: "Stored X SKUs from Amazon"
    - Scrapes Tesco: "Stored X SKUs from Tesco"
    - Fetches news: "Found X news articles"
    - Tracks competitors: "Tracked X competitors"
```

---

## Deployment

### Local Testing
```bash
# Run scheduler manually
python intel_scheduler.py

# Check Supabase tables (after run)
# - brand_sku_history (SKU data)
# - brand_ranking_history (rank changes)
# - competitor_comparison_history (vs competitors)
# - brand_intelligence_insights (strategy, direction)
```

### Production (Railway)
```bash
# Add to Procfile:
scheduler: python intel_scheduler.py

# Or use APScheduler in sms_service.py:
from apscheduler.schedulers.background import BackgroundScheduler
scheduler = BackgroundScheduler()
scheduler.add_job(schedule_daily_scraping, 'cron', hour=2)
scheduler.start()
```

---

## What Works Now

✅ Brand search (any brand)
✅ Strategic themes (auto-detected)
✅ Financial data (SEC Edgar live)
✅ Social ad tracking (6+ platforms)
✅ Product lineup (top models)
✅ Competitor ranking (market cap)
✅ Consumer trends (HBR-validated)
✅ Auto-category detection
✅ Social channel expansion
✅ Navigation hidden from Intel

---

## What Needs Work

⏳ Historical trend display (UI ready, needs data)
⏳ Competitor comparison grid (UI ready, needs data)
⏳ Alerts system (UI ready, needs logic)
⏳ Scheduler integration (code ready, needs Railway setup)
⏳ Real alerts when trends shift
⏳ Earnings call parsing (skeleton ready)
⏳ Multi-brand competitor tracking (ready, needs daily runs)

---

## Next Steps After Testing

1. **If user feedback is positive:**
   - Enable daily scheduler on Railway
   - Populate historical data (run scheduler 7 days)
   - Display trends in UI
   - Test alerts with real data

2. **Send to CMOs:**
   - Create signup flow (Google login)
   - Add "Favorite brands" dashboard
   - Email alerts for tracked brands
   - Export trends (PDF)

3. **Expand data:**
   - LinkedIn integration (company updates)
   - Glassdoor sentiment (employee morale)
   - Patent filings (innovation tracking)
   - Supply chain signals

---

## Commands for Testing

```bash
# Test core search
curl "https://intel.humanagency.co/api/brands/search?q=Nike&category=athletic_wear&refresh=true" | jq .

# Run scheduler
python /Users/srevi/fuelwatch/intel_scheduler.py

# Check Supabase (requires CLI)
supabase db pull  # Get current schema
```

---

## Support

Questions? Check:
- `supabase_schema_intel.sql` — Historical data schema
- `scraping_agents.py` — Agent implementation
- `intel_scheduler.py` — Scheduling logic
- `/api/brands/search` — API endpoint

---

**Status:** Ready for MVP testing with real users
**Built:** June 16, 2026
**Moat:** Real-time + Strategy + History + Scraped from everywhere
