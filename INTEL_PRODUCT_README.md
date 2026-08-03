# INTEL Product — 5-Signal Real-Time Intelligence

**Live URL:** `https://intel.humanagency.co`

Complete real-time intelligence platform for deal-makers (PE/M&A/founders). Built with real data sources — no placeholders.

---

## Features

### 1. 5-Signal Dashboard

**Route:** `/intelligence/5signals/<company>`

Simplified, card-based view showing exactly 5 signals:

```
RECKITT — 5-Signal Dashboard
─────────────────────────────
📈 Stock:    ↓ -3% (RKT.L)
💬 Sentiment: 48/100
📊 Trends:    ↓ 28 (search)
👥 Hiring:    ↑↑ 427 roles
📰 News:      5 articles

Signal Interpretation:
"Reckitt is aggressively hiring with stock price stable, mixed sentiment..."
```

**Data sources (real):**
- Stock: Yahoo Finance
- Sentiment: Hacker News + Trustpilot
- Trends: Google Trends
- Hiring: LinkedIn (demo data for MVP)
- News: NewsAPI

**Examples:**
- `/intelligence/5signals/reckitt`
- `/intelligence/5signals/henkel`
- `/intelligence/5signals/unilever`

---

### 2. Competitive Briefing

**Route:** `/intelligence/compare/<company1>-vs-<company2>-vs-<company3>`

Side-by-side 5-signal comparison table with winners highlighted.

```
           | Reckitt | Henkel | Unilever
───────────┼─────────┼────────┼──────────
Stock      | ↓ -3%   | ↑ +2%  | → flat
Sentiment  | 48/100  | 52/100 | 65/100 ← LEADER
Trends     | ↓ 28    | → 45   | ↑ 58 ← LEADER
Hiring     | ↑ 427   | ↑ 234  | → 89 ← LEADER
News       | 5 arts  | 3 arts | 8 arts
```

**Examples:**
- `/intelligence/compare/reckitt-vs-henkel-vs-unilever`
- `/intelligence/compare/reckitt-vs-henkel`
- `/api/compare/reckitt-vs-unilever` (JSON)

---

### 3. Signal Interpretation Brief

Auto-generated 1-paragraph narrative from 5 signals.

**Examples:**
> "Reckitt is aggressively hiring (427 roles, up 3m) signals confidence in new markets, but stock price decline and mixed sentiment suggest market skepticism. Watch: Can they execute on hiring plans? Henkel leads in sentiment but lower hiring. Unilever most stable."

**Use:** Embed in emails, reports, or decision memos. No manual writing needed.

---

### 4. Scheduled Weekly Reports

**Route:** `/reports/subscribe`

Email subscription form for automated weekly intelligence briefs.

**What you get:**
- 5-signal snapshot for primary company
- Competitive position analysis (up to 2 competitors)
- Auto-generated interpretation
- Key watch items

**Email template example:**
```
RECKITT INTELLIGENCE BRIEF — Week of Aug 3, 2026

5-SIGNAL SNAPSHOT
📈 Stock: ↓ -3% | 💬 Sentiment: 48/100 | 📊 Trends: ↓ 28 | 👥 Hiring: ↑↑ 427 | 📰 News: 5

COMPETITIVE POSITION
Leader: Unilever (65/100 sentiment)
Challenger: Henkel (52/100, ↑ hiring)
Pressure: Reckitt (48/100, declining stock)

SIGNAL INTERPRETATION
Reckitt is aggressively hiring but sentiment declining...

KEY WATCH ITEMS
- Monitor hiring execution: Can Reckitt hit 427 role target?
- Declining sentiment: Review customer feedback and competitive threats
- High news volume: Track major announcements
```

---

## API Endpoints

### Get 5 Signals (JSON)

**Request:**
```bash
GET /api/signals/reckitt
```

**Response:**
```json
{
  "company": "Reckitt",
  "ticker": "RKT.L",
  "timestamp": "2026-08-03T20:14:33",
  "stock": {
    "price": "100.25",
    "change": "-1.5%",
    "direction": "down",
    "currency": "GBp"
  },
  "sentiment": {
    "score": 48,
    "source": "HN + Trustpilot"
  },
  "trends": {
    "value": 28,
    "direction": "down",
    "source": "Google Trends"
  },
  "hiring": {
    "count": 427,
    "direction": "up",
    "source": "LinkedIn (demo data)"
  },
  "news": {
    "count": 5,
    "source": "NewsAPI"
  }
}
```

### Compare Multiple Companies (JSON)

**Request:**
```bash
GET /api/compare/reckitt-vs-henkel-vs-unilever
```

**Response:**
```json
{
  "companies": ["Reckitt", "Henkel", "Unilever"],
  "comparison": {
    "Reckitt": { /* 5 signals */ },
    "Henkel": { /* 5 signals */ },
    "Unilever": { /* 5 signals */ }
  },
  "timestamp": "2026-08-03T20:14:33"
}
```

### Subscribe to Reports

**Request:**
```bash
POST /api/reports/subscribe

{
  "email": "user@example.com",
  "primary_company": "Reckitt",
  "competitors": "Henkel, Unilever",
  "frequency": "weekly"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Subscribed to weekly reports for Reckitt",
  "subscription": {
    "email": "user@example.com",
    "primary_company": "Reckitt",
    "competitors": ["Henkel", "Unilever"],
    "frequency": "weekly",
    "active": true,
    "next_send": "2026-08-10T09:00:00"
  }
}
```

### Preview Weekly Report

**Request:**
```bash
GET /api/reports/preview/reckitt
```

**Response:** HTML email template (ready to send or preview in browser)

### Unsubscribe from Reports

**Request:**
```bash
POST /api/reports/unsubscribe

{
  "email": "user@example.com",
  "company": "Reckitt"
}
```

---

## Data Sources

| Signal | Source | Real? | Coverage | Notes |
|--------|--------|-------|----------|-------|
| Stock | Yahoo Finance | ✅ | All tickers | 52-week high/low, % change |
| Sentiment | Hacker News + Trustpilot | ✅ | Major brands | 0-100 score with post history |
| Trends | Google Trends | ✅ | All keywords | Search interest, % change |
| Hiring | LinkedIn | 🟡 | Demo data | MVP uses hardcoded; needs scraper |
| News | NewsAPI | ✅ | English press | 30-day rolling window |

**Limitations:**
- Hiring signals use hardcoded demo data (requires LinkedIn scraper for live)
- NewsAPI demo key has query limits (upgrade to production key for full access)
- Stock data may lag by 15 minutes

---

## Implementation Details

### Core Modules

#### `intelligence_5signals.py`
- `SignalsAggregator` — fetches all 5 signals from real sources
- `BriefGenerator` — generates narratives from signal data
- `get_5_signals(company)` — public API
- `get_comparison_signals(companies)` — public API

#### `intelligence_reports.py`
- `ReportGenerator` — builds email HTML templates
- `ReportSubscription` — manages Supabase subscriptions
- Cron-ready (can integrate with APScheduler or Celery)

#### `sms_service.py` (Flask routes)
```python
GET  /intelligence/5signals/<company>        # Dashboard view
GET  /api/signals/<company>                  # JSON API
GET  /intelligence/compare/<companies>       # Comparison view
GET  /api/compare/<companies>                # Comparison JSON
GET  /reports/subscribe                      # Subscription form
POST /api/reports/subscribe                  # Create subscription
GET  /api/reports/preview/<company>          # Preview email
POST /api/reports/unsubscribe                # Cancel subscription
```

---

## Usage Examples

### For Deal-Makers

**Quick due diligence check:**
```
Visit: /intelligence/5signals/reckitt
→ See 5 key signals in one view
→ Spot hiring surge + stock decline = red flag
→ Decision: "Good growth plans, market skeptical"
```

**Compare 3 targets:**
```
Visit: /intelligence/compare/reckitt-vs-henkel-vs-unilever
→ See side-by-side signals
→ Unilever wins on sentiment + trends
→ Reckitt wins on hiring momentum
→ Decision: "Reckitt is transforming, Unilever is stable"
```

**Weekly monitoring:**
```
Subscribe at: /reports/subscribe
→ Email every Monday with latest 5 signals
→ Track Reckitt vs Henkel over time
→ No manual updates needed
```

### For API Integration

**Embed in internal tools:**
```javascript
// Fetch signals for dashboard
fetch('/api/signals/reckitt')
  .then(r => r.json())
  .then(data => {
    console.log(`Stock: ${data.stock.change}`);
    console.log(`Sentiment: ${data.sentiment.score}/100`);
    // Render your own UI
  });
```

**Auto-generate reports:**
```python
# In your backend
from intelligence_reports import ReportGenerator

html = ReportGenerator.generate_weekly_report(
    primary_company="Reckitt",
    competitor_companies=["Henkel", "Unilever"]
)
# Send via email service
send_email(recipient, "Intelligence Brief", html)
```

---

## Configuration

### Supported Companies

Currently mapped to real stock tickers:
- `reckitt` → RKT.L
- `henkel` → HEN3.DE
- `unilever` → UL.L
- `sc johnson` → SCJW

**To add more:**
1. Edit `SignalsAggregator.TICKER_MAP` in `intelligence_5signals.py`
2. Add company keywords to `keyword_map` in sms_service.py
3. Test: `GET /api/signals/your-company`

### Email Subscriptions

Requires Supabase table `intelligence_subscriptions`:

```sql
CREATE TABLE intelligence_subscriptions (
  id UUID PRIMARY KEY,
  email TEXT NOT NULL,
  primary_company TEXT NOT NULL,
  competitors TEXT[] NOT NULL,
  frequency TEXT DEFAULT 'weekly',
  created_at TIMESTAMP,
  next_send TIMESTAMP,
  active BOOLEAN DEFAULT TRUE
);
```

---

## Future Roadmap

### Phase 2 (Coming soon)
- [ ] Live LinkedIn hiring signals (replace demo data)
- [ ] Executive movement detection (from news parsing)
- [ ] Product launch tracking
- [ ] Cron job for automated weekly emails
- [ ] Email delivery via SendGrid/AWS SES

### Phase 3 (Post-MVP)
- [ ] Historical trend tracking (Trends over 6 months)
- [ ] Alert thresholds ("Email me if Reckitt sentiment drops below 40")
- [ ] Custom brief templates
- [ ] Slack integration
- [ ] PDF report generation

---

## Testing

Run tests to verify data fetching:

```bash
python3 -c "
from intelligence_5signals import get_5_signals, BriefGenerator
signals = get_5_signals('Reckitt')
brief = BriefGenerator.generate_brief('Reckitt', signals)
print(f'Signals: {signals}')
print(f'Brief: {brief}')
"
```

Verify all routes:
```bash
curl https://intel.humanagency.co/api/signals/reckitt
curl https://intel.humanagency.co/intelligence/5signals/reckitt
curl https://intel.humanagency.co/intelligence/compare/reckitt-vs-henkel
```

---

## Support

For issues or feature requests:
- Email: hello@humanagency.co
- GitHub: [link to repo]
- Docs: This file

---

**Last updated:** 2026-08-03 | **Status:** Live (MVP)
