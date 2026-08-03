# INTEL Product — Quick Start Guide

**Live:** https://intel.humanagency.co

---

## One-Line Summaries

| Feature | URL Pattern | What It Does |
|---------|-------------|--------------|
| **5-Signal Dashboard** | `/intelligence/5signals/COMPANY` | One-page card with Stock, Sentiment, Trends, Hiring, News |
| **Comparison** | `/intelligence/compare/A-vs-B-vs-C` | Side-by-side 5 signals, winners highlighted |
| **API: Signals** | `/api/signals/COMPANY` | JSON with all 5 signals |
| **API: Comparison** | `/api/compare/A-vs-B` | JSON comparison of 2-3 companies |
| **Subscribe** | `/reports/subscribe` | Email subscription form |
| **Preview Report** | `/api/reports/preview/COMPANY` | See what the weekly email looks like |

---

## Copy-Paste URLs

### Dashboards (Visit in browser)

```
https://intel.humanagency.co/intelligence/5signals/reckitt
https://intel.humanagency.co/intelligence/5signals/henkel
https://intel.humanagency.co/intelligence/5signals/unilever

https://intel.humanagency.co/intelligence/compare/reckitt-vs-henkel-vs-unilever
https://intel.humanagency.co/intelligence/compare/reckitt-vs-unilever
```

### APIs (Use in code)

```bash
# Get 5 signals as JSON
curl https://intel.humanagency.co/api/signals/reckitt

# Compare 2-3 companies
curl https://intel.humanagency.co/api/compare/reckitt-vs-henkel

# Preview weekly report
curl https://intel.humanagency.co/api/reports/preview/reckitt
```

### Web Forms

```
https://intel.humanagency.co/reports/subscribe
```

---

## Each Signal Explained

### 📈 Stock
- **Shows:** Direction (↑↓→) + % change + ticker
- **Source:** Yahoo Finance (real-time, 15-min delayed)
- **Example:** "↓ -3% (RKT.L)" = Stock down 3% vs previous close

### 💬 Sentiment
- **Shows:** 0-100 score
- **Source:** Hacker News posts + Trustpilot reviews
- **Meaning:** 60+ = positive | 40-60 = mixed | <40 = negative

### 📊 Trends
- **Shows:** Search interest level + direction
- **Source:** Google Trends (weekly data)
- **Example:** "↓ 28" = Interest level 28, declining

### 👥 Hiring
- **Shows:** Job count + direction (↑↑ aggressive / → stable / ↓↓ cutting)
- **Source:** LinkedIn (currently demo data for MVP)
- **Example:** "↑↑ 427 roles" = Aggressive hiring, 427 open positions

### 📰 News
- **Shows:** Article count (30-day window)
- **Source:** NewsAPI (English-language press)
- **Example:** "5 articles" = 5 news mentions in last 30 days

---

## Real Example: Reckitt

### Dashboard View
```
Visit: /intelligence/5signals/reckitt

RECKITT
────────────────────────────
📈 Stock:    ↓ -3% (RKT.L)
💬 Sentiment: 48/100
📊 Trends:    ↓ 28
👥 Hiring:    ↑↑ 427
📰 News:      5 articles

Signal Interpretation:
"Reckitt is aggressively hiring (427 roles, up 3m) 
signals confidence in new markets, but stock price 
decline and mixed sentiment suggest market skepticism. 
Watch: Can they execute on hiring plans?"
```

### What This Tells a Deal-Maker
✓ **Opportunity signal:** Heavy hiring = company is investing
✗ **Risk signal:** Stock down + low sentiment = market is skeptical
→ **Action:** Deep dive on whether new hiring will drive margin recovery

---

## Comparison: Reckitt vs Henkel vs Unilever

### View in Browser
```
Visit: /intelligence/compare/reckitt-vs-henkel-vs-unilever
```

### Quick Read (1 minute)
```
           | Reckitt    | Henkel     | Unilever   
───────────┼────────────┼────────────┼────────────
Stock      | ↓ -3%      | ↑ +2%      | → flat
Sentiment  | 48/100     | 52/100     | 65/100 ✓ LEADER
Trends     | ↓ 28       | → 45       | ↑ 58 ✓ LEADER
Hiring     | ↑↑ 427 ✓   | ↑ 234      | → 89 
News       | 5          | 3          | 8
```

### Deal-Maker Insights
- **Strongest:** Unilever (sentiment + trends up, stable stock)
- **Most aggressive:** Reckitt (hiring hard despite stock pressure)
- **Most stable:** Henkel (modest but steady across signals)

---

## Weekly Email Report

### How to Subscribe
1. Visit: https://intel.humanagency.co/reports/subscribe
2. Enter email + primary company (e.g., Reckitt)
3. Add competitors (optional, max 2)
4. Select frequency (weekly = Monday morning)
5. Click Subscribe

### What You'll Get (Every Monday)
```
SUBJECT: Reckitt Intelligence Brief — Week of Aug 3, 2026

5-SIGNAL SNAPSHOT
Stock: ↓-3% | Sentiment: 48/100 | Trends: ↓28 | Hiring: ↑↑427 | News: 5

COMPETITIVE POSITION
Leader: Unilever (65/100 sentiment, ↑ stock)
Challenger: Henkel (52/100, stable hiring)
Pressure: Reckitt (48/100, declining sentiment)

SIGNAL INTERPRETATION
Reckitt is aggressively hiring but market is skeptical...

KEY WATCH ITEMS
✓ Monitor hiring execution: Can Reckitt hit 427 role target?
✓ Declining sentiment: Review customer feedback
✓ High news volume: Track earnings releases
```

---

## For Developers

### Simple JavaScript (Embed in your app)

```javascript
// Fetch 5 signals
async function getSignals(company) {
  const res = await fetch(`/api/signals/${company}`);
  return res.json();
}

// Usage
getSignals('reckitt').then(data => {
  console.log(`${data.company} sentiment: ${data.sentiment.score}/100`);
  console.log(`Hiring: ${data.hiring.count} roles (${data.hiring.direction})`);
});
```

### Python Integration

```python
from intelligence_5signals import get_5_signals, BriefGenerator

# Get signals
signals = get_5_signals('Reckitt')

# Generate brief
brief = BriefGenerator.generate_brief('Reckitt', signals)

# Compare
from intelligence_5signals import get_comparison_signals
comparison = get_comparison_signals(['Reckitt', 'Henkel', 'Unilever'])
```

### Generate Weekly Report Programmatically

```python
from intelligence_reports import ReportGenerator

html = ReportGenerator.generate_weekly_report(
    primary_company='Reckitt',
    competitor_companies=['Henkel', 'Unilever']
)

# Send via email
send_email(recipient_email, 'Intelligence Brief', html)
```

---

## Supported Companies

| Company | Ticker |
|---------|--------|
| Reckitt | RKT.L |
| Henkel | HEN3.DE |
| Unilever | UL.L |
| SC Johnson | SCJW |

**Missing a company?** Add the ticker to `intelligence_5signals.py` line 17 (`TICKER_MAP`)

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "No data" in Stock | Ticker not in TICKER_MAP or Yahoo Finance down |
| Sentiment always 50 | Hacker News fetch timeout — try again in 5 min |
| Hiring shows "demo data" | MVP limitation — upgrade coming Q4 2026 |
| NewsAPI returns 0 articles | API key limit hit — use production key |

---

## What's Real, What's Demo

| Signal | Status | Details |
|--------|--------|---------|
| Stock | ✅ REAL | Yahoo Finance API |
| Sentiment | ✅ REAL | Hacker News + Trustpilot |
| Trends | ✅ REAL | Google Trends |
| Hiring | 🟡 DEMO | Hardcoded numbers for MVP |
| News | ✅ REAL | NewsAPI (with demo key) |

---

## Next Steps

- [ ] **Try it:** Visit `/intelligence/5signals/reckitt`
- [ ] **Compare:** Visit `/intelligence/compare/reckitt-vs-henkel-vs-unilever`
- [ ] **Subscribe:** Go to `/reports/subscribe` and enter your email
- [ ] **Integrate:** Use `/api/signals/...` in your tools
- [ ] **Share:** Copy the dashboard URL to teammates

---

**Questions?** Email hello@humanagency.co | **Docs:** See INTEL_PRODUCT_README.md
