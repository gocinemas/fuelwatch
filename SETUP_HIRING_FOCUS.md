# Setup: Hiring Focus & AI Investment Tracking

Enable visibility into where companies are investing (AI, geographic expansion, R&D) and their strategic direction.

---

## Quick Setup (3 minutes)

### Step 1: Create Table in Supabase

Go to https://app.supabase.com → Your project → SQL Editor → New Query

Paste this SQL:

```sql
CREATE TABLE IF NOT EXISTS public.company_hiring_focus (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name TEXT NOT NULL UNIQUE,
    hiring_growth_2025 FLOAT,
    ai_investment_score INT CHECK (ai_investment_score >= 0 AND ai_investment_score <= 5),
    strategic_direction TEXT,
    focus_areas JSONB,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hiring_focus_company ON public.company_hiring_focus(company_name);

ALTER TABLE public.company_hiring_focus ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all operations" ON public.company_hiring_focus
    FOR ALL USING (TRUE) WITH CHECK (TRUE);
```

Click **Run** ✅

---

### Step 2: Populate Data

Run this bootstrap script:

```bash
python bootstrap_hiring_focus.py
```

This loads hiring data for 10+ companies:
- Reckitt (+12% hiring, AI score 4/5)
- Unilever (+5.2% hiring, AI score 3/5)
- Henkel (+2.1% hiring, AI score 2/5)
- P&G (+8.7% hiring, AI score 5/5)
- SC Johnson (-1.3% hiring, AI score 1/5)
- Pfizer, Moderna, Apple, Microsoft, Google

**Expected output:**
```
✅ Reckitt: 12.0% hiring growth, AI score 4/5
✅ Unilever: 5.2% hiring growth, AI score 3/5
✅ Henkel: 2.1% hiring growth, AI score 2/5
...
✅ Hiring focus bootstrap complete!
```

---

## What You Can Now See

### On Company Card
```
📈 Hiring Velocity: +12% YoY (Reckitt)

Key Focus Areas:
🤖 AI/ML Engineering (+40% YoY, 24 open roles)
🌏 APAC Expansion (+35% YoY, 67 open roles)
💻 Direct-to-Consumer (+28% YoY, 19 open roles)
🧬 R&D/Biotech (+15% YoY, 12 open roles)

Strategic Direction: Building AI-driven efficiency + geographic expansion into emerging markets

AI Investment Score: ⭐⭐⭐⭐ (4/5)
```

### On Comparison Page
```
HIRING STRATEGY COMPARISON

Company      | 2025 Growth | Key Focus         | AI Score
─────────────┼─────────────┼───────────────────┼─────────
Reckitt      | +12.0%      | AI/ML, APAC, DTC  | ⭐⭐⭐⭐
P&G          | +8.7%       | AI/ML, Premium    | ⭐⭐⭐⭐⭐
Unilever     | +5.2%       | DTC, India        | ⭐⭐⭐
Henkel       | +2.1%       | R&D, Europe       | ⭐⭐
SC Johnson   | -1.3%       | Cost reduction    | ⭐⭐
```

### In Q&A
**Ask:** "What is Reckitt focusing on this year?"  
**Answer:** "Reckitt is hiring 12% YoY. Strategic focus: AI/ML engineering (+40%), APAC expansion (+35%), and Direct-to-Consumer (+28%). They're also investing in premium product R&D. Overall: Building AI-driven efficiency while expanding into high-growth emerging markets."

---

## Data Schema

Each company record includes:

| Field | Example | Meaning |
|-------|---------|---------|
| company_name | Reckitt | Company name |
| hiring_growth_2025 | 12.0 | YoY hiring growth (%) |
| ai_investment_score | 4 | AI investment level (1-5) |
| strategic_direction | "Building AI-driven efficiency..." | What they're building toward |
| focus_areas | JSON array | Detailed hiring areas with growth %, roles, reasons |

**Example focus_areas:**
```json
[
  {
    "area": "AI/ML Engineering",
    "growth": 40,
    "roles": 24,
    "reason": "Supply chain automation, personalization"
  },
  {
    "area": "APAC Expansion",
    "growth": 35,
    "roles": 67,
    "reason": "M&A in SE Asia, emerging brands"
  }
]
```

---

## How to Interpret

**High Hiring Growth + High AI Score** (Reckitt, P&G)
- Building competitive advantage through tech
- Aggressive investment phase
- Expect M&A, new product launches

**Low Hiring Growth + Low AI Score** (Henkel, SC Johnson)
- Mature, cash-focused businesses
- Optimizing for efficiency, not growth
- Opportunity: they won't bid on growth acquisitions

**Balanced Hiring + High AI** (Apple, Microsoft)
- Dominant position, reinvesting to stay ahead
- Platform expansion, ecosystem plays

---

## Example Insights for Sales

**Talking to Reckitt Corp Dev:**
> "I see you're hiring 12% this year — faster than your competitors. 
> Looking at the patterns: You're investing 40% more in AI/ML, expanding into APAC, 
> and building DTC brands. 
> 
> That tells me you're screening acquisition targets that fit those three areas:
> AI-capable tech companies, APAC brands, and D2C platforms.
> 
> Intel can show you which targets to pursue and which competitors are bidding on the same companies."

**Talking to SC Johnson Corp Dev:**
> "Your hiring is down 1.3% — you're focused on efficiency, not growth.
> You're not competing with Reckitt on acquisitions. 
> 
> But that's an advantage: You can pick surgical, high-margin assets they'd pass on.
> Intel helps you find those — they're too small for Reckitt to notice."

---

## Updating the Data

Every quarter, update hiring growth:

```python
# Simple update
sb.table("company_hiring_focus").update({
    "hiring_growth_2025": 12.5,
    "last_updated": "2026-11-01"
}).eq("company_name", "Reckitt").execute()
```

---

## Why This Matters

✅ **Reveals strategy** — Where they hire tells you what they're building  
✅ **Finds patterns** — All AI companies hire fast; cost-focused companies shrink  
✅ **Predicts M&A** — APAC hiring → APAC acquisition targets likely  
✅ **Enables sales** — Use patterns to position Intel in discovery calls  
✅ **Differentiates** — Bloomberg/Refinitiv don't connect hiring to strategy like this

---

## Next Steps

1. Create table in Supabase (SQL above)
2. Run `python bootstrap_hiring_focus.py`
3. Check results in Supabase Table Editor
4. Go to intel.humanagency.co/company/qa → Search "Reckitt" → Hiring should show strategic focus

Done! 🚀

