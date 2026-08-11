# Hiring Focus Setup Runbook

**Time needed:** 10 minutes  
**Difficulty:** Easy (copy-paste SQL + run Python script)

---

## Step 1: Create Table in Supabase (3 minutes)

### Open Supabase
1. Go to https://app.supabase.com
2. Select your project
3. Click **SQL Editor** (left sidebar)
4. Click **New Query**

### Copy & Execute SQL

Copy this ENTIRE block and paste into the SQL editor:

```sql
-- Create company_hiring_focus table
CREATE TABLE IF NOT EXISTS public.company_hiring_focus (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name TEXT NOT NULL UNIQUE,
    hiring_growth_2025 FLOAT,
    ai_investment_score INT CHECK (ai_investment_score >= 0 AND ai_investment_score <= 5),
    strategic_direction TEXT,
    focus_areas JSONB,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index
CREATE INDEX IF NOT EXISTS idx_hiring_focus_company ON public.company_hiring_focus(company_name);

-- Enable RLS
ALTER TABLE public.company_hiring_focus ENABLE ROW LEVEL SECURITY;

-- Allow all operations
CREATE POLICY "Allow all operations" ON public.company_hiring_focus
    FOR ALL USING (TRUE) WITH CHECK (TRUE);
```

### Execute
- Click **Run** button (or press Cmd+Enter)
- Expected: Green checkmark ✅

**Status:** ✅ Table created

---

## Step 2: Load Data with Python (2 minutes)

### Run Bootstrap Script

Open terminal and run:

```bash
cd /Users/srevi/fuelwatch
python bootstrap_hiring_focus.py
```

### Expected Output
```
📊 Adding hiring focus for Reckitt...
✅ Reckitt: 12.0% hiring growth, AI score 4/5

📊 Adding hiring focus for Unilever...
✅ Unilever: 5.2% hiring growth, AI score 3/5

📊 Adding hiring focus for Henkel...
✅ Henkel: 2.1% hiring growth, AI score 2/5

...

✅ Hiring focus bootstrap complete!
```

**Status:** ✅ Data loaded

---

## Step 3: Verify in Supabase (2 minutes)

### Check Table Editor

1. Go back to Supabase
2. Click **Table Editor** (left sidebar)
3. Look for `company_hiring_focus` table
4. Click it → Should see ~10 rows

### Verify Reckitt Row

Look for row with:
- `company_name`: "Reckitt"
- `hiring_growth_2025`: 12.0
- `ai_investment_score`: 4
- `focus_areas`: JSON with AI/ML, APAC, DTC, R&D

**Expected:** ✅ All data present and correct

**Status:** ✅ Data verified

---

## Step 4: Test Endpoint (1 minute)

### Verify API Connection

Open terminal and test:

```bash
# Get Reckitt's hiring focus
curl "https://intel.humanagency.co/api/company/hiring-focus?company=Reckitt"
```

### Expected Response
```json
{
  "company": "Reckitt",
  "hiring_growth_2025": 12.0,
  "ai_investment_score": 4,
  "focus_areas": [
    {
      "area": "AI/ML Engineering",
      "growth": 40,
      "roles": 24,
      "reason": "Supply chain automation, personalization"
    },
    ...
  ]
}
```

**Status:** ✅ API working

---

## Step 5: View in Intel (1 minute)

### Test Single Company View

1. Go to https://intel.humanagency.co/company/qa
2. Search for "Reckitt"
3. Look for hiring trends section

**Expected:** You see:
- 📈 Hiring Velocity: +12.0% YoY
- 🤖 AI/ML Engineering (+40%)
- 🌏 APAC Expansion (+35%)
- 💻 DTC (+28%)
- 🧬 R&D (+15%)
- AI Score: ⭐⭐⭐⭐

**Status:** ✅ Single company view working

---

## Step 6: Compare Companies (1 minute)

### Test Comparison View

1. Go to https://intel.humanagency.co/company/compare
2. Add: Reckitt, Unilever, Henkel, P&G
3. Scroll to "Hiring Trends" section

**Expected:** Table showing:
```
Company  | Hiring Growth | Focus Areas       | AI Score
─────────┼───────────────┼───────────────────┼──────────
Reckitt  | +12.0%        | AI, APAC, DTC     | ⭐⭐⭐⭐
P&G      | +8.7%         | AI, Premium, Cloud| ⭐⭐⭐⭐⭐
Unilever | +5.2%         | DTC, India        | ⭐⭐⭐
Henkel   | +2.1%         | R&D, Europe       | ⭐⭐
```

**Status:** ✅ Comparison view working

---

## Troubleshooting

### Error: "Table already exists"
- This is fine — means table was created on first run
- Continue to Step 2

### Error: "Could not insert data"
- Make sure RLS policy was created (Step 1)
- Verify in Supabase: Settings → Security → RLS
- Should show policy: "Allow all operations" on `company_hiring_focus`

### Python script fails: "ModuleNotFoundError"
- Check you're in the right directory: `/Users/srevi/fuelwatch`
- Make sure Supabase env vars are set:
  ```bash
  echo $SUPABASE_URL
  echo $SUPABASE_KEY
  ```

### No data appears in Table Editor
- Wait 5 seconds for Supabase to sync
- Refresh browser (Cmd+R)
- Check script output for errors

---

## Success Checklist

- [x] SQL executed in Supabase (green checkmark)
- [x] Python script completed without errors
- [x] `company_hiring_focus` table visible in Table Editor
- [x] ~10 rows of company data present
- [x] Reckitt row shows 12.0% hiring growth + AI score 4
- [x] curl test returns JSON response
- [x] Single company view shows hiring trends
- [x] Comparison view shows hiring table

**All checked?** → Setup complete! 🎉

---

## What's Next

Now that hiring focus data is loaded:

1. **Tell sales team** — They can now see strategic focus when preparing deals
2. **Update Q&A** — Add handler to answer "What is Reckitt hiring for?"
3. **Monthly updates** — Re-run bootstrap_hiring_focus.py with updated hiring % each quarter
4. **Analyze patterns** — Use data to predict M&A targets by hiring patterns

---

## Quick Reference

**Command to reload data:**
```bash
python bootstrap_hiring_focus.py
```

**SQL to query directly:**
```sql
SELECT company_name, hiring_growth_2025, ai_investment_score 
FROM company_hiring_focus 
ORDER BY hiring_growth_2025 DESC;
```

**Clear all data (if needed):**
```sql
DELETE FROM company_hiring_focus;
```

---

**Questions?** Check SETUP_HIRING_FOCUS.md or INTEL_HIRING_TRENDS_FEATURE.md

**Ready to sell Intel with hiring insights?** You've got the data now! 🚀
