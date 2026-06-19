# Phase 1 Production Test Plan

## Status: Ready to Deploy

✅ **Local Tests Pass:**
- Data validation: 60 records, all fields valid
- API structure: Response format correct
- Market entry scoring: Logic working
- PPP pricing: Validated

---

## Production Deployment Checklist

### Step 1: SSH into Railway

```bash
railway login
railway link --project d114e3c5-e1e8-4e3c-9249-fa78f182bcda
railway shell
```

### Step 2: Create Supabase Schema

```bash
# Inside Railway shell
psql -h db.xxxxx.supabase.co -U postgres -d postgres << EOF
$(cat migrations/phase1_schema.sql)
EOF
```

Or go to Supabase Console:
1. Dashboard → SQL Editor
2. Paste `migrations/phase1_schema.sql`
3. Run

### Step 3: Import 60 Brand Records

```bash
# Run the batch insert script
python3 phase1_batch_insert.py

# Output should show:
# [batch_insert] Loaded 60 records from ...
# [batch_insert] Normalized 60 records
# [batch_insert] Inserting into brand_phase1_intelligence...
# [batch_insert] ✓ Successfully inserted 60 records
```

### Step 4: Verify Insertion

**In Railway SQL console:**

```sql
-- Check total count
SELECT COUNT(*) FROM brand_phase1_intelligence;
-- Expected: 60

-- Check by category
SELECT category, COUNT(*) FROM brand_phase1_intelligence 
GROUP BY category;
-- Expected: beverages 30, skincare 30

-- Check by market
SELECT market_country, COUNT(*) FROM brand_phase1_intelligence 
GROUP BY market_country;
-- Expected: India 20, United Kingdom 20, United States 20

-- Spot check: Neutrogena UK
SELECT brand_name, market_country, positioning_tier, price_local, 
       category_growth_cagr_3yr, confidence_score 
FROM brand_phase1_intelligence 
WHERE brand_name = 'Neutrogena' AND market_country = 'United Kingdom';
-- Expected: economy tier, £8.99, 3.5% growth, 90% confidence
```

---

## Live API Tests

### Test 1: Retrieve Brand Data

```bash
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Neutrogena&market_country=United%20Kingdom"

# Expected response:
{
  "brand_name": "Neutrogena",
  "positioning_tier": "economy",
  "price_local": 8.99,
  "price_currency": "GBP",
  "confidence_score": 90
}
```

### Test 2: Market Entry Score

```bash
curl "https://miru.humanagency.co/api/brand/phase1/score?brand_name=Pepsi&market_country=India"

# Expected response:
{
  "entry_score": 45.2,
  "recommendation": "yellow",
  "recommendation_text": "Conditional entry - requires strategy",
  "factors": {
    "market_size": 120.0,
    "category_growth": 80.0,
    "purchasing_power": 25.0
  }
}
```

### Test 3: UI Display

```bash
# Visit the brand page in browser
open "https://miru.humanagency.co/brand/full?search=Dove"

# Verify:
✓ Brand fundamentals display (founded, HQ, website)
✓ Positioning tier shows
✓ Target segment displays
✓ Pricing shows (PPP-adjusted for market)
✓ Market growth shows
✓ Distribution channels listed
```

### Test 4: Search & Filter

```bash
# Search for different brands
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Dove&market_country=USA"
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Red%20Bull&market_country=India"
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Estee%20Lauder&market_country=UK"

# All should return valid data
```

---

## Data Quality Spot Checks

### PPP Pricing Validation

Test: Same brand, different markets, PPP-adjusted pricing

```bash
# Retrieve Dove across markets
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Dove&market_country=United%20Kingdom"
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Dove&market_country=United%20States"
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Dove&market_country=India"

# Validate:
# UK: £x.xx (PPP 1.0)
# USA: $x.xx (PPP 1.0) ← should be ~1.25x UK price (currency difference)
# India: ₹xxx (PPP 0.25) ← should be ~1/4 of USD equivalent price
```

### Market Status Validation

```bash
# Skincare in mature markets
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Clinique&market_country=United%20Kingdom"
# Expected: market_status: "mature", growth_cagr: 3.5%

# Skincare in emerging market
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Clinique&market_country=India"
# Expected: market_status: "emerging" or "high-growth", growth_cagr: 9.2%
```

### Positioning Tier Validation

```bash
# Economy tier
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Neutrogena&market_country=United%20Kingdom"
# Expected: £8.99, positioning_tier: "economy"

# Premium tier (same category, same market)
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Estee%20Lauder&market_country=United%20Kingdom"
# Expected: £95.00+, positioning_tier: "premium" or "luxury"

# Price should reflect positioning tier ✓
```

---

## Load Testing (Optional)

```bash
# Test concurrent requests
for i in {1..10}; do
  curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Olay&market_country=UK" &
done
wait

# All should return 200 OK, no errors
```

---

## Success Criteria

Mark Phase 1 **PRODUCTION READY** when:

✅ Schema created in Supabase  
✅ 60 records inserted successfully  
✅ All 4 API tests pass (retrieval, scoring, UI, search)  
✅ PPP pricing validates (same brand, different markets)  
✅ Market status consistent (mature UK/US, emerging India)  
✅ Positioning tiers match pricing  
✅ No 500 errors in logs  
✅ Response times < 500ms  
✅ UI renders brand pages without errors  

---

## Troubleshooting

### "No records found" error

```sql
SELECT COUNT(*) FROM brand_phase1_intelligence;
```

If count is 0, run batch insert again:
```bash
python3 phase1_batch_insert.py
```

### API returns 404

Check endpoint is correct:
```bash
# Correct:
/api/brand/phase1/get?brand_name=Dove&market_country=UK

# Wrong (won't work):
/api/brand/phase1/get?name=Dove&country=UK
```

### Pricing looks wrong

Verify PPP indices:
```sql
SELECT DISTINCT market_country, ppp_index 
FROM brand_phase1_intelligence 
ORDER BY market_country;

-- Expected:
-- United Kingdom  1.0
-- United States   1.0
-- India           0.25
```

### Market status not recognized

Check valid values:
```sql
SELECT DISTINCT market_status FROM brand_phase1_intelligence;

-- Should be: mature, emerging, high-growth, high_growth
```

---

## Post-Production Monitoring

### Daily Check (Week 1)

```bash
# Monitor API response times
for i in {1..100}; do
  time curl -s "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Dove&market_country=UK" > /dev/null
done | grep real | avg

# Should average < 300ms
```

### Data Monitoring

```sql
-- Check for any NULL critical fields
SELECT * FROM brand_phase1_intelligence 
WHERE price_local IS NULL OR positioning_tier IS NULL 
LIMIT 10;

-- Should return 0 rows
```

---

## Next Phase (Phase 2 Prep)

Once Phase 1 is live and stable:

1. Add 30 more brands (total 50)
2. Extend to 5 markets (add Brazil, Indonesia)
3. Build market entry scoring dashboard
4. Create competitive positioning maps
5. Forecast revenue scenarios

---

## Deployment Command

```bash
# One-line deploy + test
git push && sleep 60 && \
curl "https://miru.humanagency.co/api/brand/phase1/get?brand_name=Dove&market_country=UK" && \
echo "✅ Phase 1 LIVE"
```

---

## Live Dashboard Confirmation

Visit: https://miru.humanagency.co/intel

Verify:
- [ ] Search box works
- [ ] Can search "Dove", "Red Bull", "Coca-Cola"
- [ ] Brand pages load
- [ ] Fundamentals display correctly
- [ ] PPP-adjusted pricing shows
- [ ] Market status displays (mature vs. emerging)
- [ ] No errors in browser console
