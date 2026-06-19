# Phase 2: Market Comparison & Expansion Strategy
## Implementation Plan

**Goal:** Enable users to compare same brand across UK/USA/India. See market-specific data, economics, and expansion opportunities.

---

## Sprint 1: Market Switcher UI + Market Economics (Week 1)

### Task 1.1: Market Switcher Dropdown
**What:** Add market selector to brand page header

**Changes:**
- Template: Add `<select>` dropdown in header showing UK/USA/India
- JavaScript: On change, navigate to same brand in selected market
- URL updates: `/brand/full?search=Dove&market=India`

**Code:**
```html
<!-- In header, next to brand name -->
<select id="marketSelect" onchange="switchMarket(this.value)">
    <option value="UK" {% if market == 'UK' %}selected{% endif %}>🇬🇧 UK</option>
    <option value="USA" {% if market == 'USA' %}selected{% endif %}>🇺🇸 USA</option>
    <option value="India" {% if market == 'India' %}selected{% endif %}>🇮🇳 India</option>
</select>

<script>
function switchMarket(market) {
    const brand = new URLSearchParams(window.location.search).get('search');
    window.location.href = `/brand/full?search=${encodeURIComponent(brand)}&market=${market}`;
}
</script>
```

**Status:** TODO

---

### Task 1.2: Market Economics Card
**What:** Display market context: GDP, PPP, market size, growth rate

**New data needed:**
Add to Supabase: `brand_phase1_market_economics` table
```sql
CREATE TABLE brand_phase1_market_economics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_country TEXT NOT NULL, -- 'UK', 'USA', 'India'
    category TEXT NOT NULL, -- 'skincare', 'beverages'
    
    -- Market fundamentals
    country_gdp_usd_billions NUMERIC,
    ppp_index NUMERIC,
    urban_population_millions NUMERIC,
    
    -- Category-specific
    category_market_size_usd_millions NUMERIC,
    category_cagr_3yr NUMERIC,
    category_status TEXT, -- 'mature', 'emerging', 'high_growth'
    
    -- Segment data
    affluent_consumers_millions NUMERIC,
    mass_market_consumers_millions NUMERIC,
    
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Populate with data:**
| Country | GDP | PPP | Skincare Market | CAGR | Affluent Pop |
|---------|-----|-----|-----------------|------|--------------|
| UK | 3.3T | 1.0 | $8.2B | 3.5% | 8M |
| USA | 28.0T | 1.0 | $18.5B | 3.8% | 45M |
| India | 3.9T | 0.25 | $2.1B | 8.2% | 25M |

**Template section:**
```html
<!-- Market Economics Card (new) -->
<div class="section">
    <h2>🌍 Market Economics</h2>
    <div class="market-economics">
        <div class="econ-item">
            <div class="econ-label">Country GDP</div>
            <div class="econ-value">${{ market_gdp }}T USD</div>
        </div>
        <div class="econ-item">
            <div class="econ-label">PPP Index</div>
            <div class="econ-value">{{ ppp_index }}</div>
        </div>
        <div class="econ-item">
            <div class="econ-label">Category Market Size</div>
            <div class="econ-value">${{ market_size }}M</div>
        </div>
        <div class="econ-item">
            <div class="econ-label">Category Growth (3yr)</div>
            <div class="econ-value">{{ category_growth }}% CAGR</div>
        </div>
    </div>
</div>
```

**Status:** TODO

---

### Task 1.3: Route Updates
**What:** Enhance `/brand/full` route to fetch market economics

**Changes to sms_service.py:**
```python
@app.route("/brand/full")
def brand_full_intelligence():
    search = request.args.get("search", "").strip()
    market = request.args.get("market", "UK").strip()
    
    # Existing: fetch brand data
    brand_result = sb.table("brand_phase1_intelligence").select("*") \
        .eq("brand_name", search).eq("market_country", market).execute()
    
    # NEW: fetch market economics
    econ_result = sb.table("brand_phase1_market_economics").select("*") \
        .eq("market_country", market) \
        .eq("category", brand_result.data[0].get("category")).execute()
    
    market_econ = econ_result.data[0] if econ_result.data else {}
    
    return render_template("intel_brand_phase1.html", 
        brand=brand, 
        market=market,
        market_econ=market_econ)
```

**Status:** TODO

---

## Sprint 2: Headroom Analysis (Week 2)

### Task 2.1: Headroom Calculation
**What:** TAM vs current share = expansion opportunity

**Logic:**
```
TAM = market_size_usd_millions
Current Share Estimate = (based on brand tier + market status)
Current Revenue Estimate = TAM × Current Share %
Growth Headroom = (TAM × target_share %) - Current Revenue

Example (Dove Skincare UK):
TAM: $8.2B
Est. Current Share: 2.8%
Current Revenue: $230M
If target 5%: Revenue potential $410M
Headroom: +$180M (78% growth opportunity)
```

**Template section:**
```html
<div class="section">
    <h2>📈 Expansion Headroom</h2>
    <div class="headroom">
        <div class="headroom-stat">
            <div class="label">Total Addressable Market</div>
            <div class="value">${{ tam_millions }}M</div>
        </div>
        <div class="headroom-stat">
            <div class="label">Est. Current Share</div>
            <div class="value">{{ current_share }}%</div>
        </div>
        <div class="headroom-stat">
            <div class="label">Opportunity at 5% Share</div>
            <div class="value">${{ opportunity_millions }}M</div>
        </div>
        <div class="headroom-stat">
            <div class="label">Growth Headroom</div>
            <div class="value">{{ growth_headroom }}%</div>
        </div>
    </div>
</div>
```

**Status:** TODO

---

## Sprint 3: Market Entry Scoring (Week 2)

### Task 3.1: Scoring Algorithm
**What:** Red/Yellow/Green recommendation for entering market

**Inputs:**
- Brand positioning tier vs market affluence
- Category growth rate
- Competitive intensity
- Distribution accessibility
- PPP adjustment (pricing viability)

**Output:** Score 0-10 + recommendation text

**Example:**
```
Dove in India (Skincare)
- Affluence mismatch: -1 (mass-market in emerging market)
- Category growth: +3 (8.2% CAGR is strong)
- Competition: -1 (Himalaya strong locally)
- Distribution: +2 (mass-market accessible)
- Pricing viability: +2 (PPP allows affordable premium)
Score: 6.5/10
Recommendation: 🟡 YELLOW — Conditional entry. Requires local variant positioning (affordable premium vs budget).
```

**Status:** TODO

---

## Data Sources Needed

| Data | Source | Status |
|------|--------|--------|
| Country GDP | World Bank | ✅ Known |
| PPP Indices | World Bank | ✅ Known (0.25 India, 1.0 UK/USA) |
| Category market sizes | Statista/Euromonitor | ⏳ Research needed |
| Competitive landscape | Industry reports | ⏳ Research needed |
| Brand revenue estimates | Brand websites / public filings | ⏳ Research needed |

---

## Files to Modify

1. **migrations/phase2_schema.sql** (new)
   - `brand_phase1_market_economics` table
   - `brand_phase1_market_entry_scoring` table

2. **sms_service.py**
   - Update `/brand/full` route to fetch market economics + scoring

3. **templates/intel_brand_phase1.html**
   - Add market switcher dropdown in header
   - Add market economics section
   - Add headroom analysis section
   - Add market entry scoring badge

4. **phase2_market_data.json** (new)
   - Market economics data for all 3 markets × 2 categories

5. **phase2_batch_insert.py** (new)
   - Insert market economics + scoring data into Supabase

---

## Build Sequence

1. **Day 1:** Market switcher UI + route updates
2. **Day 2:** Market economics data collection + insertion
3. **Day 3:** Headroom analysis calculations
4. **Day 4:** Market entry scoring logic
5. **Day 5:** Testing + refinements

---

## Success Criteria

- [ ] Market switcher works: UK → USA → India updates all data
- [ ] Market economics card displays GDP, PPP, market size, growth
- [ ] Headroom shows TAM and growth opportunity
- [ ] Market entry score gives actionable recommendation
- [ ] All 60 brand-market combos have economics data
- [ ] Price + distribution + growth updates when switching markets
- [ ] No 404s or missing data on any brand-market combo

---

## Phase 2 Complete When

User can:
1. Search for "Dove"
2. See UK data
3. Switch to "India"
4. See India-specific: price (₹180), market size ($2.1B), growth (8.2%), entry score (Yellow), headroom ($X opportunity)
5. Switch back to "USA"
6. See USA-specific data

All without leaving the page. Smooth market comparison experience.
