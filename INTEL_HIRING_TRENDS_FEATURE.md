# Intel: Hiring Trends & Strategic Focus

**Goal:** Show hiring velocity + strategic direction (AI, geographic, vertical focus)

---

## Current State

✅ We show: "Reckitt: 50,000 employees" (static)  
❌ We don't show: "Reckitt is hiring 12% YoY this year + aggressive in APAC + AI focus"

---

## Proposed: Hiring Trends Dashboard

### 1. Hiring Velocity Indicator (On Company Card)

```
📈 Hiring Trend: +12% YoY (2025 vs 2024)

Last 5 years trend:
2021: 45,000
2022: 46,500 (+3.3%)
2023: 48,200 (+3.7%)
2024: 52,100 (+8.1%)
2025: 58,300 (+12.0%) ← ACCELERATING
2026 forecast: ~65,000 (+11%)
```

**Visualization:**
```
Hiring Velocity Chart (mini)
│     ╱╱
│   ╱╱
│ ╱╱
└─────────────
2021  2023  2025
```

---

### 2. Key Hiring Areas (Strategic Focus)

When comparing Reckitt vs Unilever, show:

**Reckitt Hiring Focus:**
```
🎯 Key Areas (inferred from job postings + M&A + financials):

1. 🤖 AI / Machine Learning
   └─ Software engineers, ML ops, data scientists
   └─ Investment: LinkedIn shows +40% ML job postings YoY
   └─ Why: Automating supply chain, personalized recommendations

2. 🌏 Asia Pacific Expansion  
   └─ Regional hires in Singapore, India, China
   └─ Investment: M&A in APAC (acquired 3 brands last year)
   └─ Why: High growth market, emerging brands

3. 💻 Digital / E-commerce
   └─ Ecommerce managers, digital marketing, platform engineers
   └─ Investment: Direct-to-consumer brands growing 25% YoY
   └─ Why: Offset retail decline, higher margins

4. 🧬 R&D / Product Innovation
   └─ Chemists, biotech researchers, product managers
   └─ Investment: Acquired biotech startup for $200M
   └─ Why: Premium products = higher margins
```

**How to interpret this:**
- Reckitt is hiring fastest in AI → They're investing in automation/efficiency
- Reckitt is hiring in APAC → Geographic expansion play
- Unilever is hiring more in E-commerce → Betting on DTC

---

### 3. Competitive Hiring Comparison

When you compare 4 companies, show:

```
HIRING VELOCITY COMPARISON

Company         | 2025 Growth | Fastest Growing Area    | AI Focus? 
────────────────┼─────────────┼─────────────────────────┼──────────
Reckitt         | +12.0%      | 🤖 AI/ML, APAC, DTC     | ⭐⭐⭐⭐
Unilever       | +5.2%       | 💻 E-commerce, India    | ⭐⭐⭐
Henkel         | +2.1%       | 🧬 R&D, Sustainability  | ⭐⭐
SC Johnson     | -1.3%       | ❌ Restructuring        | ⭐⭐
P&G            | +8.7%       | 🤖 AI, Premium brands   | ⭐⭐⭐⭐⭐
```

**Interpretation:**
- Reckitt hiring 2x faster than Henkel → More aggressive
- Reckitt + P&G both AI-focused → Both betting on tech
- SC Johnson is shrinking → Mature, cash cow

---

## Data Architecture

### Where does this data come from?

**Layer 1: Financial Data (We Have)**
```
- Employees 2021-2025 (from company_financials table)
- YoY growth % (calculated)
- Sector average (for comparison)
```

**Layer 2: Job Market Data (Need to Add)**
```
- LinkedIn job posting volume per company
- Job posting growth % (month-over-month)
- Job categories hiring for (AI, APAC, DTC, R&D)
- Job post metadata (seniority, urgency)
```

**Layer 3: Strategic Indicators (Need to Add)**
```
- Recent M&A targets and focus areas
- Geographic expansion signals (new office locations)
- Product launches (indicates R&D hiring)
- Earnings call commentary (CEO talking about AI, expansion)
- Patent filings (R&D intensity)
```

---

## Phase 1 Implementation (Quick Wins)

### Add to Database

```sql
-- company_hiring_areas table
CREATE TABLE company_hiring_areas (
    id UUID PRIMARY KEY,
    company_name TEXT NOT NULL,
    year INT NOT NULL,
    area TEXT NOT NULL, -- "AI/ML", "APAC", "DTC", "R&D", "Operations"
    estimated_hires INT,
    job_posting_volume INT,
    job_posting_growth_pct FLOAT,
    data_source TEXT -- "linkedin_jobs", "crunchbase", "earnings_call", "m&a"
);

-- Example: Reckitt 2025
INSERT INTO company_hiring_areas VALUES
  ('reckitt', 2025, 'AI/ML', 250, 42, +40, 'linkedin_jobs'),
  ('reckitt', 2025, 'APAC', 800, 156, +35, 'linkedin_jobs + m&a'),
  ('reckitt', 2025, 'DTC', 300, 67, +28, 'linkedin_jobs + earnings'),
  ('reckitt', 2025, 'R&D', 180, 35, +15, 'linkedin_jobs');
```

### Add to Company Card (HTML)

```html
<div class="hiring-trend">
  <h3>Hiring Velocity</h3>
  
  <!-- Trend Line Chart -->
  <canvas id="hiringChart"></canvas>
  
  <!-- Key Areas -->
  <div class="hiring-areas">
    <h4>Strategic Hiring Focus</h4>
    <div class="area-badge">🤖 AI/ML (+40% YoY)</div>
    <div class="area-badge">🌏 APAC Expansion (+35% YoY)</div>
    <div class="area-badge">💻 Direct-to-Consumer (+28% YoY)</div>
  </div>
  
  <!-- Insight -->
  <p class="insight">
    Reckitt is hiring 12% faster than sector average. 
    Main focus: AI automation, geographic expansion, and premium brands.
  </p>
</div>
```

---

## Phase 2: Data Collection (Ongoing)

### LinkedIn Job Posting API
```python
# Monthly job posting volume tracking
def track_linkedin_hiring(company_name):
    # Query LinkedIn Jobs API
    # Track: total postings, new postings, fill rate
    # Categorize by: function, seniority, location
    # Calculate: YoY growth, trend direction
    return {
        "company": "Reckitt",
        "total_jobs_posted": 342,
        "new_jobs_this_month": 28,
        "categories": {
            "AI/ML": 24,
            "APAC": 67,
            "DTC": 19,
        },
        "growth_vs_last_month": "+8%"
    }
```

### Earnings Call Analysis
```python
# Parse earnings calls for hiring indicators
def analyze_earnings_call(company_name):
    transcript = fetch_earnings_transcript(company_name, quarter="Q3 2025")
    
    # Search for keywords
    keywords = {
        "AI": "Investing in AI to transform supply chain...",
        "APAC": "Expanding aggressively in Southeast Asia...",
        "DTC": "Direct-to-consumer growing 25% YoY...",
        "R&D": "R&D investment up 15% year-over-year..."
    }
    
    return {
        "company": "Reckitt",
        "quarter": "Q3 2025",
        "strategic_areas": keywords
    }
```

### M&A Pattern Analysis
```python
# Infer hiring strategy from M&A
def infer_hiring_focus_from_ma(company_name):
    deals = fetch_m&a_deals(company_name, last_n_years=3)
    
    # Analyze what types of companies acquired
    acquired_industries = {
        "Biotech": 2,      # → Hiring R&D, scientists
        "AI/ML": 3,        # → Hiring engineers
        "DTC Brands": 4,   # → Hiring marketers, ops
        "APAC Startups": 3 # → Hiring in Asia
    }
    
    return {
        "company": "Reckitt",
        "primary_focus": "AI/ML, DTC, APAC",
        "confidence": "HIGH"
    }
```

---

## UI Component: Hiring Trend Card

```html
<div class="card hiring-trends">
  <h2>Hiring Trends & Strategic Focus</h2>
  
  <!-- Metric Row -->
  <div class="metrics-row">
    <div class="metric">
      <label>YoY Hiring Growth</label>
      <div class="value">+12.0%</div>
      <div class="comparison">vs sector avg +5.3%</div>
      <div class="status badge-good">📈 Accelerating</div>
    </div>
    
    <div class="metric">
      <label>LinkedIn Job Posts</label>
      <div class="value">342</div>
      <div class="comparison">+8% this month</div>
      <div class="status">vs Henkel 156 posts</div>
    </div>
    
    <div class="metric">
      <label>AI Focus Score</label>
      <div class="value">4/5</div>
      <div class="comparison">40+ AI job postings</div>
      <div class="status badge-high">⭐⭐⭐⭐</div>
    </div>
  </div>
  
  <!-- Trend Chart -->
  <div class="chart-container">
    <canvas id="hiringTrendChart"></canvas>
  </div>
  
  <!-- Strategic Areas -->
  <div class="strategic-areas">
    <h3>Where Reckitt is Hiring (2025)</h3>
    <div class="area-list">
      <div class="area">
        <span class="emoji">🤖</span>
        <span class="name">AI/ML Engineering</span>
        <span class="metric">+40% YoY, 24 open roles</span>
        <span class="reason">Supply chain automation, personalization</span>
      </div>
      <div class="area">
        <span class="emoji">🌏</span>
        <span class="name">APAC Expansion</span>
        <span class="metric">+35% YoY, 67 open roles</span>
        <span class="reason">M&A in Southeast Asia, emerging brands</span>
      </div>
      <div class="area">
        <span class="emoji">💻</span>
        <span class="name">Direct-to-Consumer</span>
        <span class="metric">+28% YoY, 19 open roles</span>
        <span class="reason">E-commerce growth, higher margins</span>
      </div>
      <div class="area">
        <span class="emoji">🧬</span>
        <span class="name">R&D / Biotech</span>
        <span class="metric">+15% YoY, 12 open roles</span>
        <span class="reason">Premium product innovation</span>
      </div>
    </div>
  </div>
  
  <!-- Competitive Comparison -->
  <div class="comparison-table">
    <h3>vs. Competitors</h3>
    <table>
      <tr>
        <th>Company</th>
        <th>Hiring Growth</th>
        <th>Key Focus</th>
        <th>AI Investment</th>
      </tr>
      <tr class="reckitt">
        <td>Reckitt</td>
        <td>+12.0%</td>
        <td>🤖 AI, 🌏 APAC, 💻 DTC</td>
        <td>⭐⭐⭐⭐</td>
      </tr>
      <tr class="unilever">
        <td>Unilever</td>
        <td>+5.2%</td>
        <td>💻 DTC, 🌏 India, 🧬 Sustainability</td>
        <td>⭐⭐⭐</td>
      </tr>
      <tr class="henkel">
        <td>Henkel</td>
        <td>+2.1%</td>
        <td>🧬 R&D, 🌍 Europe</td>
        <td>⭐⭐</td>
      </tr>
      <tr class="scjohnson">
        <td>SC Johnson</td>
        <td>-1.3%</td>
        <td>Operations, Cost Reduction</td>
        <td>⭐⭐</td>
      </tr>
    </table>
  </div>
  
  <!-- Insight Box -->
  <div class="insight-box">
    <h4>💡 What This Means</h4>
    <p>
      <strong>Reckitt is hiring 2x faster than Henkel</strong> — they're being aggressive.
      <strong>Heavy AI investment</strong> (40% YoY growth in ML roles) suggests they're 
      building competitive advantage in automation and personalization.
      <strong>APAC focus + M&A</strong> indicates geographic expansion play — targeting 
      high-growth emerging markets.
    </p>
  </div>
</div>
```

---

## Quick Win: Add Hiring Trend to Existing Comparison Page

**Minimal change to company_compare.html:**

Add a new section after M&A Timeline:

```html
<section id="hiringTrends" class="section">
  <h2>📊 Hiring Trends & Strategic Focus</h2>
  <div id="hiringTrendsContainer"></div>
</section>
```

Add function to render:

```javascript
function renderHiringTrends(companies) {
    let html = `<table class="hiring-comparison">
        <tr>
            <th>Company</th>
            <th>2025 Hiring</th>
            <th>vs Sector</th>
            <th>Strategic Focus</th>
            <th>AI Score</th>
        </tr>`;
    
    companies.forEach(company => {
        const hiringGrowth = company.hiring_growth_2025 || "N/A";
        const sectorAvg = 5.3;
        const delta = hiringGrowth - sectorAvg;
        const focus = company.hiring_focus_areas?.join(", ") || "—";
        const aiScore = company.ai_investment_score || 2;
        
        html += `<tr>
            <td>${company.name}</td>
            <td>${hiringGrowth}%</td>
            <td>${delta > 0 ? "✅ +" : "⚠️ "}${Math.abs(delta).toFixed(1)}pp</td>
            <td>${focus}</td>
            <td>${"⭐".repeat(aiScore)}</td>
        </tr>`;
    });
    
    html += `</table>`;
    document.getElementById('hiringTrendsContainer').innerHTML = html;
}
```

---

## Implementation Roadmap

### Week 1: Bootstrap Data
- [ ] Add hiring_areas data to database (Reckitt, Unilever, Henkel, P&G, SC Johnson)
- [ ] Add AI investment score (1-5) for each company
- [ ] Add strategic focus keywords for each company

### Week 2: UI Component
- [ ] Create hiring trends chart (employees 2021-2025 with trend line)
- [ ] Add strategic areas section (shows where they're hiring)
- [ ] Add comparison table (hiring growth vs competitors)

### Week 3: Integration
- [ ] Add to company card (single company view)
- [ ] Add to comparison page (side-by-side view)
- [ ] Add to Q&A ("What are they hiring for?" → AI, APAC, DTC)

### Week 4: Data Pipeline
- [ ] LinkedIn Jobs API integration (track postings monthly)
- [ ] Earnings call parsing (extract hiring/strategy keywords)
- [ ] Dashboard showing data freshness

---

## Sample Data (To Bootstrap)

```json
{
  "company": "Reckitt",
  "hiring_2025": {
    "total_growth_pct": 12.0,
    "areas": [
      {"name": "AI/ML", "growth_pct": 40, "open_roles": 24, "reason": "Supply chain automation"},
      {"name": "APAC", "growth_pct": 35, "open_roles": 67, "reason": "Geographic expansion"},
      {"name": "DTC", "growth_pct": 28, "open_roles": 19, "reason": "E-commerce growth"},
      {"name": "R&D", "growth_pct": 15, "open_roles": 12, "reason": "Premium products"}
    ],
    "ai_investment_score": 4,
    "vs_sector_average": 5.3,
    "trend": "accelerating"
  }
}
```

---

## Why This Matters for Sales

When you pitch Intel to Reckitt:

**Before (Generic):**
> "Reckitt has 58,000 employees"

**After (Insightful):**
> "Reckitt is hiring 12% YoY — faster than Henkel (2%) or SC Johnson (-1%). 
> They're investing heavily in AI (+40% ML roles) and APAC expansion (+35%). 
> That tells you they're building competitive advantage in automation and emerging markets.
> Meanwhile, Henkel is only growing 2% — they're in harvest mode."

**This is the story Intel tells.** Not just headcount, but strategy.

---

## Success Metrics

✅ Users can see hiring velocity (not just static headcount)  
✅ Users understand WHERE companies are investing (AI? APAC? R&D?)  
✅ Users can compare hiring strategies across competitors  
✅ Users can infer company strategy from hiring patterns  
✅ Sales team can use this to position Intel: "Know what they're building before they announce it"

