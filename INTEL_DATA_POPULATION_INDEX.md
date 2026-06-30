# Intel Data Population System — Master Index

**Project Status:** ✅ Framework Complete | Research Infrastructure Ready | Execution Ready

**Objective:** Populate 93 brands in Intel database with REAL, TRACEABLE data from verified sources (100% real data guarantee).

---

## 📁 File Index & Quick Access

### 🎯 START HERE
1. **`INTEL_BRAND_POPULATION_README.md`** ← Start here for overview
   - Complete system description
   - Quick start guide for top 10 brands
   - Success metrics and timeline
   - Data quality standards
   - Common issues & solutions

### 📚 Research Planning & Execution
2. **`INTEL_TOP_10_BRANDS_SOURCE_MAP.md`** ← Use this to research top 10 brands
   - Detailed data points for each brand
   - Exact source URLs
   - Effort estimates (20-35 min per brand)
   - JSON templates for data entry
   - Multi-currency pricing examples

3. **`INTEL_RESEARCH_EXECUTION_GUIDE.md`** ← Step-by-step playbook
   - Phase-by-phase workflow (Assessment → Research → Verify → Insert)
   - Detailed instructions for each phase
   - Data collection templates
   - Database insertion strategy
   - Full 93-brand timeline (6-8 weeks)
   - Batch processing by category
   - Troubleshooting guide

### 💻 Python Scripts (Ready to Run)
4. **`intel_brand_research_framework.py`** ← Core research system
   - `BrandResearchTracker` class: Track sources per field
   - `BrandDataCollector` class: Gather from verified sources
   - `BrandResearchPlan` class: Categorize and prioritize brands
   - `generate_research_roadmap()` function
   - Usage: Import in your scripts to collect data systematically

5. **`intel_brand_population_batch.py`** ← Main execution script
   - `IntelBrandPopulator` class: Main orchestrator
   - Load brands from Supabase
   - Research each brand (with progress tracking)
   - Generate research logs with source URLs
   - Export verified data as JSON
   - Usage: `python3 intel_brand_population_batch.py`

6. **`intel_data_verification.py`** ← Quality assurance system
   - `DataVerifier` class: Validate all data
   - `verify_source_url()`: Check URL accessibility
   - `verify_field()`: Validate individual fields
   - `verify_brand_data()`: Full brand verification
   - `verify_batch()`: Batch processing
   - Usage: `python3 -c "from intel_data_verification import DataVerifier; ..."`

### 🗂️ Supporting Documentation (Reference)
7. **`INTEL_DATA_POPULATION_INDEX.md`** ← This file
   - Navigation guide
   - File descriptions
   - Workflow summary
   - Quick reference

---

## 🔄 Workflow Overview

### Step 1: Research (4-5 hours for top 10)
```
┌─ Load Brands from Supabase
├─ For Each Brand:
│  ├─ Founding Year → Wikidata
│  ├─ Financials 2025 → SEC Edgar / Annual Report
│  ├─ Products → Brand Website
│  ├─ Pricing → Retailers (UK/US/India)
│  ├─ Competitors → Yahoo Finance / Industry Reports
│  ├─ Social Media → Official @brand Accounts
│  └─ White Space → Market Analysis
├─ Document Every Source URL
└─ Export to JSON with Source Attribution
```

### Step 2: Verify (1-2 hours for top 10)
```
┌─ Load Research JSON
├─ Run Quality Checks:
│  ├─ Source URLs accessible?
│  ├─ All fields have source_url?
│  ├─ Confidence scores assigned?
│  ├─ No estimates/fabrication?
│  └─ Plausibility checks pass?
├─ Fix Any Issues
└─ Export Verification Report
```

### Step 3: Insert (1 hour for top 10)
```
┌─ Generate SQL INSERT Statements
├─ Insert into Supabase:
│  ├─ brand_profile (fundamentals)
│  ├─ brand_financials (2025 data)
│  ├─ brand_skus (products + pricing)
│  ├─ brand_competitors (competitor data)
│  └─ brand_social (social media counts)
├─ Spot-Check in Intel UI
└─ Document Results
```

### Step 4: Scale (6-8 weeks for 93 brands)
```
Batch 1: Top 10 (done in Step 1-3)
Batch 2: Technology (10 brands)
Batch 3: Beverages (15 brands)
Batch 4: Fashion (12 brands)
Batch 5: FMCG (18 brands)
Batch 6: Retail (10 brands)
Batch 7: Automotive (8 brands)
Batch 8: Pharma (7 brands)
Batch 9: Other (3 brands)
```

---

## 📊 Data Quality Standards

### Every Field Must Have:
✅ **Value** — Actual data  
✅ **Source** — Where it came from  
✅ **Source URL** — Traceable to published data  
✅ **Confidence** — Score based on source type  
✅ **Notes** — Context about data collection  

### Confidence Scoring:
- **95%** — SEC Edgar, Companies House, official annual reports
- **85%** — Wikipedia, Wikidata, Yahoo Finance, official websites
- **75%** — Industry reports, Statista, news sources
- **60%** — Secondary sources, older data
- **0%** — Fabricated, estimated, or unverifiable (DO NOT USE)

### Quality Gates (Must Pass):
- ✅ No NULL values (use "Not Available - Source Not Found")
- ✅ No estimates or AI-generated numbers
- ✅ All numeric values have source_url
- ✅ All sources traceable and verified
- ✅ No data from third-party estimators
- ✅ Social followers from official @brand accounts only
- ✅ Prices in local currency (USD, GBP, INR)
- ✅ Verification script passes 100%

---

## 🚀 Quick Start: Top 10 Brands

**Time:** ~4.3 hours

**Brands (priority order):**
1. Apple (AAPL)
2. Microsoft (MSFT)
3. Coca-Cola (KO)
4. Nike (NKE)
5. Amazon (AMZN)
6. Google/Alphabet (GOOGL)
7. Samsung (005930.KS)
8. Nestlé (NESN)
9. Tesla (TSLA)
10. Unilever (ULVR)

**Process:**
1. Open `INTEL_TOP_10_BRANDS_SOURCE_MAP.md`
2. For each brand, use the source URLs provided
3. Collect data: Founded Year, HQ, Revenue 2025, Market Cap, Employees, Products, Pricing, Competitors, Social
4. Enter into JSON format (see README for template)
5. Run verification script
6. Insert into Supabase

**See:** `INTEL_TOP_10_BRANDS_SOURCE_MAP.md` for exact URLs per brand

---

## 🛠️ Python Usage Examples

### Example 1: Research Apple
```python
from intel_brand_research_framework import BrandDataCollector, BrandResearchTracker

collector = BrandDataCollector()
tracker = BrandResearchTracker("Apple")

# Fetch founding year and HQ
wiki_data = collector.fetch_wikidata_company("Apple")
tracker.add_field("founded_year", wiki_data["founded_year"], 
                 "Wikidata", wiki_data["source_url"], 95)
tracker.add_field("headquarters", wiki_data["headquarters"],
                 "Wikidata", wiki_data["source_url"], 95)

# Fetch market cap
yahoo_data = collector.fetch_yahoo_finance_quote("AAPL")
tracker.add_field("market_cap", yahoo_data["market_cap"],
                 "Yahoo Finance", yahoo_data["source_url"], 85)

print(tracker.to_json())
```

### Example 2: Run Verification
```python
from intel_data_verification import DataVerifier
import json

with open('research_logs.json') as f:
    research_data = json.load(f)

verifier = DataVerifier()
report = verifier.verify_batch(research_data['research_logs'])
verifier.print_verification_summary(report)
```

### Example 3: Generate Research Roadmap
```python
from intel_brand_research_framework import generate_research_roadmap

brands = ["Apple", "Microsoft", "Coca-Cola", ...]  # All 93
roadmap = generate_research_roadmap(brands)

# Get priority order and sources
for brand_plan in roadmap["priority_sequence"][:10]:
    print(f"{brand_plan['brand']}: Priority {brand_plan['priority']}")
```

---

## 📋 Required Data Points per Brand

### Fundamentals (5 fields)
- Founded Year → Wikidata / Wikipedia
- Headquarters (City, Country) → SEC Edgar / Wikidata
- Website → Official website
- Description (2-3 sentences) → Official website
- Logo URL → Brand website

### Financials 2025 (7 fields)
- Revenue ($ billions) → SEC 10-K or Annual Report
- Market Cap ($ billions) → Yahoo Finance
- Profit Margin (%) → SEC 10-K
- Employees → SEC 10-K
- P/E Ratio → Yahoo Finance (if public)
- Dividend Yield (%) → Yahoo Finance (if public)
- Growth Rate (%) → SEC guidance or analyst consensus

### Products & Pricing (5-8 fields)
- Top Product 1 (name + 3 prices: USD, GBP, INR)
- Top Product 2 (name + 3 prices)
- Top Product 3 (name + 3 prices)
- Top Product 4-5 (optional, name + 3 prices)
- Availability Score (0-1, percentage in major retailers)

### Competitors (4 fields)
- Competitor 1 (name + market share %)
- Competitor 2 (name + market share %)
- Competitor 3 (name + market share %)
- Market Share Context (source)

### Social Media (4 fields)
- Instagram Followers → @brand official account
- YouTube Subscribers → @channel official account
- TikTok Followers → @brand official (if applicable)
- LinkedIn Followers (if applicable)

### White Space & Strategy (3 fields)
- Market Gaps (underserved segments)
- Growth Adjacencies (expansion opportunities)
- Strategic Opportunities (with data support)

---

## ✅ Verification Checklist

Before inserting any brand data:

- [ ] Source URLs are valid and accessible
- [ ] All fields have source_url (no NULL)
- [ ] Confidence scores assigned (95-75% preferred)
- [ ] No estimated or AI-generated values
- [ ] Founded year 1800-2025 (plausibility check)
- [ ] Revenue values < $1000B (plausibility check)
- [ ] Market cap < $100T (plausibility check)
- [ ] Social followers from official accounts only
- [ ] Prices in correct currency (USD, GBP, INR)
- [ ] Competitor data from verified sources
- [ ] Verification script passes 100%
- [ ] Manual spot-check passes (3-5 brands per batch)
- [ ] All sources documented and attributed

---

## 📞 Key Resources

### Source URLs (Bookmark These)
- **Wikidata API** → https://query.wikidata.org/sparql
- **SEC Edgar** → https://www.sec.gov/cgi-bin/browse-edgar
- **Yahoo Finance** → https://finance.yahoo.com
- **Companies House** → https://www.companieshouse.gov.uk
- **Wikipedia** → https://www.wikipedia.org
- **Google Finance** → https://www.google.com/finance

### UK Retailers (for GBP pricing)
- Tesco → https://www.tesco.com/groceries
- Sainsbury's → https://www.sainsburys.co.uk
- Asda → https://www.asda.com
- Boots → https://www.boots.com
- John Lewis → https://www.johnlewis.com

### India Retailers (for INR pricing)
- Amazon India → https://www.amazon.in
- Flipkart → https://www.flipkart.com
- Myntra (fashion) → https://www.myntra.com

---

## 🎓 Training Path

1. **Read:** `INTEL_BRAND_POPULATION_README.md` (10 min)
2. **Learn:** `INTEL_RESEARCH_EXECUTION_GUIDE.md` (20 min)
3. **Study:** `INTEL_TOP_10_BRANDS_SOURCE_MAP.md` (15 min)
4. **Review:** Python scripts docstrings (10 min)
5. **Practice:** Research 1-2 brands using SOURCE_MAP
6. **Execute:** Research top 10 brands
7. **Verify:** Run verification script
8. **Deploy:** Insert into Supabase
9. **Scale:** Repeat for remaining 83 brands

**Total Learning Time:** ~1-2 hours

---

## 📈 Success Metrics

### Per Brand:
- ✅ All required fields populated
- ✅ Sources traceable with URLs
- ✅ Confidence scores 75-100%
- ✅ No estimated data
- ✅ Verification passes
- ✅ Displays correctly in Intel UI

### Per Batch (10-15 brands):
- ✅ 100% research completion
- ✅ 95%+ verification pass rate
- ✅ <2% missing data
- ✅ All sources documented
- ✅ Spot-checks pass

### Overall (93 brands):
- ✅ All brands researched
- ✅ 95%+ field completion
- ✅ Zero fabricated data
- ✅ All sources traceable
- ✅ Quality metrics documented
- ✅ Intel UI fully populated and tested

---

## 🎯 Next Immediate Steps

1. **Read** `INTEL_BRAND_POPULATION_README.md` (understand the system)
2. **Open** `INTEL_TOP_10_BRANDS_SOURCE_MAP.md` (get specific URLs)
3. **Research** Apple using provided sources (test process)
4. **Verify** data against quality standards
5. **Run** verification script on test data
6. **Repeat** for remaining 9 brands
7. **Insert** all 10 into Supabase
8. **Spot-check** in Intel UI
9. **Document** findings
10. **Scale** to remaining 83 brands

---

## 📊 Timeline Summary

| Phase | Brands | Effort | Time |
|-------|--------|--------|------|
| Research | 10 | 4-5h | 1 day |
| Verify | 10 | 1-2h | Same day |
| Insert | 10 | 1h | Same day |
| Scale | 83 | 70-80h | 6-8 weeks |
| **TOTAL** | **93** | **75-85h** | **6-8 weeks** |

---

## 🚨 Important Reminders

- ⚠️ **NO ESTIMATES** — Only published, verifiable data
- ⚠️ **NO AI NUMBERS** — Don't use ChatGPT/Claude to generate values
- ⚠️ **SOURCE EVERYTHING** — Every number needs a URL
- ⚠️ **CONFIDENCE SCORES** — Based on source type, not your certainty
- ⚠️ **OFFICIAL ACCOUNTS ONLY** — Social media from @brand official
- ⚠️ **MARK MISSING DATA** — "Not Available - Source Not Found" (don't leave NULL)

---

## ✨ System Status

✅ Research Framework — Complete  
✅ Population Script — Complete  
✅ Verification System — Complete  
✅ Documentation — Complete  
✅ Top 10 Source Map — Complete  
✅ Execution Guide — Complete  
⏳ **Ready for Execution** — Start with top 10 brands

---

**Start with:** `INTEL_TOP_10_BRANDS_SOURCE_MAP.md`

**Expected Outcome:** 93 brands with 100% real, traceable data | All sources with URLs | Zero fabrication

**Version:** 1.0 | **Status:** Production Ready | **Last Updated:** 2026-06-30
