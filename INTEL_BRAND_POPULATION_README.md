# Intel Brand Data Population — Complete System

**Status:** ✅ Framework Ready | Research Infrastructure Complete | Ready for Execution

**Goal:** Populate all 93 brands in Intel database with REAL, TRACEABLE data from verified sources only. Every numeric value has a source URL. Zero fabrication. No estimates.

---

## 📊 What's Available Now

### 1. Research Infrastructure (Python Scripts)
- **`intel_brand_research_framework.py`** — Core research system
  - `BrandResearchTracker`: Track sources for every field
  - `BrandDataCollector`: Gather data from verified sources (Wikidata, SEC Edgar, Yahoo Finance, etc.)
  - `BrandResearchPlan`: Categorize and prioritize brands
  - `generate_research_roadmap()`: Create systematic research strategy

- **`intel_brand_population_batch.py`** — Main execution script
  - Load all 93 brands from Supabase
  - Research each brand systematically
  - Auto-generate research logs with source URLs
  - Progress tracking and reporting
  - Export verified data as JSON

- **`intel_data_verification.py`** — Quality assurance system
  - Validate source URLs (accessibility + format)
  - Verify field completeness and data types
  - Check plausibility (e.g., founded year 1800-2025)
  - Enforce quality gates (no fabrication, all sources traceable)
  - Generate comprehensive verification reports

### 2. Research Planning Documents
- **`INTEL_TOP_10_BRANDS_SOURCE_MAP.md`** — Detailed research guide
  - Complete data points for each of top 10 brands
  - Exact source URLs for every field
  - Effort estimates per brand (~20-35 min)
  - Data collection templates
  - Multi-currency pricing examples

- **`INTEL_RESEARCH_EXECUTION_GUIDE.md`** — Step-by-step playbook
  - Phase-by-phase execution plan
  - JSON templates for data entry
  - Quality verification procedures
  - Database insertion strategy
  - Batch processing by category
  - Timeline for full 93 brands (6-8 weeks)
  - Troubleshooting guide

---

## 🎯 Data Quality Standards

### Every Field Must Have:
1. **Value** — The actual data point
2. **Source** — Where it came from (e.g., "SEC Edgar 10-K")
3. **Source URL** — Traceable URL to the source
4. **Confidence** — Score 0-100 based on source quality
5. **Notes** — Context about how data was collected

### Confidence Scoring:
- **95%** — SEC Edgar 10-K, Companies House filings, official annual reports
- **85%** — Wikipedia, Wikidata, Yahoo Finance, official websites
- **75%** — Industry reports (Statista, analyst research), news sources
- **60%** — Estimates, secondary sources, older data
- **0%** — Fabricated, speculative, or unverifiable data (DO NOT USE)

### What NOT to Do:
- ❌ Do NOT estimate values
- ❌ Do NOT use AI-generated numbers
- ❌ Do NOT copy data without source verification
- ❌ Do NOT use third-party follower counters (use official @brand accounts)
- ❌ Do NOT leave fields empty (mark as "Not Available - Source Not Found")

---

## 🚀 Quick Start: Research Top 10 Brands

### Estimated Time: ~4.3 hours

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

### For Each Brand, Collect:

| Category | Examples | Time | Source |
|----------|----------|------|--------|
| **Founding & HQ** | Year, location, country | 5 min | Wikidata + Wikipedia |
| **Financials 2025** | Revenue, market cap, profit margin, employees | 10 min | SEC 10-K or annual report |
| **Products** | Top 3-5 products with names | 5 min | Brand website |
| **Pricing** | USD, GBP, INR prices per region | 10 min | Official stores + retailers |
| **Competitors** | Top 3 competitors + market share % | 5 min | Yahoo Finance + industry reports |
| **Social Media** | Instagram, YouTube, TikTok follower counts | 5 min | Official @brand accounts |
| **White Space** | Market gaps, expansion opportunities | 10 min | Industry analysis + market data |
| **Description** | 2-3 sentence brand overview | 5 min | Official website |

**See `INTEL_TOP_10_BRANDS_SOURCE_MAP.md` for exact URLs per brand.**

---

## 🔄 Full Workflow

### Phase 1: Research (4.3 hrs for top 10)
```
Load Brands → Research Systematically → Document Sources → Generate Research Log
```

### Phase 2: Verify (1 hr for top 10)
```
Load Research Log → Run Verification → Check Quality Gates → Fix Issues
```

### Phase 3: Insert (0.5 hr for top 10)
```
Generate SQL → Insert into Supabase → Spot-check in UI → Document Results
```

### Phase 4: Scale (Repeat for remaining 83 brands)
```
Batch by Category → Research → Verify → Insert → Spot-check
```

---

## 📋 Required Source Quality

### Acceptable Primary Sources:
- ✅ SEC Edgar (US public companies) - https://www.sec.gov
- ✅ Companies House (UK companies) - https://www.companieshouse.gov.uk
- ✅ Wikidata API (founding info) - https://www.wikidata.org
- ✅ Yahoo Finance (market data) - https://finance.yahoo.com
- ✅ Official brand websites (products, pricing) - brand.com
- ✅ Major retailers (pricing verification) - Amazon, Tesco, Sainsbury's, Boots, etc.
- ✅ Official social accounts (follower counts) - @brand on Instagram, YouTube, TikTok
- ✅ Industry reports (Statista, analyst research) - industry-specific

### NOT Acceptable:
- ❌ AI-generated text or numbers
- ❌ Unverified blogs or forums
- ❌ Third-party follower tracking sites
- ❌ News headlines without verification
- ❌ Estimates or "approximately"
- ❌ Internal estimates or guesses

---

## 🛠️ Execution Steps

### Step 1: Research Top 10
```bash
cd /Users/srevi/fuelwatch
python3 intel_brand_population_batch.py
```

This will:
1. Load the 93 brands from Supabase
2. Research top 10 systematically
3. Generate `intel_research_logs.json` with source URLs
4. Print progress report

### Step 2: Verify Data Quality
```bash
python3 -c "
from intel_data_verification import DataVerifier
import json

with open('intel_research_logs.json') as f:
    data = json.load(f)

verifier = DataVerifier()
report = verifier.verify_batch(data['research_logs'])
verifier.print_verification_summary(report)
verifier.export_verification_report(report, 'top10_verification.json')
"
```

This will:
1. Validate all source URLs
2. Check data quality and plausibility
3. Enforce quality gates
4. Export `top10_verification.json`

### Step 3: Review & Fix
```
Check top10_verification.json for any failures
Fix issues in research logs
Re-run verification until 100% pass rate
```

### Step 4: Insert into Supabase
```bash
python3 -c "
from intel_brand_population_batch import IntelBrandPopulator
import json

with open('top10_verified.json') as f:
    verified = json.load(f)

populator = IntelBrandPopulator()
# Generate and execute SQL INSERT statements
"
```

### Step 5: Spot-Check in UI
Visit https://intel.humanagency.co and:
1. Search for each of top 10 brands
2. Verify data is displaying correctly
3. Check that sources are attributed

### Step 6: Repeat for Remaining 83 Brands
Follow same workflow in batches by category:
- Technology (10 brands) - 8-10 hours
- Beverages (15 brands) - 10-12 hours
- Fashion (12 brands) - 8-10 hours
- FMCG (18 brands) - 12-15 hours
- Retail (10 brands) - 8-10 hours
- Automotive (8 brands) - 6-8 hours
- Pharma (7 brands) - 6-8 hours
- Other (3 brands) - 2-3 hours

---

## 📊 Success Metrics

### For Each Brand:
- [ ] Founding year found with source URL
- [ ] Headquarters location found with source URL
- [ ] Website verified
- [ ] 2025 financial data from official source
- [ ] Top 3-5 products listed with pricing (USD, GBP, INR)
- [ ] Top 3 competitors identified with market share
- [ ] Social media followers from official accounts
- [ ] All sources traceable with URLs
- [ ] Confidence scores assigned (95-100% preferred)
- [ ] Verification script passes 100%

### For Database:
- [ ] No NULL values (use "Not Available" instead)
- [ ] No estimated or AI-generated data
- [ ] All numeric values have source_url
- [ ] Prices in local currency (GBP, USD, INR)
- [ ] Social counts are from official accounts
- [ ] Competitors list is accurate
- [ ] Data displays correctly in Intel UI
- [ ] Search works properly for all brands

---

## 📚 Reference Documents

| Document | Purpose | Read Time |
|----------|---------|-----------|
| `INTEL_TOP_10_BRANDS_SOURCE_MAP.md` | Detailed research guide for top 10 | 15 min |
| `INTEL_RESEARCH_EXECUTION_GUIDE.md` | Complete playbook with workflows | 20 min |
| `intel_brand_research_framework.py` | Research infrastructure code | 10 min |
| `intel_brand_population_batch.py` | Main execution script | 10 min |
| `intel_data_verification.py` | Quality assurance code | 10 min |

---

## 🚨 Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| "Source URL not accessible" | Try alternative source or verify URL is current (some sites move) |
| "Founded year not found" | Try: Wikidata → Wikipedia → Company official history (don't estimate) |
| "Revenue data inconsistent" | Use latest SEC 10-K or annual report (most authoritative) |
| "Social followers don't match" | Use OFFICIAL @brand account only, never third-party trackers |
| "Pricing varies by region" | Collect all regions (USD/GBP/INR), note PPP differences |
| "Competitor data outdated" | Use Yahoo Finance or latest industry report (Statista) |

---

## 📅 Estimated Timeline

| Phase | Effort | Timeline |
|-------|--------|----------|
| Top 10 Brands | 4-5 hours | 1 day |
| Verification & Fixes | 1-2 hours | Same day |
| Database Insertion | 1 hour | Same day |
| Technology Category (10) | 8-10 hours | 2 days |
| Beverages Category (15) | 10-12 hours | 2-3 days |
| Fashion Category (12) | 8-10 hours | 2 days |
| FMCG Category (18) | 12-15 hours | 3 days |
| Retail Category (10) | 8-10 hours | 2 days |
| Automotive Category (8) | 6-8 hours | 1-2 days |
| Pharma Category (7) | 6-8 hours | 1-2 days |
| **TOTAL FOR 93 BRANDS** | **75-85 hours** | **6-8 weeks** |

*Can be accelerated with parallel research teams*

---

## 🎓 Data Entry Template

Use this JSON structure for every brand:

```json
{
  "brand_name": "BRAND_NAME",
  "research_date": "2026-06-30",
  "fields": {
    "founded_year": {
      "value": YEAR,
      "source": "Wikidata",
      "source_url": "https://www.wikidata.org/wiki/...",
      "confidence": 95,
      "notes": "Verified from official company history"
    },
    "headquarters": {
      "value": "City, Country",
      "source": "SEC Edgar",
      "source_url": "https://www.sec.gov/cgi-bin/browse-edgar...",
      "confidence": 95,
      "notes": "From 10-K filing"
    },
    "revenue_2025_billions": {
      "value": NUMBER,
      "currency": "USD",
      "fiscal_year": 2024,
      "source": "SEC 10-K",
      "source_url": "https://www.sec.gov/Archives/edgar/...",
      "confidence": 95,
      "notes": "Latest annual report"
    }
    // ... more fields following same structure
  }
}
```

---

## ✅ Final Checklist Before Committing Data

- [ ] All source URLs are valid and accessible
- [ ] No NULL/empty fields (use "Not Available" instead)
- [ ] All numeric values have source_url
- [ ] Confidence scores assigned (0-100)
- [ ] No estimates or AI-generated numbers
- [ ] Verification script shows 100% pass rate
- [ ] Manual spot-check in Intel UI passes
- [ ] At least 3 spot-checks done per batch
- [ ] Documentation updated with sources used
- [ ] Data quality report generated and archived

---

## 📞 Questions?

**For research methodology:**
- See `INTEL_RESEARCH_EXECUTION_GUIDE.md` (Troubleshooting section)
- Check `INTEL_TOP_10_BRANDS_SOURCE_MAP.md` for examples

**For code usage:**
- Check docstrings in `intel_brand_research_framework.py`
- Run scripts with `--help` for options

**For quality standards:**
- See "Data Quality Standards" section above
- Reference "Required Source Quality" table

---

**System Ready for Execution** ✅

Start with top 10 brands using `INTEL_TOP_10_BRANDS_SOURCE_MAP.md`

Expected completion: 4-5 hours for top 10 | 6-8 weeks for all 93

All 93 brands will have **100% real, traceable data with source URLs.**

---

**Version:** 1.0 | **Created:** 2026-06-30 | **Status:** Ready for Production
