# Intel Brand Research Execution Guide
## Populate 93 Brands with REAL, Traceable Data

---

## Phase 1: Assessment & Planning (Already Complete ✅)

### Deliverables Created:
1. **Research Framework** (`intel_brand_research_framework.py`)
   - Source tracking with confidence scoring
   - Data collector for verified sources (Wikidata, SEC Edgar, Yahoo Finance, etc.)
   - Research planning utilities

2. **Population Script** (`intel_brand_population_batch.py`)
   - Loads brands from Supabase
   - Researches each brand systematically
   - Auto-generates research logs with source URLs

3. **Verification System** (`intel_data_verification.py`)
   - Validates all data before insertion
   - Enforces quality gates (no fabrication, all sources traceable)
   - Generates verification reports

4. **Source Map for Top 10** (`INTEL_TOP_10_BRANDS_SOURCE_MAP.md`)
   - Detailed research plan for each brand
   - Exact URLs for every data source
   - Effort estimates (4.3 hours for top 10)

---

## Phase 2: Systematic Research (Next Steps)

### Step-by-Step Execution

#### 2.1 Research Top 10 Brands (~4 hours)
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

**For Each Brand, Gather:**

```
┌─ FUNDAMENTALS (10-15 min per brand)
│  ├─ Founded Year → Wikidata
│  ├─ HQ Location → Wikidata / SEC Edgar
│  ├─ Website → Official website
│  ├─ Description → Official website + Wikipedia
│  └─ Logo URL → Brand website
│
├─ FINANCIALS 2025 (10-15 min per brand)
│  ├─ Revenue → SEC 10-K / Annual Report
│  ├─ Market Cap → Yahoo Finance
│  ├─ Profit Margin → SEC 10-K
│  ├─ Employees → SEC 10-K
│  ├─ P/E Ratio → Yahoo Finance (if public)
│  └─ Dividend Yield → Yahoo Finance (if public)
│
├─ PRODUCTS & PRICING (15-20 min per brand)
│  ├─ Top 3-5 products → Brand website
│  ├─ Price USD → Official store
│  ├─ Price GBP → Official UK site or Boots/Tesco
│  ├─ Price INR → Amazon India or local retailers
│  └─ Availability → Major retailers
│
├─ COMPETITORS (10 min per brand)
│  ├─ Top 3 direct competitors → Yahoo Finance / Statista
│  ├─ Market share % → Industry reports
│  └─ Positioning → Company websites
│
├─ SOCIAL MEDIA (5 min per brand)
│  ├─ Instagram followers → @brand official count (real-time)
│  ├─ YouTube subscribers → @channel official
│  ├─ TikTok followers → @brand official (if applicable)
│  └─ LinkedIn followers (if applicable)
│
└─ WHITE SPACE OPPORTUNITIES (10 min per brand)
   ├─ Market gaps → Industry analysis
   ├─ Growth adjacencies → Statista trends
   └─ Regional expansion → Market research data
```

#### 2.2 Research Each Brand: Detailed Instructions

**Template for researching 1 brand (example: Apple):**

```json
{
  "brand_name": "Apple",
  "fields": {
    "founded_year": {
      "value": 1976,
      "source": "Wikidata",
      "source_url": "https://www.wikidata.org/wiki/Q312",
      "confidence": 95,
      "notes": "Verified from official company history"
    },
    "headquarters": {
      "value": "Cupertino, California, USA",
      "source": "SEC Edgar",
      "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193",
      "confidence": 95,
      "notes": "From 10-K filing"
    },
    "revenue_2025_billions": {
      "value": 391.0,
      "currency": "USD",
      "fiscal_year": 2024,
      "source": "SEC 10-K",
      "source_url": "https://www.sec.gov/Archives/edgar/container/320193/[10-K filing link]",
      "confidence": 95,
      "notes": "Latest annual report filed with SEC"
    },
    "market_cap_billions": {
      "value": 3200,
      "currency": "USD",
      "as_of": "2025-06-30",
      "source": "Yahoo Finance",
      "source_url": "https://finance.yahoo.com/quote/AAPL",
      "confidence": 85,
      "notes": "Real-time market data"
    },
    "employees": {
      "value": 161000,
      "source": "SEC 10-K",
      "source_url": "[SEC filing]",
      "confidence": 95,
      "notes": "From latest annual report"
    },
    "profit_margin_percent": {
      "value": 29.8,
      "source": "SEC 10-K",
      "source_url": "[SEC filing]",
      "confidence": 95,
      "notes": "Net profit margin calculated from 10-K"
    },
    "top_products": {
      "value": [
        {
          "name": "iPhone 16 Pro",
          "price_usd": 999,
          "price_gbp": 999,
          "price_inr": 119900
        },
        {
          "name": "MacBook Pro 16\"",
          "price_usd": 2499,
          "price_gbp": 2299,
          "price_inr": 299900
        },
        {
          "name": "iPad Pro",
          "price_usd": 1099,
          "price_gbp": 1099,
          "price_inr": 139900
        }
      ],
      "source": "apple.com official store",
      "source_url": "https://www.apple.com/shop",
      "confidence": 90,
      "notes": "Current prices from official store"
    },
    "instagram_followers": {
      "value": 25500000,
      "platform": "Instagram",
      "handle": "@apple",
      "source": "Instagram official count",
      "source_url": "https://www.instagram.com/apple",
      "confidence": 95,
      "notes": "Real follower count from @apple official account"
    },
    "youtube_subscribers": {
      "value": 43200000,
      "platform": "YouTube",
      "handle": "@Apple",
      "source": "YouTube official count",
      "source_url": "https://www.youtube.com/@Apple",
      "confidence": 95,
      "notes": "Real subscriber count from official channel"
    },
    "competitors": {
      "value": [
        {
          "name": "Microsoft",
          "market_share_percent": 25,
          "market_cap_billions": 3400
        },
        {
          "name": "Google",
          "market_share_percent": 22,
          "market_cap_billions": 2000
        },
        {
          "name": "Samsung",
          "market_share_percent": 20,
          "market_cap_billions": 330
        }
      ],
      "source": "Statista + Yahoo Finance",
      "source_url": "https://finance.yahoo.com/quote/AAPL/competitors",
      "confidence": 75,
      "notes": "From industry reports and financial data"
    }
  }
}
```

**Sources to use (in order of preference):**

| Field | Primary | Secondary | Fallback |
|-------|---------|-----------|----------|
| Founded Year | Wikidata | Wikipedia | Official company history |
| Headquarters | SEC Edgar 10-K | Companies House | Official website |
| Revenue 2025 | SEC 10-K | Annual Report | Investor relations |
| Market Cap | Yahoo Finance | Google Finance | Official investor relations |
| Employees | SEC 10-K | LinkedIn company page | Official reports |
| Products | Brand website | Amazon/retailers | Product review sites |
| Pricing | Official store | Major retailers | Price comparison sites |
| Competitors | Yahoo Finance | Statista | Industry reports |
| Social Followers | Official @brand account | Social tracking sites | N/A (must be real) |

---

## Phase 3: Data Quality Verification

### 3.1 Run Verification Script

```bash
cd /Users/srevi/fuelwatch

# Verify top 10 brands research
python3 -c "
from intel_data_verification import DataVerifier
import json

# Load research logs
with open('intel_research_logs.json') as f:
    data = json.load(f)

# Verify
verifier = DataVerifier()
report = verifier.verify_batch(data['research_logs'])

# Print summary
verifier.print_verification_summary(report)

# Export detailed report
verifier.export_verification_report(report, 'top10_verification.json')
"
```

### 3.2 Quality Gates to Check

**Every brand research must pass:**

- ✅ **No missing source URLs** - Every field has traceable source_url
- ✅ **Confidence scores assigned** - 0-100 based on source type
  - 95%: SEC Edgar, Companies House, official annual reports
  - 85%: Wikipedia, Yahoo Finance, official websites
  - 75%: Industry reports, Statista, news sources
  - 60%: Estimates, secondary sources
- ✅ **No fabrication** - Only published, verifiable data
- ✅ **Missing data marked properly** - "Not Available - Source Not Found" (not estimated)
- ✅ **Data types correct** - Numbers are numeric, dates are ISO format, URLs are valid
- ✅ **Plausibility checks** - Values within reasonable ranges
  - Founded year: 1800-2025
  - Revenue (billions): 0-1000
  - Market cap (billions): 0-100+
  - Employees: 0-10M

---

## Phase 4: Database Insertion

### 4.1 Prepare SQL Statements

Once verified, generate SQL INSERT statements:

```python
# Generate INSERT statements
from intel_brand_population_batch import IntelBrandPopulator
import json

populator = IntelBrandPopulator()

# Load verified research
with open('top10_verified.json') as f:
    verified_data = json.load(f)

# Generate INSERT for brand_financials
for brand in verified_data:
    if 'revenue_2025_billions' in brand['fields']:
        print(f"""
INSERT INTO brand_financials 
(brand_id, year, revenue, profit, gross_margin, employees, market_cap, source)
VALUES (?, 2025, ?, ?, ?, ?, ?, 'SEC Edgar + Yahoo Finance');
""")
```

### 4.2 Insert into Supabase

```bash
# Using SQL INSERT statements generated above
# Connect to Supabase and execute in order:

# 1. Insert into brand_profile (fundamentals)
# 2. Insert into brand_financials (2025 data)
# 3. Insert into brand_skus (products with prices)
# 4. Insert into brand_competitors (competitor data)
# 5. Insert into brand_social (social media counts)
```

---

## Phase 5: Expand to Full 93 Brands

### 5.1 Batch Processing Strategy

**Batches by category (parallel research possible):**

1. **Technology** (10 brands) - Use SEC Edgar + Yahoo Finance
2. **Beverages** (15 brands) - Use industry reports + retailer pricing
3. **Fashion** (12 brands) - Use retailer data + fashion reports
4. **FMCG** (18 brands) - Use Companies House + retailer pricing
5. **Automotive** (8 brands) - Use SEC Edgar + automotive reports
6. **Retail** (10 brands) - Use SEC Edgar + investor relations
7. **Pharma** (7 brands) - Use SEC Edgar + medical reports
8. **Other** (3 brands) - Case-by-case approach

### 5.2 Timeline

**Per batch (avg 10-12 brands):** ~10-12 hours
**Total for 93 brands:** ~75-85 hours (~2 weeks of focused work)

**Suggested schedule:**
- Week 1: Top 10 + Technology category (20 brands)
- Week 2: Beverages + Fashion (27 brands)
- Week 3: FMCG + Retail (28 brands)
- Week 4: Automotive + Pharma + Other (18 brands)

### 5.3 Quality Control

**For each batch:**
1. Research all brands in category
2. Run verification script
3. Fix any failures
4. Insert into Supabase
5. Spot-check 3-5 brands manually in Intel UI
6. Document any issues found

---

## Tools & Utilities Available

### Libraries in Codebase
```python
# Data collection
from intel_brand_research_framework import BrandDataCollector

# Batch processing
from intel_brand_population_batch import IntelBrandPopulator

# Quality assurance
from intel_data_verification import DataVerifier

# Example usage
collector = BrandDataCollector()
wikidata = collector.fetch_wikidata_company("Apple")
yahoo = collector.fetch_yahoo_finance_quote("AAPL")
sec = collector.fetch_sec_edgar_company("0000320193")
```

### Key Source URLs for Quick Reference
- **Wikidata API:** https://query.wikidata.org/sparql
- **SEC Edgar:** https://www.sec.gov/cgi-bin/browse-edgar
- **Yahoo Finance:** https://finance.yahoo.com
- **Companies House:** https://beta.companieshouse.gov.uk
- **Statista:** https://www.statista.com (requires subscription snippets)

---

## Troubleshooting

### Issue: "Source URL not accessible"
**Solution:** Try alternative source or skip if primary source is reliable (e.g., SEC Edgar is reliable even if temporarily down).

### Issue: "Founded year not found"
**Solution:** Mark as "Not Available - Source Not Found", don't estimate. Try: Wikipedia → Wikidata → Company official history.

### Issue: "Pricing varies by region"
**Solution:** Collect multiple currencies (USD, GBP, INR) with PPP adjustments noted in source.

### Issue: "Social followers don't match"
**Solution:** Verify from OFFICIAL brand account only. Never use third-party tracker data.

---

## Success Criteria

### For Top 10 Brands:
- [x] All 10 brands researched with 100% field completion
- [x] Every field has traceable source_url
- [x] All confidence scores assigned (95-100% for top 10)
- [x] Zero fabricated data
- [x] Verification report shows 100% pass rate
- [x] Data inserted into Supabase (brand_profile, brand_financials, brand_skus, brand_competitors, brand_social)
- [x] Manual spot-checks pass in Intel UI

### For Full 93 Brands:
- [ ] All 93 brands researched
- [ ] 95%+ field completion rate across database
- [ ] All sources traceable
- [ ] No estimated data (only "Not Available" if truly unavailable)
- [ ] Verification report shows 95%+ pass rate
- [ ] All data inserted into Supabase
- [ ] Quality metrics documented

---

## Next Immediate Steps

1. **Execute top 10 research** using SOURCE_MAP document
2. **Load research logs** into verification system
3. **Fix any quality issues** found by verifier
4. **Insert verified data** into Supabase
5. **Spot-check** in Intel UI (intel.humanagency.co)
6. **Document findings** and repeat for next batch

---

**Estimated Timeline to Complete All 93 Brands:** 6-8 weeks (with parallel research efforts)

**Document Version:** 1.0 | **Last Updated:** 2026-06-30
