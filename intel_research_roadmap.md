# Intel Brands Research Roadmap
**Assessment Date:** 2026-06-30  
**Total Brands:** 146 (not 93 as initially stated)

## 📊 Current Database State

| Table | Records | Brands Covered | % Coverage | Status |
|-------|---------|----------------|-----------|--------|
| brand_profile | 146 | 146 | **99.3%** | ✅ Complete |
| brand_financials | 103 | 103 | **70.5%** | 🟡 Partial |
| brand_skus_complete | 0 | 0 | **0%** | 🔴 Empty |
| brand_competitors_complete | 0 | 0 | **0%** | 🔴 Empty |
| brand_social_media | 96 | 34 | **23.3%** | 🔴 Sparse |

## 🎯 Priority Tier 1 (Top 10 Brands)
These have largest global impact + best data availability:

1. **Coca Cola** (1886, USA) - Beverage
2. **Nike** (1972, USA) - Apparel/Sports
3. **Apple** (2007, USA) - Tech
4. **McDonald's** (1940, USA) - QSR
5. **Samsung** (1969, South Korea) - Electronics
6. **Starbucks** (1971, USA) - Beverage
7. **Amazon** (1994, USA) - Retail/Tech
8. **Pepsi** (1893, USA) - Beverage
9. **Google** (1998, USA) - Tech
10. **Microsoft** (1975, USA) - Tech

## 🎯 Priority Tier 2 (Fast-follow brands)
**11-30:** Medium-sized global brands with strong data availability

- Nestlé, Unilever, P&G, Mercedes, Tesla, Adidas, H&M, LVMH, Disney, Sony, BMW, Toyota, Honda, Rolex, Chanel, Dior, Gucci, Hermès, Prada, Bulgari

## 📚 Data Sources by Category

### **Technology (Apple, Google, Microsoft, Samsung, Amazon, OnePlus, Tesla)**
- **SEC Edgar** (10-K annual reports for US public companies) — Revenue, profit margin, employees
- **Yahoo Finance** — Market cap, P/E ratio, dividend yield
- **Official investor relations** (e.g., apple.com/investor) — Latest earnings
- **Wikipedia** — Founding year, HQ, description
- **Official website** (pricing for products)
- **Retail partners** (Amazon, Best Buy) — Actual SKU pricing
- **Social media official accounts** — Follower counts

### **Food & Beverage (Coca Cola, Pepsi, Nestlé, Starbucks, McDonald's)**
- **SEC Edgar** (Coca-Cola, PepsiCo public companies)
- **Companies House** (UK companies)
- **Official investor pages** — Revenue, market share
- **Statista** (paid snippets for market size)
- **Retail pricing** (Tesco, Sainsbury's, Walmart, Target)
- **Global market research** (Eurostat, IMF, World Bank PPP)
- **Wikipedia** — Historical data

### **Apparel & Luxury (Nike, Adidas, H&M, LVMH, Chanel)**
- **SEC Edgar** (Nike, Adidas public companies)
- **Official brand websites** — Pricing, product lines
- **Retail partners** (Selfridges, Harrods, SSENSE, Farfetch)
- **Market research** (Statista fashion market share)
- **Companies House** (UK operations)
- **Instagram official accounts** — Follower counts

### **Automotive (Tesla, BMW, Mercedes, Toyota)**
- **SEC Edgar** (Tesla, GM, Ford)
- **Official manufacturer websites** — Pricing, specs
- **Dealer networks** (actual market pricing)
- **Market research firms** (Statista car market share)
- **Wikipedia** — Founding year, HQ
- **YouTube official channels** — Subscriber counts

## 📋 Data Points to Populate

### **Per Brand (brand_profile)**
- ✅ Name, founded_year, origin_city, origin_country — **DONE (99.3%)**
- ✅ Description, website, headquarters, tagline — **DONE (99.3%)**
- 🔴 Logo URL — **MISSING (check brand.com/logo)**

### **Financial Data (brand_financials)**
- **Revenue 2024/2025** (from 10-K, annual report, investor site)
- **Market Cap** (from Yahoo Finance, official reports)
- **Profit Margin** (net income / revenue)
- **Growth Rate** (YoY revenue growth %)
- **Net Income** (from financial statements)
- **EBITDA** (operating profit before depreciation)
- **Employees** (from annual report)
- **CIK Number** (for SEC EDGAR lookup)
- **Source attribution + URL** (every data point)
- **Confidence score** (SEC 95%, Wikipedia 85%, estimates 60%)

### **Products/SKUs (brand_skus_complete)**
Per brand, per market (USA, UK, India):
- **Top 3-5 products** (product name, category)
- **Pricing in local currency** (GBP, USD, INR)
- **PPP adjustment** (World Bank PPP indices)
- **Example:**
  ```
  Nike Air Force 1: $90 (USA), £72 (UK, PPP 1.0), ₹7200 (India, PPP 0.25)
  ```
- **Source attribution** (nike.com, Amazon, Boots)

### **Competitors (brand_competitors_complete)**
- **Top 3 direct competitors** (by market share)
- **Market share %** (from industry reports)
- **Positioning** (premium vs. value, positioning vs. focal brand)
- **Source** (Statista snippets, Google Finance)

### **Social Media (brand_social_media)**
- **Instagram followers** (official account only)
- **YouTube subscribers** (official channel)
- **TikTok followers** (if applicable)
- **Twitter/X followers** (if applicable)
- **Do NOT estimate** — scrape actual public counts
- **Last updated date**
- **Source: official platform accounts**

## 🔍 Source Priority Hierarchy

1. **SEC EDGAR** (10-K, quarterly reports) — Confidence: 95%
2. **Companies House** (UK annual accounts) — Confidence: 95%
3. **Official investor relations** (investor.brand.com) — Confidence: 90%
4. **Yahoo Finance / Bloomberg** (market data) — Confidence: 85%
5. **Wikipedia** (founding, HQ, description) — Confidence: 85%
6. **Statista / market research** (market share, market size) — Confidence: 80%
7. **Official brand website** (pricing, products) — Confidence: 75%
8. **Retailer actual pricing** (Amazon, Boots, Tesco) — Confidence: 75%
9. **Earnings call transcripts** (SeekingAlpha) — Confidence: 70%
10. **Press releases / news** (brand.com/news) — Confidence: 65%
11. **Reddit sentiment / estimates** — Confidence: 40% (avoid unless necessary)

## 📊 Research Batches

### Batch 1: Tier 1 Brands (Start immediately)
**Brands:** Coca Cola, Nike, Apple, McDonald's, Samsung  
**Effort:** 2-3 hours (parallel research)  
**Data points:** Full financials, 10 SKUs per market, 3 competitors, social media  
**Sources:** SEC + Companies House + Official websites + Retailers  

### Batch 2: Tier 1 Extended + Tier 2 (Days 2-3)
**Brands:** Starbucks, Amazon, Pepsi, Google, Microsoft, Nestlé, Unilever, P&G  
**Effort:** 3-4 hours (parallel research)  

### Batch 3: Tier 2 Full + Long Tail
**Brands:** Remaining 46+ brands  
**Effort:** 2-3 hours (batch processing)  
**Simplified data:** Core financials only (if SKU data unavailable)  

## ✅ Quality Gate Checklist

- [ ] Every numeric value has source URL
- [ ] Every assertion is traceable to official source
- [ ] Confidence score assigned per source quality
- [ ] Missing data marked "Not Available - Source Not Found" (not estimated)
- [ ] PPP adjustments verified against World Bank data
- [ ] Social media counts from official accounts only
- [ ] Competitor market share from institutional sources (Statista, etc.)
- [ ] SQL INSERT/UPDATE statements ready for Supabase
- [ ] Source attribution template filled for each field

## 📈 Estimated Effort

| Task | Brands | Hours | Rate |
|------|--------|-------|------|
| Tier 1 (5 brands, full data) | 5 | 2-3 | Parallel web research + data entry |
| Tier 1 Extended (8 brands) | 8 | 3-4 | Parallel web research + data entry |
| Tier 2 (30 brands, core data) | 30 | 2-3 | Batch processing, simplified |
| Tier 3 Long Tail (103 brands, essentials) | 103 | 2-3 | Batch automation where possible |
| **Total** | **146** | **9-13 hours** | **Parallel + batching** |

## 🚀 Next Steps

1. **Verify data sources** — Confirm SEC Edgar, Companies House, Yahoo Finance access
2. **Build source template** — Standardize attribution format
3. **Start Batch 1** — Research top 5 brands
4. **Monitor confidence scores** — Flag where data quality is low
5. **Automate where possible** — Yahoo Finance API, SEC EDGAR API for bulk financials
6. **Stage SQL** — Prepare INSERT/UPDATE statements before bulk load

