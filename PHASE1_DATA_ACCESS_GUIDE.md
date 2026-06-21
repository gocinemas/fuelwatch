# Phase 1 Brand Research Data - Access Guide

## Dataset Location
`/Users/srevi/fuelwatch/phase1_brand_research_data.json`

## Quick Stats
- **Total Records:** 60
- **Skincare Brands:** 10 × 3 markets = 30 records
- **Beverage Brands:** 10 × 3 markets = 30 records
- **Markets:** UK (GB), USA (US), India (IN)
- **File Size:** 106 KB
- **Format:** JSON array

## Supabase Import Instructions

### Step 1: Connect to Supabase CLI
```bash
supabase link --project-ref <your-project-ref>
```

### Step 2: Create Table (if not exists)
```sql
CREATE TABLE brand_market_data (
  id BIGSERIAL PRIMARY KEY,
  brand_name VARCHAR(255) NOT NULL,
  category VARCHAR(50) NOT NULL,
  market_country VARCHAR(50) NOT NULL,
  market_iso_code CHAR(2) NOT NULL,
  founded_year INT,
  headquarters_city VARCHAR(100),
  headquarters_country VARCHAR(100),
  official_website VARCHAR(255),
  parent_company VARCHAR(255),
  positioning_tier VARCHAR(50),
  positioning_summary TEXT,
  direct_competitor_1 VARCHAR(255),
  direct_competitor_2 VARCHAR(255),
  direct_competitor_3 VARCHAR(255),
  target_demographic VARCHAR(500),
  target_income_tier VARCHAR(100),
  segment_size_millions FLOAT,
  price_local FLOAT,
  price_currency CHAR(3),
  ppp_index FLOAT,
  price_usd_equivalent FLOAT,
  pricing_rationale TEXT,
  category_growth_cagr_3yr FLOAT,
  market_status VARCHAR(50),
  growth_driver TEXT,
  distribution_channels TEXT[], -- Array
  distribution_strategy VARCHAR(50),
  brand_tagline VARCHAR(500),
  primary_benefit VARCHAR(200),
  emotional_benefit VARCHAR(200),
  competitive_claim VARCHAR(300),
  marketing_tone VARCHAR(100),
  marketing_channels TEXT[], -- Array
  data_completeness INT,
  sources_used TEXT[], -- Array
  confidence_score INT,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(brand_name, market_iso_code, category)
);
```

### Step 3: Batch Insert from JSON
```bash
# Using psql
psql postgresql://user:pass@db.project.supabase.co/postgres \
  -c "INSERT INTO brand_market_data VALUES ..."
```

Or use Supabase Dashboard → SQL Editor and import directly.

## Data Structure

### Core Fields (Always Present)
- `brand_name` - Brand identifier (string)
- `category` - "skincare" or "beverages"
- `market_iso_code` - "GB", "US", or "IN"
- `market_country` - Full country name
- `founded_year` - Brand founding year (integer)

### Positioning & Segmentation
- `positioning_tier` - economy, mass-market, mass-prestige, premium, luxury
- `positioning_summary` - 1-2 sentence positioning statement (max 200 chars)
- `target_demographic` - Age range, income, lifestyle descriptor (max 100 chars)
- `target_income_tier` - lower-middle, middle, upper-middle, affluent
- `segment_size_millions` - Estimated segment size in millions

### Pricing (PPP-Adjusted)
- `price_local` - Retail price in local currency
- `price_currency` - GBP, USD, or INR
- `ppp_index` - Purchasing Power Parity index (GB/US=1.0, IN=0.25)
- `price_usd_equivalent` - Price in USD (PPP-adjusted)
- `pricing_rationale` - Why this price tier in this market

### Market Dynamics
- `category_growth_cagr_3yr` - 3-year compound annual growth rate (%)
- `market_status` - mature (<5%), emerging (5-10%), high-growth (>10%)
- `growth_driver` - What's driving growth in THIS market

### Competitive Intelligence
- `direct_competitor_1`, `_2`, `_3` - Three main competitors
- `competitive_claim` - How this brand differentiates (max 100 chars)
- `primary_benefit` - Main functional benefit (max 80 chars)
- `emotional_benefit` - Emotional/aspirational benefit (max 80 chars)
- `brand_tagline` - Official tagline or positioning claim

### Distribution & Marketing
- `distribution_channels` - Array of channel names (boots, walmart, amazon.in, etc.)
- `distribution_strategy` - mass_market, selective, or exclusive
- `marketing_tone` - scientific, aspirational, playful, energetic, etc.
- `marketing_channels` - Array of channels (TV, YouTube, TikTok, Sports, Social, etc.)

### Data Quality
- `data_completeness` - 0-100 (% of fields with verified data)
- `sources_used` - Array of source references (Wikipedia, Brand.com, Retailer.com)
- `confidence_score` - 0-100 (quality assessment)

## Sample Queries

### Get all brands in a market
```sql
SELECT DISTINCT brand_name, category, positioning_tier 
FROM brand_market_data 
WHERE market_iso_code = 'IN' 
ORDER BY category, positioning_tier;
```

### Compare pricing across markets
```sql
SELECT brand_name, market_iso_code, price_local, price_usd_equivalent 
FROM brand_market_data 
WHERE brand_name = 'Neutrogena' 
ORDER BY market_iso_code;
```

### Find fastest-growing segments
```sql
SELECT brand_name, market_iso_code, category_growth_cagr_3yr 
FROM brand_market_data 
WHERE category_growth_cagr_3yr > 8 
ORDER BY category_growth_cagr_3yr DESC;
```

### Get mass-prestige brands
```sql
SELECT brand_name, market_iso_code, price_usd_equivalent, segment_size_millions 
FROM brand_market_data 
WHERE positioning_tier = 'mass-prestige' 
ORDER BY market_iso_code, segment_size_millions DESC;
```

### Analyze India market
```sql
SELECT 
  brand_name, 
  category, 
  positioning_tier,
  price_local || ' ' || price_currency as local_price,
  segment_size_millions,
  category_growth_cagr_3yr
FROM brand_market_data 
WHERE market_iso_code = 'IN' 
ORDER BY category, positioning_tier, price_local DESC;
```

## Key Validation Rules

✓ All 60 records (20 brands × 3 markets) present
✓ No null values in critical fields
✓ PPP adjustments verified (India ~4-5x lower nominal pricing)
✓ Growth rates: UK 3-3.5%, US 2.5-3.2%, India 8-9.2%
✓ Positioning tiers consistent with pricing
✓ Segment sizes plausible for each market
✓ All source references documented
✓ Confidence scores 88-90%

## Data Limitations & Assumptions

1. **Pricing** - Based on 2026-06 retail snapshot; subject to promotions/discounts
2. **Segment sizes** - Estimated from category size and positioning; not exact census
3. **Growth rates** - Historical 3-year CAGR; future may differ
4. **Distribution** - Lists primary channels; brands may have wider presence
5. **Marketing channels** - Based on current marketing strategies; may evolve

## Next Steps (Phase 2+)

- Real-time pricing sync with retailer APIs
- Competitive intelligence dashboard
- Market entry strategy modeling
- Brand positioning deep-dives by segment
- Regional variant analysis (e.g., Olay Regenerist vs base Olay)

---

**Data Generated:** 2026-06-19  
**Research Method:** Secondary research + expert estimation  
**Confidence Level:** 89% average  
**Update Frequency:** Quarterly (manual)
