-- Intel Phase 1: Brand Intelligence Schema
-- Foundation layer: Real data only (no Groq, no fabrication)

-- Main table: Brand Phase 1 Intelligence
CREATE TABLE IF NOT EXISTS brand_phase1_intelligence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Identification
    brand_name TEXT NOT NULL,
    category TEXT NOT NULL, -- 'skincare', 'beverages', 'snacks', 'qsr', etc.
    market_country TEXT NOT NULL, -- 'UK', 'US', 'India', 'Brazil', 'Indonesia'
    market_iso_code VARCHAR(2), -- 'GB', 'US', 'IN', etc.

    -- 1. FUNDAMENTALS
    founded_year INT,
    headquarters_city TEXT,
    headquarters_country TEXT,
    official_website TEXT,
    parent_company TEXT,

    -- 2. COMPETITIVE POSITIONING
    positioning_tier VARCHAR(50), -- 'economy', 'mass-market', 'mass-prestige', 'premium', 'luxury'
    direct_competitor_1 TEXT,
    direct_competitor_2 TEXT,
    direct_competitor_3 TEXT,
    positioning_summary TEXT, -- e.g., "Premium efficacy at affordable price"

    -- 3. TARGET SEGMENT
    target_demographic TEXT, -- e.g., "Women 30-55, value-conscious"
    target_income_tier VARCHAR(50), -- 'low', 'lower-middle', 'upper-middle', 'affluent', 'high'
    segment_size_millions NUMERIC,
    segment_size_source TEXT, -- e.g., 'Statista', 'World Bank', 'Estimate'

    -- 4. PRICING (PPP-ADJUSTED)
    price_local NUMERIC,
    price_currency VARCHAR(3), -- 'GBP', 'USD', 'INR', 'BRL'
    ppp_index NUMERIC, -- World Bank PPP index (reference=1.0 for USD)
    price_usd_equivalent NUMERIC,
    pricing_rationale TEXT, -- e.g., "Positioned for upper-middle income segment"

    -- 5. CATEGORY DYNAMICS
    category_growth_cagr_3yr NUMERIC, -- Historical 3-year CAGR
    market_status VARCHAR(30), -- 'mature' (<5%), 'emerging' (5-10%), 'high_growth' (>10%)
    growth_driver TEXT, -- e.g., 'rising_affluence', 'premiumization', 'new_demographics'

    -- 6. DISTRIBUTION CHANNELS
    distribution_channels TEXT[], -- ARRAY of channels: ['amazon', 'tesco', 'premium_retail', ...]
    distribution_strategy VARCHAR(50), -- 'mass_market', 'selective', 'exclusive'

    -- 7. MARKETING PLAYBOOK
    brand_tagline TEXT,
    primary_benefit TEXT, -- Functional benefit
    emotional_benefit TEXT, -- Emotional/aspirational benefit
    competitive_claim TEXT, -- Why choose this over competitors?
    marketing_tone VARCHAR(50), -- 'scientific', 'aspirational', 'fun', 'trustworthy', etc.
    marketing_channels TEXT[], -- ['tv', 'instagram', 'youtube', 'digital_ads', 'retail']
    positioning_note TEXT,

    -- 8. DATA QUALITY & SOURCING
    data_completeness NUMERIC, -- 0-100%
    sources_used TEXT[], -- ['wikipedia', 'amazon', 'retailer_site', 'reddit', 'world_bank', ...]
    confidence_score NUMERIC, -- 0-100 (how confident in this data?)
    last_verified_date DATE,
    data_notes TEXT, -- e.g., "PPP index from World Bank 2024"

    -- 9. METADATA
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by TEXT,

    -- Unique constraint: one entry per brand-market-category
    UNIQUE(brand_name, category, market_country)
);

-- Top SKUs table (linked to Phase 1)
CREATE TABLE IF NOT EXISTS brand_phase1_skus (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_phase1_id UUID NOT NULL REFERENCES brand_phase1_intelligence(id),

    sku_name TEXT NOT NULL,
    sku_category TEXT, -- Product sub-category
    price_local NUMERIC,
    price_currency VARCHAR(3),
    price_usd_equivalent NUMERIC,

    availability TEXT[], -- ['amazon', 'tesco', 'nykaa', ...]
    bestseller_rank INT, -- 1 = top seller, null = not known
    reason_popular TEXT, -- e.g., "Best-selling anti-aging formula", "Market leader variant"

    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(brand_phase1_id, sku_name)
);

-- Competitive positioning reference table (for perceptual maps)
CREATE TABLE IF NOT EXISTS brand_phase1_competitors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_phase1_id UUID NOT NULL REFERENCES brand_phase1_intelligence(id),

    competitor_name TEXT NOT NULL,
    competitor_positioning_tier VARCHAR(50),
    competitor_price_local NUMERIC,
    competitor_price_currency VARCHAR(3),
    positioning_vs_subject TEXT, -- e.g., "Cheaper but lower quality", "Same quality, lower price"

    created_at TIMESTAMP DEFAULT NOW()
);

-- Market entry scoring table (Phase 2, but schema now)
CREATE TABLE IF NOT EXISTS brand_phase1_market_entry_scoring (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_phase1_id UUID NOT NULL REFERENCES brand_phase1_intelligence(id),

    -- Scoring inputs
    market_size_score NUMERIC, -- 0-100
    category_growth_score NUMERIC, -- 0-100
    purchasing_power_score NUMERIC, -- 0-100
    segment_affluence_score NUMERIC, -- 0-100
    competitive_intensity_score NUMERIC, -- 0-100 (higher = more intense)
    localization_effort_score NUMERIC, -- 0-100 (higher = more effort needed)

    -- Computed score
    overall_entry_score NUMERIC, -- 0-100
    entry_recommendation VARCHAR(20), -- 'green' (>75), 'yellow' (50-75), 'red' (<50)
    recommendation_reason TEXT,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_brand_name ON brand_phase1_intelligence(brand_name);
CREATE INDEX idx_market_country ON brand_phase1_intelligence(market_country);
CREATE INDEX idx_category ON brand_phase1_intelligence(category);
CREATE INDEX idx_positioning_tier ON brand_phase1_intelligence(positioning_tier);
CREATE INDEX idx_market_status ON brand_phase1_intelligence(market_status);

-- Create updated_at trigger
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER brand_phase1_update_timestamp
BEFORE UPDATE ON brand_phase1_intelligence
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();
