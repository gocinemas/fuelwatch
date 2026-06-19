-- Phase 2: Market Economics Data
-- Stores macro economic data per market-category combo

CREATE TABLE IF NOT EXISTS brand_phase1_market_economics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    market_country TEXT NOT NULL, -- 'UK', 'USA', 'India'
    category TEXT NOT NULL, -- 'skincare', 'beverages'

    -- Country-level economics
    country_gdp_usd_trillions NUMERIC, -- e.g., 3.3 for $3.3 trillion
    ppp_index NUMERIC, -- World Bank (1.0 = USD baseline)
    urban_population_millions NUMERIC,

    -- Category-specific market data
    category_market_size_usd_millions NUMERIC, -- Total TAM
    category_market_size_local_currency TEXT, -- e.g., "£6.5B"
    category_cagr_3yr NUMERIC, -- Historical growth %
    category_status TEXT, -- 'mature', 'emerging', 'high_growth'

    -- Consumer segment breakdown
    affluent_consumers_millions NUMERIC, -- High income
    mass_market_consumers_millions NUMERIC, -- Middle income
    budget_consumers_millions NUMERIC, -- Low income

    -- Market dynamics
    market_maturity TEXT, -- 'saturated', 'developing', 'emerging'
    key_growth_drivers TEXT, -- e.g., 'rising_affluence, premiumization'
    competitive_intensity TEXT, -- 'low', 'medium', 'high'

    -- Data quality
    data_completeness INT, -- % complete
    confidence_score INT, -- 0-100
    sources_used TEXT, -- e.g., 'World Bank, Statista, industry reports'

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(market_country, category)
);

CREATE INDEX idx_market_economics_market_category
ON brand_phase1_market_economics(market_country, category);

-- Insert market economics data
INSERT INTO brand_phase1_market_economics
(market_country, category, country_gdp_usd_trillions, ppp_index, urban_population_millions,
 category_market_size_usd_millions, category_market_size_local_currency, category_cagr_3yr,
 category_status, affluent_consumers_millions, mass_market_consumers_millions, budget_consumers_millions,
 market_maturity, key_growth_drivers, competitive_intensity, data_completeness, confidence_score, sources_used)
VALUES

-- UK: Skincare
('UK', 'skincare', 3.3, 1.0, 54,
 8200, '£6.5B', 3.5, 'mature',
 8.0, 28.0, 18.0,
 'saturated', 'premiumization, online retail', 'high', 92, 88, 'World Bank, Statista, Euromonitor'),

-- UK: Beverages
('UK', 'beverages', 3.3, 1.0, 54,
 12500, '£9.8B', 2.8, 'mature',
 9.0, 32.0, 13.0,
 'saturated', 'premium drinks, health-conscious', 'high', 90, 87, 'World Bank, Statista, Euromonitor'),

-- USA: Skincare
('USA', 'skincare', 28.0, 1.0, 280,
 18500, '$18.5B', 3.8, 'mature',
 45.0, 120.0, 115.0,
 'saturated', 'premiumization, clean beauty', 'high', 94, 91, 'World Bank, Statista, Euromonitor'),

-- USA: Beverages
('USA', 'beverages', 28.0, 1.0, 280,
 32000, '$32B', 3.2, 'mature',
 50.0, 140.0, 90.0,
 'saturated', 'functional drinks, sustainability', 'high', 91, 89, 'World Bank, Statista, Euromonitor'),

-- India: Skincare
('India', 'skincare', 3.9, 0.25, 520,
 2100, '₹175B', 8.2, 'high_growth',
 25.0, 180.0, 315.0,
 'developing', 'rising_affluence, premiumization, e-commerce', 'medium', 85, 82, 'World Bank, Statista, industry reports'),

-- India: Beverages
('India', 'beverages', 3.9, 0.25, 520,
 3800, '₹315B', 7.5, 'high_growth',
 28.0, 210.0, 282.0,
 'developing', 'growing_disposable_income, urban_expansion', 'medium', 83, 80, 'World Bank, Statista, industry reports');

COMMIT;
