-- Intel Historical Tracking Schema
-- Store all brand data over time to track trends

-- Historical financials (track revenue, profit, margins over time)
CREATE TABLE IF NOT EXISTS brand_financials_history (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    brand_name VARCHAR(255),
    tracked_date DATE DEFAULT CURRENT_DATE,
    revenue_billions DECIMAL(10, 2),
    profit_billions DECIMAL(10, 2),
    profit_margin DECIMAL(5, 2),
    revenue_per_employee DECIMAL(10, 2),
    growth_5yr DECIMAL(5, 2),
    source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(brand_name, tracked_date)
);

-- Historical market ranking (track position changes over time)
CREATE TABLE IF NOT EXISTS brand_ranking_history (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    brand_name VARCHAR(255),
    category VARCHAR(100),
    tracked_date DATE DEFAULT CURRENT_DATE,
    rank INT,
    rank_of INT,
    market_cap DECIMAL(10, 2),
    market_share DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(brand_name, category, tracked_date)
);

-- Historical SKU tracking (track prices, volumes, availability over time)
CREATE TABLE IF NOT EXISTS brand_sku_history (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    brand_name VARCHAR(255),
    sku_name VARCHAR(255),
    category VARCHAR(100),
    tracked_date DATE DEFAULT CURRENT_DATE,
    price DECIMAL(10, 2),
    price_currency VARCHAR(10) DEFAULT 'USD',
    volume_units INT,
    volume_unit_type VARCHAR(50), -- ml, grams, kg, units, etc
    retailers_count INT,
    availability_score DECIMAL(3, 2), -- 0-1, how available the product is
    trend VARCHAR(50), -- ↑ growing, ↓ declining, → flat
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(brand_name, sku_name, tracked_date)
);

-- Competitor comparison (track vs competitors over time)
CREATE TABLE IF NOT EXISTS competitor_comparison_history (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    primary_brand_name VARCHAR(255),
    competitor_name VARCHAR(255),
    category VARCHAR(100),
    tracked_date DATE DEFAULT CURRENT_DATE,
    primary_market_cap DECIMAL(10, 2),
    competitor_market_cap DECIMAL(10, 2),
    primary_market_share DECIMAL(5, 2),
    competitor_market_share DECIMAL(5, 2),
    primary_social_spend DECIMAL(10, 2),
    competitor_social_spend DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(primary_brand_name, competitor_name, tracked_date)
);

-- Strategic intelligence tracking (strategy, direction, AI focus)
CREATE TABLE IF NOT EXISTS brand_intelligence_insights (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    brand_name VARCHAR(255),
    tracked_date DATE DEFAULT CURRENT_DATE,
    strategic_direction TEXT, -- parsed from earnings calls, interviews
    ai_focus TEXT, -- AI products, investments, partnerships
    key_initiatives TEXT, -- major projects, launches
    ceo_recent_quote TEXT, -- latest public statement
    source VARCHAR(100), -- "SEC Edgar", "Earnings Call", "CEO Interview", "News"
    source_url VARCHAR(500),
    confidence_score DECIMAL(3, 2), -- 0-1, how confident the insight is
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster queries
CREATE INDEX idx_financials_history_brand ON brand_financials_history(brand_name, tracked_date DESC);
CREATE INDEX idx_ranking_history_brand ON brand_ranking_history(brand_name, tracked_date DESC);
CREATE INDEX idx_sku_history_brand ON brand_sku_history(brand_name, tracked_date DESC);
CREATE INDEX idx_competitor_history_date ON competitor_comparison_history(tracked_date DESC);
CREATE INDEX idx_intelligence_brand ON brand_intelligence_insights(brand_name, tracked_date DESC);
