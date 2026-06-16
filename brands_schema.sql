-- Brand Intelligence Database Schema
-- Created for Phase 1: Brand Essentials

-- Main brands table
CREATE TABLE IF NOT EXISTS brands (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    category VARCHAR(100),
    description TEXT,
    history TEXT,
    founded_year INT,
    website VARCHAR(255),
    wikipedia_url VARCHAR(500),
    knowledge_graph_data JSONB,  -- Store Google KG data as backup
    logo_url VARCHAR(500),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Brand SKUs/Products
CREATE TABLE IF NOT EXISTS brand_skus (
    id SERIAL PRIMARY KEY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    upc VARCHAR(20),
    sku VARCHAR(100),
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    description TEXT,
    image_url VARCHAR(500),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Brand Financials (for public companies)
CREATE TABLE IF NOT EXISTS brand_financials (
    id SERIAL PRIMARY KEY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    year INT,
    revenue BIGINT,  -- in dollars
    profit BIGINT,
    gross_margin DECIMAL(5, 2),
    employees INT,
    market_cap BIGINT,
    cik VARCHAR(20),  -- SEC CIK number
    source VARCHAR(100),  -- "SEC", "news", etc
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Brand Competitors
CREATE TABLE IF NOT EXISTS brand_competitors (
    id SERIAL PRIMARY KEY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    competitor_name VARCHAR(255),
    category VARCHAR(100),
    market_share DECIMAL(5, 2),
    tracked BOOLEAN DEFAULT FALSE,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(brand_id, competitor_name)
);

-- Competitor tracking over time
CREATE TABLE IF NOT EXISTS competitor_tracking (
    id SERIAL PRIMARY KEY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    competitor_id INT REFERENCES brand_competitors(id) ON DELETE CASCADE,
    tracking_month DATE,
    rank INT,
    market_share DECIMAL(5, 2),
    estimated_ad_spend INT,
    ad_platforms VARCHAR(500),  -- YouTube, TikTok, Instagram, etc
    tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Brand Social Media Presence
CREATE TABLE IF NOT EXISTS brand_social (
    id SERIAL PRIMARY KEY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    platform VARCHAR(50),  -- YouTube, TikTok, Instagram, Twitter
    handle VARCHAR(255),
    url VARCHAR(500),
    followers INT,
    verified BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Brand Reddit Sentiment & Mentions
CREATE TABLE IF NOT EXISTS brand_reddit_sentiment (
    id SERIAL PRIMARY KEY,
    brand_id INT REFERENCES brands(id) ON DELETE CASCADE,
    subreddit VARCHAR(255),
    mentions INT,
    sentiment_score DECIMAL(3, 2),  -- -1 to 1
    positive_comments INT,
    negative_comments INT,
    neutral_comments INT,
    last_scraped TIMESTAMP,
    tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster queries
CREATE INDEX idx_brands_name ON brands(name);
CREATE INDEX idx_brands_category ON brands(category);
CREATE INDEX idx_brand_skus_brand_id ON brand_skus(brand_id);
CREATE INDEX idx_brand_skus_upc ON brand_skus(upc);
CREATE INDEX idx_brand_competitors_brand_id ON brand_competitors(brand_id);
CREATE INDEX idx_brand_financials_brand_id ON brand_financials(brand_id);
CREATE INDEX idx_competitor_tracking_brand_id ON competitor_tracking(brand_id);
CREATE INDEX idx_brand_social_brand_id ON brand_social(brand_id);
