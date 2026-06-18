-- BRAND INTELLIGENCE SCHEMA - CLEAN SLATE
-- This script drops all existing tables first, then creates everything fresh
-- Run this in Supabase SQL editor

-- DROP ALL EXISTING TABLES (fresh start)
DROP TABLE IF EXISTS brand_ai_strategy CASCADE;
DROP TABLE IF EXISTS brand_podcasts CASCADE;
DROP TABLE IF EXISTS brand_news CASCADE;
DROP TABLE IF EXISTS brand_social_media CASCADE;
DROP TABLE IF EXISTS brand_white_space CASCADE;
DROP TABLE IF EXISTS competing_skus_complete CASCADE;
DROP TABLE IF EXISTS brand_competitors_complete CASCADE;
DROP TABLE IF EXISTS brand_skus_complete CASCADE;
DROP TABLE IF EXISTS brand_financials CASCADE;
DROP TABLE IF EXISTS brand_profile CASCADE;

-- 1. Brand Profile
CREATE TABLE brand_profile (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  founded_year INTEGER,
  origin_city TEXT,
  origin_country TEXT,
  tagline TEXT,
  description TEXT,
  logo_url TEXT,
  website TEXT,
  headquarters TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- 2. Brand Financials
CREATE TABLE brand_financials (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  year INTEGER,
  revenue TEXT,
  market_cap TEXT,
  profit_margin DECIMAL,
  growth_rate DECIMAL,
  net_income TEXT,
  ebitda TEXT,
  source TEXT,
  last_updated TIMESTAMP DEFAULT NOW(),
  UNIQUE(brand_name, year)
);

-- 3. Brand SKUs
CREATE TABLE brand_skus_complete (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  country TEXT,
  sku_name TEXT NOT NULL,
  category TEXT,
  price TEXT,
  monthly_sales_estimate TEXT,
  market_position INTEGER,
  release_year INTEGER,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(brand_name, country, sku_name)
);

-- 4. Brand Competitors
CREATE TABLE brand_competitors_complete (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  competitor_name TEXT NOT NULL,
  market_position INTEGER,
  market_share DECIMAL,
  head_to_head TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(brand_name, competitor_name)
);

-- 5. Competing SKUs
CREATE TABLE competing_skus_complete (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  competitor_name TEXT,
  competitor_sku TEXT,
  category TEXT,
  price TEXT,
  market_position INTEGER,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(brand_name, competitor_name, competitor_sku)
);

-- 6. White Space
CREATE TABLE brand_white_space (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  gap_type TEXT,
  description TEXT,
  market_size TEXT,
  opportunity_score INTEGER,
  growth_adjacency TEXT,
  fit_score DECIMAL,
  created_at TIMESTAMP DEFAULT NOW()
);

-- 7. Social Media
CREATE TABLE brand_social_media (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  platform TEXT,
  followers TEXT,
  reach TEXT,
  estimated_monthly_ad_spend TEXT,
  engagement_rate DECIMAL,
  investment_score DECIMAL,
  last_updated TIMESTAMP DEFAULT NOW(),
  UNIQUE(brand_name, platform)
);

-- 8. News
CREATE TABLE brand_news (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  title TEXT,
  url TEXT,
  source TEXT,
  published_date TIMESTAMP,
  category TEXT,
  fetched_date TIMESTAMP DEFAULT NOW()
);

-- 9. Podcasts
CREATE TABLE brand_podcasts (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  podcast_name TEXT,
  episode_title TEXT,
  url TEXT,
  air_date TIMESTAMP,
  relevance_score DECIMAL,
  fetched_date TIMESTAMP DEFAULT NOW()
);

-- 10. AI Strategy
CREATE TABLE brand_ai_strategy (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  ai_focus_area TEXT,
  announcement_date TIMESTAMP,
  source TEXT,
  fetched_date TIMESTAMP DEFAULT NOW()
);

-- INDEXES
CREATE INDEX idx_brand_profile_name ON brand_profile(name);
CREATE INDEX idx_brand_financials_name ON brand_financials(brand_name);
CREATE INDEX idx_brand_skus_name ON brand_skus_complete(brand_name);
CREATE INDEX idx_brand_competitors_name ON brand_competitors_complete(brand_name);
CREATE INDEX idx_brand_news_name ON brand_news(brand_name);
CREATE INDEX idx_brand_podcasts_name ON brand_podcasts(brand_name);

-- ===== SAMPLE DATA FOR NIKE =====

INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES ('Nike', 1964, 'Beaverton', 'USA', 'Just Do It', 'American multinational corporation that designs, manufactures, and sells athletic footwear, apparel, and accessories', 'nike.com', 'Beaverton, Oregon, USA');

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES ('Nike', 2024, '$46.7B', '$150B', 11.2, 5.3, '$5.1B', 'Yahoo Finance');

INSERT INTO brand_skus_complete (brand_name, country, sku_name, category, price, monthly_sales_estimate, market_position, release_year)
VALUES
('Nike', 'US', 'Air Force 1', 'Footwear', '$90', '500K+', 1, 1982),
('Nike', 'US', 'Air Max 90', 'Footwear', '$130', '300K+', 2, 1990),
('Nike', 'US', 'Jordan 1 Retro', 'Footwear', '$170', '200K+', 3, 1985),
('Nike', 'UK', 'Air Force 1', 'Footwear', '£85', '150K+', 1, 1982),
('Nike', 'JP', 'Air Max Plus', 'Footwear', '¥14,000', '180K+', 1, 1998);

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share)
VALUES
('Nike', 'Adidas', 2, 15.0),
('Nike', 'Puma', 3, 7.0),
('Nike', 'New Balance', 4, 5.5),
('Nike', 'Asics', 5, 4.2);

INSERT INTO competing_skus_complete (brand_name, competitor_name, competitor_sku, category, price, market_position)
VALUES
('Nike', 'Adidas', 'Superstar', 'Footwear', '$85', 1),
('Nike', 'Adidas', 'Stan Smith', 'Footwear', '$90', 2),
('Nike', 'Puma', 'Suede Classic', 'Footwear', '$75', 1),
('Nike', 'Puma', 'RS-X', 'Footwear', '$95', 2),
('Nike', 'New Balance', '574', 'Footwear', '$110', 1),
('Nike', 'New Balance', '990v5', 'Footwear', '$185', 2);

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score)
VALUES
('Nike', 'Sustainable Luxury', 'Premium eco-friendly footwear segment growing 25% YoY', '$5B', 9, 'Sustainable materials + luxury positioning', 8.5),
('Nike', 'AI Personalization', 'AI-powered custom shoe fit and design recommendations', '$2B', 8, 'AI + consumer personalization', 8.2),
('Nike', 'Senior Fitness', 'Adaptive footwear for 65+ age group', '$1.5B', 7, 'Health tech + aging population', 7.3),
('Nike', 'Virtual Sports', 'Metaverse-native sneakers and digital collectibles', '$3B', 8, 'Web3 + gaming ecosystem', 7.8);

INSERT INTO brand_social_media (brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES
('Nike', 'Instagram', '150M', '500M+', '$50M', 2.3, 9.2),
('Nike', 'TikTok', '45M', '200M+', '$30M', 5.1, 8.8),
('Nike', 'YouTube', '38M', '300M+', '$25M', 1.8, 8.5),
('Nike', 'Twitter', '35M', '100M+', '$15M', 0.9, 7.2);

INSERT INTO brand_news (brand_name, title, url, source, published_date, category)
VALUES
('Nike', 'Nike Reports Strong Q1 Growth Amid Retail Recovery', 'https://news.nike.com/q1-2024', 'Nike Newsroom', NOW() - INTERVAL '5 days', 'Earnings'),
('Nike', 'Nike Launches Sustainable Sneaker Line with 100% Recycled Materials', 'https://news.nike.com/sustainability', 'Nike Newsroom', NOW() - INTERVAL '10 days', 'Sustainability'),
('Nike', 'Nike Partners with AI Startup for Personalized Shoe Design', 'https://news.nike.com/ai-partnership', 'Tech News', NOW() - INTERVAL '15 days', 'Technology');

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date, source)
VALUES
('Nike', 'AI-powered shoe fit personalization', NOW() - INTERVAL '30 days', 'Nike Tech Blog'),
('Nike', 'Machine learning for demand forecasting', NOW() - INTERVAL '60 days', 'Nike Innovation'),
('Nike', 'Computer vision for athlete performance tracking', NOW() - INTERVAL '90 days', 'Nike Sports Lab');

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO authenticated;
