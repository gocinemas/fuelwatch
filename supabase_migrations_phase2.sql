-- Phase 2 + 3: Intel Product Migrations
-- Run this in Supabase SQL editor

-- Table 1: Brand SKUs by Country (for country-specific best sellers)
CREATE TABLE IF NOT EXISTS brand_skus_by_country (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  country TEXT NOT NULL,
  sku_name TEXT NOT NULL,
  category TEXT,
  price TEXT,
  monthly_sales INTEGER,
  market_position INTEGER DEFAULT 999,
  created_at TIMESTAMP DEFAULT NOW(),
  last_checked TIMESTAMP DEFAULT NOW(),
  UNIQUE(brand_name, country, sku_name)
);

CREATE INDEX IF NOT EXISTS idx_brand_skus_country_brand ON brand_skus_by_country(brand_name);
CREATE INDEX IF NOT EXISTS idx_brand_skus_country_country ON brand_skus_by_country(country);
CREATE INDEX IF NOT EXISTS idx_brand_skus_country_position ON brand_skus_by_country(market_position);

-- Table 2: Competitor SKUs (for Phase 3 - cross-brand comparison)
CREATE TABLE IF NOT EXISTS competitor_skus (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL,
  competitor_name TEXT NOT NULL,
  competitor_sku_name TEXT NOT NULL,
  category TEXT,
  price TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  last_checked TIMESTAMP DEFAULT NOW(),
  UNIQUE(brand_name, competitor_name, competitor_sku_name)
);

CREATE INDEX IF NOT EXISTS idx_competitor_skus_brand ON competitor_skus(brand_name);
CREATE INDEX IF NOT EXISTS idx_competitor_skus_competitor ON competitor_skus(competitor_name);

-- Seed some sample data for Nike
INSERT INTO brand_skus_by_country (brand_name, country, sku_name, category, price, market_position, monthly_sales) VALUES
  ('Nike', 'US', 'Air Force 1', 'Footwear', '90', 1, 500000),
  ('Nike', 'US', 'Jordan 1 Retro', 'Footwear', '170', 2, 300000),
  ('Nike', 'US', 'Blazer Mid', 'Footwear', '100', 3, 250000),
  ('Nike', 'US', 'Air Max 90', 'Footwear', '130', 4, 200000),
  ('Nike', 'GB', 'Air Force 1', 'Footwear', '85', 1, 150000),
  ('Nike', 'GB', 'Cortez', 'Footwear', '95', 2, 120000),
  ('Nike', 'JP', 'Air Max Plus', 'Footwear', '140', 1, 180000),
  ('Nike', 'JP', 'Dunk Low', 'Footwear', '110', 2, 160000)
ON CONFLICT DO NOTHING;

-- Seed competitor SKUs for Nike
INSERT INTO competitor_skus (brand_name, competitor_name, competitor_sku_name, category, price) VALUES
  ('Nike', 'Adidas', 'Superstar', 'Footwear', '85'),
  ('Nike', 'Adidas', 'Stan Smith', 'Footwear', '90'),
  ('Nike', 'Adidas', 'Ultraboost', 'Footwear', '180'),
  ('Nike', 'Puma', 'Suede Classic', 'Footwear', '75'),
  ('Nike', 'Puma', 'RS-X', 'Footwear', '95'),
  ('Nike', 'New Balance', '574', 'Footwear', '110'),
  ('Nike', 'New Balance', '990v5', 'Footwear', '185')
ON CONFLICT DO NOTHING;

-- Grant permissions (adjust to your Supabase user if needed)
GRANT SELECT, INSERT, UPDATE, DELETE ON brand_skus_by_country TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON competitor_skus TO authenticated;
