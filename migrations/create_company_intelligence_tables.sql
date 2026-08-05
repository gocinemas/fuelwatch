-- Company Intelligence Database Tables
-- Run this in Supabase SQL editor to set up the knowledge base

-- Table: company_queries
-- Stores all Q&A interactions about companies
CREATE TABLE IF NOT EXISTS company_queries (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  company_name TEXT NOT NULL,
  question TEXT NOT NULL,
  answer TEXT NOT NULL,
  source TEXT DEFAULT 'claude_api',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_queries_company ON company_queries(company_name);
CREATE INDEX IF NOT EXISTS idx_company_queries_created ON company_queries(created_at);

-- Table: company_profiles
-- Cumulative company intelligence (updated as queries come in)
CREATE TABLE IF NOT EXISTS company_profiles (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  company_name TEXT UNIQUE NOT NULL,
  data JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_profiles_name ON company_profiles(company_name);

-- Sample data structure for company_profiles.data:
-- {
--   "company_name": "Reckitt",
--   "total_queries": 47,
--   "ai_mentions": 23,
--   "hiring_mentions": 15,
--   "strategy_mentions": 18,
--   "risk_mentions": 12,
--   "recent_topics": ["What's their AI strategy?", "Are they hiring for AI?", ...],
--   "created_at": "2026-08-05T10:00:00",
--   "last_updated": "2026-08-05T14:30:00"
-- }

-- Enable RLS if needed
ALTER TABLE company_queries ENABLE ROW LEVEL SECURITY;
ALTER TABLE company_profiles ENABLE ROW LEVEL SECURITY;

-- Sample policies (allow all for now, restrict in production)
CREATE POLICY "Allow all queries" ON company_queries FOR ALL USING (TRUE);
CREATE POLICY "Allow all profiles" ON company_profiles FOR ALL USING (TRUE);

-- Table: company_financials
-- Historical financial metrics for trend analysis
CREATE TABLE IF NOT EXISTS company_financials (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  company_name TEXT NOT NULL,
  period TEXT NOT NULL, -- "Q1 2026", "2025", etc.
  revenue_millions NUMERIC,
  gross_margin_pct NUMERIC,
  operating_margin_pct NUMERIC,
  net_margin_pct NUMERIC,
  employees INTEGER,
  revenue_growth_pct NUMERIC,
  source TEXT DEFAULT 'manual', -- 'sec_edgar', 'yahoo', 'manual', 'news_analysis'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_financials_company ON company_financials(company_name);
CREATE INDEX IF NOT EXISTS idx_company_financials_period ON company_financials(period);

-- Table: company_deals
-- M&A activity, acquisitions, fundraising
CREATE TABLE IF NOT EXISTS company_deals (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  company_name TEXT NOT NULL,
  deal_type TEXT NOT NULL, -- 'acquisition', 'acquired_by', 'investment', 'ipo'
  target_company TEXT, -- for acquisitions
  investor_company TEXT, -- for investments
  amount_millions NUMERIC,
  announcement_date DATE,
  completion_date DATE,
  description TEXT,
  source TEXT DEFAULT 'crunchbase',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_deals_company ON company_deals(company_name);
CREATE INDEX IF NOT EXISTS idx_company_deals_date ON company_deals(announcement_date);

-- Table: company_market_trends
-- Market share, TAM, category growth trends
CREATE TABLE IF NOT EXISTS company_market_trends (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  company_name TEXT NOT NULL,
  category TEXT NOT NULL, -- 'market_share', 'tam', 'category_growth', 'regional_growth'
  metric_name TEXT NOT NULL, -- 'disinfectant_market_share', 'north_america_revenue_growth'
  value_pct NUMERIC, -- percentage value
  period TEXT NOT NULL, -- "Q1 2026", "2025", etc.
  source TEXT DEFAULT 'manual',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_market_trends_company ON company_market_trends(company_name);
CREATE INDEX IF NOT EXISTS idx_company_market_trends_category ON company_market_trends(category);
