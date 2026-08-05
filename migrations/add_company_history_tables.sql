-- New tables for company history tracking
-- Run this in Supabase SQL editor

-- Table: company_financials
-- Historical financial metrics for trend analysis
CREATE TABLE IF NOT EXISTS company_financials (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  company_name TEXT NOT NULL,
  period TEXT NOT NULL,
  revenue_millions NUMERIC,
  gross_margin_pct NUMERIC,
  operating_margin_pct NUMERIC,
  net_margin_pct NUMERIC,
  employees INTEGER,
  revenue_growth_pct NUMERIC,
  source TEXT DEFAULT 'manual',
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
  deal_type TEXT NOT NULL,
  target_company TEXT,
  investor_company TEXT,
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
  category TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  value_pct NUMERIC,
  period TEXT NOT NULL,
  source TEXT DEFAULT 'manual',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_market_trends_company ON company_market_trends(company_name);
CREATE INDEX IF NOT EXISTS idx_company_market_trends_category ON company_market_trends(category);
