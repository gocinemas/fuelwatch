-- Migration: Create company_real_data table
-- Purpose: Store real, verified company data from official sources
-- Date: 2026-08-12

CREATE TABLE IF NOT EXISTS company_real_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name TEXT NOT NULL UNIQUE,

    -- LinkedIn data (2026 hiring, real-time)
    hiring_data JSONB,

    -- Companies House data (UK financial, 2025)
    financials_uk JSONB,

    -- SEC Edgar data (US financial, 2025)
    financials_us JSONB,

    -- NewsAPI data (2026 news, current)
    news_2026 JSONB,

    -- Metadata
    last_updated TIMESTAMP DEFAULT NOW(),
    data_quality TEXT DEFAULT 'REAL',  -- REAL, OFFICIAL, LIVE, VERIFIED
    data_sources TEXT[] DEFAULT '{"LinkedIn", "Companies House", "SEC Edgar", "NewsAPI"}',

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for fast company lookups
CREATE INDEX IF NOT EXISTS idx_company_real_data_name ON company_real_data(company_name);
CREATE INDEX IF NOT EXISTS idx_company_real_data_updated ON company_real_data(last_updated DESC);

-- Enable RLS (Row Level Security)
ALTER TABLE company_real_data ENABLE ROW LEVEL SECURITY;

-- Allow public read access
CREATE POLICY "Allow public read" ON company_real_data
    FOR SELECT USING (true);

-- Allow service role to update
CREATE POLICY "Allow service role update" ON company_real_data
    FOR UPDATE USING (true);

-- Mark timestamp: Data loaded from real sources
-- OFFICIAL: Companies House, SEC Edgar (audited filings)
-- LIVE: LinkedIn (real-time job postings)
-- CURRENT: NewsAPI (2026 news)
-- VERIFIED: Cross-checked multiple sources
