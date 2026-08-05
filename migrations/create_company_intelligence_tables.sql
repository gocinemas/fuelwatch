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

CREATE INDEX idx_company_queries_company ON company_queries(company_name);
CREATE INDEX idx_company_queries_created ON company_queries(created_at);

-- Table: company_profiles
-- Cumulative company intelligence (updated as queries come in)
CREATE TABLE IF NOT EXISTS company_profiles (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  company_name TEXT UNIQUE NOT NULL,
  data JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_company_profiles_name ON company_profiles(company_name);

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
