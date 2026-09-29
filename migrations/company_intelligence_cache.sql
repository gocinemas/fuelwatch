-- Company Intelligence Cache table
-- Stores fetched company data to avoid re-fetching from EDGAR/Wikipedia

DROP TABLE IF EXISTS company_intelligence_cache CASCADE;

CREATE TABLE company_intelligence_cache (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_name TEXT NOT NULL UNIQUE,
  data JSONB NOT NULL DEFAULT '{}',
  
  -- Metadata
  cached_at TIMESTAMPTZ DEFAULT NOW(),
  last_viewed TIMESTAMPTZ,
  views INTEGER DEFAULT 0,
  
  -- Source tracking
  source TEXT,  -- "EDGAR", "Wikipedia", "Combined"
  cache_version INT DEFAULT 1,
  
  -- Lifecycle
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_company_cache_name ON company_intelligence_cache(company_name);
CREATE INDEX idx_company_cache_views ON company_intelligence_cache(views DESC);

ALTER TABLE company_intelligence_cache ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Allow all company cache" ON company_intelligence_cache;
CREATE POLICY "Allow all company cache" ON company_intelligence_cache FOR ALL USING (TRUE);
