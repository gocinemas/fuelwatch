-- Phase 7: School Terms Caching
-- Store term dates, holidays, inset days for schools to avoid repeated scraping

CREATE TABLE IF NOT EXISTS school_terms (
  id BIGSERIAL PRIMARY KEY,
  school_name TEXT NOT NULL UNIQUE,
  data JSONB DEFAULT NULL, -- {terms: [{name, start, end}], holidays: [{name, start, end}], inset_days: [...]}
  last_updated TIMESTAMP DEFAULT NOW(),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Index on school_name for fast lookups
CREATE INDEX IF NOT EXISTS idx_school_terms_name ON school_terms(school_name);

-- Enable RLS if needed (optional)
ALTER TABLE school_terms ENABLE ROW LEVEL SECURITY;

-- Policy: Allow public read access (useful for brief API)
CREATE POLICY "school_terms_public_read" ON school_terms
  FOR SELECT USING (TRUE);

-- Policy: Allow authenticated writes (admin only)
-- In practice, only the backend service account should upsert
