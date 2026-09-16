-- Company Intelligence: shareable company profile pages (/company/<name>)
--
-- NOTE: apply_migrations.py re-executes every *.sql file in this directory
-- on EVERY app startup (there is no "already applied" tracking table), so
-- every statement here must be safe to run repeatedly.

-- Force Supabase schema cache refresh by dropping and recreating
DROP TABLE IF EXISTS company_details CASCADE;

CREATE TABLE company_details (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Identity
  company_name TEXT NOT NULL,          -- display name, e.g. "Ikea"
  slug TEXT NOT NULL UNIQUE,            -- URL/lookup key, e.g. "ikea" (see slugify() in company_intelligence_routes.py)

  -- Enriched profile fields (populated by company_data_populator.py)
  description TEXT,                     -- 2-3 sentence summary
  industry TEXT,
  website TEXT,
  logo_url TEXT,
  headquarters TEXT,
  founded_year INT,
  employee_count TEXT,                  -- free text, e.g. "10,000-50,000"
  social_links JSONB DEFAULT '{}'::jsonb,   -- {twitter, linkedin, instagram, facebook}
  key_facts JSONB DEFAULT '[]'::jsonb,      -- ["fact 1", "fact 2", ...]
  sources JSONB DEFAULT '[]'::jsonb,        -- URLs used to ground the enrichment

  -- Fetch lifecycle
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'enriching', 'ready', 'failed', 'stale')),
  confidence_score NUMERIC,             -- Claude's self-reported 0-1 confidence
  fetch_error TEXT,
  requested_by TEXT,                    -- phone number / IP that first requested this page
  view_count INT DEFAULT 0,
  last_enriched_at TIMESTAMPTZ,

  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_company_details_slug ON company_details(slug);
CREATE INDEX IF NOT EXISTS idx_company_details_status ON company_details(status);
CREATE INDEX IF NOT EXISTS idx_company_details_name ON company_details(company_name);

ALTER TABLE company_details ENABLE ROW LEVEL SECURITY;

-- DROP + CREATE (not "CREATE POLICY IF NOT EXISTS" — unsupported) so this stays idempotent.
DROP POLICY IF EXISTS "Allow all company_details" ON company_details;
CREATE POLICY "Allow all company_details" ON company_details FOR ALL USING (TRUE);
