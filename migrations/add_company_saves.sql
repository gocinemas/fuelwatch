-- Company Saves Table
-- Track which companies users have bookmarked for quick access

CREATE TABLE IF NOT EXISTS company_saves (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  user_id TEXT NOT NULL, -- postcode (miru identity)
  company_name TEXT NOT NULL,
  saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  UNIQUE(user_id, company_name)
);

CREATE INDEX IF NOT EXISTS idx_saves_user ON company_saves(user_id);
CREATE INDEX IF NOT EXISTS idx_saves_company ON company_saves(company_name);

ALTER TABLE company_saves DISABLE ROW LEVEL SECURITY;
