-- Research Request Table
-- Track companies users want researched

CREATE TABLE IF NOT EXISTS company_research_requests (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  company_name TEXT NOT NULL,
  requested_by TEXT, -- email or anonymous
  status TEXT DEFAULT 'pending', -- pending, researching, completed
  notes TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_requests_company ON company_research_requests(company_name);
CREATE INDEX IF NOT EXISTS idx_requests_status ON company_research_requests(status);
CREATE INDEX IF NOT EXISTS idx_requests_created ON company_research_requests(created_at DESC);

-- Enable RLS
ALTER TABLE company_research_requests DISABLE ROW LEVEL SECURITY;
