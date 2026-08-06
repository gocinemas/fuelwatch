-- Research Findings Table
-- Store auto-gathered data + admin edits for hybrid AI/human workflow

CREATE TABLE IF NOT EXISTS company_research_findings (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  request_id INT,
  company_name TEXT NOT NULL,

  -- Auto-gathered data (from research agent)
  auto_description TEXT,
  auto_market_position TEXT,
  auto_risks TEXT[], -- JSON array
  auto_opportunities TEXT[], -- JSON array
  auto_brands TEXT[], -- JSON array
  auto_financials JSONB, -- {revenue, margin, employees, etc}

  -- Admin edits/additions
  admin_description TEXT,
  admin_market_position TEXT,
  admin_risks TEXT[],
  admin_opportunities TEXT[],
  admin_brands TEXT[],
  admin_financials JSONB,

  -- Status
  agent_status TEXT DEFAULT 'pending', -- pending, researching, completed
  admin_verified BOOLEAN DEFAULT FALSE,
  admin_notes TEXT,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_findings_company ON company_research_findings(company_name);
CREATE INDEX IF NOT EXISTS idx_findings_request ON company_research_findings(request_id);

ALTER TABLE company_research_findings DISABLE ROW LEVEL SECURITY;
