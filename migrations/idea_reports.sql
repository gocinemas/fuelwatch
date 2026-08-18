-- FrameWork: App Idea Validator Reports
-- Stealth build at /idea — save all analyses for learning loop

CREATE TABLE IF NOT EXISTS idea_reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  url TEXT NOT NULL,
  app_name TEXT,

  -- Extracted data
  title TEXT,
  value_prop TEXT,
  features TEXT,  -- JSON array of features
  positioning TEXT,

  -- Assessment scores
  idea_score INT,
  potential_score INT,
  design_score INT,
  overall_score INT,

  -- Verdict
  worth_pursuing BOOLEAN,
  confidence INT,  -- 0-100
  verdict_reason TEXT,

  -- Improvements and pivots (JSON for flexibility)
  improvements TEXT,  -- JSON array
  pivots TEXT,        -- JSON array

  -- Feedback loop: user feedback on accuracy
  user_rating INT,              -- 1-5 stars (if they rate it)
  user_feedback TEXT,
  actual_outcome TEXT,          -- Did they build it? How did it go?

  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),

  -- Analytics
  viewed_count INT DEFAULT 0,
  shared_on TEXT  -- 'twitter', 'linkedin', 'copied', etc
);

CREATE INDEX IF NOT EXISTS idx_idea_reports_score ON idea_reports(overall_score DESC);
CREATE INDEX IF NOT EXISTS idx_idea_reports_created ON idea_reports(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_idea_reports_url ON idea_reports(url);

-- Track which improvements/pivots actually mattered (for ML training)
CREATE TABLE IF NOT EXISTS idea_feedback (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  report_id UUID NOT NULL REFERENCES idea_reports(id) ON DELETE CASCADE,

  feedback_type TEXT,  -- 'improvement_implemented', 'pivot_tried', 'accuracy_feedback'
  text TEXT,
  helpful BOOLEAN,     -- Did this feedback help?

  created_at TIMESTAMPTZ DEFAULT NOW(),

  FOREIGN KEY (report_id) REFERENCES idea_reports(id)
);

CREATE INDEX IF NOT EXISTS idx_idea_feedback_report ON idea_feedback(report_id);
