-- Phase 4: Family Goals (Phase 2 - Goals Only)
-- Aug 2026: Multi-user household goal tracking

-- Link multiple WhatsApp numbers under one household
CREATE TABLE IF NOT EXISTS household_members (
  household_id TEXT NOT NULL,
  wa TEXT NOT NULL,
  display_name TEXT,        -- "Riaan's mum", set by user
  role TEXT DEFAULT 'member',  -- 'admin' | 'member'
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (household_id, wa)
);

CREATE INDEX IF NOT EXISTS idx_household_members_wa ON household_members(wa);

-- Family goals: targets set by households
CREATE TABLE IF NOT EXISTS family_goals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  household_id TEXT NOT NULL,
  goal_type TEXT NOT NULL,             -- 'spend_reduction' | 'sustainability' | 'custom'
  title TEXT NOT NULL,                 -- "Cut takeaway spend to £100/mo"
  target_value NUMERIC NOT NULL,
  target_unit TEXT NOT NULL,           -- 'gbp' | 'co2_kg' | 'count'
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  current_value NUMERIC DEFAULT 0,
  status TEXT DEFAULT 'active',        -- 'active' | 'achieved' | 'missed' | 'archived'
  created_by_wa TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  FOREIGN KEY (household_id) REFERENCES household_members(household_id)
);

CREATE INDEX IF NOT EXISTS idx_family_goals_household ON family_goals(household_id, status);
CREATE INDEX IF NOT EXISTS idx_family_goals_period ON family_goals(period_start, period_end);

-- Track goal progress updates
CREATE TABLE IF NOT EXISTS goal_progress (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  goal_id UUID NOT NULL REFERENCES family_goals(id),
  value NUMERIC NOT NULL,
  source TEXT,              -- 'spend_log' | 'manual' | 'cron'
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  FOREIGN KEY (goal_id) REFERENCES family_goals(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_goal_progress_goal ON goal_progress(goal_id, updated_at DESC);
