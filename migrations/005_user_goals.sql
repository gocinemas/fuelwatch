-- Migration: Create user_goals table for spending goals

CREATE TABLE IF NOT EXISTS user_goals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_number TEXT NOT NULL,
    title TEXT NOT NULL,
    target_value NUMERIC(10,2) NOT NULL,  -- in pence (e.g., 10000 = £100)
    period_days INT DEFAULT 7,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(from_number, title)
);

CREATE INDEX IF NOT EXISTS idx_user_goals_from_number ON user_goals(from_number);

-- Table to track goal progress (daily snapshots)
CREATE TABLE IF NOT EXISTS user_goal_progress (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    goal_id UUID NOT NULL REFERENCES user_goals(id) ON DELETE CASCADE,
    from_number TEXT NOT NULL,
    spent_pence INT DEFAULT 0,  -- Total spent in period
    progress_percent INT DEFAULT 0,  -- 0-100
    is_complete BOOLEAN DEFAULT FALSE,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_goal_progress_goal_id ON user_goal_progress(goal_id);
CREATE INDEX IF NOT EXISTS idx_user_goal_progress_from_number ON user_goal_progress(from_number);
