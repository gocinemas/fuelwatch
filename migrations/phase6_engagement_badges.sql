-- Phase 6: Engagement loops — streaks, badges, social proof (Aug 2026)
-- Builds on the Phase 3 motivation layer (savings_events, motivation_prefs) and
-- Phase 4 goals layer (family_goals, goal_progress) rather than duplicating them.
-- Two new tables only: a durable weekly history (for the streak counter, since
-- weekly_savings_cache/savings_events don't hold one row per week per user) and
-- unlocked achievement badges.

-- One row per user per ISO week. Written by the Sunday 18:00 digest cron
-- (miru/motivation/endpoints.py::cron_weekly_savings). Powers the streak
-- counter and the "days on budget" stat without re-deriving history each time.
CREATE TABLE IF NOT EXISTS weekly_win_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  wa TEXT NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  spend_pence INTEGER DEFAULT 0,
  savings_amount_pence INTEGER DEFAULT 0,   -- vs previous week; positive = spent less
  on_budget BOOLEAN DEFAULT FALSE,          -- spent <= last week (or under weekly_target_pence if set)
  days_on_budget INTEGER DEFAULT 0,         -- 0-7, days this week under the daily target
  streak_count INTEGER DEFAULT 0,           -- consecutive on_budget weeks, as of this row
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (wa, period_start)
);

CREATE INDEX IF NOT EXISTS idx_weekly_win_events_wa ON weekly_win_events(wa, period_start DESC);

-- Unlocked badges. Reconciled on read (GET /api/v2/engagement-summary), not
-- on a separate cron, so a badge appears the moment its criteria is met.
CREATE TABLE IF NOT EXISTS user_achievements (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  wa TEXT NOT NULL,
  achievement_type TEXT NOT NULL,   -- 'first_week' | 'budget_master' | 'goal_getter' | 'big_saver' | 'fuel_saver'
  unlocked_date DATE NOT NULL DEFAULT CURRENT_DATE,
  meta JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (wa, achievement_type)
);

CREATE INDEX IF NOT EXISTS idx_user_achievements_wa ON user_achievements(wa);
