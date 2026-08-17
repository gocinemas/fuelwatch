-- Phase 3: Motivation Layer Foundation (Phase 1 implementation)
-- Aug 2026: Savings events ledger + per-user feature flags

-- Canonical ledger: every win produces a savings_events row
CREATE TABLE IF NOT EXISTS savings_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  wa TEXT NOT NULL,                     -- whatsapp number
  event_type TEXT NOT NULL,             -- 'fuel_drop' | 'underspend_week' | 'cheaper_receipt' | 'sustainability_action'
  amount_pence INTEGER,                 -- money value where applicable
  co2_grams INTEGER,                    -- sustainability value where applicable
  minutes_saved INTEGER,                -- time value where applicable
  description TEXT,                     -- human-readable, used directly in WA copy
  source_id TEXT,                       -- fk-ish pointer (fuel_alerts.id, wa_saves.id, etc.)
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_savings_events_wa ON savings_events(wa, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_savings_events_type ON savings_events(wa, event_type);
CREATE INDEX IF NOT EXISTS idx_savings_events_created ON savings_events(created_at DESC);

-- Per-user feature preferences + internal tester flag
CREATE TABLE IF NOT EXISTS motivation_prefs (
  wa TEXT PRIMARY KEY,
  price_alerts_enabled BOOLEAN DEFAULT FALSE,
  weekly_summary_enabled BOOLEAN DEFAULT FALSE,
  sustainability_enabled BOOLEAN DEFAULT FALSE,
  family_goals_enabled BOOLEAN DEFAULT FALSE,
  social_proof_enabled BOOLEAN DEFAULT FALSE,
  time_saved_enabled BOOLEAN DEFAULT FALSE,
  is_internal_tester BOOLEAN DEFAULT FALSE,  -- gate: internal cohort sees features before general rollout
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Track price alerts' cumulative savings (motivational metric)
ALTER TABLE fuel_alerts
ADD COLUMN IF NOT EXISTS total_saved_pence INTEGER DEFAULT 0;

-- Weekly savings summary cache (for brief optimization)
CREATE TABLE IF NOT EXISTS weekly_savings_cache (
  wa TEXT PRIMARY KEY,
  total_spent_pence INTEGER,
  last_week_pence INTEGER,
  fuel_saved_pence INTEGER,
  receipt_count INTEGER,
  computed_at TIMESTAMPTZ DEFAULT NOW(),
  period_end_date DATE
);

CREATE INDEX IF NOT EXISTS idx_weekly_savings_cache_wa ON weekly_savings_cache(wa);
