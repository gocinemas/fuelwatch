-- Phase 5: Proactive spend + fuel-spike alerts (Aug 2026)
-- Extends the Phase 3 motivation layer so budgets can be enforced, not just reported.

-- Daily spend budget, alongside the existing weekly_target_pence.
-- Also gates all three proactive alert types (daily overspend, weekly pace, fuel spike)
-- behind one opt-in flag, and tracks last-sent dates so a re-run of the cron
-- within the same period never double-sends.
ALTER TABLE motivation_prefs
ADD COLUMN IF NOT EXISTS budget_alerts_enabled BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS daily_target_pence INTEGER,
ADD COLUMN IF NOT EXISTS last_daily_alert_date DATE,
ADD COLUMN IF NOT EXISTS last_weekly_pace_alert_date DATE;

-- Fuel price-spike tracking (separate from the existing drop-alert last_price,
-- which the /api/fuel/check-drops cron overwrites on every run).
ALTER TABLE fuel_alerts
ADD COLUMN IF NOT EXISTS spike_last_price NUMERIC,
ADD COLUMN IF NOT EXISTS spike_last_price_date DATE,
ADD COLUMN IF NOT EXISTS spike_baseline_price NUMERIC,
ADD COLUMN IF NOT EXISTS spike_baseline_date DATE,
ADD COLUMN IF NOT EXISTS spike_last_alerted_date DATE;
