-- Add commute preferences columns to user_commutes
-- Run in Supabase SQL Editor

ALTER TABLE user_commutes
ADD COLUMN IF NOT EXISTS show_on_homepage BOOLEAN DEFAULT true;

ALTER TABLE user_commutes
ADD COLUMN IF NOT EXISTS time_start TEXT DEFAULT '07:00';

ALTER TABLE user_commutes
ADD COLUMN IF NOT EXISTS time_end TEXT DEFAULT '09:00';

ALTER TABLE user_commutes
ADD COLUMN IF NOT EXISTS days_of_week JSONB DEFAULT '["Mon","Tue","Wed","Thu","Fri"]'::jsonb;

-- Verify the changes
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'user_commutes'
ORDER BY ordinal_position;
