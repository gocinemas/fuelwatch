-- Migration: Create ECA clubs table
-- Stores extracted club schedules from WhatsApp/email

CREATE TABLE IF NOT EXISTS school_eca_clubs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    school_id UUID NOT NULL,
    from_number TEXT NOT NULL,
    club_name TEXT NOT NULL,
    day_of_week TEXT NOT NULL,  -- Monday, Tuesday, Wednesday, Thursday, Friday
    start_time TEXT NOT NULL,   -- HH:MM (24-hour format)
    end_time TEXT NOT NULL,     -- HH:MM (24-hour format)
    year_group TEXT NOT NULL DEFAULT 'All Years',
    location TEXT,
    source TEXT DEFAULT 'whatsapp',  -- 'whatsapp' or 'email'
    source_message_id UUID,
    source_message_text TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(from_number, school_id, club_name, day_of_week, start_time)
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_school_eca_clubs_from_number ON school_eca_clubs(from_number);
CREATE INDEX IF NOT EXISTS idx_school_eca_clubs_school_id ON school_eca_clubs(school_id);
CREATE INDEX IF NOT EXISTS idx_school_eca_clubs_day ON school_eca_clubs(day_of_week);

-- Audit log for ECA extraction
CREATE TABLE IF NOT EXISTS school_eca_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_number TEXT NOT NULL,
    operation TEXT NOT NULL,  -- 'club_extracted', 'club_duplicate', 'extraction_error'
    clubs_count INT DEFAULT 0,
    source_message_id UUID,
    error_details TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_school_eca_audit_from_number ON school_eca_audit(from_number);
