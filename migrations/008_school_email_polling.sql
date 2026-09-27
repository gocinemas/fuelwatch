-- Phase 3: School Email Polling via IMAP
-- Stores verified school email credentials for automatic polling

CREATE TABLE IF NOT EXISTS school_email_credentials (
  id BIGSERIAL PRIMARY KEY,
  device_id TEXT NOT NULL,          -- User's phone number
  school_id TEXT NOT NULL,          -- "school-1" or "school-2"
  email TEXT NOT NULL,              -- Email address for school
  imap_password_encrypted TEXT,     -- Encrypted app password or email password
  imap_server TEXT,                 -- Auto-detected IMAP server (gmail, outlook, etc)
  verified BOOLEAN DEFAULT false,   -- Whether email + password tested successfully
  created_at TIMESTAMP DEFAULT NOW(),
  last_polled_at TIMESTAMP,         -- When we last fetched emails

  UNIQUE(device_id, school_id)
);

CREATE INDEX IF NOT EXISTS idx_school_email_device ON school_email_credentials(device_id);
CREATE INDEX IF NOT EXISTS idx_school_email_verified ON school_email_credentials(verified);

-- Table for storing extracted school emails (events, alerts, dates)
CREATE TABLE IF NOT EXISTS school_email_messages (
  id BIGSERIAL PRIMARY KEY,
  device_id TEXT NOT NULL,
  school_id TEXT NOT NULL,
  from_address TEXT,                -- Sender email
  subject TEXT,                     -- Email subject
  sent_at TIMESTAMP,                -- When email was sent
  alert_type TEXT,                  -- "urgent", "event", "payment", "newsletter"
  dates_mentioned TEXT[],           -- Extracted dates from email
  full_text TEXT,                   -- Full email body for search
  created_at TIMESTAMP DEFAULT NOW(),

  CONSTRAINT fk_credentials
    FOREIGN KEY(device_id, school_id)
    REFERENCES school_email_credentials(device_id, school_id)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_school_msg_device ON school_email_messages(device_id);
CREATE INDEX IF NOT EXISTS idx_school_msg_type ON school_email_messages(alert_type);
CREATE INDEX IF NOT EXISTS idx_school_msg_date ON school_email_messages(sent_at DESC);
