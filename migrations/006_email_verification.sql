-- Email verification for school communications (any email provider via IMAP)
-- Tables: email_verification_sessions, school_email_credentials

-- Sessions table: tracks verification code flow
CREATE TABLE IF NOT EXISTS email_verification_sessions (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  device_id TEXT NOT NULL,
  verification_id TEXT NOT NULL UNIQUE,
  email TEXT NOT NULL,
  code TEXT NOT NULL,
  verified_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  expires_at TIMESTAMP WITH TIME ZONE NOT NULL,

  CONSTRAINT valid_email CHECK (email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$')
);

CREATE INDEX IF NOT EXISTS email_verification_sessions_device_id
  ON email_verification_sessions(device_id);
CREATE INDEX IF NOT EXISTS email_verification_sessions_verification_id
  ON email_verification_sessions(verification_id);

-- Credentials table: stores encrypted IMAP credentials for monitoring
CREATE TABLE IF NOT EXISTS school_email_credentials (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  device_id TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  imap_password_encrypted TEXT NOT NULL,
  verified BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  last_polled_at TIMESTAMP WITH TIME ZONE,

  CONSTRAINT valid_email CHECK (email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$')
);

CREATE INDEX IF NOT EXISTS school_email_credentials_device_id
  ON school_email_credentials(device_id);
CREATE INDEX IF NOT EXISTS school_email_credentials_email
  ON school_email_credentials(email);
CREATE INDEX IF NOT EXISTS school_email_credentials_last_polled
  ON school_email_credentials(last_polled_at);
