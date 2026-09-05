-- Phase 1: School Comms V2 Database Schema

-- WhatsApp Groups (track which groups Miru monitors)
CREATE TABLE IF NOT EXISTS school_wa_groups (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  from_number TEXT NOT NULL,
  group_id TEXT NOT NULL,
  group_name TEXT,
  school_id UUID REFERENCES school_profiles(id),
  connected_at TIMESTAMP DEFAULT now(),
  last_message_at TIMESTAMP,
  status TEXT DEFAULT 'active', -- active, paused, error
  error_message TEXT,
  UNIQUE(from_number, group_id)
);

-- WhatsApp Messages (store all messages from monitored groups)
CREATE TABLE IF NOT EXISTS school_wa_messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  from_number TEXT NOT NULL,
  group_id TEXT NOT NULL REFERENCES school_wa_groups(group_id),
  sender_name TEXT,
  message_text TEXT,
  message_type TEXT DEFAULT 'text', -- text, image, document, voice
  received_at TIMESTAMP DEFAULT now(),
  
  -- NLP Classification
  category TEXT, -- event, action-needed, fyi, announcement, permission-slip
  confidence FLOAT,
  extracted_date DATE,
  extracted_action TEXT,
  
  -- Dedup
  wa_message_id TEXT UNIQUE,
  matched_email_event_id UUID REFERENCES school_events(id),
  
  created_at TIMESTAMP DEFAULT now()
);

-- Audit Log (track all syncs + errors)
CREATE TABLE IF NOT EXISTS school_audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  from_number TEXT NOT NULL,
  operation TEXT NOT NULL, -- email_polled, message_received, event_extracted, alert_sent, error
  details JSONB,
  status TEXT DEFAULT 'success', -- success, error, skipped
  error_message TEXT,
  created_at TIMESTAMP DEFAULT now()
);

-- Dedup Log (track which email events matched which WA messages)
CREATE TABLE IF NOT EXISTS school_dedup_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email_event_id UUID REFERENCES school_events(id),
  wa_message_id UUID REFERENCES school_wa_messages(id),
  match_reason TEXT, -- date_match, title_match, manual
  confidence FLOAT,
  created_at TIMESTAMP DEFAULT now()
);

-- WhatsApp Tokens (store encrypted refresh tokens)
CREATE TABLE IF NOT EXISTS school_wa_tokens (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  from_number TEXT NOT NULL UNIQUE,
  access_token TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  token_type TEXT DEFAULT 'Bearer',
  expires_at TIMESTAMP,
  connected_at TIMESTAMP DEFAULT now(),
  last_used TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_school_wa_groups_from_number ON school_wa_groups(from_number);
CREATE INDEX IF NOT EXISTS idx_school_wa_messages_received ON school_wa_messages(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_school_wa_messages_group ON school_wa_messages(group_id);
CREATE INDEX IF NOT EXISTS idx_school_audit_log_from ON school_audit_log(from_number, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_school_dedup_email ON school_dedup_log(email_event_id);
