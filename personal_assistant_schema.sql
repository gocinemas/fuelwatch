-- Personal WhatsApp Assistant Schema
-- Run this via Supabase SQL editor to set up tables

-- Raw inbound/outbound message log (durability first — write before any AI call)
create table if not exists messages (
  id              uuid primary key default gen_random_uuid(),
  direction       text not null check (direction in ('inbound','outbound')),
  from_number     text not null,          -- 'whatsapp:+44...' Twilio format
  to_number       text not null,
  wa_message_sid  text unique,            -- Twilio MessageSid — idempotency key
  body            text,
  media_urls      jsonb default '[]',
  num_media       int default 0,
  status          text not null default 'received'
                    check (status in ('received','processing','processed','failed')),
  created_at      timestamptz not null default now()
);

create index if not exists idx_messages_created_at on messages (created_at desc);
create index if not exists idx_messages_status on messages (status);
create index if not exists idx_messages_wa_sid on messages (wa_message_sid);

-- Claude's structured extraction per inbound message
create table if not exists message_processing (
  id              uuid primary key default gen_random_uuid(),
  message_id      uuid not null references messages(id) on delete cascade,
  summary         text,
  category        text check (category in ('todo','question','note','request','fyi')),
  sentiment       text,
  needs_response  boolean default false,
  claude_model    text,
  raw_response    jsonb,                  -- full structured output, for audit/debugging
  processed_at    timestamptz not null default now()
);

create index if not exists idx_processing_message_id on message_processing (message_id);

-- Extracted action items
create table if not exists todos (
  id                uuid primary key default gen_random_uuid(),
  source_message_id uuid references messages(id) on delete set null,
  text              text not null,
  due_date          date,
  priority          text default 'medium' check (priority in ('low','medium','high')),
  status            text not null default 'open' check (status in ('open','done','snoozed','cancelled')),
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now(),
  completed_at      timestamptz
);

create index if not exists idx_todos_status on todos (status);
create index if not exists idx_todos_due_date on todos (due_date);
create index if not exists idx_todos_source_message on todos (source_message_id);

-- What the bot sent back, for audit/tuning
create table if not exists auto_responses (
  id                uuid primary key default gen_random_uuid(),
  message_id        uuid not null references messages(id) on delete cascade,
  response_text     text not null,
  response_type     text check (response_type in ('acknowledgement','answer','clarification')),
  outbound_wa_sid   text,
  sent_at           timestamptz not null default now()
);

create index if not exists idx_auto_responses_message_id on auto_responses (message_id);

-- Rollups for daily/weekly digests
create table if not exists summaries (
  id            uuid primary key default gen_random_uuid(),
  period_type   text not null check (period_type in ('daily','weekly')),
  period_start  date not null,
  period_end    date not null,
  summary_text  text not null,
  message_count int default 0,
  todo_count    int default 0,
  created_at    timestamptz not null default now()
);

create index if not exists idx_summaries_period on summaries (period_type, period_start);

-- Single-user preferences (timezone, digest on/off, etc.)
create table if not exists personal_bot_settings (
  key    text primary key,
  value  jsonb not null
);

-- Enable Row Level Security on all tables (no public access)
alter table messages enable row level security;
alter table message_processing enable row level security;
alter table todos enable row level security;
alter table auto_responses enable row level security;
alter table summaries enable row level security;
alter table personal_bot_settings enable row level security;

-- RLS policies: server-side only (no policies needed, app uses service role key)
