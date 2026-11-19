-- Miru code-based sign-in tables. Run once in the Supabase SQL Editor.
create table if not exists auth_codes (
  id uuid primary key default gen_random_uuid(),
  phone text not null,
  code_hash text not null,
  attempts int not null default 0,
  used boolean not null default false,
  expires_at timestamptz not null,
  created_at timestamptz not null default now()
);
create index if not exists auth_codes_phone_idx on auth_codes (phone, created_at desc);

create table if not exists auth_sessions (
  token_hash text primary key,
  phone text not null,
  expires_at timestamptz not null,
  created_at timestamptz not null default now()
);

-- Only the server (service key) should touch these tables.
alter table auth_codes enable row level security;
alter table auth_sessions enable row level security;
