-- Factory page visitor stats (see factory_stats.py). Run once in the Supabase SQL Editor.
-- Nothing personal: visitor is a random id made by the browser.
create table if not exists factory_events (
  id bigserial primary key,
  at timestamptz not null default now(),
  visitor text not null,
  event text not null,
  detail text
);
create index if not exists factory_events_at_idx on factory_events (at);
create index if not exists factory_events_visitor_idx on factory_events (visitor);

-- Only the server (service key) touches this table.
alter table factory_events enable row level security;
