-- Short-lived codes that let a second device join the same reading list. Private: server only.
create table if not exists book_link_codes (
  code        text primary key,
  household   text not null,
  expires_at  timestamptz not null
);
alter table book_link_codes enable row level security;   -- no policies: the public key cannot read it
