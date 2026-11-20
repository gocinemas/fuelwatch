-- Each reader's background theme and avatar (private: only the server reads or writes this table).
alter table book_readers add column if not exists theme text not null default 'sunny';
alter table book_readers add column if not exists avatar text not null default '🙂';
