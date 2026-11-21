-- Due-back date for library books (private: server only)
alter table book_list add column if not exists due date;
