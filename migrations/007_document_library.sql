-- Document Library for RAG (Retrieval-Augmented Generation)
-- Stores indexed documents with full-text search

CREATE TABLE IF NOT EXISTS documents (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  filename TEXT NOT NULL,
  folder_path TEXT NOT NULL,
  file_type TEXT NOT NULL,  -- pdf, docx, md, txt, etc.
  title TEXT,
  excerpt TEXT,  -- First 200 chars of content
  full_text TEXT,  -- Full searchable content
  file_size_bytes BIGINT,
  indexed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

  -- Search indexes
  ts_vector tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(excerpt, '') || ' ' || coalesce(full_text, ''))) STORED
);

-- Full-text search index
CREATE INDEX IF NOT EXISTS documents_ts_idx ON documents USING gin(ts_vector);

-- Other useful indexes
CREATE INDEX IF NOT EXISTS documents_folder_path ON documents(folder_path);
CREATE INDEX IF NOT EXISTS documents_file_type ON documents(file_type);
CREATE INDEX IF NOT EXISTS documents_indexed_at ON documents(indexed_at DESC);

-- Metadata table for index stats
CREATE TABLE IF NOT EXISTS document_index_status (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  folder_path TEXT NOT NULL UNIQUE,
  doc_count INT DEFAULT 0,
  last_indexed_at TIMESTAMP WITH TIME ZONE,
  status TEXT DEFAULT 'pending'  -- pending, indexing, complete, error
);
