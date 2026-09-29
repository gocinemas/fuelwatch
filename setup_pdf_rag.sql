-- Create table for PDF chunks and embeddings
CREATE TABLE IF NOT EXISTS pdf_chunks (
    id BIGSERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    chunk_index INT,
    content TEXT,
    embedding VECTOR(384),  -- for all-MiniLM-L6-v2 model
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),

    -- For faster search
    UNIQUE(filename, chunk_index)
);

-- Create index for full-text search on content
CREATE INDEX IF NOT EXISTS idx_pdf_chunks_content ON pdf_chunks USING GIN (to_tsvector('english', content));

-- Create index for metadata queries
CREATE INDEX IF NOT EXISTS idx_pdf_chunks_metadata ON pdf_chunks USING GIN (metadata);

-- If using pgvector extension for vector search (requires: CREATE EXTENSION IF NOT EXISTS vector)
-- CREATE INDEX IF NOT EXISTS idx_pdf_chunks_embedding ON pdf_chunks USING ivfflat (embedding vector_cosine_ops);
