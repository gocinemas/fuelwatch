-- Brand Data Request Queue
-- Tracks user requests for new brands to be researched

CREATE TABLE IF NOT EXISTS brand_data_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_name TEXT NOT NULL,
    category TEXT NOT NULL,
    email TEXT,
    status TEXT DEFAULT 'pending', -- pending, collected, failed
    research_notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    UNIQUE(brand_name, category)
);

CREATE INDEX idx_brand_requests_status ON brand_data_requests(status);
CREATE INDEX idx_brand_requests_created ON brand_data_requests(created_at);
