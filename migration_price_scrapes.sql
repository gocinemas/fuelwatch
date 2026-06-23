-- Table to store scraped e-commerce prices

CREATE TABLE IF NOT EXISTS price_scrapes (
    id BIGSERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    brand VARCHAR(100) NOT NULL,
    title TEXT,
    price_inr DECIMAL(10, 2),
    price_usd DECIMAL(10, 2),
    url TEXT,
    availability VARCHAR(50),
    rating DECIMAL(2, 1),
    review_count INTEGER,
    timestamp TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(platform, brand, timestamp::DATE)
);

-- Index for fast lookups
CREATE INDEX idx_price_scrapes_brand ON price_scrapes(brand);
CREATE INDEX idx_price_scrapes_platform ON price_scrapes(platform);
CREATE INDEX idx_price_scrapes_timestamp ON price_scrapes(timestamp);

-- View: Latest prices per brand
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (brand, platform)
    brand,
    platform,
    price_inr,
    url,
    timestamp
FROM price_scrapes
ORDER BY brand, platform, timestamp DESC;
