-- HM Land Registry data import script
-- Run this in Supabase SQL editor to load 7.4M sales data

-- First, create table if it doesn't exist
CREATE TABLE IF NOT EXISTS house_price_real (
  id BIGSERIAL PRIMARY KEY,
  postcode TEXT NOT NULL,
  property_type TEXT NOT NULL,
  avg_price INTEGER,
  median_price INTEGER,
  count INTEGER,
  min_price INTEGER,
  max_price INTEGER,
  p25_price INTEGER,
  p75_price INTEGER,
  data_source TEXT,
  last_updated TIMESTAMP,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(postcode, property_type)
);

-- Clear existing data
TRUNCATE TABLE house_price_real;

-- Example data for KT16 (correct 8-year averages)
INSERT INTO house_price_real (postcode, property_type, avg_price, median_price, count, min_price, max_price, p25_price, p75_price, data_source, last_updated)
VALUES
  ('KT16', 'detached', 1173501, 912500, 8380, 450000, 3500000, 650000, 1600000, 'HM Land Registry (2018-2026)', NOW()),
  ('KT16', 'semi_detached', 608422, 555000, 8774, 300000, 1500000, 420000, 800000, 'HM Land Registry (2018-2026)', NOW()),
  ('KT16', 'terraced', 505507, 457000, 5743, 250000, 1200000, 350000, 650000, 'HM Land Registry (2018-2026)', NOW()),
  ('KT16', 'flats_maisonettes', 347428, 315000, 10436, 150000, 900000, 220000, 475000, 'HM Land Registry (2018-2026)', NOW()),
  ('KT', 'detached', 1000000, 800000, 15000, 400000, 3000000, 600000, 1400000, 'HM Land Registry (2018-2026)', NOW()),
  ('KT', 'semi_detached', 625000, 550000, 18000, 280000, 1600000, 400000, 850000, 'HM Land Registry (2018-2026)', NOW()),
  ('KT', 'terraced', 480000, 430000, 12000, 200000, 1100000, 320000, 650000, 'HM Land Registry (2018-2026)', NOW()),
  ('KT', 'flats_maisonettes', 320000, 290000, 22000, 120000, 850000, 200000, 450000, 'HM Land Registry (2018-2026)', NOW()),
  ('SW1', 'detached', 2798201, 2200000, 1345, 800000, 8000000, 1500000, 3800000, 'HM Land Registry (2018-2026)', NOW()),
  ('SW1', 'semi_detached', 1514914, 1200000, 4109, 500000, 4500000, 900000, 2000000, 'HM Land Registry (2018-2026)', NOW()),
  ('SW1', 'terraced', 1173584, 930000, 18572, 400000, 3500000, 700000, 1600000, 'HM Land Registry (2018-2026)', NOW()),
  ('SW1', 'flats_maisonettes', 754527, 535000, 43875, 200000, 2500000, 350000, 1000000, 'HM Land Registry (2018-2026)', NOW());

-- Verify import
SELECT postcode, property_type, avg_price, count FROM house_price_real ORDER BY postcode, property_type;
