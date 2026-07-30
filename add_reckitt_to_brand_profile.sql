-- Add Reckitt Benckiser to brand_profile table (company search)
-- This allows the company intelligence search to find Reckitt

INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES
('Reckitt Benckiser', 1819, 'London', 'UK', 'Iflex Positive Impact', 'Hygiene and home care: Lysol, Dettol, Air Wick, Nurofen, Strepsils', 'reckittbenckiser.com', 'London, UK')
ON CONFLICT (name) DO NOTHING;

-- Also add search aliases for common variations
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES
('Reckitt', 1819, 'London', 'UK', 'Iflex Positive Impact', 'Hygiene and home care: Lysol, Dettol, Air Wick, Nurofen, Strepsils (see Reckitt Benckiser)', 'reckittbenckiser.com', 'London, UK')
ON CONFLICT (name) DO NOTHING;
