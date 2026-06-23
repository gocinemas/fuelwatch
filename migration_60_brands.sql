-- Add 60 New Brands to Intel Phase 1
-- Categories: Skincare, Beverages, Snacks, QSR

INSERT INTO brand_phase1_intelligence
(brand_name, category, market_country, founded_year, headquarters_city, headquarters_country,
 positioning_tier, target_income_tier, price_local, price_currency, ppp_index,
 market_status, distribution_strategy)
VALUES

-- SKINCARE (20 brands)
('Neutrogena', 'skincare', 'UK', 1930, 'New Jersey', 'USA', 'mass-market', 'lower-middle', 12.99, 'GBP', 1.0, 'mature', 'mass_market'),
('Neutrogena', 'skincare', 'USA', 1930, 'New Jersey', 'USA', 'mass-market', 'lower-middle', 9.99, 'USD', 1.0, 'mature', 'mass_market'),
('Neutrogena', 'skincare', 'India', 1930, 'New Jersey', 'USA', 'mass-market', 'upper-middle', 350, 'INR', 0.25, 'high_growth', 'mass_market'),
('Clinique', 'skincare', 'UK', 1968, 'New York', 'USA', 'premium', 'affluent', 29.50, 'GBP', 1.0, 'mature', 'selective'),
('Clinique', 'skincare', 'USA', 1968, 'New York', 'USA', 'premium', 'affluent', 32.00, 'USD', 1.0, 'mature', 'selective'),
('Clinique', 'skincare', 'India', 1968, 'New York', 'USA', 'premium', 'affluent', 2200, 'INR', 0.25, 'high_growth', 'selective'),
('Estée Lauder', 'skincare', 'UK', 1946, 'New York', 'USA', 'luxury', 'affluent', 65.00, 'GBP', 1.0, 'mature', 'exclusive'),
('Estée Lauder', 'skincare', 'USA', 1946, 'New York', 'USA', 'luxury', 'affluent', 78.00, 'USD', 1.0, 'mature', 'exclusive'),
('Estée Lauder', 'skincare', 'India', 1946, 'New York', 'USA', 'luxury', 'affluent', 5800, 'INR', 0.25, 'high_growth', 'exclusive'),
('Vichy', 'skincare', 'UK', 1931, 'Vichy', 'France', 'mass-prestige', 'upper-middle', 16.99, 'GBP', 1.0, 'mature', 'selective'),
('Vichy', 'skincare', 'USA', 1931, 'Vichy', 'France', 'mass-prestige', 'upper-middle', 19.99, 'USD', 1.0, 'mature', 'selective'),
('Vichy', 'skincare', 'India', 1931, 'Vichy', 'France', 'mass-prestige', 'upper-middle', 1200, 'INR', 0.25, 'high_growth', 'selective'),
('Lotus Herbals', 'skincare', 'India', 2001, 'Mumbai', 'India', 'mass-market', 'lower-middle', 250, 'INR', 0.25, 'high_growth', 'mass_market'),
('Pond''s', 'skincare', 'UK', 1907, 'New York', 'USA', 'mass-market', 'lower-middle', 4.99, 'GBP', 1.0, 'mature', 'mass_market'),
('Pond''s', 'skincare', 'USA', 1907, 'New York', 'USA', 'mass-market', 'lower-middle', 4.99, 'USD', 1.0, 'mature', 'mass_market'),
('Pond''s', 'skincare', 'India', 1907, 'New York', 'USA', 'mass-market', 'lower-middle', 120, 'INR', 0.25, 'high_growth', 'mass_market'),
('Shiseido', 'skincare', 'UK', 1872, 'Tokyo', 'Japan', 'premium', 'affluent', 45.00, 'GBP', 1.0, 'mature', 'selective'),
('Shiseido', 'skincare', 'USA', 1872, 'Tokyo', 'Japan', 'premium', 'affluent', 52.00, 'USD', 1.0, 'mature', 'selective'),
('L''Oréal', 'skincare', 'UK', 1909, 'Paris', 'France', 'mass-prestige', 'upper-middle', 9.99, 'GBP', 1.0, 'mature', 'mass_market'),
('L''Oréal', 'skincare', 'USA', 1909, 'Paris', 'France', 'mass-prestige', 'upper-middle', 10.99, 'USD', 1.0, 'mature', 'mass_market'),

-- BEVERAGES (20 brands)
('Coca-Cola', 'beverages', 'UK', 1886, 'Atlanta', 'USA', 'mass-market', 'lower-middle', 1.50, 'GBP', 1.0, 'mature', 'mass_market'),
('Coca-Cola', 'beverages', 'USA', 1886, 'Atlanta', 'USA', 'mass-market', 'lower-middle', 2.00, 'USD', 1.0, 'mature', 'mass_market'),
('Coca-Cola', 'beverages', 'India', 1886, 'Atlanta', 'USA', 'mass-market', 'lower-middle', 40, 'INR', 0.25, 'high_growth', 'mass_market'),
('Fanta', 'beverages', 'UK', 1940, 'Atlanta', 'USA', 'mass-market', 'lower-middle', 1.20, 'GBP', 1.0, 'mature', 'mass_market'),
('Fanta', 'beverages', 'USA', 1940, 'Atlanta', 'USA', 'mass-market', 'lower-middle', 1.50, 'USD', 1.0, 'mature', 'mass_market'),
('Tropicana', 'beverages', 'UK', 1947, 'Chicago', 'USA', 'mass-market', 'lower-middle', 2.99, 'GBP', 1.0, 'mature', 'mass_market'),
('Tropicana', 'beverages', 'USA', 1947, 'Chicago', 'USA', 'mass-market', 'lower-middle', 3.99, 'USD', 1.0, 'mature', 'mass_market'),
('Gatorade', 'beverages', 'UK', 1965, 'Chicago', 'USA', 'mass-market', 'lower-middle', 2.50, 'GBP', 1.0, 'mature', 'mass_market'),
('Gatorade', 'beverages', 'USA', 1965, 'Chicago', 'USA', 'mass-market', 'lower-middle', 2.49, 'USD', 1.0, 'mature', 'mass_market'),
('Red Bull', 'beverages', 'UK', 1987, 'Salzburg', 'Austria', 'premium', 'upper-middle', 2.50, 'GBP', 1.0, 'high_growth', 'selective'),
('Red Bull', 'beverages', 'USA', 1987, 'Salzburg', 'Austria', 'premium', 'upper-middle', 2.99, 'USD', 1.0, 'high_growth', 'selective'),
('Red Bull', 'beverages', 'India', 1987, 'Salzburg', 'Austria', 'premium', 'upper-middle', 100, 'INR', 0.25, 'high_growth', 'selective'),
('Starbucks', 'beverages', 'UK', 1971, 'Seattle', 'USA', 'premium', 'upper-middle', 4.50, 'GBP', 1.0, 'high_growth', 'selective'),
('Starbucks', 'beverages', 'USA', 1971, 'Seattle', 'USA', 'premium', 'upper-middle', 5.45, 'USD', 1.0, 'high_growth', 'selective'),
('Lipton', 'beverages', 'UK', 1890, 'London', 'UK', 'mass-market', 'lower-middle', 0.99, 'GBP', 1.0, 'mature', 'mass_market'),
('Lipton', 'beverages', 'USA', 1890, 'London', 'UK', 'mass-market', 'lower-middle', 1.49, 'USD', 1.0, 'mature', 'mass_market'),
('Minute Maid', 'beverages', 'USA', 1945, 'Houston', 'USA', 'mass-market', 'lower-middle', 3.99, 'USD', 1.0, 'mature', 'mass_market'),
('Nescafé', 'beverages', 'UK', 1938, 'Vevey', 'Switzerland', 'mass-market', 'lower-middle', 3.50, 'GBP', 1.0, 'mature', 'mass_market'),
('Nescafé', 'beverages', 'India', 1938, 'Vevey', 'Switzerland', 'mass-market', 'lower-middle', 150, 'INR', 0.25, 'high_growth', 'mass_market'),

-- SNACKS (10 brands)
('Lay''s', 'snacks', 'UK', 1932, 'Greenwich', 'UK', 'mass-market', 'lower-middle', 1.50, 'GBP', 1.0, 'mature', 'mass_market'),
('Lay''s', 'snacks', 'USA', 1932, 'Greenwich', 'UK', 'mass-market', 'lower-middle', 1.49, 'USD', 1.0, 'mature', 'mass_market'),
('Lay''s', 'snacks', 'India', 1932, 'Greenwich', 'UK', 'mass-market', 'lower-middle', 50, 'INR', 0.25, 'high_growth', 'mass_market'),
('Doritos', 'snacks', 'UK', 1966, 'Greenwich', 'UK', 'mass-market', 'lower-middle', 1.75, 'GBP', 1.0, 'mature', 'mass_market'),
('Doritos', 'snacks', 'USA', 1966, 'Greenwich', 'UK', 'mass-market', 'lower-middle', 1.49, 'USD', 1.0, 'mature', 'mass_market'),
('Cheetos', 'snacks', 'USA', 1948, 'Greenwich', 'UK', 'mass-market', 'lower-middle', 1.49, 'USD', 1.0, 'mature', 'mass_market'),
('Pringles', 'snacks', 'UK', 1968, 'Jackson', 'USA', 'mass-market', 'lower-middle', 1.50, 'GBP', 1.0, 'mature', 'mass_market'),
('Pringles', 'snacks', 'USA', 1968, 'Jackson', 'USA', 'mass-market', 'lower-middle', 1.29, 'USD', 1.0, 'mature', 'mass_market'),
('Oreo', 'snacks', 'UK', 1912, 'Madison', 'USA', 'mass-market', 'lower-middle', 2.50, 'GBP', 1.0, 'mature', 'mass_market'),
('Oreo', 'snacks', 'USA', 1912, 'Madison', 'USA', 'mass-market', 'lower-middle', 3.49, 'USD', 1.0, 'mature', 'mass_market'),

-- QSR (10 brands)
('McDonald''s', 'qsr', 'UK', 1940, 'Chicago', 'USA', 'mass-market', 'lower-middle', 6.99, 'GBP', 1.0, 'mature', 'mass_market'),
('McDonald''s', 'qsr', 'USA', 1940, 'Chicago', 'USA', 'mass-market', 'lower-middle', 7.49, 'USD', 1.0, 'mature', 'mass_market'),
('McDonald''s', 'qsr', 'India', 1940, 'Chicago', 'USA', 'mass-market', 'lower-middle', 200, 'INR', 0.25, 'high_growth', 'mass_market'),
('KFC', 'qsr', 'UK', 1952, 'Louisville', 'USA', 'mass-market', 'lower-middle', 8.99, 'GBP', 1.0, 'mature', 'mass_market'),
('KFC', 'qsr', 'USA', 1952, 'Louisville', 'USA', 'mass-market', 'lower-middle', 9.99, 'USD', 1.0, 'mature', 'mass_market'),
('KFC', 'qsr', 'India', 1952, 'Louisville', 'USA', 'mass-market', 'lower-middle', 250, 'INR', 0.25, 'high_growth', 'mass_market'),
('Subway', 'qsr', 'UK', 1965, 'Milford', 'USA', 'mass-market', 'lower-middle', 7.50, 'GBP', 1.0, 'mature', 'mass_market'),
('Subway', 'qsr', 'USA', 1965, 'Milford', 'USA', 'mass-market', 'lower-middle', 8.50, 'USD', 1.0, 'mature', 'mass_market'),
('Domino''s', 'qsr', 'UK', 1960, 'Michigan', 'USA', 'mass-market', 'lower-middle', 15.99, 'GBP', 1.0, 'mature', 'mass_market'),
('Domino''s', 'qsr', 'USA', 1960, 'Michigan', 'USA', 'mass-market', 'lower-middle', 17.99, 'USD', 1.0, 'mature', 'mass_market'),
('Domino''s', 'qsr', 'India', 1960, 'Michigan', 'USA', 'mass-market', 'lower-middle', 350, 'INR', 0.25, 'high_growth', 'mass_market'),

-- Note: Duplicates (same brand, different markets) are intentional to show market-specific data
ON CONFLICT DO NOTHING;
