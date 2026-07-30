-- Add Reckitt Benckiser to brand_phase1_intelligence table
-- Run with: psql -U postgres -h [host] -d [db] -f add_reckitt_brand.sql

INSERT INTO brand_phase1_intelligence (
  brand_name,
  category,
  market_country,
  founded_year,
  headquarters_city,
  headquarters_country,
  positioning_tier,
  target_income_tier,
  price_local,
  price_currency,
  ppp_index,
  market_status,
  distribution_strategy,
  parent_company
)
VALUES
-- Reckitt UK Market
('Reckitt Benckiser', 'Home Care & Hygiene', 'UK', 1819, 'London', 'UK', 'mass_market', 'middle_to_upper', '£2.50', 'GBP', 1.0, 'active', 'omnichannel', 'Reckitt Benckiser'),
('Reckitt Benckiser', 'Home Care & Hygiene', 'USA', 1819, 'New Jersey', 'USA', 'mass_market', 'middle_to_upper', '$3.99', 'USD', 1.0, 'active', 'omnichannel', 'Reckitt Benckiser'),
('Reckitt Benckiser', 'Home Care & Hygiene', 'Germany', 1819, 'London', 'UK', 'mass_market', 'middle_to_upper', '€2.99', 'EUR', 0.95, 'active', 'omnichannel', 'Reckitt Benckiser'),
('Reckitt Benckiser', 'Home Care & Hygiene', 'India', 1819, 'London', 'UK', 'mass_market', 'middle', '₹199', 'INR', 0.25, 'active', 'distribution', 'Reckitt Benckiser');

-- Add Reckitt brands (sub-brands)
INSERT INTO brand_phase1_intelligence (
  brand_name,
  category,
  market_country,
  founded_year,
  headquarters_city,
  headquarters_country,
  positioning_tier,
  target_income_tier,
  price_local,
  price_currency,
  ppp_index,
  market_status,
  distribution_strategy,
  parent_company
)
VALUES
-- Dettol (Reckitt's disinfectant brand)
('Dettol', 'Disinfectant & Hygiene', 'UK', 1933, 'London', 'UK', 'mass_market', 'middle', '£1.99', 'GBP', 1.0, 'active', 'omnichannel', 'Reckitt Benckiser'),
('Dettol', 'Disinfectant & Hygiene', 'USA', 1933, 'New Jersey', 'USA', 'mass_market', 'middle', '$2.99', 'USD', 1.0, 'active', 'omnichannel', 'Reckitt Benckiser'),
('Dettol', 'Disinfectant & Hygiene', 'India', 1933, 'Mumbai', 'India', 'mass_market', 'middle', '₹99', 'INR', 0.25, 'active', 'distribution', 'Reckitt Benckiser'),

-- Lysol (Reckitt's disinfectant spray brand)
('Lysol', 'Disinfectant & Hygiene', 'UK', 1889, 'New Jersey', 'USA', 'mass_market', 'middle', '£3.99', 'GBP', 1.0, 'active', 'retail', 'Reckitt Benckiser'),
('Lysol', 'Disinfectant & Hygiene', 'USA', 1889, 'New Jersey', 'USA', 'mass_market', 'middle', '$4.99', 'USD', 1.0, 'active', 'omnichannel', 'Reckitt Benckiser'),

-- Nurofen (Reckitt's pain relief brand)
('Nurofen', 'Pharmaceuticals & Pain Relief', 'UK', 1974, 'London', 'UK', 'mass_market', 'middle_to_upper', '£5.99', 'GBP', 1.0, 'active', 'pharmacy', 'Reckitt Benckiser'),
('Nurofen', 'Pharmaceuticals & Pain Relief', 'USA', 1974, 'New Jersey', 'USA', 'mass_market', 'middle_to_upper', '$7.99', 'USD', 1.0, 'active', 'omnichannel', 'Reckitt Benckiser'),

-- Air Wick (Reckitt's air freshener brand)
('Air Wick', 'Home Fragrance', 'UK', 1928, 'London', 'UK', 'mass_market', 'middle', '£2.49', 'GBP', 1.0, 'active', 'retail', 'Reckitt Benckiser'),
('Air Wick', 'Home Fragrance', 'USA', 1928, 'New Jersey', 'USA', 'mass_market', 'middle', '$3.49', 'USD', 1.0, 'active', 'retail', 'Reckitt Benckiser');
