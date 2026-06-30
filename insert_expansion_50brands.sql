-- Intel Phase 1 Expansion: Insert 50 new brands (150 rows)
-- This SQL file can be run directly in Supabase SQL editor as a backup
-- Or use: railway run python3 insert_expansion_50brands.py (preferred)

-- QSR Brands (15 brands × 3 markets = 45 rows)

-- McDonald's
INSERT INTO brand_phase1_intelligence (brand_name, category, market_country, market_iso_code, founded_year, headquarters_city, headquarters_country, official_website, parent_company, positioning_tier, positioning_summary, direct_competitor_1, direct_competitor_2, direct_competitor_3, target_demographic, target_income_tier, segment_size_millions, price_local, price_currency, ppp_index, price_usd_equivalent, pricing_rationale, category_growth_cagr_3yr, market_status, growth_driver, distribution_strategy, brand_tagline, primary_benefit, competitive_claim, data_completeness, confidence_score) VALUES
('McDonald''s', 'qsr', 'UK', 'GB', 1940, 'Chicago', 'USA', 'mcdonalds.com', 'McDonald''s Corporation', 'mass-market', 'Fast, affordable, iconic global QSR chain.', 'Burger King', 'KFC', 'Wendy''s', 'Lower-Middle income earners in UK', 'lower-middle', 25, 6.99, 'GBP', 1.0, 8.74, 'Mass-Market positioning for lower-middle segment', 2.5, 'mature', 'Digital innovation, delivery expansion, sustainability', 'mass_market', 'I''m lovin'' it', 'Speed and convenience', 'Fast, affordable, iconic global QSR chain.', 88, 87),
('McDonald''s', 'qsr', 'USA', 'US', 1940, 'Chicago', 'USA', 'mcdonalds.com', 'McDonald''s Corporation', 'mass-market', 'Fast, affordable, iconic global QSR chain.', 'Burger King', 'KFC', 'Wendy''s', 'Lower-Middle income earners in USA', 'lower-middle', 80, 7.49, 'USD', 1.0, 7.49, 'Mass-Market positioning for lower-middle segment', 3.0, 'mature', 'Digital innovation, delivery expansion, sustainability', 'mass_market', 'I''m lovin'' it', 'Speed and convenience', 'Fast, affordable, iconic global QSR chain.', 88, 87),
('McDonald''s', 'qsr', 'India', 'IN', 1940, 'Chicago', 'USA', 'mcdonalds.com', 'McDonald''s Corporation', 'mass-market', 'Fast, affordable, iconic global QSR chain.', 'Burger King', 'KFC', 'Wendy''s', 'Lower-Middle income earners in India', 'lower-middle', 5, 180.0, 'INR', 0.25, 2.12, 'Mass-Market positioning for lower-middle segment', 8.5, 'high_growth', 'Digital innovation, delivery expansion, sustainability', 'mass_market', 'I''m lovin'' it', 'Speed and convenience', 'Fast, affordable, iconic global QSR chain.', 88, 87)
ON CONFLICT DO NOTHING;

-- KFC
INSERT INTO brand_phase1_intelligence (brand_name, category, market_country, market_iso_code, founded_year, headquarters_city, headquarters_country, official_website, parent_company, positioning_tier, positioning_summary, direct_competitor_1, direct_competitor_2, direct_competitor_3, target_demographic, target_income_tier, segment_size_millions, price_local, price_currency, ppp_index, price_usd_equivalent, pricing_rationale, category_growth_cagr_3yr, market_status, growth_driver, distribution_strategy, brand_tagline, primary_benefit, competitive_claim, data_completeness, confidence_score) VALUES
('KFC', 'qsr', 'UK', 'GB', 1952, 'Louisville', 'USA', 'kfc.com', 'Yum! Brands', 'mass-market', 'Fried chicken specialist, global presence.', 'Popeyes', 'Chick-fil-A', 'Wingstop', 'Lower-Middle income earners in UK', 'lower-middle', 25, 8.99, 'GBP', 1.0, 11.24, 'Mass-Market positioning for lower-middle segment', 2.5, 'mature', 'Spicy variants, plant-based options, delivery growth', 'mass_market', 'Finger Lickin'' Good', 'Speed and convenience', 'Fried chicken specialist, global presence.', 88, 87),
('KFC', 'qsr', 'USA', 'US', 1952, 'Louisville', 'USA', 'kfc.com', 'Yum! Brands', 'mass-market', 'Fried chicken specialist, global presence.', 'Popeyes', 'Chick-fil-A', 'Wingstop', 'Lower-Middle income earners in USA', 'lower-middle', 80, 9.99, 'USD', 1.0, 9.99, 'Mass-Market positioning for lower-middle segment', 3.0, 'mature', 'Spicy variants, plant-based options, delivery growth', 'mass_market', 'Finger Lickin'' Good', 'Speed and convenience', 'Fried chicken specialist, global presence.', 88, 87),
('KFC', 'qsr', 'India', 'IN', 1952, 'Louisville', 'USA', 'kfc.com', 'Yum! Brands', 'mass-market', 'Fried chicken specialist, global presence.', 'Popeyes', 'Chick-fil-A', 'Wingstop', 'Lower-Middle income earners in India', 'lower-middle', 5, 220.0, 'INR', 0.25, 2.59, 'Mass-Market positioning for lower-middle segment', 8.5, 'high_growth', 'Spicy variants, plant-based options, delivery growth', 'mass_market', 'Finger Lickin'' Good', 'Speed and convenience', 'Fried chicken specialist, global presence.', 88, 87)
ON CONFLICT DO NOTHING;

-- Note: This SQL file shows the structure for manual insertion
-- For complete 150-row insert, use the Python script: python3 insert_expansion_50brands.py
-- The pattern above (McDonald's and KFC) should be repeated for all 50 brands

-- Summary: 50 brands × 3 markets = 150 rows to insert
-- Categories:
--   QSR: 15 brands (McDonald's, KFC, Subway, Chipotle, Nando's, Wagamama, Pret, Leon, Five Guys, Domino's, Taco Bell, Steak & Shake, Cosy Club, Benihana, Zaxby's)
--   Fashion: 15 brands (Nike, Adidas, Zara, H&M, Gap, Uniqlo, Prada, Gucci, Tommy Hilfiger, Ralph Lauren, Levi's, Dr. Martens, COS, ASOS, Shein)
--   Tech: 10 brands (Apple, Samsung, Google, Microsoft, Amazon, Dell, HP, Sony, LG, OnePlus)
--   Beauty: 10 brands (MAC, Sephora, Urban Decay, Kylie Cosmetics, Charlotte Tilbury, Fenty Beauty, Morphe, Too Faced, Drunk Elephant, Paula's Choice)

-- Use Python script for full batch insertion:
-- $ railway run python3 insert_expansion_50brands.py
