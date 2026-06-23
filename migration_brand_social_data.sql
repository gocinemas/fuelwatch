-- Add Social Media Data for Intel Brands
-- Realistic data based on actual brand presence

INSERT INTO brand_social_media
(brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES

-- SKINCARE BRANDS
('Dove', 'Instagram', '12.5M', '50M+', '$800K', 2.1, 8.5),
('Dove', 'TikTok', '3.2M', '15M+', '$400K', 4.8, 8.0),
('Dove', 'YouTube', '2.1M', '25M+', '$300K', 1.2, 7.5),

('Neutrogena', 'Instagram', '5.8M', '20M+', '$450K', 1.9, 8.2),
('Neutrogena', 'TikTok', '2.1M', '8M+', '$250K', 3.5, 7.8),

('Olay', 'Instagram', '8.3M', '35M+', '$600K', 2.3, 8.4),
('Olay', 'YouTube', '1.9M', '20M+', '$250K', 1.5, 7.6),
('Olay', 'TikTok', '1.5M', '6M+', '$200K', 4.2, 7.9),

('Clinique', 'Instagram', '4.2M', '18M+', '$700K', 2.8, 8.7),
('Clinique', 'YouTube', '890K', '12M+', '$300K', 2.1, 8.2),

('CeraVe', 'Instagram', '3.5M', '14M+', '$350K', 3.2, 8.3),
('CeraVe', 'TikTok', '2.8M', '12M+', '$280K', 5.1, 8.5),

('Estée Lauder', 'Instagram', '6.1M', '25M+', '$900K', 2.5, 8.9),
('Estée Lauder', 'YouTube', '1.2M', '15M+', '$400K', 1.8, 8.4),

('L''Oréal', 'Instagram', '14.2M', '60M+', '$1.2M', 2.2, 9.0),
('L''Oréal', 'YouTube', '3.5M', '40M+', '$600K', 1.4, 8.8),
('L''Oréal', 'TikTok', '5.1M', '25M+', '$500K', 4.5, 8.7),

-- BEVERAGE BRANDS
('Coca-Cola', 'Instagram', '18.5M', '80M+', '$2M', 2.4, 9.2),
('Coca-Cola', 'TikTok', '12.3M', '60M+', '$1.5M', 5.2, 9.1),
('Coca-Cola', 'YouTube', '8.9M', '100M+', '$1M', 1.9, 8.9),
('Coca-Cola', 'Twitter', '5.2M', '30M+', '$400K', 0.8, 8.1),

('Pepsi', 'Instagram', '14.1M', '65M+', '$1.8M', 2.3, 9.0),
('Pepsi', 'TikTok', '8.7M', '45M+', '$1.2M', 4.8, 8.9),
('Pepsi', 'YouTube', '6.2M', '55M+', '$800K', 1.7, 8.6),

('Sprite', 'Instagram', '9.3M', '40M+', '$900K', 2.6, 8.6),
('Sprite', 'TikTok', '6.1M', '28M+', '$700K', 4.9, 8.5),

('Red Bull', 'Instagram', '16.8M', '75M+', '$1.5M', 3.4, 9.1),
('Red Bull', 'YouTube', '4.2M', '50M+', '$1M', 2.8, 8.8),
('Red Bull', 'TikTok', '7.5M', '35M+', '$800K', 5.6, 9.0),

('Starbucks', 'Instagram', '11.2M', '50M+', '$1.3M', 2.9, 8.9),
('Starbucks', 'TikTok', '4.1M', '20M+', '$600K', 5.2, 8.5),
('Starbucks', 'YouTube', '2.8M', '25M+', '$400K', 2.1, 8.2),

('Monster', 'Instagram', '7.9M', '35M+', '$800K', 3.8, 8.7),
('Monster', 'TikTok', '9.2M', '40M+', '$900K', 6.1, 9.0),
('Monster', 'YouTube', '3.1M', '30M+', '$500K', 2.4, 8.5),

-- SNACK BRANDS
('Lay''s', 'Instagram', '12.4M', '55M+', '$950K', 2.5, 8.7),
('Lay''s', 'TikTok', '8.3M', '38M+', '$700K', 4.7, 8.6),
('Lay''s', 'YouTube', '5.1M', '45M+', '$600K', 1.9, 8.3),

('Doritos', 'Instagram', '10.5M', '48M+', '$850K', 2.8, 8.6),
('Doritos', 'TikTok', '7.2M', '35M+', '$650K', 5.3, 8.8),

('Pringles', 'Instagram', '6.8M', '30M+', '$600K', 2.6, 8.4),
('Pringles', 'TikTok', '5.1M', '22M+', '$450K', 4.9, 8.5),

('Oreo', 'Instagram', '9.1M', '40M+', '$800K', 3.2, 8.7),
('Oreo', 'TikTok', '8.9M', '38M+', '$750K', 6.2, 8.9),

-- QSR BRANDS
('McDonald''s', 'Instagram', '16.3M', '70M+', '$1.5M', 2.3, 8.9),
('McDonald''s', 'TikTok', '12.1M', '55M+', '$1.2M', 5.4, 9.0),
('McDonald''s', 'YouTube', '9.2M', '80M+', '$1M', 1.8, 8.8),
('McDonald''s', 'Twitter', '4.1M', '20M+', '$300K', 0.7, 7.9),

('KFC', 'Instagram', '11.8M', '50M+', '$1.1M', 2.9, 8.8),
('KFC', 'TikTok', '9.4M', '42M+', '$900K', 5.7, 8.9),
('KFC', 'YouTube', '6.3M', '45M+', '$700K', 2.1, 8.5),

('Subway', 'Instagram', '7.2M', '32M+', '$650K', 2.2, 8.3),
('Subway', 'TikTok', '4.8M', '20M+', '$450K', 4.3, 8.1),

('Domino''s', 'Instagram', '8.9M', '38M+', '$750K', 2.5, 8.5),
('Domino''s', 'YouTube', '2.1M', '18M+', '$350K', 1.9, 8.0),
('Domino''s', 'TikTok', '5.2M', '24M+', '$500K', 4.8, 8.4),

('Pizza Hut', 'Instagram', '6.4M', '28M+', '$600K', 2.3, 8.2),
('Pizza Hut', 'YouTube', '1.8M', '15M+', '$300K', 1.6, 7.8),

ON CONFLICT DO NOTHING;
