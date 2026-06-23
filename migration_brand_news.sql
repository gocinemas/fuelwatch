-- Add Recent News & Campaigns for Intel Brands

INSERT INTO brand_news
(brand_name, title, url, source, published_date, category)
VALUES

-- SKINCARE
('Dove', 'Dove Launches Sustainable Packaging Initiative', 'https://dove.com/news/sustainability-2026', 'Dove Newsroom', NOW() - INTERVAL '5 days', 'Sustainability'),
('Dove', 'Dove Real Beauty Campaign 2026: Celebrating Diversity', 'https://dove.com/real-beauty-2026', 'Dove Newsroom', NOW() - INTERVAL '12 days', 'Marketing'),

('Neutrogena', 'Neutrogena Partners with Dermatologists for New Acne Line', 'https://neutrogena.com/news/acne-launch', 'Neutrogena PR', NOW() - INTERVAL '8 days', 'Product Launch'),
('Neutrogena', 'Neutrogena Expands into Premium Skincare', 'https://neutrogena.com/news/premium-line', 'Neutrogena PR', NOW() - INTERVAL '20 days', 'Product Launch'),

('Olay', 'Olay Regenerist AI-Powered Skincare Analysis Tool', 'https://olay.com/news/ai-skincare', 'Olay Newsroom', NOW() - INTERVAL '3 days', 'Technology'),
('Olay', 'Olay Reports Record Growth in Asia Markets', 'https://olay.com/news/asia-growth', 'Olay Newsroom', NOW() - INTERVAL '15 days', 'Earnings'),

('Clinique', 'Clinique ID: Personalized Skincare System Expansion', 'https://clinique.com/news/personalization', 'Clinique PR', NOW() - INTERVAL '7 days', 'Product Launch'),

('CeraVe', 'CeraVe Clinical Dermatologist Collaboration', 'https://cerave.com/news/dermatology', 'CeraVe PR', NOW() - INTERVAL '10 days', 'Partnership'),

('Estée Lauder', 'Estée Lauder Advanced Skincare Innovation Lab', 'https://esteelauder.com/news/innovation', 'Estée Lauder PR', NOW() - INTERVAL '6 days', 'Technology'),

('L''Oréal', 'L''Oréal Commits to Carbon Neutrality by 2030', 'https://loreal.com/news/carbon-neutral', 'L''Oréal News', NOW() - INTERVAL '2 days', 'Sustainability'),
('L''Oréal', 'L''Oréal Acquires AI Beauty Tech Startup', 'https://loreal.com/news/acquisition', 'L''Oréal News', NOW() - INTERVAL '18 days', 'Acquisition'),

-- BEVERAGES
('Coca-Cola', 'Coca-Cola Launches AI-Powered Vending Experience', 'https://coca-cola.com/news/ai-vending', 'Coca-Cola Newsroom', NOW() - INTERVAL '4 days', 'Technology'),
('Coca-Cola', 'Coca-Cola Expands Zero Sugar Portfolio', 'https://coca-cola.com/news/zero-sugar', 'Coca-Cola Newsroom', NOW() - INTERVAL '11 days', 'Product Launch'),
('Coca-Cola', 'Coca-Cola Reports Q2 2026 Strong Growth', 'https://coca-cola.com/news/q2-earnings', 'Coca-Cola Investor', NOW() - INTERVAL '22 days', 'Earnings'),

('Pepsi', 'Pepsi Partnerships with Gen-Z Creators', 'https://pepsi.com/news/creator-collab', 'Pepsi PR', NOW() - INTERVAL '6 days', 'Marketing'),
('Pepsi', 'Pepsi Introduces Plant-Based Drink Line', 'https://pepsi.com/news/plant-based', 'Pepsi PR', NOW() - INTERVAL '14 days', 'Product Launch'),

('Sprite', 'Sprite Refreshes Brand Identity', 'https://sprite.com/news/rebrand-2026', 'Sprite PR', NOW() - INTERVAL '9 days', 'Branding'),

('Red Bull', 'Red Bull Extreme Sports Festival 2026', 'https://redbull.com/news/xsports-2026', 'Red Bull Media', NOW() - INTERVAL '5 days', 'Marketing'),
('Red Bull', 'Red Bull Sponsors Olympic Athletes', 'https://redbull.com/news/olympics-2026', 'Red Bull Media', NOW() - INTERVAL '21 days', 'Sponsorship'),

('Starbucks', 'Starbucks AI-Powered Personalization in App', 'https://starbucks.com/news/ai-app', 'Starbucks Newsroom', NOW() - INTERVAL '3 days', 'Technology'),
('Starbucks', 'Starbucks Sustainability Report 2026', 'https://starbucks.com/news/sustainability', 'Starbucks Newsroom', NOW() - INTERVAL '8 days', 'Sustainability'),

('Monster', 'Monster Energy Gaming Tournament 2026', 'https://monsterenergy.com/news/gaming', 'Monster PR', NOW() - INTERVAL '7 days', 'Marketing'),

-- SNACKS
('Lay''s', 'Lay''s Introduces AI-Customized Flavors', 'https://lays.com/news/ai-flavors', 'Lay''s PR', NOW() - INTERVAL '5 days', 'Product Launch'),
('Lay''s', 'Lay''s Commits to 100% Sustainable Packaging', 'https://lays.com/news/sustainability', 'Lay''s PR', NOW() - INTERVAL '19 days', 'Sustainability'),

('Doritos', 'Doritos Bold New Marketing Campaign', 'https://doritos.com/news/campaign', 'Doritos PR', NOW() - INTERVAL '6 days', 'Marketing'),

('Pringles', 'Pringles Launches Premium Flavor Line', 'https://pringles.com/news/premium', 'Pringles PR', NOW() - INTERVAL '9 days', 'Product Launch'),

('Oreo', 'Oreo Limited Edition Collaborations 2026', 'https://oreo.com/news/collaborations', 'Oreo PR', NOW() - INTERVAL '4 days', 'Product Launch'),

-- QSR
('McDonald''s', 'McDonald''s AI Crew Assistant in Drive-Thru', 'https://mcdonalds.com/news/ai-crew', 'McDonald''s Newsroom', NOW() - INTERVAL '2 days', 'Technology'),
('McDonald''s', 'McDonald''s Global Expansion 2026', 'https://mcdonalds.com/news/expansion', 'McDonald''s Newsroom', NOW() - INTERVAL '12 days', 'Expansion'),
('McDonald''s', 'McDonald''s Sustainability Milestones', 'https://mcdonalds.com/news/sustainability', 'McDonald''s Newsroom', NOW() - INTERVAL '20 days', 'Sustainability'),

('KFC', 'KFC Innovation Kitchen Opens in London', 'https://kfc.com/news/innovation-lab', 'KFC PR', NOW() - INTERVAL '7 days', 'Expansion'),
('KFC', 'KFC Celebrity Chef Partnership', 'https://kfc.com/news/chef-collab', 'KFC PR', NOW() - INTERVAL '16 days', 'Partnership'),

('Subway', 'Subway Franchise Model Innovation', 'https://subway.com/news/franchise-update', 'Subway PR', NOW() - INTERVAL '8 days', 'Business'),

('Domino''s', 'Domino''s Drone Delivery Expansion', 'https://dominos.com/news/drone-delivery', 'Domino''s PR', NOW() - INTERVAL '4 days', 'Technology'),
('Domino''s', 'Domino''s Q1 2026 Record Sales', 'https://dominos.com/news/q1-earnings', 'Domino''s Investor', NOW() - INTERVAL '14 days', 'Earnings'),

('Pizza Hut', 'Pizza Hut Metaverse Presence Launch', 'https://pizzahut.com/news/metaverse', 'Pizza Hut PR', NOW() - INTERVAL '10 days', 'Technology'),

ON CONFLICT DO NOTHING;
