-- Add sample brands to demo: Coca-Cola, Apple, Magnum, Adidas

-- ===== COCA-COLA =====
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES ('Coca Cola', 1886, 'Atlanta', 'USA', 'Open Happiness', 'The Coca-Cola Company is a multinational beverage corporation and the world''s largest beverage company, known for producing soft drinks, juices, and water.', 'coca-cola.com', 'Atlanta, Georgia, USA')
ON CONFLICT DO NOTHING;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES ('Coca Cola', 2024, '$43.8B', '$280B', 27.5, 8.2, '$12.1B', 'Yahoo Finance')
ON CONFLICT DO NOTHING;

INSERT INTO brand_skus_complete (brand_name, country, sku_name, category, price, monthly_sales_estimate, market_position, release_year)
VALUES
('Coca Cola', 'US', 'Coca-Cola Classic', 'Soft Drink', '$2.50', '800K+', 1, 1886),
('Coca Cola', 'US', 'Coca-Cola Zero Sugar', 'Soft Drink', '$2.50', '400K+', 2, 2005),
('Coca Cola', 'US', 'Sprite', 'Lemon-Lime', '$2.50', '300K+', 3, 1961),
('Coca Cola', 'UK', 'Coca-Cola Original', 'Soft Drink', '£2.20', '200K+', 1, 1886),
('Coca Cola', 'JP', 'コカ・コーラ', 'Soft Drink', '¥140', '250K+', 1, 1886)
ON CONFLICT DO NOTHING;

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share)
VALUES
('Coca Cola', 'PepsiCo', 2, 26.0),
('Coca Cola', 'Keurig Dr Pepper', 3, 8.5),
('Coca Cola', 'Monster Energy', 4, 5.2),
('Coca Cola', 'Red Bull', 5, 4.8)
ON CONFLICT DO NOTHING;

INSERT INTO competing_skus_complete (brand_name, competitor_name, competitor_sku, category, price, market_position)
VALUES
('Coca Cola', 'PepsiCo', 'Pepsi Cola', 'Soft Drink', '$2.50', 1),
('Coca Cola', 'PepsiCo', 'Mountain Dew', 'Citrus', '$2.50', 2),
('Coca Cola', 'Red Bull', 'Red Bull Energy', 'Energy Drink', '$3.00', 1),
('Coca Cola', 'Monster Energy', 'Monster', 'Energy Drink', '$2.50', 1)
ON CONFLICT DO NOTHING;

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score)
VALUES
('Coca Cola', 'Health-Focused Beverages', 'Low-sugar, functional drinks with vitamins and adaptogens', '$12B', 9, 'Wellness trend + premium positioning', 8.7),
('Coca Cola', 'AI-Personalized Hydration', 'Smart packaging with hydration recommendation AI', '$2.5B', 8, 'IoT + personalization', 8.1),
('Coca Cola', 'Sustainable Packaging', 'Circular economy bottles and carbon-neutral production', '$8B', 9, 'ESG + premium brand', 8.9),
('Coca Cola', 'Metaverse Experiences', 'Virtual product launches and NFT collectibles', '$1.8B', 7, 'Web3 + gaming', 7.2)
ON CONFLICT DO NOTHING;

INSERT INTO brand_social_media (brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES
('Coca Cola', 'Instagram', '143M', '600M+', '$45M', 2.8, 9.1),
('Coca Cola', 'TikTok', '67M', '450M+', '$35M', 6.2, 9.0),
('Coca Cola', 'YouTube', '55M', '500M+', '$40M', 3.1, 8.8),
('Coca Cola', 'Twitter', '48M', '200M+', '$20M', 1.5, 7.5)
ON CONFLICT DO NOTHING;

INSERT INTO brand_news (brand_name, title, url, source, published_date, category)
VALUES
('Coca Cola', 'Coca-Cola Invests $1B in Sustainable Packaging Innovation', 'https://news.cocacola.com/sustainability', 'Coca-Cola Newsroom', NOW() - INTERVAL '3 days', 'Sustainability'),
('Coca Cola', 'Coca-Cola Reports Record Q1 2024 Revenue Growth', 'https://news.cocacola.com/q1-2024', 'Coca-Cola Newsroom', NOW() - INTERVAL '7 days', 'Earnings'),
('Coca Cola', 'New AI-Driven Vending Machines Launch Across North America', 'https://news.cocacola.com/ai-vending', 'Tech News', NOW() - INTERVAL '10 days', 'Technology')
ON CONFLICT DO NOTHING;

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date, source)
VALUES
('Coca Cola', 'AI-powered demand forecasting and supply chain optimization', NOW() - INTERVAL '30 days', 'Coca-Cola Innovation'),
('Coca Cola', 'Personalized marketing with customer preference AI', NOW() - INTERVAL '60 days', 'Coca-Cola Digital'),
('Coca Cola', 'Sustainability AI for carbon footprint tracking', NOW() - INTERVAL '90 days', 'Coca-Cola ESG')
ON CONFLICT DO NOTHING;

-- ===== APPLE =====
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES ('Apple', 1976, 'Los Altos', 'USA', 'Think Different', 'Apple Inc. is a multinational technology company that designs, manufactures, and markets consumer electronics, software, and services worldwide.', 'apple.com', 'Cupertino, California, USA')
ON CONFLICT DO NOTHING;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES ('Apple', 2024, '$394.3B', '$3.2T', 31.2, 5.8, '$123.2B', 'Yahoo Finance')
ON CONFLICT DO NOTHING;

INSERT INTO brand_skus_complete (brand_name, country, sku_name, category, price, monthly_sales_estimate, market_position, release_year)
VALUES
('Apple', 'US', 'iPhone 16 Pro Max', 'Smartphone', '$1,199', '450K+', 1, 2024),
('Apple', 'US', 'MacBook Pro M4', 'Laptop', '$2,499', '200K+', 2, 2024),
('Apple', 'US', 'Apple Watch Series 10', 'Wearable', '$399', '150K+', 3, 2024),
('Apple', 'UK', 'iPhone 16', 'Smartphone', '£799', '180K+', 1, 2024),
('Apple', 'JP', 'iPad Pro', 'Tablet', '¥159,800', '120K+', 2, 2024)
ON CONFLICT DO NOTHING;

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share)
VALUES
('Apple', 'Samsung', 2, 19.5),
('Apple', 'Xiaomi', 3, 13.2),
('Apple', 'Google', 4, 8.7),
('Apple', 'OnePlus', 5, 3.5)
ON CONFLICT DO NOTHING;

INSERT INTO competing_skus_complete (brand_name, competitor_name, competitor_sku, category, price, market_position)
VALUES
('Apple', 'Samsung', 'Galaxy S24 Ultra', 'Smartphone', '$1,299', 1),
('Apple', 'Google', 'Pixel 9 Pro', 'Smartphone', '$999', 2),
('Apple', 'Xiaomi', 'Xiaomi 14 Ultra', 'Smartphone', '$1,099', 1)
ON CONFLICT DO NOTHING;

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score)
VALUES
('Apple', 'Health Tech Integration', 'Advanced biometric sensors for continuous health monitoring', '$15B', 9, 'Health + premium wearables', 8.8),
('Apple', 'AI-Powered Personal Assistant', 'On-device LLMs for enhanced Siri capabilities', '$8B', 9, 'AI + privacy-first', 8.9),
('Apple', 'Extended Reality (AR/VR)', 'Lightweight AR glasses and immersive experiences', '$18B', 8, 'Spatial computing + ecosystem', 8.5),
('Apple', 'Home Automation Hub', 'Centralized smart home control and automation', '$12B', 8, 'IoT + ecosystem lock-in', 8.3)
ON CONFLICT DO NOTHING;

INSERT INTO brand_social_media (brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES
('Apple', 'Instagram', '327M', '900M+', '$60M', 3.2, 9.3),
('Apple', 'TikTok', '112M', '800M+', '$50M', 7.1, 9.1),
('Apple', 'YouTube', '89M', '1B+', '$70M', 2.9, 9.0),
('Apple', 'Twitter', '76M', '400M+', '$30M', 1.8, 8.2)
ON CONFLICT DO NOTHING;

INSERT INTO brand_news (brand_name, title, url, source, published_date, category)
VALUES
('Apple', 'Apple Introduces Revolutionary AI Features Across All Devices', 'https://news.apple.com/ai-2024', 'Apple Newsroom', NOW() - INTERVAL '2 days', 'Technology'),
('Apple', 'Apple Achieves New Record Quarterly Revenue of $124.3B', 'https://news.apple.com/q2-2024', 'Apple Newsroom', NOW() - INTERVAL '5 days', 'Earnings'),
('Apple', 'New MacBook Pro with M4 Max Chip Launches with Advanced AI', 'https://news.apple.com/macbook-m4', 'Tech News', NOW() - INTERVAL '8 days', 'Product Launch')
ON CONFLICT DO NOTHING;

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date, source)
VALUES
('Apple', 'On-device AI and machine learning for privacy', NOW() - INTERVAL '20 days', 'Apple WWDC 2024'),
('Apple', 'AI-powered photo and video editing tools', NOW() - INTERVAL '45 days', 'Apple Intelligence'),
('Apple', 'Advanced computational photography with neural engines', NOW() - INTERVAL '60 days', 'Apple Camera AI')
ON CONFLICT DO NOTHING;

-- ===== MAGNUM ICE CREAM =====
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES ('Magnum', 1987, 'Oss', 'Netherlands', 'Taste the Unexpected', 'Magnum is a premium ice cream brand owned by Unilever, known for its indulgent, high-quality ice cream covered in chocolate coating.', 'magnumicecream.com', 'Amsterdam, Netherlands')
ON CONFLICT DO NOTHING;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES ('Magnum', 2024, '$2.1B', '—', 18.5, 12.3, '$388M', 'Unilever Reports')
ON CONFLICT DO NOTHING;

INSERT INTO brand_skus_complete (brand_name, country, sku_name, category, price, monthly_sales_estimate, market_position, release_year)
VALUES
('Magnum', 'US', 'Magnum Classic', 'Premium Ice Cream', '$4.99', '600K+', 1, 1987),
('Magnum', 'US', 'Magnum Double Caramel', 'Premium Ice Cream', '$5.49', '350K+', 2, 2003),
('Magnum', 'US', 'Magnum Almond', 'Premium Ice Cream', '$5.49', '280K+', 3, 2008),
('Magnum', 'UK', 'Magnum Original', 'Premium Ice Cream', '£4.50', '400K+', 1, 1987),
('Magnum', 'JP', 'マグナム', 'Premium Ice Cream', '¥290', '180K+', 1, 1987)
ON CONFLICT DO NOTHING;

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share)
VALUES
('Magnum', 'Ben & Jerry''s', 2, 22.0),
('Magnum', 'Häagen-Dazs', 3, 18.5),
('Magnum', 'Cornetto', 4, 12.0),
('Magnum', 'Drumstick', 5, 8.5)
ON CONFLICT DO NOTHING;

INSERT INTO competing_skus_complete (brand_name, competitor_name, competitor_sku, category, price, market_position)
VALUES
('Magnum', 'Ben & Jerry''s', 'Chunky Monkey', 'Premium Ice Cream', '$5.99', 1),
('Magnum', 'Häagen-Dazs', 'Strawberry', 'Premium Ice Cream', '$5.49', 2),
('Magnum', 'Cornetto', 'Classico', 'Ice Cream Cone', '$2.99', 1)
ON CONFLICT DO NOTHING;

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score)
VALUES
('Magnum', 'Plant-Based Premium', 'Vegan luxury ice cream with exotic flavors', '$800M', 8, 'Plant-based + sustainability', 8.2),
('Magnum', 'Functional Ice Cream', 'Protein-enriched and health-boosted premium ice cream', '$600M', 7, 'Health + indulgence', 7.5),
('Magnum', 'Personalized Experiences', 'Customizable ice cream with AI flavor recommendations', '$400M', 7, 'Personalization + premium', 7.3),
('Magnum', 'Sustainability Leadership', 'Fully compostable packaging and ethically sourced cacao', '$500M', 8, 'ESG + premium', 8.1)
ON CONFLICT DO NOTHING;

INSERT INTO brand_social_media (brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES
('Magnum', 'Instagram', '28M', '200M+', '$15M', 5.8, 8.5),
('Magnum', 'TikTok', '12M', '150M+', '$12M', 8.2, 8.2),
('Magnum', 'YouTube', '8M', '80M+', '$8M', 3.5, 7.8),
('Magnum', 'Twitter', '5M', '30M+', '$3M', 1.2, 6.5)
ON CONFLICT DO NOTHING;

INSERT INTO brand_news (brand_name, title, url, source, published_date, category)
VALUES
('Magnum', 'Magnum Launches Revolutionary Plant-Based Ice Cream Line', 'https://magnum.com/news/plantbased', 'Magnum Newsroom', NOW() - INTERVAL '4 days', 'Product Launch'),
('Magnum', 'Magnum Commits to 100% Sustainable Packaging by 2025', 'https://magnum.com/news/sustainability', 'Unilever ESG', NOW() - INTERVAL '12 days', 'Sustainability'),
('Magnum', 'Magnum Records 15% Growth in Asia-Pacific Region', 'https://magnum.com/news/growth', 'Magnum Global', NOW() - INTERVAL '14 days', 'Business')
ON CONFLICT DO NOTHING;

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date, source)
VALUES
('Magnum', 'AI-powered flavor recommendation engine for personalization', NOW() - INTERVAL '25 days', 'Magnum Innovation Lab'),
('Magnum', 'Machine learning for supply chain optimization', NOW() - INTERVAL '50 days', 'Magnum Operations'),
('Magnum', 'Predictive analytics for seasonal demand forecasting', NOW() - INTERVAL '75 days', 'Unilever AI Centre')
ON CONFLICT DO NOTHING;

-- ===== ADIDAS =====
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES ('Adidas', 1949, 'Herzogenaurach', 'Germany', 'Impossible is Nothing', 'Adidas is a multinational corporation that designs, manufactures, and sells athletic shoes, apparel, and accessories.', 'adidas.com', 'Herzogenaurach, Germany')
ON CONFLICT DO NOTHING;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES ('Adidas', 2024, '$23.6B', '$85B', 12.8, 7.4, '$3.0B', 'Yahoo Finance')
ON CONFLICT DO NOTHING;

INSERT INTO brand_skus_complete (brand_name, country, sku_name, category, price, monthly_sales_estimate, market_position, release_year)
VALUES
('Adidas', 'US', 'Superstar', 'Footwear', '$100', '350K+', 1, 1969),
('Adidas', 'US', 'Stan Smith', 'Footwear', '$110', '250K+', 2, 1965),
('Adidas', 'US', 'UltraBoost', 'Footwear', '$180', '200K+', 3, 2015),
('Adidas', 'UK', 'Superstar', 'Footwear', '£95', '120K+', 1, 1969),
('Adidas', 'JP', 'Superstar', 'Footwear', '¥13,500', '100K+', 1, 1969)
ON CONFLICT DO NOTHING;

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share)
VALUES
('Adidas', 'Nike', 1, 37.0),
('Adidas', 'Puma', 3, 7.0),
('Adidas', 'New Balance', 4, 5.5)
ON CONFLICT DO NOTHING;

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score)
VALUES
('Adidas', 'Sustainable Performance Gear', 'Eco-friendly high-performance athletic wear', '$3B', 8, 'Sustainability + performance', 8.1),
('Adidas', 'AI Fit Optimization', 'ML-powered shoe sizing and comfort customization', '$1.5B', 8, 'AI + personalization', 8.0)
ON CONFLICT DO NOTHING;

INSERT INTO brand_social_media (brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES
('Adidas', 'Instagram', '85M', '400M+', '$30M', 3.5, 8.7),
('Adidas', 'TikTok', '32M', '300M+', '$20M', 5.8, 8.3),
('Adidas', 'YouTube', '28M', '200M+', '$15M', 2.1, 7.9)
ON CONFLICT DO NOTHING;

INSERT INTO brand_news (brand_name, title, url, source, published_date, category)
VALUES
('Adidas', 'Adidas Launches Fully Sustainable Footwear Collection', 'https://adidas.com/news/sustainability', 'Adidas Newsroom', NOW() - INTERVAL '6 days', 'Sustainability'),
('Adidas', 'Adidas Reports Strong Q2 Growth in China Market', 'https://adidas.com/news/q2', 'Adidas Newsroom', NOW() - INTERVAL '9 days', 'Earnings')
ON CONFLICT DO NOTHING;

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date, source)
VALUES
('Adidas', 'AI-powered shoe design and customization', NOW() - INTERVAL '30 days', 'Adidas Innovation'),
('Adidas', 'Demand forecasting with machine learning', NOW() - INTERVAL '60 days', 'Adidas Operations')
ON CONFLICT DO NOTHING;

-- ===== STARBUCKS =====
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES ('Starbucks', 1971, 'Seattle', 'USA', 'To inspire and nurture the human spirit', 'Starbucks Corporation is an American multinational coffeehouse chain and roastery reserves retailer.', 'starbucks.com', 'Seattle, Washington, USA')
ON CONFLICT DO NOTHING;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES ('Starbucks', 2024, '$36.2B', '$120B', 16.3, 6.5, '$5.9B', 'Yahoo Finance')
ON CONFLICT DO NOTHING;

INSERT INTO brand_skus_complete (brand_name, country, sku_name, category, price, monthly_sales_estimate, market_position, release_year)
VALUES
('Starbucks', 'US', 'Caffe Latte', 'Coffee', '$5.25', '1.2M+', 1, 1995),
('Starbucks', 'US', 'Cold Brew', 'Coffee', '$3.95', '800K+', 2, 2009),
('Starbucks', 'US', 'Frappuccino', 'Coffee Beverage', '$5.95', '600K+', 3, 1995),
('Starbucks', 'UK', 'Caffe Latte', 'Coffee', '£4.80', '200K+', 1, 1995),
('Starbucks', 'JP', 'Caffe Latte', 'Coffee', '¥650', '250K+', 1, 1995)
ON CONFLICT DO NOTHING;

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share)
VALUES
('Starbucks', 'Dunkin''', 2, 18.0),
('Starbucks', 'Tim Hortons', 3, 12.0),
('Starbucks', 'Cafe Coffee Day', 4, 6.5)
ON CONFLICT DO NOTHING;

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score)
VALUES
('Starbucks', 'AI-Powered Personalization', 'Predictive ordering and personalized drink recommendations', '$2B', 8, 'AI + loyalty program', 8.2),
('Starbucks', 'Sustainability Premium Tier', 'Zero-waste, premium coffee subscription with environmental credits', '$1.5B', 8, 'ESG + premium', 8.0)
ON CONFLICT DO NOTHING;

INSERT INTO brand_social_media (brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES
('Starbucks', 'Instagram', '42M', '300M+', '$25M', 4.2, 8.5),
('Starbucks', 'TikTok', '18M', '200M+', '$15M', 7.1, 8.0),
('Starbucks', 'YouTube', '12M', '100M+', '$10M', 2.8, 7.5)
ON CONFLICT DO NOTHING;

INSERT INTO brand_news (brand_name, title, url, source, published_date, category)
VALUES
('Starbucks', 'Starbucks Introduces AI Barista Assistant in 500 Locations', 'https://starbucks.com/news/ai-barista', 'Starbucks Newsroom', NOW() - INTERVAL '3 days', 'Technology'),
('Starbucks', 'Starbucks Reaches 1M Rewards Members Milestone', 'https://starbucks.com/news/rewards', 'Starbucks Newsroom', NOW() - INTERVAL '11 days', 'Business')
ON CONFLICT DO NOTHING;

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date, source)
VALUES
('Starbucks', 'AI-powered inventory management and waste reduction', NOW() - INTERVAL '20 days', 'Starbucks Digital'),
('Starbucks', 'Machine learning for personalized marketing', NOW() - INTERVAL '45 days', 'Starbucks Rewards')
ON CONFLICT DO NOTHING;

-- ===== TESLA =====
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES ('Tesla', 2003, 'San Carlos', 'USA', 'The Future of Energy', 'Tesla, Inc. is an electric vehicle and clean energy company that manufactures electric cars and energy storage systems.', 'tesla.com', 'Austin, Texas, USA')
ON CONFLICT DO NOTHING;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES ('Tesla', 2024, '$81.5B', '$1.1T', 15.6, 18.3, '$12.7B', 'Yahoo Finance')
ON CONFLICT DO NOTHING;

INSERT INTO brand_skus_complete (brand_name, country, sku_name, category, price, monthly_sales_estimate, market_position, release_year)
VALUES
('Tesla', 'US', 'Model 3', 'EV Sedan', '$43,990', '180K+', 1, 2017),
('Tesla', 'US', 'Model Y', 'EV SUV', '$52,990', '220K+', 2, 2020),
('Tesla', 'US', 'Model S', 'EV Sedan', '$94,990', '80K+', 3, 2012),
('Tesla', 'UK', 'Model 3', 'EV Sedan', '£39,990', '50K+', 1, 2017),
('Tesla', 'JP', 'Model 3', 'EV Sedan', '¥5,990,000', '40K+', 1, 2017)
ON CONFLICT DO NOTHING;

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share)
VALUES
('Tesla', 'BYD', 2, 18.0),
('Tesla', 'Volkswagen', 3, 9.5),
('Tesla', 'BMW', 4, 6.2)
ON CONFLICT DO NOTHING;

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score)
VALUES
('Tesla', 'Advanced Autonomous Driving', 'Full Level 5 autonomous capability rollout', '$25B', 9, 'AI + mobility', 9.0),
('Tesla', 'Energy Storage at Scale', 'Home and grid-scale battery storage with AI optimization', '$15B', 9, 'Clean energy + AI', 8.8)
ON CONFLICT DO NOTHING;

INSERT INTO brand_social_media (brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES
('Tesla', 'X (Twitter)', '78M', '500M+', '$10M', 8.5, 8.9),
('Tesla', 'YouTube', '52M', '400M+', '$20M', 4.2, 8.5),
('Tesla', 'Instagram', '38M', '300M+', '$15M', 5.1, 8.3)
ON CONFLICT DO NOTHING;

INSERT INTO brand_news (brand_name, title, url, source, published_date, category)
VALUES
('Tesla', 'Tesla Achieves Record Quarterly Deliveries at 1.8M Units', 'https://tesla.com/news/q1-2024', 'Tesla Newsroom', NOW() - INTERVAL '4 days', 'Earnings'),
('Tesla', 'Tesla Advances Full Self-Driving Beta with Latest Neural Networks', 'https://tesla.com/news/fsd-v12', 'Tesla AI Blog', NOW() - INTERVAL '8 days', 'Technology')
ON CONFLICT DO NOTHING;

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date, source)
VALUES
('Tesla', 'Full Self-Driving AI with neural networks', NOW() - INTERVAL '15 days', 'Tesla AI Division'),
('Tesla', 'Energy optimization with machine learning', NOW() - INTERVAL '40 days', 'Tesla Energy'),
('Tesla', 'Humanoid robotics AI development (Optimus)', NOW() - INTERVAL '70 days', 'Tesla Robotics')
ON CONFLICT DO NOTHING;

-- ===== SAMSUNG =====
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES ('Samsung', 1938, 'Seoul', 'South Korea', 'Inspire the World, Create the Future', 'Samsung Electronics is a multinational conglomerate that manufactures consumer and commercial electronics.', 'samsung.com', 'Seoul, South Korea')
ON CONFLICT DO NOTHING;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES ('Samsung', 2024, '$250.4B', '$1.25T', 8.5, 3.2, '$21.3B', 'Yahoo Finance')
ON CONFLICT DO NOTHING;

INSERT INTO brand_skus_complete (brand_name, country, sku_name, category, price, monthly_sales_estimate, market_position, release_year)
VALUES
('Samsung', 'US', 'Galaxy S24 Ultra', 'Smartphone', '$1,299', '380K+', 1, 2024),
('Samsung', 'US', 'Galaxy Tab S10', 'Tablet', '$899', '150K+', 2, 2024),
('Samsung', 'US', 'QLED TV 85-inch', 'TV', '$3,299', '80K+', 3, 2022),
('Samsung', 'UK', 'Galaxy S24', 'Smartphone', '£999', '120K+', 1, 2024),
('Samsung', 'JP', 'Galaxy S24', 'Smartphone', '¥155,000', '100K+', 1, 2024)
ON CONFLICT DO NOTHING;

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share)
VALUES
('Samsung', 'Apple', 1, 26.0),
('Samsung', 'Xiaomi', 3, 13.0),
('Samsung', 'OPPO', 4, 10.5)
ON CONFLICT DO NOTHING;

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score)
VALUES
('Samsung', 'AI Chip Manufacturing Leadership', 'Custom AI chips for edge computing and mobile devices', '$8B', 8, 'Semiconductors + AI', 8.3),
('Samsung', 'Foldable Innovation at Scale', 'Advanced foldable displays for mainstream market', '$5B', 8, 'Innovation + consumer electronics', 8.1)
ON CONFLICT DO NOTHING;

INSERT INTO brand_social_media (brand_name, platform, followers, reach, estimated_monthly_ad_spend, engagement_rate, investment_score)
VALUES
('Samsung', 'Instagram', '152M', '700M+', '$50M', 2.9, 8.9),
('Samsung', 'TikTok', '68M', '500M+', '$35M', 6.5, 8.6),
('Samsung', 'YouTube', '64M', '600M+', '$45M', 3.2, 8.7)
ON CONFLICT DO NOTHING;

INSERT INTO brand_news (brand_name, title, url, source, published_date, category)
VALUES
('Samsung', 'Samsung Unveils Next-Gen AI Chip Architecture', 'https://samsung.com/news/ai-chip', 'Samsung Newsroom', NOW() - INTERVAL '2 days', 'Technology'),
('Samsung', 'Samsung Reports Record Profit Despite Market Challenges', 'https://samsung.com/news/q1-earnings', 'Samsung Newsroom', NOW() - INTERVAL '7 days', 'Earnings')
ON CONFLICT DO NOTHING;

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date, source)
VALUES
('Samsung', 'On-device AI processing with HexagonAI', NOW() - INTERVAL '25 days', 'Samsung Research'),
('Samsung', 'AI-enhanced display technology and optimization', NOW() - INTERVAL '55 days', 'Samsung Display')
ON CONFLICT DO NOTHING;
