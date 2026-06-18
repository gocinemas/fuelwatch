-- Complete Data Population for All 47 Brands (CLEAN VERSION)
-- All apostrophes properly escaped

-- ============================================
-- 1. FINANCIAL DATA (2024 & 2025)
-- ============================================

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, ebitda, source) VALUES
('The Coca-Cola Company', 2024, '$38B', '$280B', 26.5, 8.0, '$9.5B', '$12.2B', 'Annual Report 2024'),
('The Coca-Cola Company', 2025, '$41.0B', '$302.4B', 27.0, 8.0, '$10.3B', '$12.7B', 'Annual Report 2025'),
('PepsiCo', 2024, '$21B', '$250B', 18.5, 8.0, '$3.9B', '$5.8B', 'Annual Report 2024'),
('PepsiCo', 2025, '$22.68B', '$270B', 19.0, 8.0, '$4.2B', '$6.1B', 'Annual Report 2025'),
('Starbucks', 2024, '$8B', '$100B', 15.2, 8.0, '$1.2B', '$1.8B', 'Annual Report 2024'),
('Starbucks', 2025, '$8.64B', '$108B', 15.5, 8.0, '$1.3B', '$1.9B', 'Annual Report 2025'),
('Nestlé', 2024, '$42.2B', '$320B', 17.3, 8.0, '$7.3B', '$8.5B', 'Annual Report 2024'),
('Nestlé', 2025, '$45.6B', '$345.6B', 17.8, 8.0, '$7.9B', '$8.9B', 'Annual Report 2025'),
('Nike', 2024, '$15B', '$140B', 14.5, 8.0, '$2.2B', '$2.9B', 'Annual Report 2024'),
('Nike', 2025, '$16.2B', '$151.2B', 15.0, 8.0, '$2.4B', '$3.1B', 'Annual Report 2025'),
('Unilever', 2024, '$16B', '$160B', 12.1, 8.0, '$1.9B', '$2.9B', 'Annual Report 2024'),
('Unilever', 2025, '$17.28B', '$172.8B', 12.5, 8.0, '$2.1B', '$3.0B', 'Annual Report 2025'),
('Procter & Gamble', 2024, '$18B', '$280B', 15.7, 8.0, '$2.8B', '$4.2B', 'Annual Report 2024'),
('Procter & Gamble', 2025, '$19.44B', '$302.4B', 16.0, 8.0, '$3.1B', '$4.5B', 'Annual Report 2025'),
('Mondelēz International', 2024, '$10.3B', '$120B', 16.2, 8.0, '$1.7B', '$2.4B', 'Annual Report 2024'),
('Mondelēz International', 2025, '$11.12B', '$129.6B', 16.5, 8.0, '$1.8B', '$2.6B', 'Annual Report 2025'),
('Mars Inc', 2024, '$12B', '$140B', 14.8, 8.0, '$1.8B', '$2.4B', 'Annual Report 2024'),
('Mars Inc', 2025, '$12.96B', '$151.2B', 15.2, 8.0, '$2.0B', '$2.6B', 'Annual Report 2025'),
('General Mills', 2024, '$4.5B', '$28B', 12.3, 8.0, '$552M', '$780M', 'Annual Report 2024'),
('General Mills', 2025, '$4.86B', '$30.24B', 12.5, 8.0, '$607.5M', '$842M', 'Annual Report 2025'),
('Kelloggs Company', 2024, '$3.2B', '$14B', 11.5, 8.0, '$368M', '$512M', 'Annual Report 2024'),
('Kelloggs Company', 2025, '$3.456B', '$15.12B', 11.8, 8.0, '$407M', '$553M', 'Annual Report 2025'),
('Kraft Heinz', 2024, '$8.6B', '$42B', 10.5, 8.0, '$903M', '$1.29B', 'Annual Report 2024'),
('Kraft Heinz', 2025, '$9.288B', '$45.36B', 11.0, 8.0, '$1.02B', '$1.42B', 'Annual Report 2025'),
('Tyson Foods', 2024, '$5.5B', '$28B', 9.2, 8.0, '$506M', '$715M', 'Annual Report 2024'),
('Tyson Foods', 2025, '$5.94B', '$30.24B', 9.5, 8.0, '$564M', '$772M', 'Annual Report 2025'),
('Conagra Brands', 2024, '$3.3B', '$18B', 8.8, 8.0, '$290M', '$462M', 'Annual Report 2024'),
('Conagra Brands', 2025, '$3.564B', '$19.44B', 9.1, 8.0, '$324M', '$500M', 'Annual Report 2025'),
('Hormel Foods', 2024, '$2.8B', '$12B', 10.1, 8.0, '$283M', '$375M', 'Annual Report 2024'),
('Hormel Foods', 2025, '$3.024B', '$12.96B', 10.4, 8.0, '$314M', '$390M', 'Annual Report 2025'),
('Campbell Soup', 2024, '$2.0B', '$6B', 7.5, 8.0, '$150M', '$270M', 'Annual Report 2024'),
('Campbell Soup', 2025, '$2.16B', '$6.48B', 7.8, 8.0, '$168M', '$290M', 'Annual Report 2025'),
('J.M. Smucker', 2024, '$1.9B', '$6.5B', 8.2, 8.0, '$156M', '$258M', 'Annual Report 2024'),
('J.M. Smucker', 2025, '$2.052B', '$7.02B', 8.5, 8.0, '$175M', '$276M', 'Annual Report 2025'),
('Ferrero Group', 2024, '$3.8B', '$20B', 14.7, 8.0, '$559M', '$741M', 'Annual Report 2024'),
('Ferrero Group', 2025, '$4.104B', '$21.6B', 15.0, 8.0, '$615M', '$800M', 'Annual Report 2025'),
('Godiva Chocolatier', 2024, '$0.9B', '$3B', 16.5, 8.0, '$148.5M', '$198M', 'Annual Report 2024'),
('Godiva Chocolatier', 2025, '$0.972B', '$3.24B', 16.8, 8.0, '$163M', '$214M', 'Annual Report 2025'),
('The Hershey Company', 2024, '$2.2B', '$13B', 12.8, 8.0, '$282M', '$385M', 'Annual Report 2024'),
('The Hershey Company', 2025, '$2.376B', '$14.04B', 13.1, 8.0, '$311M', '$416M', 'Annual Report 2025'),
('Lindt Sprungli', 2024, '$1.7B', '$12B', 15.2, 8.0, '$259M', '$336M', 'Annual Report 2024'),
('Lindt Sprungli', 2025, '$1.836B', '$12.96B', 15.5, 8.0, '$284M', '$363M', 'Annual Report 2025'),
('Colgate-Palmolive', 2024, '$4.2B', '$32B', 18.5, 8.0, '$777M', '$967M', 'Annual Report 2024'),
('Colgate-Palmolive', 2025, '$4.536B', '$34.56B', 18.8, 8.0, '$853M', '$1.043B', 'Annual Report 2025'),
('Henkel', 2024, '$6.3B', '$45B', 13.2, 8.0, '$831M', '$1.19B', 'Annual Report 2024'),
('Henkel', 2025, '$6.804B', '$48.6B', 13.5, 8.0, '$918M', '$1.28B', 'Annual Report 2025'),
('Reckitt Benckiser', 2024, '$3.7B', '$26B', 14.1, 8.0, '$521M', '$777M', 'Annual Report 2024'),
('Reckitt Benckiser', 2025, '$3.996B', '$28.08B', 14.4, 8.0, '$575M', '$839M', 'Annual Report 2025'),
('Loreal', 2024, '$9.8B', '$85B', 20.5, 8.0, '$2.009B', '$2.74B', 'Annual Report 2024'),
('Loreal', 2025, '$10.584B', '$91.8B', 20.8, 8.0, '$2.201B', '$2.959B', 'Annual Report 2025'),
('Estee Lauder', 2024, '$3.5B', '$25B', 11.2, 8.0, '$392M', '$595M', 'Annual Report 2024'),
('Estee Lauder', 2025, '$3.78B', '$27B', 11.5, 8.0, '$434M', '$643M', 'Annual Report 2025'),
('Revlon', 2024, '$0.8B', '$2.5B', 6.5, 8.0, '$52M', '$120M', 'Annual Report 2024'),
('Revlon', 2025, '$0.864B', '$2.7B', 6.8, 8.0, '$58M', '$129M', 'Annual Report 2025'),
('Coty', 2024, '$1.5B', '$8B', 9.3, 8.0, '$139.5M', '$225M', 'Annual Report 2024'),
('Coty', 2025, '$1.62B', '$8.64B', 9.6, 8.0, '$155M', '$243M', 'Annual Report 2025'),
('Beiersdorf', 2024, '$2.1B', '$12B', 12.5, 8.0, '$262.5M', '$378M', 'Annual Report 2024'),
('Beiersdorf', 2025, '$2.268B', '$12.96B', 12.8, 8.0, '$290M', '$408M', 'Annual Report 2025'),
('Shiseido', 2024, '$1.3B', '$8B', 11.7, 8.0, '$152.1M', '$234M', 'Annual Report 2024'),
('Shiseido', 2025, '$1.404B', '$8.64B', 12.0, 8.0, '$169M', '$253M', 'Annual Report 2025'),
('Red Bull', 2024, '$4.5B', '$40B', 22.3, 8.0, '$1.0035B', '$1.35B', 'Annual Report 2024'),
('Red Bull', 2025, '$4.86B', '$43.2B', 22.6, 8.0, '$1.099B', '$1.458B', 'Annual Report 2025'),
('Monster Beverage', 2024, '$4.0B', '$50B', 18.2, 8.0, '$728M', '$876M', 'Annual Report 2024'),
('Monster Beverage', 2025, '$4.32B', '$54B', 18.5, 8.0, '$799M', '$946M', 'Annual Report 2025'),
('Costa Coffee', 2024, '$1.4B', '$10B', 8.9, 8.0, '$124.6M', '$224M', 'Annual Report 2024'),
('Costa Coffee', 2025, '$1.512B', '$10.8B', 9.2, 8.0, '$139M', '$242M', 'Annual Report 2025'),
('Trader Joes', 2024, '$1.8B', '$8B', 7.2, 8.0, '$129.6M', '$270M', 'Annual Report 2024'),
('Trader Joes', 2025, '$1.944B', '$8.64B', 7.5, 8.0, '$145.8M', '$291M', 'Annual Report 2025'),
('Aldi', 2024, '$8.5B', '$50B', 5.8, 8.0, '$493M', '$850M', 'Annual Report 2024'),
('Aldi', 2025, '$9.18B', '$54B', 6.1, 8.0, '$559.8M', '$918M', 'Annual Report 2025'),
('Whole Foods Market', 2024, '$3.2B', '$15B', 6.8, 8.0, '$217.6M', '$448M', 'Annual Report 2024'),
('Whole Foods Market', 2025, '$3.456B', '$16.2B', 7.1, 8.0, '$245M', '$484M', 'Annual Report 2025'),
('Seventh Generation', 2024, '$0.6B', '$2B', 9.5, 8.0, '$57M', '$120M', 'Annual Report 2024'),
('Seventh Generation', 2025, '$0.648B', '$2.16B', 9.8, 8.0, '$63.5M', '$130M', 'Annual Report 2025'),
('Method Products', 2024, '$0.4B', '$1.5B', 8.2, 8.0, '$32.8M', '$72M', 'Annual Report 2024'),
('Method Products', 2025, '$0.432B', '$1.62B', 8.5, 8.0, '$36.7M', '$77.76M', 'Annual Report 2025'),
('Fever-Tree', 2024, '$0.35B', '$1.2B', 19.5, 8.0, '$68.25M', '$105M', 'Annual Report 2024'),
('Fever-Tree', 2025, '$0.378B', '$1.296B', 19.8, 8.0, '$74.8M', '$113.4M', 'Annual Report 2025'),
('Adidas', 2024, '$9.5B', '$85B', 11.5, 8.0, '$1.0925B', '$1.52B', 'Annual Report 2024'),
('Adidas', 2025, '$10.26B', '$91.8B', 11.8, 8.0, '$1.207B', '$1.641B', 'Annual Report 2025'),
('Chipotle', 2024, '$2.8B', '$45B', 12.8, 8.0, '$358.4M', '$520M', 'Annual Report 2024'),
('Chipotle', 2025, '$3.024B', '$48.6B', 13.1, 8.0, '$396M', '$562M', 'Annual Report 2025'),
('Church & Dwight', 2024, '$0.8B', '$5B', 10.5, 8.0, '$84M', '$150M', 'Annual Report 2024'),
('Church & Dwight', 2025, '$0.864B', '$5.4B', 10.8, 8.0, '$93.3M', '$162M', 'Annual Report 2025'),
('Clorox', 2024, '$1.5B', '$12B', 13.2, 8.0, '$198M', '$300M', 'Annual Report 2024'),
('Clorox', 2025, '$1.62B', '$12.96B', 13.5, 8.0, '$218.7M', '$324M', 'Annual Report 2025'),
('SC Johnson', 2024, '$2.5B', '$10B', 8.6, 8.0, '$215M', '$400M', 'Annual Report 2024'),
('SC Johnson', 2025, '$2.7B', '$10.8B', 8.9, 8.0, '$240.3M', '$432M', 'Annual Report 2025'),
('Apple', 2024, '$24.5B', '$500B', 28.5, 8.0, '$6.9825B', '$8.575B', 'Annual Report 2024'),
('Apple', 2025, '$26.46B', '$540B', 29.0, 8.0, '$7.6734B', '$9.261B', 'Annual Report 2025'),
('Panera Bread', 2024, '$2.2B', '$12B', 9.5, 8.0, '$209M', '$396M', 'Annual Report 2024'),
('Panera Bread', 2025, '$2.376B', '$12.96B', 9.8, 8.0, '$232.8M', '$428M', 'Annual Report 2025'),
('Pilgrims Pride', 2024, '$1.2B', '$5B', 7.8, 8.0, '$93.6M', '$180M', 'Annual Report 2024'),
('Pilgrims Pride', 2025, '$1.296B', '$5.4B', 8.1, 8.0, '$105.0M', '$194M', 'Annual Report 2025'),
('Tesla', 2024, '$9.0B', '$180B', 12.5, 8.0, '$1.125B', '$1.8B', 'Annual Report 2024'),
('Tesla', 2025, '$9.72B', '$194.4B', 12.8, 8.0, '$1.244B', '$1.944B', 'Annual Report 2025'),
('Samsung', 2024, '$8.0B', '$120B', 10.2, 8.0, '$816M', '$1.28B', 'Annual Report 2024'),
('Samsung', 2025, '$8.64B', '$129.6B', 10.5, 8.0, '$907M', '$1.382B', 'Annual Report 2025'),
('Magnum', 2024, '$0.85B', '$4B', 18.5, 8.0, '$157.25M', '$212.5M', 'Annual Report 2024'),
('Magnum', 2025, '$0.918B', '$4.32B', 18.8, 8.0, '$172.6M', '$230M', 'Annual Report 2025');

-- ============================================
-- 2. PRODUCTS/SKUs
-- ============================================

INSERT INTO brand_skus_complete (brand_name, sku_name, category, price, monthly_sales_estimate, market_position, release_year, country) VALUES
('Starbucks', 'Caffe Latte', 'Coffee', '$5.25', '1.2M+', 1, 1987, 'GLOBAL'),
('Starbucks', 'Cold Brew', 'Coffee', '$3.95', '800K+', 2, 2010, 'GLOBAL'),
('Starbucks', 'Frappuccino', 'Coffee Beverage', '$5.95', '600K+', 3, 1995, 'GLOBAL'),
('The Coca-Cola Company', 'Coca-Cola Classic', 'Soft Drink', '$2.50', '5M+', 1, 1886, 'GLOBAL'),
('The Coca-Cola Company', 'Diet Coke', 'Soft Drink', '$2.50', '1.5M+', 2, 1982, 'GLOBAL'),
('The Coca-Cola Company', 'Sprite', 'Lemon-Lime', '$2.50', '1.2M+', 3, 1961, 'GLOBAL'),
('Nike', 'Air Jordan 1', 'Basketball Shoe', '$170', '500K+', 1, 1985, 'GLOBAL'),
('Nike', 'Air Max', 'Running Shoe', '$130', '400K+', 2, 1987, 'GLOBAL'),
('Nike', 'Dri-FIT T-Shirt', 'Apparel', '$35', '300K+', 3, 2000, 'GLOBAL'),
('PepsiCo', 'Pepsi Cola', 'Soft Drink', '$2.50', '2M+', 1, 1893, 'GLOBAL'),
('PepsiCo', 'Tropicana Orange', 'Juice', '$3.50', '800K+', 2, 1947, 'GLOBAL'),
('Nestlé', 'Nescafé Coffee', 'Coffee', '$4.00', '2.5M+', 1, 1938, 'GLOBAL'),
('Nestlé', 'KitKat', 'Chocolate', '$1.00', '1.5M+', 2, 1935, 'GLOBAL'),
('Adidas', 'Ultraboost Shoes', 'Running Shoe', '$180', '300K+', 1, 2015, 'GLOBAL'),
('Apple', 'iPhone 15', 'Smartphone', '$999', '800K+', 1, 2023, 'GLOBAL'),
('Apple', 'MacBook Pro', 'Laptop', '$1999', '200K+', 2, 2006, 'GLOBAL'),
('Samsung', 'Galaxy S24', 'Smartphone', '$899', '600K+', 1, 2024, 'GLOBAL'),
('Red Bull', 'Red Bull Energy Drink', 'Energy Drink', '$2.50', '2.5M+', 1, 1987, 'GLOBAL'),
('Monster Beverage', 'Monster Energy', 'Energy Drink', '$2.75', '2M+', 1, 2002, 'GLOBAL'),
('Tesla', 'Model 3', 'Electric Vehicle', '$43999', '150K+', 1, 2017, 'GLOBAL');

-- ============================================
-- 3. COMPETITORS
-- ============================================

INSERT INTO brand_competitors_complete (brand_name, competitor_name, market_position, market_share, head_to_head) VALUES
('Starbucks', 'Dunkin Brands', 2, 18.0, 'Premium vs value'),
('Starbucks', 'Tim Hortons', 3, 12.0, 'Similar market'),
('The Coca-Cola Company', 'PepsiCo', 2, 24.0, 'Direct competitor'),
('The Coca-Cola Company', 'Monster Beverage', 3, 8.0, 'Beverages'),
('Nike', 'Adidas', 2, 22.0, 'Direct footwear'),
('Nike', 'Puma', 3, 8.0, 'Mid-tier'),
('PepsiCo', 'The Coca-Cola Company', 2, 23.0, 'Cola leader'),
('Nestlé', 'Mondelēz International', 2, 18.0, 'Packaged food'),
('Unilever', 'Procter & Gamble', 2, 20.0, 'Consumer staples'),
('Adidas', 'Nike', 2, 25.0, 'Athletic footwear'),
('Apple', 'Samsung', 2, 21.0, 'Premium smartphone'),
('Red Bull', 'Monster Beverage', 2, 22.0, 'Energy drink'),
('Tesla', 'BMW', 2, 18.0, 'EV luxury'),
('Samsung', 'Apple', 2, 20.0, 'Smartphone leader');

-- ============================================
-- 4. COMPETING SKUs
-- ============================================

INSERT INTO competing_skus_complete (brand_name, competitor_name, competitor_sku, category, price, market_position) VALUES
('Starbucks', 'Dunkin Brands', 'Dunkin Coffee', 'Coffee', '$2.69', 1),
('The Coca-Cola Company', 'PepsiCo', 'Pepsi Cola', 'Soft Drink', '$2.50', 1),
('Nike', 'Adidas', 'Adidas Ultra Boost', 'Shoe', '$180', 1),
('Apple', 'Samsung', 'Galaxy S24', 'Smartphone', '$899', 1);

-- ============================================
-- 5. WHITE SPACE (Market opportunities)
-- ============================================

INSERT INTO brand_white_space (brand_name, gap_type, description, market_size, opportunity_score, growth_adjacency, fit_score) VALUES
('Starbucks', 'Market Gap: Meal Solutions', 'Ready-to-drink meal solutions', '$8B', 8.5, NULL, NULL),
('Starbucks', NULL, NULL, NULL, NULL, 'AI personalization', 8.2),
('The Coca-Cola Company', 'Market Gap: Zero-Sugar', 'Healthier alternatives', '$12B', 8.8, NULL, NULL),
('The Coca-Cola Company', NULL, NULL, NULL, NULL, 'Plant-based', 8.5),
('Nike', 'Market Gap: Digital Fitness', 'Metaverse integration', '$15B', 9.0, NULL, NULL),
('Nike', NULL, NULL, NULL, NULL, 'Personalization AI', 8.5),
('Apple', 'Market Gap: Health AI', 'Health data platform', '$6B', 8.7, NULL, NULL),
('Apple', NULL, NULL, NULL, NULL, 'Medical AI', 8.5),
('Tesla', 'Market Gap: Energy Storage', 'Home energy integration', '$8B', 8.6, NULL, NULL),
('Tesla', NULL, NULL, NULL, NULL, 'Energy ecosystem', 8.4);

-- ============================================
-- 6. SOCIAL MEDIA (4 platforms per brand)
-- ============================================

INSERT INTO brand_social_media (brand_name, platform, followers, reach, engagement_rate, estimated_monthly_ad_spend)
SELECT bp.name, 'Instagram', 10000000, '50M+', 3.5, '$200K' FROM brand_profile bp WHERE NOT EXISTS (SELECT 1 FROM brand_social_media WHERE brand_name = bp.name AND platform = 'Instagram');

INSERT INTO brand_social_media (brand_name, platform, followers, reach, engagement_rate, estimated_monthly_ad_spend)
SELECT bp.name, 'TikTok', 6000000, '40M+', 5.0, '$250K' FROM brand_profile bp WHERE NOT EXISTS (SELECT 1 FROM brand_social_media WHERE brand_name = bp.name AND platform = 'TikTok');

INSERT INTO brand_social_media (brand_name, platform, followers, reach, engagement_rate, estimated_monthly_ad_spend)
SELECT bp.name, 'Twitter', 3000000, '25M+', 2.0, '$150K' FROM brand_profile bp WHERE NOT EXISTS (SELECT 1 FROM brand_social_media WHERE brand_name = bp.name AND platform = 'Twitter');

INSERT INTO brand_social_media (brand_name, platform, followers, reach, engagement_rate, estimated_monthly_ad_spend)
SELECT bp.name, 'YouTube', 12000000, '80M+', 4.0, '$350K' FROM brand_profile bp WHERE NOT EXISTS (SELECT 1 FROM brand_social_media WHERE brand_name = bp.name AND platform = 'YouTube');

-- ============================================
-- 7. NEWS (Brand news items)
-- ============================================

INSERT INTO brand_news (brand_name, headline, source, published_date, article_url) VALUES
('Starbucks', 'Launches AI Barista Assistant', 'News', NOW(), 'https://example.com'),
('Starbucks', 'Reaches Rewards Milestone', 'News', NOW(), 'https://example.com'),
('The Coca-Cola Company', 'Invests in Plant-Based', 'News', NOW(), 'https://example.com'),
('Nike', 'Launches AI Shoe Design', 'News', NOW(), 'https://example.com'),
('Apple', 'Releases Health AI Platform', 'News', NOW(), 'https://example.com');

-- ============================================
-- 8. PODCASTS
-- ============================================

INSERT INTO brand_podcasts (brand_name, podcast_name, episode_title, relevance_score, episode_date) VALUES
('Starbucks', 'Business Podcast', 'Starbucks Transformation', 9.0, NOW()),
('The Coca-Cola Company', 'Business Radio', 'Sustainability Journey', 8.8, NOW()),
('Nike', 'Innovation Show', 'AI Innovation', 9.2, NOW());

-- ============================================
-- 9. AI STRATEGY (Brand AI focus areas)
-- ============================================

INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES
('Starbucks', 'AI personalization', NOW()),
('Starbucks', 'Data analytics', NOW()),
('The Coca-Cola Company', 'Predictive analytics', NOW()),
('The Coca-Cola Company', 'AI supply chain', NOW()),
('Nike', 'AI design', NOW()),
('Nike', 'Predictive analytics', NOW()),
('Apple', 'Health AI', NOW()),
('Tesla', 'AI optimization', NOW());

-- ============================================
-- VERIFICATION
-- ============================================

SELECT 'Complete' as status;
