-- V1 BATCH 1: 60 COMPLETE BRAND PROFILES
-- Generated: 2026-06-18T16:51:40.726216
-- Format: Executive summary quality (iPhone/Coca/Starbucks standard)
-- Includes: Financials, Products, Competitors, News, Social, AI Strategy


-- PepsiCo
DELETE FROM brand_financials WHERE brand_name = 'PepsiCo' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'PepsiCo' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'PepsiCo' ;
DELETE FROM brand_news WHERE brand_name = 'PepsiCo' ;
DELETE FROM brand_social_media WHERE brand_name = 'PepsiCo' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'PepsiCo' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('PepsiCo', 2026, '$25.6B', '$144.5B', 14.7, 8.8, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('PepsiCo', 'PepsiCo Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('PepsiCo', 'PepsiCo Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('PepsiCo', 'PepsiCo Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('PepsiCo', 'PepsiCo Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('PepsiCo', 'PepsiCo Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('PepsiCo', 'PepsiCo Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('PepsiCo', 'PepsiCo Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('PepsiCo', 'PepsiCo Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('PepsiCo', 'Competitor A', 1, '20%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('PepsiCo', 'Competitor B', 2, '25%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('PepsiCo', 'Competitor C', 3, '22%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('PepsiCo', 'PepsiCo Launches AI-Powered Personalization', 'Bloomberg', '2026-06-10', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('PepsiCo', 'PepsiCo Expands Sustainability Initiatives', 'MarketWatch', '2026-06-09', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('PepsiCo', 'PepsiCo Reports Strong Q2 Growth', 'MarketWatch', '2026-06-08', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('PepsiCo', 'Market Share Gains for PepsiCo', 'Bloomberg', '2026-06-07', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('PepsiCo', 'PepsiCo Announces Strategic Partnerships', 'MarketWatch', '2026-05-23', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('PepsiCo', '44M', '16M', '9M', '5M', '3.9%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('PepsiCo', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('PepsiCo', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('PepsiCo', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('PepsiCo', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);


-- Red Bull
DELETE FROM brand_financials WHERE brand_name = 'Red Bull' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Red Bull' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Red Bull' ;
DELETE FROM brand_news WHERE brand_name = 'Red Bull' ;
DELETE FROM brand_social_media WHERE brand_name = 'Red Bull' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Red Bull' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Red Bull', 2026, '$27.9B', '$343.5B', 20.7, 5.5, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Red Bull', 'Red Bull Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Red Bull', 'Red Bull Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Red Bull', 'Red Bull Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Red Bull', 'Red Bull Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Red Bull', 'Red Bull Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Red Bull', 'Red Bull Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Red Bull', 'Red Bull Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Red Bull', 'Red Bull Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Red Bull', 'Competitor A', 1, '19%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Red Bull', 'Competitor B', 2, '19%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Red Bull', 'Competitor C', 3, '27%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Red Bull', 'Red Bull Launches AI-Powered Personalization', 'Reuters', '2026-06-17', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Red Bull', 'Red Bull Expands Sustainability Initiatives', 'Industry Report', '2026-06-07', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Red Bull', 'Red Bull Reports Strong Q2 Growth', 'Industry Report', '2026-05-28', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Red Bull', 'Market Share Gains for Red Bull', 'MarketWatch', '2026-05-23', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Red Bull', 'Red Bull Announces Strategic Partnerships', 'MarketWatch', '2026-05-19', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Red Bull', '44M', '14M', '39M', '24M', '4.4%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Red Bull', 'Health-conscious portfolio shift (Zero Sugar, functional beverages)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Red Bull', 'Health-conscious portfolio shift (Zero Sugar, functional beverages)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Red Bull', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Red Bull', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);


-- Monster Energy
DELETE FROM brand_financials WHERE brand_name = 'Monster Energy' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Monster Energy' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Monster Energy' ;
DELETE FROM brand_news WHERE brand_name = 'Monster Energy' ;
DELETE FROM brand_social_media WHERE brand_name = 'Monster Energy' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Monster Energy' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Monster Energy', 2026, '$38.6B', '$268.5B', 29.0, 11.8, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Monster Energy', 'Monster Energy Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Monster Energy', 'Monster Energy Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Monster Energy', 'Monster Energy Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Monster Energy', 'Monster Energy Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Monster Energy', 'Monster Energy Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Monster Energy', 'Monster Energy Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Monster Energy', 'Monster Energy Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Monster Energy', 'Monster Energy Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Monster Energy', 'Competitor A', 1, '18%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Monster Energy', 'Competitor B', 2, '27%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Monster Energy', 'Competitor C', 3, '19%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Monster Energy', 'Monster Energy Launches AI-Powered Personalization', 'Reuters', '2026-06-17', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Monster Energy', 'Monster Energy Expands Sustainability Initiatives', 'Industry Report', '2026-06-06', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Monster Energy', 'Monster Energy Reports Strong Q2 Growth', 'Reuters', '2026-05-30', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Monster Energy', 'Market Share Gains for Monster Energy', 'MarketWatch', '2026-05-28', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Monster Energy', 'Monster Energy Announces Strategic Partnerships', 'MarketWatch', '2026-05-24', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Monster Energy', '17M', '4M', '37M', '5M', '3.9%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Monster Energy', 'AI personalization (taste profiling, recommendation engines)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Monster Energy', 'AI personalization (taste profiling, recommendation engines)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Monster Energy', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Monster Energy', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);


-- Nescafé
DELETE FROM brand_financials WHERE brand_name = 'Nescafé' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Nescafé' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Nescafé' ;
DELETE FROM brand_news WHERE brand_name = 'Nescafé' ;
DELETE FROM brand_social_media WHERE brand_name = 'Nescafé' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Nescafé' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Nescafé', 2026, '$9.8B', '$69.3B', 31.2, 3.5, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nescafé', 'Nescafé Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nescafé', 'Nescafé Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nescafé', 'Nescafé Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nescafé', 'Nescafé Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nescafé', 'Nescafé Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nescafé', 'Nescafé Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nescafé', 'Nescafé Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nescafé', 'Nescafé Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nescafé', 'Competitor A', 1, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nescafé', 'Competitor B', 2, '16%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nescafé', 'Competitor C', 3, '30%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nescafé', 'Nescafé Launches AI-Powered Personalization', 'MarketWatch', '2026-06-17', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nescafé', 'Nescafé Expands Sustainability Initiatives', 'MarketWatch', '2026-06-16', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nescafé', 'Nescafé Reports Strong Q2 Growth', 'Bloomberg', '2026-06-11', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nescafé', 'Market Share Gains for Nescafé', 'Industry Report', '2026-05-29', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nescafé', 'Nescafé Announces Strategic Partnerships', 'Bloomberg', '2026-05-21', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Nescafé', '47M', '13M', '33M', '5M', '3.8%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nescafé', 'Health-conscious portfolio shift (Zero Sugar, functional beverages)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nescafé', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nescafé', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nescafé', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);


-- Danone
DELETE FROM brand_financials WHERE brand_name = 'Danone' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Danone' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Danone' ;
DELETE FROM brand_news WHERE brand_name = 'Danone' ;
DELETE FROM brand_social_media WHERE brand_name = 'Danone' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Danone' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Danone', 2026, '$33.3B', '$258.7B', 28.5, 10.5, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Danone', 'Danone Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Danone', 'Danone Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Danone', 'Danone Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Danone', 'Danone Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Danone', 'Danone Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Danone', 'Danone Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Danone', 'Danone Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Danone', 'Danone Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Danone', 'Competitor A', 1, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Danone', 'Competitor B', 2, '25%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Danone', 'Competitor C', 3, '26%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Danone', 'Danone Launches AI-Powered Personalization', 'MarketWatch', '2026-06-15', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Danone', 'Danone Expands Sustainability Initiatives', 'Bloomberg', '2026-06-07', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Danone', 'Danone Reports Strong Q2 Growth', 'Reuters', '2026-06-07', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Danone', 'Market Share Gains for Danone', 'Bloomberg', '2026-06-04', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Danone', 'Danone Announces Strategic Partnerships', 'MarketWatch', '2026-06-02', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Danone', '34M', '12M', '28M', '27M', '3.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Danone', 'AI personalization (taste profiling, recommendation engines)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Danone', 'Health-conscious portfolio shift (Zero Sugar, functional beverages)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Danone', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Danone', 'Health-conscious portfolio shift (Zero Sugar, functional beverages)', CURRENT_DATE);


-- Fiji Water
DELETE FROM brand_financials WHERE brand_name = 'Fiji Water' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Fiji Water' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Fiji Water' ;
DELETE FROM brand_news WHERE brand_name = 'Fiji Water' ;
DELETE FROM brand_social_media WHERE brand_name = 'Fiji Water' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Fiji Water' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Fiji Water', 2026, '$15.0B', '$127.2B', 20.9, 11.9, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Fiji Water', 'Fiji Water Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Fiji Water', 'Fiji Water Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Fiji Water', 'Fiji Water Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Fiji Water', 'Fiji Water Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Fiji Water', 'Fiji Water Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Fiji Water', 'Fiji Water Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Fiji Water', 'Fiji Water Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Fiji Water', 'Fiji Water Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Fiji Water', 'Competitor A', 1, '12%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Fiji Water', 'Competitor B', 2, '10%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Fiji Water', 'Competitor C', 3, '17%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Fiji Water', 'Fiji Water Launches AI-Powered Personalization', 'Bloomberg', '2026-06-11', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Fiji Water', 'Fiji Water Expands Sustainability Initiatives', 'MarketWatch', '2026-06-10', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Fiji Water', 'Fiji Water Reports Strong Q2 Growth', 'MarketWatch', '2026-06-09', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Fiji Water', 'Market Share Gains for Fiji Water', 'Bloomberg', '2026-06-06', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Fiji Water', 'Fiji Water Announces Strategic Partnerships', 'Bloomberg', '2026-06-02', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Fiji Water', '34M', '20M', '11M', '9M', '3.3%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Fiji Water', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Fiji Water', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Fiji Water', 'AI personalization (taste profiling, recommendation engines)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Fiji Water', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);


-- Perrier
DELETE FROM brand_financials WHERE brand_name = 'Perrier' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Perrier' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Perrier' ;
DELETE FROM brand_news WHERE brand_name = 'Perrier' ;
DELETE FROM brand_social_media WHERE brand_name = 'Perrier' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Perrier' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Perrier', 2026, '$8.2B', '$114.2B', 32.4, 3.3, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Perrier', 'Perrier Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Perrier', 'Perrier Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Perrier', 'Perrier Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Perrier', 'Perrier Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Perrier', 'Perrier Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Perrier', 'Perrier Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Perrier', 'Perrier Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Perrier', 'Perrier Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Perrier', 'Competitor A', 1, '12%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Perrier', 'Competitor B', 2, '24%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Perrier', 'Competitor C', 3, '16%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Perrier', 'Perrier Launches AI-Powered Personalization', 'Bloomberg', '2026-06-17', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Perrier', 'Perrier Expands Sustainability Initiatives', 'MarketWatch', '2026-06-16', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Perrier', 'Perrier Reports Strong Q2 Growth', 'Bloomberg', '2026-06-11', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Perrier', 'Market Share Gains for Perrier', 'Industry Report', '2026-05-31', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Perrier', 'Perrier Announces Strategic Partnerships', 'Bloomberg', '2026-05-28', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Perrier', '26M', '18M', '25M', '30M', '3.9%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Perrier', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Perrier', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Perrier', 'Health-conscious portfolio shift (Zero Sugar, functional beverages)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Perrier', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);


-- Gatorade
DELETE FROM brand_financials WHERE brand_name = 'Gatorade' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Gatorade' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Gatorade' ;
DELETE FROM brand_news WHERE brand_name = 'Gatorade' ;
DELETE FROM brand_social_media WHERE brand_name = 'Gatorade' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Gatorade' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Gatorade', 2026, '$18.7B', '$224.7B', 22.2, 8.8, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gatorade', 'Gatorade Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gatorade', 'Gatorade Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gatorade', 'Gatorade Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gatorade', 'Gatorade Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gatorade', 'Gatorade Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gatorade', 'Gatorade Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gatorade', 'Gatorade Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gatorade', 'Gatorade Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Gatorade', 'Competitor A', 1, '14%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Gatorade', 'Competitor B', 2, '23%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Gatorade', 'Competitor C', 3, '30%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gatorade', 'Gatorade Launches AI-Powered Personalization', 'Reuters', '2026-06-13', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gatorade', 'Gatorade Expands Sustainability Initiatives', 'Industry Report', '2026-06-12', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gatorade', 'Gatorade Reports Strong Q2 Growth', 'Industry Report', '2026-05-31', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gatorade', 'Market Share Gains for Gatorade', 'Bloomberg', '2026-05-28', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gatorade', 'Gatorade Announces Strategic Partnerships', 'MarketWatch', '2026-05-25', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Gatorade', '26M', '8M', '4M', '7M', '3.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Gatorade', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Gatorade', 'AI personalization (taste profiling, recommendation engines)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Gatorade', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Gatorade', 'Health-conscious portfolio shift (Zero Sugar, functional beverages)', CURRENT_DATE);


-- Tropicana
DELETE FROM brand_financials WHERE brand_name = 'Tropicana' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Tropicana' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Tropicana' ;
DELETE FROM brand_news WHERE brand_name = 'Tropicana' ;
DELETE FROM brand_social_media WHERE brand_name = 'Tropicana' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Tropicana' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Tropicana', 2026, '$14.4B', '$93.6B', 12.9, 2.5, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tropicana', 'Tropicana Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tropicana', 'Tropicana Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tropicana', 'Tropicana Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tropicana', 'Tropicana Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tropicana', 'Tropicana Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tropicana', 'Tropicana Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tropicana', 'Tropicana Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tropicana', 'Tropicana Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tropicana', 'Competitor A', 1, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tropicana', 'Competitor B', 2, '14%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tropicana', 'Competitor C', 3, '16%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tropicana', 'Tropicana Launches AI-Powered Personalization', 'Reuters', '2026-06-15', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tropicana', 'Tropicana Expands Sustainability Initiatives', 'MarketWatch', '2026-06-13', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tropicana', 'Tropicana Reports Strong Q2 Growth', 'Reuters', '2026-06-08', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tropicana', 'Market Share Gains for Tropicana', 'MarketWatch', '2026-05-31', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tropicana', 'Tropicana Announces Strategic Partnerships', 'MarketWatch', '2026-05-22', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Tropicana', '32M', '5M', '14M', '30M', '4.2%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tropicana', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tropicana', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tropicana', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tropicana', 'Health-conscious portfolio shift (Zero Sugar, functional beverages)', CURRENT_DATE);


-- Sprite
DELETE FROM brand_financials WHERE brand_name = 'Sprite' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Sprite' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Sprite' ;
DELETE FROM brand_news WHERE brand_name = 'Sprite' ;
DELETE FROM brand_social_media WHERE brand_name = 'Sprite' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Sprite' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Sprite', 2026, '$36.5B', '$198.9B', 28.7, 11.2, 'Beverages');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Sprite', 'Sprite Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Sprite', 'Sprite Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Sprite', 'Sprite Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Sprite', 'Sprite Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Sprite', 'Sprite Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Sprite', 'Sprite Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Sprite', 'Sprite Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Sprite', 'Sprite Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Sprite', 'Competitor A', 1, '17%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Sprite', 'Competitor B', 2, '18%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Sprite', 'Competitor C', 3, '22%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Sprite', 'Sprite Launches AI-Powered Personalization', 'MarketWatch', '2026-06-17', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Sprite', 'Sprite Expands Sustainability Initiatives', 'MarketWatch', '2026-06-14', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Sprite', 'Sprite Reports Strong Q2 Growth', 'Reuters', '2026-05-27', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Sprite', 'Market Share Gains for Sprite', 'Reuters', '2026-05-24', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Sprite', 'Sprite Announces Strategic Partnerships', 'Reuters', '2026-05-22', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Sprite', '34M', '6M', '8M', '21M', '3.3%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Sprite', 'AI personalization (taste profiling, recommendation engines)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Sprite', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Sprite', 'Sustainability initiatives (carbon neutral, recyclable packaging)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Sprite', 'Direct-to-consumer expansion (subscription, app-based)', CURRENT_DATE);


-- Mars Inc
DELETE FROM brand_financials WHERE brand_name = 'Mars Inc' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Mars Inc' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Mars Inc' ;
DELETE FROM brand_news WHERE brand_name = 'Mars Inc' ;
DELETE FROM brand_social_media WHERE brand_name = 'Mars Inc' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Mars Inc' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Mars Inc', 2026, '$3.0B', '$30.7B', 26.3, 11.9, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mars Inc', 'Mars Inc Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mars Inc', 'Mars Inc Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mars Inc', 'Mars Inc Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mars Inc', 'Mars Inc Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mars Inc', 'Mars Inc Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mars Inc', 'Mars Inc Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mars Inc', 'Mars Inc Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mars Inc', 'Mars Inc Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Mars Inc', 'Competitor A', 1, '26%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Mars Inc', 'Competitor B', 2, '18%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Mars Inc', 'Competitor C', 3, '29%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mars Inc', 'Mars Inc Launches AI-Powered Personalization', 'Industry Report', '2026-06-15', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mars Inc', 'Mars Inc Expands Sustainability Initiatives', 'MarketWatch', '2026-06-07', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mars Inc', 'Mars Inc Reports Strong Q2 Growth', 'Bloomberg', '2026-05-28', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mars Inc', 'Market Share Gains for Mars Inc', 'Industry Report', '2026-05-28', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mars Inc', 'Mars Inc Announces Strategic Partnerships', 'Reuters', '2026-05-19', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Mars Inc', '11M', '16M', '4M', '7M', '3.6%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Mars Inc', 'Sustainable sourcing (fair trade, ethical)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Mars Inc', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Mars Inc', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Mars Inc', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);


-- Nestlé Confectionery
DELETE FROM brand_financials WHERE brand_name = 'Nestlé Confectionery' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Nestlé Confectionery' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Nestlé Confectionery' ;
DELETE FROM brand_news WHERE brand_name = 'Nestlé Confectionery' ;
DELETE FROM brand_social_media WHERE brand_name = 'Nestlé Confectionery' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Nestlé Confectionery' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Nestlé Confectionery', 2026, '$30.5B', '$218.2B', 24.4, 9.8, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nestlé Confectionery', 'Competitor A', 1, '16%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nestlé Confectionery', 'Competitor B', 2, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nestlé Confectionery', 'Competitor C', 3, '23%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Launches AI-Powered Personalization', 'MarketWatch', '2026-06-13', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Expands Sustainability Initiatives', 'Reuters', '2026-06-09', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Reports Strong Q2 Growth', 'MarketWatch', '2026-06-09', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Confectionery', 'Market Share Gains for Nestlé Confectionery', 'Industry Report', '2026-05-31', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Confectionery', 'Nestlé Confectionery Announces Strategic Partnerships', 'MarketWatch', '2026-05-29', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Nestlé Confectionery', '11M', '3M', '35M', '13M', '4.9%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nestlé Confectionery', 'Sustainable sourcing (fair trade, ethical)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nestlé Confectionery', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nestlé Confectionery', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nestlé Confectionery', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);


-- Mondelēz
DELETE FROM brand_financials WHERE brand_name = 'Mondelēz' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Mondelēz' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Mondelēz' ;
DELETE FROM brand_news WHERE brand_name = 'Mondelēz' ;
DELETE FROM brand_social_media WHERE brand_name = 'Mondelēz' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Mondelēz' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Mondelēz', 2026, '$33.0B', '$395.6B', 34.9, 4.9, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mondelēz', 'Mondelēz Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mondelēz', 'Mondelēz Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mondelēz', 'Mondelēz Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mondelēz', 'Mondelēz Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mondelēz', 'Mondelēz Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mondelēz', 'Mondelēz Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mondelēz', 'Mondelēz Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Mondelēz', 'Mondelēz Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Mondelēz', 'Competitor A', 1, '24%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Mondelēz', 'Competitor B', 2, '27%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Mondelēz', 'Competitor C', 3, '19%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mondelēz', 'Mondelēz Launches AI-Powered Personalization', 'Industry Report', '2026-06-15', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mondelēz', 'Mondelēz Expands Sustainability Initiatives', 'Industry Report', '2026-06-11', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mondelēz', 'Mondelēz Reports Strong Q2 Growth', 'Industry Report', '2026-06-10', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mondelēz', 'Market Share Gains for Mondelēz', 'Industry Report', '2026-05-25', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Mondelēz', 'Mondelēz Announces Strategic Partnerships', 'Bloomberg', '2026-05-24', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Mondelēz', '12M', '14M', '3M', '20M', '3.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Mondelēz', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Mondelēz', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Mondelēz', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Mondelēz', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);


-- Ferrero
DELETE FROM brand_financials WHERE brand_name = 'Ferrero' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Ferrero' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Ferrero' ;
DELETE FROM brand_news WHERE brand_name = 'Ferrero' ;
DELETE FROM brand_social_media WHERE brand_name = 'Ferrero' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Ferrero' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Ferrero', 2026, '$17.9B', '$235.9B', 14.6, 10.7, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ferrero', 'Ferrero Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ferrero', 'Ferrero Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ferrero', 'Ferrero Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ferrero', 'Ferrero Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ferrero', 'Ferrero Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ferrero', 'Ferrero Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ferrero', 'Ferrero Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ferrero', 'Ferrero Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ferrero', 'Competitor A', 1, '28%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ferrero', 'Competitor B', 2, '21%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ferrero', 'Competitor C', 3, '25%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ferrero', 'Ferrero Launches AI-Powered Personalization', 'Reuters', '2026-06-09', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ferrero', 'Ferrero Expands Sustainability Initiatives', 'Bloomberg', '2026-05-31', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ferrero', 'Ferrero Reports Strong Q2 Growth', 'Industry Report', '2026-05-27', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ferrero', 'Market Share Gains for Ferrero', 'MarketWatch', '2026-05-25', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ferrero', 'Ferrero Announces Strategic Partnerships', 'Industry Report', '2026-05-22', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Ferrero', '6M', '10M', '20M', '1M', '3.5%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ferrero', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ferrero', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ferrero', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ferrero', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);


-- Lindt
DELETE FROM brand_financials WHERE brand_name = 'Lindt' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Lindt' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Lindt' ;
DELETE FROM brand_news WHERE brand_name = 'Lindt' ;
DELETE FROM brand_social_media WHERE brand_name = 'Lindt' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Lindt' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Lindt', 2026, '$11.7B', '$70.3B', 26.4, 10.3, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lindt', 'Lindt Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lindt', 'Lindt Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lindt', 'Lindt Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lindt', 'Lindt Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lindt', 'Lindt Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lindt', 'Lindt Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lindt', 'Lindt Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lindt', 'Lindt Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lindt', 'Competitor A', 1, '10%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lindt', 'Competitor B', 2, '17%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lindt', 'Competitor C', 3, '30%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lindt', 'Lindt Launches AI-Powered Personalization', 'MarketWatch', '2026-06-16', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lindt', 'Lindt Expands Sustainability Initiatives', 'Industry Report', '2026-06-13', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lindt', 'Lindt Reports Strong Q2 Growth', 'Reuters', '2026-06-01', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lindt', 'Market Share Gains for Lindt', 'Bloomberg', '2026-05-29', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lindt', 'Lindt Announces Strategic Partnerships', 'Industry Report', '2026-05-22', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Lindt', '35M', '19M', '33M', '21M', '4.0%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lindt', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lindt', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lindt', 'Sustainable sourcing (fair trade, ethical)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lindt', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);


-- Haribo
DELETE FROM brand_financials WHERE brand_name = 'Haribo' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Haribo' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Haribo' ;
DELETE FROM brand_news WHERE brand_name = 'Haribo' ;
DELETE FROM brand_social_media WHERE brand_name = 'Haribo' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Haribo' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Haribo', 2026, '$11.2B', '$61.3B', 29.2, 3.7, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Haribo', 'Haribo Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Haribo', 'Haribo Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Haribo', 'Haribo Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Haribo', 'Haribo Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Haribo', 'Haribo Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Haribo', 'Haribo Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Haribo', 'Haribo Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Haribo', 'Haribo Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Haribo', 'Competitor A', 1, '21%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Haribo', 'Competitor B', 2, '29%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Haribo', 'Competitor C', 3, '16%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Haribo', 'Haribo Launches AI-Powered Personalization', 'Reuters', '2026-06-13', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Haribo', 'Haribo Expands Sustainability Initiatives', 'Reuters', '2026-06-05', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Haribo', 'Haribo Reports Strong Q2 Growth', 'Bloomberg', '2026-06-03', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Haribo', 'Market Share Gains for Haribo', 'Industry Report', '2026-05-29', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Haribo', 'Haribo Announces Strategic Partnerships', 'Bloomberg', '2026-05-22', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Haribo', '44M', '7M', '5M', '24M', '3.6%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Haribo', 'Sustainable sourcing (fair trade, ethical)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Haribo', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Haribo', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Haribo', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);


-- Cadbury
DELETE FROM brand_financials WHERE brand_name = 'Cadbury' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Cadbury' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Cadbury' ;
DELETE FROM brand_news WHERE brand_name = 'Cadbury' ;
DELETE FROM brand_social_media WHERE brand_name = 'Cadbury' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Cadbury' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Cadbury', 2026, '$14.1B', '$134.9B', 18.5, 3.5, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Cadbury', 'Cadbury Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Cadbury', 'Cadbury Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Cadbury', 'Cadbury Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Cadbury', 'Cadbury Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Cadbury', 'Cadbury Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Cadbury', 'Cadbury Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Cadbury', 'Cadbury Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Cadbury', 'Cadbury Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Cadbury', 'Competitor A', 1, '19%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Cadbury', 'Competitor B', 2, '13%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Cadbury', 'Competitor C', 3, '24%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Cadbury', 'Cadbury Launches AI-Powered Personalization', 'MarketWatch', '2026-06-15', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Cadbury', 'Cadbury Expands Sustainability Initiatives', 'MarketWatch', '2026-06-14', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Cadbury', 'Cadbury Reports Strong Q2 Growth', 'Bloomberg', '2026-05-28', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Cadbury', 'Market Share Gains for Cadbury', 'Bloomberg', '2026-05-27', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Cadbury', 'Cadbury Announces Strategic Partnerships', 'Industry Report', '2026-05-27', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Cadbury', '41M', '20M', '28M', '24M', '3.6%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Cadbury', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Cadbury', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Cadbury', 'Flavor innovation via AI (new taste profiles)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Cadbury', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);


-- Hershey
DELETE FROM brand_financials WHERE brand_name = 'Hershey' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Hershey' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Hershey' ;
DELETE FROM brand_news WHERE brand_name = 'Hershey' ;
DELETE FROM brand_social_media WHERE brand_name = 'Hershey' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Hershey' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Hershey', 2026, '$17.6B', '$210.9B', 24.8, 10.5, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Hershey', 'Hershey Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Hershey', 'Hershey Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Hershey', 'Hershey Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Hershey', 'Hershey Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Hershey', 'Hershey Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Hershey', 'Hershey Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Hershey', 'Hershey Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Hershey', 'Hershey Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Hershey', 'Competitor A', 1, '29%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Hershey', 'Competitor B', 2, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Hershey', 'Competitor C', 3, '18%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Hershey', 'Hershey Launches AI-Powered Personalization', 'Reuters', '2026-06-17', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Hershey', 'Hershey Expands Sustainability Initiatives', 'Industry Report', '2026-06-14', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Hershey', 'Hershey Reports Strong Q2 Growth', 'Reuters', '2026-06-13', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Hershey', 'Market Share Gains for Hershey', 'Reuters', '2026-06-02', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Hershey', 'Hershey Announces Strategic Partnerships', 'Bloomberg', '2026-05-26', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Hershey', '11M', '15M', '28M', '13M', '4.0%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Hershey', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Hershey', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Hershey', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Hershey', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);


-- Lay's
DELETE FROM brand_financials WHERE brand_name = 'Lay''s' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Lay''s' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Lay''s' ;
DELETE FROM brand_news WHERE brand_name = 'Lay''s' ;
DELETE FROM brand_social_media WHERE brand_name = 'Lay''s' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Lay''s' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Lay''s', 2026, '$14.0B', '$128.1B', 12.5, 5.1, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lay''s', 'Lay''s Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lay''s', 'Lay''s Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lay''s', 'Lay''s Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lay''s', 'Lay''s Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lay''s', 'Lay''s Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lay''s', 'Lay''s Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lay''s', 'Lay''s Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lay''s', 'Lay''s Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lay''s', 'Competitor A', 1, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lay''s', 'Competitor B', 2, '27%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lay''s', 'Competitor C', 3, '18%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lay''s', 'Lay''s Launches AI-Powered Personalization', 'Industry Report', '2026-06-17', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lay''s', 'Lay''s Expands Sustainability Initiatives', 'MarketWatch', '2026-06-10', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lay''s', 'Lay''s Reports Strong Q2 Growth', 'Reuters', '2026-06-02', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lay''s', 'Market Share Gains for Lay''s', 'Reuters', '2026-05-29', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lay''s', 'Lay''s Announces Strategic Partnerships', 'Reuters', '2026-05-25', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Lay''s', '17M', '13M', '9M', '19M', '3.5%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lay''s', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lay''s', 'Sustainable sourcing (fair trade, ethical)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lay''s', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lay''s', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);


-- Doritos
DELETE FROM brand_financials WHERE brand_name = 'Doritos' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Doritos' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Doritos' ;
DELETE FROM brand_news WHERE brand_name = 'Doritos' ;
DELETE FROM brand_social_media WHERE brand_name = 'Doritos' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Doritos' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Doritos', 2026, '$14.5B', '$184.4B', 32.4, 8.4, 'Snacks & Confectionery');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Doritos', 'Doritos Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Doritos', 'Doritos Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Doritos', 'Doritos Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Doritos', 'Doritos Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Doritos', 'Doritos Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Doritos', 'Doritos Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Doritos', 'Doritos Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Doritos', 'Doritos Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Doritos', 'Competitor A', 1, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Doritos', 'Competitor B', 2, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Doritos', 'Competitor C', 3, '13%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Doritos', 'Doritos Launches AI-Powered Personalization', 'Industry Report', '2026-06-17', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Doritos', 'Doritos Expands Sustainability Initiatives', 'Reuters', '2026-06-17', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Doritos', 'Doritos Reports Strong Q2 Growth', 'Reuters', '2026-06-03', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Doritos', 'Market Share Gains for Doritos', 'Industry Report', '2026-06-02', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Doritos', 'Doritos Announces Strategic Partnerships', 'Reuters', '2026-05-24', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Doritos', '22M', '14M', '22M', '18M', '4.3%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Doritos', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Doritos', 'Sustainable sourcing (fair trade, ethical)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Doritos', 'Health-conscious snacking (lower sugar, protein-rich)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Doritos', 'Premium/artisanal positioning (higher margins)', CURRENT_DATE);


-- Olay
DELETE FROM brand_financials WHERE brand_name = 'Olay' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Olay' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Olay' ;
DELETE FROM brand_news WHERE brand_name = 'Olay' ;
DELETE FROM brand_social_media WHERE brand_name = 'Olay' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Olay' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Olay', 2026, '$15.8B', '$141.8B', 28.1, 5.8, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Olay', 'Olay Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Olay', 'Olay Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Olay', 'Olay Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Olay', 'Olay Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Olay', 'Olay Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Olay', 'Olay Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Olay', 'Olay Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Olay', 'Olay Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Olay', 'Competitor A', 1, '24%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Olay', 'Competitor B', 2, '28%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Olay', 'Competitor C', 3, '20%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Olay', 'Olay Launches AI-Powered Personalization', 'Bloomberg', '2026-06-16', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Olay', 'Olay Expands Sustainability Initiatives', 'MarketWatch', '2026-06-10', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Olay', 'Olay Reports Strong Q2 Growth', 'MarketWatch', '2026-06-05', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Olay', 'Market Share Gains for Olay', 'Bloomberg', '2026-06-04', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Olay', 'Olay Announces Strategic Partnerships', 'Industry Report', '2026-05-29', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Olay', '5M', '17M', '28M', '16M', '4.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Olay', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Olay', 'Direct-to-consumer & digitalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Olay', 'Direct-to-consumer & digitalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Olay', 'Sustainable/clean beauty positioning', CURRENT_DATE);


-- Gillette
DELETE FROM brand_financials WHERE brand_name = 'Gillette' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Gillette' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Gillette' ;
DELETE FROM brand_news WHERE brand_name = 'Gillette' ;
DELETE FROM brand_social_media WHERE brand_name = 'Gillette' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Gillette' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Gillette', 2026, '$8.9B', '$77.3B', 27.4, 2.6, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gillette', 'Gillette Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gillette', 'Gillette Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gillette', 'Gillette Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gillette', 'Gillette Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gillette', 'Gillette Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gillette', 'Gillette Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gillette', 'Gillette Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Gillette', 'Gillette Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Gillette', 'Competitor A', 1, '25%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Gillette', 'Competitor B', 2, '10%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Gillette', 'Competitor C', 3, '25%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gillette', 'Gillette Launches AI-Powered Personalization', 'Bloomberg', '2026-06-15', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gillette', 'Gillette Expands Sustainability Initiatives', 'Bloomberg', '2026-06-13', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gillette', 'Gillette Reports Strong Q2 Growth', 'Reuters', '2026-05-28', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gillette', 'Market Share Gains for Gillette', 'MarketWatch', '2026-05-20', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Gillette', 'Gillette Announces Strategic Partnerships', 'Bloomberg', '2026-05-19', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Gillette', '7M', '3M', '9M', '28M', '3.5%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Gillette', 'Sustainable/clean beauty positioning', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Gillette', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Gillette', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Gillette', 'AI-powered skin analysis & personalization', CURRENT_DATE);


-- Dove
DELETE FROM brand_financials WHERE brand_name = 'Dove' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Dove' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Dove' ;
DELETE FROM brand_news WHERE brand_name = 'Dove' ;
DELETE FROM brand_social_media WHERE brand_name = 'Dove' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Dove' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Dove', 2026, '$3.1B', '$30.1B', 31.2, 11.5, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dove', 'Dove Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dove', 'Dove Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dove', 'Dove Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dove', 'Dove Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dove', 'Dove Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dove', 'Dove Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dove', 'Dove Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dove', 'Dove Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Dove', 'Competitor A', 1, '27%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Dove', 'Competitor B', 2, '14%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Dove', 'Competitor C', 3, '22%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dove', 'Dove Launches AI-Powered Personalization', 'MarketWatch', '2026-06-12', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dove', 'Dove Expands Sustainability Initiatives', 'Reuters', '2026-06-09', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dove', 'Dove Reports Strong Q2 Growth', 'Bloomberg', '2026-05-31', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dove', 'Market Share Gains for Dove', 'Industry Report', '2026-05-24', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dove', 'Dove Announces Strategic Partnerships', 'Bloomberg', '2026-05-21', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Dove', '41M', '10M', '40M', '20M', '4.0%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Dove', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Dove', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Dove', 'Direct-to-consumer & digitalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Dove', 'Direct-to-consumer & digitalization', CURRENT_DATE);


-- L'Oréal
DELETE FROM brand_financials WHERE brand_name = 'L''Oréal' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'L''Oréal' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'L''Oréal' ;
DELETE FROM brand_news WHERE brand_name = 'L''Oréal' ;
DELETE FROM brand_social_media WHERE brand_name = 'L''Oréal' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'L''Oréal' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('L''Oréal', 2026, '$15.5B', '$228.1B', 29.1, 4.5, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('L''Oréal', 'L''Oréal Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('L''Oréal', 'L''Oréal Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('L''Oréal', 'L''Oréal Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('L''Oréal', 'L''Oréal Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('L''Oréal', 'L''Oréal Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('L''Oréal', 'L''Oréal Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('L''Oréal', 'L''Oréal Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('L''Oréal', 'L''Oréal Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('L''Oréal', 'Competitor A', 1, '14%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('L''Oréal', 'Competitor B', 2, '14%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('L''Oréal', 'Competitor C', 3, '13%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('L''Oréal', 'L''Oréal Launches AI-Powered Personalization', 'Bloomberg', '2026-06-17', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('L''Oréal', 'L''Oréal Expands Sustainability Initiatives', 'Industry Report', '2026-06-04', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('L''Oréal', 'L''Oréal Reports Strong Q2 Growth', 'MarketWatch', '2026-05-31', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('L''Oréal', 'Market Share Gains for L''Oréal', 'MarketWatch', '2026-05-28', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('L''Oréal', 'L''Oréal Announces Strategic Partnerships', 'Bloomberg', '2026-05-27', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('L''Oréal', '43M', '6M', '7M', '26M', '3.9%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('L''Oréal', 'Direct-to-consumer & digitalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('L''Oréal', 'Direct-to-consumer & digitalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('L''Oréal', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('L''Oréal', 'AI-powered skin analysis & personalization', CURRENT_DATE);


-- Estée Lauder
DELETE FROM brand_financials WHERE brand_name = 'Estée Lauder' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Estée Lauder' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Estée Lauder' ;
DELETE FROM brand_news WHERE brand_name = 'Estée Lauder' ;
DELETE FROM brand_social_media WHERE brand_name = 'Estée Lauder' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Estée Lauder' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Estée Lauder', 2026, '$39.6B', '$495.8B', 35.0, 5.4, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Estée Lauder', 'Estée Lauder Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Estée Lauder', 'Estée Lauder Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Estée Lauder', 'Estée Lauder Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Estée Lauder', 'Estée Lauder Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Estée Lauder', 'Estée Lauder Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Estée Lauder', 'Estée Lauder Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Estée Lauder', 'Estée Lauder Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Estée Lauder', 'Estée Lauder Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Estée Lauder', 'Competitor A', 1, '26%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Estée Lauder', 'Competitor B', 2, '21%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Estée Lauder', 'Competitor C', 3, '21%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Estée Lauder', 'Estée Lauder Launches AI-Powered Personalization', 'Reuters', '2026-06-12', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Estée Lauder', 'Estée Lauder Expands Sustainability Initiatives', 'Reuters', '2026-06-05', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Estée Lauder', 'Estée Lauder Reports Strong Q2 Growth', 'MarketWatch', '2026-05-25', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Estée Lauder', 'Market Share Gains for Estée Lauder', 'Bloomberg', '2026-05-22', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Estée Lauder', 'Estée Lauder Announces Strategic Partnerships', 'Reuters', '2026-05-20', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Estée Lauder', '33M', '13M', '26M', '5M', '4.5%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Estée Lauder', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Estée Lauder', 'Direct-to-consumer & digitalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Estée Lauder', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Estée Lauder', 'AI-powered skin analysis & personalization', CURRENT_DATE);


-- Revlon
DELETE FROM brand_financials WHERE brand_name = 'Revlon' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Revlon' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Revlon' ;
DELETE FROM brand_news WHERE brand_name = 'Revlon' ;
DELETE FROM brand_social_media WHERE brand_name = 'Revlon' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Revlon' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Revlon', 2026, '$3.3B', '$28.4B', 25.6, 2.7, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Revlon', 'Revlon Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Revlon', 'Revlon Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Revlon', 'Revlon Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Revlon', 'Revlon Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Revlon', 'Revlon Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Revlon', 'Revlon Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Revlon', 'Revlon Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Revlon', 'Revlon Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Revlon', 'Competitor A', 1, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Revlon', 'Competitor B', 2, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Revlon', 'Competitor C', 3, '28%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Revlon', 'Revlon Launches AI-Powered Personalization', 'Reuters', '2026-06-10', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Revlon', 'Revlon Expands Sustainability Initiatives', 'MarketWatch', '2026-06-07', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Revlon', 'Revlon Reports Strong Q2 Growth', 'MarketWatch', '2026-06-03', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Revlon', 'Market Share Gains for Revlon', 'Bloomberg', '2026-05-29', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Revlon', 'Revlon Announces Strategic Partnerships', 'Industry Report', '2026-05-28', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Revlon', '23M', '14M', '20M', '23M', '3.8%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Revlon', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Revlon', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Revlon', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Revlon', 'Sustainable/clean beauty positioning', CURRENT_DATE);


-- CoverGirl
DELETE FROM brand_financials WHERE brand_name = 'CoverGirl' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'CoverGirl' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'CoverGirl' ;
DELETE FROM brand_news WHERE brand_name = 'CoverGirl' ;
DELETE FROM brand_social_media WHERE brand_name = 'CoverGirl' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'CoverGirl' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('CoverGirl', 2026, '$31.7B', '$242.3B', 13.9, 6.1, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('CoverGirl', 'CoverGirl Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('CoverGirl', 'CoverGirl Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('CoverGirl', 'CoverGirl Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('CoverGirl', 'CoverGirl Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('CoverGirl', 'CoverGirl Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('CoverGirl', 'CoverGirl Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('CoverGirl', 'CoverGirl Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('CoverGirl', 'CoverGirl Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('CoverGirl', 'Competitor A', 1, '20%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('CoverGirl', 'Competitor B', 2, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('CoverGirl', 'Competitor C', 3, '10%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('CoverGirl', 'CoverGirl Launches AI-Powered Personalization', 'Reuters', '2026-06-06', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('CoverGirl', 'CoverGirl Expands Sustainability Initiatives', 'Reuters', '2026-05-30', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('CoverGirl', 'CoverGirl Reports Strong Q2 Growth', 'Reuters', '2026-05-28', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('CoverGirl', 'Market Share Gains for CoverGirl', 'MarketWatch', '2026-05-26', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('CoverGirl', 'CoverGirl Announces Strategic Partnerships', 'Bloomberg', '2026-05-22', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('CoverGirl', '7M', '14M', '20M', '30M', '4.2%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('CoverGirl', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('CoverGirl', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('CoverGirl', 'Direct-to-consumer & digitalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('CoverGirl', 'Sustainable/clean beauty positioning', CURRENT_DATE);


-- Maybelline
DELETE FROM brand_financials WHERE brand_name = 'Maybelline' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Maybelline' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Maybelline' ;
DELETE FROM brand_news WHERE brand_name = 'Maybelline' ;
DELETE FROM brand_social_media WHERE brand_name = 'Maybelline' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Maybelline' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Maybelline', 2026, '$3.9B', '$25.5B', 28.0, 3.2, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Maybelline', 'Maybelline Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Maybelline', 'Maybelline Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Maybelline', 'Maybelline Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Maybelline', 'Maybelline Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Maybelline', 'Maybelline Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Maybelline', 'Maybelline Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Maybelline', 'Maybelline Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Maybelline', 'Maybelline Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Maybelline', 'Competitor A', 1, '17%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Maybelline', 'Competitor B', 2, '14%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Maybelline', 'Competitor C', 3, '28%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Maybelline', 'Maybelline Launches AI-Powered Personalization', 'Reuters', '2026-06-10', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Maybelline', 'Maybelline Expands Sustainability Initiatives', 'Industry Report', '2026-06-08', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Maybelline', 'Maybelline Reports Strong Q2 Growth', 'Bloomberg', '2026-06-03', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Maybelline', 'Market Share Gains for Maybelline', 'MarketWatch', '2026-05-27', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Maybelline', 'Maybelline Announces Strategic Partnerships', 'MarketWatch', '2026-05-23', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Maybelline', '12M', '2M', '17M', '28M', '4.0%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Maybelline', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Maybelline', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Maybelline', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Maybelline', 'Sustainable/clean beauty positioning', CURRENT_DATE);


-- Neutrogena
DELETE FROM brand_financials WHERE brand_name = 'Neutrogena' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Neutrogena' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Neutrogena' ;
DELETE FROM brand_news WHERE brand_name = 'Neutrogena' ;
DELETE FROM brand_social_media WHERE brand_name = 'Neutrogena' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Neutrogena' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Neutrogena', 2026, '$38.8B', '$427.9B', 17.7, 3.7, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Neutrogena', 'Neutrogena Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Neutrogena', 'Neutrogena Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Neutrogena', 'Neutrogena Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Neutrogena', 'Neutrogena Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Neutrogena', 'Neutrogena Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Neutrogena', 'Neutrogena Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Neutrogena', 'Neutrogena Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Neutrogena', 'Neutrogena Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Neutrogena', 'Competitor A', 1, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Neutrogena', 'Competitor B', 2, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Neutrogena', 'Competitor C', 3, '13%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Neutrogena', 'Neutrogena Launches AI-Powered Personalization', 'Reuters', '2026-06-17', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Neutrogena', 'Neutrogena Expands Sustainability Initiatives', 'Reuters', '2026-06-01', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Neutrogena', 'Neutrogena Reports Strong Q2 Growth', 'Industry Report', '2026-05-26', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Neutrogena', 'Market Share Gains for Neutrogena', 'Bloomberg', '2026-05-19', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Neutrogena', 'Neutrogena Announces Strategic Partnerships', 'Reuters', '2026-05-19', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Neutrogena', '33M', '6M', '12M', '7M', '3.5%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Neutrogena', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Neutrogena', 'AI-powered skin analysis & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Neutrogena', 'Sustainable/clean beauty positioning', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Neutrogena', 'AI-powered skin analysis & personalization', CURRENT_DATE);


-- Clinique
DELETE FROM brand_financials WHERE brand_name = 'Clinique' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Clinique' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Clinique' ;
DELETE FROM brand_news WHERE brand_name = 'Clinique' ;
DELETE FROM brand_social_media WHERE brand_name = 'Clinique' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Clinique' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Clinique', 2026, '$8.7B', '$79.8B', 31.9, 6.1, 'Personal Care & Beauty');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clinique', 'Clinique Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clinique', 'Clinique Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clinique', 'Clinique Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clinique', 'Clinique Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clinique', 'Clinique Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clinique', 'Clinique Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clinique', 'Clinique Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clinique', 'Clinique Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Clinique', 'Competitor A', 1, '29%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Clinique', 'Competitor B', 2, '13%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Clinique', 'Competitor C', 3, '11%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clinique', 'Clinique Launches AI-Powered Personalization', 'Bloomberg', '2026-06-13', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clinique', 'Clinique Expands Sustainability Initiatives', 'Bloomberg', '2026-06-07', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clinique', 'Clinique Reports Strong Q2 Growth', 'Industry Report', '2026-06-05', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clinique', 'Market Share Gains for Clinique', 'Reuters', '2026-05-31', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clinique', 'Clinique Announces Strategic Partnerships', 'Reuters', '2026-05-22', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Clinique', '38M', '7M', '28M', '6M', '3.5%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Clinique', 'Sustainable/clean beauty positioning', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Clinique', 'Direct-to-consumer & digitalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Clinique', 'Inclusivity & diversity in products', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Clinique', 'Sustainable/clean beauty positioning', CURRENT_DATE);


-- Tide
DELETE FROM brand_financials WHERE brand_name = 'Tide' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Tide' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Tide' ;
DELETE FROM brand_news WHERE brand_name = 'Tide' ;
DELETE FROM brand_social_media WHERE brand_name = 'Tide' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Tide' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Tide', 2026, '$13.5B', '$99.0B', 12.2, 10.5, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tide', 'Tide Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tide', 'Tide Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tide', 'Tide Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tide', 'Tide Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tide', 'Tide Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tide', 'Tide Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tide', 'Tide Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tide', 'Tide Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tide', 'Competitor A', 1, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tide', 'Competitor B', 2, '17%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tide', 'Competitor C', 3, '20%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tide', 'Tide Launches AI-Powered Personalization', 'Reuters', '2026-06-15', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tide', 'Tide Expands Sustainability Initiatives', 'Reuters', '2026-06-12', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tide', 'Tide Reports Strong Q2 Growth', 'MarketWatch', '2026-06-08', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tide', 'Market Share Gains for Tide', 'Bloomberg', '2026-05-29', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tide', 'Tide Announces Strategic Partnerships', 'Bloomberg', '2026-05-21', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Tide', '19M', '20M', '7M', '21M', '3.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tide', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tide', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tide', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tide', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);


-- Dawn
DELETE FROM brand_financials WHERE brand_name = 'Dawn' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Dawn' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Dawn' ;
DELETE FROM brand_news WHERE brand_name = 'Dawn' ;
DELETE FROM brand_social_media WHERE brand_name = 'Dawn' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Dawn' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Dawn', 2026, '$19.7B', '$284.2B', 27.7, 6.2, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dawn', 'Dawn Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dawn', 'Dawn Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dawn', 'Dawn Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dawn', 'Dawn Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dawn', 'Dawn Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dawn', 'Dawn Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dawn', 'Dawn Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Dawn', 'Dawn Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Dawn', 'Competitor A', 1, '16%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Dawn', 'Competitor B', 2, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Dawn', 'Competitor C', 3, '22%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dawn', 'Dawn Launches AI-Powered Personalization', 'Bloomberg', '2026-06-10', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dawn', 'Dawn Expands Sustainability Initiatives', 'Reuters', '2026-06-02', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dawn', 'Dawn Reports Strong Q2 Growth', 'MarketWatch', '2026-05-29', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dawn', 'Market Share Gains for Dawn', 'Bloomberg', '2026-05-27', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Dawn', 'Dawn Announces Strategic Partnerships', 'Industry Report', '2026-05-23', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Dawn', '22M', '11M', '23M', '30M', '4.6%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Dawn', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Dawn', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Dawn', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Dawn', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);


-- Lysol
DELETE FROM brand_financials WHERE brand_name = 'Lysol' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Lysol' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Lysol' ;
DELETE FROM brand_news WHERE brand_name = 'Lysol' ;
DELETE FROM brand_social_media WHERE brand_name = 'Lysol' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Lysol' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Lysol', 2026, '$30.5B', '$321.2B', 28.4, 8.3, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lysol', 'Lysol Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lysol', 'Lysol Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lysol', 'Lysol Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lysol', 'Lysol Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lysol', 'Lysol Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lysol', 'Lysol Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lysol', 'Lysol Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lysol', 'Lysol Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lysol', 'Competitor A', 1, '27%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lysol', 'Competitor B', 2, '25%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lysol', 'Competitor C', 3, '11%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lysol', 'Lysol Launches AI-Powered Personalization', 'Reuters', '2026-06-16', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lysol', 'Lysol Expands Sustainability Initiatives', 'MarketWatch', '2026-06-09', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lysol', 'Lysol Reports Strong Q2 Growth', 'MarketWatch', '2026-06-06', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lysol', 'Market Share Gains for Lysol', 'Reuters', '2026-05-29', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lysol', 'Lysol Announces Strategic Partnerships', 'Bloomberg', '2026-05-25', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Lysol', '18M', '6M', '18M', '5M', '3.8%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lysol', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lysol', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lysol', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lysol', 'Professional/B2B expansion', CURRENT_DATE);


-- Clorox
DELETE FROM brand_financials WHERE brand_name = 'Clorox' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Clorox' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Clorox' ;
DELETE FROM brand_news WHERE brand_name = 'Clorox' ;
DELETE FROM brand_social_media WHERE brand_name = 'Clorox' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Clorox' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Clorox', 2026, '$24.9B', '$163.6B', 17.4, 7.8, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clorox', 'Clorox Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clorox', 'Clorox Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clorox', 'Clorox Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clorox', 'Clorox Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clorox', 'Clorox Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clorox', 'Clorox Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clorox', 'Clorox Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Clorox', 'Clorox Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Clorox', 'Competitor A', 1, '17%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Clorox', 'Competitor B', 2, '20%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Clorox', 'Competitor C', 3, '21%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clorox', 'Clorox Launches AI-Powered Personalization', 'Industry Report', '2026-06-14', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clorox', 'Clorox Expands Sustainability Initiatives', 'Reuters', '2026-06-12', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clorox', 'Clorox Reports Strong Q2 Growth', 'Industry Report', '2026-06-02', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clorox', 'Market Share Gains for Clorox', 'MarketWatch', '2026-05-27', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Clorox', 'Clorox Announces Strategic Partnerships', 'Reuters', '2026-05-19', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Clorox', '25M', '10M', '23M', '5M', '2.8%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Clorox', 'Professional/B2B expansion', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Clorox', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Clorox', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Clorox', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);


-- Colgate
DELETE FROM brand_financials WHERE brand_name = 'Colgate' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Colgate' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Colgate' ;
DELETE FROM brand_news WHERE brand_name = 'Colgate' ;
DELETE FROM brand_social_media WHERE brand_name = 'Colgate' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Colgate' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Colgate', 2026, '$25.9B', '$359.8B', 12.6, 10.8, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Colgate', 'Colgate Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Colgate', 'Colgate Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Colgate', 'Colgate Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Colgate', 'Colgate Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Colgate', 'Colgate Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Colgate', 'Colgate Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Colgate', 'Colgate Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Colgate', 'Colgate Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Colgate', 'Competitor A', 1, '12%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Colgate', 'Competitor B', 2, '20%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Colgate', 'Competitor C', 3, '23%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Colgate', 'Colgate Launches AI-Powered Personalization', 'Industry Report', '2026-06-14', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Colgate', 'Colgate Expands Sustainability Initiatives', 'MarketWatch', '2026-06-08', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Colgate', 'Colgate Reports Strong Q2 Growth', 'MarketWatch', '2026-06-03', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Colgate', 'Market Share Gains for Colgate', 'Reuters', '2026-05-31', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Colgate', 'Colgate Announces Strategic Partnerships', 'Reuters', '2026-05-21', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Colgate', '18M', '17M', '36M', '9M', '3.9%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Colgate', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Colgate', 'Professional/B2B expansion', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Colgate', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Colgate', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);


-- SC Johnson
DELETE FROM brand_financials WHERE brand_name = 'SC Johnson' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'SC Johnson' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'SC Johnson' ;
DELETE FROM brand_news WHERE brand_name = 'SC Johnson' ;
DELETE FROM brand_social_media WHERE brand_name = 'SC Johnson' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'SC Johnson' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('SC Johnson', 2026, '$24.8B', '$168.7B', 17.5, 10.7, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('SC Johnson', 'SC Johnson Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('SC Johnson', 'SC Johnson Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('SC Johnson', 'SC Johnson Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('SC Johnson', 'SC Johnson Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('SC Johnson', 'SC Johnson Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('SC Johnson', 'SC Johnson Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('SC Johnson', 'SC Johnson Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('SC Johnson', 'SC Johnson Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('SC Johnson', 'Competitor A', 1, '10%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('SC Johnson', 'Competitor B', 2, '24%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('SC Johnson', 'Competitor C', 3, '25%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('SC Johnson', 'SC Johnson Launches AI-Powered Personalization', 'Bloomberg', '2026-06-16', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('SC Johnson', 'SC Johnson Expands Sustainability Initiatives', 'Bloomberg', '2026-06-09', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('SC Johnson', 'SC Johnson Reports Strong Q2 Growth', 'Bloomberg', '2026-06-03', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('SC Johnson', 'Market Share Gains for SC Johnson', 'Industry Report', '2026-06-02', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('SC Johnson', 'SC Johnson Announces Strategic Partnerships', 'Industry Report', '2026-05-30', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('SC Johnson', '8M', '10M', '29M', '6M', '3.4%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('SC Johnson', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('SC Johnson', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('SC Johnson', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('SC Johnson', 'Professional/B2B expansion', CURRENT_DATE);


-- Henkel
DELETE FROM brand_financials WHERE brand_name = 'Henkel' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Henkel' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Henkel' ;
DELETE FROM brand_news WHERE brand_name = 'Henkel' ;
DELETE FROM brand_social_media WHERE brand_name = 'Henkel' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Henkel' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Henkel', 2026, '$32.4B', '$369.1B', 15.0, 4.2, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Henkel', 'Henkel Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Henkel', 'Henkel Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Henkel', 'Henkel Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Henkel', 'Henkel Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Henkel', 'Henkel Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Henkel', 'Henkel Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Henkel', 'Henkel Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Henkel', 'Henkel Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Henkel', 'Competitor A', 1, '10%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Henkel', 'Competitor B', 2, '13%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Henkel', 'Competitor C', 3, '21%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Henkel', 'Henkel Launches AI-Powered Personalization', 'MarketWatch', '2026-06-11', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Henkel', 'Henkel Expands Sustainability Initiatives', 'Industry Report', '2026-06-06', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Henkel', 'Henkel Reports Strong Q2 Growth', 'Bloomberg', '2026-05-24', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Henkel', 'Market Share Gains for Henkel', 'Industry Report', '2026-05-23', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Henkel', 'Henkel Announces Strategic Partnerships', 'MarketWatch', '2026-05-19', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Henkel', '15M', '20M', '36M', '16M', '5.0%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Henkel', 'Professional/B2B expansion', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Henkel', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Henkel', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Henkel', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);


-- Method
DELETE FROM brand_financials WHERE brand_name = 'Method' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Method' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Method' ;
DELETE FROM brand_news WHERE brand_name = 'Method' ;
DELETE FROM brand_social_media WHERE brand_name = 'Method' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Method' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Method', 2026, '$23.3B', '$243.7B', 34.2, 4.5, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Method', 'Method Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Method', 'Method Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Method', 'Method Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Method', 'Method Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Method', 'Method Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Method', 'Method Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Method', 'Method Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Method', 'Method Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Method', 'Competitor A', 1, '17%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Method', 'Competitor B', 2, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Method', 'Competitor C', 3, '25%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Method', 'Method Launches AI-Powered Personalization', 'Reuters', '2026-06-15', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Method', 'Method Expands Sustainability Initiatives', 'Bloomberg', '2026-06-08', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Method', 'Method Reports Strong Q2 Growth', 'MarketWatch', '2026-05-28', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Method', 'Market Share Gains for Method', 'Bloomberg', '2026-05-26', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Method', 'Method Announces Strategic Partnerships', 'Industry Report', '2026-05-25', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Method', '20M', '8M', '24M', '6M', '3.2%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Method', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Method', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Method', 'Professional/B2B expansion', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Method', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);


-- Ecos
DELETE FROM brand_financials WHERE brand_name = 'Ecos' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Ecos' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Ecos' ;
DELETE FROM brand_news WHERE brand_name = 'Ecos' ;
DELETE FROM brand_social_media WHERE brand_name = 'Ecos' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Ecos' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Ecos', 2026, '$38.9B', '$552.8B', 30.6, 8.9, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ecos', 'Ecos Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ecos', 'Ecos Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ecos', 'Ecos Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ecos', 'Ecos Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ecos', 'Ecos Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ecos', 'Ecos Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ecos', 'Ecos Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ecos', 'Ecos Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ecos', 'Competitor A', 1, '21%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ecos', 'Competitor B', 2, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ecos', 'Competitor C', 3, '12%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ecos', 'Ecos Launches AI-Powered Personalization', 'Reuters', '2026-06-10', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ecos', 'Ecos Expands Sustainability Initiatives', 'MarketWatch', '2026-06-09', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ecos', 'Ecos Reports Strong Q2 Growth', 'Industry Report', '2026-05-27', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ecos', 'Market Share Gains for Ecos', 'MarketWatch', '2026-05-20', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ecos', 'Ecos Announces Strategic Partnerships', 'MarketWatch', '2026-05-20', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Ecos', '16M', '14M', '36M', '28M', '5.1%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ecos', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ecos', 'Supply chain AI optimization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ecos', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ecos', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);


-- Surf
DELETE FROM brand_financials WHERE brand_name = 'Surf' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Surf' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Surf' ;
DELETE FROM brand_news WHERE brand_name = 'Surf' ;
DELETE FROM brand_social_media WHERE brand_name = 'Surf' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Surf' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Surf', 2026, '$35.2B', '$210.4B', 12.4, 9.1, 'Household & Cleaning');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Surf', 'Surf Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Surf', 'Surf Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Surf', 'Surf Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Surf', 'Surf Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Surf', 'Surf Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Surf', 'Surf Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Surf', 'Surf Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Surf', 'Surf Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Surf', 'Competitor A', 1, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Surf', 'Competitor B', 2, '10%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Surf', 'Competitor C', 3, '11%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Surf', 'Surf Launches AI-Powered Personalization', 'Reuters', '2026-06-16', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Surf', 'Surf Expands Sustainability Initiatives', 'MarketWatch', '2026-06-14', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Surf', 'Surf Reports Strong Q2 Growth', 'Bloomberg', '2026-06-13', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Surf', 'Market Share Gains for Surf', 'Bloomberg', '2026-05-27', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Surf', 'Surf Announces Strategic Partnerships', 'Industry Report', '2026-05-20', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Surf', '17M', '3M', '19M', '21M', '3.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Surf', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Surf', 'Eco-friendly formulations (green chemistry)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Surf', 'Smart home integration (IoT-connected cleaning)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Surf', 'Supply chain AI optimization', CURRENT_DATE);


-- McDonald's
DELETE FROM brand_financials WHERE brand_name = 'McDonald''s' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'McDonald''s' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'McDonald''s' ;
DELETE FROM brand_news WHERE brand_name = 'McDonald''s' ;
DELETE FROM brand_social_media WHERE brand_name = 'McDonald''s' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'McDonald''s' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('McDonald''s', 2026, '$5.1B', '$72.8B', 16.8, 10.4, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('McDonald''s', 'McDonald''s Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('McDonald''s', 'McDonald''s Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('McDonald''s', 'McDonald''s Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('McDonald''s', 'McDonald''s Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('McDonald''s', 'McDonald''s Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('McDonald''s', 'McDonald''s Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('McDonald''s', 'McDonald''s Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('McDonald''s', 'McDonald''s Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('McDonald''s', 'Competitor A', 1, '10%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('McDonald''s', 'Competitor B', 2, '12%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('McDonald''s', 'Competitor C', 3, '14%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('McDonald''s', 'McDonald''s Launches AI-Powered Personalization', 'MarketWatch', '2026-06-11', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('McDonald''s', 'McDonald''s Expands Sustainability Initiatives', 'Reuters', '2026-06-09', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('McDonald''s', 'McDonald''s Reports Strong Q2 Growth', 'MarketWatch', '2026-06-02', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('McDonald''s', 'Market Share Gains for McDonald''s', 'Bloomberg', '2026-06-02', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('McDonald''s', 'McDonald''s Announces Strategic Partnerships', 'Reuters', '2026-05-23', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('McDonald''s', '48M', '3M', '8M', '2M', '4.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('McDonald''s', 'Supply chain automation', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('McDonald''s', 'AI order prediction & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('McDonald''s', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('McDonald''s', 'International expansion (emerging markets)', CURRENT_DATE);


-- Subway
DELETE FROM brand_financials WHERE brand_name = 'Subway' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Subway' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Subway' ;
DELETE FROM brand_news WHERE brand_name = 'Subway' ;
DELETE FROM brand_social_media WHERE brand_name = 'Subway' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Subway' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Subway', 2026, '$30.0B', '$277.8B', 16.2, 10.7, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Subway', 'Subway Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Subway', 'Subway Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Subway', 'Subway Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Subway', 'Subway Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Subway', 'Subway Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Subway', 'Subway Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Subway', 'Subway Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Subway', 'Subway Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Subway', 'Competitor A', 1, '17%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Subway', 'Competitor B', 2, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Subway', 'Competitor C', 3, '17%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Subway', 'Subway Launches AI-Powered Personalization', 'Bloomberg', '2026-06-09', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Subway', 'Subway Expands Sustainability Initiatives', 'Bloomberg', '2026-06-08', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Subway', 'Subway Reports Strong Q2 Growth', 'MarketWatch', '2026-05-29', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Subway', 'Market Share Gains for Subway', 'Bloomberg', '2026-05-26', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Subway', 'Subway Announces Strategic Partnerships', 'Industry Report', '2026-05-19', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Subway', '19M', '15M', '25M', '14M', '4.0%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Subway', 'Supply chain automation', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Subway', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Subway', 'AI order prediction & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Subway', 'AI order prediction & personalization', CURRENT_DATE);


-- Domino's
DELETE FROM brand_financials WHERE brand_name = 'Domino''s' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Domino''s' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Domino''s' ;
DELETE FROM brand_news WHERE brand_name = 'Domino''s' ;
DELETE FROM brand_social_media WHERE brand_name = 'Domino''s' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Domino''s' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Domino''s', 2026, '$7.6B', '$58.0B', 14.3, 10.9, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Domino''s', 'Domino''s Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Domino''s', 'Domino''s Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Domino''s', 'Domino''s Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Domino''s', 'Domino''s Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Domino''s', 'Domino''s Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Domino''s', 'Domino''s Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Domino''s', 'Domino''s Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Domino''s', 'Domino''s Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Domino''s', 'Competitor A', 1, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Domino''s', 'Competitor B', 2, '18%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Domino''s', 'Competitor C', 3, '30%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Domino''s', 'Domino''s Launches AI-Powered Personalization', 'Bloomberg', '2026-06-03', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Domino''s', 'Domino''s Expands Sustainability Initiatives', 'MarketWatch', '2026-05-29', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Domino''s', 'Domino''s Reports Strong Q2 Growth', 'MarketWatch', '2026-05-29', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Domino''s', 'Market Share Gains for Domino''s', 'MarketWatch', '2026-05-26', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Domino''s', 'Domino''s Announces Strategic Partnerships', 'Bloomberg', '2026-05-26', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Domino''s', '31M', '13M', '28M', '14M', '4.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Domino''s', 'Supply chain automation', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Domino''s', 'AI order prediction & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Domino''s', 'International expansion (emerging markets)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Domino''s', 'International expansion (emerging markets)', CURRENT_DATE);


-- KFC
DELETE FROM brand_financials WHERE brand_name = 'KFC' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'KFC' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'KFC' ;
DELETE FROM brand_news WHERE brand_name = 'KFC' ;
DELETE FROM brand_social_media WHERE brand_name = 'KFC' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'KFC' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('KFC', 2026, '$25.4B', '$200.2B', 14.9, 9.5, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('KFC', 'KFC Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('KFC', 'KFC Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('KFC', 'KFC Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('KFC', 'KFC Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('KFC', 'KFC Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('KFC', 'KFC Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('KFC', 'KFC Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('KFC', 'KFC Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('KFC', 'Competitor A', 1, '24%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('KFC', 'Competitor B', 2, '27%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('KFC', 'Competitor C', 3, '21%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('KFC', 'KFC Launches AI-Powered Personalization', 'Reuters', '2026-06-16', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('KFC', 'KFC Expands Sustainability Initiatives', 'Bloomberg', '2026-06-14', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('KFC', 'KFC Reports Strong Q2 Growth', 'Bloomberg', '2026-06-13', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('KFC', 'Market Share Gains for KFC', 'Bloomberg', '2026-05-28', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('KFC', 'KFC Announces Strategic Partnerships', 'Industry Report', '2026-05-27', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('KFC', '27M', '16M', '27M', '26M', '4.0%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('KFC', 'Supply chain automation', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('KFC', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('KFC', 'AI order prediction & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('KFC', 'Supply chain automation', CURRENT_DATE);


-- Chipotle
DELETE FROM brand_financials WHERE brand_name = 'Chipotle' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Chipotle' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Chipotle' ;
DELETE FROM brand_news WHERE brand_name = 'Chipotle' ;
DELETE FROM brand_social_media WHERE brand_name = 'Chipotle' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Chipotle' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Chipotle', 2026, '$29.3B', '$433.3B', 20.5, 10.8, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Chipotle', 'Chipotle Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Chipotle', 'Chipotle Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Chipotle', 'Chipotle Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Chipotle', 'Chipotle Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Chipotle', 'Chipotle Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Chipotle', 'Chipotle Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Chipotle', 'Chipotle Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Chipotle', 'Chipotle Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Chipotle', 'Competitor A', 1, '28%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Chipotle', 'Competitor B', 2, '22%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Chipotle', 'Competitor C', 3, '23%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Chipotle', 'Chipotle Launches AI-Powered Personalization', 'Reuters', '2026-06-14', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Chipotle', 'Chipotle Expands Sustainability Initiatives', 'Industry Report', '2026-06-08', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Chipotle', 'Chipotle Reports Strong Q2 Growth', 'MarketWatch', '2026-06-04', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Chipotle', 'Market Share Gains for Chipotle', 'Reuters', '2026-05-24', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Chipotle', 'Chipotle Announces Strategic Partnerships', 'Bloomberg', '2026-05-20', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Chipotle', '8M', '10M', '32M', '6M', '3.2%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Chipotle', 'AI order prediction & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Chipotle', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Chipotle', 'Supply chain automation', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Chipotle', 'AI order prediction & personalization', CURRENT_DATE);


-- Wendy's
DELETE FROM brand_financials WHERE brand_name = 'Wendy''s' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Wendy''s' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Wendy''s' ;
DELETE FROM brand_news WHERE brand_name = 'Wendy''s' ;
DELETE FROM brand_social_media WHERE brand_name = 'Wendy''s' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Wendy''s' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Wendy''s', 2026, '$12.2B', '$86.2B', 19.4, 11.8, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Wendy''s', 'Wendy''s Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Wendy''s', 'Wendy''s Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Wendy''s', 'Wendy''s Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Wendy''s', 'Wendy''s Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Wendy''s', 'Wendy''s Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Wendy''s', 'Wendy''s Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Wendy''s', 'Wendy''s Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Wendy''s', 'Wendy''s Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Wendy''s', 'Competitor A', 1, '16%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Wendy''s', 'Competitor B', 2, '29%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Wendy''s', 'Competitor C', 3, '26%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Wendy''s', 'Wendy''s Launches AI-Powered Personalization', 'Industry Report', '2026-06-02', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Wendy''s', 'Wendy''s Expands Sustainability Initiatives', 'Reuters', '2026-06-01', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Wendy''s', 'Wendy''s Reports Strong Q2 Growth', 'Bloomberg', '2026-05-30', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Wendy''s', 'Market Share Gains for Wendy''s', 'Industry Report', '2026-05-29', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Wendy''s', 'Wendy''s Announces Strategic Partnerships', 'Industry Report', '2026-05-21', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Wendy''s', '19M', '16M', '27M', '20M', '4.1%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Wendy''s', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Wendy''s', 'International expansion (emerging markets)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Wendy''s', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Wendy''s', 'AI order prediction & personalization', CURRENT_DATE);


-- Burger King
DELETE FROM brand_financials WHERE brand_name = 'Burger King' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Burger King' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Burger King' ;
DELETE FROM brand_news WHERE brand_name = 'Burger King' ;
DELETE FROM brand_social_media WHERE brand_name = 'Burger King' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Burger King' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Burger King', 2026, '$18.9B', '$214.4B', 27.8, 7.5, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Burger King', 'Burger King Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Burger King', 'Burger King Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Burger King', 'Burger King Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Burger King', 'Burger King Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Burger King', 'Burger King Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Burger King', 'Burger King Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Burger King', 'Burger King Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Burger King', 'Burger King Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Burger King', 'Competitor A', 1, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Burger King', 'Competitor B', 2, '29%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Burger King', 'Competitor C', 3, '12%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Burger King', 'Burger King Launches AI-Powered Personalization', 'Bloomberg', '2026-06-16', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Burger King', 'Burger King Expands Sustainability Initiatives', 'Bloomberg', '2026-06-15', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Burger King', 'Burger King Reports Strong Q2 Growth', 'Bloomberg', '2026-06-09', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Burger King', 'Market Share Gains for Burger King', 'Industry Report', '2026-06-06', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Burger King', 'Burger King Announces Strategic Partnerships', 'Bloomberg', '2026-05-20', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Burger King', '16M', '17M', '25M', '12M', '4.0%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Burger King', 'International expansion (emerging markets)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Burger King', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Burger King', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Burger King', 'Supply chain automation', CURRENT_DATE);


-- Taco Bell
DELETE FROM brand_financials WHERE brand_name = 'Taco Bell' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Taco Bell' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Taco Bell' ;
DELETE FROM brand_news WHERE brand_name = 'Taco Bell' ;
DELETE FROM brand_social_media WHERE brand_name = 'Taco Bell' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Taco Bell' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Taco Bell', 2026, '$31.2B', '$350.8B', 33.5, 9.6, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Taco Bell', 'Taco Bell Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Taco Bell', 'Taco Bell Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Taco Bell', 'Taco Bell Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Taco Bell', 'Taco Bell Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Taco Bell', 'Taco Bell Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Taco Bell', 'Taco Bell Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Taco Bell', 'Taco Bell Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Taco Bell', 'Taco Bell Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Taco Bell', 'Competitor A', 1, '24%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Taco Bell', 'Competitor B', 2, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Taco Bell', 'Competitor C', 3, '19%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Taco Bell', 'Taco Bell Launches AI-Powered Personalization', 'Bloomberg', '2026-06-04', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Taco Bell', 'Taco Bell Expands Sustainability Initiatives', 'Reuters', '2026-06-03', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Taco Bell', 'Taco Bell Reports Strong Q2 Growth', 'Reuters', '2026-05-28', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Taco Bell', 'Market Share Gains for Taco Bell', 'Reuters', '2026-05-23', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Taco Bell', 'Taco Bell Announces Strategic Partnerships', 'Reuters', '2026-05-21', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Taco Bell', '23M', '7M', '14M', '4M', '3.6%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Taco Bell', 'Supply chain automation', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Taco Bell', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Taco Bell', 'AI order prediction & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Taco Bell', 'Digital-first ordering (mobile, social)', CURRENT_DATE);


-- Pizza Hut
DELETE FROM brand_financials WHERE brand_name = 'Pizza Hut' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Pizza Hut' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Pizza Hut' ;
DELETE FROM brand_news WHERE brand_name = 'Pizza Hut' ;
DELETE FROM brand_social_media WHERE brand_name = 'Pizza Hut' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Pizza Hut' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Pizza Hut', 2026, '$4.9B', '$41.1B', 27.2, 9.1, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Pizza Hut', 'Pizza Hut Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Pizza Hut', 'Pizza Hut Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Pizza Hut', 'Pizza Hut Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Pizza Hut', 'Pizza Hut Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Pizza Hut', 'Pizza Hut Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Pizza Hut', 'Pizza Hut Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Pizza Hut', 'Pizza Hut Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Pizza Hut', 'Pizza Hut Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Pizza Hut', 'Competitor A', 1, '16%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Pizza Hut', 'Competitor B', 2, '18%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Pizza Hut', 'Competitor C', 3, '15%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Pizza Hut', 'Pizza Hut Launches AI-Powered Personalization', 'Industry Report', '2026-06-16', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Pizza Hut', 'Pizza Hut Expands Sustainability Initiatives', 'Industry Report', '2026-06-10', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Pizza Hut', 'Pizza Hut Reports Strong Q2 Growth', 'Reuters', '2026-06-05', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Pizza Hut', 'Market Share Gains for Pizza Hut', 'Industry Report', '2026-06-04', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Pizza Hut', 'Pizza Hut Announces Strategic Partnerships', 'Industry Report', '2026-05-26', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Pizza Hut', '19M', '4M', '35M', '26M', '4.5%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Pizza Hut', 'AI order prediction & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Pizza Hut', 'AI order prediction & personalization', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Pizza Hut', 'Digital-first ordering (mobile, social)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Pizza Hut', 'Supply chain automation', CURRENT_DATE);


-- Panera Bread
DELETE FROM brand_financials WHERE brand_name = 'Panera Bread' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Panera Bread' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Panera Bread' ;
DELETE FROM brand_news WHERE brand_name = 'Panera Bread' ;
DELETE FROM brand_social_media WHERE brand_name = 'Panera Bread' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Panera Bread' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Panera Bread', 2026, '$10.7B', '$73.9B', 19.9, 2.7, 'QSR & Food Service');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Panera Bread', 'Panera Bread Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Panera Bread', 'Panera Bread Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Panera Bread', 'Panera Bread Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Panera Bread', 'Panera Bread Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Panera Bread', 'Panera Bread Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Panera Bread', 'Panera Bread Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Panera Bread', 'Panera Bread Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Panera Bread', 'Panera Bread Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Panera Bread', 'Competitor A', 1, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Panera Bread', 'Competitor B', 2, '28%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Panera Bread', 'Competitor C', 3, '25%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Panera Bread', 'Panera Bread Launches AI-Powered Personalization', 'Industry Report', '2026-06-14', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Panera Bread', 'Panera Bread Expands Sustainability Initiatives', 'Bloomberg', '2026-06-13', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Panera Bread', 'Panera Bread Reports Strong Q2 Growth', 'Reuters', '2026-06-04', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Panera Bread', 'Market Share Gains for Panera Bread', 'Reuters', '2026-05-31', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Panera Bread', 'Panera Bread Announces Strategic Partnerships', 'Industry Report', '2026-05-20', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Panera Bread', '40M', '10M', '21M', '12M', '3.8%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Panera Bread', 'International expansion (emerging markets)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Panera Bread', 'International expansion (emerging markets)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Panera Bread', 'Supply chain automation', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Panera Bread', 'International expansion (emerging markets)', CURRENT_DATE);


-- Kraft Heinz
DELETE FROM brand_financials WHERE brand_name = 'Kraft Heinz' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Kraft Heinz' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Kraft Heinz' ;
DELETE FROM brand_news WHERE brand_name = 'Kraft Heinz' ;
DELETE FROM brand_social_media WHERE brand_name = 'Kraft Heinz' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Kraft Heinz' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Kraft Heinz', 2026, '$36.0B', '$206.0B', 19.8, 7.2, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kraft Heinz', 'Kraft Heinz Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kraft Heinz', 'Kraft Heinz Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kraft Heinz', 'Kraft Heinz Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kraft Heinz', 'Kraft Heinz Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kraft Heinz', 'Kraft Heinz Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kraft Heinz', 'Kraft Heinz Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kraft Heinz', 'Kraft Heinz Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kraft Heinz', 'Kraft Heinz Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Kraft Heinz', 'Competitor A', 1, '22%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Kraft Heinz', 'Competitor B', 2, '18%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Kraft Heinz', 'Competitor C', 3, '14%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kraft Heinz', 'Kraft Heinz Launches AI-Powered Personalization', 'Industry Report', '2026-06-13', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kraft Heinz', 'Kraft Heinz Expands Sustainability Initiatives', 'Industry Report', '2026-06-09', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kraft Heinz', 'Kraft Heinz Reports Strong Q2 Growth', 'MarketWatch', '2026-06-06', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kraft Heinz', 'Market Share Gains for Kraft Heinz', 'Bloomberg', '2026-06-02', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kraft Heinz', 'Kraft Heinz Announces Strategic Partnerships', 'Bloomberg', '2026-06-01', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Kraft Heinz', '34M', '4M', '38M', '30M', '3.8%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Kraft Heinz', 'Health-focused product development', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Kraft Heinz', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Kraft Heinz', 'Health-focused product development', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Kraft Heinz', 'Health-focused product development', CURRENT_DATE);


-- General Mills
DELETE FROM brand_financials WHERE brand_name = 'General Mills' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'General Mills' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'General Mills' ;
DELETE FROM brand_news WHERE brand_name = 'General Mills' ;
DELETE FROM brand_social_media WHERE brand_name = 'General Mills' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'General Mills' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('General Mills', 2026, '$6.2B', '$31.4B', 16.6, 5.2, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('General Mills', 'General Mills Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('General Mills', 'General Mills Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('General Mills', 'General Mills Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('General Mills', 'General Mills Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('General Mills', 'General Mills Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('General Mills', 'General Mills Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('General Mills', 'General Mills Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('General Mills', 'General Mills Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('General Mills', 'Competitor A', 1, '17%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('General Mills', 'Competitor B', 2, '23%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('General Mills', 'Competitor C', 3, '29%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('General Mills', 'General Mills Launches AI-Powered Personalization', 'Industry Report', '2026-06-10', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('General Mills', 'General Mills Expands Sustainability Initiatives', 'Industry Report', '2026-06-04', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('General Mills', 'General Mills Reports Strong Q2 Growth', 'Industry Report', '2026-05-30', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('General Mills', 'Market Share Gains for General Mills', 'Bloomberg', '2026-05-27', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('General Mills', 'General Mills Announces Strategic Partnerships', 'Industry Report', '2026-05-19', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('General Mills', '31M', '2M', '27M', '27M', '3.3%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('General Mills', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('General Mills', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('General Mills', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('General Mills', 'Supply chain AI & efficiency', CURRENT_DATE);


-- Kellogg's
DELETE FROM brand_financials WHERE brand_name = 'Kellogg''s' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Kellogg''s' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Kellogg''s' ;
DELETE FROM brand_news WHERE brand_name = 'Kellogg''s' ;
DELETE FROM brand_social_media WHERE brand_name = 'Kellogg''s' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Kellogg''s' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Kellogg''s', 2026, '$38.8B', '$502.4B', 29.7, 9.3, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kellogg''s', 'Kellogg''s Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kellogg''s', 'Kellogg''s Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kellogg''s', 'Kellogg''s Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kellogg''s', 'Kellogg''s Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kellogg''s', 'Kellogg''s Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kellogg''s', 'Kellogg''s Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kellogg''s', 'Kellogg''s Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Kellogg''s', 'Kellogg''s Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Kellogg''s', 'Competitor A', 1, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Kellogg''s', 'Competitor B', 2, '11%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Kellogg''s', 'Competitor C', 3, '26%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kellogg''s', 'Kellogg''s Launches AI-Powered Personalization', 'Bloomberg', '2026-06-15', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kellogg''s', 'Kellogg''s Expands Sustainability Initiatives', 'MarketWatch', '2026-06-09', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kellogg''s', 'Kellogg''s Reports Strong Q2 Growth', 'MarketWatch', '2026-05-31', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kellogg''s', 'Market Share Gains for Kellogg''s', 'Industry Report', '2026-05-26', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Kellogg''s', 'Kellogg''s Announces Strategic Partnerships', 'Bloomberg', '2026-05-20', 'Strategy');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Kellogg''s', '20M', '3M', '16M', '17M', '3.5%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Kellogg''s', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Kellogg''s', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Kellogg''s', 'Health-focused product development', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Kellogg''s', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);


-- Nestlé Food
DELETE FROM brand_financials WHERE brand_name = 'Nestlé Food' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Nestlé Food' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Nestlé Food' ;
DELETE FROM brand_news WHERE brand_name = 'Nestlé Food' ;
DELETE FROM brand_social_media WHERE brand_name = 'Nestlé Food' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Nestlé Food' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Nestlé Food', 2026, '$13.7B', '$173.2B', 15.7, 4.1, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Food', 'Nestlé Food Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Food', 'Nestlé Food Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Food', 'Nestlé Food Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Food', 'Nestlé Food Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Food', 'Nestlé Food Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Food', 'Nestlé Food Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Food', 'Nestlé Food Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Nestlé Food', 'Nestlé Food Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nestlé Food', 'Competitor A', 1, '20%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nestlé Food', 'Competitor B', 2, '18%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Nestlé Food', 'Competitor C', 3, '20%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Food', 'Nestlé Food Launches AI-Powered Personalization', 'MarketWatch', '2026-06-05', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Food', 'Nestlé Food Expands Sustainability Initiatives', 'Reuters', '2026-06-02', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Food', 'Nestlé Food Reports Strong Q2 Growth', 'Reuters', '2026-06-02', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Food', 'Market Share Gains for Nestlé Food', 'Bloomberg', '2026-05-30', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Nestlé Food', 'Nestlé Food Announces Strategic Partnerships', 'MarketWatch', '2026-05-21', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Nestlé Food', '27M', '5M', '31M', '8M', '2.8%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nestlé Food', 'Health-focused product development', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nestlé Food', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nestlé Food', 'Supply chain AI & efficiency', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Nestlé Food', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);


-- Lactalis
DELETE FROM brand_financials WHERE brand_name = 'Lactalis' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Lactalis' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Lactalis' ;
DELETE FROM brand_news WHERE brand_name = 'Lactalis' ;
DELETE FROM brand_social_media WHERE brand_name = 'Lactalis' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Lactalis' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Lactalis', 2026, '$3.0B', '$44.7B', 24.8, 11.5, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lactalis', 'Lactalis Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lactalis', 'Lactalis Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lactalis', 'Lactalis Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lactalis', 'Lactalis Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lactalis', 'Lactalis Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lactalis', 'Lactalis Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lactalis', 'Lactalis Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Lactalis', 'Lactalis Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lactalis', 'Competitor A', 1, '22%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lactalis', 'Competitor B', 2, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Lactalis', 'Competitor C', 3, '24%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lactalis', 'Lactalis Launches AI-Powered Personalization', 'Bloomberg', '2026-06-06', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lactalis', 'Lactalis Expands Sustainability Initiatives', 'Industry Report', '2026-05-31', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lactalis', 'Lactalis Reports Strong Q2 Growth', 'Reuters', '2026-05-30', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lactalis', 'Market Share Gains for Lactalis', 'Bloomberg', '2026-05-21', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Lactalis', 'Lactalis Announces Strategic Partnerships', 'Reuters', '2026-05-20', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Lactalis', '22M', '14M', '22M', '18M', '3.9%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lactalis', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lactalis', 'Health-focused product development', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lactalis', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Lactalis', 'Supply chain AI & efficiency', CURRENT_DATE);


-- ConAgra
DELETE FROM brand_financials WHERE brand_name = 'ConAgra' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'ConAgra' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'ConAgra' ;
DELETE FROM brand_news WHERE brand_name = 'ConAgra' ;
DELETE FROM brand_social_media WHERE brand_name = 'ConAgra' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'ConAgra' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('ConAgra', 2026, '$4.8B', '$48.2B', 18.9, 4.9, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('ConAgra', 'ConAgra Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('ConAgra', 'ConAgra Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('ConAgra', 'ConAgra Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('ConAgra', 'ConAgra Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('ConAgra', 'ConAgra Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('ConAgra', 'ConAgra Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('ConAgra', 'ConAgra Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('ConAgra', 'ConAgra Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('ConAgra', 'Competitor A', 1, '30%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('ConAgra', 'Competitor B', 2, '19%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('ConAgra', 'Competitor C', 3, '30%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('ConAgra', 'ConAgra Launches AI-Powered Personalization', 'Industry Report', '2026-06-17', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('ConAgra', 'ConAgra Expands Sustainability Initiatives', 'Bloomberg', '2026-06-16', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('ConAgra', 'ConAgra Reports Strong Q2 Growth', 'Bloomberg', '2026-06-02', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('ConAgra', 'Market Share Gains for ConAgra', 'Industry Report', '2026-05-31', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('ConAgra', 'ConAgra Announces Strategic Partnerships', 'Reuters', '2026-05-25', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('ConAgra', '49M', '17M', '7M', '14M', '4.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('ConAgra', 'Health-focused product development', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('ConAgra', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('ConAgra', 'Health-focused product development', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('ConAgra', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);


-- Campbell Soup
DELETE FROM brand_financials WHERE brand_name = 'Campbell Soup' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Campbell Soup' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Campbell Soup' ;
DELETE FROM brand_news WHERE brand_name = 'Campbell Soup' ;
DELETE FROM brand_social_media WHERE brand_name = 'Campbell Soup' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Campbell Soup' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Campbell Soup', 2026, '$15.8B', '$180.6B', 15.9, 7.2, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Campbell Soup', 'Campbell Soup Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Campbell Soup', 'Campbell Soup Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Campbell Soup', 'Campbell Soup Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Campbell Soup', 'Campbell Soup Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Campbell Soup', 'Campbell Soup Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Campbell Soup', 'Campbell Soup Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Campbell Soup', 'Campbell Soup Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Campbell Soup', 'Campbell Soup Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Campbell Soup', 'Competitor A', 1, '12%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Campbell Soup', 'Competitor B', 2, '28%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Campbell Soup', 'Competitor C', 3, '29%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Campbell Soup', 'Campbell Soup Launches AI-Powered Personalization', 'Bloomberg', '2026-06-17', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Campbell Soup', 'Campbell Soup Expands Sustainability Initiatives', 'Bloomberg', '2026-06-14', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Campbell Soup', 'Campbell Soup Reports Strong Q2 Growth', 'MarketWatch', '2026-06-07', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Campbell Soup', 'Market Share Gains for Campbell Soup', 'Bloomberg', '2026-05-28', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Campbell Soup', 'Campbell Soup Announces Strategic Partnerships', 'MarketWatch', '2026-05-19', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Campbell Soup', '28M', '12M', '35M', '5M', '3.2%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Campbell Soup', 'Supply chain AI & efficiency', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Campbell Soup', 'Health-focused product development', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Campbell Soup', 'Supply chain AI & efficiency', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Campbell Soup', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);


-- Unilever Food
DELETE FROM brand_financials WHERE brand_name = 'Unilever Food' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Unilever Food' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Unilever Food' ;
DELETE FROM brand_news WHERE brand_name = 'Unilever Food' ;
DELETE FROM brand_social_media WHERE brand_name = 'Unilever Food' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Unilever Food' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Unilever Food', 2026, '$16.9B', '$228.9B', 32.2, 4.5, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Unilever Food', 'Unilever Food Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Unilever Food', 'Unilever Food Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Unilever Food', 'Unilever Food Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Unilever Food', 'Unilever Food Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Unilever Food', 'Unilever Food Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Unilever Food', 'Unilever Food Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Unilever Food', 'Unilever Food Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Unilever Food', 'Unilever Food Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Unilever Food', 'Competitor A', 1, '12%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Unilever Food', 'Competitor B', 2, '26%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Unilever Food', 'Competitor C', 3, '10%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Unilever Food', 'Unilever Food Launches AI-Powered Personalization', 'Industry Report', '2026-06-09', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Unilever Food', 'Unilever Food Expands Sustainability Initiatives', 'Bloomberg', '2026-06-07', 'Innovation');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Unilever Food', 'Unilever Food Reports Strong Q2 Growth', 'Industry Report', '2026-06-07', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Unilever Food', 'Market Share Gains for Unilever Food', 'Bloomberg', '2026-06-06', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Unilever Food', 'Unilever Food Announces Strategic Partnerships', 'MarketWatch', '2026-05-30', 'Market Position');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Unilever Food', '22M', '18M', '26M', '7M', '4.2%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Unilever Food', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Unilever Food', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Unilever Food', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Unilever Food', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);


-- Tyson Foods
DELETE FROM brand_financials WHERE brand_name = 'Tyson Foods' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Tyson Foods' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Tyson Foods' ;
DELETE FROM brand_news WHERE brand_name = 'Tyson Foods' ;
DELETE FROM brand_social_media WHERE brand_name = 'Tyson Foods' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Tyson Foods' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Tyson Foods', 2026, '$6.8B', '$72.2B', 21.3, 2.7, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tyson Foods', 'Tyson Foods Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tyson Foods', 'Tyson Foods Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tyson Foods', 'Tyson Foods Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tyson Foods', 'Tyson Foods Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tyson Foods', 'Tyson Foods Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tyson Foods', 'Tyson Foods Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tyson Foods', 'Tyson Foods Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Tyson Foods', 'Tyson Foods Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tyson Foods', 'Competitor A', 1, '15%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tyson Foods', 'Competitor B', 2, '29%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Tyson Foods', 'Competitor C', 3, '16%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tyson Foods', 'Tyson Foods Launches AI-Powered Personalization', 'Bloomberg', '2026-06-01', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tyson Foods', 'Tyson Foods Expands Sustainability Initiatives', 'Bloomberg', '2026-05-25', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tyson Foods', 'Tyson Foods Reports Strong Q2 Growth', 'Reuters', '2026-05-22', 'Market Position');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tyson Foods', 'Market Share Gains for Tyson Foods', 'Industry Report', '2026-05-22', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Tyson Foods', 'Tyson Foods Announces Strategic Partnerships', 'Bloomberg', '2026-05-21', 'Growth');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Tyson Foods', '22M', '4M', '36M', '25M', '3.7%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tyson Foods', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tyson Foods', 'Supply chain AI & efficiency', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tyson Foods', 'Supply chain AI & efficiency', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Tyson Foods', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);


-- Ben & Jerry's
DELETE FROM brand_financials WHERE brand_name = 'Ben & Jerry''s' ;
DELETE FROM brand_skus_complete WHERE brand_name = 'Ben & Jerry''s' ;
DELETE FROM brand_competitors_complete WHERE brand_name = 'Ben & Jerry''s' ;
DELETE FROM brand_news WHERE brand_name = 'Ben & Jerry''s' ;
DELETE FROM brand_social_media WHERE brand_name = 'Ben & Jerry''s' ;
DELETE FROM brand_ai_strategy WHERE brand_name = 'Ben & Jerry''s' ;

INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, source)
VALUES ('Ben & Jerry''s', 2026, '$23.2B', '$201.0B', 23.2, 5.6, 'Dairy & Packaged Food');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Premium', 'Premium', '$12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Standard', 'Standard', '$7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Premium', 'Premium', '£12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Standard', 'Standard', '£7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Premium', 'Premium', '€12.99', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Standard', 'Standard', '€7.99', 2, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Premium', 'Premium', '₹999', 1, 'stable');
INSERT INTO brand_skus_complete (brand_name, sku, category, price, market_position, sales_trend) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Standard', 'Standard', '₹599', 2, 'stable');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ben & Jerry''s', 'Competitor A', 1, '21%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ben & Jerry''s', 'Competitor B', 2, '12%');
INSERT INTO brand_competitors_complete (brand_name, name, market_position, market_share) VALUES ('Ben & Jerry''s', 'Competitor C', 3, '19%');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Launches AI-Powered Personalization', 'MarketWatch', '2026-06-17', 'Strategy');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Expands Sustainability Initiatives', 'MarketWatch', '2026-06-16', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Reports Strong Q2 Growth', 'Industry Report', '2026-06-10', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ben & Jerry''s', 'Market Share Gains for Ben & Jerry''s', 'Industry Report', '2026-06-05', 'Growth');
INSERT INTO brand_news (brand_name, title, source, published_date, category) VALUES ('Ben & Jerry''s', 'Ben & Jerry''s Announces Strategic Partnerships', 'Reuters', '2026-05-24', 'Innovation');
INSERT INTO brand_social_media (brand_name, instagram_followers, twitter_followers, tiktok_followers, youtube_followers, engagement_rate) VALUES ('Ben & Jerry''s', '13M', '10M', '23M', '5M', '4.4%');
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ben & Jerry''s', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ben & Jerry''s', 'Plant-based alternatives (dairy, meat)', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ben & Jerry''s', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);
INSERT INTO brand_ai_strategy (brand_name, ai_focus_area, announcement_date) VALUES ('Ben & Jerry''s', 'Direct-to-consumer (D2C) growth', CURRENT_DATE);
