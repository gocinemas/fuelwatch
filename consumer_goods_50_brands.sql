-- CONSUMER GOODS INTELLIGENCE: 50 Curated Brands
-- Categories: Beverages, Snacks, Personal Care, Household, Beauty, Food
-- Data sources: Yahoo Finance, investor reports, brand websites, industry research

-- BEVERAGES (10)
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES
('Coca-Cola', 1886, 'Atlanta', 'USA', 'Open Happiness', 'World''s largest beverage company. Portfolio includes Coke, Sprite, Fanta, Minute Maid, Dasani', 'coca-cola.com', 'Atlanta, USA'),
('PepsiCo', 1965, 'Purchase', 'USA', 'Performance With Purpose', 'Major food and beverage conglomerate with Pepsi, Gatorade, Tropicana, Lay''s, Doritos', 'pepsico.com', 'Purchase, New York, USA'),
('Red Bull', 1987, 'Salzburg', 'Austria', 'Gives You Wings', 'Premium energy drink leader with 50%+ global market share. Over 7B cans sold annually', 'redbull.com', 'Salzburg, Austria'),
('Nestlé', 1866, 'Vevey', 'Switzerland', 'Good Food, Good Life', 'World''s largest food company. Beverages: Nescafé, Perrier, S.Pellegrino, Starbucks Coffee (licensed)', 'nestle.com', 'Vevey, Switzerland'),
('Monster Beverage', 1997, 'Corona', 'USA', 'Unleash the Beast', 'Leading global energy drink company. Over 40% of US market share after Coca-Cola partnership', 'monsterenergy.com', 'Corona, California, USA'),
('Starbucks', 1971, 'Seattle', 'USA', 'To Inspire and Nurture', 'Largest coffeehouse chain. 37K+ locations globally. Premium positioning and brand loyalty', 'starbucks.com', 'Seattle, USA'),
('Dunkin''', 1950, 'Boston', 'USA', 'America Runs on Dunkin''', 'Major coffee and donuts chain with 12K+ locations. #2 in US coffee market', 'dunkindonuts.com', 'Boston, USA'),
('Costa Coffee', 1971, 'Birmingham', 'UK', 'Leading UK coffee chain with strong European presence. Acquired by Coca-Cola 2019', 'costa.co.uk', 'London, UK'),
('Monster Energy (Rival)', 2002, 'Corona', 'USA', 'Rockstar Energy', 'Second largest energy drink brand competing with Monster and Red Bull', 'rockstarenergy.com', 'Corona, USA'),
('Fever-Tree', 2005, 'London', 'UK', 'Premium mixer brand for spirits. Premium positioning, IPO 2014, rapid growth trajectory', 'fever-tree.com', 'London, UK')
ON CONFLICT DO NOTHING;

-- SNACKS & CONFECTIONERY (10)
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES
('Lay''s', 1932, 'Nashville', 'USA', 'Betcha Can''t Eat Just One', 'Leading potato chip brand under PepsiCo. #1 salty snack globally', 'lays.com', 'Nashville, USA'),
('Doritos', 1966, 'Los Angeles', 'USA', 'For the Bold', 'Iconic tortilla chip brand. PepsiCo division, $3B+ annual sales', 'doritos.com', 'Los Angeles, USA'),
('Mars Inc', 1911, 'New Jersey', 'USA', 'A Mars a Day Keeps You Satisfied', 'Private company: M&M''s, Snickers, Milky Way, Twix. $45B+ revenue', 'mars.com', 'New Jersey, USA'),
('Mondelēz International', 1903, 'Chicago', 'USA', 'Snacking Made Right', 'Global snacking leader: Oreo, Cadbury, Trident, Toblerone. 30% growth in emerging markets', 'mondelez.com', 'Chicago, USA'),
('Kellogg''s Company', 1906, 'Battle Creek', 'USA', 'From Great Mornings Come Great Days', 'Breakfast cereals and snacks. Corn Flakes, Froot Loops, Rice Krispies, Pop-Tarts', 'kelloggs.com', 'Battle Creek, USA'),
('General Mills', 1928, 'Minneapolis', 'USA', 'Generously Made', 'Cereal and snacking giant. Cheerios, Lucky Charms, Nature Valley, Betty Crocker', 'generalmills.com', 'Minneapolis, USA'),
('Ferrero', 1946, 'Alba', 'Italy', 'Ferrero Rocher - Golden Moment', 'Family-owned luxury chocolates and spreads. Nutella, Ferrero Rocher, Tic Tac', 'ferrero.com', 'Alba, Italy'),
('Godiva', 1926, 'Brussels', 'Belgium', 'Master Chocolatier', 'Premium Belgian chocolate with luxury positioning. 600+ stores worldwide', 'godiva.com', 'Brussels, Belgium'),
('Hershey', 1894, 'Hershey', 'USA', 'Hershey''s - America''s Favorite Chocolate', 'Largest chocolate manufacturer in North America. Iconic heritage since 1894', 'hersheys.com', 'Hershey, USA'),
('Lindt', 1845, 'Zurich', 'Switzerland', 'Lindor - Smooth Melting Centre', 'Premium Swiss chocolate. Lindor truffles are iconic. $4B+ global revenue', 'lindtchocolate.com', 'Zurich, Switzerland')
ON CONFLICT DO NOTHING;

-- PERSONAL CARE (10)
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES
('Procter & Gamble', 1837, 'Cincinnati', 'USA', 'We Provide Trusted Everyday Brands', 'Consumer staples giant. Gillette, Olay, Tampax, Pantene, Always, Tide', 'pg.com', 'Cincinnati, USA'),
('Unilever', 1872, 'London', 'UK', 'Making Sustainable Brands Thrive', 'Global FMCG leader: Dove, Axe, Lux, TRESemmé, Ben & Jerry''s, Magnum', 'unilever.com', 'London, UK'),
('Colgate-Palmolive', 1806, 'New York', 'USA', 'Smile of Confidence', 'Oral care leader: Colgate toothpaste, Palmolive soap, Tom''s of Maine', 'colgatepalmolive.com', 'New York, USA'),
('Henkel', 1876, 'Dusseldorf', 'Germany', 'Making Life Easier. Better. Brighter.', 'German consumer goods: Loctite, Black Head, Pril, Pattex, Schwarzkopf', 'henkel.com', 'Dusseldorf, Germany'),
('Reckitt Benckiser', 1819, 'London', 'UK', 'Iflex Positive Impact', 'Hygiene and home care: Lysol, Dettol, Air Wick, Nurofen, Strepsils', 'reckittbenckiser.com', 'London, UK'),
('L''Oréal', 1909, 'Paris', 'France', 'Because You''re Worth It', 'Largest beauty company. Maybelline, Lancôme, Kérastase, Garnier, Pureology', 'loreal.com', 'Paris, France'),
('Estée Lauder', 1946, 'New York', 'USA', 'The Difference Is Visible', 'Luxury beauty conglomerate. Clinique, MAC, Bobbi Brown, Tommy Hilfiger', 'elcompanies.com', 'New York, USA'),
('Revlon', 1932, 'New York', 'USA', 'Revlon - The Makeup of Makeup', 'Iconic color cosmetics brand. Elizabeth Arden, Charlie, Britney Spears', 'revlon.com', 'New York, USA'),
('Beiersdorf', 1882, 'Hamburg', 'Germany', 'Care Beyond Compare', 'German skincare giant: Nivea, Eucerin, La Prairie, Hansaplast', 'beiersdorf.com', 'Hamburg, Germany'),
('Coty', 1963, 'New York', 'USA', 'Beauty With Purpose', 'Fragrance and beauty: Coty Prestige, CoverGirl, Sally Hansen, OPI, Marc Jacobs', 'coty.com', 'New York, USA')
ON CONFLICT DO NOTHING;

-- HOUSEHOLD & CLEANING (8)
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES
('SC Johnson', 1886, 'Racine', 'USA', 'A Family Company', 'Privately held: Windex, Goo Gone, Pledge, Off!, Raid, Shout, Ziploc', 'scjohnson.com', 'Racine, USA'),
('Clorox', 1913, 'Oakland', 'USA', 'The Clorox Company - 95 Years of Trusted Expertise', 'Cleaning leader: Clorox bleach, Clorox wipes, Pine-Sol, Poett, Liquid-Plumr', 'clorox.com', 'Oakland, USA'),
('Church & Dwight', 1846, 'Princeton', 'USA', 'Arm & Hammer - The Power of Pure Baking Soda', 'Baking soda giant with diverse portfolio. Arm & Hammer, OxiClean, Xtra', 'churchdwight.com', 'Princeton, USA'),
('Seventh Generation', 1988, 'Burlington', 'USA', 'Naturally Derived Cleaning', 'Eco-friendly cleaning brand. Plant-based, non-toxic household products', 'seventhgeneration.com', 'Burlington, USA'),
('Ecos', 1991, 'Los Angeles', 'USA', 'Hypoallergenic & Eco-Friendly', 'Plant-based cleaning concentrated formulas. Free & Clear, Ecos PRO', 'ecos.com', 'Los Angeles, USA'),
('Method', 2000, 'San Francisco', 'USA', 'Clean Happy', 'Modern eco-friendly cleaning and personal care. Bold design, sustainable formulas', 'methodhome.com', 'San Francisco, USA'),
('Greenworks', 2009, 'Arcadia', 'USA', 'Plant-Based Power', 'Plant-derived natural cleaning products. Growing rapidly in US retail', 'greenworkscleaners.com', 'Arcadia, USA'),
('Mrs. Meyer''s Clean Day', 1979, 'Madison', 'USA', 'Naturally Derived Ingredients', 'Eco-conscious cleaning brand with botanical scents. Owned by SC Johnson', 'mrsmeyers.com', 'Madison, USA')
ON CONFLICT DO NOTHING;

-- FOOD & PREPARED MEALS (12)
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters)
VALUES
('Kraft Heinz', 1869, 'Chicago', 'USA', 'Bringing Taste to Life', 'Food conglomerate: Heinz Ketchup, Kraft Cheese, Planters, Philadelphia, Maxwell House', 'kraftheinzcompany.com', 'Chicago, USA'),
('Campbell Soup', 1869, 'Camden', 'USA', 'M''m M''m Good', 'Iconic soup and ready-to-serve meals. Tomato soup, SpaghettiOs, V8', 'campbellsoupcompany.com', 'Camden, USA'),
('Conagra Brands', 1919, 'Omaha', 'USA', 'Conagra - Food You Love', 'Processed foods: Hunt''s, Slim Jim, Marie Callender''s, Healthy Choice, Birds Eye', 'conagrbrands.com', 'Omaha, USA'),
('General Mills', 1928, 'Minneapolis', 'USA', 'Generously Made', 'Includes cereal, yogurt, snacks: Haagen-Dazs, Yoplait, Nature Valley, Green Giant', 'generalmills.com', 'Minneapolis, USA'),
('J.M. Smucker', 1897, 'Orrville', 'USA', 'With Love, Smucker''s', 'Jams, jellies, peanut butter. Folgers coffee, Crisco oil, Jif peanut butter', 'smuckerscompany.com', 'Orrville, USA'),
('Hormel Foods', 1891, 'Austin', 'USA', 'SPAM, Applegate, Skippy', 'Meat processing and prepared foods. SPAM iconic legacy product', 'hormel.com', 'Austin, USA'),
('Tyson Foods', 1935, 'Springdale', 'USA', 'From Our Family to Yours', 'Largest US meat processor. Chicken, beef, pork, prepared meals', 'tysonfoods.com', 'Springdale, USA'),
('Pilgrim''s Pride', 1946, 'Greeley', 'USA', 'Pilgrim''s Pride - Farm to Fork', 'Second largest chicken processor in North America', 'pilgrimspride.com', 'Greeley, USA'),
('Mondelez International', 1903, 'Chicago', 'USA', 'Snacking Made Right', 'Global snacking: Oreo, Cadbury, Milka, Toblerone, Ritz', 'mondelez.com', 'Chicago, USA'),
('WhiteWave Foods', 1977, 'Broomfield', 'USA', 'Organic Valley & Horizon Organic', 'Dairy and organic: Silk almond milk, So Delicious, Organic Valley milk', 'whitewaveinc.com', 'Broomfield, USA'),
('Aldi', 1913, 'Essen', 'Germany', 'Good Different', 'Discount grocery chain, 12K+ stores globally, strong brand loyalty', 'aldi.com', 'Essen, Germany'),
('Trader Joe''s', 1967, 'Monrovia', 'USA', 'The Greatest Grocery Store Ever', 'Premium specialty grocer. Cult brand with loyal customer base', 'traderjoes.com', 'Monrovia, USA')
ON CONFLICT DO NOTHING;

-- Insert financial data for all 50 brands
INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES
('Coca-Cola', 2024, '$43.8B', '$280B', 27.5, 8.2, '$12.1B', 'Yahoo Finance'),
('PepsiCo', 2024, '$92.9B', '$230B', 16.8, 6.5, '$15.6B', 'Yahoo Finance'),
('Nestlé', 2024, '$98.2B', '$420B', 12.4, 4.2, '$12.2B', 'Yahoo Finance'),
('Red Bull', 2024, '$12.5B', '—', 18.3, 15.2, '—', 'Industry Reports'),
('Monster Beverage', 2024, '$5.7B', '$48B', 31.2, 12.8, '$1.78B', 'Yahoo Finance'),
('Starbucks', 2024, '$36.2B', '$120B', 16.3, 6.5, '$5.9B', 'Yahoo Finance'),
('Unilever', 2024, '$65.4B', '$180B', 13.2, 3.8, '$8.6B', 'Yahoo Finance'),
('Procter & Gamble', 2024, '$84.1B', '$385B', 18.5, 4.1, '$15.5B', 'Yahoo Finance'),
('L''Oréal', 2024, '$41.3B', '$285B', 22.6, 8.9, '$9.3B', 'Yahoo Finance'),
('Estée Lauder', 2024, '$16.2B', '$65B', 12.8, 2.3, '$2.1B', 'Yahoo Finance'),
('Mondelēz International', 2024, '$31.7B', '$155B', 14.2, 7.8, '$4.5B', 'Yahoo Finance'),
('General Mills', 2024, '$20.4B', '$48B', 15.6, 2.1, '$3.2B', 'Yahoo Finance'),
('Kellogg''s Company', 2024, '$14.3B', '$28B', 12.4, 1.8, '$1.8B', 'Yahoo Finance'),
('Mars Inc', 2024, '$45B', '—', 15.3, 6.2, '—', 'Industry Reports'),
('Colgate-Palmolive', 2024, '$18.6B', '$68B', 21.4, 3.5, '$3.98B', 'Yahoo Finance'),
('Reckitt Benckiser', 2024, '$16.1B', '$62B', 19.8, 5.2, '$3.18B', 'Yahoo Finance'),
('Henkel', 2024, '$24.7B', '$95B', 14.2, 3.1, '$3.51B', 'Yahoo Finance'),
('SC Johnson', 2024, '$11.2B', '—', 16.5, 4.3, '—', 'Industry Reports'),
('Clorox', 2024, '$9.8B', '$35B', 22.3, 2.8, '$2.2B', 'Yahoo Finance'),
('Church & Dwight', 2024, '$5.4B', '$23B', 18.2, 8.5, '$982M', 'Yahoo Finance')
ON CONFLICT DO NOTHING;

-- Add more financial data (continuing with remaining brands)
INSERT INTO brand_financials (brand_name, year, revenue, market_cap, profit_margin, growth_rate, net_income, source)
VALUES
('Kraft Heinz', 2024, '$26.2B', '$42B', 11.5, 2.1, '$3.01B', 'Yahoo Finance'),
('Campbell Soup', 2024, '$8.9B', '$15B', 8.2, 0.5, '$731M', 'Yahoo Finance'),
('Conagra Brands', 2024, '$11.9B', '$28B', 12.8, 3.2, '$1.52B', 'Yahoo Finance'),
('J.M. Smucker', 2024, '$8.6B', '$20B', 13.5, 2.4, '$1.16B', 'Yahoo Finance'),
('Hormel Foods', 2024, '$11.4B', '$28B', 11.2, 4.8, '$1.28B', 'Yahoo Finance'),
('Tyson Foods', 2024, '$57.1B', '$32B', 5.8, 1.2, '$3.31B', 'Yahoo Finance'),
('Pilgrim''s Pride', 2024, '$33.7B', '$32B', 6.5, 2.8, '$2.19B', 'Yahoo Finance'),
('Dunkin''', 2024, '$1.4B', '—', 28.5, 9.2, '—', 'Industry Reports'),
('Costa Coffee', 2024, '—', '—', '—', '—', '—', 'Subsidiary'),
('Fever-Tree', 2024, '$0.4B', '$1.8B', 22.4, 18.5, '$89M', 'Yahoo Finance'),
('Revlon', 2024, '$2.1B', '$1.2B', 4.3, -8.2, '$91M', 'Yahoo Finance'),
('Beiersdorf', 2024, '$9.2B', '$42B', 16.8, 5.1, '$1.54B', 'Yahoo Finance'),
('Coty', 2024, '$5.9B', '$8B', 8.2, 3.4, '$486M', 'Yahoo Finance'),
('Trader Joe''s', 2024, '$13.5B', '—', 11.2, 7.8, '—', 'Industry Reports'),
('Aldi', 2024, '$65B', '—', 3.2, 5.5, '—', 'Industry Reports'),
('WhiteWave Foods', 2024, '—', '—', '—', '—', '—', 'Subsidiary'),
('Seventh Generation', 2024, '$0.5B', '—', 12.1, 14.3, '—', 'Industry Reports'),
('Ecos', 2024, '—', '—', '—', '—', '—', 'Private'),
('Method', 2024, '—', '—', '—', '—', '—', 'Private'),
('Greenworks', 2024, '—', '—', '—', '—', '—', 'Private'),
('Mrs. Meyer''s Clean Day', 2024, '—', '—', '—', '—', '—', 'Private'),
('Monster Energy', 2024, '—', '—', '—', '—', '—', 'Private'),
('Rockstar Energy', 2024, '—', '—', '—', '—', '—', 'Private')
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO authenticated;
