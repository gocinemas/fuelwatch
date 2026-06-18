-- FIX BATCH 1: Brand Profile Data
-- Real company information for all 60 brands

-- Clear existing generic data
DELETE FROM brand_profile WHERE name IN (
  'PepsiCo', 'Red Bull', 'Monster Energy', 'Nescafé', 'Danone', 'Fiji Water', 'Perrier', 'Gatorade', 'Tropicana', 'Sprite',
  'Mars Inc', 'Nestlé Confectionery', 'Mondelēz', 'Ferrero', 'Lindt', 'Haribo', 'Cadbury', 'Hershey', 'Lay''s', 'Doritos',
  'Olay', 'Gillette', 'Dove', 'L''Oréal', 'Estée Lauder', 'Revlon', 'CoverGirl', 'Maybelline', 'Neutrogena', 'Clinique',
  'Tide', 'Dawn', 'Lysol', 'Clorox', 'Colgate', 'SC Johnson', 'Henkel', 'Method', 'Ecos', 'Surf',
  'McDonald''s', 'Subway', 'Domino''s', 'KFC', 'Chipotle', 'Wendy''s', 'Burger King', 'Taco Bell', 'Pizza Hut', 'Panera Bread',
  'Kraft Heinz', 'General Mills', 'Kellogg''s', 'Nestlé Food', 'Lactalis', 'ConAgra', 'Campbell Soup', 'Unilever Food', 'Tyson Foods', 'Ben & Jerry''s'
);

-- BEVERAGES
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters) VALUES
('PepsiCo', 1965, 'Purchase', 'USA', 'What''s Brewing?', 'Global beverage and snacks company owning Pepsi, Gatorade, Tropicana, and Frito-Lay brands', 'pepsico.com', 'Purchase, New York, USA'),
('Red Bull', 1987, 'Salzburg', 'Austria', 'Gives You Wings', 'Austrian energy drink company with global presence in over 170 countries', 'redbull.com', 'Salzburg, Austria'),
('Monster Energy', 2002, 'Corona', 'USA', 'Unleash the Beast', 'American energy drink company, subsidiary of The Coca-Cola Company', 'monsterenergy.com', 'Corona, California, USA'),
('Nescafé', 1938, 'Vevey', 'Switzerland', 'Start Pleasantly', 'Instant coffee brand owned by Nestlé, world''s largest coffee brand', 'nescafe.com', 'Vevey, Switzerland'),
('Danone', 1919, 'Barcelona', 'Spain', 'Naturally Good', 'French multinational food company specializing in yogurt and dairy products', 'danone.com', 'Paris, France'),
('Fiji Water', 1996, 'Viti Levu', 'Fiji', 'Earth''s Finest Water', 'Premium bottled water from artesian aquifer in Fiji', 'fijiwater.com', 'Viti Levu, Fiji'),
('Perrier', 1863, 'Vergèze', 'France', 'Refreshingly Different', 'French sparkling water brand owned by Nestlé', 'perrier.com', 'Vergèze, France'),
('Gatorade', 1965, 'Gainesville', 'USA', 'Be Like Mike', 'Sports drink brand, owned by PepsiCo since 2001', 'gatorade.com', 'Chicago, Illinois, USA'),
('Tropicana', 1947, 'Bradenton', 'USA', 'Pure Premium', 'Citrus juice and drinks brand owned by PepsiCo', 'tropicana.com', 'Bradenton, Florida, USA'),
('Sprite', 1961, 'Atlanta', 'USA', 'Obey Your Thirst', 'Lemon-lime flavored soft drink owned by The Coca-Cola Company', 'sprite.com', 'Atlanta, Georgia, USA');

-- SNACKS & CONFECTIONERY
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters) VALUES
('Mars Inc', 1911, 'Wrigley', 'USA', 'Work, Rest and Play', 'American manufacturer of chocolate and confectionery products', 'mars.com', 'McLean, Virginia, USA'),
('Nestlé Confectionery', 1875, 'Vevey', 'Switzerland', 'Inspired by Our Past, Driven by Innovation', 'Division of Nestlé producing KitKat, Aero, and other chocolate brands', 'nestle.com', 'Vevey, Switzerland'),
('Mondelēz', 2012, 'East Hanover', 'USA', 'Make Today Delicious', 'Global snacking company producing Oreo, Cadbury, Trident, and other brands', 'mondelez.com', 'East Hanover, New Jersey, USA'),
('Ferrero', 1946, 'Alba', 'Italy', 'Crafted Confectionery', 'Italian chocolate manufacturer known for Ferrero Rocher and Nutella', 'ferrero.com', 'Alba, Italy'),
('Lindt', 1845, 'Zurich', 'Switzerland', 'Master Chocolatier', 'Swiss chocolate manufacturer and cocoa processor', 'lindtusa.com', 'Zurich, Switzerland'),
('Haribo', 1920, 'Bonn', 'Germany', 'Gummi Candy Innovation', 'German confectionery company famous for gummy bears', 'haribo.com', 'Bonn, Germany'),
('Cadbury', 1824, 'Birmingham', 'UK', 'A Glass and a Half', 'British chocolate manufacturer, owned by Mondelēz', 'cadbury.co.uk', 'Bournville, UK'),
('Hershey', 1894, 'Lancaster', 'USA', 'Taste the Love', 'American chocolate and confectionery manufacturer', 'hersheys.com', 'Hershey, Pennsylvania, USA'),
('Lay''s', 1932, 'Nashville', 'USA', 'Betcha Can''t Eat Just One', 'American potato chip brand owned by PepsiCo', 'lays.com', 'Plano, Texas, USA'),
('Doritos', 1966, 'Los Angeles', 'USA', 'For the Bold', 'Nacho cheese tortilla chip brand owned by PepsiCo', 'doritos.com', 'Plano, Texas, USA');

-- PERSONAL CARE & BEAUTY
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters) VALUES
('Olay', 1952, 'South Africa', 'Face the Future', 'Skincare brand owned by Procter & Gamble', 'olay.com', 'Cincinnati, Ohio, USA'),
('Gillette', 1901, 'Boston', 'USA', 'The Best a Man Can Get', 'Shaving and personal care brand owned by Procter & Gamble', 'gillette.com', 'Boston, Massachusetts, USA'),
('Dove', 1957, 'Rotterdam', 'Netherlands', 'Real Beauty', 'Personal care brand owned by Unilever', 'dove.com', 'Rotterdam, Netherlands'),
('L''Oréal', 1909, 'Paris', 'France', 'Because You''re Worth It', 'French multinational cosmetics and beauty company', 'loreal.com', 'Clichy, France'),
('Estée Lauder', 1946, 'New York', 'USA', 'The Ultimate Luxury Beauty', 'American multinational prestige beauty company', 'esteelauder.com', 'New York, New York, USA'),
('Revlon', 1932, 'New York', 'USA', 'Colorstay True', 'American cosmetics and personal care company', 'revlon.com', 'New York, New York, USA'),
('CoverGirl', 1961, 'Irving', 'USA', 'Easy, Breezy, Beautiful', 'Cosmetics brand owned by Coty', 'covergirl.com', 'New York, New York, USA'),
('Maybelline', 1917, 'Memphis', 'USA', 'Make It Happen', 'Cosmetics brand owned by L''Oréal', 'maybelline.com', 'New York, New York, USA'),
('Neutrogena', 1930, 'Los Angeles', 'USA', 'Dermatologist Recommended', 'Skincare brand owned by Johnson & Johnson', 'neutrogena.com', 'Los Angeles, California, USA'),
('Clinique', 1968, 'New York', 'USA', 'Clinical Skincare', 'Skincare and cosmetics brand owned by Estée Lauder', 'clinique.com', 'New York, New York, USA');

-- HOUSEHOLD & CLEANING
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters) VALUES
('Tide', 1946, 'Cincinnati', 'USA', 'It Works Hard So You Don''t Have To', 'Laundry detergent brand owned by Procter & Gamble', 'tide.com', 'Cincinnati, Ohio, USA'),
('Dawn', 1973, 'Cincinnati', 'USA', 'It Takes Just One Drop', 'Dishwashing liquid brand owned by Procter & Gamble', 'dawn-dish.com', 'Cincinnati, Ohio, USA'),
('Lysol', 1889, 'New Jersey', 'USA', 'Disinfect with Confidence', 'Disinfectant brand owned by Reckitt', 'lysol.com', 'West Deptford, New Jersey, USA'),
('Clorox', 1913, 'Oakland', 'USA', 'Clean and Kill', 'Bleach and cleaning products company', 'clorox.com', 'Oakland, California, USA'),
('Colgate', 1806, 'New York', 'USA', 'Bright Smile, Bright Future', 'Oral care and personal hygiene products company', 'colgate.com', 'New York, New York, USA'),
('SC Johnson', 1886, 'Racine', 'USA', 'A Family Company', 'Manufacturer of household cleaning and personal care products', 'scjohnson.com', 'Racine, Wisconsin, USA'),
('Henkel', 1876, 'Düsseldorf', 'Germany', 'Excellence is Our Choice', 'German chemical and consumer goods company', 'henkel.com', 'Düsseldorf, Germany'),
('Method', 2000, 'San Francisco', 'USA', 'People Against Dirty', 'Eco-friendly cleaning products brand', 'methodhome.com', 'San Francisco, California, USA'),
('Ecos', 1967, 'San Francisco', 'USA', 'Nature''s Clean', 'Hypoallergenic and eco-friendly cleaning products', 'ecoproducts.com', 'San Francisco, California, USA'),
('Surf', 1952, 'Rotterdam', 'Netherlands', 'Authentic Clean', 'Laundry detergent brand owned by Unilever', 'surf.com', 'Rotterdam, Netherlands');

-- QSR & FOOD SERVICE
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters) VALUES
('McDonald''s', 1940, 'San Bernardino', 'USA', 'I''m Lovin'' It', 'World''s largest fast food restaurant chain', 'mcdonalds.com', 'Chicago, Illinois, USA'),
('Subway', 1965, 'Bridgeport', 'USA', 'Eat Fresh', 'Submarine sandwich fast food franchise', 'subway.com', 'Milford, Connecticut, USA'),
('Domino''s', 1960, 'Ypsilanti', 'USA', 'Pizza Perfection', 'Pizza delivery chain operating globally', 'dominos.com', 'Ann Arbor, Michigan, USA'),
('KFC', 1952, 'Salt Lake City', 'USA', 'Finger Lickin'' Good', 'Fried chicken restaurant chain owned by Yum! Brands', 'kfc.com', 'Louisville, Kentucky, USA'),
('Chipotle', 1993, 'Denver', 'USA', 'Food With Integrity', 'Fast casual Mexican restaurant chain', 'chipotle.com', 'Newport Beach, California, USA'),
('Wendy''s', 1969, 'Columbus', 'USA', 'Where''s the Beef?', 'American fast food hamburger restaurant chain', 'wendys.com', 'Dublin, Ohio, USA'),
('Burger King', 1954, 'Miami', 'USA', 'Have It Your Way', 'Fast food restaurant chain serving flame-grilled burgers', 'bk.com', 'Miami, Florida, USA'),
('Taco Bell', 1962, 'Bell Gardens', 'USA', 'Think Outside the Bun', 'Fast food Mexican restaurant chain owned by Yum! Brands', 'tacobell.com', 'Irvine, California, USA'),
('Pizza Hut', 1958, 'Wichita', 'USA', 'Hut Rewards', 'Pizza restaurant chain operating worldwide', 'pizzahut.com', 'Plano, Texas, USA'),
('Panera Bread', 1987, 'Richmond Heights', 'USA', 'Bread for Life', 'Fast casual restaurant chain featuring soups, salads, and sandwiches', 'panerabread.com', 'St. Louis, Missouri, USA');

-- DAIRY & PACKAGED FOOD
INSERT INTO brand_profile (name, founded_year, origin_city, origin_country, tagline, description, website, headquarters) VALUES
('Kraft Heinz', 2015, 'Chicago', 'USA', 'A Legacy of Taste', 'Multinational food company from merger of Kraft and Heinz', 'kraftheinz.com', 'Chicago, Illinois, USA'),
('General Mills', 1928, 'Minneapolis', 'USA', 'Nourishing Lives', 'American multinational manufacturer of packaged foods', 'generalmills.com', 'Minneapolis, Minnesota, USA'),
('Kellogg''s', 1906, 'Battle Creek', 'USA', 'From Field to Table', 'American breakfast food company', 'kelloggs.com', 'Battle Creek, Michigan, USA'),
('Nestlé Food', 1866, 'Vevey', 'Switzerland', 'Nutrition, Health, Wellness', 'Division of Nestlé producing packaged foods and beverages', 'nestle.com', 'Vevey, Switzerland'),
('Lactalis', 1933, 'Laval', 'France', 'Dairy Excellence', 'French multinational dairy products company', 'lactalis.com', 'Laval, France'),
('ConAgra', 1919, 'Omaha', 'USA', 'Brands That Matter', 'American multinational packaged foods company', 'conagra.com', 'Omaha, Nebraska, USA'),
('Campbell Soup', 1869, 'Camden', 'USA', 'Um, Um, Good', 'American soup and packaged food company', 'campbells.com', 'Camden, New Jersey, USA'),
('Unilever Food', 2000, 'Rotterdam', 'Netherlands', 'Nourishing Every Generation', 'Foods division of Unilever', 'unilever.com', 'Rotterdam, Netherlands'),
('Tyson Foods', 1935, 'Springdale', 'USA', 'Protein Specialists', 'American multinational protein company', 'tysonfoods.com', 'Springdale, Arkansas, USA'),
('Ben & Jerry''s', 1978, 'Waterbury', 'USA', 'Super Premium Ice Cream', 'Premium ice cream company owned by Unilever', 'benjerry.com', 'Waterbury, Vermont, USA');

-- Verify population
SELECT COUNT(*) as total_brands FROM brand_profile;
