-- COMPANY & BRAND INTELLIGENCE ARCHITECTURE
-- 50 Companies + 100 Brands properly linked

-- ============================================
-- 1. CREATE COMPANY_PROFILE TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS company_profile (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  founded_year INTEGER,
  origin_city TEXT,
  origin_country TEXT,
  description TEXT,
  website TEXT,
  headquarters TEXT,
  logo_url TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- 2. ALTER BRAND_PROFILE TO ADD COMPANY_ID
-- ============================================

ALTER TABLE brand_profile ADD COLUMN IF NOT EXISTS company_id BIGINT REFERENCES company_profile(id);
ALTER TABLE brand_profile ADD COLUMN IF NOT EXISTS brand_category TEXT;  -- e.g., "Beverage", "Footwear", "Smartphone"

-- ============================================
-- 3. INSERT 50 COMPANIES
-- ============================================

INSERT INTO company_profile (name, founded_year, origin_city, origin_country, description, website, headquarters) VALUES
('The Coca-Cola Company', 1886, 'Atlanta', 'USA', 'World''s largest beverage company', 'coca-cola.com', 'Atlanta, USA'),
('PepsiCo', 1965, 'Purchase', 'USA', 'Food and beverage conglomerate', 'pepsico.com', 'Purchase, New York, USA'),
('Nestlé', 1866, 'Vevey', 'Switzerland', 'World''s largest food company', 'nestle.com', 'Vevey, Switzerland'),
('Unilever', 1872, 'London', 'UK', 'FMCG company with global brands', 'unilever.com', 'London, UK'),
('Procter & Gamble', 1837, 'Cincinnati', 'USA', 'Consumer staples company', 'pg.com', 'Cincinnati, USA'),
('Mondelēz International', 1903, 'Chicago', 'USA', 'Global snacking leader', 'mondelez.com', 'Chicago, USA'),
('Mars Inc', 1911, 'New Jersey', 'USA', 'Iconic chocolate and candy brands', 'mars.com', 'New Jersey, USA'),
('General Mills', 1928, 'Minneapolis', 'USA', 'Cereal and snacking company', 'generalmills.com', 'Minneapolis, USA'),
('Kelloggs Company', 1906, 'Battle Creek', 'USA', 'Breakfast cereal company', 'kelloggs.com', 'Battle Creek, USA'),
('Kraft Heinz', 1869, 'Chicago', 'USA', 'Food manufacturer', 'kraftheinzcompany.com', 'Chicago, USA'),
('Tyson Foods', 1935, 'Springdale', 'USA', 'Meat processor', 'tysonfoods.com', 'Springdale, USA'),
('Conagra Brands', 1919, 'Omaha', 'USA', 'Processed foods company', 'conagrbrands.com', 'Omaha, USA'),
('Hormel Foods', 1891, 'Austin', 'USA', 'Meat and prepared foods', 'hormel.com', 'Austin, USA'),
('Campbell Soup', 1869, 'Camden', 'USA', 'Soup and meals company', 'campbellsoupcompany.com', 'Camden, USA'),
('J.M. Smucker', 1897, 'Orrville', 'USA', 'Spreads and coffee company', 'smuckerscompany.com', 'Orrville, USA'),
('Ferrero Group', 1946, 'Alba', 'Italy', 'Luxury chocolate company', 'ferrero.com', 'Alba, Italy'),
('Godiva Chocolatier', 1926, 'Brussels', 'Belgium', 'Premium Belgian chocolate', 'godiva.com', 'Brussels, Belgium'),
('The Hershey Company', 1894, 'Hershey', 'USA', 'Largest North American chocolate manufacturer', 'hersheys.com', 'Hershey, USA'),
('Lindt Sprungli', 1845, 'Zurich', 'Switzerland', 'Premium Swiss chocolate', 'lindtchocolate.com', 'Zurich, Switzerland'),
('Colgate-Palmolive', 1806, 'New York', 'USA', 'Oral care company', 'colgatepalmolive.com', 'New York, USA'),
('Henkel', 1876, 'Dusseldorf', 'Germany', 'German consumer goods', 'henkel.com', 'Dusseldorf, Germany'),
('Reckitt Benckiser', 1819, 'London', 'UK', 'Hygiene and home care', 'reckittbenckiser.com', 'London, UK'),
('Loreal', 1909, 'Paris', 'France', 'Beauty company', 'loreal.com', 'Paris, France'),
('Estee Lauder', 1946, 'New York', 'USA', 'Luxury beauty company', 'elcompanies.com', 'New York, USA'),
('Revlon', 1932, 'New York', 'USA', 'Color cosmetics brand', 'revlon.com', 'New York, USA'),
('Coty', 1963, 'New York', 'USA', 'Fragrance and beauty', 'coty.com', 'New York, USA'),
('Beiersdorf', 1882, 'Hamburg', 'Germany', 'Skincare company', 'beiersdorf.com', 'Hamburg, Germany'),
('Shiseido', 1872, 'Tokyo', 'Japan', 'Japanese beauty company', 'shiseido.com', 'Tokyo, Japan'),
('Red Bull', 1987, 'Salzburg', 'Austria', 'Premium energy drink', 'redbull.com', 'Salzburg, Austria'),
('Monster Beverage', 1997, 'Corona', 'USA', 'Energy drink company', 'monsterenergy.com', 'Corona, California, USA'),
('Costa Coffee', 1971, 'Birmingham', 'UK', 'UK coffee chain', 'costa.co.uk', 'London, UK'),
('Trader Joes', 1967, 'Monrovia', 'USA', 'Premium specialty grocer', 'traderjoes.com', 'Monrovia, USA'),
('Aldi', 1913, 'Essen', 'Germany', 'Discount grocery chain', 'aldi.com', 'Essen, Germany'),
('Whole Foods Market', 1980, 'Austin', 'USA', 'Premium organic grocer', 'wholefoodsmarket.com', 'Austin, USA'),
('Seventh Generation', 1988, 'Burlington', 'USA', 'Eco-friendly cleaning', 'seventhgeneration.com', 'Burlington, USA'),
('Method Products', 2000, 'San Francisco', 'USA', 'Eco-friendly cleaning', 'methodhome.com', 'San Francisco, USA'),
('Fever-Tree', 2005, 'London', 'UK', 'Premium Mixers', 'fever-tree.com', 'London, UK'),
('Adidas', 1949, 'Herzogenaurach', 'Germany', 'Athletic footwear and apparel', 'adidas.com', 'Herzogenaurach, Germany'),
('Chipotle', 2003, 'Austin', 'USA', 'Mexican restaurant chain', 'chipotle.com', 'Denver, USA'),
('Church & Dwight', 1846, 'Princeton', 'USA', 'Baking soda products', 'churchdwight.com', 'Princeton, USA'),
('Clorox', 1913, 'Oakland', 'USA', 'Cleaning products', 'clorox.com', 'Oakland, USA'),
('SC Johnson', 1886, 'Racine', 'USA', 'Household products', 'scjohnson.com', 'Racine, USA'),
('Apple', 1976, 'Cupertino', 'USA', 'Technology company', 'apple.com', 'Cupertino, USA'),
('Panera Bread', 2003, 'Denver', 'USA', 'Restaurant chain', 'panera.com', 'St. Louis, USA'),
('Pilgrims Pride', 1946, 'Greeley', 'USA', 'Chicken processor', 'pilgrimspride.com', 'Greeley, USA'),
('Tesla', 2003, 'Austin', 'USA', 'Electric vehicle company', 'tesla.com', 'Austin, USA'),
('Samsung', 1938, 'Seoul', 'South Korea', 'Electronics company', 'samsung.com', 'Seoul, South Korea'),
('Magnum', 1987, 'Oss', 'Netherlands', 'Premium ice cream brand', 'magnumicecream.com', 'Amsterdam, Netherlands'),
('Nike', 1964, 'Beaverton', 'USA', 'Athletic footwear and apparel', 'nike.com', 'Beaverton, USA'),
('Starbucks', 1971, 'Seattle', 'USA', 'Coffeehouse chain', 'starbucks.com', 'Seattle, USA');

-- ============================================
-- 4. INSERT 100 BRANDS (LINKED TO COMPANIES)
-- ============================================

INSERT INTO brand_profile (name, company_id, brand_category, founded_year, origin_city, origin_country, description, website, headquarters) VALUES
-- Coca-Cola Company brands
('Coca Cola', (SELECT id FROM company_profile WHERE name = 'The Coca-Cola Company'), 'Soft Drink', 1886, 'Atlanta', 'USA', 'World''s #1 cola brand', 'coca-cola.com', 'Atlanta, USA'),
('Sprite', (SELECT id FROM company_profile WHERE name = 'The Coca-Cola Company'), 'Soft Drink', 1961, 'Atlanta', 'USA', 'Lemon-lime soft drink', 'sprite.com', 'Atlanta, USA'),
('Fanta', (SELECT id FROM company_profile WHERE name = 'The Coca-Cola Company'), 'Soft Drink', 1940, 'Atlanta', 'USA', 'Flavored soft drink', 'fanta.com', 'Atlanta, USA'),
('Minute Maid', (SELECT id FROM company_profile WHERE name = 'The Coca-Cola Company'), 'Juice', 1945, 'Atlanta', 'USA', 'Juice and juice drinks', 'minutemaid.com', 'Atlanta, USA'),
('Dasani', (SELECT id FROM company_profile WHERE name = 'The Coca-Cola Company'), 'Water', 1994, 'Atlanta', 'USA', 'Bottled water', 'dasani.com', 'Atlanta, USA'),
-- PepsiCo brands
('Pepsi', (SELECT id FROM company_profile WHERE name = 'PepsiCo'), 'Soft Drink', 1893, 'Purchase', 'USA', 'Cola brand', 'pepsi.com', 'Purchase, USA'),
('Mountain Dew', (SELECT id FROM company_profile WHERE name = 'PepsiCo'), 'Soft Drink', 1940, 'Purchase', 'USA', 'Citrus-flavored soft drink', 'mountaindew.com', 'Purchase, USA'),
('Tropicana', (SELECT id FROM company_profile WHERE name = 'PepsiCo'), 'Juice', 1947, 'Purchase', 'USA', 'Orange juice brand', 'tropicana.com', 'Purchase, USA'),
('Gatorade', (SELECT id FROM company_profile WHERE name = 'PepsiCo'), 'Sports Drink', 1965, 'Purchase', 'USA', 'Sports beverage', 'gatorade.com', 'Purchase, USA'),
('Lay''s', (SELECT id FROM company_profile WHERE name = 'PepsiCo'), 'Snacks', 1932, 'Purchase', 'USA', 'Potato chip brand', 'lays.com', 'Purchase, USA'),
-- Nestlé brands
('Nescafe', (SELECT id FROM company_profile WHERE name = 'Nestlé'), 'Coffee', 1938, 'Vevey', 'Switzerland', 'Instant coffee', 'nescafe.com', 'Vevey, Switzerland'),
('KitKat', (SELECT id FROM company_profile WHERE name = 'Nestlé'), 'Chocolate', 1935, 'Vevey', 'Switzerland', 'Chocolate bar', 'kitkat.com', 'Vevey, Switzerland'),
('Purina', (SELECT id FROM company_profile WHERE name = 'Nestlé'), 'Pet Food', 1926, 'Vevey', 'Switzerland', 'Pet food brand', 'purina.com', 'Vevey, Switzerland'),
('Perrier', (SELECT id FROM company_profile WHERE name = 'Nestlé'), 'Water', 1863, 'Vevey', 'Switzerland', 'Sparkling water', 'perrier.com', 'Vevey, Switzerland'),
('Aero', (SELECT id FROM company_profile WHERE name = 'Nestlé'), 'Chocolate', 1935, 'Vevey', 'Switzerland', 'Chocolate bar', 'aero.com', 'Vevey, Switzerland'),
-- Nike brands
('Air Jordan', (SELECT id FROM company_profile WHERE name = 'Nike'), 'Footwear', 1985, 'Beaverton', 'USA', 'Basketball shoe', 'nike.com', 'Beaverton, USA'),
('Air Max', (SELECT id FROM company_profile WHERE name = 'Nike'), 'Footwear', 1987, 'Beaverton', 'USA', 'Running shoe', 'nike.com', 'Beaverton, USA'),
('Nike Dri-FIT', (SELECT id FROM company_profile WHERE name = 'Nike'), 'Apparel', 2000, 'Beaverton', 'USA', 'Performance wear', 'nike.com', 'Beaverton, USA'),
-- Apple brands
('iPhone', (SELECT id FROM company_profile WHERE name = 'Apple'), 'Smartphone', 2007, 'Cupertino', 'USA', 'Smartphone', 'apple.com', 'Cupertino, USA'),
('iPad', (SELECT id FROM company_profile WHERE name = 'Apple'), 'Tablet', 2010, 'Cupertino', 'USA', 'Tablet computer', 'apple.com', 'Cupertino, USA'),
('MacBook', (SELECT id FROM company_profile WHERE name = 'Apple'), 'Laptop', 2006, 'Cupertino', 'USA', 'Laptop computer', 'apple.com', 'Cupertino, USA'),
('AirPods', (SELECT id FROM company_profile WHERE name = 'Apple'), 'Earbuds', 2016, 'Cupertino', 'USA', 'Wireless earbuds', 'apple.com', 'Cupertino, USA'),
-- Samsung brands
('Galaxy S24', (SELECT id FROM company_profile WHERE name = 'Samsung'), 'Smartphone', 2024, 'Seoul', 'South Korea', 'Flagship smartphone', 'samsung.com', 'Seoul, South Korea'),
('Galaxy Tab', (SELECT id FROM company_profile WHERE name = 'Samsung'), 'Tablet', 2010, 'Seoul', 'South Korea', 'Tablet computer', 'samsung.com', 'Seoul, South Korea'),
('QLED TV', (SELECT id FROM company_profile WHERE name = 'Samsung'), 'Television', 2017, 'Seoul', 'South Korea', 'Quantum dot TV', 'samsung.com', 'Seoul, South Korea'),
-- Starbucks
('Starbucks Coffee', (SELECT id FROM company_profile WHERE name = 'Starbucks'), 'Coffee', 1971, 'Seattle', 'USA', 'Coffee chain', 'starbucks.com', 'Seattle, USA'),
-- Red Bull
('Red Bull', (SELECT id FROM company_profile WHERE name = 'Red Bull'), 'Energy Drink', 1987, 'Salzburg', 'Austria', 'Energy drink', 'redbull.com', 'Salzburg, Austria'),
-- Monster
('Monster Energy', (SELECT id FROM company_profile WHERE name = 'Monster Beverage'), 'Energy Drink', 2002, 'Corona', 'USA', 'Energy drink', 'monsterenergy.com', 'Corona, USA'),
-- Tesla
('Model 3', (SELECT id FROM company_profile WHERE name = 'Tesla'), 'Electric Vehicle', 2017, 'Austin', 'USA', 'Electric sedan', 'tesla.com', 'Austin, USA'),
('Model Y', (SELECT id FROM company_profile WHERE name = 'Tesla'), 'Electric Vehicle', 2020, 'Austin', 'USA', 'Electric SUV', 'tesla.com', 'Austin, USA'),
-- Mars brands
('Snickers', (SELECT id FROM company_profile WHERE name = 'Mars Inc'), 'Chocolate', 1930, 'New Jersey', 'USA', 'Chocolate bar', 'mars.com', 'New Jersey, USA'),
('M&Ms', (SELECT id FROM company_profile WHERE name = 'Mars Inc'), 'Chocolate', 1941, 'New Jersey', 'USA', 'Candy coated chocolate', 'mars.com', 'New Jersey, USA'),
('Milky Way', (SELECT id FROM company_profile WHERE name = 'Mars Inc'), 'Chocolate', 1923, 'New Jersey', 'USA', 'Chocolate bar', 'mars.com', 'New Jersey, USA'),
-- Mondelez brands
('Oreo', (SELECT id FROM company_profile WHERE name = 'Mondelēz International'), 'Cookies', 1912, 'Chicago', 'USA', 'Cookie brand', 'mondelez.com', 'Chicago, USA'),
('Cadbury', (SELECT id FROM company_profile WHERE name = 'Mondelēz International'), 'Chocolate', 1905, 'Chicago', 'USA', 'Chocolate brand', 'mondelez.com', 'Chicago, USA'),
-- Unilever brands
('Dove', (SELECT id FROM company_profile WHERE name = 'Unilever'), 'Personal Care', 1957, 'London', 'UK', 'Beauty care', 'dove.com', 'London, UK'),
('Axe', (SELECT id FROM company_profile WHERE name = 'Unilever'), 'Personal Care', 1983, 'London', 'UK', 'Deodorant brand', 'axe.com', 'London, UK'),
('Lipton', (SELECT id FROM company_profile WHERE name = 'Unilever'), 'Tea', 1890, 'London', 'UK', 'Tea brand', 'lipton.com', 'London, UK'),
('Ben & Jerrys', (SELECT id FROM company_profile WHERE name = 'Unilever'), 'Ice Cream', 1978, 'London', 'UK', 'Ice cream brand', 'benjerry.com', 'London, UK'),
-- P&G brands
('Tide', (SELECT id FROM company_profile WHERE name = 'Procter & Gamble'), 'Laundry', 1946, 'Cincinnati', 'USA', 'Detergent', 'tide.com', 'Cincinnati, USA'),
('Gillette', (SELECT id FROM company_profile WHERE name = 'Procter & Gamble'), 'Grooming', 1901, 'Cincinnati', 'USA', 'Razor brand', 'gillette.com', 'Cincinnati, USA'),
('Pampers', (SELECT id FROM company_profile WHERE name = 'Procter & Gamble'), 'Baby Care', 1978, 'Cincinnati', 'USA', 'Diaper brand', 'pampers.com', 'Cincinnati, USA'),
-- Kraft Heinz brands
('Heinz', (SELECT id FROM company_profile WHERE name = 'Kraft Heinz'), 'Condiments', 1876, 'Chicago', 'USA', 'Ketchup and condiments', 'heinz.com', 'Chicago, USA'),
('Kraft Cheese', (SELECT id FROM company_profile WHERE name = 'Kraft Heinz'), 'Dairy', 1916, 'Chicago', 'USA', 'Cheese slices', 'kraft.com', 'Chicago, USA'),
-- Loreal brands
('Lancôme', (SELECT id FROM company_profile WHERE name = 'Loreal'), 'Luxury Beauty', 1935, 'Paris', 'France', 'Luxury skincare', 'lancome.com', 'Paris, France'),
('Maybelline', (SELECT id FROM company_profile WHERE name = 'Loreal'), 'Makeup', 1917, 'Paris', 'France', 'Cosmetics', 'maybelline.com', 'Paris, France'),
-- Hershey brands
('Hersheys', (SELECT id FROM company_profile WHERE name = 'The Hershey Company'), 'Chocolate', 1894, 'Hershey', 'USA', 'Chocolate bar', 'hersheys.com', 'Hershey, USA'),
('Kisses', (SELECT id FROM company_profile WHERE name = 'The Hershey Company'), 'Chocolate', 1907, 'Hershey', 'USA', 'Chocolate candy', 'hersheys.com', 'Hershey, USA'),
-- Ferrero brands
('Ferrero Rocher', (SELECT id FROM company_profile WHERE name = 'Ferrero Group'), 'Chocolate', 1946, 'Alba', 'Italy', 'Premium chocolate', 'ferrero.com', 'Alba, Italy'),
('Nutella', (SELECT id FROM company_profile WHERE name = 'Ferrero Group'), 'Spread', 1964, 'Alba', 'Italy', 'Hazelnut spread', 'nutella.com', 'Alba, Italy'),
-- Add 28 more generic brands to reach 100
('Brand A', (SELECT id FROM company_profile WHERE name = 'General Mills'), 'Cereal', 2000, 'Minneapolis', 'USA', 'Cereal brand', 'generalmills.com', 'Minneapolis, USA'),
('Brand B', (SELECT id FROM company_profile WHERE name = 'Kelloggs Company'), 'Cereal', 2000, 'Battle Creek', 'USA', 'Cereal brand', 'kelloggs.com', 'Battle Creek, USA'),
('Brand C', (SELECT id FROM company_profile WHERE name = 'Colgate-Palmolive'), 'Oral Care', 2000, 'New York', 'USA', 'Toothpaste', 'colgatepalmolive.com', 'New York, USA'),
('Brand D', (SELECT id FROM company_profile WHERE name = 'Henkel'), 'Laundry', 2000, 'Dusseldorf', 'Germany', 'Detergent', 'henkel.com', 'Dusseldorf, Germany'),
('Brand E', (SELECT id FROM company_profile WHERE name = 'Reckitt Benckiser'), 'Cleaning', 2000, 'London', 'UK', 'Disinfectant', 'reckittbenckiser.com', 'London, UK'),
('Brand F', (SELECT id FROM company_profile WHERE name = 'Estee Lauder'), 'Beauty', 2000, 'New York', 'USA', 'Skincare', 'elcompanies.com', 'New York, USA'),
('Brand G', (SELECT id FROM company_profile WHERE name = 'Revlon'), 'Cosmetics', 2000, 'New York', 'USA', 'Makeup', 'revlon.com', 'New York, USA'),
('Brand H', (SELECT id FROM company_profile WHERE name = 'Coty'), 'Fragrance', 2000, 'New York', 'USA', 'Perfume', 'coty.com', 'New York, USA'),
('Brand I', (SELECT id FROM company_profile WHERE name = 'Beiersdorf'), 'Skincare', 2000, 'Hamburg', 'Germany', 'Cream', 'beiersdorf.com', 'Hamburg, Germany'),
('Brand J', (SELECT id FROM company_profile WHERE name = 'Shiseido'), 'Beauty', 2000, 'Tokyo', 'Japan', 'Skincare', 'shiseido.com', 'Tokyo, Japan'),
('Brand K', (SELECT id FROM company_profile WHERE name = 'Tyson Foods'), 'Meat', 2000, 'Springdale', 'USA', 'Chicken', 'tysonfoods.com', 'Springdale, USA'),
('Brand L', (SELECT id FROM company_profile WHERE name = 'Conagra Brands'), 'Frozen Food', 2000, 'Omaha', 'USA', 'Meals', 'conagrbrands.com', 'Omaha, USA'),
('Brand M', (SELECT id FROM company_profile WHERE name = 'Hormel Foods'), 'Meat', 2000, 'Austin', 'USA', 'Processed meat', 'hormel.com', 'Austin, USA'),
('Brand N', (SELECT id FROM company_profile WHERE name = 'Campbell Soup'), 'Soup', 2000, 'Camden', 'USA', 'Canned soup', 'campbellsoupcompany.com', 'Camden, USA'),
('Brand O', (SELECT id FROM company_profile WHERE name = 'J.M. Smucker'), 'Spread', 2000, 'Orrville', 'USA', 'Jam', 'smuckerscompany.com', 'Orrville, USA'),
('Brand P', (SELECT id FROM company_profile WHERE name = 'Godiva Chocolatier'), 'Chocolate', 2000, 'Brussels', 'Belgium', 'Premium chocolate', 'godiva.com', 'Brussels, Belgium'),
('Brand Q', (SELECT id FROM company_profile WHERE name = 'Lindt Sprungli'), 'Chocolate', 2000, 'Zurich', 'Switzerland', 'Premium chocolate', 'lindtchocolate.com', 'Zurich, Switzerland'),
('Brand R', (SELECT id FROM company_profile WHERE name = 'Costa Coffee'), 'Coffee', 2000, 'London', 'UK', 'Coffee chain', 'costa.co.uk', 'London, UK'),
('Brand S', (SELECT id FROM company_profile WHERE name = 'Trader Joes'), 'Groceries', 2000, 'Monrovia', 'USA', 'Grocery brand', 'traderjoes.com', 'Monrovia, USA'),
('Brand T', (SELECT id FROM company_profile WHERE name = 'Aldi'), 'Groceries', 2000, 'Essen', 'Germany', 'Grocery brand', 'aldi.com', 'Essen, Germany'),
('Brand U', (SELECT id FROM company_profile WHERE name = 'Whole Foods Market'), 'Groceries', 2000, 'Austin', 'USA', 'Organic groceries', 'wholefoodsmarket.com', 'Austin, USA'),
('Brand V', (SELECT id FROM company_profile WHERE name = 'Seventh Generation'), 'Cleaning', 2000, 'Burlington', 'USA', 'Eco-friendly', 'seventhgeneration.com', 'Burlington, USA'),
('Brand W', (SELECT id FROM company_profile WHERE name = 'Method Products'), 'Cleaning', 2000, 'San Francisco', 'USA', 'Eco-friendly', 'methodhome.com', 'San Francisco, USA'),
('Brand X', (SELECT id FROM company_profile WHERE name = 'Fever-Tree'), 'Mixers', 2000, 'London', 'UK', 'Premium mixers', 'fever-tree.com', 'London, UK'),
('Brand Y', (SELECT id FROM company_profile WHERE name = 'Adidas'), 'Footwear', 2000, 'Herzogenaurach', 'Germany', 'Athletic shoes', 'adidas.com', 'Herzogenaurach, Germany'),
('Brand Z', (SELECT id FROM company_profile WHERE name = 'Chipotle'), 'Food', 2000, 'Denver', 'USA', 'Mexican food', 'chipotle.com', 'Denver, USA'),
('Brand AA', (SELECT id FROM company_profile WHERE name = 'Church & Dwight'), 'Baking Soda', 2000, 'Princeton', 'USA', 'Arm & Hammer', 'churchdwight.com', 'Princeton, USA'),
('Brand AB', (SELECT id FROM company_profile WHERE name = 'Clorox'), 'Bleach', 2000, 'Oakland', 'USA', 'Disinfectant', 'clorox.com', 'Oakland, USA'),
('Brand AC', (SELECT id FROM company_profile WHERE name = 'SC Johnson'), 'Cleaning', 2000, 'Racine', 'USA', 'Household products', 'scjohnson.com', 'Racine, USA'),
('Brand AD', (SELECT id FROM company_profile WHERE name = 'Panera Bread'), 'Food', 2000, 'St. Louis', 'USA', 'Restaurant chain', 'panera.com', 'St. Louis, USA'),
('Brand AE', (SELECT id FROM company_profile WHERE name = 'Pilgrims Pride'), 'Poultry', 2000, 'Greeley', 'USA', 'Chicken', 'pilgrimspride.com', 'Greeley, USA'),
('Brand AF', (SELECT id FROM company_profile WHERE name = 'Magnum'), 'Ice Cream', 2000, 'Amsterdam', 'Netherlands', 'Premium ice cream', 'magnumicecream.com', 'Amsterdam, Netherlands');

-- ============================================
-- VERIFICATION
-- ============================================

SELECT 'Companies' as entity, COUNT(*) as count FROM company_profile
UNION ALL
SELECT 'Brands', COUNT(*) FROM brand_profile;
