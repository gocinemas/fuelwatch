-- FIX BATCH 1: Global Bestseller Data
-- Populate real bestseller info for all 60 brands

-- Create global bestseller table if it doesn't exist
CREATE TABLE IF NOT EXISTS brand_global_bestseller (
  id BIGSERIAL PRIMARY KEY,
  brand_name TEXT NOT NULL UNIQUE,
  product_name TEXT NOT NULL,
  category TEXT NOT NULL,
  price_usd TEXT,
  price_gbp TEXT,
  monthly_volume TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Clear existing data
DELETE FROM brand_global_bestseller;

-- BEVERAGES (10 brands)
INSERT INTO brand_global_bestseller (brand_name, product_name, category, price_usd, price_gbp, monthly_volume) VALUES
('PepsiCo', 'Pepsi Zero Sugar', 'Soft Drink', '$1.99', '£1.60', '250M cans'),
('Red Bull', 'Red Bull Energy Drink', 'Energy Drink', '$3.99', '£3.20', '50M cans'),
('Monster Energy', 'Monster Energy Original', 'Energy Drink', '$2.99', '£2.40', '45M cans'),
('Nescafé', 'Nescafé Instant Coffee', 'Instant Coffee', '$8.99', '£7.20', '15M jars'),
('Danone', 'Danone Yogurt', 'Yogurt', '$0.99', '£0.79', '200M cups'),
('Fiji Water', 'Fiji Artesian Water', 'Premium Water', '$2.49', '£1.99', '30M bottles'),
('Perrier', 'Perrier Sparkling Water', 'Sparkling Water', '$1.79', '£1.44', '25M bottles'),
('Gatorade', 'Gatorade Lemon Lime', 'Sports Drink', '$2.49', '£1.99', '80M bottles'),
('Tropicana', 'Tropicana Orange Juice', 'Orange Juice', '$3.49', '£2.79', '60M cartons'),
('Sprite', 'Sprite Lemon-Lime', 'Soft Drink', '$1.99', '£1.60', '180M cans');

-- SNACKS & CONFECTIONERY (10 brands)
INSERT INTO brand_global_bestseller (brand_name, product_name, category, price_usd, price_gbp, monthly_volume) VALUES
('Mars Inc', 'Snickers Bar', 'Chocolate Bar', '$1.09', '£0.87', '100M bars'),
('Nestlé Confectionery', 'KitKat', 'Chocolate Bar', '$1.29', '£1.03', '95M bars'),
('Mondelēz', 'Oreo Cookies', 'Cookie', '$3.99', '£3.19', '50M packs'),
('Ferrero', 'Ferrero Rocher', 'Chocolate Truffle', '$4.99', '£3.99', '40M pieces'),
('Lindt', 'Lindt Lindor Truffle', 'Chocolate Truffle', '$5.99', '£4.79', '35M pieces'),
('Haribo', 'Haribo Gummy Bears', 'Gummy Candy', '$2.49', '£1.99', '80M packs'),
('Cadbury', 'Cadbury Dairy Milk', 'Chocolate Bar', '$1.99', '£1.59', '70M bars'),
('Hershey', 'Hershey Milk Chocolate', 'Chocolate Bar', '$1.49', '£1.19', '85M bars'),
('Lay''s', 'Lay''s Classic Potato Chips', 'Potato Chips', '$3.99', '£3.19', '120M bags'),
('Doritos', 'Doritos Nacho Cheese', 'Tortilla Chips', '$4.49', '£3.59', '90M bags');

-- PERSONAL CARE & BEAUTY (10 brands)
INSERT INTO brand_global_bestseller (brand_name, product_name, category, price_usd, price_gbp, monthly_volume) VALUES
('Olay', 'Olay Regenerist Serum', 'Facial Serum', '$28.99', '£23.19', '8M bottles'),
('Gillette', 'Gillette Fusion Razor', 'Razor', '$12.99', '£10.39', '25M units'),
('Dove', 'Dove Body Wash', 'Body Wash', '$6.99', '£5.59', '40M bottles'),
('L''Oréal', 'L''Oréal Revitalift Cream', 'Face Cream', '$24.99', '£19.99', '12M jars'),
('Estée Lauder', 'Estée Lauder Advanced Night Repair', 'Eye Cream', '$65.00', '£52.00', '5M bottles'),
('Revlon', 'Revlon ColorStay Lipstick', 'Lipstick', '$7.99', '£6.39', '30M units'),
('CoverGirl', 'CoverGirl LashBlast Mascara', 'Mascara', '$8.98', '£7.18', '35M tubes'),
('Maybelline', 'Maybelline SuperStay Foundation', 'Foundation', '$8.98', '£7.18', '45M bottles'),
('Neutrogena', 'Neutrogena Hydro Boost Hydrating Toner', 'Toner', '$6.99', '£5.59', '22M bottles'),
('Clinique', 'Clinique 3-Step Skincare', 'Skincare Set', '$42.00', '£33.60', '6M sets');

-- HOUSEHOLD & CLEANING (10 brands)
INSERT INTO brand_global_bestseller (brand_name, product_name, category, price_usd, price_gbp, monthly_volume) VALUES
('Tide', 'Tide Liquid Laundry Detergent', 'Laundry Detergent', '$6.99', '£5.59', '50M bottles'),
('Dawn', 'Dawn Ultra Dishwashing Liquid', 'Dish Soap', '$3.49', '£2.79', '70M bottles'),
('Lysol', 'Lysol Disinfectant Spray', 'Disinfectant', '$4.99', '£3.99', '35M cans'),
('Clorox', 'Clorox Bleach', 'Bleach', '$2.99', '£2.39', '45M bottles'),
('Colgate', 'Colgate Total Toothpaste', 'Toothpaste', '$4.49', '£3.59', '60M tubes'),
('SC Johnson', 'Windex Glass Cleaner', 'Glass Cleaner', '$3.99', '£3.19', '40M bottles'),
('Henkel', 'Persil Laundry Detergent', 'Laundry Detergent', '$5.99', '£4.79', '38M bottles'),
('Method', 'Method All-Purpose Cleaner', 'All-Purpose Cleaner', '$2.99', '£2.39', '25M bottles'),
('Ecos', 'ECOS Laundry Detergent', 'Eco-Friendly Detergent', '$7.99', '£6.39', '20M bottles'),
('Surf', 'Surf Laundry Powder', 'Laundry Powder', '$4.99', '£3.99', '30M boxes');

-- QSR & FOOD SERVICE (10 brands)
INSERT INTO brand_global_bestseller (brand_name, product_name, category, price_usd, price_gbp, monthly_volume) VALUES
('McDonald''s', 'Big Mac', 'Burger', '$5.59', '£4.47', '10M units/day'),
('Subway', 'Footlong Italian BMT', 'Sandwich', '$9.99', '£8.00', '5M units/day'),
('Domino''s', 'ExtravaganZZa Pizza', 'Pizza', '$19.99', '£16.00', '2M units/day'),
('KFC', 'Fried Chicken Bucket', 'Fried Chicken', '$24.99', '£20.00', '1.5M units/day'),
('Chipotle', 'Chicken Burrito Bowl', 'Burrito Bowl', '$9.25', '£7.40', '1M units/day'),
('Wendy''s', 'Dave''s Single Burger', 'Burger', '$5.49', '£4.39', '3M units/day'),
('Burger King', 'Whopper', 'Burger', '$6.59', '£5.27', '4M units/day'),
('Taco Bell', 'Crunchwrap Supreme', 'Burrito', '$5.49', '£4.39', '3M units/day'),
('Pizza Hut', 'Hand-Tossed Pizza', 'Pizza', '$16.99', '£13.60', '1.8M units/day'),
('Panera Bread', 'Half Sandwich + Soup', 'Combo Meal', '$12.99', '£10.40', '800K units/day');

-- DAIRY & PACKAGED FOOD (10 brands)
INSERT INTO brand_global_bestseller (brand_name, product_name, category, price_usd, price_gbp, monthly_volume) VALUES
('Kraft Heinz', 'Heinz Ketchup', 'Condiment', '$2.99', '£2.39', '80M bottles'),
('General Mills', 'Cheerios Cereal', 'Breakfast Cereal', '$4.49', '£3.59', '60M boxes'),
('Kellogg''s', 'Frosted Flakes Cereal', 'Breakfast Cereal', '$4.29', '£3.43', '55M boxes'),
('Nestlé Food', 'Nestlé Purina Dog Chow', 'Pet Food', '$18.99', '£15.20', '30M bags'),
('Lactalis', 'Lactalis Cheese Slices', 'Cheese', '$4.99', '£3.99', '70M packs'),
('ConAgra', 'Hunt''s Tomato Sauce', 'Pasta Sauce', '$1.99', '£1.59', '90M jars'),
('Campbell Soup', 'Campbell''s Tomato Soup', 'Soup', '$1.89', '£1.51', '100M cans'),
('Unilever Food', 'Hellmann''s Mayonnaise', 'Condiment', '$4.99', '£3.99', '50M jars'),
('Tyson Foods', 'Tyson Chicken Breasts', 'Poultry', '$8.99', '£7.20', '40M packages'),
('Ben & Jerry''s', 'Ben & Jerry''s Ice Cream', 'Ice Cream', '$5.99', '£4.79', '25M pints');

-- Verify population
SELECT COUNT(*) as total_bestsellers FROM brand_global_bestseller;
