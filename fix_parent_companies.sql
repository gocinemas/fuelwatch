-- Fill in missing parent_company values for brands without them

-- PepsiCo brands
UPDATE brand_phase1_intelligence SET parent_company = 'PepsiCo' WHERE brand_name IN ('Gatorade', 'Tropicana', 'Lay''s', 'Doritos', 'Cheetos', 'Pringles', 'Pepsi');

-- Mondelez brands
UPDATE brand_phase1_intelligence SET parent_company = 'Mondelez International' WHERE brand_name IN ('Oreo', 'Cadbury');

-- Restaurant Brands International
UPDATE brand_phase1_intelligence SET parent_company = 'Restaurant Brands International' WHERE brand_name IN ('Domino''s', 'Burger King');

-- Yum! Brands
UPDATE brand_phase1_intelligence SET parent_company = 'Yum! Brands' WHERE brand_name IN ('KFC', 'Pizza Hut', 'Taco Bell');

-- Subway
UPDATE brand_phase1_intelligence SET parent_company = 'Subway' WHERE brand_name = 'Subway';

-- Nestlé brands (fill remaining)
UPDATE brand_phase1_intelligence SET parent_company = 'Nestlé' WHERE brand_name IN ('Nescafé', 'Minute Maid', 'Kit Kat', 'Aero');

-- Lipton (Unilever)
UPDATE brand_phase1_intelligence SET parent_company = 'Unilever' WHERE brand_name = 'Lipton';

-- Lotus Herbals
UPDATE brand_phase1_intelligence SET parent_company = 'Lotus Herbals' WHERE brand_name = 'Lotus Herbals';

-- Neutrogena (Johnson & Johnson)
UPDATE brand_phase1_intelligence SET parent_company = 'Johnson & Johnson' WHERE brand_name = 'Neutrogena';

-- Verify the update
SELECT COUNT(*) as total_brands, COUNT(parent_company) as with_company, COUNT(DISTINCT parent_company) as unique_companies FROM brand_phase1_intelligence;
SELECT DISTINCT parent_company FROM brand_phase1_intelligence WHERE parent_company IS NOT NULL ORDER BY parent_company;
