-- Corrected parent_company assignments

-- PepsiCo brands ✅
UPDATE brand_phase1_intelligence SET parent_company = 'PepsiCo' WHERE brand_name IN ('Gatorade', 'Tropicana', 'Lay''s', 'Doritos', 'Cheetos', 'Pringles', 'Pepsi');

-- Mondelez International ✅
UPDATE brand_phase1_intelligence SET parent_company = 'Mondelez International' WHERE brand_name IN ('Oreo', 'Cadbury');

-- Yum! Brands ✅
UPDATE brand_phase1_intelligence SET parent_company = 'Yum! Brands' WHERE brand_name IN ('KFC', 'Pizza Hut', 'Taco Bell');

-- Domino's is independent (public company) ✅
UPDATE brand_phase1_intelligence SET parent_company = 'Domino''s Pizza Inc.' WHERE brand_name = 'Domino''s';

-- Subway is independent (recently acquired by Roark Capital, but operates as Subway)
UPDATE brand_phase1_intelligence SET parent_company = 'Subway' WHERE brand_name = 'Subway';

-- Nestlé brands ✅
UPDATE brand_phase1_intelligence SET parent_company = 'Nestlé' WHERE brand_name IN ('Nescafé', 'Minute Maid', 'KitKat', 'Aero');

-- Unilever ✅ (Lipton, Pond's, etc.)
UPDATE brand_phase1_intelligence SET parent_company = 'Unilever' WHERE brand_name IN ('Lipton', 'Pond''s');

-- Lotus Herbals (independent Indian company) ✅
UPDATE brand_phase1_intelligence SET parent_company = 'Lotus Herbals' WHERE brand_name = 'Lotus Herbals';

-- Johnson & Johnson ✅
UPDATE brand_phase1_intelligence SET parent_company = 'Johnson & Johnson' WHERE brand_name = 'Neutrogena';

-- Verify
SELECT COUNT(DISTINCT parent_company) as companies FROM brand_phase1_intelligence WHERE parent_company IS NOT NULL;
SELECT parent_company, COUNT(*) as brand_count FROM brand_phase1_intelligence WHERE parent_company IS NOT NULL GROUP BY parent_company ORDER BY brand_count DESC;
