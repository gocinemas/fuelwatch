#!/usr/bin/env python3
"""Check which brands are missing data for markets"""

# SQL to run in Supabase to find gaps
sql = """
-- Find brands that don't have all 3 markets (UK, USA, India)
SELECT 
    brand_name,
    COUNT(DISTINCT market_country) as market_count,
    STRING_AGG(DISTINCT market_country, ', ') as markets_present,
    CASE 
        WHEN COUNT(DISTINCT market_country) = 3 THEN '✅ Complete'
        WHEN COUNT(DISTINCT market_country) = 2 THEN '⚠️ Incomplete'
        WHEN COUNT(DISTINCT market_country) = 1 THEN '❌ Missing'
    END as status
FROM brand_phase1_intelligence
GROUP BY brand_name
HAVING COUNT(DISTINCT market_country) < 3
ORDER BY market_count DESC, brand_name;

-- Show what markets each brand has
SELECT 
    brand_name,
    STRING_AGG(DISTINCT market_country, ', ' ORDER BY market_country) as markets,
    COUNT(*) as total_rows
FROM brand_phase1_intelligence
GROUP BY brand_name
ORDER BY brand_name;
"""

print("Copy & paste into Supabase SQL Editor:\n")
print(sql)
