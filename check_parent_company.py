#!/usr/bin/env python3
"""Check parent_company data status"""

# Sample SQL to verify in Supabase SQL Editor:
sql = """
-- Check how many brands have parent_company set
SELECT 
  COUNT(*) as total_brands,
  COUNT(parent_company) as with_parent_company,
  COUNT(DISTINCT parent_company) as unique_companies
FROM brand_phase1_intelligence;

-- Show first 20 brands and their parent companies
SELECT DISTINCT brand_name, parent_company, category 
FROM brand_phase1_intelligence 
LIMIT 20;
"""

print("Copy & paste this into Supabase SQL Editor to check:\n")
print(sql)
