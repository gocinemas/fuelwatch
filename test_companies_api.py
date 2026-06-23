#!/usr/bin/env python3
import os
from supabase import create_client

sb_url = os.environ.get("SUPABASE_URL")
sb_key = os.environ.get("SUPABASE_KEY")

if not sb_url or not sb_key:
    print("❌ Env vars not set")
    exit(1)

try:
    sb = create_client(sb_url, sb_key)
    print("✅ Connected to Supabase\n")
    
    # Test query
    print("Testing: SELECT parent_company FROM brand_phase1_intelligence\n")
    result = sb.table("brand_phase1_intelligence").select("parent_company").execute()
    
    if result.data:
        print(f"✅ Query returned {len(result.data)} rows\n")
        
        # Count unique companies
        companies = {}
        for row in result.data:
            company = row.get("parent_company", "").strip()
            if company and company.lower() != "unknown":
                companies[company] = companies.get(company, 0) + 1
        
        print(f"📊 Found {len(companies)} unique companies:")
        for company, count in sorted(companies.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"   {company}: {count} brands")
    else:
        print("❌ No data returned")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
