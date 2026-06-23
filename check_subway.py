#!/usr/bin/env python3
import os
from supabase import create_client

sb_url = os.environ.get("SUPABASE_URL")
sb_key = os.environ.get("SUPABASE_KEY")

if not sb_url or not sb_key:
    print("❌ Env vars not set locally\n")
    print("SQL to run in Supabase:")
    print("SELECT * FROM brand_phase1_intelligence WHERE brand_name = 'Subway' AND market_country = 'India';")
    print("\nOr check all Subway entries:")
    print("SELECT * FROM brand_phase1_intelligence WHERE brand_name = 'Subway';")
    exit(0)

try:
    sb = create_client(sb_url, sb_key)
    result = sb.table("brand_phase1_intelligence").select("*").eq("brand_name", "Subway").eq("market_country", "India").execute()
    
    if result.data:
        print(f"✅ Subway India found!")
        for row in result.data:
            print(f"  Brand: {row.get('brand_name')}")
            print(f"  Market: {row.get('market_country')}")
            print(f"  Category: {row.get('category')}")
            print(f"  Price: {row.get('price_local')}")
    else:
        print("❌ Subway not found for India market")
        
        # Check if Subway exists for other markets
        result2 = sb.table("brand_phase1_intelligence").select("*").eq("brand_name", "Subway").execute()
        if result2.data:
            print("\n✅ Subway found in these markets:")
            for row in result2.data:
                print(f"  - {row.get('market_country')}")
        else:
            print("❌ Subway not in database at all")
            
except Exception as e:
    print(f"Error: {e}")
