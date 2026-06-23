#!/usr/bin/env python3
import os
from supabase import create_client

sb_url = os.environ.get("SUPABASE_URL")
sb_key = os.environ.get("SUPABASE_KEY")

if not sb_url or not sb_key:
    print("❌ Env vars not set locally")
    exit(1)

try:
    sb = create_client(sb_url, sb_key)
    response = sb.table("brand_data_requests").select("*").order("created_at", desc=True).limit(10).execute()
    
    print(f"\n📋 Last 10 requests in DB:\n")
    for r in response.data:
        print(f"  {r['brand_name']:15} | {r['status']:10} | {r['created_at'][:10]}")
    
except Exception as e:
    print(f"Error: {e}")
