#!/usr/bin/env python3
"""Check brand request status in Supabase"""

import os
from supabase import create_client
import json

sb_url = os.environ.get("SUPABASE_URL")
sb_key = os.environ.get("SUPABASE_KEY")

if not sb_url or not sb_key:
    print("❌ Supabase env vars not set")
    exit(1)

sb = create_client(sb_url, sb_key)

# Get all requests
try:
    response = sb.table("brand_data_requests").select("*").execute()
    requests = response.data
    
    print(f"\n📋 Brand Requests Status ({len(requests)} total)\n")
    
    for req in requests:
        status = req.get('status', 'unknown')
        brand = req.get('brand_name', 'Unknown')
        created = req.get('created_at', '')[:10] if req.get('created_at') else ''
        completed = req.get('completed_at')
        
        emoji = '⏳' if status == 'pending' else '✅' if status == 'collected' else '❌'
        print(f"{emoji} {brand:20} → {status:12} ({created})")
        
except Exception as e:
    print(f"Error: {e}")
