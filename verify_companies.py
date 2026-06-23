#!/usr/bin/env python3
"""Check if parent_company data exists in database"""

import os
import sys

# For Railway testing - would need env vars set
sb_url = os.environ.get("SUPABASE_URL")
sb_key = os.environ.get("SUPABASE_KEY")

if not sb_url:
    print("⚠️  This script needs SUPABASE_URL & SUPABASE_KEY env vars")
    print("\nQuick test without env vars:")
    print("  Go to: intel.humanagency.co/brand")
    print("  Open DevTools (F12) → Network tab")
    print("  Refresh → Click on 'companies' request")
    print("  Check Response tab for error details")
    exit(0)

from supabase import create_client

try:
    sb = create_client(sb_url, sb_key)
    result = sb.table("brand_phase1_intelligence").select(
        "brand_name, parent_company"
    ).limit(20).execute()
    
    print(f"✅ Found {len(result.data)} brand records (showing first 20):\n")
    
    for row in result.data:
        print(f"  {row.get('brand_name', 'N/A'):20} → {row.get('parent_company', 'N/A')}")
        
except Exception as e:
    print(f"❌ Error: {e}")
