#!/usr/bin/env python3
"""
Insert Phase 1b Batch 1: 30 new brand records
Run: railway run python3 insert_phase1b_batch1.py
"""

import os
import json
from supabase import create_client, Client

def insert_batch1_brands():
    """Insert 30 new brand records from batch 1"""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        print("❌ Credentials not set")
        return False

    sb: Client = create_client(url, key)

    print("\n[batch1_insert] Loading phase1b_additional_brands_batch1.json...\n")

    try:
        with open("phase1b_additional_brands_batch1.json", "r") as f:
            records = json.load(f)
    except Exception as e:
        print(f"❌ Could not load data file: {e}")
        return False

    print(f"✓ Loaded {len(records)} records\n[batch1_insert] Inserting into Supabase...\n")

    inserted = 0
    errors = 0

    for i, record in enumerate(records):
        try:
            # Upsert to handle duplicates
            result = sb.table("brand_phase1_intelligence").upsert(record).execute()

            brand = record.get("brand_name")
            market = record.get("market_country")
            print(f"  [{i + 1}/{len(records)}] ✓ {brand:20s} ({market:6s})")
            inserted += 1

        except Exception as e:
            brand = record.get("brand_name")
            market = record.get("market_country")
            print(f"  [{i + 1}/{len(records)}] ❌ {brand:20s} ({market:6s}) - {str(e)[:50]}")
            errors += 1

    print(f"\n[batch1_insert] Summary:")
    print(f"  Total: {len(records)}")
    print(f"  Inserted: {inserted}")
    print(f"  Errors: {errors}")

    if inserted > 0:
        # Verify totals
        total_result = sb.table("brand_phase1_intelligence").select("count", count="exact").execute()
        total = total_result.count if hasattr(total_result, 'count') else 0

        unique_result = sb.table("brand_phase1_intelligence").select("brand_name").execute()
        unique_brands = len(set(row['brand_name'] for row in unique_result.data)) if unique_result.data else 0

        print(f"\n[batch1_insert] Database Now Contains:")
        print(f"  Total Records: {total}")
        print(f"  Unique Brands: {unique_brands}")

        if inserted == len(records):
            print(f"\n✅ Batch 1 inserted successfully!")
            return True
        else:
            print(f"\n⚠️  Partial insert ({inserted}/{len(records)})")
            return inserted > 0
    else:
        print(f"\n❌ No records inserted")
        return False


if __name__ == "__main__":
    success = insert_batch1_brands()
    if not success:
        exit(1)
