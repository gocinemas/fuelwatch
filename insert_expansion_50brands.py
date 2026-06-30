#!/usr/bin/env python3
"""
Intel Phase 1 Expansion: Insert 50 new brands (150 rows across 3 markets)
Expands Intel from 60 brands to 110+ brands

Categories:
  - QSR: 15 brands (McDonald's, KFC, Subway, Chipotle, Nando's, Wagamama, Pret, Leon, Five Guys, Domino's, Taco Bell, Steak & Shake, Cosy Club, Benihana, Zaxby's)
  - Fashion: 15 brands (Nike, Adidas, Zara, H&M, Gap, Uniqlo, Prada, Gucci, Tommy Hilfiger, Ralph Lauren, Levi's, Dr. Martens, COS, ASOS, Shein)
  - Tech: 10 brands (Apple, Samsung, Google, Microsoft, Amazon, Dell, HP, Sony, LG, OnePlus)
  - Beauty: 10 brands (MAC, Sephora, Urban Decay, Kylie Cosmetics, Charlotte Tilbury, Fenty Beauty, Morphe, Too Faced, Drunk Elephant, Paula's Choice)

Run: railway run python3 insert_expansion_50brands.py
"""

import os
import json
from supabase import create_client, Client

def insert_expansion_brands():
    """Insert 50 new brands (150 rows) from expansion data"""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        print("❌ Credentials not set. Run with: railway run python3 insert_expansion_50brands.py")
        return False

    sb: Client = create_client(url, key)

    print("\n" + "=" * 70)
    print("INTEL PHASE 1 EXPANSION: 50 New Brands")
    print("=" * 70)
    print("\n[expansion_insert] Loading brand_expansion_50brands.json...\n")

    try:
        with open("brand_expansion_50brands.json", "r") as f:
            records = json.load(f)
    except Exception as e:
        print(f"❌ Could not load data file: {e}")
        return False

    print(f"✓ Loaded {len(records)} records ({len(records) // 3} brands × 3 markets)\n")

    # Show summary by category
    categories = {}
    for r in records:
        cat = r.get("category")
        if cat not in categories:
            categories[cat] = 0
        categories[cat] += 1

    print("[expansion_insert] Summary by Category:")
    for cat in sorted(categories.keys()):
        count = categories[cat]
        print(f"  {cat.upper():10s}: {count // 3:2d} brands × 3 markets = {count:3d} rows")

    print(f"\n[expansion_insert] Inserting into Supabase...\n")

    inserted = 0
    errors = 0
    error_details = []

    for i, record in enumerate(records):
        try:
            # Upsert to handle potential duplicates gracefully
            result = sb.table("brand_phase1_intelligence").upsert(record).execute()

            brand = record.get("brand_name", "Unknown")
            market = record.get("market_country", "XX")
            category = record.get("category", "?")
            if (i + 1) % 10 == 0 or (i + 1) == len(records):
                print(f"  [{i + 1:3d}/{len(records)}] ✓ Inserted {brand:20s} ({market:6s}) [{category}]")
            inserted += 1

        except Exception as e:
            brand = record.get("brand_name", "Unknown")
            market = record.get("market_country", "XX")
            category = record.get("category", "?")
            error_msg = str(e)[:80]
            print(f"  [{i + 1:3d}/{len(records)}] ❌ {brand:20s} ({market:6s}) - {error_msg}")
            error_details.append({"brand": brand, "market": market, "error": error_msg})
            errors += 1

    print(f"\n[expansion_insert] Insertion Summary:")
    print(f"  Total Records:  {len(records)}")
    print(f"  Inserted:       {inserted}")
    print(f"  Errors:         {errors}")

    if inserted > 0:
        # Verify database state
        print(f"\n[expansion_insert] Verifying database state...\n")

        try:
            total_result = sb.table("brand_phase1_intelligence").select("count", count="exact").execute()
            total = total_result.count if hasattr(total_result, 'count') else 0

            unique_result = sb.table("brand_phase1_intelligence").select("brand_name").execute()
            unique_brands = len(set(row['brand_name'] for row in unique_result.data)) if unique_result.data else 0

            print(f"[expansion_insert] Database Now Contains:")
            print(f"  Total Records:     {total:,}")
            print(f"  Unique Brands:     {unique_brands}")

            # Count records by category
            cat_result = sb.table("brand_phase1_intelligence").select("category").execute()
            if cat_result.data:
                cat_counts = {}
                for row in cat_result.data:
                    cat = row.get("category", "unknown")
                    cat_counts[cat] = cat_counts.get(cat, 0) + 1

                print(f"\n[expansion_insert] Records by Category:")
                for cat in sorted(cat_counts.keys()):
                    print(f"  {cat:15s}: {cat_counts[cat]:5d} records")

        except Exception as e:
            print(f"⚠️  Could not verify database state: {e}")

        if inserted == len(records):
            print(f"\n" + "=" * 70)
            print(f"✅ SUCCESS: All {inserted} records inserted!")
            print(f"✅ Intel Phase 1 expanded from 60 to 110+ brands")
            print(f"=" * 70 + "\n")
            return True
        else:
            print(f"\n⚠️  Partial insert ({inserted}/{len(records)})")
            if error_details:
                print(f"\nFirst 5 errors:")
                for err in error_details[:5]:
                    print(f"  - {err['brand']} ({err['market']}): {err['error']}")
            return inserted >= len(records) * 0.8  # Success if 80%+ inserted
    else:
        print(f"\n❌ No records inserted")
        return False


if __name__ == "__main__":
    success = insert_expansion_brands()
    if success:
        # FINAL STEP: Auto-update brand count references
        print("\n[final_step] Updating brand count references everywhere...")
        try:
            from update_brand_counts import main as update_counts
            update_counts()
        except Exception as e:
            print(f"⚠️  Could not auto-update counts: {e}")
        exit(0)
    else:
        exit(1)
