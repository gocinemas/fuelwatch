#!/usr/bin/env python3
"""
Fix Currency Data Across All 60 Brand Records
Recalculates USD equivalents using consistent exchange rates
Run: railway run python3 fix_currency_data.py
"""

import os
from supabase import create_client, Client
from currency_service import convert_to_usd, format_price, get_ppp_index


def fix_all_brand_prices():
    """Recalculate and fix all brand prices in database"""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        print("❌ Credentials not set")
        return False

    sb: Client = create_client(url, key)

    print("\n[currency_fix] Fetching all 60 brand records...\n")

    # Fetch all records
    result = sb.table("brand_phase1_intelligence").select("*").execute()

    if not result.data:
        print("❌ No records found")
        return False

    total = len(result.data)
    updated = 0
    errors = 0

    print(f"Fixing {total} records...\n")

    for i, row in enumerate(result.data):
        try:
            brand_name = row.get("brand_name")
            market_country = row.get("market_country")
            price_local = row.get("price_local")
            price_currency = row.get("price_currency")
            row_id = row.get("id")

            if not price_local or not price_currency:
                print(f"  [{i + 1}/{total}] ⏭️  {brand_name} ({market_country}) - No price data")
                continue

            # Convert to USD using exchange rate
            price_usd = convert_to_usd(float(price_local), price_currency)

            # Update record
            update_result = sb.table("brand_phase1_intelligence").update(
                {"price_usd_equivalent": round(price_usd, 2)}
            ).eq("id", row_id).execute()

            if update_result:
                print(
                    f"  [{i + 1}/{total}] ✓ {brand_name:15s} ({market_country:6s}) "
                    f"{price_currency}{price_local:6.2f} → ${round(price_usd, 2)}"
                )
                updated += 1
            else:
                errors += 1

        except Exception as e:
            print(f"  [{i + 1}/{total}] ❌ {brand_name} ({market_country}): {e}")
            errors += 1

    print(f"\n[currency_fix] Summary:")
    print(f"  Total: {total}")
    print(f"  Updated: {updated}")
    print(f"  Errors: {errors}")

    if updated == total:
        print(f"\n✅ All {total} prices fixed successfully!")
        return True
    else:
        print(f"\n⚠️  {errors} records had issues")
        return updated > 0


if __name__ == "__main__":
    success = fix_all_brand_prices()
    if not success:
        exit(1)
