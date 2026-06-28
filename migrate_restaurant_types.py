"""
Migration script: Tag existing receipts with restaurant type.
Retroactively adds 'restaurant_type' field to ma_receipts based on merchant name.
"""
import sys
sys.path.insert(0, '/Users/srevi/fuelwatch')

from restaurant_classifier import classify_restaurant
from miru_lib import lib

def migrate_receipts():
    """Tag all existing receipts with restaurant type."""
    sb = lib._sb()

    # Get all receipts
    receipts = sb.table("ma_receipts").select("*").execute().data or []

    print(f"Found {len(receipts)} receipts to process...")

    updated = 0
    for receipt in receipts:
        merchant = receipt.get("merchant", "")
        if not merchant:
            continue

        # Classify the restaurant
        rtype = classify_restaurant(merchant)

        # Skip if already tagged or unknown
        if receipt.get("restaurant_type") or rtype == "unknown":
            continue

        # Update receipt with type
        try:
            sb.table("ma_receipts").update({
                "restaurant_type": rtype
            }).eq("id", receipt["id"]).execute()

            updated += 1
            print(f"✓ {merchant} → {rtype}")

        except Exception as e:
            print(f"✗ Error updating {merchant}: {e}")

    print(f"\n✅ Migrated {updated} receipts")

if __name__ == "__main__":
    migrate_receipts()
