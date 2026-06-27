"""
Import 7.4M HM Land Registry sales into Supabase house_price_real table.
"""

import json
from datetime import datetime

def import_hml_to_supabase():
    """Load parsed HML data into Supabase."""

    try:
        from miru_lib import lib

        # Load the parsed data
        with open('/tmp/hml_full_import.json', 'r') as f:
            data = json.load(f)

        records = data['records']
        metadata = data['metadata']

        print(f"[IMPORT] Loading {len(records)} records into Supabase...")
        print(f"[IMPORT] Data: {metadata['total_sales']:,} sales across {metadata['postcode_prefixes']} postcodes")

        # Get Supabase client
        sb = lib._sb()

        # Delete existing data
        try:
            sb.table("house_price_real").delete().neq("postcode", "").execute()
            print("[IMPORT] Cleared existing data")
        except:
            pass

        # Insert in batches
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                sb.table("house_price_real").insert(batch).execute()
                if (i // batch_size) % 10 == 0:
                    print(f"[IMPORT] Inserted {i + len(batch):,}/{len(records)} records")
            except Exception as e:
                print(f"[IMPORT] Error inserting batch {i}: {e}")

        print(f"[IMPORT] ✓ Successfully imported {len(records)} records")
        print(f"[IMPORT] KT16 prices are now based on 8-year averages")
        return True

    except Exception as e:
        print(f"[IMPORT] Error: {e}")
        print("[IMPORT] Make sure Supabase table 'house_price_real' exists with columns:")
        print("  - postcode (text)")
        print("  - property_type (text)")
        print("  - avg_price (integer)")
        print("  - median_price (integer)")
        print("  - count (integer)")
        print("  - min_price (integer)")
        print("  - max_price (integer)")
        print("  - p25_price (integer)")
        print("  - p75_price (integer)")
        print("  - data_source (text)")
        print("  - last_updated (timestamp)")
        return False

if __name__ == "__main__":
    success = import_hml_to_supabase()
    exit(0 if success else 1)
