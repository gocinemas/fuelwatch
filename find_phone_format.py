#!/usr/bin/env python3
"""Find what phone format Tesco receipt is stored with"""
import os
import sys

sys.path.insert(0, '/Users/srevi/fuelwatch')
import library as lib

phone = "+447595075735"
print(f"\n🔍 Searching for receipts with phone: {phone}")
print("=" * 70)

sb = lib._sb()

# Search for ANY receipts with this phone (check all possible formats)
print(f"\n1️⃣  Checking wa_saves table for ANY receipts...")
try:
    # Get ALL records from wa_saves that have 🧾 emoji (any user)
    all_receipts = sb.table("wa_saves").select("id,from_number,title,summary,created_at").ilike("title", "%🧾%").limit(500).execute().data or []

    # Filter for ones that match your phone in ANY format
    matching = []
    for row in all_receipts:
        from_num = row.get("from_number", "")
        # Check if any variation of your phone appears in from_number
        if (phone in from_num or
            phone.replace("+", "") in from_num or
            f"whatsapp:{phone}" in from_num or
            "447595075735" in from_num):
            matching.append(row)

    print(f"Found {len(matching)} receipts matching your phone number")
    for i, row in enumerate(matching):
        print(f"\n[{i}] from_number: '{row.get('from_number')}'")
        title = row.get('title', '')
        print(f"    title: {title[:70]}")
        if 'tesco' in title.lower():
            print(f"    ✅ THIS IS THE TESCO RECEIPT!")
        print(f"    date: {row.get('created_at', '')[:10]}")

except Exception as e:
    print(f"❌ Error: {e}")

# Also check receipts table
print(f"\n2️⃣  Checking receipts table (PDF imports)...")
try:
    # Try all phone format variations
    formats_to_try = [
        phone,                      # +447595075735
        phone.replace("+", ""),     # 447595075735
        f"whatsapp:{phone}",        # whatsapp:+447595075735
        f"whatsapp:{phone.replace('+', '')}"  # whatsapp:447595075735
    ]

    all_found = []
    for fmt in formats_to_try:
        try:
            rows = sb.table("receipts").select("id,merchant,phone,shop_date,items").eq("phone", fmt).limit(50).execute().data or []
            if rows:
                print(f"\n   Format '{fmt}' found {len(rows)} receipts:")
                for row in rows[:3]:
                    print(f"      - {row.get('merchant')} on {row.get('shop_date')}")
                    if 'tesco' in row.get('merchant', '').lower():
                        print(f"        ✅ TESCO!")
                all_found.extend(rows)
        except:
            pass

    if not all_found:
        print(f"   No receipts found in receipts table with any phone format")

except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 70)
