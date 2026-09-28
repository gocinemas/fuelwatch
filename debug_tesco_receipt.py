#!/usr/bin/env python3
"""Debug script to find why Tesco receipt isn't being found"""
import os
import sys
import json

# Import library
sys.path.insert(0, '/Users/srevi/fuelwatch')
import library as lib

def debug_receipt_search(phone_input: str):
    """Search for Tesco receipt using various phone formats"""
    sb = lib._sb()

    print(f"\n🔍 Debugging Tesco receipt search for phone: {phone_input}")
    print("=" * 60)

    # Try to figure out what format the phone should be in
    from_number = phone_input
    if not from_number.startswith("whatsapp:"):
        from_number = f"whatsapp:{phone_input}" if phone_input.startswith("+") or phone_input.startswith("44") else phone_input

    print(f"\n1️⃣  Using from_number: {from_number}")

    # Check wa_saves table
    print(f"\n2️⃣  Searching wa_saves table with from_number='{from_number}'")
    try:
        wa_rows = sb.table("wa_saves").select("id,title,summary,from_number,created_at") \
            .eq("from_number", from_number) \
            .ilike("title", "%🧾%") \
            .order("created_at", desc=True).limit(20).execute().data or []

        print(f"   Found {len(wa_rows)} receipts")
        for i, row in enumerate(wa_rows[:3]):
            print(f"   [{i}] title: {row.get('title', '')[:60]}...")
            if "tesco" in row.get('title', '').lower():
                print(f"       ✅ CONTAINS 'tesco'!")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # Check receipts table
    phone_clean = from_number.replace("whatsapp:", "").strip()
    print(f"\n3️⃣  Searching receipts table with phone='{phone_clean}'")
    try:
        pdf_rows = sb.table("receipts").select("id,merchant,total,shop_date,phone,created_at") \
            .eq("phone", phone_clean) \
            .order("shop_date", desc=True).limit(20).execute().data or []

        print(f"   Found {len(pdf_rows)} PDF receipts")
        for i, row in enumerate(pdf_rows[:3]):
            print(f"   [{i}] merchant: {row.get('merchant', '')}, phone: {row.get('phone', '')}")
            if "tesco" in row.get('merchant', '').lower():
                print(f"       ✅ CONTAINS 'tesco'!")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # Brute force: check ALL from_number values in wa_saves (first 100 unique)
    print(f"\n4️⃣  Finding ALL unique from_numbers in wa_saves table...")
    try:
        all_saves = sb.table("wa_saves").select("from_number").ilike("title", "%🧾%") \
            .order("created_at", desc=True).limit(200).execute().data or []

        from_numbers = list(set(row.get("from_number", "") for row in all_saves if row.get("from_number")))
        print(f"   Found {len(from_numbers)} unique phone formats in wa_saves:")
        for fn in from_numbers[:10]:
            print(f"      - {fn}")

        # Try searching with each one
        print(f"\n5️⃣  Searching for Tesco receipt with each format:")
        for fn in from_numbers:
            tesco_count = 0
            try:
                tesco_rows = sb.table("wa_saves").select("id,title") \
                    .eq("from_number", fn) \
                    .ilike("title", "%🧾%") \
                    .ilike("title", "%tesco%") \
                    .execute().data or []
                tesco_count = len(tesco_rows)
            except:
                pass

            if tesco_count > 0:
                print(f"   ✅ {fn} → Found {tesco_count} Tesco receipts!")

    except Exception as e:
        print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    # Try common phone formats
    phone = os.getenv("MIRU_PHONE") or "+447595075735"  # Your phone from gitStatus
    debug_receipt_search(phone)

    # Also try without whatsapp prefix
    if phone.startswith("whatsapp:"):
        debug_receipt_search(phone.replace("whatsapp:", ""))
