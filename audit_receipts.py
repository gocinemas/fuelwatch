"""Audit and fix existing receipt merchant names and categories."""
import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.dirname(__file__))

import library as lib
from intelligence_engine import _receipt_category

def audit_and_fix_receipts():
    """Scan wa_saves for receipts with location-based merchant names and fix them."""
    try:
        sb = lib._sb()

        # Find all receipts
        receipts = sb.table("wa_saves").select("id,title,summary,category,url,created_at").ilike("title", "🧾%").execute().data or []
        print(f"[audit] Found {len(receipts)} receipts total")

        # UK location keywords that shouldn't be merchant names
        uk_locations = ["weybridge", "london", "manchester", "birmingham", "leeds", "liverpool",
                        "bristol", "edinburgh", "cardiff", "belfast", "surrey", "essex", "kent", "sussex",
                        "oxford", "cambridge", "york", "canterbury", "windsor"]

        problematic = []
        for r in receipts:
            title = (r.get("title") or "").replace("🧾", "").strip()
            title_lower = title.lower()

            # Check if merchant name looks like a location
            if any(loc in title_lower for loc in uk_locations):
                problematic.append({
                    "id": r.get("id"),
                    "title": title,
                    "category": r.get("category"),
                    "date": r.get("created_at", "")[:10],
                    "has_url": bool(r.get("url"))
                })

        print(f"[audit] Found {len(problematic)} receipts with location-based names:")
        for p in problematic[:50]:
            print(f"  {p['date']} | {p['title']:40} | Category: {p['category'] or 'None':15} | Has URL: {p['has_url']}")

        if len(problematic) > 50:
            print(f"  ... and {len(problematic) - 50} more")

        print(f"\n[audit] Summary:")
        print(f"  Total receipts: {len(receipts)}")
        print(f"  Problematic: {len(problematic)} ({100*len(problematic)//len(receipts)}%)")

        # Category distribution of problematic ones
        categories = {}
        for p in problematic:
            cat = p['category'] or 'None'
            categories[cat] = categories.get(cat, 0) + 1
        print(f"\n[audit] Problematic receipts by category:")
        for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {count}")

        return {
            "total": len(receipts),
            "problematic": len(problematic),
            "problematic_list": problematic,
            "categories": categories
        }

    except Exception as e:
        print(f"[audit] Error: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}

if __name__ == "__main__":
    result = audit_and_fix_receipts()
    print(f"\n[audit] Done. Result keys: {result.keys()}")
