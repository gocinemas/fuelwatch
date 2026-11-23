#!/usr/bin/env python3
"""
Save reviewed receipts (from the review CSV) into Miru's existing receipts store, wa_saves,
using the same layout as receipts saved from WhatsApp:
  title "🧾 <retailer>", summary with META line, and category.

Dry run by default. Pass --commit to insert.

Usage:
  railway run python3 scripts/save_receipts_to_wa_saves.py <review.csv> --from-number +44...
  railway run python3 scripts/save_receipts_to_wa_saves.py <review.csv> --from-number +44... --commit
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # import library from the fuelwatch repo


def category_for(receipt_type: str) -> str:
    t = (receipt_type or "").lower()
    if "groc" in t or "household" in t or "snack" in t or "bakery" in t:
        return "Groceries"
    if "fuel" in t:
        return "Transport"
    if "coffee" in t or "eat" in t or "lunch" in t or "food" in t:
        return "Eating out"
    return "Other"


def display_date(iso: str) -> str:
    if iso and iso != "unknown":
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%d %b %Y")
    return "date not on receipt"


def build_row(r: dict, from_number: str) -> dict:
    total = float(r["total"])
    retailer = r["retailer"] or "Unknown"
    date_label = display_date(r["receipt_date"])
    lines = [
        f"💰 Total: £{total:.2f}",
        f"META:📅 {date_label} · 📍 {retailer}",
        f"• Type: {r['type']}",
    ]
    if r["items"]:
        lines.append(f"• Items: {r['items']}")
    if float(r["vat"] or 0) > 0:
        lines.append(f"• VAT: £{float(r['vat']):.2f}")
    lines.append("• Personal. Imported from photo; check against the receipt if it matters")

    row = {
        "from_number": from_number,
        "title": f"🧾 {retailer}",
        "summary": "\n".join(lines),
        "category": category_for(r["type"]),
        "status": "pending",
        # url is NOT NULL in wa_saves; existing receipts store a link, so use a retailer search link
        "url": "https://www.google.com/search?q=" + retailer.replace(" ", "+"),
    }
    if r["receipt_date"] and r["receipt_date"] != "unknown":
        # Noon London time so the receipt lands on the right day in date filters
        row["created_at"] = f"{r['receipt_date']}T12:00:00+00:00"
    return row


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path", type=Path)
    ap.add_argument("--from-number", required=True, help="e.g. +447...")
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()

    with args.csv_path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    usable = [r for r in rows if float(r["total"] or 0) > 0 and not r["error"]]
    skipped = [r["file"] for r in rows if r not in usable]

    wa_from = f"whatsapp:{args.from_number}"
    payloads = [build_row(r, wa_from) for r in usable]
    print(f"{len(payloads)} receipts to save, skipping {len(skipped)}: {', '.join(skipped)}")

    if not args.commit:
        for p in payloads[:3]:
            print("---", p["title"], p.get("created_at", "(no date, uses today)"))
            print(p["summary"])
        print("Dry run only. Re-run with --commit to save.")
        return

    import library as lib
    saved = 0
    for p in payloads:
        p = {k: v for k, v in p.items() if v is not None}
        lib._sb().table("wa_saves").insert(p).execute()
        saved += 1
    print(f"Saved {saved} receipts to wa_saves.")


if __name__ == "__main__":
    main()
