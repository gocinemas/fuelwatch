#!/usr/bin/env python3
"""
Batch-import receipt photos into Miru's work_expenses table.

Pipeline (cheap: OCR is local, the LLM only sees text):
  1. OCR each photo locally with Tesseract (no tokens, nothing uploaded).
  2. One short text-only Groq call per receipt -> date, retailer, type, items, total, VAT.
  3. Write a CSV for review. Nothing is saved to Miru until you run with --commit.

Usage:
  python3 scripts/import_receipts.py ~/Downloads/receipts-oct --from-number +447XXXXXXXXX
      -> writes receipts_review.csv next to the folder (dry run)
  python3 scripts/import_receipts.py ~/Downloads/receipts-oct --from-number +447XXXXXXXXX --commit
      -> inserts the rows from the CSV into work_expenses

Requires: tesseract (brew install tesseract), pip3 install pytesseract pillow groq
Env: GROQ_API_KEY, plus whatever library.py needs for Supabase.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # so `import library` resolves to the fuelwatch repo

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".tif", ".tiff", ".bmp", ".webp"}
GROQ_MODEL = "openai/gpt-oss-20b"  # small, cheap text model; llama-3.1-8b-instant is no longer available to this key
CSV_FIELDS = ["file", "receipt_date", "retailer", "type", "items", "total", "vat", "category", "confidence", "error"]
CATEGORIES = {"Supplies", "Fuel", "Meals", "Equipment", "Transport", "Accommodation",
              "Communications", "Subscriptions", "Training", "Other"}

PROMPT = """Read this receipt text and return JSON only, no markdown:
{{
  "date": "YYYY-MM-DD or unknown",
  "retailer": "shop or business name",
  "type": "short type such as Groceries, Coffee, Eating out, Fuel, Hardware, Transport, Pharmacy, Other",
  "items": ["each line item, short, no prices"],
  "total": 0.00,
  "vat": 0.00
}}
If a field is not visible use "unknown" (or 0 for numbers).

RECEIPT TEXT:
{text}"""


def ocr_image(path: Path) -> str:
    # Call the tesseract binary directly: the pytesseract wrapper rejects PIL images
    # on some Python installs, and the binary is all we need.
    import shutil
    import subprocess
    if not shutil.which("tesseract"):
        raise SystemExit("Tesseract not installed. Run: brew install tesseract")
    # iPhone photos store rotation in EXIF; Tesseract ignores it, so apply it first.
    import tempfile
    from PIL import Image, ImageOps
    img = ImageOps.exif_transpose(Image.open(path)).convert("L")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_png = Path(tmp) / "page.png"
        img.save(tmp_png)
        result = subprocess.run(["tesseract", str(tmp_png), "stdout"],
                                capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"tesseract failed: {result.stderr.strip()[:200]}")
    return result.stdout


def extract_fields(text: str, attempts: int = 4) -> dict:
    import time
    from groq import Groq
    client = Groq(api_key=os.environ["GROQ_API_KEY"])
    for attempt in range(1, attempts + 1):
        try:
            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": PROMPT.format(text=text[:3000])}],
                temperature=0.0,
                max_tokens=1500,  # reasoning models spend tokens before the JSON
            )
            raw = (resp.choices[0].message.content or "").strip()
            start, end = raw.find("{"), raw.rfind("}")
            if start == -1 or end == -1:
                raise ValueError(f"no JSON in model reply: {raw[:120]!r}")
            return json.loads(raw[start:end + 1])
        except Exception as e:
            # Retry rate limits and empty/bad replies with a pause; give up after the last try.
            if attempt == attempts:
                raise
            time.sleep(15 * attempt)
    raise RuntimeError("unreachable")


def to_float(value) -> float:
    try:
        return round(float(str(value).replace("£", "").replace(",", "").strip()), 2)
    except (TypeError, ValueError):
        return 0.0


def process_folder(folder: Path, out_csv: Path) -> list[dict]:
    rows = []
    photos = sorted(p for p in folder.iterdir() if p.suffix.lower() in IMAGE_EXTS)
    print(f"Found {len(photos)} photos in {folder}")
    for i, photo in enumerate(photos, 1):
        row = {f: "" for f in CSV_FIELDS}
        row["file"] = photo.name
        try:
            text = ocr_image(photo)
            data = extract_fields(text)
            row.update({
                "receipt_date": data.get("date", "unknown"),
                "retailer": data.get("retailer", "unknown"),
                "type": data.get("type", "Other"),
                "items": "; ".join(data.get("items", [])),
                "total": to_float(data.get("total")),
                "vat": to_float(data.get("vat")),
                "category": data.get("type") if data.get("type") in CATEGORIES else "Other",
                "confidence": "check" if not text.strip() else "ok",
            })
        except SystemExit:
            raise
        except Exception as e:
            row["error"] = str(e)[:200]
        rows.append(row)
        print(f"  [{i}/{len(photos)}] {photo.name}: {row['retailer'] or row['error']}")

    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Review file written: {out_csv}")
    return rows


def commit_csv(csv_path: Path, from_number: str) -> None:
    import library as lib
    inserted = skipped = 0
    with csv_path.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("error") or not row.get("total"):
                skipped += 1
                continue
            date = row["receipt_date"]
            if not (len(date) == 10 and date[4] == "-"):
                date = None  # keep the row, let the DB default apply
            payload = {
                "from_number": from_number,
                "amount": to_float(row["total"]),
                "currency": "GBP",
                "merchant": row["retailer"] or "Unknown",
                "receipt_date": date,
                "category": row["category"] or "Other",
                "description": f"{row['type']}: {row['items']}"[:1000],
                "vat_amount": to_float(row["vat"]),
                "business_use": False,  # these receipts were confirmed as personal
                "extraction_confidence": 0.8 if row["confidence"] == "ok" else 0.5,
                "expense_status": "pending",
            }
            payload = {k: v for k, v in payload.items() if v is not None}
            lib._sb().table("work_expenses").insert(payload).execute()
            inserted += 1
    print(f"Inserted {inserted} rows into work_expenses, skipped {skipped}.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", type=Path, help="folder of receipt photos")
    ap.add_argument("--from-number", required=True, help="WhatsApp number the expenses belong to, e.g. +447...")
    ap.add_argument("--commit", action="store_true", help="insert the CSV rows into Miru (default: dry run)")
    args = ap.parse_args()

    out_csv = args.folder.parent / f"{args.folder.name}_review.csv"
    if not args.commit:
        if not os.environ.get("GROQ_API_KEY"):
            raise SystemExit("GROQ_API_KEY is not set")
        process_folder(args.folder, out_csv)
        print("Dry run only. Check the CSV, then re-run with --commit to save to Miru.")
    else:
        if not out_csv.exists():
            raise SystemExit(f"No review file at {out_csv}. Run without --commit first.")
        commit_csv(out_csv, args.from_number)


if __name__ == "__main__":
    main()
