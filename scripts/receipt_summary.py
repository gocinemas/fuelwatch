#!/usr/bin/env python3
"""
Print a weekly or monthly receipts summary (the same text the WhatsApp digest uses).
Read-only: sends nothing.

Usage (from the fuelwatch folder):
  railway run python3 scripts/receipt_summary.py --period week
  railway run python3 scripts/receipt_summary.py --period month --month 2026-10
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import library as lib  # noqa: E402
from miru.receipts_summary import build_summary  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-number", required=True, help="e.g. +447...")
    ap.add_argument("--period", choices=["week", "month"], required=True)
    ap.add_argument("--today", help="YYYY-MM-DD, defaults to today")
    ap.add_argument("--month", help="YYYY-MM")
    args = ap.parse_args()

    today = date.fromisoformat(args.today) if args.today else None
    result = build_summary(lib, f"whatsapp:{args.from_number}", args.period, today=today, month=args.month)
    print(result["text"])


if __name__ == "__main__":
    main()
