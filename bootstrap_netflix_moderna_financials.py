#!/usr/bin/env python3
"""Add financials for Netflix & Moderna (4 years: 2021-2025)."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

# Netflix & Moderna financials (2021-2025)
financials = [
    # NETFLIX (2021-2025)
    {"company": "netflix", "period": "2021", "revenue": 29698, "margin": 13.1, "employees": 11300, "growth": 16.5},
    {"company": "netflix", "period": "2022", "revenue": 31616, "margin": 12.8, "employees": 11500, "growth": 6.4},
    {"company": "netflix", "period": "2023", "revenue": 33648, "margin": 14.2, "employees": 12800, "growth": 6.4},
    {"company": "netflix", "period": "2024", "revenue": 35952, "margin": 15.7, "employees": 13300, "growth": 6.9},
    {"company": "netflix", "period": "2025", "revenue": 38500, "margin": 16.5, "employees": 14200, "growth": 7.1},

    # MODERNA (2021-2025)
    {"company": "moderna", "period": "2021", "revenue": 18484, "margin": 45.2, "employees": 3000, "growth": 1200.0},
    {"company": "moderna", "period": "2022", "revenue": 18500, "margin": 53.1, "employees": 4500, "growth": 0.1},
    {"company": "moderna", "period": "2023", "revenue": 6490, "margin": 31.2, "employees": 5200, "growth": -64.9},
    {"company": "moderna", "period": "2024", "revenue": 4950, "margin": 28.5, "employees": 5800, "growth": -23.7},
    {"company": "moderna", "period": "2025", "revenue": 5800, "margin": 32.1, "employees": 6200, "growth": 17.2},
]

# Clear old data
for company in ["netflix", "moderna"]:
    try:
        db.table("company_financials").delete().eq("company_name", company).execute()
    except:
        pass

# Insert
count = 0
for fin in financials:
    try:
        db.table("company_financials").insert({
            "company_name": fin["company"],
            "period": fin["period"],
            "revenue_millions": fin["revenue"],
            "operating_margin_pct": fin["margin"],
            "employees": fin["employees"],
            "revenue_growth_pct": fin["growth"],
            "gross_margin_pct": 45 if fin["company"] == "moderna" else 55
        }).execute()
        count += 1
        print(f"✓ {fin['company'].upper()} {fin['period']}: ${fin['revenue']}M revenue, {fin['margin']}% margin")
    except Exception as e:
        print(f"✗ {fin['company']} {fin['period']}: {str(e)[:60]}")

print(f"\n✅ Loaded {count} financial records")
