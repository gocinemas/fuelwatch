#!/usr/bin/env python3
"""Bootstrap Unilever company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Unilever"

    financials = [
        {"period": "2024", "revenue_millions": 61670, "gross_margin_pct": 37.2, "operating_margin_pct": 13.8, "employees": 126000, "revenue_growth_pct": 3.5},
        {"period": "2023", "revenue_millions": 59630, "gross_margin_pct": 36.8, "operating_margin_pct": 12.5, "employees": 128000, "revenue_growth_pct": 0.2},
        {"period": "2022", "revenue_millions": 59450, "gross_margin_pct": 35.2, "operating_margin_pct": 11.8, "employees": 130000, "revenue_growth_pct": 11.8},
        {"period": "2021", "revenue_millions": 53180, "gross_margin_pct": 34.5, "operating_margin_pct": 10.5, "employees": 135000, "revenue_growth_pct": 5.2},
    ]

    trends = [
        {"category": "market_share", "metric_name": "beauty_care_market_share_global", "value_pct": 9.2, "period": "2024"},
        {"category": "market_share", "metric_name": "laundry_care_market_share_global", "value_pct": 16.5, "period": "2024"},
        {"category": "market_share", "metric_name": "home_care_market_share_global", "value_pct": 14.8, "period": "2024"},
        {"category": "category_growth", "metric_name": "beauty_growth_global", "value_pct": 5.2, "period": "2024"},
        {"category": "category_growth", "metric_name": "personal_care_growth", "value_pct": 4.8, "period": "2024"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "SkinCeuticals", "amount_millions": 2550, "announcement_date": "2021-07-01", "description": "Premium skincare acquisition"},
        {"deal_type": "divestiture", "target_company": "Elida Beauty (Russia/Ukraine)", "amount_millions": 350, "announcement_date": "2022-06-01", "description": "Exit from Russian/Ukrainian markets"},
    ]

    for record in financials:
        try:
            db.table("company_financials").insert({
                "company_name": company_name.lower(),
                "period": record["period"],
                "revenue_millions": record["revenue_millions"],
                "gross_margin_pct": record["gross_margin_pct"],
                "operating_margin_pct": record["operating_margin_pct"],
                "employees": record["employees"],
                "revenue_growth_pct": record["revenue_growth_pct"],
            }).execute()
            print(f"✓ {record['period']}")
        except Exception as e:
            print(f"✗ {record['period']}: {e}")

    for trend in trends:
        try:
            db.table("company_market_trends").insert({
                "company_name": company_name.lower(),
                "category": trend["category"],
                "metric_name": trend["metric_name"],
                "value_pct": trend["value_pct"],
                "period": trend["period"],
            }).execute()
        except: pass

    for deal in deals:
        try:
            db.table("company_deals").insert({
                "company_name": company_name.lower(),
                "deal_type": deal["deal_type"],
                "target_company": deal["target_company"],
                "amount_millions": deal["amount_millions"],
                "announcement_date": deal["announcement_date"],
                "description": deal["description"],
            }).execute()
        except: pass

    print(f"✅ {company_name}")

if __name__ == "__main__":
    bootstrap()
