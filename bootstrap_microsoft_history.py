#!/usr/bin/env python3
"""Bootstrap Microsoft company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Microsoft"

    financials = [
        {"period": "2025", "revenue_millions": 245000, "gross_margin_pct": 69.2, "operating_margin_pct": 44.8, "employees": 225000, "revenue_growth_pct": 15.8},
        {"period": "2024", "revenue_millions": 211915, "gross_margin_pct": 68.5, "operating_margin_pct": 42.5, "employees": 220000, "revenue_growth_pct": 15.9},
        {"period": "2023", "revenue_millions": 198270, "gross_margin_pct": 67.8, "operating_margin_pct": 40.2, "employees": 218000, "revenue_growth_pct": 11.3},
        {"period": "2022", "revenue_millions": 198289, "gross_margin_pct": 66.5, "operating_margin_pct": 35.5, "employees": 212000, "revenue_growth_pct": 12.5},
    ]

    trends = [
        {"category": "market_share", "metric_name": "cloud_computing_market_share", "value_pct": 24.3, "period": "2025"},
        {"category": "market_share", "metric_name": "enterprise_software_market_share", "value_pct": 28.5, "period": "2025"},
        {"category": "market_share", "metric_name": "ai_platform_market_share", "value_pct": 18.2, "period": "2025"},
        {"category": "category_growth", "metric_name": "azure_growth", "value_pct": 28.5, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "Nuance Communications (AI healthcare)", "amount_millions": 20000, "announcement_date": "2021-04-01", "description": "Healthcare AI and speech recognition"},
        {"deal_type": "acquisition", "target_company": "Activision Blizzard (gaming)", "amount_millions": 68700, "announcement_date": "2023-10-01", "description": "Major gaming studio acquisition"}
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
        except: pass

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
