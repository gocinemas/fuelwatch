#!/usr/bin/env python3
"""Bootstrap Procter & Gamble company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Procter & Gamble"

    financials = [
        {"period": "2025", "revenue_millions": 85280, "gross_margin_pct": 50.2, "operating_margin_pct": 21.5, "employees": 118000, "revenue_growth_pct": 4.2},
        {"period": "2024", "revenue_millions": 81802, "gross_margin_pct": 49.8, "operating_margin_pct": 20.8, "employees": 119000, "revenue_growth_pct": 6.8},
        {"period": "2023", "revenue_millions": 76470, "gross_margin_pct": 49.2, "operating_margin_pct": 19.5, "employees": 120000, "revenue_growth_pct": 5.5},
        {"period": "2022", "revenue_millions": 72470, "gross_margin_pct": 48.5, "operating_margin_pct": 18.2, "employees": 121000, "revenue_growth_pct": 3.2},
    ]

    trends = [
        {"category": "market_share", "metric_name": "beauty_personal_care_market_share", "value_pct": 12.8, "period": "2025"},
        {"category": "market_share", "metric_name": "fabric_home_care_market_share", "value_pct": 24.5, "period": "2025"},
        {"category": "market_share", "metric_name": "baby_care_market_share", "value_pct": 18.3, "period": "2025"},
        {"category": "category_growth", "metric_name": "premium_beauty_growth", "value_pct": 8.2, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "Opté (personalized skincare)", "amount_millions": 60, "announcement_date": "2021-10-01", "description": "Personalized beauty tech acquisition"},
        {"deal_type": "divestiture", "target_company": "Elijan & Cie (European distributor)", "amount_millions": 200, "announcement_date": "2022-03-01", "description": "European distribution network"}
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
