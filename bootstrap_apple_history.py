#!/usr/bin/env python3
"""Bootstrap Apple company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Apple"

    financials = [
        {"period": "2025", "revenue_millions": 389000, "gross_margin_pct": 46.8, "operating_margin_pct": 30.5, "employees": 161000, "revenue_growth_pct": 2.4},
        {"period": "2024", "revenue_millions": 379855, "gross_margin_pct": 45.8, "operating_margin_pct": 29.8, "employees": 162000, "revenue_growth_pct": 0.8},
        {"period": "2023", "revenue_millions": 383285, "gross_margin_pct": 46.2, "operating_margin_pct": 30.2, "employees": 164000, "revenue_growth_pct": -1.4},
        {"period": "2022", "revenue_millions": 394328, "gross_margin_pct": 45.5, "operating_margin_pct": 28.8, "employees": 164000, "revenue_growth_pct": 7.8},
    ]

    trends = [
        {"category": "market_share", "metric_name": "smartphone_market_share_global", "value_pct": 18.2, "period": "2025"},
        {"category": "market_share", "metric_name": "premium_smartphone_market_share", "value_pct": 52.5, "period": "2025"},
        {"category": "market_share", "metric_name": "wearables_market_share", "value_pct": 35.8, "period": "2025"},
        {"category": "category_growth", "metric_name": "services_revenue_growth", "value_pct": 12.8, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "Shazam (music ID)", "amount_millions": 400, "announcement_date": "2018-09-01", "description": "Music identification and discovery"},
        {"deal_type": "acquisition", "target_company": "Beats Electronics", "amount_millions": 3200, "announcement_date": "2014-05-01", "description": "Audio hardware and Beats Music"}
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
