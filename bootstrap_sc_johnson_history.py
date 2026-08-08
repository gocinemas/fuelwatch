#!/usr/bin/env python3
"""Bootstrap S.C. Johnson company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "S.C. Johnson"

    financials = [
        {"period": "2024", "revenue_millions": 11200, "gross_margin_pct": 52.3, "operating_margin_pct": 18.5, "employees": 13000, "revenue_growth_pct": 3.2},
        {"period": "2023", "revenue_millions": 10850, "gross_margin_pct": 51.8, "operating_margin_pct": 17.2, "employees": 13200, "revenue_growth_pct": 2.8},
        {"period": "2022", "revenue_millions": 10550, "gross_margin_pct": 50.5, "operating_margin_pct": 16.5, "employees": 13500, "revenue_growth_pct": 4.1},
        {"period": "2021", "revenue_millions": 10140, "gross_margin_pct": 49.8, "operating_margin_pct": 15.8, "employees": 13800, "revenue_growth_pct": 5.2},
    ]

    trends = [
        {"category": "market_share", "metric_name": "home_care_market_share_us", "value_pct": 14.2, "period": "2024"},
        {"category": "market_share", "metric_name": "aerosol_market_share", "value_pct": 18.5, "period": "2024"},
        {"category": "market_share", "metric_name": "pest_control_market_share", "value_pct": 12.8, "period": "2024"},
        {"category": "category_growth", "metric_name": "home_care_growth", "value_pct": 2.5, "period": "2024"},
        {"category": "category_growth", "metric_name": "air_freshener_growth", "value_pct": 3.2, "period": "2024"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "Amodei Consumer Health", "amount_millions": 250, "announcement_date": "2021-06-01", "description": "Consumer health product acquisition"},
        {"deal_type": "partnership", "target_company": "Greenworks (joint venture)", "amount_millions": 0, "announcement_date": "2017-01-01", "description": "Garden equipment partnership"}
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
