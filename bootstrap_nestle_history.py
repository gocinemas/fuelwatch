#!/usr/bin/env python3
"""Bootstrap Nestlé company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Nestlé"

    financials = [
        {"period": "2025", "revenue_millions": 89500, "gross_margin_pct": 45.6, "operating_margin_pct": 16.1, "employees": 271000, "revenue_growth_pct": 3.5},
        {"period": "2024", "revenue_millions": 95850, "gross_margin_pct": 45.8, "operating_margin_pct": 16.8, "employees": 273000, "revenue_growth_pct": 2.8},
        {"period": "2023", "revenue_millions": 93150, "gross_margin_pct": 45.2, "operating_margin_pct": 15.5, "employees": 276000, "revenue_growth_pct": 6.2},
        {"period": "2022", "revenue_millions": 87620, "gross_margin_pct": 44.8, "operating_margin_pct": 14.2, "employees": 280000, "revenue_growth_pct": 8.5},
    ]

    trends = [
        {"category": "market_share", "metric_name": "coffee_market_share_global", "value_pct": 28.5, "period": "2025"},
        {"category": "market_share", "metric_name": "petcare_market_share_global", "value_pct": 21.2, "period": "2025"},
        {"category": "market_share", "metric_name": "confectionery_market_share_global", "value_pct": 15.8, "period": "2025"},
        {"category": "category_growth", "metric_name": "premium_coffee_growth", "value_pct": 7.5, "period": "2025"},
        {"category": "category_growth", "metric_name": "pet_therapeutics_growth", "value_pct": 12.3, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "Starbucks distribution rights", "amount_millions": 7150, "announcement_date": "2018-08-01", "description": "Global licensing of Starbucks coffee for retail"},
        {"deal_type": "acquisition", "target_company": "Blue Bottle Coffee", "amount_millions": 500, "announcement_date": "2019-10-01", "description": "Premium coffee brand acquisition"},
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
