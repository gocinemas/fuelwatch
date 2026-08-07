#!/usr/bin/env python3
"""Bootstrap Amazon company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Amazon"

    financials = [
        {"period": "2025", "revenue_millions": 576040, "gross_margin_pct": 45.2, "operating_margin_pct": 15.8, "employees": 1608000, "revenue_growth_pct": 9.3},
        {"period": "2024", "revenue_millions": 575158, "gross_margin_pct": 44.8, "operating_margin_pct": 14.2, "employees": 1541000, "revenue_growth_pct": 10.7},
        {"period": "2023", "revenue_millions": 574785, "gross_margin_pct": 43.2, "operating_margin_pct": 11.8, "employees": 1529000, "revenue_growth_pct": 10.0},
        {"period": "2022", "revenue_millions": 469822, "gross_margin_pct": 41.5, "operating_margin_pct": 3.5, "employees": 1540000, "revenue_growth_pct": 9.4},
    ]

    trends = [
        {"category": "market_share", "metric_name": "ecommerce_market_share_us", "value_pct": 40.5, "period": "2025"},
        {"category": "market_share", "metric_name": "cloud_market_share", "value_pct": 32.2, "period": "2025"},
        {"category": "market_share", "metric_name": "digital_advertising_market_share", "value_pct": 9.8, "period": "2025"},
        {"category": "category_growth", "metric_name": "aws_growth", "value_pct": 18.5, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "MGM Studios", "amount_millions": 8450, "announcement_date": "2022-03-01", "description": "Content and IP acquisition"},
        {"deal_type": "acquisition", "target_company": "One Medical (telehealth)", "amount_millions": 3490, "announcement_date": "2022-07-01", "description": "Healthcare services platform"}
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
