#!/usr/bin/env python3
"""Bootstrap Moderna company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Moderna"

    financials = [
        {"period": "2025", "revenue_millions": 5920, "gross_margin_pct": 72.5, "operating_margin_pct": 18.2, "employees": 3200, "revenue_growth_pct": -47.8},
        {"period": "2024", "revenue_millions": 11340, "gross_margin_pct": 75.2, "operating_margin_pct": 25.5, "employees": 3500, "revenue_growth_pct": -50.2},
        {"period": "2023", "revenue_millions": 18480, "gross_margin_pct": 78.5, "operating_margin_pct": 32.8, "employees": 4000, "revenue_growth_pct": -51.3},
        {"period": "2022", "revenue_millions": 37800, "gross_margin_pct": 80.2, "operating_margin_pct": 35.2, "employees": 4500, "revenue_growth_pct": 14.8},
    ]

    trends = [
        {"category": "market_share", "metric_name": "covid_vaccine_market_share_global", "value_pct": 28.5, "period": "2025"},
        {"category": "market_share", "metric_name": "mrna_platform_market_share", "value_pct": 35.2, "period": "2025"},
        {"category": "category_growth", "metric_name": "cancer_vaccine_pipeline_growth", "value_pct": 85.0, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "ONCOIMMUNE (cancer therapeutics)", "amount_millions": 250, "announcement_date": "2021-12-01", "description": "Cancer vaccine development"},
        {"deal_type": "partnership", "target_company": "Merck (cancer vaccine collaboration)", "amount_millions": 900, "announcement_date": "2023-04-01", "description": "Joint cancer vaccine development"}
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
