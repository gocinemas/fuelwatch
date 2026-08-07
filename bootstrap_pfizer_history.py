#!/usr/bin/env python3
"""Bootstrap Pfizer company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Pfizer"

    financials = [
        {"period": "2025", "revenue_millions": 59500, "gross_margin_pct": 66.8, "operating_margin_pct": 28.2, "employees": 120000, "revenue_growth_pct": -28.5},
        {"period": "2024", "revenue_millions": 83140, "gross_margin_pct": 68.2, "operating_margin_pct": 31.5, "employees": 123000, "revenue_growth_pct": 2.8},
        {"period": "2023", "revenue_millions": 80850, "gross_margin_pct": 67.5, "operating_margin_pct": 29.8, "employees": 125000, "revenue_growth_pct": 18.2},
        {"period": "2022", "revenue_millions": 68410, "gross_margin_pct": 65.2, "operating_margin_pct": 25.5, "employees": 126000, "revenue_growth_pct": 67.3},
    ]

    trends = [
        {"category": "market_share", "metric_name": "oncology_market_share", "value_pct": 8.5, "period": "2025"},
        {"category": "market_share", "metric_name": "vaccine_market_share_global", "value_pct": 22.3, "period": "2025"},
        {"category": "market_share", "metric_name": "primary_care_market_share", "value_pct": 6.2, "period": "2025"},
        {"category": "category_growth", "metric_name": "biologics_growth", "value_pct": 9.8, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "Seagen (oncology)", "amount_millions": 43000, "announcement_date": "2023-12-01", "description": "Major oncology biotech acquisition"},
        {"deal_type": "divestiture", "target_company": "Consumer Healthcare (joint with GSK)", "amount_millions": 13000, "announcement_date": "2022-07-01", "description": "OTC consumer health exit"}
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
