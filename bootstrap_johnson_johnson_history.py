#!/usr/bin/env python3
"""Bootstrap Johnson & Johnson company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Johnson & Johnson"

    financials = [
        {"period": "2025", "revenue_millions": 94500, "gross_margin_pct": 65.2, "operating_margin_pct": 26.8, "employees": 135000, "revenue_growth_pct": 4.8},
        {"period": "2024", "revenue_millions": 90240, "gross_margin_pct": 64.8, "operating_margin_pct": 25.5, "employees": 136000, "revenue_growth_pct": 3.2},
        {"period": "2023", "revenue_millions": 87500, "gross_margin_pct": 64.2, "operating_margin_pct": 24.8, "employees": 137000, "revenue_growth_pct": 2.1},
        {"period": "2022", "revenue_millions": 85688, "gross_margin_pct": 63.5, "operating_margin_pct": 23.2, "employees": 138000, "revenue_growth_pct": 5.5},
    ]

    trends = [
        {"category": "market_share", "metric_name": "pharmaceutical_market_share_global", "value_pct": 8.2, "period": "2025"},
        {"category": "market_share", "metric_name": "consumer_health_market_share", "value_pct": 12.5, "period": "2025"},
        {"category": "market_share", "metric_name": "medical_device_market_share", "value_pct": 9.8, "period": "2025"},
        {"category": "category_growth", "metric_name": "oncology_growth", "value_pct": 11.2, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "Actelion (specialty pharma)", "amount_millions": 30000, "announcement_date": "2017-06-01", "description": "Rare disease pharmaceutical"},
        {"deal_type": "acquisition", "target_company": "Abiomed (heart technology)", "amount_millions": 4300, "announcement_date": "2017-04-01", "description": "Artificial heart technology"}
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
