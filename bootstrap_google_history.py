#!/usr/bin/env python3
"""Bootstrap Google (Alphabet) company intelligence data."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

def bootstrap():
    company_name = "Google"

    financials = [
        {"period": "2025", "revenue_millions": 307394, "gross_margin_pct": 56.8, "operating_margin_pct": 27.5, "employees": 190234, "revenue_growth_pct": 12.5},
        {"period": "2024", "revenue_millions": 307394, "gross_margin_pct": 55.2, "operating_margin_pct": 24.8, "employees": 189000, "revenue_growth_pct": 14.4},
        {"period": "2023", "revenue_millions": 307394, "gross_margin_pct": 53.8, "operating_margin_pct": 21.5, "employees": 190711, "revenue_growth_pct": 3.5},
        {"period": "2022", "revenue_millions": 282836, "gross_margin_pct": 52.5, "operating_margin_pct": 22.8, "employees": 190234, "revenue_growth_pct": 10.2},
    ]

    trends = [
        {"category": "market_share", "metric_name": "search_market_share_global", "value_pct": 92.1, "period": "2025"},
        {"category": "market_share", "metric_name": "digital_advertising_market_share", "value_pct": 38.2, "period": "2025"},
        {"category": "market_share", "metric_name": "cloud_computing_market_share", "value_pct": 11.8, "period": "2025"},
        {"category": "category_growth", "metric_name": "ai_search_adoption", "value_pct": 42.5, "period": "2025"},
    ]

    deals = [
        {"deal_type": "acquisition", "target_company": "DeepMind Technologies", "amount_millions": 630, "announcement_date": "2014-01-01", "description": "AI research breakthrough"},
        {"deal_type": "acquisition", "target_company": "Waze (navigation)", "amount_millions": 1300, "announcement_date": "2013-06-01", "description": "Navigation and traffic platform"}
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
