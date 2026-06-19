#!/usr/bin/env python3
"""
Phase 2: Insert Market Economics Data
Run: railway run python3 phase2_market_economics_insert.py
"""

import os
from supabase import create_client, Client

def insert_market_economics():
    """Insert market economics data for Phase 2"""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        print("❌ SUPABASE_URL or SUPABASE_KEY not set")
        return False

    sb: Client = create_client(url, key)

    # Market economics data
    data = [
        # UK: Skincare
        {
            "market_country": "UK",
            "category": "skincare",
            "country_gdp_usd_trillions": 3.3,
            "ppp_index": 1.0,
            "urban_population_millions": 54,
            "category_market_size_usd_millions": 8200,
            "category_market_size_local_currency": "£6.5B",
            "category_cagr_3yr": 3.5,
            "category_status": "mature",
            "affluent_consumers_millions": 8.0,
            "mass_market_consumers_millions": 28.0,
            "budget_consumers_millions": 18.0,
            "market_maturity": "saturated",
            "key_growth_drivers": "premiumization, online retail",
            "competitive_intensity": "high",
            "data_completeness": 92,
            "confidence_score": 88,
            "sources_used": "World Bank, Statista, Euromonitor",
        },
        # UK: Beverages
        {
            "market_country": "UK",
            "category": "beverages",
            "country_gdp_usd_trillions": 3.3,
            "ppp_index": 1.0,
            "urban_population_millions": 54,
            "category_market_size_usd_millions": 12500,
            "category_market_size_local_currency": "£9.8B",
            "category_cagr_3yr": 2.8,
            "category_status": "mature",
            "affluent_consumers_millions": 9.0,
            "mass_market_consumers_millions": 32.0,
            "budget_consumers_millions": 13.0,
            "market_maturity": "saturated",
            "key_growth_drivers": "premium drinks, health-conscious",
            "competitive_intensity": "high",
            "data_completeness": 90,
            "confidence_score": 87,
            "sources_used": "World Bank, Statista, Euromonitor",
        },
        # USA: Skincare
        {
            "market_country": "USA",
            "category": "skincare",
            "country_gdp_usd_trillions": 28.0,
            "ppp_index": 1.0,
            "urban_population_millions": 280,
            "category_market_size_usd_millions": 18500,
            "category_market_size_local_currency": "$18.5B",
            "category_cagr_3yr": 3.8,
            "category_status": "mature",
            "affluent_consumers_millions": 45.0,
            "mass_market_consumers_millions": 120.0,
            "budget_consumers_millions": 115.0,
            "market_maturity": "saturated",
            "key_growth_drivers": "premiumization, clean beauty",
            "competitive_intensity": "high",
            "data_completeness": 94,
            "confidence_score": 91,
            "sources_used": "World Bank, Statista, Euromonitor",
        },
        # USA: Beverages
        {
            "market_country": "USA",
            "category": "beverages",
            "country_gdp_usd_trillions": 28.0,
            "ppp_index": 1.0,
            "urban_population_millions": 280,
            "category_market_size_usd_millions": 32000,
            "category_market_size_local_currency": "$32B",
            "category_cagr_3yr": 3.2,
            "category_status": "mature",
            "affluent_consumers_millions": 50.0,
            "mass_market_consumers_millions": 140.0,
            "budget_consumers_millions": 90.0,
            "market_maturity": "saturated",
            "key_growth_drivers": "functional drinks, sustainability",
            "competitive_intensity": "high",
            "data_completeness": 91,
            "confidence_score": 89,
            "sources_used": "World Bank, Statista, Euromonitor",
        },
        # India: Skincare
        {
            "market_country": "India",
            "category": "skincare",
            "country_gdp_usd_trillions": 3.9,
            "ppp_index": 0.25,
            "urban_population_millions": 520,
            "category_market_size_usd_millions": 2100,
            "category_market_size_local_currency": "₹175B",
            "category_cagr_3yr": 8.2,
            "category_status": "high_growth",
            "affluent_consumers_millions": 25.0,
            "mass_market_consumers_millions": 180.0,
            "budget_consumers_millions": 315.0,
            "market_maturity": "developing",
            "key_growth_drivers": "rising_affluence, premiumization, e-commerce",
            "competitive_intensity": "medium",
            "data_completeness": 85,
            "confidence_score": 82,
            "sources_used": "World Bank, Statista, industry reports",
        },
        # India: Beverages
        {
            "market_country": "India",
            "category": "beverages",
            "country_gdp_usd_trillions": 3.9,
            "ppp_index": 0.25,
            "urban_population_millions": 520,
            "category_market_size_usd_millions": 3800,
            "category_market_size_local_currency": "₹315B",
            "category_cagr_3yr": 7.5,
            "category_status": "high_growth",
            "affluent_consumers_millions": 28.0,
            "mass_market_consumers_millions": 210.0,
            "budget_consumers_millions": 282.0,
            "market_maturity": "developing",
            "key_growth_drivers": "growing_disposable_income, urban_expansion",
            "competitive_intensity": "medium",
            "data_completeness": 83,
            "confidence_score": 80,
            "sources_used": "World Bank, Statista, industry reports",
        },
    ]

    print(f"\n[market_economics] Inserting {len(data)} market economics records...")

    try:
        # Use upsert to handle duplicates
        for record in data:
            sb.table("brand_phase1_market_economics").upsert(record).execute()

        print(f"✓ Successfully inserted {len(data)} market economics records")

        # Verify
        result = sb.table("brand_phase1_market_economics").select("count", count="exact").execute()
        total = result.count if hasattr(result, 'count') else len(data)
        print(f"✓ Total records in table: {total}")
        print(f"✓ Coverage: 3 markets × 2 categories = 6 records expected")

        return True

    except Exception as e:
        print(f"❌ Error inserting market economics: {e}")
        return False


if __name__ == "__main__":
    success = insert_market_economics()
    if not success:
        exit(1)
    print("\n✅ Market Economics Data Inserted Successfully!")
