"""
Phase 1 Batch Insert Script
Loads 60 brand-market records from JSON and inserts into Supabase.
"""

import json
import library as lib
from datetime import datetime

def batch_insert_phase1_data(json_file: str) -> dict:
    """
    Load JSON and batch insert into Supabase brand_phase1_intelligence table.
    """
    try:
        # Load JSON
        with open(json_file, 'r') as f:
            records = json.load(f)

        print(f"\n[batch_insert] Loaded {len(records)} records from {json_file}")

        # Initialize Supabase
        sb = lib._sb()

        # Normalize records for Supabase
        normalized_records = []

        for record in records:
            # Map country names to match schema
            market_country = record.get("market_country", "").strip()
            if market_country == "United Kingdom":
                market_country = "UK"
            elif market_country == "United States":
                market_country = "USA"
            elif market_country == "India":
                market_country = "India"

            normalized = {
                "brand_name": record.get("brand_name"),
                "category": record.get("category"),
                "market_country": market_country,
                "market_iso_code": record.get("market_iso_code"),

                # Fundamentals
                "founded_year": record.get("founded_year"),
                "headquarters_city": record.get("headquarters_city"),
                "headquarters_country": record.get("headquarters_country"),
                "official_website": record.get("official_website"),
                "parent_company": record.get("parent_company"),

                # Positioning
                "positioning_tier": record.get("positioning_tier"),
                "positioning_summary": record.get("positioning_summary"),
                "direct_competitor_1": record.get("direct_competitor_1"),
                "direct_competitor_2": record.get("direct_competitor_2"),
                "direct_competitor_3": record.get("direct_competitor_3"),

                # Segment
                "target_demographic": record.get("target_demographic"),
                "target_income_tier": record.get("target_income_tier"),
                "segment_size_millions": record.get("segment_size_millions"),
                "segment_size_source": "Research",

                # Pricing
                "price_local": record.get("price_local"),
                "price_currency": record.get("price_currency"),
                "ppp_index": record.get("ppp_index"),
                "price_usd_equivalent": record.get("price_usd_equivalent"),
                "pricing_rationale": record.get("pricing_rationale"),

                # Category
                "category_growth_cagr_3yr": record.get("category_growth_cagr_3yr"),
                "market_status": record.get("market_status"),
                "growth_driver": record.get("growth_driver"),

                # Distribution
                "distribution_channels": record.get("distribution_channels", []),
                "distribution_strategy": record.get("distribution_strategy"),

                # Marketing
                "brand_tagline": record.get("brand_tagline"),
                "primary_benefit": record.get("primary_benefit"),
                "emotional_benefit": record.get("emotional_benefit"),
                "competitive_claim": record.get("competitive_claim"),
                "marketing_tone": record.get("marketing_tone"),
                "marketing_channels": record.get("marketing_channels", []),

                # Quality
                "data_completeness": record.get("data_completeness", 0),
                "sources_used": record.get("sources_used", []),
                "confidence_score": record.get("confidence_score", 0),
                "last_verified_date": datetime.now().date().isoformat(),

                "created_by": "phase1_batch_insert"
            }

            # Remove None values
            normalized = {k: v for k, v in normalized.items() if v is not None}
            normalized_records.append(normalized)

        print(f"[batch_insert] Normalized {len(normalized_records)} records")

        # Batch upsert to Supabase (upsert handles duplicates)
        print(f"[batch_insert] Inserting into brand_phase1_intelligence...")

        response = sb.table("brand_phase1_intelligence").upsert(normalized_records).execute()

        inserted_count = len(response.data) if response.data else 0
        print(f"[batch_insert] ✓ Successfully inserted {inserted_count} records")

        return {
            "success": True,
            "records_loaded": len(records),
            "records_inserted": inserted_count,
            "message": f"Batch insert complete: {inserted_count}/{len(records)} records"
        }

    except Exception as e:
        print(f"[batch_insert] ERROR: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def verify_insertion() -> dict:
    """
    Verify that records were inserted correctly.
    Returns: Count by category, by market, by positioning tier.
    """
    try:
        sb = lib._sb()

        # Query counts
        response = sb.table("brand_phase1_intelligence").select("count", count="exact").execute()
        total_count = response.count if hasattr(response, 'count') else len(response.data)

        # Count by category
        categories = {}
        for category in ["skincare", "beverages", "snacks", "qsr", "household"]:
            try:
                resp = sb.table("brand_phase1_intelligence").select("count", count="exact").eq(
                    "category", category
                ).execute()
                categories[category] = resp.count if hasattr(resp, 'count') else 0
            except:
                categories[category] = 0

        # Count by market
        markets = {}
        for market in ["UK", "USA", "India"]:
            try:
                resp = sb.table("brand_phase1_intelligence").select("count", count="exact").eq(
                    "market_country", market
                ).execute()
                markets[market] = resp.count if hasattr(resp, 'count') else 0
            except:
                markets[market] = 0

        return {
            "total_records": total_count,
            "by_category": categories,
            "by_market": markets
        }

    except Exception as e:
        print(f"[verify] ERROR: {e}")
        return {"error": str(e)}


if __name__ == "__main__":
    import sys

    # Insert
    result = batch_insert_phase1_data("/Users/srevi/fuelwatch/phase1_brand_research_data.json")
    print(f"\n{result}")

    if result.get("success"):
        # Verify
        print("\n[verify] Checking insertion results...")
        verify = verify_insertion()
        print(f"\nTotal Records: {verify.get('total_records')}")
        print(f"By Category: {verify.get('by_category')}")
        print(f"By Market: {verify.get('by_market')}")
