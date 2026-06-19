"""
Phase 1 Service Layer
- Insert collected data into Supabase
- Query Phase 1 data
- Generate market entry recommendations
"""

import os
import json
from datetime import datetime
import library as lib

def insert_phase1_data(brand_data: dict) -> dict:
    """
    Insert Phase 1 collected data into Supabase.
    Returns: {success, record_id, message}
    """
    try:
        sb = lib._sb()

        # Prepare record
        record = {
            "brand_name": brand_data.get("brand_name"),
            "category": brand_data.get("category"),
            "market_country": brand_data.get("market_country"),
            "market_iso_code": brand_data.get("market_iso_code"),

            # Fundamentals
            "founded_year": brand_data.get("founded_year"),
            "headquarters_city": extract_city(brand_data.get("headquarters", "")),
            "headquarters_country": extract_country(brand_data.get("headquarters", "")),
            "official_website": brand_data.get("website"),
            "parent_company": brand_data.get("parent_company"),

            # Positioning (placeholder - will enhance in Phase 2)
            "positioning_tier": infer_positioning_tier(brand_data),
            "positioning_summary": brand_data.get("description", "")[:200],

            # Segment
            "target_demographic": brand_data.get("target_demographic"),
            "target_income_tier": brand_data.get("target_income_tier"),
            "segment_size_millions": brand_data.get("segment_size_millions"),
            "segment_size_source": "Estimate",

            # Pricing
            "ppp_index": brand_data.get("ppp_index"),
            "price_local": brand_data.get("price_local"),
            "price_currency": get_currency_for_market(brand_data.get("market_iso_code")),

            # Category
            "category_growth_cagr_3yr": estimate_category_growth(brand_data),
            "market_status": "emerging",  # Will refine in Phase 2

            # Quality
            "data_completeness": brand_data.get("data_completeness", 0),
            "sources_used": brand_data.get("sources_used", []),
            "confidence_score": brand_data.get("confidence_score", 0),
            "last_verified_date": brand_data.get("last_verified_date"),

            "created_by": "phase1_collector"
        }

        # Remove None values
        record = {k: v for k, v in record.items() if v is not None}

        # Upsert (insert or update if exists)
        response = sb.table("brand_phase1_intelligence").upsert(record).execute()

        if response.data:
            record_id = response.data[0].get("id")
            return {
                "success": True,
                "record_id": record_id,
                "message": f"Inserted {brand_data.get('brand_name')} ({brand_data.get('market_country')})"
            }
        else:
            return {"success": False, "message": "Insert returned no data"}

    except Exception as e:
        print(f"[phase1_service] Insert error: {e}")
        return {"success": False, "message": str(e)}


def get_brand_phase1(brand_name: str, market_country: str) -> dict:
    """
    Retrieve Phase 1 data for a brand in a specific market.
    """
    try:
        sb = lib._sb()

        response = sb.table("brand_phase1_intelligence").select("*").eq(
            "brand_name", brand_name
        ).eq(
            "market_country", market_country
        ).execute()

        if response.data:
            return response.data[0]
        else:
            return {"error": "Not found"}

    except Exception as e:
        print(f"[phase1_service] Query error: {e}")
        return {"error": str(e)}


def query_brands_by_market(market_country: str, category: str = None) -> list:
    """
    Query all Phase 1 brands for a specific market.
    Optionally filter by category.
    """
    try:
        sb = lib._sb()

        query = sb.table("brand_phase1_intelligence").select("*").eq(
            "market_country", market_country
        )

        if category:
            query = query.eq("category", category)

        response = query.execute()
        return response.data

    except Exception as e:
        print(f"[phase1_service] Query error: {e}")
        return []


def score_market_entry(brand_name: str, market_country: str) -> dict:
    """
    Generate market entry scoring for Phase 2.
    Returns: {entry_score, recommendation, rationale}
    """
    try:
        sb = lib._sb()

        # Get Phase 1 data
        response = sb.table("brand_phase1_intelligence").select("*").eq(
            "brand_name", brand_name
        ).eq(
            "market_country", market_country
        ).execute()

        if not response.data:
            return {"error": "Brand-market not found"}

        data = response.data[0]

        # Score components (0-100)
        market_size_score = min(data.get("segment_size_millions", 0) / 50 * 100, 100)
        category_growth_score = min((data.get("category_growth_cagr_3yr", 0) + 5) / 15 * 100, 100)
        purchasing_power_score = (data.get("ppp_index", 0.5) * 100)

        # Estimate competitive intensity (higher = more difficult)
        # Placeholder logic: mature markets have more competition
        market_status = data.get("market_status", "emerging")
        competitive_intensity = 70 if market_status == "mature" else 50

        # Estimate localization effort
        # Placeholder: different regions need different effort
        localization_effort = 40 if market_country in ["UK", "US"] else 70

        # Calculate overall score
        # Formula: (Size × Growth × PPP × PPP) / (Competition × Localization)
        overall_entry_score = (
            (market_size_score * category_growth_score * purchasing_power_score * 100) /
            (competitive_intensity * localization_effort)
        ) / 100

        overall_entry_score = min(overall_entry_score, 100)

        # Recommendation
        if overall_entry_score > 75:
            recommendation = "green"
            recommendation_text = "Strong entry candidate"
        elif overall_entry_score > 50:
            recommendation = "yellow"
            recommendation_text = "Conditional entry - requires strategy"
        else:
            recommendation = "red"
            recommendation_text = "Not recommended without major differentiation"

        return {
            "brand_name": brand_name,
            "market": market_country,
            "entry_score": round(overall_entry_score, 1),
            "recommendation": recommendation,
            "recommendation_text": recommendation_text,
            "factors": {
                "market_size": round(market_size_score, 1),
                "category_growth": round(category_growth_score, 1),
                "purchasing_power": round(purchasing_power_score, 1),
                "competitive_intensity": competitive_intensity,
                "localization_effort": localization_effort
            }
        }

    except Exception as e:
        print(f"[phase1_service] Scoring error: {e}")
        return {"error": str(e)}


# Helper functions

def extract_city(headquarters: str) -> str:
    """Extract city from 'City, Country' string."""
    if not headquarters:
        return None
    parts = headquarters.split(",")
    return parts[0].strip() if parts else None


def extract_country(headquarters: str) -> str:
    """Extract country from 'City, Country' string."""
    if not headquarters:
        return None
    parts = headquarters.split(",")
    return parts[-1].strip() if len(parts) > 1 else None


def get_currency_for_market(market_iso: str) -> str:
    """Get currency code for market ISO code."""
    currency_map = {
        "GB": "GBP",
        "US": "USD",
        "IN": "INR",
        "BR": "BRL",
        "ID": "IDR",
        "CN": "CNY",
        "MX": "MXN"
    }
    return currency_map.get(market_iso, "USD")


def infer_positioning_tier(brand_data: dict) -> str:
    """Infer brand positioning tier from available data."""
    # Placeholder: will enhance with actual brand knowledge
    brand_name = brand_data.get("brand_name", "").lower()

    if any(x in brand_name for x in ["olay", "dove", "garnier"]):
        return "mass-prestige"
    elif any(x in brand_name for x in ["estée", "chanel", "dior"]):
        return "luxury"
    elif any(x in brand_name for x in ["neutrogena", "cetaphil"]):
        return "economy"
    else:
        return "mass-market"


def estimate_category_growth(brand_data: dict) -> float:
    """Estimate category growth based on category + market."""
    category = brand_data.get("category", "").lower()
    market_iso = brand_data.get("market_iso_code", "")

    growth_matrix = {
        ("skincare", "GB"): 3.5,
        ("skincare", "US"): 3.2,
        ("skincare", "IN"): 9.2,
        ("skincare", "BR"): 6.5,
        ("skincare", "ID"): 11.2,
        ("beverages", "GB"): 2.1,
        ("beverages", "US"): 1.8,
        ("beverages", "IN"): 8.5,
        ("beverages", "BR"): 5.2,
        ("beverages", "ID"): 10.1,
    }

    key = (category, market_iso)
    return growth_matrix.get(key, 4.0)  # Default 4% growth


if __name__ == "__main__":
    # Test
    print("Phase 1 Service Layer Ready")
    print("Use: insert_phase1_data(), get_brand_phase1(), score_market_entry()")
