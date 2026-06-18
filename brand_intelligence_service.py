"""
Brand Intelligence Service
Smart data retrieval with background enrichment.

Strategy:
1. Show existing data immediately (no blocking)
2. If < 75% complete → background job enriches the data
3. Next request gets better data
"""

import library as lib
from datetime import datetime
from threading import Thread

# Brand aliases: map user inputs to canonical brand names (not company names)
BRAND_ALIASES = {
    "Coca Cola": ["coke", "coca-cola", "diet coke"],
    "Sprite": ["sprite"],
    "Fanta": ["fanta"],
    "Hershey": ["hersheys", "hershey bars"],
    "Starbucks": ["starbucks coffee", "sbux"],
    "Pepsi": ["pepsi cola"],
    "Tropicana": ["tropicana orange"],
    "Gatorade": ["gatorade"],
    "Nike": ["nike air", "air jordan"],
    "Adidas": ["three stripes"],
    "iPhone": ["iphone"],
    "iPad": ["ipad"],
    "MacBook": ["macbook"],
    "AirPods": ["airpods"],
    "Galaxy": ["samsung galaxy"],
    "Tesla Model S": ["model s", "model 3"],
    "Powerwall": ["powerwall"],
    "Red Bull": ["red bull", "redbull"],
    "Monster Energy": ["monster energy"],
}

def resolve_brand_alias(user_input: str) -> str:
    """Resolve user input to canonical brand name using aliases."""
    user_lower = user_input.lower().strip()

    # Check if input matches any alias
    for canonical_name, aliases in BRAND_ALIASES.items():
        if user_lower in [a.lower() for a in aliases]:
            return canonical_name

    # Return original input if no alias found
    return user_input

def get_brand_intelligence_smart(brand_name: str) -> dict:
    """
    Get brand intelligence with smart background enrichment.

    - Returns existing data immediately
    - If new brand: fetch and populate
    - If incomplete (< 75%): trigger background enrichment
    """
    try:
        from brand_data_validator import calculate_brand_completeness
        sb = lib._sb()

        # Step 1: Resolve alias (e.g., "coke" → "Coca Cola")
        brand_name = resolve_brand_alias(brand_name)

        # Step 2: Normalize for case-insensitive matching
        brand_name_lower = brand_name.lower() if brand_name else brand_name

        # Step 3: Check if brand exists in database
        all_profiles = sb.table("brand_profile").select("name").execute().data
        profile = [p for p in all_profiles if p['name'].lower() == brand_name_lower]

        # If found, use the correct casing from database
        if profile:
            brand_name_normalized = profile[0]['name']
            profile = sb.table("brand_profile").select("*").eq("name", brand_name_normalized).execute().data
        else:
            profile = None
            brand_name_normalized = brand_name.title()  # Fallback for new brands

        # NEW BRAND: Fetch and populate
        if not profile:
            print(f"[smart_service] New brand: {brand_name_normalized}, fetching data...")
            from brand_data_fetcher_v2 import fetch_and_populate_brand
            fetch_and_populate_brand(brand_name_normalized)
            # Re-fetch after population
            profile = sb.table("brand_profile").select("*").eq("name", brand_name_normalized).execute().data

        # GET ALL DATA (don't filter, show what we have)
        financials = sb.table("brand_financials").select("*").eq("brand_name", brand_name_normalized).neq("revenue", None).order("year", desc=True).limit(1).execute().data
        skus = sb.table("brand_skus_complete").select("*").eq("brand_name", brand_name_normalized).order("market_position").execute().data
        competitors = sb.table("brand_competitors_complete").select("*").eq("brand_name", brand_name_normalized).order("market_position").execute().data
        competing_skus = sb.table("competing_skus_complete").select("*").eq("brand_name", brand_name_normalized).execute().data
        white_space = sb.table("brand_white_space").select("*").eq("brand_name", brand_name_normalized).order("opportunity_score", desc=True).execute().data
        social = sb.table("brand_social_media").select("*").eq("brand_name", brand_name_normalized).execute().data
        news = sb.table("brand_news").select("*").eq("brand_name", brand_name_normalized).order("published_date", desc=True).limit(5).execute().data
        podcasts = sb.table("brand_podcasts").select("*").eq("brand_name", brand_name_normalized).order("relevance_score", desc=True).limit(5).execute().data
        ai_strategy = sb.table("brand_ai_strategy").select("*").eq("brand_name", brand_name_normalized).execute().data

        # Calculate completeness
        completeness_data = calculate_brand_completeness(brand_name_normalized)
        completeness_score = completeness_data.get("overall", 0)

        print(f"[smart_service] {brand_name_normalized} completeness: {completeness_score}%")

        # BACKGROUND ENRICHMENT: If < 75% complete, trigger async update
        if completeness_score < 75:
            print(f"[smart_service] Triggering background enrichment for {brand_name_normalized}...")
            thread = Thread(
                target=_background_enrich_brand,
                args=(brand_name_normalized,),
                daemon=True
            )
            thread.start()

        # BUILD RESPONSE with whatever data we have
        from brand_intelligence_engine import (
            _format_brand_fundamentals,
            _format_financials,
            _format_products,
            _format_competitors,
            _format_white_space,
            _format_brand_presence
        )

        result = {
            "brand": _format_brand_fundamentals(profile),
            "financials": _format_financials(financials),
            "products": _format_products(skus, brand_name_normalized),
            "competitors": _format_competitors(competitors, competing_skus),
            "white_space": _format_white_space(white_space),
            "brand_presence": _format_brand_presence(social),
            "intelligence": {
                "latest_news": news,
                "podcasts": podcasts,
                "ai_strategy": [{"focus": a.get("ai_focus_area"), "announced": a.get("announcement_date")} for a in ai_strategy]
            },
            "metadata": {
                "last_updated": datetime.now().isoformat(),
                "data_completeness": completeness_score,
                "quality_level": completeness_data.get("quality_level", "UNKNOWN"),
                "is_enriching": completeness_score < 75  # Tell frontend if background job is running
            }
        }

        return result

    except Exception as e:
        import traceback
        print(f"[smart_service] Error: {e}")
        print(traceback.format_exc())
        return {"error": str(e), "name": brand_name}


def _background_enrich_brand(brand_name: str):
    """Background task: enrich incomplete brand data."""
    try:
        print(f"[background] Starting enrichment for {brand_name}...")
        from brand_data_fetcher_v2 import fetch_and_populate_brand

        # Fetch fresh data and update database
        success = fetch_and_populate_brand(brand_name)

        if success:
            print(f"[background] ✓ Enrichment complete for {brand_name}")
        else:
            print(f"[background] ⚠ Enrichment partial for {brand_name}")

    except Exception as e:
        print(f"[background] Error enriching {brand_name}: {e}")
