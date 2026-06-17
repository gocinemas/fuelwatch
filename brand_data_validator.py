"""
Brand Data Validator
Ensures brand data completeness and quality before marking as "ready to serve".
Only brands with >70% data completeness are considered valid.
"""

import library as lib
from datetime import datetime, timedelta

COMPLETENESS_THRESHOLD = 70  # Minimum 70% data required

def calculate_brand_completeness(brand_name: str) -> dict:
    """
    Calculate data completeness score for a brand (0-100%).
    Returns: {
        "overall": 75,  # percentage
        "scores": {
            "profile": 100,
            "financials": 50,
            "products": 0,
            "competitors": 100,
            "white_space": 0,
            "social": 60,
            "news": 100,
            "ai_strategy": 80
        },
        "is_complete": True,  # True if >= 70%
        "missing_sections": ["products", "white_space"],
        "quality_level": "GOOD"  # EXCELLENT (90+), GOOD (70-89), POOR (<70)
    }
    """
    try:
        sb = lib._sb()

        scores = {
            "profile": 0,
            "financials": 0,
            "products": 0,
            "competitors": 0,
            "white_space": 0,
            "social": 0,
            "news": 0,
            "ai_strategy": 0,
        }

        # 1. Profile Completeness
        profile = sb.table("brand_profile").select("*").eq("name", brand_name).execute().data
        if profile:
            p = profile[0]
            profile_fields = [
                p.get("founded_year"),
                p.get("origin_city"),
                p.get("origin_country"),
                p.get("description"),
                p.get("website"),
                p.get("headquarters")
            ]
            filled = sum(1 for f in profile_fields if f and f != "—")
            scores["profile"] = int((filled / len(profile_fields)) * 100) if profile_fields else 0

        # 2. Financials Completeness
        financials = sb.table("brand_financials").select("*").eq("brand_name", brand_name).limit(1).execute().data
        if financials:
            f = financials[0]
            fin_fields = [
                f.get("revenue"),
                f.get("market_cap"),
                f.get("profit_margin"),
                f.get("growth_rate")
            ]
            filled = sum(1 for field in fin_fields if field and field != "—" and field is not None)
            scores["financials"] = int((filled / len(fin_fields)) * 100) if fin_fields else 0

        # 3. Products Completeness
        products = sb.table("brand_skus_complete").select("*").eq("brand_name", brand_name).execute().data
        if products and len(products) >= 3:  # Need at least 3 SKUs
            scores["products"] = min(100, len(products) * 20)  # 5 SKUs = 100%
        elif products:
            scores["products"] = len(products) * 25

        # 4. Competitors Completeness
        competitors = sb.table("brand_competitors_complete").select("*").eq("brand_name", brand_name).execute().data
        if competitors and len(competitors) >= 2:  # Need at least 2 competitors
            scores["competitors"] = min(100, len(competitors) * 25)

        # 5. White Space Completeness
        white_space = sb.table("brand_white_space").select("*").eq("brand_name", brand_name).execute().data
        if white_space and len(white_space) >= 2:
            scores["white_space"] = min(100, len(white_space) * 30)

        # 6. Social Media Completeness
        social = sb.table("brand_social_media").select("*").eq("brand_name", brand_name).execute().data
        if social and len(social) >= 2:  # Need at least 2 platforms
            scores["social"] = min(100, len(social) * 25)

        # 7. News Completeness
        news = sb.table("brand_news").select("*").eq("brand_name", brand_name).execute().data
        if news and len(news) >= 2:  # Need at least 2 news items
            scores["news"] = min(100, len(news) * 30)

        # 8. AI Strategy Completeness
        ai = sb.table("brand_ai_strategy").select("*").eq("brand_name", brand_name).execute().data
        if ai and len(ai) >= 1:  # At least 1 AI focus
            scores["ai_strategy"] = min(100, len(ai) * 40)

        # Calculate overall
        overall = int(sum(scores.values()) / len(scores))
        is_complete = overall >= COMPLETENESS_THRESHOLD

        # Quality level
        if overall >= 90:
            quality = "EXCELLENT"
        elif overall >= 70:
            quality = "GOOD"
        else:
            quality = "POOR"

        missing = [k for k, v in scores.items() if v < 50]

        return {
            "brand": brand_name,
            "overall": overall,
            "scores": scores,
            "is_complete": is_complete,
            "threshold": COMPLETENESS_THRESHOLD,
            "missing_sections": missing,
            "quality_level": quality,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        print(f"[validator] Error calculating completeness for {brand_name}: {e}")
        return {
            "brand": brand_name,
            "overall": 0,
            "scores": {},
            "is_complete": False,
            "quality_level": "ERROR"
        }


def should_retry_fetch(brand_name: str) -> bool:
    """
    Check if we should retry fetching data for a brand.
    Returns True if:
    - Brand completeness < 70%, AND
    - Last fetch was > 24 hours ago
    """
    completeness = calculate_brand_completeness(brand_name)

    if completeness["is_complete"]:
        return False  # Already good

    # Check last fetch timestamp (would need to track this in DB)
    # For now, return True to allow retry
    return completeness["overall"] < 70


def get_data_quality_badge(completeness_score: int) -> str:
    """Return visual badge for data quality"""
    if completeness_score >= 90:
        return "🟢 EXCELLENT (90%+)"
    elif completeness_score >= 70:
        return "🟡 GOOD (70-89%)"
    else:
        return "🔴 INCOMPLETE (<70%)"


def mark_brand_as_fetched(brand_name: str, completeness: int) -> bool:
    """
    Track when a brand was fetched and its quality.
    This prevents re-fetching good data constantly.
    """
    try:
        sb = lib._sb()

        # Update brand_profile with fetch metadata
        sb.table("brand_profile").update({
            "last_fetch": datetime.now().isoformat(),
            "data_quality_score": completeness
        }).eq("name", brand_name).execute()

        return True

    except Exception as e:
        print(f"[validator] Error marking {brand_name} as fetched: {e}")
        return False
