"""
Brand Intelligence Engine
Aggregates brand data and generates insights:
- White space detection
- Growth adjacency scoring
- Competitive positioning
- Social investment analysis
"""

import json
from datetime import datetime

def get_brand_full_intelligence(brand_name: str) -> dict:
    """
    Comprehensive brand intelligence report combining all data sources.

    Returns structured intelligence for:
    - Brand fundamentals (history, origin, tagline)
    - Financials (revenue, market cap, margins)
    - Products (bestsellers by country)
    - Competitors (ranked, with SKUs)
    - White space (market gaps, adjacencies)
    - Brand presence (social media, campaigns)
    - Real-time intelligence (news, podcasts, AI focus)
    """

    try:
        import library as lib
        from brand_data_fetcher import fetch_and_populate_brand
        sb = lib._sb()

        # If brand not in DB, fetch and populate it
        from brand_data_validator import calculate_brand_completeness, should_retry_fetch, mark_brand_as_fetched

        profile = sb.table("brand_profile").select("*").eq("name", brand_name).execute().data

        if not profile:
            # Brand not in DB, fetch it
            fetch_and_populate_brand(brand_name)
        elif should_retry_fetch(brand_name):
            # Brand exists but data quality too low, retry fetch
            print(f"[intelligence] Retrying fetch for {brand_name} (low completeness)")
            fetch_and_populate_brand(brand_name)

        # Fetch all brand data in parallel
        profile = sb.table("brand_profile").select("*").eq("name", brand_name).execute().data
        financials = sb.table("brand_financials").select("*").eq("brand_name", brand_name).order("year", desc=True).limit(1).execute().data
        skus = sb.table("brand_skus_complete").select("*").eq("brand_name", brand_name).order("market_position").execute().data
        competitors = sb.table("brand_competitors_complete").select("*").eq("brand_name", brand_name).order("market_position").execute().data
        competing_skus = sb.table("competing_skus_complete").select("*").eq("brand_name", brand_name).execute().data
        white_space = sb.table("brand_white_space").select("*").eq("brand_name", brand_name).order("opportunity_score", desc=True).execute().data
        social = sb.table("brand_social_media").select("*").eq("brand_name", brand_name).execute().data
        news = sb.table("brand_news").select("*").eq("brand_name", brand_name).order("published_date", desc=True).limit(5).execute().data
        podcasts = sb.table("brand_podcasts").select("*").eq("brand_name", brand_name).order("relevance_score", desc=True).limit(5).execute().data
        ai_strategy = sb.table("brand_ai_strategy").select("*").eq("brand_name", brand_name).execute().data

        # Build response
        result = {
            "brand": _format_brand_fundamentals(profile),
            "financials": _format_financials(financials),
            "products": _format_products(skus, brand_name),
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
                "data_completeness": _calculate_completeness(profile, financials, skus, competitors, white_space, social, news),
                "quality_assessment": calculate_brand_completeness(brand_name)
            }
        }

        return result

    except Exception as e:
        print(f"[brand_intelligence] Error fetching brand data for {brand_name}: {e}")
        return {"error": str(e), "name": brand_name}


def _format_brand_fundamentals(profile_data):
    """Format brand profile section"""
    if not profile_data:
        return {}

    p = profile_data[0]
    return {
        "name": p.get("name"),
        "founded": p.get("founded_year"),
        "origin": {
            "city": p.get("origin_city"),
            "country": p.get("origin_country")
        },
        "tagline": p.get("tagline"),
        "description": p.get("description"),
        "website": p.get("website"),
        "headquarters": p.get("headquarters"),
        "logo": p.get("logo_url")
    }


def _format_financials(financials_data):
    """Format financial metrics section"""
    if not financials_data:
        return {}

    f = financials_data[0]
    return {
        "year": f.get("year"),
        "revenue": f.get("revenue"),
        "market_cap": f.get("market_cap"),
        "profit_margin": f"{f.get('profit_margin')}%" if f.get("profit_margin") else "—",
        "growth_rate": f"{f.get('growth_rate')}% YoY" if f.get("growth_rate") else "—",
        "net_income": f.get("net_income"),
        "ebitda": f.get("ebitda"),
        "source": f.get("source")
    }


def _format_products(skus_data, brand_name):
    """Format product ecosystem with global bestseller and country breakdown"""
    if not skus_data:
        return {"global_bestseller": None, "by_country": {}}

    # Get global bestseller (no country filter or world)
    global_bestseller = next((s for s in skus_data if not s.get("country") or s.get("country") == "GLOBAL"), None) or skus_data[0]

    # Group by country
    by_country = {}
    for sku in skus_data:
        country = sku.get("country", "Global")
        if country not in by_country:
            by_country[country] = []
        by_country[country].append({
            "name": sku.get("sku_name"),
            "category": sku.get("category"),
            "price": sku.get("price"),
            "sales_monthly": sku.get("monthly_sales_estimate"),
            "position": sku.get("market_position"),
            "released": sku.get("release_year")
        })

    return {
        "global_bestseller": {
            "name": global_bestseller.get("sku_name"),
            "category": global_bestseller.get("category"),
            "price": global_bestseller.get("price"),
            "monthly_sales": global_bestseller.get("monthly_sales_estimate"),
            "position": "#1 Global"
        },
        "by_country": by_country
    }


def _format_competitors(competitors_data, competing_skus_data):
    """Format competitive landscape with competing SKUs"""
    if not competitors_data:
        return {"direct_competitors": [], "competing_skus": {}}

    # Format competitors ranked by market position
    direct_competitors = [
        {
            "name": c.get("competitor_name"),
            "market_position": c.get("market_position"),
            "market_share": f"{c.get('market_share')}%" if c.get("market_share") else "—",
            "vs_brand": c.get("head_to_head")
        }
        for c in competitors_data
    ]

    # Group competing SKUs by competitor
    competing_by_competitor = {}
    for sku in competing_skus_data:
        comp_name = sku.get("competitor_name")
        if comp_name not in competing_by_competitor:
            competing_by_competitor[comp_name] = []
        competing_by_competitor[comp_name].append({
            "sku": sku.get("competitor_sku"),
            "category": sku.get("category"),
            "price": sku.get("price"),
            "market_position": sku.get("market_position")
        })

    return {
        "direct_competitors": direct_competitors,
        "competing_skus": competing_by_competitor
    }


def _format_white_space(white_space_data):
    """Format market opportunity and growth adjacencies"""
    if not white_space_data:
        return {"market_gaps": [], "growth_adjacencies": []}

    # Separate gaps and adjacencies
    gaps = [
        {
            "gap": w.get("gap_type"),
            "description": w.get("description"),
            "market_size": w.get("market_size"),
            "opportunity_score": w.get("opportunity_score")
        }
        for w in white_space_data if w.get("gap_type") and "gap" in w.get("gap_type", "").lower()
    ]

    adjacencies = [
        {
            "adjacency": w.get("growth_adjacency"),
            "fit_score": w.get("fit_score"),
            "opportunity": w.get("opportunity_score")
        }
        for w in white_space_data if w.get("growth_adjacency")
    ]

    return {
        "market_gaps": gaps,
        "growth_adjacencies": adjacencies
    }


def _format_brand_presence(social_data):
    """Format social media and brand investment analysis"""
    if not social_data:
        return {}

    by_platform = {}
    total_investment = 0
    total_followers = 0

    for s in social_data:
        platform = s.get("platform")
        by_platform[platform] = {
            "followers": s.get("followers"),
            "reach": s.get("reach"),
            "engagement_rate": f"{s.get('engagement_rate')}%" if s.get("engagement_rate") else "—",
            "monthly_ad_spend": s.get("estimated_monthly_ad_spend")
        }

    return {
        "by_platform": by_platform,
        "investment_overview": {
            "total_platforms": len(social_data),
            "avg_engagement": f"{sum(s.get('engagement_rate', 0) for s in social_data) / len(social_data):.1f}%" if social_data else "—",
            "investment_intensity": "HIGH" if any("M" in (s.get("estimated_monthly_ad_spend") or "") for s in social_data) else "MEDIUM"
        }
    }


def _calculate_completeness(profile, financials, skus, competitors, white_space, social, news):
    """Calculate data completeness score (0-100)"""
    completeness = 0
    max_points = 700  # 100 points per data category

    if profile:
        completeness += 100
    if financials:
        completeness += 100
    if skus:
        completeness += 100
    if competitors:
        completeness += 100
    if white_space:
        completeness += 100
    if social:
        completeness += 100
    if news:
        completeness += 100

    return int((completeness / max_points) * 100)
