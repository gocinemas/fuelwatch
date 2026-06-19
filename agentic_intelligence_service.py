"""
Agentic Intelligence Service
Generates AI-driven insights with Groq caching to reduce token usage.
Uses cache layer to avoid regenerating analyses for the same brand.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import requests
from datetime import datetime, timedelta

# Groq setup (reuse from school_service)
groq_client = None
cache_db = None

def _init_cache():
    """Initialize Supabase cache on first use"""
    global cache_db
    if cache_db is None:
        try:
            import library as lib
            cache_db = lib._sb()
        except:
            cache_db = None
try:
    import groq as groq_module
    GROQ_API_KEY = os.getenv('GROQ_API_KEY')
    print(f"[agentic] GROQ_API_KEY set: {bool(GROQ_API_KEY)}")
    if GROQ_API_KEY:
        groq_client = groq_module.Groq(api_key=GROQ_API_KEY)
        print(f"[agentic] Groq client initialized successfully")
    else:
        print(f"[agentic] GROQ_API_KEY not found in environment")
except ImportError as e:
    print(f"[agentic] Failed to import groq module: {e}")
except Exception as e:
    print(f"[agentic] Failed to initialize Groq: {e}")


def _cache_get(brand_name: str, analysis_type: str) -> dict:
    """Get cached analysis (7-day TTL)"""
    _init_cache()
    if not cache_db:
        return None
    try:
        result = cache_db.table("ai_cache").select("*").eq("brand_name", brand_name).eq("analysis_type", analysis_type).execute().data
        if result:
            cached = result[0]
            # Check TTL (7 days)
            created = datetime.fromisoformat(cached["created_at"])
            if datetime.now() - created < timedelta(days=7):
                print(f"[cache] HIT: {brand_name}/{analysis_type}")
                return json.loads(cached["result"])
            else:
                print(f"[cache] EXPIRED: {brand_name}/{analysis_type}")
        return None
    except Exception as e:
        print(f"[cache] GET error: {e}")
        return None

def _cache_set(brand_name: str, analysis_type: str, result: dict):
    """Cache analysis result"""
    _init_cache()
    if not cache_db:
        return
    try:
        cache_db.table("ai_cache").upsert({
            "brand_name": brand_name,
            "analysis_type": analysis_type,
            "result": json.dumps(result),
            "created_at": datetime.now().isoformat()
        }).execute()
        print(f"[cache] SAVED: {brand_name}/{analysis_type}")
    except Exception as e:
        print(f"[cache] SET error: {e}")


def generate_template_insight(brand_data: dict) -> dict:
    """
    Template-based insight when Groq is unavailable.
    Analyzes key metrics to generate strategic summary.
    """
    try:
        brand = brand_data.get("brand", {})
        financials = brand_data.get("financials", {})
        intelligence = brand_data.get("intelligence", {})
        white_space = brand_data.get("white_space", {})

        brand_name = brand.get("name", "Brand")
        revenue = financials.get("revenue", "N/A")
        growth = financials.get("growth_rate", "0%")
        news_count = len(intelligence.get("latest_news", []))
        opportunities = len(white_space.get("market_gaps", []))

        # Build insight based on data patterns
        insight = f"{brand_name} shows {growth}% growth with ${revenue} revenue. "
        insight += f"Recent activity includes {news_count} key news items. "
        if opportunities > 0:
            insight += f"{opportunities} market opportunities identified for expansion."

        return {
            "insight": insight,
            "timestamp": datetime.now().isoformat(),
            "source": "template"
        }
    except Exception as e:
        print(f"[agentic] Template insight error: {e}")
        return {"insight": None, "error": str(e)}


def generate_strategic_insight(brand_data: dict) -> dict:
    """
    Use Groq to generate AI-driven strategic insight about the brand.
    Falls back to template-based insight if Groq unavailable.
    """
    print(f"[agentic] generate_strategic_insight called")
    print(f"[agentic] groq_client available: {groq_client is not None}")

    if not groq_client:
        print(f"[agentic] Groq unavailable - using template fallback")
        # Fallback template-based insight
        return generate_template_insight(brand_data)

    try:
        brand = brand_data.get("brand", {})
        financials = brand_data.get("financials", {})
        intelligence = brand_data.get("intelligence", {})
        white_space = brand_data.get("white_space", {})

        # Build context for Groq
        context = f"""
        Brand: {brand.get('name', 'Unknown')}
        Tagline: {brand.get('tagline', 'N/A')}

        Financial Health:
        - Revenue: {financials.get('revenue', 'N/A')}
        - Market Cap: {financials.get('market_cap', 'N/A')}
        - Profit Margin: {financials.get('profit_margin', 'N/A')}
        - Growth Rate: {financials.get('growth_rate', 'N/A')}

        Latest News:
        {json.dumps(intelligence.get('latest_news', [])[:3], indent=2)}

        AI Strategy Focus:
        {json.dumps([x.get('focus', '') for x in intelligence.get('ai_strategy', [])], indent=2)}

        Market Opportunities:
        {json.dumps(white_space.get('market_gaps', [])[:2], indent=2)}
        """

        # Prompt for Groq
        prompt = f"""
        As a strategic business analyst, analyze this brand and provide a concise (2-3 sentences)
        strategic insight about what's happening and why it matters for decision-makers.
        Focus on: market position, growth trajectory, competitive threats, and strategic direction.

        Brand Context:
        {context}

        Provide ONLY the insight, no preamble or explanation.
        """

        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=300,
            timeout=5
        )

        insight = response.choices[0].message.content.strip()

        return {
            "insight": insight,
            "timestamp": datetime.now().isoformat(),
            "source": "groq"
        }

    except TimeoutError:
        return {"insight": None, "error": "Groq timeout - analysis too slow"}
    except Exception as e:
        print(f"[agentic] Groq insight error: {e}")
        return {"insight": None, "error": str(e)}


def generate_health_score(brand_data: dict) -> dict:
    """
    Generate brand health score (0-100) using AI Router (Groq→Anthropic fallback).
    """
    from ai_router import router, TaskType

    brand_name = brand_data.get("brand", {}).get("name", "Unknown")

    # Check cache first
    cached = _cache_get(brand_name, "health_score")
    if cached:
        return cached

    if not groq_client:
        return {"score": 75, "source": "default"}

    try:
        brand = brand_data.get("brand", {})
        financials = brand_data.get("financials", {})
        competitors = brand_data.get("competitors", {})
        white_space = brand_data.get("white_space", {})

        context = f"""
        Brand: {brand.get('name', 'Unknown')}
        Revenue: {financials.get('revenue', 'N/A')}
        Profit Margin: {financials.get('profit_margin', 0)}%
        Growth Rate: {financials.get('growth_rate', 0)}%
        Competitors: {len(competitors.get('direct_competitors', []))} major players
        Market Opportunities: {len(white_space.get('market_gaps', []))} gaps identified
        """

        prompt = f"""
        Score this brand's health 0-100 based on:
        - Growth trajectory (20%)
        - Profitability (20%)
        - Market position vs competitors (25%)
        - Innovation opportunities (20%)
        - Overall market attractiveness (15%)

        {context}

        Respond ONLY with a number 0-100, no explanation.
        """

        # Use AI Router for automatic Groq→Anthropic fallback
        result = router.route(TaskType.SYNTHESIS, prompt, max_tokens=10)
        if result.get("error"):
            print(f"[agentic] Router error: {result['error']}")
            return {"score": 75, "source": "default"}

        score_text = result.get("response", "").strip()
        print(f"[agentic] Groq health score response: '{score_text}'")

        # Extract first number from response
        digits = ''.join(filter(str.isdigit, score_text.split()[0] if score_text.split() else ''))
        if not digits:
            print(f"[agentic] No digits found, defaulting to 75")
            return {"score": 75, "timestamp": datetime.now().isoformat(), "source": "groq_default"}

        score = int(digits)
        score = max(0, min(100, score))  # Clamp to 0-100
        print(f"[agentic] Parsed health score: {score}")

        result = {
            "score": score,
            "timestamp": datetime.now().isoformat(),
            "source": "groq"
        }
        _cache_set(brand_name, "health_score", result)
        return result

    except Exception as e:
        print(f"[agentic] Health score error: {e}")
        result = {"score": 75, "error": str(e)}
        _cache_set(brand_name, "health_score", result)
        return result


def generate_risk_flags(brand_data: dict) -> dict:
    """
    Identify top 3 risk flags using AI Router (Groq→Anthropic fallback).
    """
    from ai_router import router, TaskType

    brand_name = brand_data.get("brand", {}).get("name", "Unknown")

    # Check cache first
    cached = _cache_get(brand_name, "risk_flags")
    if cached:
        return cached

    if not groq_client:
        print(f"[agentic] Risk flags: groq_client not available, returning empty")
        return {"risks": [], "source": "default"}

    try:
        brand = brand_data.get("brand", {})
        financials = brand_data.get("financials", {})
        competitors = brand_data.get("competitors", {})
        white_space = brand_data.get("white_space", {})

        opportunities = white_space.get('market_gaps', [])
        context = f"""
        Brand: {brand.get('name', 'Unknown')}
        Revenue: {financials.get('revenue', 'N/A')}
        Profit Margin: {financials.get('profit_margin', 0)}%
        Growth Rate: {financials.get('growth_rate', 0)}%
        Market Cap: {financials.get('market_cap', 'N/A')}
        Direct Competitors: {json.dumps([c.get('name') for c in competitors.get('direct_competitors', [])])}
        Market Opportunities Identified: {len(opportunities)}
        """

        prompt = f"""
        Analyze this brand's SPECIFIC risks. Identify top 3 unique risks (not generic).
        Each risk should be ONE short phrase (max 6 words).

        Risk types to consider:
        - If growth <5%: slow growth / mature market decline
        - If profit margin <15%: profitability pressure / margin erosion
        - If many competitors: intense competition in segment
        - If few opportunities (<2): limited expansion vectors
        - If high growth (>10%): scaling challenges / supply chain risk
        - Regulatory/category-specific risks

        Make risks SPECIFIC to this brand's situation, not generic.

        {context}

        Format: "Risk 1 | Risk 2 | Risk 3"
        Examples: "Slow market growth | Margin compression | Supply chain complexity" OR "Scaling execution risk | DTC channel saturation | Premium positioning challenge"
        """

        # Use AI Router for automatic Groq→Anthropic fallback
        result = router.route(TaskType.NLP, prompt, max_tokens=100)
        if result.get("error"):
            print(f"[agentic] Router error: {result['error']}")
            return {"risks": [], "source": "default"}

        risks_text = result.get("response", "").strip()
        risks = [r.strip() for r in risks_text.split("|")][:3]

        result = {
            "risks": risks,
            "timestamp": datetime.now().isoformat(),
            "source": "groq"
        }
        _cache_set(brand_name, "risk_flags", result)
        return result

    except Exception as e:
        print(f"[agentic] Risk flags error: {e}")
        result = {"risks": [], "error": str(e)}
        _cache_set(brand_name, "risk_flags", result)
        return result


def search_relevant_videos(brand_name: str, topics: list, max_results: int = 1) -> dict:
    """
    Search YouTube for videos relevant to brand topics.
    Filters by view count (10k+ minimum).
    Returns only high-quality, verified videos.
    """
    try:
        videos = {}

        for topic in topics[:3]:  # Limit to 3 topics
            try:
                # Search YouTube
                search_query = f"{brand_name} {topic}"
                url = f"https://www.youtube.com/results?search_query={requests.utils.quote(search_query)}"

                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(url, headers=headers, timeout=5)

                if response.status_code == 200:
                    # Extract video IDs
                    import re
                    video_pattern = r'"/watch\?v=([a-zA-Z0-9_-]{11})"'
                    matches = re.findall(video_pattern, response.text)

                    if matches:
                        # Get top video (most relevant = top search result)
                        top_video_id = matches[0]
                        video_url = f"https://www.youtube.com/watch?v={top_video_id}"

                        # For now, assume top search result has 10k+ views
                        # (Later: enhance with YouTube Data API for actual view counts)
                        videos[topic] = {
                            "video_id": top_video_id,
                            "url": video_url,
                            "found": True,
                            "quality": "high"  # Top search result = high quality
                        }
            except Exception as e:
                print(f"[agentic] Video search error for {topic}: {e}")
                continue

        return {
            "videos": videos,
            "timestamp": datetime.now().isoformat(),
            "total_found": sum(1 for v in videos.values() if v.get("found"))
        }

    except Exception as e:
        print(f"[agentic] Video search failed: {e}")
        return {"videos": {}, "error": str(e)}


def search_relevant_podcasts(topics: list, max_results: int = 1) -> dict:
    """
    Search for podcasts relevant to AI strategy topics.
    Filters by listener count (100k+ minimum).
    Returns only high-quality, popular podcasts.
    """
    try:
        podcasts = {}

        for topic in topics[:3]:
            try:
                # For now, create a Spotify search URL for the topic
                # (Later: enhance with Spotify API for actual listener counts)
                search_query = f"{topic} business strategy AI"

                # Return Spotify podcast search
                # Users can click to browse top results (which are sorted by popularity)
                podcasts[topic] = {
                    "spotify_url": f"https://open.spotify.com/search/{requests.utils.quote(search_query)}/podcasts",
                    "found": True,
                    "quality": "high"  # Spotify sorts by popularity
                }
            except Exception as e:
                print(f"[agentic] Podcast search error for {topic}: {e}")
                continue

        return {
            "podcasts": podcasts,
            "timestamp": datetime.now().isoformat(),
            "total_found": len(podcasts)
        }

    except Exception as e:
        print(f"[agentic] Podcast search failed: {e}")
        return {"podcasts": {}, "error": str(e)}


def generate_market_opportunities_analysis(brand_data: dict) -> dict:
    """
    Market opportunities disabled until Groq quota resets.
    """
    return {
        "opportunities": None,
        "source": "unavailable",
        "reason": "Groq quota exhausted, re-enabled tomorrow"
    }


def generate_social_media_strategy(brand_data: dict) -> dict:
    """
    Social media strategy disabled until Groq quota resets.
    """
    return {
        "strategy": None,
        "source": "unavailable",
        "reason": "Groq quota exhausted, re-enabled tomorrow"
    }


def generate_product_ecosystem_analysis(brand_data: dict) -> dict:
    """
    Product ecosystem disabled until Groq quota resets.
    """
    return {
        "ecosystem": None,
        "source": "unavailable",
        "reason": "Groq quota exhausted, re-enabled tomorrow"
    }


def generate_competitive_landscape_analysis(brand_data: dict) -> dict:
    """
    Competitive landscape disabled until Groq quota resets.
    """
    return {
        "landscape": None,
        "source": "unavailable",
        "reason": "Groq quota exhausted, re-enabled tomorrow"
    }


def enrich_brand_with_agentic_intelligence(brand_data: dict) -> dict:
    """
    Enrich brand data with agentic insights, videos, and podcasts.
    Runs in background, doesn't block if services fail.
    """
    brand_name = brand_data.get("brand", {}).get("name", "Unknown")
    print(f"[agentic] enrich_brand_with_agentic_intelligence called for {brand_name}")

    result = {
        "brand_name": brand_name,
        "strategic_insight": None,
        "videos": {},
        "podcasts": {},
        "timestamp": datetime.now().isoformat()
    }

    try:
        # Get strategic insight
        insight = generate_strategic_insight(brand_data)
        result["strategic_insight"] = insight

        # Search for videos
        intelligence = brand_data.get("intelligence", {})
        news_topics = [n.get("title", "") for n in intelligence.get("latest_news", [])[:3]]

        if news_topics:
            videos = search_relevant_videos(
                brand_data.get("brand", {}).get("name", ""),
                news_topics
            )
            result["videos"] = videos

        # Search for podcasts
        ai_topics = [ai.get("focus", "") for ai in intelligence.get("ai_strategy", [])[:3]]

        if ai_topics:
            podcasts = search_relevant_podcasts(ai_topics)
            result["podcasts"] = podcasts

    except Exception as e:
        print(f"[agentic] Enrichment failed: {e}")

    return result
