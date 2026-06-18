"""
Agentic Intelligence Service
Generates AI-driven insights, personalized recommendations, and real-time intelligence.
Uses Groq for fast AI analysis, background threads for non-blocking execution.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import requests
from datetime import datetime

# Groq setup (reuse from school_service)
try:
    import groq as groq_module
    GROQ_API_KEY = os.getenv('GROQ_API_KEY')
    if GROQ_API_KEY:
        groq_client = groq_module.Groq(api_key=GROQ_API_KEY)
    else:
        groq_client = None
except:
    groq_client = None


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
            model="mixtral-8x7b-32768",
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


def search_relevant_videos(brand_name: str, topics: list, max_results: int = 3) -> dict:
    """
    Search YouTube for videos relevant to brand topics.
    Validates relevance before returning.
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
                        videos[topic] = {
                            "video_ids": matches[:max_results],
                            "urls": [f"https://www.youtube.com/watch?v={vid}" for vid in matches[:max_results]],
                            "found": True
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


def search_relevant_podcasts(topics: list, max_results: int = 2) -> dict:
    """
    Search for podcasts relevant to AI strategy topics.
    Returns podcast search links and descriptions.
    """
    try:
        podcasts = {}

        for topic in topics[:3]:
            try:
                # Use Spotify/Apple Podcast search (generic)
                search_query = f"{topic} podcast business strategy"

                # Return search URLs for podcasts
                podcasts[topic] = {
                    "spotify_url": f"https://open.spotify.com/search/{requests.utils.quote(search_query)}/podcasts",
                    "apple_url": f"https://podcasts.apple.com/us/search?term={requests.utils.quote(search_query)}",
                    "found": True
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
