"""
FrameWork Phase 2b: Competitor Analysis
Search for similar apps and analyze their positioning, features, and market presence.

Returns competitive landscape + positioning gaps.
"""

import requests
import re
from bs4 import BeautifulSoup


def find_competitors(app_analysis: dict) -> list:
    """
    Search for competing products based on app analysis.

    Query patterns:
    - "alternatives to [app_name]"
    - "[category] tools"
    - "competitors to [app_name]"
    """
    try:
        app_name = app_analysis.get("title", "").strip()
        value_prop = app_analysis.get("value_prop", "").strip()

        if not app_name:
            return []

        # For MVP: Return mock competitor data
        # Production: Use Google Custom Search API or web scraping
        return _get_mock_competitors(app_name, value_prop)

    except Exception as e:
        print(f"[competitors] Error: {e}")
        return []


def analyze_competitor(competitor_name: str, competitor_url: str = None) -> dict:
    """
    Analyze a single competitor:
    - Positioning (headline + value prop)
    - Features (top 5-10)
    - Pricing model
    - Design quality (modern? professional?)
    - Market presence (GitHub stars, Product Hunt ranking, etc.)
    """
    try:
        data = {
            "name": competitor_name,
            "url": competitor_url,
            "positioning": "",
            "features": [],
            "pricing_model": "unknown",
            "design_quality_score": 0,
            "market_presence": {},
        }

        if competitor_url:
            # Fetch and analyze competitor website
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                response = requests.get(competitor_url, headers=headers, timeout=10)
                soup = BeautifulSoup(response.text, "html.parser")

                # Extract positioning
                h1 = soup.find("h1")
                data["positioning"] = h1.get_text(strip=True) if h1 else ""

                # Extract features
                feature_lists = soup.find_all(["ul", "ol"])
                for ul in feature_lists[:2]:
                    items = ul.find_all("li")
                    for li in items[:5]:
                        text = li.get_text(strip=True)
                        if text and len(text) > 10:
                            data["features"].append(text)

                # Detect pricing model
                html_text = response.text.lower()
                if "free trial" in html_text or "freemium" in html_text:
                    data["pricing_model"] = "freemium"
                elif "pricing" in html_text and "$" in html_text:
                    data["pricing_model"] = "paid"
                else:
                    data["pricing_model"] = "free"

                # Design quality heuristic (simple for MVP)
                data["design_quality_score"] = _score_design_quality(response.text)

            except Exception as web_err:
                print(f"[competitor] Failed to analyze {competitor_url}: {web_err}")

        return data

    except Exception as e:
        print(f"[competitor_analysis] Error: {e}")
        return None


def _score_design_quality(html: str) -> int:
    """
    Simple heuristic to score design quality (0-100).
    Real implementation would use visual analysis.
    """
    score = 50  # Base score

    # Check for modern tech indicators
    if "react" in html.lower() or "vue" in html.lower() or "tailwind" in html.lower():
        score += 15
    if "modern" in html.lower() or "elegant" in html.lower():
        score += 10
    if "responsive" in html.lower():
        score += 10

    # Check for poor practices
    if "loading" in html.lower() or "slow" in html.lower():
        score -= 10

    return min(100, max(0, score))


def build_competitive_matrix(app_analysis: dict) -> dict:
    """
    Compare app's features against top competitors.

    Returns:
    - Feature matrix (what each competitor has)
    - Positioning gaps (what's missing)
    - Market saturation score (0-100)
    """
    try:
        competitors = find_competitors(app_analysis)

        if not competitors:
            return {
                "competitors_analyzed": 0,
                "feature_matrix": {},
                "positioning_gaps": [],
                "saturation_score": 50,
            }

        # Build feature matrix
        all_features = set()
        feature_matrix = {}

        app_features = set(app_analysis.get("features", []))

        for comp in competitors:
            comp_analysis = analyze_competitor(comp["name"], comp.get("url"))
            if comp_analysis:
                feature_matrix[comp["name"]] = comp_analysis
                all_features.update(comp_analysis.get("features", []))

        # Find positioning gaps (features in competitors but not in app)
        positioning_gaps = list(all_features - app_features)

        # Calculate saturation score (0-100)
        # More competitors + more similar features = higher saturation
        saturation_score = min(100, len(competitors) * 15 + len(all_features) * 2)

        return {
            "competitors_analyzed": len(competitors),
            "competitors": competitors,
            "feature_matrix": feature_matrix,
            "positioning_gaps": positioning_gaps[:5],  # Top 5 gaps
            "saturation_score": saturation_score,
        }

    except Exception as e:
        print(f"[competitive_matrix] Error: {e}")
        return {
            "competitors_analyzed": 0,
            "feature_matrix": {},
            "positioning_gaps": [],
            "saturation_score": 50,
        }


def _get_mock_competitors(app_name: str, value_prop: str) -> list:
    """
    Mock competitor data for MVP.
    Production: Use web search + Product Hunt API.
    """
    # Generic competitors for any app
    generic_competitors = [
        {
            "name": "Notion",
            "url": "https://notion.so",
            "category": "productivity",
        },
        {
            "name": "Linear",
            "url": "https://linear.app",
            "category": "productivity",
        },
        {
            "name": "Figma",
            "url": "https://figma.com",
            "category": "design",
        },
        {
            "name": "Asana",
            "url": "https://asana.com",
            "category": "productivity",
        },
        {
            "name": "Monday.com",
            "url": "https://monday.com",
            "category": "productivity",
        },
    ]

    # For MVP, return top 3-5
    return generic_competitors[:5]


# Main entry point
def analyze_competition(app_analysis: dict) -> dict:
    """
    Full pipeline: find competitors → analyze each → build matrix → score saturation.
    """
    try:
        competitive_matrix = build_competitive_matrix(app_analysis)

        return {
            "status": "ok",
            "competition_analysis": competitive_matrix
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}
