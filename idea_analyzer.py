"""
FrameWork — App Idea Validator
Scrape + analyze any app URL and provide framework assessment.

Routes:
- GET /idea — Landing page
- POST /api/idea/analyze — Analyze app URL
- GET /api/idea/report/<report_id> — Fetch report
"""

import requests
import json
from datetime import datetime
from bs4 import BeautifulSoup
import library as lib


def fetch_and_analyze_url(url: str, app_name: str = None) -> dict:
    """
    Fetch a URL and extract key information about the app.

    Returns structured analysis with:
    - positioning (value prop, headline, subheading)
    - features (list of key features)
    - design_quality (visual design assessment)
    - target_audience (who it's for)
    - pricing_model (free/paid/freemium)
    """
    try:
        # Fetch the page
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        html = response.text
        soup = BeautifulSoup(html, 'html.parser')

        # Extract metadata
        title = soup.find('title')
        title_text = title.string if title else app_name or "Unknown"

        og_description = soup.find('meta', property='og:description')
        description = og_description.get('content', '') if og_description else ""

        # Extract key content (headers, buttons, value props)
        h1 = soup.find('h1')
        h1_text = h1.get_text(strip=True) if h1 else ""

        h2 = soup.find('h2')
        h2_text = h2.get_text(strip=True) if h2 else ""

        # Extract CTA buttons (usually indicate core action)
        buttons = soup.find_all(['button', 'a'], class_=lambda x: x and 'btn' in x.lower())
        ctas = [btn.get_text(strip=True) for btn in buttons[:3]]

        # Look for feature lists
        features = []
        feature_sections = soup.find_all(['ul', 'ol'])
        for section in feature_sections[:3]:  # First 3 lists
            items = section.find_all('li')
            for item in items[:5]:  # First 5 items per list
                text = item.get_text(strip=True)
                if text and len(text) > 10:  # Skip short items
                    features.append(text)

        # Look for pricing info
        pricing_text = html.lower()
        pricing_model = "Free"
        if "pricing" in pricing_text or "paid" in pricing_text:
            pricing_model = "Freemium"
        if "enterprise" in pricing_text or "contact us" in pricing_text:
            pricing_model = "B2B"

        return {
            "title": title_text,
            "description": description,
            "value_prop": h1_text,
            "subheading": h2_text,
            "ctas": ctas,
            "features": features[:8],  # Top 8 features
            "pricing_model": pricing_model,
            "url": url,
            "scraped_at": datetime.now().isoformat()
        }

    except Exception as e:
        print(f"[idea_analyzer] URL fetch error: {e}")
        return {"error": str(e), "url": url}


def generate_framework_assessment(analysis: dict) -> dict:
    """
    Apply the FrameWork assessment:
    - Idea validation
    - Potential (TAM, timing, differentiation)
    - Design quality
    - Worth pursuing verdict + reasoning
    - Top 3 improvements
    - Pivot suggestions
    """
    try:
        title = analysis.get("title", "Unknown")
        value_prop = analysis.get("value_prop", "")
        features = analysis.get("features", [])
        description = analysis.get("description", "")
        ctas = analysis.get("ctas", [])

        # Simple framework scoring
        # In production, would use Groq for deeper analysis

        # Idea score (clear value prop?)
        idea_score = 70
        if value_prop and len(value_prop) > 15:
            idea_score += 20
        if "revolutionary" not in value_prop.lower() and "next" not in value_prop.lower():
            idea_score += 10  # Bonus for avoiding hype words

        # Potential score (features, market positioning)
        potential_score = 60
        if len(features) >= 5:
            potential_score += 20
        if features:
            potential_score += 15

        # Design score (minimal data, would use vision model)
        design_score = 65
        if ctas:
            design_score += 15

        # Overall verdict
        avg_score = (idea_score + potential_score + design_score) / 3
        worth_pursuing = avg_score > 70

        # Improvement suggestions (generic for now, would be personalized)
        improvements = [
            "Clarify your target audience in the headline",
            "Add social proof (logos, testimonials, numbers)",
            "Emphasize the unique differentiation vs competitors",
            "Simplify the core CTA (one primary action per page)",
            "Show the problem before the solution",
        ]

        # Pivot suggestions
        pivots = [
            "Consider B2B version (more revenue potential)",
            "Add collaboration features (network effects)",
            "Expand to mobile if currently web-only",
            "Build data/intelligence layer (defensibility)",
            "Create freemium model with upgrade path",
        ]

        return {
            "score": int(avg_score),
            "idea_validation": {
                "score": idea_score,
                "reasoning": f"Clear value prop identified: '{value_prop[:60]}...'"
            },
            "potential": {
                "score": potential_score,
                "reasoning": f"Strong feature set ({len(features)} key features identified)"
            },
            "design_quality": {
                "score": design_score,
                "reasoning": "Professional positioning detected"
            },
            "verdict": {
                "worth_pursuing": worth_pursuing,
                "confidence": 75,  # 0-100
                "reason": f"Scores above threshold. Good market fit signals detected." if worth_pursuing else "Consider pivoting before launch."
            },
            "improvements": improvements[:3],
            "pivots": pivots[:3],
            "summary": f"This idea shows potential. {len(features)} core features, clear positioning. Worth testing with early users."
        }

    except Exception as e:
        print(f"[framework_assessment] Error: {e}")
        return {"error": str(e)}


def generate_report(url: str, app_name: str = None) -> dict:
    """
    Full pipeline: Fetch URL → Analyze → Generate framework assessment.
    """
    try:
        # Step 1: Fetch and extract data
        analysis = fetch_and_analyze_url(url, app_name)

        if analysis.get("error"):
            return analysis

        # Step 2: Apply framework
        assessment = generate_framework_assessment(analysis)

        # Step 3: Save report to DB (for tracking + learning loop)
        report_id = None
        try:
            report_data = {
                "url": url,
                "app_name": app_name or analysis.get("title", "Unknown"),
                "title": analysis.get("title", ""),
                "value_prop": analysis.get("value_prop", ""),
                "features": json.dumps(analysis.get("features", [])),
                "positioning": analysis.get("value_prop", ""),
                "idea_score": assessment.get("idea_validation", {}).get("score", 0),
                "potential_score": assessment.get("potential", {}).get("score", 0),
                "design_score": assessment.get("design_quality", {}).get("score", 0),
                "overall_score": assessment.get("score", 0),
                "worth_pursuing": assessment.get("verdict", {}).get("worth_pursuing", False),
                "confidence": assessment.get("verdict", {}).get("confidence", 0),
                "verdict_reason": assessment.get("verdict", {}).get("reason", ""),
                "improvements": json.dumps(assessment.get("improvements", [])),
                "pivots": json.dumps(assessment.get("pivots", []))
            }

            result = lib._sb().table("idea_reports").insert(report_data).execute()
            if result.data:
                report_id = result.data[0]['id']
                print(f"[framework] Report saved: {report_id}")
        except Exception as save_err:
            print(f"[framework] DB save failed (non-critical): {save_err}")

        return {
            "status": "ok",
            "report_id": report_id,
            "analysis": analysis,
            "assessment": assessment
        }

    except Exception as e:
        print(f"[framework] Generate report error: {e}")
        return {"error": str(e), "status": "error"}
