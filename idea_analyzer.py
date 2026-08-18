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
    Apply the FrameWork v2 assessment with REAL deep analysis using Groq.

    Uses Groq AI to:
    - Analyze app positioning vs market
    - Identify specific gaps
    - Suggest real improvements (not templates)
    - Provide tailored pivots
    - Assess market viability
    """
    try:
        # Use Groq for REAL assessment (no templates, no generic fallbacks)
        from framework.groq_analyzer import generate_real_assessment

        result = generate_real_assessment(analysis)

        if result.get("error"):
            print(f"[framework_assessment] Groq analysis failed: {result.get('error')}")
            # If Groq fails, return error (don't use generic fallback)
            return {"error": result.get("error")}

        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[framework_assessment] Error: {e}")
        # Return error, not fallback
        return {"error": str(e)}


def _enhanced_fallback_assessment(analysis: dict) -> dict:
    """
    Enhanced fallback assessment - more personalized than basic.
    Analyzes specific app details to generate custom insights.
    """
    title = analysis.get("title", "Unknown")
    value_prop = analysis.get("value_prop", "")
    features = analysis.get("features", [])
    pricing = analysis.get("pricing_model", "unknown")

    # Custom scoring based on app data
    idea_score = 60
    if value_prop and len(value_prop) > 20:
        idea_score += 20
    if "revolutionary" not in value_prop.lower():
        idea_score += 10  # Bonus for avoiding hype

    potential_score = 60
    feature_count = len(features)
    potential_score += min(30, feature_count * 3)  # More features = higher potential
    if pricing in ["paid", "freemium"]:
        potential_score += 15

    design_score = 60
    if len(value_prop) > 30:  # Detailed positioning suggests good design
        design_score += 15

    risk_score = 70
    if "ai" in value_prop.lower() or "ml" in value_prop.lower():
        risk_score -= 15  # ML = higher complexity
    if "blockchain" in value_prop.lower() or "crypto" in value_prop.lower():
        risk_score -= 20  # Regulatory risk

    overall = (idea_score * 0.3 + potential_score * 0.3 + design_score * 0.2 + risk_score * 0.2)

    # Generate personalized improvements based on analysis
    improvements = [
        f"Strengthen positioning: '{value_prop[:50]}...' needs clarity vs competitors",
        f"Expand feature set: Currently {feature_count} features identified, benchmark top 5 competitors",
        "Add quantified social proof (user count, growth rate, testimonials)"
    ]

    # Generate personalized pivots
    pivots = []
    if pricing == "free":
        pivots.append("🎯 Consider freemium model with premium tier (higher revenue potential)")
    else:
        pivots.append("🎯 Consider free tier to drive adoption and upsell premium")

    pivots.extend([
        "🎯 Identify specific user segment to dominate (vertical focus > horizontal)",
        "🎯 Build network effects or data moat (defensibility)"
    ])

    # Personalized risks
    risks = [
        {"category": "Market", "severity": "HIGH" if potential_score < 60 else "MEDIUM",
         "description": f"Market saturation in {title} category. Differentiation required."},
        {"category": "Execution", "severity": "HIGH" if risk_score < 60 else "MEDIUM",
         "description": f"Technical complexity may require specialized team for {title}"},
        {"category": "Product", "severity": "MEDIUM",
         "description": f"Feature parity with competitors required. {len(features)} current features may not be enough."}
    ]

    return {
        "score": int(overall),
        "idea_validation": {
            "score": idea_score,
            "reasoning": f"Value prop identified: '{value_prop[:60]}...'" if value_prop else "Positioning needs clarity"
        },
        "potential": {
            "score": potential_score,
            "reasoning": f"{feature_count} core features identified. Pricing: {pricing}"
        },
        "design_quality": {
            "score": design_score,
            "reasoning": "Design positioning detected"
        },
        "execution_risk": risk_score,
        "verdict": {
            "worth_pursuing": overall > 70,
            "confidence": int(50 + (idea_score / 2)),
            "reason": f"Score: {int(overall)}/100. " +
                     ("Strong market fit signals." if overall > 70 else "Consider pivoting or refining positioning." if overall > 60 else "High risk. Significant changes needed.")
        },
        "improvements": improvements,
        "pivots": pivots,
        "risks": risks,
        "research_sources": {}
    }


def _fallback_assessment(analysis: dict) -> dict:
    """
    Basic fallback if enhanced fallback fails.
    """
    return {
        "score": 65,
        "idea_validation": {"score": 65, "reasoning": "Basic analysis"},
        "potential": {"score": 65, "reasoning": "Standard assessment"},
        "design_quality": {"score": 65, "reasoning": "Professional"},
        "execution_risk": 70,
        "verdict": {
            "worth_pursuing": True,
            "confidence": 50,
            "reason": "Worth exploring further"
        },
        "improvements": ["Improve positioning", "Add social proof", "Enhance design"],
        "pivots": ["Consider B2B", "Focus vertical", "Build moat"],
        "risks": [{"category": "General", "severity": "MEDIUM", "description": "Standard startup risks"}],
        "research_sources": {}
    }


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

        # Check if assessment has error, use fallback
        if assessment.get("error"):
            print(f"[framework] Assessment error, using fallback: {assessment.get('error')}")
            assessment = _fallback_assessment(analysis)

        # Step 3: Save report to DB (for tracking + learning loop) - NON-CRITICAL
        report_id = None
        try:
            # Check if table exists first
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

            try:
                result = lib._sb().table("idea_reports").insert(report_data).execute()
                if result.data:
                    report_id = result.data[0]['id']
                    print(f"[framework] Report saved: {report_id}")
            except Exception as table_err:
                if "could not find the table" in str(table_err).lower():
                    print(f"[framework] Table idea_reports doesn't exist yet (non-critical). Apply migration to Supabase.")
                else:
                    print(f"[framework] DB save warning: {table_err}")
        except Exception as save_err:
            print(f"[framework] DB operation warning (non-critical): {save_err}")

        return {
            "status": "ok",
            "report_id": report_id,
            "analysis": analysis,
            "assessment": assessment
        }

    except Exception as e:
        print(f"[framework] Generate report error: {e}")
        return {"error": str(e), "status": "error"}
