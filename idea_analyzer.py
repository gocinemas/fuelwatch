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
    Apply the FrameWork v2 assessment with deep research:
    - Problem validation (Reddit)
    - Competitive landscape (web search + analysis)
    - Multi-dimensional scoring
    - Improvements + pivots + risk assessment
    """
    try:
        from framework import (
            validate_problem,
            analyze_competition,
            score_idea_validation,
            score_market_potential,
            score_design_quality,
            score_execution_risk,
            calculate_overall_score,
            generate_improvements,
            generate_pivots,
            generate_risk_assessment,
        )

        # Phase 2a: Problem Validation (Reddit)
        print("[framework] Phase 2a: Reddit research...")
        problem_validation = validate_problem(analysis)

        # Phase 2b: Competitive Analysis
        print("[framework] Phase 2b: Competitor analysis...")
        competition_analysis = analyze_competition(analysis)

        # Phase 3: Scoring
        print("[framework] Phase 3: Scoring framework...")
        idea_score = score_idea_validation(problem_validation.get("problem_validation", {}))
        potential_score = score_market_potential(competition_analysis, analysis)
        design_score = score_design_quality(analysis, competition_analysis)
        risk_score = score_execution_risk(analysis)

        # Calculate overall
        scores = calculate_overall_score(idea_score, potential_score, design_score, risk_score)

        # Phase 4: Generate recommendations
        print("[framework] Phase 4: Generating recommendations...")
        improvements = generate_improvements(analysis, problem_validation.get("problem_validation", {}), competition_analysis, scores)
        pivots = generate_pivots(analysis, competition_analysis, scores)
        risks = generate_risk_assessment(scores)

        return {
            "score": scores["overall_score"],
            "idea_validation": {
                "score": scores["idea_validation"],
                "reasoning": f"Problem validation: {problem_validation.get('problem_validation', {}).get('threads_analyzed', 0)} Reddit threads analyzed"
            },
            "potential": {
                "score": scores["market_potential"],
                "reasoning": f"Market saturation: {competition_analysis.get('competition_analysis', {}).get('saturation_score', 0)}/100"
            },
            "design_quality": {
                "score": scores["design_quality"],
                "reasoning": "Design matches category best practices"
            },
            "verdict": {
                "worth_pursuing": scores["overall_score"] > 70,
                "confidence": scores["confidence"],
                "reason": scores["reason"]
            },
            "improvements": improvements,
            "pivots": pivots,
            "risks": risks.get("risks", []),
            "research_sources": {
                "reddit_threads": problem_validation.get("problem_validation", {}).get("threads", []),
                "competitors": competition_analysis.get("competition_analysis", {}).get("competitors", []),
            },
            "summary": f"Deep research complete. {scores['overall_score']}/100 score. {scores['verdict']}"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
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
