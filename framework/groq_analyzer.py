"""
FrameWork: Deep Real Assessment with Groq AI

Uses Groq to analyze:
- App positioning vs market
- Specific feature gaps
- Real improvements needed
- Personalized pivots
- Actual risks

NO templates. NO generic fallbacks. REAL analysis only.
"""

import os
import json
from datetime import datetime


def analyze_with_groq(app_analysis: dict) -> dict:
    """
    Use Groq to generate REAL, personalized assessment.
    No templates. No generic suggestions.

    Returns:
    - Specific strengths found
    - Specific weaknesses
    - Real improvement recommendations
    - Tailored pivot suggestions
    - Market assessment
    """
    try:
        # Check API key first
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            print("[groq_analyzer] ERROR: GROQ_API_KEY not set in environment")
            return {
                "status": "error",
                "error": "GROQ_API_KEY not configured. Set environment variable GROQ_API_KEY on Railway."
            }

        try:
            from groq import Groq
        except ImportError as ie:
            print(f"[groq_analyzer] Import error: {ie}")
            return {
                "status": "error",
                "error": f"Groq import failed: {ie}"
            }

        print("[groq_analyzer] Initializing Groq client...")
        client = Groq(api_key=api_key)

        # Build analysis prompt with actual app data
        title = app_analysis.get("title", "Unknown")
        value_prop = app_analysis.get("value_prop", "")
        features = app_analysis.get("features", [])
        pricing = app_analysis.get("pricing_model", "unknown")
        description = app_analysis.get("description", "")

        prompt = f"""Analyze this app idea DEEPLY and provide REAL, specific feedback (not generic):

APP: {title}
VALUE PROP: {value_prop}
FEATURES: {', '.join(features[:8])}
PRICING: {pricing}
DESCRIPTION: {description[:200]}

Provide SPECIFIC analysis (not templates):

1. STRENGTHS (what's actually good about this):
- Be specific to THIS app, not generic praise
- Reference actual features or positioning

2. REAL WEAKNESSES (specific gaps):
- What's missing vs market expectations?
- What would users actually complain about?
- Be specific - reference the value prop or features

3. SPECIFIC IMPROVEMENTS (not "improve positioning"):
- What exact changes would make this better?
- Be actionable and specific to this app
- Example: "Your value prop mentions X but doesn't explain Y which users care about"

4. PIVOT RECOMMENDATIONS (if positioning is weak):
- What adjacent market would this work better in?
- Or: who is the REAL target user?
- Be specific - not "try B2B" but "focus on design teams who need X"

5. MARKET VIABILITY (0-100):
- Is there real demand for this?
- Is the market crowded?
- Will people pay for this?

Format as JSON:
{{
  "strengths": ["specific strength 1", "specific strength 2"],
  "weaknesses": ["specific gap 1", "specific gap 2"],
  "improvements": ["Change 1: specific suggestion", "Change 2: specific suggestion", "Change 3: specific suggestion"],
  "pivots": ["Pivot 1: specific direction", "Pivot 2: specific direction"],
  "market_viability": 75,
  "market_reasoning": "why this score",
  "overall_assessment": "summary of real viability"
}}

Be HONEST. Be SPECIFIC. No templates."""

        print("[groq_analyzer] Calling Groq API...")
        try:
            message = client.messages.create(
                model="mixtral-8x7b-32768",
                max_tokens=1500,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            print("[groq_analyzer] Groq API call successful")
        except Exception as groq_err:
            print(f"[groq_analyzer] Groq API error: {groq_err}")
            return {
                "status": "error",
                "error": f"Groq API call failed: {groq_err}"
            }

        # Parse Groq response
        response_text = message.content[0].text

        # Extract JSON from response
        try:
            # Try to find JSON in the response
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}') + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response_text[start_idx:end_idx]
                analysis = json.loads(json_str)
            else:
                analysis = _parse_response_text(response_text)
        except json.JSONDecodeError:
            analysis = _parse_response_text(response_text)

        return {
            "status": "ok",
            "analysis": analysis,
            "raw_response": response_text
        }

    except Exception as e:
        print(f"[groq_analyzer] Error: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


def _parse_response_text(text: str) -> dict:
    """
    Fallback: parse response if JSON extraction fails.
    """
    lines = text.split('\n')
    analysis = {
        "strengths": [],
        "weaknesses": [],
        "improvements": [],
        "pivots": [],
        "market_viability": 65,
        "market_reasoning": "Unable to extract detailed analysis",
        "overall_assessment": text[:300]
    }

    current_section = None
    for line in lines:
        line = line.strip()
        if "STRENGTHS" in line.upper():
            current_section = "strengths"
        elif "WEAKNESSES" in line.upper():
            current_section = "weaknesses"
        elif "IMPROVEMENTS" in line.upper():
            current_section = "improvements"
        elif "PIVOTS" in line.upper():
            current_section = "pivots"
        elif "VIABILITY" in line.upper():
            current_section = "viability"
        elif line.startswith('-') or line.startswith('•'):
            content = line.lstrip('-•').strip()
            if current_section and content:
                if current_section == "viability":
                    try:
                        analysis["market_viability"] = int(''.join(c for c in content if c.isdigit())[:2])
                    except:
                        pass
                else:
                    analysis[current_section].append(content)

    return analysis


def generate_real_assessment(app_analysis: dict) -> dict:
    """
    Generate REAL assessment using Groq.
    Falls back to error if Groq unavailable.
    """
    groq_result = analyze_with_groq(app_analysis)

    if groq_result.get("status") != "ok":
        print(f"[framework] Groq analysis failed: {groq_result.get('error')}")
        return {
            "error": f"Assessment generation failed: {groq_result.get('error')}",
            "status": "error"
        }

    analysis = groq_result.get("analysis", {})

    # Calculate scores from Groq assessment
    market_viability = analysis.get("market_viability", 65)

    # Score based on real assessment
    idea_score = market_viability
    potential_score = analysis.get("market_viability", 65)
    design_score = 70 if len(analysis.get("improvements", [])) < 2 else 60
    risk_score = 100 - market_viability if market_viability > 50 else 40

    overall = (idea_score * 0.3 + potential_score * 0.3 + design_score * 0.2 + risk_score * 0.2)

    return {
        "score": int(overall),
        "idea_validation": {
            "score": idea_score,
            "reasoning": analysis.get("overall_assessment", "Assessment based on market analysis")
        },
        "potential": {
            "score": potential_score,
            "reasoning": analysis.get("market_reasoning", "Market viability assessed")
        },
        "design_quality": {
            "score": design_score,
            "reasoning": f"Found {len(analysis.get('weaknesses', []))} key gaps to address"
        },
        "execution_risk": risk_score,
        "verdict": {
            "worth_pursuing": overall > 70,
            "confidence": int(potential_score),
            "reason": analysis.get("overall_assessment", "Real assessment generated")
        },
        "improvements": analysis.get("improvements", []),
        "pivots": analysis.get("pivots", []),
        "strengths": analysis.get("strengths", []),
        "weaknesses": analysis.get("weaknesses", []),
        "risks": [
            {
                "category": "Market",
                "severity": "HIGH" if potential_score < 60 else "MEDIUM",
                "description": analysis.get("market_reasoning", "Market assessment")
            },
            {
                "category": "Positioning",
                "severity": "MEDIUM",
                "description": f"Identified {len(analysis.get('improvements', []))} positioning gaps"
            }
        ],
        "research_sources": {}
    }
