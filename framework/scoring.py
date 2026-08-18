"""
FrameWork Phase 3: Multi-dimensional Scoring
Score app idea across 4 dimensions + generate verdict.

Dimensions:
1. Idea Validation (problem real? demand?)
2. Market Potential (TAM, timing, growth)
3. Design Quality (execution quality vs competitors)
4. Execution Risk (tech difficulty, distribution, regulatory)
"""


def score_idea_validation(problem_validation: dict) -> int:
    """
    Score 1: Idea Validation (20-100)

    Based on Reddit analysis:
    - How many mentions? (demand signal)
    - Sentiment? (is problem frustrating or not?)
    - Unique angle? (vs existing discussions)

    Formula:
    - Mentions: 30 pts (at 5+)
    - Positive sentiment: 20 pts
    - Problem clarity: 25 pts
    - Unique positioning: 25 pts
    """
    try:
        mentions = problem_validation.get("threads_analyzed", 0)
        sentiment = problem_validation.get("average_sentiment", 0)
        validation_score = problem_validation.get("validation_score", 50)

        # Base: validation_score from Reddit analysis
        score = validation_score

        # Boost for high engagement
        if mentions >= 5:
            score = min(100, score + 20)

        # Boost for positive sentiment
        if sentiment > 0.3:
            score = min(100, score + 15)

        return int(score)

    except Exception as e:
        print(f"[scoring] Idea validation error: {e}")
        return 60


def score_market_potential(competition_analysis: dict, app_analysis: dict) -> int:
    """
    Score 2: Market Potential (20-100)

    Based on:
    - TAM estimation (from competitor funding, search volume)
    - Market timing (growing vs mature vs declining)
    - Competitive saturation (how crowded?)

    Formula:
    - Low saturation: 40 pts
    - Growing market: 30 pts
    - High willingness to pay: 30 pts
    """
    try:
        saturation_score = competition_analysis.get("competition_analysis", {}).get("saturation_score", 50)

        # Inverse saturation (low saturation = high potential)
        market_openness = 100 - saturation_score

        # Check if pricing model is premium (higher margin)
        pricing = app_analysis.get("pricing_model", "unknown")
        willingness_to_pay = 60  # Assume average
        if pricing == "paid":
            willingness_to_pay = 80
        elif pricing == "free":
            willingness_to_pay = 40

        # Weighted score
        score = (market_openness * 0.5) + (willingness_to_pay * 0.5)

        return int(score)

    except Exception as e:
        print(f"[scoring] Market potential error: {e}")
        return 60


def score_design_quality(app_analysis: dict, competition_analysis: dict) -> int:
    """
    Score 3: Design Quality (20-100)

    Based on:
    - UI/UX quality (modern, professional?)
    - Feature parity (vs competitors)
    - Unique visual identity?

    Formula:
    - Matches best practices: 40 pts
    - Clean/professional design: 30 pts
    - Unique identity: 30 pts
    """
    try:
        # Extract app's design score (from vision analysis)
        app_design_score = app_analysis.get("design_quality_score", 65)

        # Compare vs competitors
        feature_gaps = len(competition_analysis.get("competition_analysis", {}).get("positioning_gaps", []))
        feature_parity = max(0, 80 - feature_gaps * 10)  # Lose points for each missing feature

        # Weighted
        score = (app_design_score * 0.6) + (feature_parity * 0.4)

        return int(score)

    except Exception as e:
        print(f"[scoring] Design quality error: {e}")
        return 65


def score_execution_risk(app_analysis: dict) -> int:
    """
    Score 4: Execution Risk (20-100, INVERTED scoring)

    Lower score = higher risk. Higher score = lower risk.

    Based on:
    - Technical complexity (real-time? machine learning? blockchain?)
    - Distribution difficulty (B2C crowded? B2B needs sales?)
    - Regulatory risk (fintech, healthcare, crypto?)

    Scoring:
    - Simple tech: +30 pts
    - Clear distribution: +30 pts
    - No regulatory issues: +40 pts
    """
    try:
        risk_score = 50  # Base

        # Check for technical red flags
        features = " ".join([f.lower() for f in app_analysis.get("features", [])])
        title = (app_analysis.get("title") or "").lower()
        value_prop = (app_analysis.get("value_prop") or "").lower()
        combined = title + " " + value_prop + " " + features

        # Tech complexity heuristics
        if any(word in combined for word in ["real-time", "blockchain", "ai", "ml", "machine learning"]):
            risk_score -= 15  # High complexity
        else:
            risk_score += 15  # Simple tech

        # Distribution difficulty
        if any(word in combined for word in ["b2b", "enterprise", "saas"]):
            risk_score -= 10  # Needs sales/enterprise
        elif any(word in combined for word in ["consumer", "b2c"]):
            risk_score -= 5  # Crowded but possible

        # Regulatory risk
        if any(word in combined for word in ["crypto", "fintech", "healthcare", "medical", "banking"]):
            risk_score -= 20  # High regulatory burden
        else:
            risk_score += 10  # Low regulatory risk

        return max(0, min(100, risk_score))

    except Exception as e:
        print(f"[scoring] Execution risk error: {e}")
        return 50


def calculate_overall_score(
    idea_score: int,
    potential_score: int,
    design_score: int,
    risk_score: int,
) -> dict:
    """
    Weighted overall score.

    Weights:
    - Idea Validation: 30%
    - Market Potential: 30%
    - Design Quality: 20%
    - Execution Risk: 20%
    """
    overall = (
        idea_score * 0.30 +
        potential_score * 0.30 +
        design_score * 0.20 +
        risk_score * 0.20
    )

    # Determine verdict
    if overall >= 75:
        verdict = "✅ WORTH PURSUING"
        confidence = min(95, overall)
        reason = "Strong signals across dimensions. High confidence this is worth building."
    elif overall >= 60:
        verdict = "⚠️ PROCEED WITH CAUTION"
        confidence = max(50, overall - 10)
        reason = "Decent potential but some risks. Validate with real users before building."
    else:
        verdict = "🔄 NEEDS PIVOT"
        confidence = max(30, 100 - overall)
        reason = "Current concept needs significant changes. Consider pivoting before launch."

    return {
        "overall_score": int(overall),
        "idea_validation": idea_score,
        "market_potential": potential_score,
        "design_quality": design_score,
        "execution_risk": risk_score,
        "verdict": verdict,
        "confidence": int(confidence),
        "reason": reason,
    }


def generate_improvements(
    app_analysis: dict,
    problem_validation: dict,
    competition_analysis: dict,
    scores: dict,
) -> list:
    """
    Generate top 3 improvements based on analysis.

    Sources:
    - Competitor feature gaps
    - Design benchmarking
    - Market feedback (Reddit sentiment)
    """
    improvements = []

    try:
        # Improvement 1: Feature gaps
        gaps = competition_analysis.get("competition_analysis", {}).get("positioning_gaps", [])
        if gaps:
            improvements.append(
                f"💡 Add missing feature: '{gaps[0]}' (identified in competitor analysis)"
            )

        # Improvement 2: Design/positioning clarity
        if scores.get("design_score", 0) < 70:
            improvements.append(
                "💡 Strengthen visual design and brand identity to differentiate vs competitors"
            )
        else:
            improvements.append(
                "💡 Clarify your unique value proposition in the headline (current positioning is generic)"
            )

        # Improvement 3: Based on market feedback
        threads = problem_validation.get("problem_validation", {}).get("threads", [])
        if threads:
            top_thread = threads[0]
            improvements.append(
                f"💡 Address user pain point mentioned in community: 'Better support for {top_thread.get('title', 'workflow')[:30]}...'"
            )
        else:
            improvements.append(
                "💡 Add social proof (testimonials, case studies) to build credibility"
            )

        return improvements[:3]

    except Exception as e:
        print(f"[improvements] Error: {e}")
        return [
            "💡 Clarify your unique value proposition",
            "💡 Add social proof and testimonials",
            "💡 Emphasize differentiation vs competitors",
        ]


def generate_pivots(
    app_analysis: dict,
    competition_analysis: dict,
    scores: dict,
) -> list:
    """
    Suggest pivots based on risk assessment + market gaps.
    """
    pivots = []

    try:
        execution_risk = scores.get("execution_risk", 0)
        potential = scores.get("market_potential", 0)

        # Pivot 1: High risk suggests a different distribution model
        if execution_risk < 40:
            pivots.append("🎯 Consider B2B enterprise model (higher margin, slower but more defensible)")

        # Pivot 2: Saturation suggests vertical focus
        saturation = competition_analysis.get("competition_analysis", {}).get("saturation_score", 50)
        if saturation > 70:
            pivots.append("🎯 Focus on a vertical (design teams, writers, startups) vs horizontal")

        # Pivot 3: Low potential suggests adjacent market
        if potential < 60:
            pivots.append("🎯 Expand to adjacent use case with higher demand (check Reddit for user suggestions)")

        return pivots[:3]

    except Exception as e:
        print(f"[pivots] Error: {e}")
        return [
            "🎯 Consider B2B vs B2C distribution",
            "🎯 Focus on a specific vertical or use case",
            "🎯 Build data/intelligence layer for defensibility",
        ]


def generate_risk_assessment(scores: dict) -> dict:
    """
    Identify main risks to execution.
    """
    risks = []

    try:
        # Technical risk
        if scores.get("execution_risk", 0) < 50:
            risks.append({
                "category": "Execution",
                "severity": "HIGH",
                "description": "Technical complexity or distribution difficulty. May require specialized team.",
            })

        # Market risk
        if scores.get("market_potential", 0) < 60:
            risks.append({
                "category": "Market",
                "severity": "HIGH",
                "description": "Saturated or declining market. Limited TAM or slow adoption.",
            })

        # Design risk
        if scores.get("design_score", 0) < 65:
            risks.append({
                "category": "Product",
                "severity": "MEDIUM",
                "description": "Design/UX needs improvement vs competitors. User experience may be barrier.",
            })

        return {"risks": risks[:3]}

    except Exception as e:
        print(f"[risk_assessment] Error: {e}")
        return {"risks": []}
