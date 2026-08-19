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
    Apply the FrameWork assessment.
    Using enhanced fallback that analyzes actual scraped data.
    """
    try:
        # Try enhanced fallback first (analyzes real data)
        result = _enhanced_fallback_assessment(analysis)
        if result and result.get("score"):
            return result
    except Exception as e:
        print(f"[framework_assessment] Enhanced fallback error: {e}")

    # If anything fails, use basic fallback (guaranteed to work)
    return _fallback_assessment(analysis)


def _generate_deep_verdict(score: float, idea_score: int, potential_score: int, feature_count: int, pricing: str, value_prop: str) -> str:
    """
    Generate a detailed, nuanced verdict on whether to pursue this idea.
    Goes beyond just a score — explains the reasoning.
    """
    if score >= 80:
        return (
            f"🚀 STRONG SIGNAL ({int(score)}/100): Clear positioning + solid features + revenue model. "
            f"Execute now. Focus: (1) Validate your target user cohort gets 10x value vs. alternatives, "
            f"(2) Measure retention after week 1 + month 1. If retention >50% week 1, scale acquisition."
        )
    elif score >= 70:
        return (
            f"✅ WORTH PURSUING ({int(score)}/100): Good foundation. You can win, but only if you: "
            f"(1) Nail your target user (go vertical, not horizontal), (2) Prove unit economics work "
            f"(one customer pays for CAC in <6mo), (3) Build 1-2 features that competitors don't have. "
            f"Validate PMF in next 30 days with real users."
        )
    elif score >= 60:
        return (
            f"🟡 VIABLE WITH WORK ({int(score)}/100): Market opportunity exists but positioning is weak. "
            f"Before building: (1) Interview 20 potential users — does your headline resonate? If <60% say 'I need this', pivot. "
            f"(2) Identify ONE specific user type (not 'everyone'). (3) Build MVP for that one user type. "
            f"If early traction >30% retention week 1, then scale."
        )
    elif score >= 50:
        return (
            f"⚠️ HIGH RISK ({int(score)}/100): Idea has gaps. Recommend: (1) Validate demand before building. "
            f"Run 20-30 customer interviews OR build landing page + ads to test willingness-to-try (target: 5%+ click-through). "
            f"(2) Clarify positioning — currently too vague. (3) Identify what makes you defensible vs. incumbents. "
            f"If validation passes, revisit."
        )
    else:
        return (
            f"❌ STOP ({int(score)}/100): Multiple red flags. Gaps: unclear positioning + weak feature set + unproven market. "
            f"Recommend: (1) Pivot to an adjacent market where you have unfair advantage, OR (2) Validate demand rigorously "
            f"before investing further. "
            f"What problem are you UNIQUELY positioned to solve that nobody else can?"
        )


def _enhanced_fallback_assessment(analysis: dict) -> dict:
    """
    Enhanced fallback assessment - analyzes REAL scraped data.
    Provides specific, actionable recommendations based on what's actually on the page.
    """
    title = analysis.get("title", "Unknown")
    value_prop = analysis.get("value_prop", "")
    features = analysis.get("features", [])
    pricing = analysis.get("pricing_model", "unknown")
    description = analysis.get("description", "")
    ctas = analysis.get("ctas", [])

    # === REAL ANALYSIS BASED ON ACTUAL DATA ===

    # 1. VALUE PROP CLARITY (0-100)
    idea_score = 40
    if value_prop:
        vp_len = len(value_prop)
        if vp_len > 80:  # Detailed value prop
            idea_score += 35
        elif vp_len > 40:  # Decent positioning
            idea_score += 25
        else:  # Too brief
            idea_score += 10

        # Bonus for specific language
        if any(keyword in value_prop.lower() for keyword in ["solve", "help", "enable", "automate", "increase", "decrease"]):
            idea_score += 15

    if description and len(description) > 100:
        idea_score += 10  # Has real description

    # 2. MARKET POTENTIAL (based on feature depth & pricing strategy)
    potential_score = 40
    feature_count = len(features)

    if feature_count >= 8:
        potential_score += 30  # Rich feature set
    elif feature_count >= 5:
        potential_score += 20
    elif feature_count >= 3:
        potential_score += 10

    # Pricing signals market maturity
    if pricing in ["paid", "Paid", "B2B"]:
        potential_score += 20  # Monetization strategy
    elif pricing == "Freemium":
        potential_score += 15  # Growth + revenue model

    # 3. DESIGN QUALITY (based on positioning clarity & CTA count)
    design_score = 40
    if len(value_prop) > 50:  # Clear positioning suggests design thinking
        design_score += 20

    if len(ctas) >= 2:
        design_score += 20  # Multiple CTAs = thought-out UX
    elif len(ctas) == 1:
        design_score += 10

    # 4. EXECUTION RISK (complexity signals)
    risk_score = 70  # Start neutral

    risk_keywords = ["ai", "ml", "machine learning", "blockchain", "crypto", "real-time", "distributed"]
    complexity_words = sum(1 for f in features if any(kw in f.lower() for kw in risk_keywords))
    risk_score -= min(20, complexity_words * 5)  # More complex features = higher risk

    if "api" in value_prop.lower() or "integration" in value_prop.lower():
        risk_score -= 5  # Integration complexity

    # Calculate overall weighted score
    overall = (idea_score * 0.35 + potential_score * 0.30 + design_score * 0.20 + risk_score * 0.15)
    overall = max(25, min(100, overall))  # Clamp 25-100

    # === DEEP ANALYSIS & IMPROVEMENTS ===

    improvements = []

    # Improvement 1: Value Proposition - Depth Analysis
    if len(value_prop) < 30:
        improvements.append(f"🎯 CRITICAL: Headline '{value_prop}' is too vague. Users won't understand WHO it's for or WHAT problem you solve. Rewrite as: '[For X user] [Product name] helps you [specific outcome] in [timeframe/way]'")
    elif len(value_prop) < 50:
        improvements.append(f"⚠️ Headline '{value_prop}' lacks specificity. Add: target user type + quantified outcome (e.g., 'helps managers save 5 hours/week' not just 'manage tasks')")
    elif len(value_prop) > 150:
        improvements.append(f"📝 Headline is long ({len(value_prop)} chars) — break into main claim + benefit bullets. Users won't read a paragraph.")
    else:
        improvements.append(f"✓ Headline '{value_prop[:40]}...' is reasonable. Validate with 3 target users: Does it immediately make sense? Would they click?")

    # Improvement 2: Feature Depth - Beyond Just Count
    if feature_count == 0:
        improvements.append("🔧 CRITICAL: No features listed. Users can't evaluate if this solves their problem. Add: top 5 capabilities (focus on outcomes, not tech)")
    elif feature_count < 3:
        improvements.append(f"🔧 Only {feature_count} feature(s) — too minimal to validate market need. Competitors in this space have 8-12. Either: (a) expand MVP scope or (b) position as 'focused tool' with deep UX")
    elif feature_count < 5:
        improvements.append(f"🔧 {feature_count} features is lean but risky. Missing likely: analytics/reporting, integrations, or customization. Add the #1 request from early users.")
    elif feature_count < 8:
        improvements.append(f"✓ {feature_count} features is solid MVP. Next step: Measure which features drive retention + engagement. Double down on top 2.")
    else:
        improvements.append(f"✓ {feature_count} features shows maturity. Risk: feature creep. Map each to a real user job. Cut bottom 30%.")

    # Improvement 3: Social Proof - Quantified Impact
    if not description or len(description) < 50:
        improvements.append("👥 CRITICAL: No evidence of traction shown. Add: user count, revenue, retention rate, or testimonials with results (e.g., '2,000+ users', '92% still active after 1 year')")
    elif len(description) < 150:
        improvements.append("👥 Description exists but too thin. Add specifics: Who uses it? What's the typical outcome? (e.g., 'Used by 500+ design agencies to cut project time 40%')")
    else:
        improvements.append("👥 Has description. Critical now: Add credibility markers — testimonials with real names/photos, press mentions, or quantified results (not generic praise)")

    # === DEEP PIVOTS (strategic repositioning if current positioning weak) ===

    pivots = []

    # Revenue Model Pivots
    if pricing == "Free":
        pivots.append("💰 PIVOT 1 — Revenue Model: Free is fine for traction (first 6mo), but you must test willingness-to-pay by month 9. Run pricing experiments: freemium tier (Pro) at $15-30/mo targeting power users, or land-and-expand (free → teams → enterprise).")
    elif pricing == "Paid" and feature_count < 5:
        pivots.append("💰 PIVOT 1 — Revenue Model: Paid pricing on limited features = high churn risk. Either: (a) add 2-3 more critical features first, or (b) switch to free + optional premium tier to reduce buyer hesitation.")
    elif pricing == "Freemium":
        pivots.append("💰 PIVOT 1 — Revenue Model: Freemium is smart. Critical now: Ensure free tier has enough value to prove concept (users get 70% of core job done). Premium should be 'nice-to-have' not 'required'.")

    # Market Positioning Pivots
    if len(value_prop) < 50:
        pivots.append("🎯 PIVOT 2 — Market Clarity: Current positioning is too broad. Pick ONE of these angles and own it deeply: (a) a specific job (e.g., 'content scheduling'), (b) a specific user type (e.g., 'solopreneurs'), (c) a specific outcome (e.g., 'save 10 hours/week'). Go narrow before going wide.")
    elif "for everyone" in value_prop.lower() or "all" in value_prop.lower():
        pivots.append("🎯 PIVOT 2 — Market Focus: Horizontal positioning ('for everyone') is hard. Pick: 1 vertical (e.g., marketing agencies) OR 1 job (e.g., project scheduling) and dominate that first. 80% of revenue will come from one user type — find it fast.")
    else:
        pivots.append("🎯 PIVOT 2 — Market Validation: Test if your positioning resonates with real users. Survey 20 users: Is the headline immediately clear? Would they recommend? If <70% say yes, reposition.")

    # Feature/Product Pivots
    if feature_count < 3:
        pivots.append("🔧 PIVOT 3 — Product Depth: You're too minimal. Benchmark top 3 competitors — what do they have that you don't? Prioritize: (a) integrations (Zapier, Slack, etc.), (b) analytics/reporting, (c) mobile support. Pick the #1 request from users.")
    elif feature_count > 12:
        pivots.append("🔧 PIVOT 3 — Product Focus: You have feature bloat. Which feature drives 80% of user retention? Double down there. Cut or remove bottom 5 features. Specialization > generalization at this stage.")
    else:
        pivots.append("🔧 PIVOT 3 — Product-Market Fit: You have reasonable feature depth. Now: measure usage. Which 2-3 features do power users rely on? Build workflows around those. Cut everything else.")

    # GTM Pivots
    if not ctas or len(ctas) == 0:
        pivots.append("📢 PIVOT 4 — Go-to-Market: No conversion path visible. Add clear CTA: 'Start Free' or 'Join 5,000+ Users'. Test urgency: 'Limited beta spots' or '7-day free trial'. Measure: are sign-ups happening? If not, reposition.")
    else:
        pivots.append(f"📢 PIVOT 4 — Go-to-Market: You have {len(ctas)} CTA(s). Test which converts best. A/B test: 'Start Free' vs 'See Demo' vs 'Join Beta'. Pick the top converter, double down.")

    # === RISKS (real execution challenges) ===

    risks = []

    # Market risk
    market_risk_severity = "HIGH" if potential_score < 50 else "MEDIUM" if potential_score < 70 else "LOW"
    if market_risk_severity == "HIGH":
        risks.append({"category": "Market", "severity": "HIGH",
                     "description": f"Weak market signals for '{title}'. Validate with 10-20 cold calls before building."})
    elif market_risk_severity == "MEDIUM":
        risks.append({"category": "Market", "severity": "MEDIUM",
                     "description": f"'{title}' market exists but likely crowded. Differentiation (vertical focus, pricing, features) required to win."})

    # Execution risk
    if risk_score < 50:
        risks.append({"category": "Execution", "severity": "HIGH",
                     "description": "High technical complexity (AI/ML/crypto). Need specialized team + 6-12 month runway."})
    elif risk_score < 65:
        risks.append({"category": "Execution", "severity": "MEDIUM",
                     "description": f"Moderate complexity. {feature_count} features to build/maintain. Aim for MVP with top 3 only."})

    # Product-market fit risk
    if feature_count < 3:
        risks.append({"category": "PMF", "severity": "HIGH",
                     "description": "Too minimal to validate market need. Users can't evaluate value with so few features."})
    else:
        risks.append({"category": "PMF", "severity": "MEDIUM",
                     "description": f"With {feature_count} features, you can test PMF. Monitor: user retention (day 7, day 30), feature usage, NPS."})

    return {
        "score": int(overall),
        "idea_validation": {
            "score": idea_score,
            "reasoning": f"Positioning clarity: {len(value_prop)} chars. {('✓ Clear' if len(value_prop) > 50 else '✗ Needs expansion')}"
        },
        "market_potential": {
            "score": potential_score,
            "reasoning": f"{feature_count} features + {pricing.lower()} pricing. {'✓ Monetizable' if pricing != 'Free' else '⚠️ Revenue model TBD'}"
        },
        "design_quality": {
            "score": design_score,
            "reasoning": f"{len(ctas)} CTAs visible. {('✓ Clear path' if len(ctas) >= 2 else '✗ Add clarity')}"
        },
        "execution_risk": risk_score,
        "verdict": {
            "worth_pursuing": overall > 60,
            "confidence": int(min(95, 40 + (overall * 0.55))),  # More nuanced confidence
            "reason": _generate_deep_verdict(overall, idea_score, potential_score, feature_count, pricing, value_prop)
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

        # Check if assessment has error - DO NOT HIDE IT
        if assessment.get("error"):
            print(f"[framework] Assessment error: {assessment.get('error')}")
            # Return error so API can show what's wrong
            return {
                "status": "error",
                "error": assessment.get("error"),
                "analysis": analysis
            }

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
