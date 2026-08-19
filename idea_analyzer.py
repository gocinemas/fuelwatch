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
    Fetch a URL and extract key information about the app — going DEEP.

    Extracts:
    - positioning (value prop, headline, all visible text)
    - features (from lists, cards, sections, bold text)
    - description (meta, og:description, first paragraphs)
    - design signals (CTAs, structure)
    - pricing_model
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

        # === EXTRACT TITLE ===
        title = soup.find('title')
        title_text = title.string if title else app_name or "Unknown"

        # === EXTRACT DESCRIPTION (multiple sources) ===
        description = ""
        og_description = soup.find('meta', property='og:description')
        if og_description:
            description = og_description.get('content', '')

        # Fallback: get first meaningful paragraph
        if not description or len(description) < 50:
            for tag in soup.find_all(['p', 'div']):
                text = tag.get_text(strip=True)
                if text and len(text) > 80 and len(text) < 500:
                    description = text
                    break

        # === EXTRACT VALUE PROP (headline) ===
        h1 = soup.find('h1')
        h1_text = h1.get_text(strip=True) if h1 else ""

        h2 = soup.find('h2')
        h2_text = h2.get_text(strip=True) if h2 else ""

        # === EXTRACT FEATURES (DEEP SCAN) ===
        features = []

        # Method 1: Lists (ul/ol)
        for section in soup.find_all(['ul', 'ol']):
            items = section.find_all('li')
            for item in items:
                text = item.get_text(strip=True)
                if text and len(text) > 8 and len(text) < 200:
                    features.append(text)

        # Method 2: Cards/divs with class patterns (feature-*, item, card, etc.)
        for div in soup.find_all('div'):
            classes = div.get('class', [])
            if any(pattern in str(classes).lower() for pattern in ['feature', 'item', 'card', 'module', 'capability', 'benefit']):
                text = div.get_text(strip=True)
                if text and 10 < len(text) < 200 and text not in features:
                    features.append(text)

        # Method 3: Bold/strong text (often used for feature names)
        for strong in soup.find_all(['strong', 'b']):
            text = strong.get_text(strip=True)
            if text and 5 < len(text) < 80 and text not in features:
                features.append(text)

        # Method 4: Section headings (h3, h4) that describe features
        for heading in soup.find_all(['h3', 'h4']):
            text = heading.get_text(strip=True)
            if text and len(text) > 8 and len(text) < 100 and text not in features:
                features.append(text)

        # Method 5: Look for "What you get" or similar sections
        for section in soup.find_all(['section', 'div']):
            section_text = section.get_text(strip=True).lower()
            if any(phrase in section_text for phrase in ['features', 'capabilities', 'modules', 'what you get', 'includes']):
                # Extract items in this section
                items = section.find_all(['li', 'div', 'span'])
                for item in items[:10]:
                    text = item.get_text(strip=True)
                    if 8 < len(text) < 150 and text not in features:
                        features.append(text)

        # Deduplicate and clean
        features = list(dict.fromkeys(features))  # Remove dupes, keep order
        features = [f for f in features if len(f) > 8 and f not in [h1_text, h2_text, title_text]]  # Remove noise
        features = features[:15]  # Top 15 features

        # === EXTRACT CTA BUTTONS ===
        ctas = []
        for btn in soup.find_all(['button', 'a']):
            classes = btn.get('class', [])
            if any(pattern in str(classes).lower() for pattern in ['btn', 'button', 'cta']):
                text = btn.get_text(strip=True)
                if text and 2 < len(text) < 50:
                    ctas.append(text)

        ctas = list(dict.fromkeys(ctas))[:5]  # Dedupe, top 5

        # === DETECT PRICING MODEL ===
        pricing_text = html.lower()
        pricing_model = "Free"
        if "pricing" in pricing_text or "pro" in pricing_text or "premium" in pricing_text:
            pricing_model = "Freemium"
        if "enterprise" in pricing_text or "contact sales" in pricing_text or "custom pricing" in pricing_text:
            pricing_model = "B2B"
        if "subscribe" in pricing_text or "$" in pricing_text:
            pricing_model = "Paid"

        return {
            "title": title_text,
            "description": description[:500] if description else "",  # First 500 chars
            "value_prop": h1_text,
            "subheading": h2_text,
            "ctas": ctas,
            "features": features,
            "num_features": len(features),
            "pricing_model": pricing_model,
            "url": url,
            "scraped_at": datetime.now().isoformat(),
            "html_length": len(html)
        }

    except Exception as e:
        print(f"[idea_analyzer] URL fetch error: {e}")
        return {"error": str(e), "url": url}


def generate_framework_assessment(analysis: dict) -> dict:
    """
    Simple, direct assessment:
    1. What is it?
    2. What's winning?
    3. What needs work?
    4. What's next?
    """
    return _simple_assessment(analysis)
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


def _get_product_category(title: str, value_prop: str, features: list) -> str:
    """
    Detect product category to determine GTM playbook.
    """
    combined_text = (title + " " + value_prop + " " + " ".join(features)).lower()

    if any(word in combined_text for word in ["saas", "software", "tool", "app", "platform", "management", "analytics", "crm", "erp"]):
        if any(word in combined_text for word in ["b2b", "business", "enterprise", "company", "team", "workflow"]):
            return "SaaS B2B"
        else:
            return "SaaS B2C"

    if any(word in combined_text for word in ["mobile", "ios", "android", "app store"]):
        return "Mobile App"

    if any(word in combined_text for word in ["marketplace", "platform", "buy", "sell", "trade", "service"]):
        return "Marketplace"

    if any(word in combined_text for word in ["ai", "chatbot", "automation", "agent"]):
        return "AI Tool"

    if any(word in combined_text for word in ["plugin", "extension", "api", "integration"]):
        return "Developer Tool"

    return "General SaaS"


def _get_gtm_playbook(product_category: str) -> dict:
    """
    Return 90-day GTM roadmap based on product type.
    """
    playbooks = {
        "SaaS B2B": {
            "month_1": [
                "Week 1-2: ProductHunt launch + HN post (target: 300-500 upvotes, 20-50 signups)",
                "Week 3: Guest post on industry blog (Substack, Medium) + LinkedIn outreach to 100 ICP profiles",
                "Week 4: Email founders + early customers asking 'what would make you use this?'"
            ],
            "month_2": [
                "Week 5-6: Land first 5 paying customers (directly reach out, offer free month)",
                "Week 7: Create case study from first customer",
                "Week 8: Cold email + LinkedIn to 500 SMB founders in target vertical"
            ],
            "month_3": [
                "Week 9-10: Optimize based on feedback, build 1 killer feature",
                "Week 11: Launch on G2 + Capterra for reviews",
                "Week 12: Analyze which channel got best customers (retention + LTV), 3x that channel"
            ],
            "winning_signal": "5+ paying customers, >50% week-1 retention, $500+ MRR"
        },
        "SaaS B2C": {
            "month_1": [
                "Week 1-2: ProductHunt launch (target: #1 Product of the Day)",
                "Week 3: TikTok/YouTube shorts showing before/after (if relevant)",
                "Week 4: Email outreach to 10 micro-influencers in space + Reddit communities"
            ],
            "month_2": [
                "Week 5-6: Paid ads test ($500 budget) on TikTok/Instagram targeting similar products",
                "Week 7: Optimize creative based on what converts",
                "Week 8: Viral content push (if applicable) or community building"
            ],
            "month_3": [
                "Week 9-10: Build referral program (give $5-10 credit per referral)",
                "Week 11: Launch on AppStore (if mobile) or launch landing page campaign",
                "Week 12: Measure: which channel has best LTV? Double down there"
            ],
            "winning_signal": "1000+ signups, 100+ paid, >30% day-1 retention, $1k+ MRR"
        },
        "Mobile App": {
            "month_1": [
                "Week 1-2: AppStore + PlayStore optimization (ASO), ProductHunt for web version",
                "Week 3: Reach out to 20 iOS/Android reviewers + tech blogs",
                "Week 4: TikTok/Instagram content showing app in action"
            ],
            "month_2": [
                "Week 5-6: Paid user acquisition test on Apple Search Ads ($500 budget)",
                "Week 7: Optimize based on CAC, increase if profitable",
                "Week 8: Build retention features (notifications, streaks, social)"
            ],
            "month_3": [
                "Week 9-10: Launch on Reddit + Discord communities for feedback",
                "Week 11: Build referral system for organic growth",
                "Week 12: Measure retention cohorts, optimize for day-30 retention >20%"
            ],
            "winning_signal": "10k+ downloads, 1k+ DAU, >20% day-30 retention"
        },
        "Marketplace": {
            "month_1": [
                "Week 1-2: Recruit 50 supply-side users (sellers/service providers) directly",
                "Week 3: ProductHunt launch highlighting unique value prop",
                "Week 4: Recruit demand-side users (buyers) via ads or organic"
            ],
            "month_2": [
                "Week 5-6: Drive first 20 transactions manually (help close deals)",
                "Week 7: Create case study from first transaction",
                "Week 8: Referral program launch (incentivize supply + demand)"
            ],
            "month_3": [
                "Week 9-10: Paid ads test for demand-side ($1k budget)",
                "Week 11: Organic supply-side growth (influencers, affiliate program)",
                "Week 12: Measure GMV, transaction volume, repeat rate"
            ],
            "winning_signal": "100+ supply + 500+ demand users, 20+ transactions, $1k+ GMV/week"
        },
        "AI Tool": {
            "month_1": [
                "Week 1-2: ProductHunt + HN launch (emphasize unique capability vs. ChatGPT)",
                "Week 3: Guest appearances on AI podcasts + newsletters (Neuron, Import AI)",
                "Week 4: Twitter/LinkedIn thought leadership (share learnings, not just selling)"
            ],
            "month_2": [
                "Week 5-6: Integrate into relevant Discord/Slack communities + offer free tier",
                "Week 7: Build partnerships with adjacent tools (Zapier, Make, etc.)",
                "Week 8: Create viral use case demo (Twitter thread, video)"
            ],
            "month_3": [
                "Week 9-10: Paid ads test targeting AI enthusiasts ($1k budget)",
                "Week 11: B2B outreach to companies using your use case",
                "Week 12: Build enterprise tier, reach out to 50 companies"
            ],
            "winning_signal": "5k+ users, 500+ paid, $5k+ MRR, 50%+ week-1 retention"
        },
        "General SaaS": {
            "month_1": [
                "Week 1-2: ProductHunt launch",
                "Week 3: HN + relevant subreddits",
                "Week 4: Email founders + target audience (100 emails)"
            ],
            "month_2": [
                "Week 5-6: Close first 5 customers",
                "Week 7: Create case study",
                "Week 8: Double down on best performing channel"
            ],
            "month_3": [
                "Week 9-10: Optimize product based on retention data",
                "Week 11: Launch on G2 + integrate feedback",
                "Week 12: Measure retention, CAC, LTV. Scale winning channel"
            ],
            "winning_signal": "5+ customers, >40% week-1 retention, $500+ MRR"
        }
    }

    return playbooks.get(product_category, playbooks["General SaaS"])


def _find_competitors(title: str, value_prop: str, features: list) -> list:
    """
    Identify likely competitors based on product description.
    (In real implementation, would query Crunchbase API + ProductHunt + Google)
    """
    # For now, return common competitors by category
    # In production: call Crunchbase + ProductHunt API + Google to find real competitors

    combined_text = (title + " " + value_prop).lower()

    if "productivity" in combined_text or "task" in combined_text or "todo" in combined_text:
        return ["Notion", "Asana", "Monday.com", "ClickUp", "Trello"]
    elif "finance" in combined_text or "expense" in combined_text or "budget" in combined_text:
        return ["Emma", "Snoop", "Plum", "Starling Bank", "YNAB"]
    elif "analytics" in combined_text or "dashboard" in combined_text:
        return ["Mixpanel", "Amplitude", "Segment", "Metabase", "Looker"]
    elif "communication" in combined_text or "chat" in combined_text:
        return ["Slack", "Discord", "Teams", "Telegram", "WhatsApp Business"]
    elif "design" in combined_text or "creative" in combined_text:
        return ["Figma", "Adobe XD", "Sketch", "Canva", "Penpot"]
    elif "hr" in combined_text or "recruiting" in combined_text:
        return ["Workday", "BambooHR", "Ashby", "Guidepoint", "Lever"]
    else:
        # Generic SaaS competitors
        return ["Zapier", "IFTTT", "n8n", "Make", "Integromat"]


def _score_defensibility(title: str, competitors: list, feature_count: int) -> dict:
    """
    Score how defensible your idea is against competitors.
    """
    score = 50
    reasoning = []

    # Competitor count impact
    if len(competitors) <= 2:
        score += 25
        reasoning.append("✅ Low competition: 1-2 major players. High defensibility.")
    elif len(competitors) <= 5:
        score += 15
        reasoning.append("🟡 Moderate competition: 3-5 players. Must differentiate.")
    elif len(competitors) <= 10:
        score += 5
        reasoning.append("⚠️ High competition: 5-10 players. Difficult to win.")
    else:
        score -= 10
        reasoning.append("🔴 Very high competition: 10+ players. Crowded market.")

    # Feature count vs. competitors
    if feature_count >= 8:
        score += 10
        reasoning.append("✅ Feature parity: 8+ features matches competitor depth.")
    elif feature_count >= 5:
        score += 0
        reasoning.append("🟡 Lean feature set: Competitors likely have more. Need differentiation.")
    else:
        score -= 15
        reasoning.append("🔴 Too minimal: Need more features to compete.")

    # Defensibility factors
    if "ai" in title.lower() or "ml" in title.lower():
        score -= 10
        reasoning.append("⚠️ AI/ML market: Hard to defend. Google/OpenAI will enter fast.")

    # Network effects check
    if any(word in title.lower() for word in ["social", "marketplace", "network", "community"]):
        score += 15
        reasoning.append("✅ Network effects potential: Can build defensibility through scale.")

    # Data moat
    if any(word in title.lower() for word in ["analytics", "data", "intelligence", "insights"]):
        score += 10
        reasoning.append("✅ Data moat possible: More users = better product = defensible.")

    score = max(20, min(100, score))

    return {
        "score": score,
        "level": "🟢 Strong" if score >= 70 else "🟡 Moderate" if score >= 50 else "🔴 Weak",
        "reasoning": reasoning
    }


def _simple_assessment(analysis: dict) -> dict:
    """
    Phase 1: Competitive Framework
    Answer: What is it? What's winning? What needs work? How do you win?
    """
    title = analysis.get("title", "Unknown App")
    value_prop = analysis.get("value_prop", "")
    features = analysis.get("features", [])
    description = analysis.get("description", "")
    ctas = analysis.get("ctas", [])
    pricing = analysis.get("pricing_model", "Free")

    # DETECT PRODUCT CATEGORY
    product_category = _get_product_category(title, value_prop, features)

    # FIND COMPETITORS
    competitors = _find_competitors(title, value_prop, features)

    # SCORE DEFENSIBILITY
    defensibility = _score_defensibility(title, competitors, len(features))

    # GET GTM PLAYBOOK
    gtm_playbook = _get_gtm_playbook(product_category)

    # 1. WHAT IS IT?
    what_is_it = f"Product: {title}\nCategory: {product_category}"
    if value_prop:
        what_is_it += f"\nMission: {value_prop}"
    if description:
        what_is_it += f"\nAbout: {description[:200]}..."

    # 2. WHAT'S WINNING? (identify strong points)
    winning = []

    if len(features) >= 5:
        winning.append(f"✅ Feature depth: {len(features)} modules/features built. Shows serious product")
    if value_prop and len(value_prop) > 40:
        winning.append(f"✅ Clear positioning: '{value_prop[:60]}...' is understandable")
    if len(ctas) >= 2:
        winning.append(f"✅ Active engagement: {len(ctas)} calls-to-action show users are converting")
    if pricing != "Free":
        winning.append(f"✅ Monetization: {pricing} model shows revenue thinking")
    if description and len(description) > 150:
        winning.append(f"✅ Market narrative: Rich description suggests real user understanding")

    if not winning:
        winning.append("⚠️ Need to assess: What makes this better than alternatives?")

    # 3. WHAT NEEDS IMPROVEMENT?
    improvements = []

    if len(value_prop) < 30:
        improvements.append(f"📝 Headline too vague ('{value_prop}'). Users don't immediately understand what this does.")

    if len(features) < 3:
        improvements.append(f"🔧 Only {len(features)} feature(s) listed. Either expand or better showcase what you have.")

    if len(ctas) == 0:
        improvements.append(f"📢 No clear call-to-action. Add 'Join', 'Try Now', or 'Learn More' button.")

    if not description or len(description) < 100:
        improvements.append(f"👥 Missing social proof. Add: user count, testimonials, or results achieved.")

    if len(features) > 12:
        improvements.append(f"🎯 Too many features ({len(features)}). Pick top 3-5 and promote those heavily.")

    if not improvements:
        improvements.append("✓ Foundation is solid. Next: measure what users actually value.")

    # 4. COMPETITIVE LANDSCAPE
    competitive_landscape = {
        "category": product_category,
        "competitors": competitors,
        "competitor_count": len(competitors),
        "defensibility": defensibility,
        "winning_factors": [
            f"🎯 Differentiation potential: Pick 1 area competitors don't own (e.g., better UX, lower price, vertical focus)",
            f"🛡️ Defensibility: {defensibility['level']} ({defensibility['score']}/100). " +
            (" Build moat through data/network effects" if defensibility['score'] >= 70 else
             " You can win with superior execution" if defensibility['score'] >= 50 else
             " Must differentiate aggressively or pick new idea")
        ]
    }

    # 5. GO-TO-MARKET ROADMAP
    gtm_roadmap = {
        "category": product_category,
        "winning_signal": gtm_playbook.get("winning_signal", ""),
        "month_1": gtm_playbook.get("month_1", []),
        "month_2": gtm_playbook.get("month_2", []),
        "month_3": gtm_playbook.get("month_3", []),
        "summary": f"90-day roadmap for {product_category} launching. Focus on: {gtm_playbook.get('winning_signal', '')}"
    }

    # 4. WHAT'S NEXT? (Updated with GTM focus)
    next_steps = []
    next_steps.append(f"🎯 Category: You're building a {product_category}")
    next_steps.append(f"🏆 Competitors: {', '.join(competitors[:3])} + {len(competitors)-3} others. Study them.")
    next_steps.append(f"🛡️ Defensibility: {defensibility['level']}. {defensibility['reasoning'][0]}")
    next_steps.append(f"📅 GTM: Follow the {product_category} playbook (see below)")
    next_steps.append(f"🚀 Win condition: {gtm_playbook.get('winning_signal', '')}")

    # Score (simple: 1-100 based on completeness + defensibility)
    score = 50  # Start middle
    if value_prop and len(value_prop) > 40:
        score += 15
    if len(features) >= 5:
        score += 15
    if description and len(description) > 100:
        score += 10
    if len(ctas) >= 2:
        score += 10
    if pricing != "Free":
        score += 10

    # Adjust for defensibility
    score += defensibility['score'] // 5  # Add defensibility to overall score

    return {
        "score": min(100, score),
        "what_is_it": what_is_it,
        "winning": winning,
        "needs_improvement": improvements,
        "competitive_landscape": competitive_landscape,
        "gtm_roadmap": gtm_roadmap,
        "next_steps": next_steps,
        "verdict": {
            "summary": f"{'🚀 Strong position' if score >= 75 else '🟡 Viable' if score >= 60 else '⚠️ Needs work'} ({score}/100)",
            "confidence": 85,
            "defensibility": defensibility['level'],
            "category": product_category
        },
        "idea_validation": {"score": (score // 2), "reasoning": "Positioning clarity"},
        "market_potential": {"score": defensibility['score'], "reasoning": f"Defensibility: {defensibility['level']}"},
        "design_quality": {"score": (score // 2), "reasoning": "UX signals"},
        "execution_risk": 100 - defensibility['score'],
        "improvements": [f"• {i}" for i in improvements],
        "pivots": next_steps,
        "risks": defensibility['reasoning']
    }


def _generate_deep_verdict(score: float, idea_score: int, potential_score: int, feature_count: int, pricing: str, value_prop: str, has_traction: bool = True) -> str:
    """
    Adaptive verdict: Different based on traction status.
    - With traction: Growth/optimization advice
    - No traction: Validation/positioning advice
    """
    if not has_traction:
        # EARLY STAGE / NEW IDEA MODE
        if score >= 75:
            return (
                f"🚀 STRONG IDEA ({int(score)}/100): Clear positioning + good features. "
                f"NEXT: Talk to 20 potential users — does your value prop resonate? "
                f"Build MVP, measure: 30%+ week-1 retention = signal to scale."
            )
        elif score >= 60:
            return (
                f"🟡 VIABLE ({int(score)}/100): Idea has legs but positioning needs clarity. "
                f"VALIDATE FIRST: (1) Interview 20 potential users about your headline. "
                f"(2) If <70% understand it immediately, rewrite. (3) Build MVP. (4) Measure retention."
            )
        elif score >= 50:
            return (
                f"⚠️ RISKY ({int(score)}/100): Multiple gaps (positioning + features). "
                f"BEFORE building: Run 30 customer interviews. Validate real demand. "
                f"If <5 people say 'I'd pay for this', pivot or kill."
            )
        else:
            return (
                f"❌ HOLD ({int(score)}/100): Needs significant rethinking. "
                f"Gaps: unclear positioning + weak feature set. What problem are YOU uniquely "
                f"positioned to solve? Rewrite positioning, then validate with users."
            )

    else:
        # EXISTING PRODUCT WITH TRACTION MODE
        if score >= 80:
            return (
                f"🚀 STRONG PRODUCT ({int(score)}/100): Clear positioning + multiple modules working + real users. "
                f"Next: (1) Measure what drives retention (which modules?), (2) Identify your power users + what they value most, "
                f"(3) Double down on that use case. Growth opportunity: expand the winning vertical/feature into enterprise/B2B."
            )
        elif score >= 70:
            return (
                f"✅ SOLID FOUNDATION ({int(score)}/100): Real traction exists. You have: positioning + features + users. "
                f"Strategic priorities: (1) Vertical focus — which user segment is most engaged? (2) Monetization — test willingness to pay, "
                f"(3) Network effects — can users help each other? Build community around your strongest module."
            )
        elif score >= 60:
            return (
                f"🟡 GOOD START ({int(score)}/100): Product has potential but unfocused. "
                f"Analysis: You're doing many things (which is good for learning) but diluting brand. Next steps: "
                f"(1) Measure engagement per module — which ONE drives the most value/retention? (2) Go deep on that. "
                f"(3) Position brand around the winning module. (4) Add monetization there. Other modules become secondary."
            )
        elif score >= 50:
            return (
                f"⚠️ NEEDS FOCUS ({int(score)}/100): Multiple modules but unclear which one matters most. "
                f"Risk: You're spread thin. Recommend: (1) Data audit — which feature/module has highest retention + DAU? "
                f"(2) Interview top 5 power users — why do they use it? What's irreplaceable? (3) Cut bottom 50% of features. "
                f"(4) Focus 90% of marketing + product on the winning use case."
            )
        else:
            return (
                f"🔴 CRITICAL: Product lacks clear value prop or traction. "
                f"Urgent: (1) Talk to users — what keeps them using it? What would make them leave? "
                f"(2) Audit metrics: which module/feature has highest engagement? (3) Kill everything else. "
                f"(4) Rebrand around the winning insight. Right now you're invisible because you do everything poorly instead of one thing well."
            )


def _detect_traction(analysis: dict) -> dict:
    """
    Detect if product has traction signals.
    Returns: {has_traction: bool, signals: [list], confidence: 0-100}
    """
    signals = []
    traction_score = 0

    description = (analysis.get("description", "") + analysis.get("value_prop", "")).lower()
    full_text = description

    # Signal 1: User/customer count
    import re
    user_patterns = [
        r'(\d+[kK])\+?\s*(users|customers|companies|teams)',
        r'(\d+)\+?\s*(million|thousand|hundred)\s*(users|customers)',
        r'over\s*(\d+[kK])\s*(users|people)',
    ]
    for pattern in user_patterns:
        if re.search(pattern, full_text):
            signals.append("User count mentioned")
            traction_score += 25
            break

    # Signal 2: Growth/retention metrics
    if any(metric in full_text for metric in ["%", "retention", "growth", "active", "engagement", "dau", "mau"]):
        signals.append("Engagement metrics visible")
        traction_score += 15

    # Signal 3: Testimonials/social proof
    if any(phrase in full_text for phrase in ["customer says", "testimonial", "users love", "rated", "review", "feedback", "★", "5 star", "recommended"]):
        signals.append("Social proof/testimonials")
        traction_score += 20

    # Signal 4: Press/media mentions
    if any(phrase in full_text for phrase in ["featured in", "press", "media", "news", "podcast", "article", "publication", "mention", "times", "forbes", "techcrunch"]):
        signals.append("Press mentions")
        traction_score += 20

    # Signal 5: Customer logos/case studies
    if any(phrase in full_text for phrase in ["case study", "customer story", "industry leaders", "trusted by", "used by", "powered by"]):
        signals.append("Case studies/logos")
        traction_score += 15

    # Signal 6: Revenue/paid customers
    if any(phrase in full_text for phrase in ["revenue", "profitable", "paid", "subscription", "customers pay", "annual revenue"]):
        signals.append("Revenue signal")
        traction_score += 25

    # Signal 7: Funding/investment
    if any(phrase in full_text for phrase in ["funded", "investment", "seed", "series", "raised", "venture"]):
        signals.append("Funding/investment")
        traction_score += 20

    # Signal 8: Launch/anniversary
    if any(phrase in full_text for phrase in ["since", "founded", "launched", "year", "anniversary", "2020", "2021", "2022", "2023", "2024", "2025", "2026"]):
        signals.append("Established timeframe")
        traction_score += 5

    has_traction = traction_score >= 30 or len(signals) >= 2

    return {
        "has_traction": has_traction,
        "signals": signals,
        "score": min(100, traction_score),
        "confidence": len(signals) * 20  # 20 points per signal
    }


def _enhanced_fallback_assessment(analysis: dict) -> dict:
    """
    Enhanced fallback assessment - adapts based on traction.

    If product has traction → Growth/optimization advice
    If new idea → Validation/positioning advice
    """
    title = analysis.get("title", "Unknown")
    value_prop = analysis.get("value_prop", "")
    features = analysis.get("features", [])
    pricing = analysis.get("pricing_model", "unknown")
    description = analysis.get("description", "")
    ctas = analysis.get("ctas", [])

    # Detect traction to choose analysis mode
    traction = _detect_traction(analysis)
    has_traction = traction["has_traction"]

    # Store traction data in analysis for later use
    analysis["_traction"] = traction
    analysis["_has_traction"] = has_traction

    # === ADAPTIVE ANALYSIS: TRACTION vs. NEW IDEA ===

    if has_traction:
        # MODE: EXISTING PRODUCT WITH USERS
        # Focus: positioning clarity + feature depth + growth opportunities
        analysis_mode = "product"
    else:
        # MODE: NEW IDEA / EARLY STAGE
        # Focus: market fit + positioning clarity + feature completeness
        analysis_mode = "idea"

    # 1. POSITIONING CLARITY (how well does the headline explain the product?)
    idea_score = 50 if has_traction else 40  # Start higher for products with traction
    if value_prop:
        vp_len = len(value_prop)
        # Clear positioning signals
        if vp_len > 60:  # Detailed, specific
            idea_score += 30
        elif vp_len > 30:  # Reasonable
            idea_score += 15
        else:
            idea_score += 5  # Too brief but might have sub-heading

        # Clarity keywords
        if any(kw in value_prop.lower() for kw in ["help", "for", "your", "everyday", "assistant", "save", "manage"]):
            idea_score += 10

    if description and len(description) > 200:
        idea_score += 5  # Rich description = mature product

    # 2. PRODUCT DEPTH (breadth of capabilities)
    potential_score = 50  # Start middle
    feature_count = len(features)

    if feature_count >= 10:
        potential_score += 30  # Multiple modules/features
    elif feature_count >= 6:
        potential_score += 20
    elif feature_count >= 3:
        potential_score += 10

    # Monetization signal (existing products should show revenue model)
    if pricing in ["Paid", "B2B"]:
        potential_score += 15  # Clear paid model
    elif pricing == "Freemium":
        potential_score += 10  # Freemium (growth + future revenue)
    # Free is OK for existing products (bootstrap/early stage)

    # 3. MARKET SIGNAL (CTA presence = user traction/engagement)
    design_score = 50
    if len(ctas) >= 2:
        design_score += 25  # Multiple CTAs = active userbase
    elif len(ctas) == 1:
        design_score += 15

    if len(value_prop) > 40:
        design_score += 10  # Clear UX

    # 4. EXECUTION MATURITY (complexity + feature breadth signal)
    risk_score = 70  # Start neutral

    # Complex features are OK for existing products (they're already built)
    # Risk is about maintenance/scaling, not development risk
    complexity_keywords = ["ai", "ml", "real-time", "integration", "api"]
    has_complexity = sum(1 for f in features if any(kw in f.lower() for kw in complexity_keywords))

    if has_complexity > 3:
        risk_score += 15  # Sophisticated product = lower risk (already de-risked)
    elif has_complexity > 0:
        risk_score += 5

    # Multiple features signal good execution
    if feature_count >= 8:
        risk_score += 10  # Teams that ship many features are competent

    # Calculate overall: existing product (higher baseline, no harsh penalties)
    overall = (idea_score * 0.30 + potential_score * 0.35 + design_score * 0.20 + risk_score * 0.15)
    overall = max(40, min(100, overall))  # Clamp 40-100 for existing products

    # === IMPROVEMENTS FOR EXISTING PRODUCTS ===
    # Focus: what's working, what to optimize, growth opportunities

    improvements = []

    # Insight 1: Positioning Clarity
    if len(value_prop) < 30:
        improvements.append(f"📝 Headline is vague ('{value_prop}'). For existing products, clarity matters. Rewrite to be specific: 'Your everyday [job/outcome]' or 'AI assistant for [specific user]'")
    elif len(value_prop) > 100:
        improvements.append(f"📝 Headline is wordy ({len(value_prop)} chars). Shorten to <60 chars. Users decide in 2 seconds. Test on your most engaged users: does it resonate?")
    else:
        improvements.append(f"✓ Positioning is clear enough ('{value_prop[:50]}...'). Next: A/B test against benefit-focused alternative. Which converts better?")

    # Insight 2: Feature/Module Analysis
    if feature_count >= 8:
        improvements.append(f"🎯 {feature_count} modules is strong (means real product depth). OPPORTUNITY: Identify which 1-2 modules drive 80% of retention. Go deeper there. Make those modules industry-leading, cut or simplify the rest.")
    elif feature_count >= 5:
        improvements.append(f"✓ {feature_count} features is solid. NEXT: Audit usage metrics per feature. Which one has highest DAU + retention? Make that your flagship. Build brand around it.")
    else:
        improvements.append(f"⚠️ {feature_count} features might be limiting. Are you underselling the product? Check: are there capabilities not listed? If yes, add them to homepage. If no, consider expanding product or going deeper on core features.")

    # Insight 3: Market Traction Signals
    if len(ctas) >= 2:
        improvements.append(f"✓ {len(ctas)} CTAs show product engagement. OPTIMIZATION: Test which CTA converts best (join vs. demo vs. learn). Double down on winner. Measure conversion rate per CTA.")
    elif len(ctas) == 1:
        improvements.append(f"💡 Single CTA. OPPORTUNITY: Add secondary action (e.g., 'See Demo' alongside 'Join'). Measure: do people prefer trying first or joining blind? Build roadmap around top choice.")

    # Insight 4: Social Proof & Credibility
    if description and len(description) > 200:
        improvements.append(f"✓ Description is rich. NEXT: Add quantified traction (users, companies, retention, NPS). E.g., '10k+ daily active users' or '92% retention after 1 month'. This is what converts.")
    elif description:
        improvements.append(f"💡 Description exists but thin. ADD: Proof points. What metric best shows product value? (DAU, retention %, testimonial NPS, time saved, $$ saved per user?). Lead with that.")
    else:
        improvements.append(f"🔴 No description/social proof visible. CRITICAL for growth: Add 1-2 quantified wins (user count, engagement stat, customer testimonial with metric). This is how users evaluate.")

    # === GROWTH OPPORTUNITIES FOR EXISTING PRODUCTS ===

    pivots = []

    # Growth Opportunity 1: Revenue/Monetization
    if pricing == "Free":
        pivots.append("💰 GROWTH 1 — Monetization: Product is free. Path forward: (1) Identify your power users (10% most engaged). (2) Test premium tier targeting them ($10-30/mo). (3) What would they pay for? Ask directly. (4) Build that feature, charge for it.")
    elif pricing == "Paid":
        pivots.append("💰 GROWTH 1 — Expansion Revenue: You're charging. Opportunity: (1) Audit power users — do they share/invite? (2) Add team/enterprise pricing (5-10x base). (3) Test: 'Teams' tier with admin controls. (4) Measure: % of users who go premium. Target: >5%.")
    else:
        pivots.append("💰 GROWTH 1 — Revenue Model: Current model is freemium. Test: (1) Which features do paying users use most? (2) Build premium tier around those. (3) Premium should feel 'premium' — not just ad-free, but genuinely better. (4) Measure LTV:CAC ratio.")

    # Growth Opportunity 2: Market/Vertical Expansion
    if feature_count >= 8:
        pivots.append("🎯 GROWTH 2 — Vertical Dominance: With multiple modules, you can own a vertical. Which user segment (job/company/geography) gets the MOST value? Pick that segment, own it. Build case studies, testimonials, community. Then expand to adjacent verticals.")
    else:
        pivots.append("🎯 GROWTH 2 — Market Expansion: Build one capability until it's best-in-class. Then expand. Current strategy (many features) is smart for learning but hard to market. Pick the feature that excites you most. Make it legendary.")

    # Growth Opportunity 3: Network Effects / Community
    if len(ctas) >= 2:
        pivots.append("👥 GROWTH 3 — Community: You have active users (multiple CTAs). Opportunity: Build social layer. Can users connect? Share tips? Build community around your core use case. This creates lock-in + viral growth.")
    else:
        pivots.append("👥 GROWTH 3 — Engagement: Low CTA presence suggests engagement potential. What keeps users coming back? Build features around that. Social (share results), gamification (streaks), or community (ask + answer).")

    # Growth Opportunity 4: Enterprise / B2B
    if pricing != "B2B":
        pivots.append("🏢 GROWTH 4 — Enterprise Expansion: Consumer product working? Consider B2B angle. Which company type would benefit most? (e.g., if consumer app, would teams/companies pay more?). Build enterprise features: admin panel, team management, reporting, SSO.")
    else:
        pivots.append("🏢 GROWTH 4 — Scale B2B: You're B2B-focused. Next: (1) Build partnerships with resellers/consultants. (2) Create certifications or training programs. (3) Partner with adjacent B2B products. (4) Measure: CAC, LTV, expansion revenue per account.")

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
            "worth_pursuing": overall > (60 if has_traction else 65),
            "confidence": int(min(95, 40 + (overall * 0.55))),
            "reason": _generate_deep_verdict(overall, idea_score, potential_score, feature_count, pricing, value_prop, has_traction),
            "analysis_mode": "EXISTING PRODUCT" if has_traction else "NEW IDEA",
            "traction_signals": traction.get("signals", [])
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
