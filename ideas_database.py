"""
Ideas Database - Curated app/business ideas with competitive analysis + GTM playbooks.

Seed data: 50+ ideas with:
- Market size + TAM
- Competitive landscape
- Defensibility score
- GTM playbook
- Viability verdict
"""

# Curated Ideas Seed Data
IDEAS_SEED = [
    # === PRODUCTIVITY & AI ===
    {
        "id": "idea_001",
        "name": "AI Code Review Assistant",
        "description": "WhatsApp-native AI that reviews code PRs, suggests improvements, catches bugs",
        "category": "AI Tool",
        "market_size": "$500M-1B",
        "tam": "$200M-500M",
        "target_users": "Developers, Engineering teams",
        "competitors": ["GitHub Copilot", "CodeRabbit", "Replit", "Tabnine"],
        "defensibility_score": 45,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "GitHub has 100M users, Copilot is free/bundled. Need unique angle (vertical focus or cost)",
        "key_differentiator": "WhatsApp-native (no Slack install), targets emerging markets (cheaper)",
        "gtm_playbook": "Hacker News + Dev communities (Reddit r/webdev) + Twitter dev influencers",
        "winning_signal": "1k+ developer users, 100+ paid teams, $2k+/MRR in 90 days",
        "verdict": "🟡 Viable with focus",
        "verdict_reason": "GitHub dominates but niche opportunity exists (WhatsApp for devs in India/Latin America)",
        "estimated_tam": "Emerging markets developers (5M+ in India alone)",
        "moat_potential": "Community of developers sharing best practices + AI training on real code",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-20"
    },
    {
        "id": "idea_002",
        "name": "Expense Report Automation (Freelancers)",
        "description": "Snap receipt → auto-categorize + create expense report + export for tax",
        "category": "SaaS B2B",
        "market_size": "$500M-1B",
        "tam": "$50M-200M",
        "target_users": "Freelancers, Solo consultants, 1099 contractors",
        "competitors": ["Expensify", "Soldo", "Wave", "Zoho Expense", "Receipt Bank"],
        "defensibility_score": 55,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Crowded market. Need vertical focus (UK freelancers? Contractors?) + better UX",
        "key_differentiator": "Mobile-first + AI categorization + direct tax export (vs. manual entry)",
        "gtm_playbook": "ProductHunt + Reddit r/freelance + Twitter + Slack communities for freelancers",
        "winning_signal": "5k+ users, 500+ paid, $5k+/MRR in 90 days",
        "verdict": "🟡 Viable with vertical focus",
        "verdict_reason": "Huge market but crowded. Win by owning one vertical (e.g., UK contractors, fitness coaches)",
        "estimated_tam": "UK freelancers: 3M+ (£1B/year in expense tracking)",
        "moat_potential": "Integration with tax software + accounting APIs",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-20"
    },
    {
        "id": "idea_003",
        "name": "School Parent Communication Hub",
        "description": "WhatsApp-native platform for schools to send alerts + permission slips + emergency comms to parents",
        "category": "SaaS B2B",
        "market_size": "$1B-5B",
        "tam": "$200M-500M",
        "target_users": "Primary schools, K-12 administrators",
        "competitors": ["ClassDojo", "MySchoolUpdate", "SchoolPing", "Bloomz", "Remind"],
        "defensibility_score": 75,
        "defensibility_level": "🟢 Strong",
        "defensibility_reason": "WhatsApp-native is unique moat. Parents already on WhatsApp. No app install friction.",
        "key_differentiator": "Parents get alerts in WhatsApp (not app). Schools avoid SMS costs.",
        "gtm_playbook": "Partner with 5-10 schools directly (free trial) → Case studies → Regional expansion → Enterprise features",
        "winning_signal": "20+ schools, 5k+ parents using, £500+/MRR in 90 days",
        "verdict": "🚀 STRONG opportunity",
        "verdict_reason": "Clear pain (fragmented comms) + huge TAM (100k schools UK) + WhatsApp moat. Defensible.",
        "estimated_tam": "UK schools: 24k primary + 3.7k secondary = 27.7k × £500/year average = £13.8B TAM",
        "moat_potential": "WhatsApp exclusivity + parent/school lock-in + network effects",
        "risk_level": "LOW",
        "created_at": "2026-08-20"
    },
    {
        "id": "idea_004",
        "name": "AI Personal Trainer (Video + Form Correction)",
        "description": "Phone camera detects exercise form in real-time, gives corrections + workout plans",
        "category": "Mobile App",
        "market_size": "$5B-10B",
        "tam": "$1B-2B",
        "target_users": "Fitness enthusiasts, Home gym users, Crossfit athletes",
        "competitors": ["Apple Fitness+", "Peloton", "Fitbod", "Strong", "Hevy"],
        "defensibility_score": 50,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "AI/ML is easy to copy. Apple + Amazon entering market. Differentiation via community or vertical focus.",
        "key_differentiator": "Offline-first (runs on device), video form correction (vs. just counting reps), free tier",
        "gtm_playbook": "TikTok fitness creators → Reddit r/fitness → YouTube demo videos → Influencer partnerships",
        "winning_signal": "50k+ downloads, 10k+ paid users, $20k+/MRR in 90 days",
        "verdict": "🟡 Viable but crowded",
        "verdict_reason": "Huge TAM but intense competition (Apple, Peloton, Fitbod). Win by owning one niche (e.g., CrossFit, weightlifting form)",
        "estimated_tam": "Home fitness market: 50M people globally × £10/mo avg = £6B TAM",
        "moat_potential": "Community (challenge competitions), vertical expertise (CrossFit form + workouts)",
        "risk_level": "MEDIUM-HIGH",
        "created_at": "2026-08-20"
    },
    {
        "id": "idea_005",
        "name": "Freelance Financial Dashboard",
        "description": "See all income streams (Upwork, Fiverr, Stripe) + profit margins + tax estimation in one dashboard",
        "category": "SaaS B2C",
        "market_size": "$200M-500M",
        "tam": "$50M-150M",
        "target_users": "Freelancers with multiple income streams, Solo entrepreneurs",
        "competitors": ["Stripe Dashboard", "Wave", "Zoho Books", "QuickBooks Self-Employed", "Lunar"],
        "defensibility_score": 60,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Data aggregation is sticky but Stripe/Wave already do this. Differentiate via UX or features.",
        "key_differentiator": "Beautiful dashboard + profit margin tracking + tax forecasting + payment reminders",
        "gtm_playbook": "ProductHunt + Twitter + Indie Hackers + Reddit r/freelance + Email to Upwork communities",
        "winning_signal": "5k+ users, 500+ paid, $3k+/MRR in 90 days",
        "verdict": "🟡 Viable",
        "verdict_reason": "Clear problem (fragmented income visibility). Crowded but UX focus can differentiate.",
        "estimated_tam": "Freelancers globally: 50M+ × £5-10/mo = £2.5B-5B TAM",
        "moat_potential": "Data integrations (Upwork, Stripe, PayPal) + community features (compare earnings)",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-20"
    },

    # === LOCAL INTELLIGENCE ===
    {
        "id": "idea_006",
        "name": "Hyper-Local Food Delivery for Small Towns",
        "description": "Uber Eats for tier-2 cities (UK towns with <100k people) where Uber doesn't operate",
        "category": "Marketplace",
        "market_size": "$100M-500M",
        "tam": "$50M-200M",
        "target_users": "Small town residents, Local restaurants",
        "competitors": ["JustEat", "Deliveroo", "Uber Eats", "Local independent services"],
        "defensibility_score": 70,
        "defensibility_level": "🟢 Strong",
        "defensibility_reason": "Tier-2 cities underserved by big players. Local network effects + local restaurant relationships = moat.",
        "key_differentiator": "Only platform in small towns + support for cash payments + WhatsApp integration for restaurants",
        "gtm_playbook": "Partner with 20-30 restaurants directly → Local advertising (Facebook, local radio) → Word of mouth",
        "winning_signal": "100+ restaurants, 5k+ users, £2k+/week orders in 90 days",
        "verdict": "🟢 STRONG",
        "verdict_reason": "Clear wedge (underserved towns) + network effects + defensible through local relationships",
        "estimated_tam": "Small UK towns: 500+ towns × 50k population avg × £3/week food delivery = £7.5B TAM",
        "moat_potential": "Local restaurant relationships + brand + delivery fleet",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-20"
    },

    # === NICHE B2B ===
    {
        "id": "idea_007",
        "name": "Payroll for Gig Workers (India)",
        "description": "Automated payroll + payment system for gig platform workers (Uber drivers, delivery partners)",
        "category": "SaaS B2B",
        "market_size": "$500M-1B",
        "tam": "$100M-300M",
        "target_users": "Gig platforms (Uber, Ola, Swiggy), Driver payment teams",
        "competitors": ["Stripe Connect", "Wise for Business", "PayU", "RazorpayX", "Local payroll services"],
        "defensibility_score": 65,
        "defensibility_level": "🟡 Moderate-Strong",
        "defensibility_reason": "India-specific + gig-economy expertise. Vertical focus on gig = defensible.",
        "key_differentiator": "Handles irregular hours + KYC automation + instant payouts + tax compliance",
        "gtm_playbook": "Direct outreach to gig platforms (Uber India, Ola, Swiggy) + payment team partnerships",
        "winning_signal": "1-2 gig platforms as customers, 500k+ worker payouts/month, $20k+/MRR in 90 days",
        "verdict": "🟡 Viable niche",
        "verdict_reason": "Huge market (India gig economy = 30M+ workers) but requires enterprise sales + regulatory expertise",
        "estimated_tam": "India gig workers: 30M+ × £50/month payroll processing = £1.5B TAM",
        "moat_potential": "Regulatory expertise + Aadhaar/KYC integrations + gig economy knowledge",
        "risk_level": "MEDIUM-HIGH",
        "created_at": "2026-08-20"
    },

    # === COMMUNITY/SOCIAL ===
    {
        "id": "idea_008",
        "name": "Niche Community Platform (Specific Interest Groups)",
        "description": "Private community for specific niche (e.g., solopreneurs, indie hackers, single parents)",
        "category": "SaaS B2C",
        "market_size": "$500M-1B",
        "tam": "$100M-500M",
        "target_users": "Niche communities (solopreneurs, indie hackers, parenting groups)",
        "competitors": ["Mighty Networks", "Circle", "Slack", "Discord", "Facebook Groups"],
        "defensibility_score": 40,
        "defensibility_level": "🔴 Weak",
        "defensibility_reason": "Facebook Groups are free and have scale. Slack/Discord owned by enterprises. Hard to differentiate.",
        "key_differentiator": "Niche-specific features (e.g., 'Solopreneurs' with earnings tracking, tax tips, etc.)",
        "gtm_playbook": "Find existing influencer in niche → Recruit 100 core members → Launch on ProductHunt as 'community for X'",
        "winning_signal": "1k+ active members, 10% paid premium tier, £1k+/MRR in 90 days",
        "verdict": "❌ HIGH RISK",
        "verdict_reason": "Community is hard. Most fail. Big players dominate (Facebook, Slack, Discord). Only win with founder influence.",
        "estimated_tam": "Niche communities: 1000+ niches × 10k members avg × £5/mo = £500M TAM (but fragmented)",
        "moat_potential": "Community lock-in + founder credibility + niche-specific features",
        "risk_level": "HIGH",
        "created_at": "2026-08-20"
    },

    # === DEVELOPER TOOLS ===
    {
        "id": "idea_009",
        "name": "API Analytics for Indie Developers",
        "description": "Dashboard showing API performance, usage trends, errors, costs across all services (Stripe, Twilio, AWS)",
        "category": "Developer Tool",
        "market_size": "$200M-500M",
        "tam": "$50M-200M",
        "target_users": "Indie developers, Small SaaS teams",
        "competitors": ["Datadog", "New Relic", "Sentry", "AWS CloudWatch", "Stripe Dashboard"],
        "defensibility_score": 50,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Monitoring is crowded (Datadog, New Relic, Sentry). Differentiate via indie-focused pricing.",
        "key_differentiator": "Cheap ($5-50/mo for indie devs), all-in-one (not just errors, also costs + usage), beautiful UI",
        "gtm_playbook": "ProductHunt + HackerNews + Indie Hackers + Twitter + Dev.to",
        "winning_signal": "5k+ developers, 500+ paid, $2k+/MRR in 90 days",
        "verdict": "🟡 Viable with pricing focus",
        "verdict_reason": "Market is crowded but indie segment underserved by expensive enterprise tools.",
        "estimated_tam": "Indie developers: 500k+ globally × £10-50/mo = £50M-250M TAM",
        "moat_potential": "Ease of use + indie community + integrations",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-20"
    },

    # === SUSTAINABILITY/IMPACT ===
    {
        "id": "idea_010",
        "name": "Carbon Footprint Tracker (Personal)",
        "description": "Track personal carbon footprint (flights, driving, shopping) + get offsetting recommendations",
        "category": "Mobile App",
        "market_size": "$100M-500M",
        "tam": "$50M-200M",
        "target_users": "Environmentally conscious consumers, Companies wanting carbon tracking",
        "competitors": ["Carbon Footprint app", "Klima", "Offset Earth", "Wren", "Ecosia"],
        "defensibility_score": 35,
        "defensibility_level": "🔴 Weak",
        "defensibility_reason": "Crowded. Klima, Wren already established. Differentiation is hard (data is similar).",
        "key_differentiator": "Gamification + social comparison + integration with shopping (Shopify, Amazon)",
        "gtm_playbook": "TikTok environmental creators + Reddit r/environment + Instagram sustainability influencers",
        "winning_signal": "50k+ users, 5k+ paid, £1k+/MRR in 90 days",
        "verdict": "⚠️ HIGH RISK",
        "verdict_reason": "Crowded market with established players. Difficult to differentiate. Users may churn when guilt fades.",
        "estimated_tam": "Environmentally conscious: 500M+ people × £2-5/mo = £1B-2.5B TAM (but engagement is low)",
        "moat_potential": "Data collection + B2B partnerships (companies wanting carbon reporting)",
        "risk_level": "HIGH",
        "created_at": "2026-08-20"
    }
]


def get_all_ideas():
    """Return all curated ideas."""
    return IDEAS_SEED


def get_idea_by_id(idea_id: str):
    """Get single idea by ID."""
    for idea in IDEAS_SEED:
        if idea["id"] == idea_id:
            return idea
    return None


def filter_ideas(category=None, defensibility_min=None, market_size=None, verdict=None):
    """
    Filter ideas by criteria.
    """
    results = IDEAS_SEED

    if category:
        results = [i for i in results if i["category"].lower() == category.lower()]

    if defensibility_min:
        results = [i for i in results if i["defensibility_score"] >= defensibility_min]

    if verdict:
        results = [i for i in results if verdict.lower() in i["verdict"].lower()]

    return results


def get_trending_ideas(limit=10):
    """Return most saved/viewed ideas (placeholder - in production, track usage)."""
    # For now, return highest defensibility scored ideas
    return sorted(IDEAS_SEED, key=lambda x: x["defensibility_score"], reverse=True)[:limit]
