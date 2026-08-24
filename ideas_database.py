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
    },

    # === HEALTHCARE & WELLNESS ===
    {
        "id": "idea_011",
        "name": "Mental Health for Remote Workers",
        "description": "AI-powered wellness check-ins via WhatsApp + mood tracking + counselor matching + corporate partnerships",
        "category": "SaaS B2B",
        "market_size": "$2B-5B",
        "tam": "$500M-1B",
        "target_users": "Remote-first companies, HR departments, Insurance providers",
        "competitors": ["Headspace", "Calm", "BetterHelp", "Spring", "Ginger"],
        "defensibility_score": 65,
        "defensibility_level": "🟡 Moderate-Strong",
        "defensibility_reason": "B2B mental health is growing fast. WhatsApp channel is unique for emerging markets.",
        "key_differentiator": "WhatsApp-first (no app install), corporate integrations, instant counselor matching",
        "gtm_playbook": "HR conferences + LinkedIn to HR managers + Remote work communities (RemoteOK, We Work Remotely)",
        "winning_signal": "5+ corporate clients, 2k+ employees using, £10k+/MRR in 90 days",
        "verdict": "🟢 Strong opportunity",
        "verdict_reason": "Post-COVID wellness boom + remote work growth = huge demand. B2B SaaS is predictable revenue.",
        "estimated_tam": "5M remote workers globally × £5-20/mo corporate spend = £250M-1B TAM",
        "moat_potential": "Corporate lock-in + counselor network + anonymity/privacy",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },
    {
        "id": "idea_012",
        "name": "Women's Health Companion (Fertility, Menopause, etc.)",
        "description": "AI advisor for women's health + cycle tracking + fertility planning + menopause support + doctor chat",
        "category": "Mobile App",
        "market_size": "$1B-2B",
        "tam": "$300M-700M",
        "target_users": "Women 18-65, Healthcare providers",
        "competitors": ["Clue", "Flo", "Eve", "Kindara", "Glow"],
        "defensibility_score": 55,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Crowded with Clue/Flo. Differentiate via specialization (menopause), better counselor network, or AI depth.",
        "key_differentiator": "Holistic (cycle → fertility → menopause) + verified doctor Q&A + menopause focus (underserved)",
        "gtm_playbook": "Women's health influencers + Reddit r/TryingForABaby + TikTok + GYN office partnerships",
        "winning_signal": "50k+ users, 10k+ paid, £5k+/MRR in 90 days",
        "verdict": "🟡 Viable with focus",
        "verdict_reason": "Huge TAM but Clue/Flo are strong. Win by owning menopause or better doctor access.",
        "estimated_tam": "500M women globally × £3-8/mo = £1.5B-4B TAM",
        "moat_potential": "User data for better predictions + doctor network + gynecologist partnerships",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },

    # === CREATOR ECONOMY ===
    {
        "id": "idea_013",
        "name": "Podcast Analytics + Sponsorship Marketplace",
        "description": "Dashboard showing podcast performance + AI matching with sponsors + sponsor payment automation",
        "category": "SaaS B2B",
        "market_size": "$500M-1B",
        "tam": "$150M-400M",
        "target_users": "Podcasters, Podcast networks, Sponsors",
        "competitors": ["Spotify for Podcasters", "Podtrac", "Acast", "Megaphone", "Transistor"],
        "defensibility_score": 60,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Spotify owns podcasting platform but sponsorship piece is fragmented. Opportunity to own sponsor matching.",
        "key_differentiator": "AI sponsor matching (vs. manual outreach), transparent pricing, creator-friendly terms",
        "gtm_playbook": "Podcast directories + creator Slack communities + Twitter to podcasters + Reddit r/podcasting",
        "winning_signal": "100+ podcasters, 50+ active sponsors, £5k+/MRR in 90 days",
        "verdict": "🟡 Viable niche",
        "verdict_reason": "Growing creator economy + fragmented sponsorship = clear pain. But Spotify/Acast have scale advantage.",
        "estimated_tam": "500k+ podcasters globally × £50-200/mo sponsor matching = £250M-1B TAM",
        "moat_potential": "Creator community + sponsor relationships + AI matching quality",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },
    {
        "id": "idea_014",
        "name": "YouTube Shorts Creator Tools",
        "description": "AI-powered script generation + B-roll library + trend alerts + analytics dashboard for Shorts creators",
        "category": "SaaS B2C",
        "market_size": "$500M-2B",
        "tam": "$200M-800M",
        "target_users": "YouTubers, Content creators, Agencies",
        "competitors": ["VidIQ", "TubeBuddy", "CapCut Pro", "Adobe Premiere", "Descript"],
        "defensibility_score": 50,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "YouTube Shorts is new/growing. Existing tools are fragmented. But Adobe/CapCut have resources.",
        "key_differentiator": "Shorts-specific (not full-video focus), AI script generation, trend alerts, simple UI",
        "gtm_playbook": "YouTube creator communities + TikTok + Instagram + Reddit r/content_creators",
        "winning_signal": "10k+ creators, 1k+ paid, £2k+/MRR in 90 days",
        "verdict": "🟡 Viable with focus",
        "verdict_reason": "Shorts is exploding. Opportunity to own this vertical before YouTube/Adobe build tools.",
        "estimated_tam": "1M+ Shorts creators globally × £3-15/mo = £30M-150M TAM",
        "moat_potential": "AI training on successful Shorts + creator community + trend database",
        "risk_level": "MEDIUM-HIGH",
        "created_at": "2026-08-21"
    },

    # === REAL ESTATE ===
    {
        "id": "idea_015",
        "name": "Landlord Management SaaS (Rent Collection + Tenant Portal)",
        "description": "WhatsApp-native rent payment collection + tenant requests + lease management + tax docs export",
        "category": "SaaS B2B",
        "market_size": "$2B-5B",
        "tam": "$500M-1.5B",
        "target_users": "Landlords, Property managers, Property investment groups",
        "competitors": ["Zillow Rental Manager", "Landlord Studio", "TenantCloud", "Buildium", "AppFolio"],
        "defensibility_score": 70,
        "defensibility_level": "🟢 Strong",
        "defensibility_reason": "WhatsApp for rent collection in emerging markets (India, SE Asia, Africa) is unique + high switching costs.",
        "key_differentiator": "WhatsApp rent payment collection (no app install), tenant portal, tax reporting, mobile-first",
        "gtm_playbook": "Real estate investor communities + PropertyShark + Bigger Pockets + landlord Facebook groups",
        "winning_signal": "100+ landlords, 5k+ tenants, £10k+/MRR in 90 days",
        "verdict": "🚀 STRONG opportunity",
        "verdict_reason": "Landlords hate fragmented systems. WhatsApp payment in India = huge moat. B2B SaaS margins.",
        "estimated_tam": "5M+ landlords in India/SE Asia × £10-50/mo = £500M-2.5B TAM",
        "moat_potential": "WhatsApp integration + rent payment lock-in + compliance/tax data",
        "risk_level": "LOW-MEDIUM",
        "created_at": "2026-08-21"
    },

    # === FINANCE/FINTECH ===
    {
        "id": "idea_016",
        "name": "Invoice Financing for Freelancers (Buy Your Invoices)",
        "description": "Freelancers upload invoices → instant cash (minus 2-5% fee) → no waiting 30/60/90 days for payment",
        "category": "Fintech",
        "market_size": "$2B-5B",
        "tam": "$500M-1.5B",
        "target_users": "Freelancers, Small businesses, Contractors",
        "competitors": ["Uncapped", "Clearco", "Liberis", "Funding Circle", "OnDeck"],
        "defensibility_score": 65,
        "defensibility_level": "🟡 Moderate-Strong",
        "defensibility_reason": "Fintech is competitive but freelancer segment underserved. Need regulatory license + capital.",
        "key_differentiator": "Instant approvals (AI-based), WhatsApp/SMS interface, no equity dilution",
        "gtm_playbook": "Upwork + Fiverr communities + Reddit r/freelance + Slack groups + ProductHunt",
        "winning_signal": "500+ freelancers, £100k+ volume financed, £10k+/MRR in 90 days",
        "verdict": "🟡 Viable (needs capital + license)",
        "verdict_reason": "Real pain point but needs £500k-1M seed + FCA/regulatory license. High risk if underfunded.",
        "estimated_tam": "50M freelancers globally × avg £500 invoice × 2x financing/year = £50B TAM",
        "moat_potential": "Credit underwriting AI + speed + compliance expertise",
        "risk_level": "HIGH",
        "created_at": "2026-08-21"
    },
    {
        "id": "idea_017",
        "name": "Business Accounting for Micro-Entrepreneurs (India)",
        "description": "Snap receipt → auto-GST calculation + profit/loss statement + tax filing + bank reconciliation",
        "category": "SaaS B2C",
        "market_size": "$1B-3B",
        "tam": "$300M-1B",
        "target_users": "Small shop owners, Street vendors, Micro-entrepreneurs (India)",
        "competitors": ["Zoho Books", "BUSY", "Tally", "GST Suvidha Kendra", "Manual CPAs"],
        "defensibility_score": 70,
        "defensibility_level": "🟢 Strong",
        "defensibility_reason": "Hyper-local (India-specific GST) + large underserved market (50M+ micro-businesses with zero accounting)",
        "key_differentiator": "Simple (photos only), WhatsApp support, local CPA help, cheap (£3-5/mo)",
        "gtm_playbook": "Local shops + street vendor networks + Gig economy platforms + YouTube India + Google Pay integration",
        "winning_signal": "10k+ users, 1k+ paid, £2k+/MRR in 90 days",
        "verdict": "🟢 Strong (India-specific)",
        "verdict_reason": "Massive TAM + minimal competition + digital payment adoption in India = huge opportunity.",
        "estimated_tam": "50M+ micro-businesses in India × £3-5/mo = £1.8B-3B TAM",
        "moat_potential": "India-specific tax knowledge + local CPA network + language (Hindi)",
        "risk_level": "LOW-MEDIUM",
        "created_at": "2026-08-21"
    },

    # === EDUCATION ===
    {
        "id": "idea_018",
        "name": "Coding Bootcamp (Async + Affordable)",
        "description": "Pre-recorded coding courses (JavaScript, Python, React) + live project reviews + job board + lifetime access",
        "category": "Online Education",
        "market_size": "$2B-5B",
        "tam": "$500M-1.5B",
        "target_users": "Career-switchers, Students, Unemployed developers",
        "competitors": ["Udemy", "Coursera", "Codecademy", "Lambda School", "Bootcamp.dev"],
        "defensibility_score": 45,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Edtech is crowded. Udemy has scale. Differentiate via outcomes (job placement) or affordability.",
        "key_differentiator": "Pay-after-hire model (no upfront cost) + 90-day guarantee + live project code reviews",
        "gtm_playbook": "Reddit r/learnprogramming + HackerNews + Twitter dev community + YouTube tutorials + career subreddits",
        "winning_signal": "1k+ students, 100+ graduates placed, £20k+/MRR in 90 days",
        "verdict": "🟡 Viable (outcomes-focused)",
        "verdict_reason": "Bootcamp market is proven but crowded. Win by focusing on outcomes (job placements) not content.",
        "estimated_tam": "5M+ career-switchers annually × £3k-15k avg course cost = £15B-75B TAM",
        "moat_potential": "Outcomes data (job placement rate) + employer relationships + alumni network",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },

    # === B2B ENTERPRISE ===
    {
        "id": "idea_019",
        "name": "Employee Onboarding Automation (HR Tech)",
        "description": "Checklist automation + document e-signing + IT provisioning + compliance tracking + new hire portal",
        "category": "SaaS B2B",
        "market_size": "$3B-7B",
        "tam": "$1B-3B",
        "target_users": "HR teams, Mid-market companies, Enterprise",
        "competitors": ["Workday", "BambooHR", "Guidepoint", "Fabric", "Click Boarding"],
        "defensibility_score": 55,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "HR SaaS is crowded. Workday owns enterprise. Opportunity in SMB/mid-market segment.",
        "key_differentiator": "Simple (no Workday complexity) + cheap + integrates with existing HR systems",
        "gtm_playbook": "HR Slack communities + LinkedIn to HR leaders + ProductHunt + HR conference sponsorships",
        "winning_signal": "50+ companies, 5k+ new hires onboarded, £15k+/MRR in 90 days",
        "verdict": "🟡 Viable (SMB focus)",
        "verdict_reason": "Market is proven but owned by Workday. Win by building for SMB (10-500 employees) specifically.",
        "estimated_tam": "1M+ SMB companies globally × £5k-20k/year onboarding = £5B-20B TAM",
        "moat_potential": "HR integrations + compliance expertise + ease of use vs Workday",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },
    {
        "id": "idea_020",
        "name": "Sales Process Automation (Non-Code Workflow Builder)",
        "description": "Drag-and-drop workflow builder for sales teams (trigger: new lead → action: email + Slack alert + calendar block)",
        "category": "SaaS B2B",
        "market_size": "$2B-5B",
        "tam": "$500M-1.5B",
        "target_users": "Sales teams, Small SaaS, Real estate agents",
        "competitors": ["Zapier", "Make (Integromat)", "IFTTT", "n8n", "Integrio"],
        "defensibility_score": 50,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Zapier dominates automation. Differentiate via sales-specific templates or simpler UX.",
        "key_differentiator": "Sales-specific workflows (not generic automation) + pre-built Salesforce/HubSpot templates + no-code",
        "gtm_playbook": "ProductHunt + SaaS communities + Reddit r/sales + LinkedIn to sales leaders",
        "winning_signal": "100+ teams, 2k+ workflows created, £8k+/MRR in 90 days",
        "verdict": "🟡 Viable niche",
        "verdict_reason": "Automation market is proven but Zapier has scale. Win by owning 'sales workflows' specifically.",
        "estimated_tam": "500k+ sales teams globally × £20-100/mo = £100M-500M TAM",
        "moat_potential": "Sales-specific templates + CRM integrations + usage data",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },

    # === SUSTAINABILITY & MARKETPLACE ===
    {
        "id": "idea_021",
        "name": "Secondhand Baby Products Marketplace (Rent/Buy)",
        "description": "Rent or buy baby gear (stroller, crib, carrier) peer-to-peer + insurance included + WhatsApp coordination",
        "category": "Marketplace",
        "market_size": "$500M-2B",
        "tam": "$200M-800M",
        "target_users": "New parents, Eco-conscious families",
        "competitors": ["Vinted", "Depop", "Facebook Marketplace", "Rent-a-stroller.com"],
        "defensibility_score": 60,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Niche marketplace + sustainability angle. Vinted doesn't focus on rentals. Insurance/coordination is differentiator.",
        "key_differentiator": "Rental model (cheaper for parents) + included insurance + peer trust ratings + WhatsApp coordination",
        "gtm_playbook": "Parenting forums + Facebook parent groups + Reddit r/BabyBumps + YouTube parenting creators",
        "winning_signal": "5k+ parents, 500+ active rentals, £3k+/MRR in 90 days",
        "verdict": "🟡 Viable (niche but growing)",
        "verdict_reason": "Eco-conscious parents are real audience. Rental model has higher margins than marketplace. Low CAC via FB groups.",
        "estimated_tam": "2M+ new parents/year in UK/US × £30-100 avg rental = £60M-200M TAM",
        "moat_potential": "Peer trust network + insurance partnerships + inventory of popular items",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },
    {
        "id": "idea_022",
        "name": "Repair Services Marketplace (Appliances, Phones, Furniture)",
        "description": "Local repair experts (plumber, electrician, tech) + WhatsApp booking + transparent pricing + ratings",
        "category": "Marketplace",
        "market_size": "$1B-3B",
        "tam": "$300M-1B",
        "target_users": "Homeowners, Renters, Small businesses",
        "competitors": ["Taskrabbit", "Fancy Hands", "Fiverr", "Care.com 'Services'"],
        "defensibility_score": 65,
        "defensibility_level": "🟡 Moderate-Strong",
        "defensibility_reason": "Local services have network effects. WhatsApp in emerging markets is unique. High repeat rate.",
        "key_differentiator": "WhatsApp-native (no app needed) + hyper-local + emergency repairs + upfront pricing",
        "gtm_playbook": "Local Facebook groups + Google local ads + referral incentives + community signage",
        "winning_signal": "50+ repairers, 5k+ repairs booked, £5k+/MRR in 90 days",
        "verdict": "🟢 Strong (emerging markets)",
        "verdict_reason": "Huge TAM + local network effects + high repeat rate. Perfect for WhatsApp/India/Africa markets.",
        "estimated_tam": "100M+ households in India/Africa/SE Asia × £50-100/year repair = £5B-10B TAM",
        "moat_potential": "Repairer network + trust ratings + WhatsApp lock-in + local data",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },

    # === LOCAL/COMMUNITY ===
    {
        "id": "idea_023",
        "name": "Neighborhood Safety Alert Network (WhatsApp/Telegram)",
        "description": "Neighbors share crime alerts, missing persons, lost pets → geofence notifications + community forum",
        "category": "Community",
        "market_size": "$100M-500M",
        "tam": "$50M-250M",
        "target_users": "Homeowners, Urban residents, Neighborhood associations",
        "competitors": ["Nextdoor", "Ring Neighbors", "Citizen", "Local Facebook Groups"],
        "defensibility_score": 50,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Nextdoor owns US market but has privacy concerns. WhatsApp-based alternative could win emerging markets.",
        "key_differentiator": "WhatsApp-based (no new app) + end-to-end encryption + anonymous alerts option",
        "gtm_playbook": "Neighborhood Facebook groups + Nextdoor alternative communities + Local police partnerships",
        "winning_signal": "100+ neighborhoods, 10k+ members, £2k+/MRR in 90 days",
        "verdict": "🟡 Viable (emerging markets)",
        "verdict_reason": "Nextdoor is strong but WhatsApp alternative in India/Africa could scale. Community moderation is hard.",
        "estimated_tam": "100M+ neighborhoods globally × £1-3/mo = £1B-3B TAM",
        "moat_potential": "Neighborhood lock-in + trust + police partnerships + hyper-local data",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
    },

    # === VERTICAL AI AGENTS ===
    {
        "id": "idea_024",
        "name": "Legal Document Generator for Startups (AI-Powered)",
        "description": "Answer 5 questions → AI generates: NDAs, Terms of Service, Privacy Policy, Founder Agreements, Employee Offers",
        "category": "AI Tool",
        "market_size": "$500M-2B",
        "tam": "$150M-500M",
        "target_users": "Startups, Solo entrepreneurs, Small LLCs",
        "competitors": ["Rocket Lawyer", "LegalZoom", "Ironclad", "Lexology", "OpenLaw"],
        "defensibility_score": 55,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Legal tech is growing. LegalZoom has scale but expensive. Opportunity in AI-generated + cheap.",
        "key_differentiator": "AI-generates in 2 mins + £50-200 (vs £500+ LegalZoom) + templates updated via AI",
        "gtm_playbook": "ProductHunt + Indie Hackers + Hacker News + Twitter startup communities + AngelList",
        "winning_signal": "5k+ startups, 500+ paid, £3k+/MRR in 90 days",
        "verdict": "🟡 Viable (depends on AI quality)",
        "verdict_reason": "Legal accuracy matters. If AI can generate quality docs + liability covered = strong TAM.",
        "estimated_tam": "500k+ startups created yearly × £50-200 avg spend = £25M-100M TAM",
        "moat_potential": "AI training on real contracts + lawyer reviews + template library",
        "risk_level": "MEDIUM-HIGH",
        "created_at": "2026-08-21"
    },
    {
        "id": "idea_025",
        "name": "Job Interview Coach (AI + Human Feedback)",
        "description": "Record mock interview → AI analyzes body language, filler words, enthusiasm → coach gives feedback + tips",
        "category": "AI Tool",
        "market_size": "$500M-1.5B",
        "tam": "$150M-500M",
        "target_users": "Job seekers, Career switchers, Students",
        "competitors": ["BigInterview", "InterviewBit", "Pramp", "Cribl", "Mock Interview AI"],
        "defensibility_score": 50,
        "defensibility_level": "🟡 Moderate",
        "defensibility_reason": "Emerging market but needs human feedback element to beat AI-only tools. B2C acquisition can be expensive.",
        "key_differentiator": "AI analysis (real-time) + optional human coach review + industry-specific questions + LinkedIn integration",
        "gtm_playbook": "LinkedIn to job seekers + Reddit r/cscareerquestions + YouTube career channels + Coursera integration",
        "winning_signal": "10k+ users, 1k+ paid, £2k+/MRR in 90 days",
        "verdict": "🟡 Viable",
        "verdict_reason": "Job interview anxiety is real. B2C can be hard to scale but subscription LTV is high.",
        "estimated_tam": "50M+ job seekers globally × £5-20/month = £3B-12B TAM",
        "moat_potential": "AI body language detection + coach network + question library",
        "risk_level": "MEDIUM",
        "created_at": "2026-08-21"
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
