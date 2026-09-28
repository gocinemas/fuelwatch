"""
AI Opportunities Database — Deep use case library by industry.

Provides industry-specific, high-impact AI use cases with:
- Problem statement
- Concrete AI solution
- Impact quantification (cost/revenue)
- Implementation timeline
- ROI estimate
- Department/function affected
"""

AI_OPPORTUNITIES_BY_INDUSTRY = {
    "retail": {
        "primary_focus": "Customer Experience & Cost Reduction",
        "use_cases": [
            {
                "icon": "🛍️",
                "title": "Dynamic Pricing Engine",
                "department": "Revenue Management",
                "problem": "Fixed pricing leaves money on the table during demand spikes; competitors adjust prices in real-time",
                "solution": "AI analyzes demand patterns, competitor pricing, inventory levels → automatically adjusts prices to maximize margin while maintaining competitiveness",
                "impact": "5-15% revenue increase; 3-8% margin improvement",
                "timeline": "6-10 weeks",
                "roi": "Very High",
                "complexity": "Medium"
            },
            {
                "icon": "👤",
                "title": "Personalized Shopping Assistant",
                "department": "Customer Experience",
                "problem": "Generic product recommendations don't drive conversion; customers browse without guidance",
                "solution": "AI learns customer preferences from browsing, purchase history, demographics → recommends products in real-time on website and via SMS/email",
                "impact": "8-12% conversion uplift; 15-25% AOV increase",
                "timeline": "8-12 weeks",
                "roi": "Very High",
                "complexity": "Medium"
            },
            {
                "icon": "📊",
                "title": "Demand Forecasting",
                "department": "Supply Chain",
                "problem": "Inventory mismatches waste capital and shelf space; stockouts lose sales",
                "solution": "AI predicts demand by SKU, location, season → guides optimal stock levels and ordering",
                "impact": "15-20% inventory reduction; 2-5% revenue protection (fewer stockouts)",
                "timeline": "10-14 weeks",
                "roi": "High",
                "complexity": "Medium-High"
            },
            {
                "icon": "💳",
                "title": "Fraud Detection",
                "department": "Risk & Compliance",
                "problem": "Fraud costs 1-2% of revenue; manual reviews are slow and inconsistent",
                "solution": "AI flags suspicious transactions (velocity, geography, amount patterns) in real-time; blocks/escalates based on risk",
                "impact": "Reduce fraud losses by 70-80%; faster transaction processing",
                "timeline": "4-8 weeks",
                "roi": "Very High",
                "complexity": "Low-Medium"
            },
            {
                "icon": "🔍",
                "title": "Visual Search",
                "department": "Customer Experience",
                "problem": "Customers struggle to find similar items; text-based search misses visual intent",
                "solution": "AI recognizes objects in customer photos → finds matching products in catalog",
                "impact": "10-18% new customer acquisition; reduces search friction",
                "timeline": "8-12 weeks",
                "roi": "High",
                "complexity": "Medium"
            },
            {
                "icon": "🤖",
                "title": "Customer Service Chatbot",
                "department": "Customer Service",
                "problem": "High contact center costs; customers wait hours for response; FAQ volume is overwhelming",
                "solution": "AI chatbot handles 60-70% of inquiries (returns, orders, tracking, FAQs) → human agents handle complex cases",
                "impact": "40-50% contact center cost savings; 24/7 availability; CSAT +15%",
                "timeline": "4-6 weeks",
                "roi": "Very High",
                "complexity": "Low"
            },
        ]
    },

    "manufacturing": {
        "primary_focus": "Operational Efficiency & Quality",
        "use_cases": [
            {
                "icon": "🔧",
                "title": "Predictive Maintenance",
                "department": "Operations",
                "problem": "Equipment failures cause unplanned downtime (£10K+/day); maintenance is reactive",
                "solution": "AI analyzes sensor data → predicts failures 2-4 weeks ahead; schedules maintenance proactively",
                "impact": "60-70% reduction in unplanned downtime; £200K-500K annual savings",
                "timeline": "8-12 weeks",
                "roi": "Very High",
                "complexity": "Medium-High"
            },
            {
                "icon": "🎯",
                "title": "Quality Control Automation",
                "department": "Manufacturing",
                "problem": "Manual QC misses 2-5% of defects; expensive rework and recalls",
                "solution": "Computer vision inspects every unit → detects defects (cracks, misalignment, color) faster than humans",
                "impact": "Reduce defects by 90%; prevent recalls; improve yield by 3-5%",
                "timeline": "10-16 weeks",
                "roi": "Very High",
                "complexity": "High"
            },
            {
                "icon": "📉",
                "title": "Production Optimization",
                "department": "Operations",
                "problem": "Suboptimal production schedules cause bottlenecks and waste; changeover time is long",
                "solution": "AI optimizes production schedules, machine allocation, and changeover sequencing",
                "impact": "10-15% throughput increase; 5-8% waste reduction; 2-3% margin improvement",
                "timeline": "12-16 weeks",
                "roi": "High",
                "complexity": "High"
            },
            {
                "icon": "🏭",
                "title": "Supply Chain Visibility",
                "department": "Supply Chain",
                "problem": "No real-time visibility into supplier performance; late deliveries disrupt production",
                "solution": "AI monitors supplier data → predicts delays; suggests alternatives; optimizes reorder points",
                "impact": "On-time delivery +8-12%; inventory reduction 10-15%; supplier risk ↓50%",
                "timeline": "6-10 weeks",
                "roi": "High",
                "complexity": "Medium"
            },
            {
                "icon": "⚙️",
                "title": "Process Documentation AI",
                "department": "Operations",
                "problem": "Process documentation is outdated; training takes weeks; inconsistent execution",
                "solution": "AI auto-documents processes from video; generates step-by-step guides and training materials",
                "impact": "Training time -50%; consistency +70%; onboarding -3 weeks",
                "timeline": "4-8 weeks",
                "roi": "High",
                "complexity": "Low-Medium"
            },
            {
                "icon": "💰",
                "title": "Cost Variance Analysis",
                "department": "Finance",
                "problem": "Cost overruns discovered too late; no real-time visibility into actual vs budgeted costs",
                "solution": "AI ingests production data → predicts cost overruns; flags variances automatically",
                "impact": "Catch 95% of overruns early; 2-4% cost savings; faster month-end close",
                "timeline": "4-6 weeks",
                "roi": "High",
                "complexity": "Low"
            },
        ]
    },

    "services": {
        "primary_focus": "Service Delivery & Profitability",
        "use_cases": [
            {
                "icon": "📄",
                "title": "Invoice Automation",
                "department": "Finance",
                "problem": "Manual invoicing takes 30+ hours/month; errors cause payment delays and disputes",
                "solution": "AI reads timesheets → generates invoices → sends to clients; handles recurring billing",
                "impact": "Save 30-40 hrs/month; reduce DSO by 10-15 days; 3-5% revenue acceleration",
                "timeline": "2-4 weeks",
                "roi": "Very High",
                "complexity": "Low"
            },
            {
                "icon": "👨‍💼",
                "title": "Resource Allocation",
                "department": "Operations",
                "problem": "Billable utilization is suboptimal; project staffing decisions are manual and often wrong",
                "solution": "AI predicts project needs → recommends optimal resource allocation; matches skills to projects",
                "impact": "Utilization +5-8%; project profitability +10-15%; improve delivery on time",
                "timeline": "6-10 weeks",
                "roi": "High",
                "complexity": "Medium"
            },
            {
                "icon": "💬",
                "title": "Proposal Generation",
                "department": "Sales",
                "problem": "Creating custom proposals takes 8-12 hours; delayed responses lose deals",
                "solution": "AI generates proposals from client brief + historical templates; ~80% ready to send",
                "impact": "Proposal cycle -5 days; close rate +10-15%; sales team saves 8-10 hrs/week",
                "timeline": "4-8 weeks",
                "roi": "High",
                "complexity": "Medium"
            },
            {
                "icon": "💡",
                "title": "Client Profitability Analysis",
                "department": "Finance",
                "problem": "Unknown which clients are actually profitable; loss-makers subsidized by winners",
                "solution": "AI calculates real profitability by client accounting for all costs (labor, overhead, margin)",
                "impact": "Identify £50K+ in unprofitable work; guide repricing; improve overall margin",
                "timeline": "2-4 weeks",
                "roi": "Very High",
                "complexity": "Low"
            },
            {
                "icon": "🔄",
                "title": "Project Delivery Intelligence",
                "department": "Project Management",
                "problem": "Projects run late/over-budget; no early warning system",
                "solution": "AI learns project patterns → predicts delays and cost overruns → alerts PM",
                "impact": "Project success rate +20%; cost variance -3-5%; client satisfaction +8%",
                "timeline": "6-10 weeks",
                "roi": "High",
                "complexity": "Medium"
            },
            {
                "icon": "📧",
                "title": "Client Communication Bot",
                "department": "Customer Service",
                "problem": "Clients ask repetitive questions; status updates take manual effort",
                "solution": "AI chatbot answers FAQs, provides status updates, schedules meetings",
                "impact": "Reduce CS inquiries by 40%; faster response; CSAT +12%; free up team",
                "timeline": "3-5 weeks",
                "roi": "Very High",
                "complexity": "Low-Medium"
            },
        ]
    },

    "healthcare": {
        "primary_focus": "Patient Safety & Operational Efficiency",
        "use_cases": [
            {
                "icon": "🏥",
                "title": "Patient Risk Prediction",
                "department": "Clinical",
                "problem": "Identify at-risk patients early to prevent readmissions and complications",
                "solution": "AI analyzes patient history → flags high-risk individuals for proactive intervention",
                "impact": "Reduce readmissions 15-25%; improve outcomes; prevent adverse events",
                "timeline": "8-12 weeks",
                "roi": "Very High",
                "complexity": "High"
            },
            {
                "icon": "🔬",
                "title": "Medical Image Analysis",
                "department": "Radiology/Diagnostics",
                "problem": "Radiologist shortage; diagnostic delays; human error in image interpretation",
                "solution": "AI assists in reading X-rays/CT/MRI → highlights anomalies; speeds diagnosis",
                "impact": "Diagnostic accuracy +5-10%; turnaround -2 days; catch more early cancers",
                "timeline": "12-16 weeks",
                "roi": "Very High",
                "complexity": "Very High"
            },
            {
                "icon": "💊",
                "title": "Drug-Drug Interaction Alerts",
                "department": "Pharmacy/Clinical",
                "problem": "Dangerous interactions missed; medication errors harm patients",
                "solution": "AI checks every prescription against patient history and other meds → alerts on interactions",
                "impact": "Prevent medication errors 99%+; improve patient safety; reduce liability",
                "timeline": "4-8 weeks",
                "roi": "Very High",
                "complexity": "Low"
            },
            {
                "icon": "📋",
                "title": "Clinical Documentation",
                "department": "Clinical Operations",
                "problem": "Doctors spend 2+ hours/day on documentation; errors in medical records",
                "solution": "AI transcribes patient visit → generates draft clinical notes; doctor reviews/approves",
                "impact": "Documentation time -50%; fewer errors; improved audit trail; doctor job satisfaction ↑",
                "timeline": "4-8 weeks",
                "roi": "High",
                "complexity": "Medium"
            },
            {
                "icon": "🚑",
                "title": "ED Triage Optimization",
                "department": "Emergency Department",
                "problem": "ED overcrowding; long wait times; inefficient patient flow",
                "solution": "AI predicts patient volume; recommends bed allocation; optimizes discharge planning",
                "impact": "Reduce wait times 20-30%; improve throughput; better patient satisfaction",
                "timeline": "6-10 weeks",
                "roi": "High",
                "complexity": "Medium"
            },
            {
                "icon": "💰",
                "title": "Revenue Cycle Automation",
                "department": "Finance",
                "problem": "Billing delays cost 5-10% of revenue; high denial rates",
                "solution": "AI checks claims for compliance → reduces denials; automates appeals; accelerates collections",
                "impact": "Reduce DSO by 15-20 days; improve claim approval rate; 2-3% revenue uplift",
                "timeline": "6-10 weeks",
                "roi": "Very High",
                "complexity": "Medium"
            },
        ]
    },

    "financial_services": {
        "primary_focus": "Risk Management & Customer Experience",
        "use_cases": [
            {
                "icon": "🚨",
                "title": "Fraud Detection",
                "department": "Risk & Compliance",
                "problem": "Fraud losses 0.5-2% of revenue; high false positive rates create friction",
                "solution": "AI learns legitimate transaction patterns → flags actual fraud with 99%+ accuracy",
                "impact": "Reduce fraud 70-80%; maintain customer experience; comply with regulations",
                "timeline": "8-12 weeks",
                "roi": "Very High",
                "complexity": "Medium-High"
            },
            {
                "icon": "💰",
                "title": "Credit Risk Modeling",
                "department": "Credit",
                "problem": "Traditional credit models miss emerging risks; loan losses 1-3%",
                "solution": "AI analyzes 100+ signals → predicts default probability; improves lending decisions",
                "impact": "Reduce loan losses 20-30%; approve more good customers; improve portfolio quality",
                "timeline": "10-14 weeks",
                "roi": "Very High",
                "complexity": "High"
            },
            {
                "icon": "🎯",
                "title": "Customer Churn Prediction",
                "department": "Customer Success",
                "problem": "Customers leave without warning; high acquisition cost wasted",
                "solution": "AI identifies churn signals → recommend targeted retention actions",
                "impact": "Reduce churn 10-20%; improve lifetime value; lower CAC impact",
                "timeline": "4-8 weeks",
                "roi": "High",
                "complexity": "Low-Medium"
            },
            {
                "icon": "📊",
                "title": "Portfolio Optimization",
                "department": "Trading/Investment",
                "problem": "Manual portfolio management misses opportunities; suboptimal risk-return",
                "solution": "AI suggests rebalancing based on market conditions and risk parameters",
                "impact": "Improved risk-adjusted returns; faster rebalancing; reduced tracking error",
                "timeline": "8-12 weeks",
                "roi": "High",
                "complexity": "High"
            },
            {
                "icon": "🤖",
                "title": "Customer Service AI",
                "department": "Customer Service",
                "problem": "High volume of routine inquiries; long wait times; expensive customer service",
                "solution": "AI handles account inquiries, balance checks, transfers, payment setup 24/7",
                "impact": "Handle 50-70% of inquiries; 24/7 availability; reduce CS costs 30-40%",
                "timeline": "4-8 weeks",
                "roi": "Very High",
                "complexity": "Low"
            },
            {
                "icon": "📈",
                "title": "AML/KYC Automation",
                "department": "Compliance",
                "problem": "Manual KYC takes weeks; high false positive rate; regulatory burden",
                "solution": "AI automates customer verification, document review, ongoing monitoring",
                "impact": "KYC cycle -70%; improve compliance; reduce false positives 50%+",
                "timeline": "6-10 weeks",
                "roi": "High",
                "complexity": "Medium-High"
            },
        ]
    },

    "technology": {
        "primary_focus": "Product Innovation & DevOps Efficiency",
        "use_cases": [
            {
                "icon": "🐛",
                "title": "Automated Bug Detection",
                "department": "Engineering",
                "problem": "Bugs slip to production; users discover issues; costly fixes and hotfixes",
                "solution": "AI analyzes code changes → predicts bugs before deployment; flags risky changes",
                "impact": "Reduce production bugs 40-60%; faster deployment cycles; improve uptime",
                "timeline": "4-8 weeks",
                "roi": "High",
                "complexity": "Medium"
            },
            {
                "icon": "🔒",
                "title": "Security Vulnerability Detection",
                "department": "Security",
                "problem": "Vulnerabilities discovered by hackers; breaches cost millions",
                "solution": "AI scans code and dependencies → identifies vulnerabilities before exploit",
                "impact": "Find 90%+ of vulnerabilities; reduce incident response time; improve security posture",
                "timeline": "2-4 weeks",
                "roi": "Very High",
                "complexity": "Low"
            },
            {
                "icon": "⚙️",
                "title": "DevOps Optimization",
                "department": "DevOps/SRE",
                "problem": "Manual deployment processes; high incident rates; slow MTTR",
                "solution": "AI predicts infrastructure issues; automates scaling and remediation",
                "impact": "Reduce incidents 30-50%; improve uptime; MTTR -40%; free up ops team",
                "timeline": "6-10 weeks",
                "roi": "High",
                "complexity": "Medium-High"
            },
            {
                "icon": "📝",
                "title": "Code Documentation",
                "department": "Engineering",
                "problem": "Documentation outdated; onboarding takes months; knowledge silos",
                "solution": "AI generates documentation from code; creates architecture diagrams; maintains specs",
                "impact": "Onboarding -2 weeks; reduce knowledge silos; improve code maintainability",
                "timeline": "2-4 weeks",
                "roi": "High",
                "complexity": "Low"
            },
            {
                "icon": "🚀",
                "title": "Feature Flag Optimization",
                "department": "Product/Engineering",
                "problem": "Slow rollouts; feature rollbacks are risky; test coverage gaps",
                "solution": "AI recommends optimal rollout percentages; monitors impact in real-time",
                "impact": "Faster safe rollouts; reduce rollback incidents; improve experimentation velocity",
                "timeline": "3-6 weeks",
                "roi": "High",
                "complexity": "Low-Medium"
            },
            {
                "icon": "📊",
                "title": "Performance Monitoring",
                "department": "Engineering",
                "problem": "Slow features discovered by users; difficult to debug performance issues",
                "solution": "AI analyzes performance metrics → identifies anomalies; suggests optimizations",
                "impact": "Faster performance issues; improved user experience; reduce customer complaints",
                "timeline": "2-4 weeks",
                "roi": "High",
                "complexity": "Low"
            },
        ]
    }
}


def get_opportunities_for_industry(industry: str) -> dict:
    """Get AI opportunities for an industry."""
    industry_lower = (industry or "").lower().strip()

    if industry_lower not in AI_OPPORTUNITIES_BY_INDUSTRY:
        # Default to retail if unknown
        industry_lower = "retail"

    return AI_OPPORTUNITIES_BY_INDUSTRY.get(industry_lower, {})


def get_top_use_cases(industry: str, limit: int = 6) -> list:
    """Get top use cases for an industry, sorted by ROI."""
    opportunities = get_opportunities_for_industry(industry)
    use_cases = opportunities.get("use_cases", [])

    # Sort by ROI priority
    roi_priority = {"Very High": 3, "High": 2, "Medium": 1, "Low": 0}
    sorted_cases = sorted(
        use_cases,
        key=lambda x: roi_priority.get(x.get("roi", "Medium"), 1),
        reverse=True
    )

    return sorted_cases[:limit]
