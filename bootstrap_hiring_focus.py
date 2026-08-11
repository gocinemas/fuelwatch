"""
Bootstrap hiring focus areas and AI investment scores for Intel companies.
Run after: python bootstrap_100_companies.py
"""

from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")

# Hiring focus data by company
HIRING_FOCUS = {
    "Reckitt": {
        "hiring_growth_2025": 12.0,
        "ai_score": 4,
        "focus_areas": [
            {"area": "AI/ML Engineering", "growth": 40, "roles": 24, "reason": "Supply chain automation, personalization"},
            {"area": "APAC Expansion", "growth": 35, "roles": 67, "reason": "M&A in SE Asia, emerging brands"},
            {"area": "Direct-to-Consumer", "growth": 28, "roles": 19, "reason": "E-commerce growth, higher margins"},
            {"area": "R&D/Biotech", "growth": 15, "roles": 12, "reason": "Premium product innovation"},
        ],
        "strategic_direction": "Building AI-driven efficiency + geographic expansion into high-growth emerging markets"
    },
    "Unilever": {
        "hiring_growth_2025": 5.2,
        "ai_score": 3,
        "focus_areas": [
            {"area": "Direct-to-Consumer", "growth": 22, "roles": 45, "reason": "Digital transformation, DTC brands"},
            {"area": "India Expansion", "growth": 18, "roles": 35, "reason": "Highest growth market, Indigo Brands"},
            {"area": "Sustainability", "growth": 12, "roles": 28, "reason": "ESG commitments, green supply chain"},
            {"area": "AI/ML", "growth": 20, "roles": 18, "reason": "Marketing automation, supply chain"},
        ],
        "strategic_direction": "Balanced growth: DTC channels + emerging markets + sustainability focus"
    },
    "Henkel": {
        "hiring_growth_2025": 2.1,
        "ai_score": 2,
        "focus_areas": [
            {"area": "R&D/Innovation", "growth": 8, "roles": 22, "reason": "Premium adhesives, specialty chemicals"},
            {"area": "Europe Consolidation", "growth": 1, "roles": 15, "reason": "Efficiency gains, cost optimization"},
            {"area": "Operations", "growth": -5, "roles": 8, "reason": "Automation, headcount reduction"},
            {"area": "Sustainability", "growth": 5, "roles": 10, "reason": "Carbon neutral targets"},
        ],
        "strategic_direction": "Mature company in harvest mode: focus on efficiency and premium segments"
    },
    "Procter & Gamble": {
        "hiring_growth_2025": 8.7,
        "ai_score": 5,
        "focus_areas": [
            {"area": "AI/ML Engineering", "growth": 45, "roles": 78, "reason": "Personalization, supply chain AI"},
            {"area": "Premium Brands", "growth": 24, "roles": 56, "reason": "Prestige beauty, luxury acquisitions"},
            {"area": "Cloud/Tech", "growth": 35, "roles": 42, "reason": "AWS migration, cloud infrastructure"},
            {"area": "Data Science", "growth": 38, "roles": 34, "reason": "Consumer insights, predictive analytics"},
        ],
        "strategic_direction": "Tech transformation: Heavy AI investment, cloud-first, premium brand portfolio"
    },
    "SC Johnson": {
        "hiring_growth_2025": -1.3,
        "ai_score": 1,
        "focus_areas": [
            {"area": "Operations", "growth": -8, "roles": 10, "reason": "Automation, cost reduction"},
            {"area": "Sustainability", "growth": 2, "roles": 5, "reason": "Plastic reduction initiatives"},
            {"area": "Emerging Markets", "growth": 5, "roles": 8, "reason": "Limited expansion into India, Brazil"},
        ],
        "strategic_direction": "Cost-focused: Shrinking workforce, limited growth investments"
    },
    "Pfizer": {
        "hiring_growth_2025": 9.2,
        "ai_score": 4,
        "focus_areas": [
            {"area": "AI/ML Research", "growth": 38, "roles": 52, "reason": "Drug discovery, clinical trials AI"},
            {"area": "Oncology", "growth": 16, "roles": 28, "reason": "High-margin cancer drugs"},
            {"area": "Gene Therapy", "growth": 22, "roles": 35, "reason": "Breakthrough treatments"},
            {"area": "Data Science", "growth": 28, "roles": 24, "reason": "Real-world evidence, RWE analysis"},
        ],
        "strategic_direction": "Innovation-led: Heavy R&D, AI-powered drug discovery, premium oncology focus"
    },
    "Moderna": {
        "hiring_growth_2025": 14.5,
        "ai_score": 4,
        "focus_areas": [
            {"area": "mRNA Science", "growth": 25, "roles": 45, "reason": "Vaccine pipeline expansion"},
            {"area": "Manufacturing", "growth": 18, "roles": 35, "reason": "Scale-up for multiple programs"},
            {"area": "AI/ML", "growth": 32, "roles": 28, "reason": "Sequence design, target validation"},
            {"area": "Clinical Ops", "growth": 20, "roles": 22, "reason": "Trial expansion, data analysis"},
        ],
        "strategic_direction": "Aggressive growth: Scaling manufacturing, pipeline expansion, AI-driven R&D"
    },
    "Apple": {
        "hiring_growth_2025": 6.3,
        "ai_score": 5,
        "focus_areas": [
            {"area": "AI/ML Engineering", "growth": 52, "roles": 89, "reason": "On-device AI, machine learning ops"},
            {"area": "Services", "growth": 18, "roles": 42, "reason": "Recurring revenue, software platform"},
            {"area": "Custom Silicon", "growth": 35, "roles": 67, "reason": "ARM-based processors, chip design"},
            {"area": "Privacy/Security", "growth": 28, "roles": 45, "reason": "Encryption, security engineering"},
        ],
        "strategic_direction": "Platform shift: Heavy AI/ML investment, vertical integration, software services"
    },
    "Microsoft": {
        "hiring_growth_2025": 11.8,
        "ai_score": 5,
        "focus_areas": [
            {"area": "AI/ML Research", "growth": 48, "roles": 124, "reason": "Foundation models, Azure AI"},
            {"area": "Cloud Infrastructure", "growth": 22, "roles": 78, "reason": "Data centers, edge computing"},
            {"area": "Gaming/Xbox", "growth": 8, "roles": 34, "reason": "Game Pass, Activision integration"},
            {"area": "Enterprise AI", "growth": 42, "roles": 65, "reason": "Copilot for every product"},
        ],
        "strategic_direction": "AI-first company: Massive R&D spend, cloud platform expansion, AI copilot everywhere"
    },
    "Google": {
        "hiring_growth_2025": 7.5,
        "ai_score": 5,
        "focus_areas": [
            {"area": "AI/ML Research", "growth": 44, "roles": 118, "reason": "Gemini, foundation models, search AI"},
            {"area": "Cloud Platform", "growth": 26, "roles": 65, "reason": "Competing with AWS/Azure"},
            {"area": "Quantum Computing", "growth": 15, "roles": 28, "reason": "Quantum chip development"},
            {"area": "Assistant Products", "growth": 38, "roles": 74, "reason": "AI assistant platform"},
        ],
        "strategic_direction": "AI core business: Massive foundation model investment, cloud growth, AI-first products"
    },
}


def bootstrap_hiring_focus():
    """Insert hiring focus data into Supabase."""
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    for company_name, hiring_data in HIRING_FOCUS.items():
        print(f"\n📊 Adding hiring focus for {company_name}...")

        # Insert into company_hiring_focus table
        try:
            result = sb.table("company_hiring_focus").insert({
                "company_name": company_name,
                "hiring_growth_2025": hiring_data["hiring_growth_2025"],
                "ai_investment_score": hiring_data["ai_score"],
                "strategic_direction": hiring_data["strategic_direction"],
                "focus_areas": hiring_data["focus_areas"],
                "last_updated": "2026-08-11"
            }).execute()

            if result.data:
                print(f"✅ {company_name}: {hiring_data['hiring_growth_2025']}% hiring growth, AI score {hiring_data['ai_score']}/5")
            else:
                print(f"⚠️  {company_name}: Insert returned no data")

        except Exception as e:
            print(f"❌ {company_name}: {e}")

    print("\n✅ Hiring focus bootstrap complete!")


if __name__ == "__main__":
    bootstrap_hiring_focus()
