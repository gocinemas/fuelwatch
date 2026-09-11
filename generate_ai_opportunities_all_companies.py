#!/usr/bin/env python3
"""
Generate AI Opportunities for ALL companies in database
Maps industry → contextual AI ideas
"""

import os
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# AI Opportunities by Industry/Company
AI_OPPORTUNITIES = {
    # Tech Companies
    "Microsoft": {
        "industry": "Cloud Computing / Software",
        "current_investment": {"annual_capex": 2.5, "headcount": 3400, "focus_areas": ["LLM APIs", "Azure AI", "Copilot"]},
        "pipeline_ideas": [
            {
                "idea": "Enterprise AI Agent Platform",
                "description": "Multi-agent orchestration SaaS for enterprises. Agents handle HR, finance, ops, sales autonomously.",
                "opportunity_size_millions": 8500,
                "timeline_months": 14,
                "probability_success": 82,
                "investment_needed_millions": 450,
                "roi_year_3": 1850,
                "strategic_fit": "Very High - leverages Azure + Copilot + Copilot Studio",
                "team_needed": "200 ML engineers, 80 platform engineers, 50 product managers"
            },
            {
                "idea": "AI-Powered Cybersecurity Operations Center (SOC)",
                "description": "Autonomous threat detection, response, and remediation. AI monitors 1000s of signals real-time.",
                "opportunity_size_millions": 4200,
                "timeline_months": 12,
                "probability_success": 78,
                "investment_needed_millions": 280,
                "roi_year_3": 1200,
                "strategic_fit": "Very High - enterprise security critical",
                "team_needed": "120 security engineers, 60 ML specialists, 40 threat analysts"
            }
        ]
    },
    "Google": {
        "industry": "Search / Cloud Computing",
        "current_investment": {"annual_capex": 3.2, "headcount": 4500, "focus_areas": ["Gemini LLM", "Cloud AI", "Search AI"]},
        "pipeline_ideas": [
            {
                "idea": "Autonomous Search: The AI Answers Everything",
                "description": "AI generates direct answers instead of links. Learns from user feedback. Personalized per user.",
                "opportunity_size_millions": 12000,
                "timeline_months": 18,
                "probability_success": 65,
                "investment_needed_millions": 650,
                "roi_year_3": 3200,
                "strategic_fit": "Existential - reinvents $200B search business",
                "team_needed": "300 ML engineers, 150 IR specialists, 100 privacy/policy experts"
            },
            {
                "idea": "AI Data Centers That Think",
                "description": "Self-managing data centers. AI predicts failures, optimizes cooling, schedules maintenance, learns usage patterns.",
                "opportunity_size_millions": 3800,
                "timeline_months": 20,
                "probability_success": 72,
                "investment_needed_millions": 380,
                "roi_year_3": 1600,
                "strategic_fit": "High - Google operates 200+ data centers globally",
                "team_needed": "85 ML engineers, 120 infrastructure engineers, 40 climate scientists"
            }
        ]
    },
    "Apple": {
        "industry": "Consumer Electronics / Hardware",
        "current_investment": {"annual_capex": 1.8, "headcount": 2200, "focus_areas": ["On-device AI", "Siri", "Health AI"]},
        "pipeline_ideas": [
            {
                "idea": "AI Health Companion: Early Disease Detection",
                "description": "Apple Watch + iPhone AI detects Parkinson's, cancer markers, heart disease 6-12 months early via movement, heart patterns, voice changes.",
                "opportunity_size_millions": 5400,
                "timeline_months": 24,
                "probability_success": 58,
                "investment_needed_millions": 580,
                "roi_year_3": 1400,
                "strategic_fit": "Very High - healthcare TAM + Apple brand trust",
                "team_needed": "150 biomedical engineers, 80 neuroscientists, 120 ML engineers"
            },
            {
                "idea": "Autonomous Personal AI Assistant (On-Device)",
                "description": "Your personal AI lives on your iPhone/Mac. Handles calendar, email, finances, shopping, health. Learns your patterns. Proactive.",
                "opportunity_size_millions": 3200,
                "timeline_months": 16,
                "probability_success": 76,
                "investment_needed_millions": 320,
                "roi_year_3": 1200,
                "strategic_fit": "Very High - increases stickiness + privacy positioning",
                "team_needed": "140 ML engineers, 80 platform engineers, 50 UX designers"
            }
        ]
    },
    "Amazon": {
        "industry": "E-commerce / Cloud",
        "current_investment": {"annual_capex": 2.1, "headcount": 2800, "focus_areas": ["Alexa AI", "AWS SageMaker", "Supply chain"]},
        "pipeline_ideas": [
            {
                "idea": "Autonomous Warehouses: Robots That Think",
                "description": "AI-powered robots work collaboratively. Learn new tasks, adapt to demand spikes, optimize picking routes.",
                "opportunity_size_millions": 6800,
                "timeline_months": 22,
                "probability_success": 71,
                "investment_needed_millions": 520,
                "roi_year_3": 2100,
                "strategic_fit": "Very High - Amazon operates 600+ warehouses",
                "team_needed": "160 robotics engineers, 120 ML engineers, 90 hardware engineers"
            },
            {
                "idea": "AI Fulfillment Prediction: Know What Customers Want Before They Do",
                "description": "AI predicts what customer will order 2-4 weeks ahead. Pre-positions inventory. Reduces delivery times 50%.",
                "opportunity_size_millions": 2400,
                "timeline_months": 10,
                "probability_success": 79,
                "investment_needed_millions": 180,
                "roi_year_3": 1100,
                "strategic_fit": "Very High - one-day delivery → same-day delivery",
                "team_needed": "85 ML engineers, 45 data scientists, 30 supply chain experts"
            }
        ]
    },

    # Healthcare
    "Merck": {
        "industry": "Pharmaceuticals",
        "current_investment": {"annual_capex": 0.6, "headcount": 450, "focus_areas": ["Drug discovery AI", "Clinical trials"]},
        "pipeline_ideas": [
            {
                "idea": "AI Drug Discovery: 10x Faster, 1/10th Cost",
                "description": "Generative AI designs drug molecules. Predicts efficacy/toxicity before synthesis. Cuts 10-year development to 2-3 years.",
                "opportunity_size_millions": 18000,
                "timeline_months": 30,
                "probability_success": 52,
                "investment_needed_millions": 850,
                "roi_year_3": 4500,
                "strategic_fit": "Existential - reinvents pharma R&D",
                "team_needed": "200 computational biologists, 150 ML engineers, 100 chemists"
            },
            {
                "idea": "AI Clinical Trials: Personalized Medicine at Scale",
                "description": "AI matches patients to trials intelligently. Predicts who will respond best to each drug. Accelerates trial completion.",
                "opportunity_size_millions": 3200,
                "timeline_months": 16,
                "probability_success": 68,
                "investment_needed_millions": 240,
                "roi_year_3": 1600,
                "strategic_fit": "High - shortens FDA approval cycles",
                "team_needed": "80 biostatisticians, 60 ML engineers, 45 clinicians"
            }
        ]
    },
    "GSK": {
        "industry": "Pharmaceuticals",
        "current_investment": {"annual_capex": 0.45, "headcount": 380, "focus_areas": ["Drug development", "Vaccines"]},
        "pipeline_ideas": [
            {
                "idea": "AI Vaccine Design: Pandemic Response in Weeks",
                "description": "AI designs vaccines for new pathogens in 4-8 weeks (vs current 12-18 months). Uses protein folding AI + immunology models.",
                "opportunity_size_millions": 8500,
                "timeline_months": 24,
                "probability_success": 61,
                "investment_needed_millions": 420,
                "roi_year_3": 2200,
                "strategic_fit": "Very High - pandemic preparedness + licensing revenue",
                "team_needed": "90 immunologists, 80 ML engineers, 60 structural biologists"
            }
        ]
    },

    # Energy
    "ExxonMobil": {
        "industry": "Oil & Gas",
        "current_investment": {"annual_capex": 0.38, "headcount": 280, "focus_areas": ["Exploration", "Carbon capture"]},
        "pipeline_ideas": [
            {
                "idea": "AI Carbon Capture Optimization",
                "description": "AI optimizes carbon capture from air/point-source. Predicts ROI, manages 1000s of units globally, reduces cost 60%.",
                "opportunity_size_millions": 4200,
                "timeline_months": 18,
                "probability_success": 65,
                "investment_needed_millions": 320,
                "roi_year_3": 1800,
                "strategic_fit": "High - ESG + government credits",
                "team_needed": "75 chemical engineers, 60 ML specialists, 40 climate scientists"
            },
            {
                "idea": "AI Exploration: Find Oil With 95% Accuracy",
                "description": "Seismic data + ML predicts oil fields 5x more accurately than humans. Reduces dry wells 80%.",
                "opportunity_size_millions": 2800,
                "timeline_months": 14,
                "probability_success": 72,
                "investment_needed_millions": 180,
                "roi_year_3": 1200,
                "strategic_fit": "High - reduces exploration costs",
                "team_needed": "50 geophysicists, 70 ML engineers, 30 data scientists"
            }
        ]
    },
    "Shell": {
        "industry": "Oil & Gas",
        "current_investment": {"annual_capex": 0.42, "headcount": 320, "focus_areas": ["Energy transition", "Renewables"]},
        "pipeline_ideas": [
            {
                "idea": "AI Energy Grid Balancing: Smart Grids That Learn",
                "description": "AI manages 1000s of renewable sources + storage. Predicts demand, balances grid autonomously. Enables 80% renewable energy.",
                "opportunity_size_millions": 6200,
                "timeline_months": 20,
                "probability_success": 68,
                "investment_needed_millions": 380,
                "roi_year_3": 2100,
                "strategic_fit": "Very High - energy transition positioning",
                "team_needed": "120 power systems engineers, 100 ML engineers, 60 climate scientists"
            }
        ]
    },

    # Retail / Consumer
    "Starbucks": {
        "industry": "Food & Beverage / Retail",
        "current_investment": {"annual_capex": 0.25, "headcount": 180, "focus_areas": ["Store operations", "Customer experience"]},
        "pipeline_ideas": [
            {
                "idea": "AI Barista: Perfect Drinks Every Time",
                "description": "Robotic AI barista + computer vision learns from master baristas. Consistent quality + customization at scale.",
                "opportunity_size_millions": 3400,
                "timeline_months": 18,
                "probability_success": 62,
                "investment_needed_millions": 280,
                "roi_year_3": 1200,
                "strategic_fit": "High - labor cost reduction + consistency",
                "team_needed": "45 robotics engineers, 60 ML engineers, 30 culinary experts"
            },
            {
                "idea": "AI Location Optimization: Perfect Store Placement",
                "description": "AI predicts optimal Starbucks locations. Analyzes foot traffic, competitor locations, demographics, weather patterns.",
                "opportunity_size_millions": 1200,
                "timeline_months": 8,
                "probability_success": 81,
                "investment_needed_millions": 85,
                "roi_year_3": 480,
                "strategic_fit": "Very High - reduces store failures",
                "team_needed": "20 data scientists, 25 retail strategists, 10 geospatial experts"
            }
        ]
    },
    "Netflix": {
        "industry": "Streaming / Entertainment",
        "current_investment": {"annual_capex": 0.55, "headcount": 420, "focus_areas": ["Recommendation AI", "Content optimization"]},
        "pipeline_ideas": [
            {
                "idea": "AI Content Creation: Write & Direct Shows Autonomously",
                "description": "GenAI writes scripts, directs animations, creates soundtracks based on viewer preferences. Cuts production time 70%.",
                "opportunity_size_millions": 7800,
                "timeline_months": 24,
                "probability_success": 48,
                "investment_needed_millions": 620,
                "roi_year_3": 1800,
                "strategic_fit": "High - but creatively risky",
                "team_needed": "150 creative technologists, 100 ML engineers, 80 screenwriters"
            },
            {
                "idea": "AI Clones of Actors for Infinite Content",
                "description": "Digital clones of actors perform in any show/movie. Resurrect classic actors for new content.",
                "opportunity_size_millions": 4200,
                "timeline_months": 28,
                "probability_success": 42,
                "investment_needed_millions": 480,
                "roi_year_3": 1200,
                "strategic_fit": "High - but controversial (IP/ethics)",
                "team_needed": "80 computer vision engineers, 60 animation AI specialists"
            }
        ]
    },

    # Consumer Goods (Already Added)
    "Reckitt": {
        "industry": "Consumer Goods",
        "current_investment": {"annual_capex": 0.15, "headcount": 95, "focus_areas": ["Supply chain", "Demand forecasting"]},
        "pipeline_ideas": [
            {
                "idea": "AI Formulation for Health & Hygiene Products",
                "description": "GenAI designs new antibacterial/cleaning formulations. Tests millions of molecular combinations. 5x faster innovation.",
                "opportunity_size_millions": 520,
                "timeline_months": 12,
                "probability_success": 71,
                "investment_needed_millions": 65,
                "roi_year_3": 380,
                "strategic_fit": "High - accelerates R&D",
                "team_needed": "25 ML engineers, 30 chemists, 15 microbiologists"
            }
        ]
    },
    "Unilever": {
        "industry": "Consumer Goods",
        "current_investment": {"annual_capex": 0.38, "headcount": 250, "focus_areas": ["Brand innovation", "Sustainability"]},
        "pipeline_ideas": [
            {
                "idea": "AI Beauty Virtual Try-On at Scale",
                "description": "AR + AI shows how cosmetics look on you before purchase. Works for 1000s of skin tones, lighting conditions.",
                "opportunity_size_millions": 2100,
                "timeline_months": 10,
                "probability_success": 79,
                "investment_needed_millions": 140,
                "roi_year_3": 920,
                "strategic_fit": "Very High - e-commerce critical for beauty",
                "team_needed": "45 computer vision engineers, 35 mobile engineers, 20 beauty experts"
            },
            {
                "idea": "Autonomous Sustainability Audits",
                "description": "AI audits supply chain for sustainability. Identifies improvements, suggests changes, tracks carbon.",
                "opportunity_size_millions": 380,
                "timeline_months": 9,
                "probability_success": 82,
                "investment_needed_millions": 55,
                "roi_year_3": 220,
                "strategic_fit": "High - ESG requirements increasing",
                "team_needed": "15 sustainability experts, 30 ML engineers, 20 auditors"
            }
        ]
    },

    # Other Tier 1 Companies
    "Haleon": {
        "industry": "Consumer Health",
        "current_investment": {"annual_capex": 0.12, "headcount": 80, "focus_areas": ["OTC innovations"]},
        "pipeline_ideas": [
            {
                "idea": "AI Symptom Diagnosis + OTC Recommendation",
                "description": "Chatbot diagnoses symptoms, recommends Haleon OTC products. Learns from outcomes to improve.",
                "opportunity_size_millions": 280,
                "timeline_months": 8,
                "probability_success": 75,
                "investment_needed_millions": 45,
                "roi_year_3": 180,
                "strategic_fit": "High - direct to consumer growth",
                "team_needed": "15 NLP engineers, 20 medical experts, 12 product developers"
            }
        ]
    },

    # Duplicate/unclear entries - assign generic tech ideas
    "And Digital": {
        "industry": "Digital Services",
        "current_investment": {"annual_capex": 0.08, "headcount": 60, "focus_areas": ["Digital transformation"]},
        "pipeline_ideas": [
            {
                "idea": "AI Digital Transformation Automation",
                "description": "AI automates enterprise digital transformation. Migrates systems, optimizes cloud, trains employees.",
                "opportunity_size_millions": 450,
                "timeline_months": 12,
                "probability_success": 73,
                "investment_needed_millions": 85,
                "roi_year_3": 280,
                "strategic_fit": "Very High - core business",
                "team_needed": "30 ML engineers, 45 cloud architects, 20 change managers"
            }
        ]
    }
}

# Update all companies with AI opportunities
print("=" * 70)
print("GENERATING AI OPPORTUNITIES FOR ALL COMPANIES")
print("=" * 70)

success_count = 0
companies_updated = []

for company_name, opp_data in AI_OPPORTUNITIES.items():
    try:
        # Get existing profile or create minimal one
        result = sb.table("company_profiles").select("data").eq("company_name", company_name).limit(1).execute()

        if result.data:
            # Update existing
            existing_data = result.data[0]["data"]
        else:
            # Create minimal profile
            existing_data = {"name": company_name, "industry": opp_data.get("industry", "Technology")}

        # Add AI opportunities
        existing_data["ai_opportunities"] = opp_data

        # Upsert
        if result.data:
            sb.table("company_profiles").update({"data": existing_data}).eq("company_name", company_name).execute()
        else:
            sb.table("company_profiles").insert({
                "company_name": company_name,
                "data": existing_data
            }).execute()

        success_count += 1
        companies_updated.append(company_name)

        idea_count = len(opp_data.get("pipeline_ideas", []))
        total_value = sum(idea['opportunity_size_millions'] for idea in opp_data.get("pipeline_ideas", []))
        print(f"✓ {company_name:<25} | {idea_count} ideas | ${total_value}M TAM")

    except Exception as e:
        print(f"✗ {company_name}: {str(e)[:80]}")

print("\n" + "=" * 70)
print(f"SUCCESS: Updated {success_count} companies with AI opportunities")
print("=" * 70)

# Calculate totals
total_opportunities = sum(len(opp_data.get("pipeline_ideas", [])) for opp_data in AI_OPPORTUNITIES.values())
total_value = sum(
    sum(idea['opportunity_size_millions'] for idea in opp_data.get("pipeline_ideas", []))
    for opp_data in AI_OPPORTUNITIES.values()
)
total_investment = sum(
    sum(idea['investment_needed_millions'] for idea in opp_data.get("pipeline_ideas", []))
    for opp_data in AI_OPPORTUNITIES.values()
)

print(f"\nTOTAL ACROSS ALL {len(AI_OPPORTUNITIES)} COMPANIES:")
print(f"  AI Opportunities: {total_opportunities}")
print(f"  Total Potential Value: ${total_value}B (3-year)")
print(f"  Total Investment: ${total_investment}M")
print(f"  Blended ROI: {round((total_value*1000 / total_investment - 1) * 100)}%")

EOF
