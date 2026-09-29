"""
Complete company enrichment - adds brands, divisions, competitors, strategy.
Uses AI to generate data from company Wikipedia extract.
"""

import os
import json
from groq import Groq

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

def enrich_company_complete(company_name: str, wiki_extract: str = None) -> dict:
    """
    Generate complete company intelligence using AI:
    - Brands & products
    - Business divisions
    - Competitors
    - Strategic focus
    - Use cases
    """
    if not GROQ_API_KEY or not wiki_extract:
        return _get_fallback_enrichment(company_name)
    
    try:
        client = Groq(api_key=GROQ_API_KEY)
        
        prompt = f"""For {company_name}, analyze and extract/generate:

Text: {wiki_extract[:2000]}

Return JSON:
{{
  "brands": ["brand1", "brand2", "brand3"],
  "divisions": ["division1", "division2"],
  "competitors": ["competitor1", "competitor2", "competitor3"],
  "strategic_focus": ["focus1", "focus2", "focus3"],
  "use_cases": [
    {{"title": "Use Case 1", "description": "...", "impact": "High"}},
    {{"title": "Use Case 2", "description": "...", "impact": "Medium"}}
  ]
}}

Focus on realistic, factual data from the text."""
        
        response = client.messages.create(
            model="mixtral-8x7b-32768",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1000
        )
        
        try:
            result = json.loads(response.choices[0].message.content)
            return result
        except:
            return _get_fallback_enrichment(company_name)
    except Exception as e:
        print(f"[enricher] AI enrichment failed: {e}")
        return _get_fallback_enrichment(company_name)

def _get_fallback_enrichment(company_name: str) -> dict:
    """Fallback enrichment data for known companies"""
    data = {
        "unilever": {
            "brands": ["Dove", "Axe", "Knorr", "Hellmann's", "Ben & Jerry's", "Magnum", "Lipton", "Vaseline"],
            "divisions": ["Beauty & Personal Care", "Foods & Refreshment", "Homecare"],
            "competitors": ["Procter & Gamble", "Reckitt", "Henkel", "Nestlé"],
            "strategic_focus": ["Sustainability", "Digital transformation", "Emerging markets", "Health & wellness"],
            "use_cases": [
                {"title": "Personalized Beauty AI", "description": "AI recommends products based on skin type, climate", "impact": "High"},
                {"title": "Supply Chain Optimization", "description": "ML predicts demand, optimizes logistics", "impact": "High"},
                {"title": "Sustainability Tracking", "description": "Blockchain tracks carbon footprint end-to-end", "impact": "Medium"},
                {"title": "Consumer Insights AI", "description": "NLP analyzes social media for trends", "impact": "High"}
            ]
        },
        "apple": {
            "brands": ["iPhone", "Mac", "iPad", "Apple Watch", "AirPods", "Apple TV"],
            "divisions": ["Hardware", "Services", "Software"],
            "competitors": ["Microsoft", "Google", "Samsung", "Meta"],
            "strategic_focus": ["Services growth", "Privacy", "Sustainability", "AI integration"],
            "use_cases": [
                {"title": "On-device AI", "description": "Run AI models locally for privacy", "impact": "High"},
                {"title": "Health monitoring", "description": "Apple Watch tracks health metrics", "impact": "High"},
                {"title": "Ecosystem integration", "description": "Seamless device connectivity", "impact": "High"}
            ]
        },
        "microsoft": {
            "brands": ["Windows", "Office 365", "Azure", "Teams", "Xbox", "GitHub Copilot"],
            "divisions": ["Productivity & Business Processes", "Intelligent Cloud", "More Personal Computing"],
            "competitors": ["Google", "Amazon", "Apple"],
            "strategic_focus": ["AI/LLMs", "Cloud infrastructure", "Enterprise security"],
            "use_cases": [
                {"title": "GitHub Copilot", "description": "AI-powered code generation", "impact": "High"},
                {"title": "Azure AI Services", "description": "Enterprise AI infrastructure", "impact": "High"},
                {"title": "Teams AI Assistant", "description": "AI meeting transcription and analysis", "impact": "Medium"}
            ]
        },
    }
    
    normalized = company_name.lower().strip()
    return data.get(normalized, {
        "brands": [],
        "divisions": [],
        "competitors": [],
        "strategic_focus": [],
        "use_cases": []
    })

if __name__ == "__main__":
    result = enrich_company_complete("Unilever", "Unilever is a multinational...")
    print(json.dumps(result, indent=2))
