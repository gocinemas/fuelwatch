"""
Load essential company data into ai_cache table.
Run once on deployment to populate Intel with real data.
"""
import os
from supabase import create_client

def load_company_data():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        print("❌ SUPABASE credentials missing")
        return
    
    sb = create_client(url, key)
    
    companies = {
        "Unilever": {
            "name": "Unilever",
            "wiki": {"founded": "1929", "hq": "London, UK", "industry": "Consumer Goods", "revenue": "$64.5B", "employees": "130,000", "extract": "Unilever plc is a British multinational consumer goods company"},
            "news": [], "share": {"market_cap": "$108B"}, "job_signals": {}, "ai_signals": {}, "suggested_name": ""
        },
        "OpenAI": {
            "name": "OpenAI",
            "wiki": {"founded": "2015", "hq": "San Francisco, USA", "industry": "AI", "revenue": "$3.4B", "employees": "1,200", "extract": "OpenAI is an AI research company"},
            "news": [], "share": {"market_cap": "$150B"}, "job_signals": {"total": 25}, "ai_signals": {"ai_pct": 100}, "suggested_name": ""
        },
        "Google DeepMind": {
            "name": "Google DeepMind",
            "wiki": {"founded": "2010", "hq": "London/Mountain View", "industry": "AI Research", "revenue": "Part of Alphabet", "employees": "3,000", "extract": "DeepMind is an AI research lab"},
            "news": [], "share": {"market_cap": "$2.1T (Alphabet)"}, "job_signals": {"total": 150}, "ai_signals": {"ai_pct": 95}, "suggested_name": ""
        },
        "Meta": {
            "name": "Meta",
            "wiki": {"founded": "2004", "hq": "Menlo Park, USA", "industry": "Social Media/AI", "revenue": "$134.9B", "employees": "67,317", "extract": "Meta Platforms is a technology company"},
            "news": [], "share": {"market_cap": "$1.3T"}, "job_signals": {"total": 80}, "ai_signals": {"ai_pct": 65}, "suggested_name": ""
        }
    }
    
    for name, data in companies.items():
        key = f"company:{name.lower()}|v17"
        try:
            sb.table("ai_cache").upsert({"key": key, "data": data}).execute()
            print(f"✓ {name}")
        except Exception as e:
            print(f"✗ {name}: {str(e)[:50]}")

if __name__ == "__main__":
    load_company_data()
