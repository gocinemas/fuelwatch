"""
Company intelligence caching - store fetched data in DB for reuse.
When user views a company, cache everything so next view is instant.
"""

import os
import json
from datetime import datetime, timedelta
import library as lib

CACHE_DURATION_DAYS = 30

def get_cached_company(company_name: str) -> dict:
    """Get cached company data from DB, or None if not cached/stale"""
    try:
        sb = lib._sb()
        result = sb.table("company_intelligence_cache").select("*").eq(
            "company_name", company_name
        ).limit(1).execute()
        
        if result.data:
            cached = result.data[0]
            # Check if cache is stale
            if cached.get("cached_at"):
                cached_date = datetime.fromisoformat(cached["cached_at"])
                if (datetime.now() - cached_date).days < CACHE_DURATION_DAYS:
                    return json.loads(cached.get("data", "{}"))
        
        return None
    except Exception as e:
        print(f"[cache] Get failed: {e}")
        return None

def cache_company_data(company_name: str, data: dict) -> bool:
    """Store company data in cache"""
    try:
        sb = lib._sb()
        
        cache_record = {
            "company_name": company_name,
            "data": json.dumps(data),
            "cached_at": datetime.now().isoformat(),
            "views": 0,
            "last_viewed": datetime.now().isoformat()
        }
        
        sb.table("company_intelligence_cache").upsert(
            cache_record,
            on_conflict="company_name"
        ).execute()
        
        return True
    except Exception as e:
        print(f"[cache] Store failed: {e}")
        return False

def get_top_viewed_companies(limit: int = 10) -> list:
    """Get most viewed companies"""
    try:
        sb = lib._sb()
        result = sb.table("company_intelligence_cache").select("company_name,views").order(
            "views", desc=True
        ).limit(limit).execute()
        
        return [r["company_name"] for r in result.data or []]
    except Exception as e:
        print(f"[cache] Top viewed failed: {e}")
        return []

if __name__ == "__main__":
    # Test
    data = {"name": "Test Corp", "revenue": 100}
    cache_company_data("Test Corp", data)
    cached = get_cached_company("Test Corp")
    print(f"Cached: {cached}")
