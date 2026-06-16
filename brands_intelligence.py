"""
Brand Intelligence Module — Phase 1: Brand Essentials
Fetches brand data from Wikipedia, Google Knowledge Graph, UPCItemDB, and SEC Edgar
Stores all data in Supabase
"""

import requests
import json
from datetime import datetime
import library as lib

# API Keys (from environment)
UPCITEMDB_API = "https://api.upcitemdb.com/prod/trial/lookup"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
GOOGLE_KG_API = "https://kgsearch.googleapis.com/v1/entities:search"
SEC_EDGAR_API = "https://data.sec.gov/api/xbrl"

def fetch_brand_from_wikipedia(brand_name):
    """Fetch brand info from Wikipedia"""
    try:
        params = {
            "action": "query",
            "format": "json",
            "titles": brand_name,
            "prop": "extracts",
            "explaintext": True,
            "redirects": 1,
            "exintro": True
        }
        r = requests.get(WIKIPEDIA_API, params=params, timeout=5)
        data = r.json()
        pages = data.get("query", {}).get("pages", {})

        for page_id, page in pages.items():
            if page_id != "-1":  # Found
                extract = page.get("extract", "").strip()
                if not extract:
                    return None
                # Get first 1-2 sentences
                sentences = extract.split(". ")
                description = ". ".join(sentences[:2]) if len(sentences) > 1 else extract[:300]

                return {
                    "source": "wikipedia",
                    "description": description,
                    "wikipedia_url": f"https://en.wikipedia.org/wiki/{page.get('title', brand_name).replace(' ', '_')}"
                }
    except Exception as e:
        print(f"[Wikipedia] Error fetching {brand_name}: {e}")

    return None

def fetch_brand_from_google_kg(brand_name, api_key):
    """Fallback: Fetch brand info from Google Knowledge Graph"""
    try:
        params = {
            "query": brand_name,
            "key": api_key,
            "limit": 1,
            "types": ["Organization", "Brand"]
        }
        r = requests.get(GOOGLE_KG_API, params=params, timeout=5)
        data = r.json()

        elements = data.get("itemListElement", [])
        if elements:
            entity = elements[0].get("result", {})
            return {
                "source": "google_knowledge_graph",
                "description": entity.get("description", ""),
                "detailed_description": entity.get("detailedDescription", {}).get("articleBody", ""),
                "image": entity.get("image", {}).get("url", ""),
                "url": entity.get("url", ""),
                "kg_data": entity
            }
    except Exception as e:
        print(f"[Google KG] Error fetching {brand_name}: {e}")

    return None

def fetch_brand_skus(brand_name):
    """Fetch SKUs from UPCItemDB"""
    try:
        params = {"brand": brand_name}
        r = requests.get(UPCITEMDB_API, params=params, timeout=10)
        data = r.json()

        skus = []
        for item in data.get("items", [])[:20]:  # Limit to 20 SKUs
            skus.append({
                "upc": item.get("upc"),
                "sku": item.get("sku", ""),
                "product_name": item.get("title", ""),
                "category": item.get("category", [{"name": "Unknown"}])[0].get("name"),
                "description": item.get("description", "")
            })

        return skus
    except Exception as e:
        print(f"[UPCItemDB] Error fetching SKUs for {brand_name}: {e}")

    return []

def fetch_brand_financials(brand_name, cik=None):
    """Fetch financials from SEC Edgar for public companies"""
    # For now, return mock data
    # Real implementation would query SEC Edgar API
    return {
        "status": "pending",
        "note": "SEC Edgar integration coming soon"
    }

def store_brand_in_supabase(brand_data):
    """Store brand data in Supabase"""
    try:
        # Insert or update brand
        result = lib._sb().table("brands").upsert({
            "name": brand_data.get("name"),
            "description": brand_data.get("description"),
            "wikipedia_url": brand_data.get("wikipedia_url"),
            "knowledge_graph_data": brand_data.get("knowledge_graph_data"),
            "source": brand_data.get("source")
        }).execute()

        brand_id = result.data[0].get("id") if result.data else None

        # Store SKUs
        if brand_id and brand_data.get("skus"):
            for sku in brand_data["skus"]:
                try:
                    lib._sb().table("brand_skus").insert({
                        "brand_id": brand_id,
                        "upc": sku.get("upc"),
                        "product_name": sku.get("product_name"),
                        "category": sku.get("category")
                    }).execute()
                except Exception as e:
                    print(f"[SKU] Error storing SKU: {e}")

        return brand_id, result.data[0] if result.data else None
    except Exception as e:
        print(f"[Supabase] Error storing brand: {e}")
        return None, None

def get_brand_from_supabase(brand_name):
    """Retrieve brand from Supabase if already cached"""
    try:
        result = lib._sb().table("brands").select("*").ilike("name", brand_name).limit(1).execute()
        if result.data:
            return result.data[0]
    except Exception as e:
        print(f"[Supabase] Error retrieving brand: {e}")

    return None

def search_and_store_brand(brand_name, google_kg_api_key=None):
    """
    Complete brand search flow:
    1. Check if brand exists in Supabase (cache)
    2. Try Wikipedia → Get basic info, history
    3. Fall back to Google Knowledge Graph
    4. Fetch SKUs from UPCItemDB
    5. Store in Supabase
    """

    # Check cache first
    cached_brand = get_brand_from_supabase(brand_name)
    if cached_brand:
        return {
            "name": cached_brand.get("name"),
            "description": cached_brand.get("description"),
            "wikipedia_url": cached_brand.get("wikipedia_url"),
            "skus": [],  # TODO: fetch linked SKUs
            "source": "cache"
        }

    # Step 1: Try Wikipedia first
    wiki_data = fetch_brand_from_wikipedia(brand_name)

    # Step 2: Fall back to Google KG if Wikipedia fails
    kg_data = None
    if not wiki_data and google_kg_api_key:
        kg_data = fetch_brand_from_google_kg(brand_name, google_kg_api_key)

    # Step 3: Fetch SKUs
    skus = fetch_brand_skus(brand_name)

    # Step 4: Prepare brand record
    brand_record = {
        "name": brand_name,
        "description": wiki_data.get("description") if wiki_data else kg_data.get("description") if kg_data else "",
        "wikipedia_url": wiki_data.get("wikipedia_url") if wiki_data else None,
        "knowledge_graph_data": kg_data if kg_data else None,
        "skus": skus,
        "source": "wikipedia" if wiki_data else "google_knowledge_graph" if kg_data else None
    }

    # Step 5: Store in Supabase
    brand_id, stored = store_brand_in_supabase(brand_record)

    return brand_record
