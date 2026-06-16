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
        headers = {
            "User-Agent": "MiruIntel/1.0 (brand intelligence; +https://miru.humanagency.co)"
        }
        params = {
            "action": "query",
            "format": "json",
            "titles": brand_name,
            "prop": "extracts",
            "explaintext": True,
            "redirects": 1,
            "exintro": True
        }
        r = requests.get(WIKIPEDIA_API, params=params, headers=headers, timeout=5)
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

def fetch_brand_from_wikidata(brand_name):
    """Fallback: Fetch brand info from Wikidata (structured Wikipedia)"""
    try:
        headers = {
            "User-Agent": "MiruIntel/1.0 (brand intelligence; +https://miru.humanagency.co)"
        }
        # Search for the brand in Wikidata
        params = {
            "action": "wbsearchentities",
            "search": brand_name,
            "language": "en",
            "format": "json",
            "type": "item"
        }
        r = requests.get("https://www.wikidata.org/w/api.php", params=params, headers=headers, timeout=5)
        data = r.json()

        search_results = data.get("search", [])
        if search_results:
            entity_id = search_results[0].get("id")
            # Get detailed entity info
            entity_params = {
                "action": "wbgetentities",
                "ids": entity_id,
                "format": "json",
                "props": "labels|descriptions"
            }
            entity_r = requests.get("https://www.wikidata.org/w/api.php", params=entity_params, headers=headers, timeout=5)
            entity_data = entity_r.json()

            entities = entity_data.get("entities", {})
            if entities:
                entity = list(entities.values())[0]
                description = entity.get("descriptions", {}).get("en", {}).get("value", "")
                label = entity.get("labels", {}).get("en", {}).get("value", brand_name)

                if description:
                    return {
                        "source": "wikidata",
                        "description": description,
                        "label": label,
                        "wikidata_id": entity_id
                    }
    except Exception as e:
        print(f"[Wikidata] Error fetching {brand_name}: {e}")

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
    """Fetch product SKUs — currently returning empty, will integrate with product APIs"""
    # TODO: Integrate with OpenFoodFacts, Shopify product search, or Amazon product search
    # For now, return empty to avoid failed API calls
    return []

def fetch_brand_ranking(brand_name):
    """Get brand's market ranking and category"""
    rankings = {
        "tesla": {"category": "Electric Vehicles", "rank": 2, "rank_of": 50, "market_cap": 850.0},
        "nike": {"category": "Athletic Apparel", "rank": 1, "rank_of": 100, "market_cap": 180.0},
        "apple": {"category": "Technology", "rank": 1, "rank_of": 500, "market_cap": 3200.0},
        "coca-cola": {"category": "Beverages", "rank": 1, "rank_of": 200, "market_cap": 280.0},
        "adidas": {"category": "Athletic Apparel", "rank": 2, "rank_of": 100, "market_cap": 64.0},
        "samsung": {"category": "Electronics", "rank": 2, "rank_of": 300, "market_cap": 420.0},
        "volkswagen": {"category": "Automotive", "rank": 3, "rank_of": 150, "market_cap": 85.0},
        "bmw": {"category": "Luxury Automotive", "rank": 2, "rank_of": 80, "market_cap": 63.0},
    }

    key = brand_name.lower()
    return rankings.get(key, None)

def fetch_brand_competitors(brand_name):
    """Fetch competitor brands for a given brand"""
    # Hardcoded competitor data by brand/category
    # TODO: Replace with dynamic market data from Crunchbase or similar
    competitors_map = {
        "tesla": [
            {"name": "Ford", "market_cap": 37.5, "market_share": 12.5},
            {"name": "General Motors", "market_cap": 40.2, "market_share": 14.2},
            {"name": "Volkswagen", "market_cap": 85.3, "market_share": 18.5},
            {"name": "BMW", "market_cap": 63.4, "market_share": 8.2},
            {"name": "Lucid Motors", "market_cap": 2.1, "market_share": 0.3},
        ],
        "nike": [
            {"name": "Adidas", "market_cap": 64.2, "market_share": 16.5},
            {"name": "Puma", "market_cap": 18.5, "market_share": 5.2},
            {"name": "Skechers", "market_cap": 8.3, "market_share": 2.1},
            {"name": "New Balance", "market_cap": 5.0, "market_share": 1.8},
        ],
        "coca-cola": [
            {"name": "PepsiCo", "market_cap": 238.5, "market_share": 25.2},
            {"name": "Keurig Dr Pepper", "market_cap": 35.2, "market_share": 8.5},
            {"name": "Monster Beverage", "market_cap": 54.3, "market_share": 6.2},
        ],
        "apple": [
            {"name": "Microsoft", "market_cap": 3450.0, "market_share": 15.2},
            {"name": "Samsung", "market_cap": 420.0, "market_share": 18.5},
            {"name": "Google", "market_cap": 2100.0, "market_share": 12.3},
            {"name": "Meta", "market_cap": 650.0, "market_share": 5.2},
        ],
    }

    key = brand_name.lower()
    return competitors_map.get(key, [])

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

def search_and_store_brand(brand_name, google_kg_api_key=None, force_refresh=False):
    """
    Complete brand search flow:
    1. Check if brand exists in Supabase (cache) — skip if force_refresh=True
    2. Try Wikipedia → Get basic info, history
    3. Fall back to Wikidata → Structured data
    4. Fall back to Google Knowledge Graph
    5. Fetch SKUs from UPCItemDB
    6. Store in Supabase
    """

    # Check cache first (unless force_refresh is True)
    if not force_refresh:
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

    # Step 2: Fall back to Wikidata if Wikipedia fails
    wikidata = None
    if not wiki_data:
        wikidata = fetch_brand_from_wikidata(brand_name)

    # Step 3: Fall back to Google KG if both Wikipedia and Wikidata fail
    kg_data = None
    if not wiki_data and not wikidata and google_kg_api_key:
        kg_data = fetch_brand_from_google_kg(brand_name, google_kg_api_key)

    # Step 3: Fetch SKUs
    skus = fetch_brand_skus(brand_name)

    # Step 3b: Fetch competitors
    competitors = fetch_brand_competitors(brand_name)

    # Step 3c: Fetch market ranking
    ranking = fetch_brand_ranking(brand_name)

    # Step 4: Prepare brand record
    description = ""
    source = None
    wikipedia_url = None

    if wiki_data:
        description = wiki_data.get("description")
        source = "wikipedia"
        wikipedia_url = wiki_data.get("wikipedia_url")
    elif wikidata:
        description = wikidata.get("description")
        source = "wikidata"
    elif kg_data:
        description = kg_data.get("description")
        source = "google_knowledge_graph"

    brand_record = {
        "name": brand_name,
        "description": description,
        "wikipedia_url": wikipedia_url,
        "knowledge_graph_data": kg_data if kg_data else None,
        "skus": skus,
        "competitors": competitors,
        "ranking": ranking,
        "source": source
    }

    # Step 5: Store in Supabase
    brand_id, stored = store_brand_in_supabase(brand_record)

    return brand_record
