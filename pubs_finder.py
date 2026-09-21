#!/usr/bin/env python3
"""
Pubs Finder — Query unified Supabase pubs database (FHRS + OSM + confidence tiers)
"""

import os
import json
import time
import requests
from typing import List, Dict, Any
from search import postcode_to_latlon, haversine_km
import library as lib

# Cache: {postcode: (data, timestamp)}
_PUBS_CACHE = {}
_PUBS_CACHE_TTL = 3600  # 1 hour


def _enrich_osm_data_for_area(lat: float, lon: float, radius_km: float = 15) -> Dict[str, Dict]:
    """
    Fetch detailed OSM pub data (phone, website, opening_hours) for a geographic area.
    Returns dict: {(lat,lon): {phone, website, opening_hours, osm_id}, ...}
    Keyed by coordinates for proximity-based matching in database.
    """
    try:
        # Create bbox for area (convert km to degrees: 1 degree ≈ 111 km)
        delta = (radius_km / 111.0)
        bbox = f"{lat - delta},{lon - delta},{lat + delta},{lon + delta}"

        overpass_query = f"""
        [bbox:{bbox}];
        (
          node["amenity"="pub"];
          way["amenity"="pub"];
          relation["amenity"="pub"];
        );
        out center;
        """

        url = "https://overpass-api.de/api/interpreter"
        headers = {"User-Agent": "Miru/1.0"}

        print(f"[osm] Fetching OSM data for area ({lat}, {lon})...")
        response = requests.post(url, data=overpass_query, headers=headers, timeout=15)

        if response.status_code != 200:
            print(f"[osm] Overpass returned {response.status_code}")
            return {}

        data = response.json()
        osm_map = {}

        for elem in data.get("elements", []):
            if "center" in elem and "tags" in elem:
                center = elem["center"]
                osm_lat = round(center["lat"], 4)
                osm_lon = round(center["lon"], 4)
                tags = elem["tags"]

                phone = tags.get("phone", "").strip() or None
                website = tags.get("website", "").strip() or None
                hours = tags.get("opening_hours", "").strip() or None

                # Key by coordinates for proximity matching in DB
                osm_map[(osm_lat, osm_lon)] = {
                    "phone": phone,
                    "website": website,
                    "opening_hours": hours,
                    "osm_id": f"osm_{elem.get('id', 'unknown')}"
                }

        print(f"[osm] Found OSM data for {len(osm_map)} pubs")
        return osm_map

    except Exception as e:
        print(f"[osm] Error: {e}")
        return {}


def _update_pubs_with_osm_data(osm_data: Dict[tuple, Dict], sb: Any) -> None:
    """
    Update pubs in database with OSM phone/website/opening_hours data.
    Matches by coordinate proximity since pubs keyed as (lat, lon).
    """
    try:
        updated = 0
        for (osm_lat, osm_lon), info in osm_data.items():
            if not any([info.get("phone"), info.get("website"), info.get("opening_hours")]):
                continue

            try:
                # Find pub within 100m (0.001 degrees) of OSM coordinates
                delta = 0.001
                nearby = sb.table("pubs").select("id,name") \
                    .gte("lat", osm_lat - delta).lte("lat", osm_lat + delta) \
                    .gte("lon", osm_lon - delta).lte("lon", osm_lon + delta) \
                    .limit(1).execute().data or []

                if nearby:
                    pub_id = nearby[0]["id"]
                    sb.table("pubs").update({
                        "phone": info.get("phone"),
                        "website": info.get("website"),
                        "opening_hours": info.get("opening_hours"),
                        "osm_id": info.get("osm_id")
                    }).eq("id", pub_id).execute()
                    updated += 1
            except Exception as e:
                pass  # Silently skip if update fails

        if updated > 0:
            print(f"[osm] Updated {updated} pubs with phone/website/opening_hours")
    except Exception as e:
        print(f"[osm] Error updating database: {e}")


def get_nearby_pubs(postcode: str, limit: int = 5, confidence_tier: str = None) -> List[Dict]:
    """
    Query unified Supabase pubs database by postcode.
    Returns top N pubs by distance, optionally filtered by confidence tier.

    Args:
        postcode: UK postcode (e.g., "SW1A 1AA")
        limit: Max pubs to return
        confidence_tier: Filter by "VERIFIED", "LIKELY", or "UNVERIFIED" (None = all)

    Returns: [{"name": str, "distance_km": float, "rating": int, "tier": str}, ...]
    """
    postcode_clean = postcode.strip().upper()

    # Check cache
    cache_key = f"{postcode_clean}:{confidence_tier}:{limit}"
    if cache_key in _PUBS_CACHE:
        cached, ts = _PUBS_CACHE[cache_key]
        if time.time() - ts < _PUBS_CACHE_TTL:
            print(f"[pubs] Cache hit for {postcode_clean}")
            return cached

    print(f"[pubs] Fetching pubs for {postcode_clean}...")

    try:
        coords = postcode_to_latlon(postcode_clean)
        if not coords:
            print(f"[pubs] Invalid postcode: {postcode_clean}")
            return []

        user_lat, user_lon = coords

        # Enrich OSM data for this area in background thread (truly non-blocking)
        def _bg_enrich():
            try:
                osm_data = _enrich_osm_data_for_area(user_lat, user_lon, radius_km=20)
                if osm_data:
                    sb = lib._sb()
                    _update_pubs_with_osm_data(osm_data, sb)
            except Exception as e:
                print(f"[pubs] Background enrichment failed: {e}")

        import threading
        enrich_thread = threading.Thread(target=_bg_enrich, daemon=True)
        enrich_thread.start()

        # Query Supabase pubs table with pagination (fetch ALL 38k+ pubs)
        sb = lib._sb()
        all_rows = []
        page = 0
        page_size = 1000

        while True:
            query = sb.table("pubs").select("*").range(page * page_size, (page + 1) * page_size - 1)

            # Filter by confidence tier if specified
            if confidence_tier:
                query = query.eq("confidence_tier", confidence_tier)

            rows = query.execute().data or []
            if not rows:
                break

            all_rows.extend(rows)
            page += 1
            print(f"[pubs] Fetched page {page} ({len(all_rows)} total)")

        print(f"[pubs] Total pubs to search: {len(all_rows)}")

        # Calculate distances and filter
        nearby = []
        for pub in all_rows:
            pub_lat = pub.get("lat")
            pub_lon = pub.get("lon")

            if not (pub_lat and pub_lon):
                continue

            dist = haversine_km(user_lat, user_lon, pub_lat, pub_lon)
            if dist <= 15:  # Within 15km
                nearby.append({
                    "name": pub.get("name", ""),
                    "postcode": pub.get("postcode", ""),
                    "distance_km": round(dist, 1),
                    "lat": pub_lat,
                    "lon": pub_lon,
                    "phone": pub.get("phone"),
                    "website": pub.get("website"),
                    "opening_hours": pub.get("opening_hours"),
                    "fhrs_rating": pub.get("fhrs_rating"),  # 5=very good, 0=awaiting
                    "confidence_tier": pub.get("confidence_tier", "UNVERIFIED"),
                    "match_confidence": pub.get("match_confidence"),
                    "fhrs_id": pub.get("fhrs_id"),
                    "osm_id": pub.get("osm_id"),
                })

        # Sort by distance and limit
        nearby.sort(key=lambda x: x["distance_km"])
        result = nearby[:limit]

        # Cache
        _PUBS_CACHE[cache_key] = (result, time.time())

        print(f"[pubs] Found {len(result)} pubs near {postcode_clean} (searched {len(all_rows)} total)")
        return result

    except Exception as e:
        print(f"[pubs] Error querying Supabase: {e}")
        return []


def get_pubs_by_coords(lat: float, lon: float, limit: int = 5, radius_km: float = 10.0) -> List[Dict]:
    """
    Query pubs by latitude/longitude directly.
    """
    try:
        sb = lib._sb()
        all_rows = []
        page = 0
        page_size = 1000

        # Paginate through all pubs
        while True:
            rows = sb.table("pubs").select("*").range(page * page_size, (page + 1) * page_size - 1).execute().data or []
            if not rows:
                break
            all_rows.extend(rows)
            page += 1

        nearby = []
        for pub in all_rows:
            pub_lat = pub.get("lat")
            pub_lon = pub.get("lon")

            if not (pub_lat and pub_lon):
                continue

            dist = haversine_km(lat, lon, pub_lat, pub_lon)
            if dist <= radius_km:
                nearby.append({
                    "name": pub.get("name", ""),
                    "distance_km": round(dist, 1),
                    "lat": pub_lat,
                    "lon": pub_lon,
                    "postcode": pub.get("postcode", ""),
                    "phone": pub.get("phone"),
                    "website": pub.get("website"),
                    "opening_hours": pub.get("opening_hours"),
                    "fhrs_rating": pub.get("fhrs_rating"),
                    "confidence_tier": pub.get("confidence_tier"),
                })

        nearby.sort(key=lambda x: x["distance_km"])
        return nearby[:limit]

    except Exception as e:
        print(f"[pubs] Error: {e}")
        return []


if __name__ == "__main__":
    # Test
    pubs = get_nearby_pubs("SW1A 1AA", limit=5)
    print(json.dumps(pubs, indent=2, default=str))
