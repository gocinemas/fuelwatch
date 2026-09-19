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

# Reverse geocoding cache: {(lat, lon): place_name}
_AREA_CACHE = {}
_AREA_CACHE_TTL = 3600  # 1 hour


def _get_area_name(lat: float, lon: float) -> str:
    """
    Reverse geocode coordinates to get place/area name (Longcross, Virginia Water, etc.)
    Uses OpenStreetMap Nominatim API.
    """
    cache_key = (round(lat, 4), round(lon, 4))

    if cache_key in _AREA_CACHE:
        cached, ts = _AREA_CACHE[cache_key]
        if time.time() - ts < _AREA_CACHE_TTL:
            return cached

    try:
        # Use Nominatim (free, no API key needed)
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&zoom=10"
        headers = {"User-Agent": "Miru-PubsFinder/1.0"}

        resp = requests.get(url, headers=headers, timeout=3)
        if resp.status_code == 200:
            data = resp.json()

            # Priority: village > town > suburb > county
            address = data.get("address", {})
            area = (
                address.get("village") or
                address.get("town") or
                address.get("suburb") or
                address.get("county") or
                address.get("city") or
                "Unknown"
            )

            _AREA_CACHE[cache_key] = (area, time.time())
            return area
    except Exception as e:
        print(f"[pubs] Reverse geocode error for ({lat}, {lon}): {e}")

    return None


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

        # Query Supabase pubs table with pagination (fetch ALL 38k+ pubs)
        sb = lib._sb()
        all_rows = []
        page = 0
        page_size = 1000

        print(f"[pubs] Starting pagination: page_size={page_size}")

        while page < 50:  # Safety limit
            start = page * page_size
            end = (page + 1) * page_size - 1

            query = sb.table("pubs").select("*").range(start, end)

            # Filter by confidence tier if specified
            if confidence_tier:
                query = query.eq("confidence_tier", confidence_tier)

            rows = query.execute().data or []
            print(f"[pubs] Page {page}: range({start}, {end}) -> {len(rows)} rows")

            if not rows:
                print(f"[pubs] No rows at page {page}, stopping")
                break

            all_rows.extend(rows)
            page += 1

        print(f"[pubs] Total pubs fetched: {len(all_rows)}")

        # Calculate distances and filter
        nearby = []
        for pub in all_rows:
            pub_lat = pub.get("lat")
            pub_lon = pub.get("lon")

            if not (pub_lat and pub_lon):
                continue

            dist = haversine_km(user_lat, user_lon, pub_lat, pub_lon)
            if dist <= 15:  # Within 15km
                # Use postcode if available, else do reverse geocoding
                pub_postcode = pub.get("postcode", "").strip()
                area_name = pub_postcode or _get_area_name(pub_lat, pub_lon) or ""

                nearby.append({
                    "name": pub.get("name", ""),
                    "area": area_name,  # Area/place name or postcode
                    "postcode": pub_postcode,
                    "distance_km": round(dist, 1),
                    "lat": pub_lat,
                    "lon": pub_lon,
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
                pub_postcode = pub.get("postcode", "").strip()
                area_name = pub_postcode or _get_area_name(pub_lat, pub_lon) or ""

                nearby.append({
                    "name": pub.get("name", ""),
                    "area": area_name,
                    "postcode": pub_postcode,
                    "distance_km": round(dist, 1),
                    "lat": pub_lat,
                    "lon": pub_lon,
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
