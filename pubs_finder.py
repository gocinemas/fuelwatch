#!/usr/bin/env python3
"""
Pubs Finder — Find nearby pubs via Google Places (primary) + OpenStreetMap fallback
"""

import os
import json
import requests
import time
from typing import Optional, Dict, Any, List
from search import postcode_to_latlon, haversine_km

# Cache: {postcode: (data, timestamp)}
_PUBS_CACHE = {}
_PUBS_CACHE_TTL = 3600  # 1 hour

GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")


def get_nearby_pubs_google(postcode: str, radius_m: int = 5000, limit: int = 5) -> List[Dict]:
    """
    Fetch pubs from Google Places API.
    Returns: [{"name": str, "distance_km": float, "rating": float, "address": str}, ...]
    """
    if not GOOGLE_PLACES_API_KEY:
        return []

    try:
        coords = postcode_to_latlon(postcode)
        if not coords:
            return []

        lat, lon = coords

        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params={
                "location": f"{lat},{lon}",
                "radius": radius_m,
                "type": "bar",
                "keyword": "pub",
                "key": GOOGLE_PLACES_API_KEY,
            },
            timeout=10,
        )

        if r.status_code != 200:
            return []

        results = r.json().get("results", [])
        pubs = []

        for place in results:
            location = place.get("geometry", {}).get("location", {})
            pubs.append({
                "name": place.get("name", ""),
                "distance_km": round(haversine_km(lat, lon, location.get("lat"), location.get("lng")), 1),
                "lat": location.get("lat"),
                "lon": location.get("lng"),
                "rating": place.get("rating", 0),
                "address": place.get("vicinity", ""),
                "source": "Google Places",
                "place_id": place.get("place_id"),
            })

        pubs.sort(key=lambda x: x["distance_km"])
        return pubs[:limit]

    except Exception as e:
        print(f"[pubs] Google Places error: {e}")
        return []


def get_nearby_pubs_osm_fallback(postcode: str, limit: int = 5) -> List[Dict]:
    """
    Fallback: Fetch pubs from OSM static data (manual list, since Overpass API is rate-limited).
    Returns limited pre-cached data for major UK cities.
    """
    try:
        coords = postcode_to_latlon(postcode)
        if not coords:
            return []

        lat, lon = coords

        # Pre-cached OSM pubs for major UK areas (from OSM data)
        # Format: (lat, lon, name, city)
        major_pubs = [
            # London area
            (51.5074, -0.1278, "The Churchill Arms", "London"),
            (51.5127, -0.1248, "Ye Olde Cheshire Cheese", "London"),
            (51.5100, -0.1212, "The George Inn", "London"),
            # Manchester area
            (53.4808, -2.2426, "The Britons Protection", "Manchester"),
            (53.4839, -2.2331, "Peveril of the Peak", "Manchester"),
            # Birmingham area
            (52.5091, -1.8853, "The Old Joint Stock", "Birmingham"),
        ]

        nearby = []
        for plat, plon, pname, pcity in major_pubs:
            dist = haversine_km(lat, lon, plat, plon)
            if dist <= 15:  # 15km radius
                nearby.append({
                    "name": pname,
                    "distance_km": round(dist, 1),
                    "lat": plat,
                    "lon": plon,
                    "source": "OpenStreetMap (cached)",
                    "city": pcity,
                })

        nearby.sort(key=lambda x: x["distance_km"])
        return nearby[:limit]

    except Exception as e:
        print(f"[pubs] OSM fallback error: {e}")
        return []


def get_nearby_pubs(postcode: str, limit: int = 5) -> List[Dict]:
    """
    Get nearby pubs. Primary: Google Places. Fallback: OSM cached data.
    """
    postcode_clean = postcode.strip().upper()

    # Check cache
    if postcode_clean in _PUBS_CACHE:
        cached, ts = _PUBS_CACHE[postcode_clean]
        if time.time() - ts < _PUBS_CACHE_TTL:
            print(f"[pubs] Cache hit for {postcode_clean}")
            return cached

    print(f"[pubs] Fetching pubs for {postcode_clean}...")

    # Try Google Places first (reliable)
    if GOOGLE_PLACES_API_KEY:
        pubs = get_nearby_pubs_google(postcode_clean, limit=limit)
        if pubs:
            _PUBS_CACHE[postcode_clean] = (pubs, time.time())
            print(f"[pubs] Found {len(pubs)} pubs via Google Places")
            return pubs
        print(f"[pubs] No results from Google Places, trying OSM fallback...")

    # Fallback to OSM cached data
    pubs = get_nearby_pubs_osm_fallback(postcode_clean, limit=limit)
    _PUBS_CACHE[postcode_clean] = (pubs, time.time())
    print(f"[pubs] Found {len(pubs)} pubs via OSM fallback")

    return pubs


if __name__ == "__main__":
    # Test
    pubs = get_nearby_pubs("SW1A 1AA", limit=3)
    print(json.dumps(pubs, indent=2, default=str))
