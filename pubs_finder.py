#!/usr/bin/env python3
"""
Pubs Finder — Find nearby pubs via OpenStreetMap + Google Places
Integrated into postcode context enrichment
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


def get_nearby_pubs_osm(postcode: str, radius_km: float = 5.0, limit: int = 5) -> List[Dict]:
    """
    Fetch nearby pubs from OpenStreetMap (Overpass API).
    Returns: [{"name": str, "distance_km": float, "lat": float, "lon": float, "tags": {...}}, ...]
    """
    try:
        coords = postcode_to_latlon(postcode)
        if not coords:
            return []

        lat, lon = coords

        # Overpass API query: pubs within radius
        # amenity=pub OR amenity=bar (OSM tags for pubs)
        query = f"""
        [out:json];
        [bbox:{lat - radius_km/111:.4f},{lon - radius_km/111/.83:.4f},{lat + radius_km/111:.4f},{lon + radius_km/111/.83:.4f}];
        (
          node["amenity"="pub"];
          way["amenity"="pub"];
          node["amenity"="bar"];
          way["amenity"="bar"];
        );
        out center;
        """

        r = requests.post(
            "https://overpass-api.de/api/interpreter",
            data=query,
            timeout=10,
            headers={"User-Agent": "Miru/1.0"}
        )

        if r.status_code != 200:
            return []

        data = r.json()
        elements = data.get("elements", [])

        pubs = []
        for elem in elements:
            name = elem.get("tags", {}).get("name", "Unknown Pub")
            lat_e = elem.get("lat") or (elem.get("center", {}).get("lat") if elem.get("center") else None)
            lon_e = elem.get("lon") or (elem.get("center", {}).get("lon") if elem.get("center") else None)

            if not (lat_e and lon_e):
                continue

            dist = haversine_km(lat, lon, lat_e, lon_e)
            if dist <= radius_km:
                pubs.append({
                    "name": name,
                    "distance_km": round(dist, 1),
                    "lat": lat_e,
                    "lon": lon_e,
                    "source": "OpenStreetMap",
                    "tags": elem.get("tags", {}),
                    "osm_id": elem.get("id"),
                })

        # Sort by distance
        pubs.sort(key=lambda x: x["distance_km"])
        return pubs[:limit]

    except Exception as e:
        print(f"[pubs] OSM error: {e}")
        return []


def get_nearby_pubs_google_places(postcode: str, radius_m: float = 5000, limit: int = 5) -> List[Dict]:
    """
    Fetch pubs from Google Places API (fallback/enrichment).
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


def get_nearby_pubs(postcode: str, limit: int = 5) -> List[Dict]:
    """
    Get nearby pubs from both OSM and Google Places, merged.
    Prefers OSM (community-verified) but fills gaps with Google Places.
    """
    postcode_clean = postcode.strip().upper()

    # Check cache
    if postcode_clean in _PUBS_CACHE:
        cached, ts = _PUBS_CACHE[postcode_clean]
        if time.time() - ts < _PUBS_CACHE_TTL:
            return cached

    print(f"[pubs] Fetching pubs for {postcode_clean}...")

    # Get from OSM first
    osm_pubs = get_nearby_pubs_osm(postcode_clean, limit=limit)

    # If OSM has enough, use it
    if len(osm_pubs) >= limit:
        result = osm_pubs[:limit]
        _PUBS_CACHE[postcode_clean] = (result, time.time())
        return result

    # Otherwise, fill gaps with Google Places
    google_pubs = get_nearby_pubs_google_places(postcode_clean, limit=limit - len(osm_pubs))

    # Merge (prefer OSM, add Google for gaps)
    result = osm_pubs + google_pubs
    result = result[:limit]

    # Cache
    _PUBS_CACHE[postcode_clean] = (result, time.time())

    print(f"[pubs] Found {len(result)} pubs for {postcode_clean}")
    return result


if __name__ == "__main__":
    # Test
    pubs = get_nearby_pubs("KT16 0DA", limit=5)
    print(json.dumps(pubs, indent=2, default=str))
