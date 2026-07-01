"""
Places Cache Layer — Replaces Google Places API with free Overpass + Supabase cache.

Strategy: Fetch places once per day, cache for 24h, serve from cache.
Cost: £0 instead of £300+/month
"""

import requests
import json
import os
from datetime import datetime, timedelta
import library as lib


def _overpass_query(query: str) -> list:
    """Execute Overpass API query (free)."""
    try:
        resp = requests.post(
            "https://overpass-api.de/api/interpreter",
            data=query,
            timeout=15
        )
        if resp.status_code == 200:
            return resp.json().get("elements", [])
        return []
    except Exception as e:
        print(f"[overpass] error: {e}")
        return []


def fetch_places_for_postcode(postcode: str, place_types: list = None) -> dict:
    """
    Fetch places (restaurants, cafes, bars, parks) for a postcode via Overpass.

    Args:
        postcode: UK postcode (e.g., "KT16 0DA")
        place_types: List of types to fetch (default: ["restaurant", "cafe", "bar", "park"])

    Returns:
        {
            "postcode": "KT16 0DA",
            "restaurants": [{"name": "...", "distance_mi": 0.5}, ...],
            "cafes": [...],
            "bars": [...],
            "parks": [...],
            "cached_at": "2026-07-01T10:30:00",
            "expires_at": "2026-07-02T10:30:00"
        }
    """
    if place_types is None:
        place_types = ["restaurant", "cafe", "bar", "park"]

    try:
        # Get lat/lon from postcode
        from search import postcode_to_latlon, haversine_km
        ll = postcode_to_latlon(postcode.strip().upper())
        if not ll:
            return {}

        lat, lon = ll
        results = {"postcode": postcode, "cached_at": datetime.utcnow().isoformat()}

        # Fetch each type
        amenity_map = {
            "restaurant": "restaurant",
            "cafe": "cafe",
            "bar": "bar",
            "park": "park"
        }

        for ptype in place_types:
            amenity = amenity_map.get(ptype, ptype)

            # Overpass query for this place type
            query = f"""
[out:json][timeout:10];
(
  node["amenity"="{amenity}"](around:2000,{lat},{lon});
  way["amenity"="{amenity}"](around:2000,{lat},{lon});
);
out body 20;
"""
            elements = _overpass_query(query)
            places = []

            for elem in elements:
                name = elem.get("tags", {}).get("name", "").strip()
                if not name:
                    continue

                # Get lat/lon
                elat = elem.get("lat")
                elon = elem.get("lon")
                if elat is None or elon is None:
                    continue

                dist_km = haversine_km(lat, lon, elat, elon)
                dist_mi = round(dist_km / 1.60934, 1)

                places.append({
                    "name": name,
                    "distance_mi": dist_mi,
                })

            # Sort by distance, limit to 10
            places.sort(key=lambda x: x["distance_mi"])
            results[ptype] = places[:10]

        # Add expiration (24h from now)
        results["expires_at"] = (datetime.utcnow() + timedelta(hours=24)).isoformat()
        return results

    except Exception as e:
        print(f"[places-fetch] {postcode}: {e}")
        return {}


def cache_places(postcode: str) -> bool:
    """Fetch places and store in Supabase cache."""
    try:
        places = fetch_places_for_postcode(postcode)
        if not places:
            return False

        sb = lib._sb()
        sb.table("places_cache").upsert({
            "postcode": postcode,
            "data": places,
            "cached_at": datetime.utcnow().isoformat(),
            "expires_at": places.get("expires_at"),
        }).execute()

        print(f"[cache] {postcode}: OK")
        return True
    except Exception as e:
        print(f"[cache] {postcode}: {e}")
        return False


def get_cached_places(postcode: str) -> dict:
    """Get places from cache (or fetch if expired)."""
    try:
        sb = lib._sb()
        rows = sb.table("places_cache").select("data,expires_at") \
            .eq("postcode", postcode) \
            .limit(1).execute().data or []

        if not rows:
            return {}

        row = rows[0]
        expires = row.get("expires_at")

        # Check if expired
        if expires:
            exp_dt = datetime.fromisoformat(expires)
            if datetime.utcnow() > exp_dt:
                # Expired - refresh in background
                print(f"[cache] {postcode}: expired, will refresh")
                # Don't block - refresh happens in cron job

        return row.get("data", {})
    except Exception as e:
        print(f"[cache-get] {postcode}: {e}")
        return {}


def refresh_all_postcodes() -> dict:
    """Background job: refresh ALL user postcodes. Call once daily via cron."""
    try:
        sb = lib._sb()

        # Get all unique postcodes from ma_details (user locations)
        rows = sb.table("ma_details").select("data") \
            .eq("type", "v2_prefs") \
            .limit(1000).execute().data or []

        postcodes = set()
        for row in rows:
            data = row.get("data", {})
            if data.get("fuel_postcode"):
                postcodes.add(data["fuel_postcode"].strip().upper())

        print(f"[cache-refresh] Refreshing {len(postcodes)} postcodes...")

        success = 0
        failed = 0

        for postcode in postcodes:
            if cache_places(postcode):
                success += 1
            else:
                failed += 1

        return {
            "postcodes_refreshed": success,
            "postcodes_failed": failed,
            "total": len(postcodes)
        }

    except Exception as e:
        print(f"[cache-refresh] error: {e}")
        return {"error": str(e)}
