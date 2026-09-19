#!/usr/bin/env python3
"""
Postcode Context Engine — Background enrichment for onboarding
Fetches nearest train stations, next trains, fuel, council, MP for a postcode
Called async after user enters postcode during onboarding or via profile update
"""

import os
import json
import requests
import threading
import time
from typing import Optional, Dict, Any
from datetime import datetime
from search import postcode_to_latlon, fetch_all_stations, haversine_km
import library as lib

# Cache: {postcode: (data, timestamp)}
_CONTEXT_CACHE = {}
_CONTEXT_CACHE_TTL = 3600  # 1 hour


def get_nearby_station(postcode: str, max_distance_km: float = 5.0) -> Optional[Dict]:
    """
    Find the closest train station to a postcode.
    Returns: {"name": str, "crs": str, "distance_km": float, "lat": float, "lon": float}
    """
    try:
        coords = postcode_to_latlon(postcode)
        if not coords or len(coords) != 2:
            return None

        lat, lon = coords
        if not (lat and lon):
            return None

        all_stations = fetch_all_stations()
        if not all_stations:
            return None

        closest = None
        closest_dist = float('inf')

        for s in all_stations:
            s_lat = s.get("latitude")
            s_lon = s.get("longitude")
            if not (s_lat and s_lon):
                continue

            dist = haversine_km(lat, lon, s_lat, s_lon)
            if dist < closest_dist and dist <= max_distance_km:
                closest_dist = dist
                closest = {
                    "name": s.get("name", ""),
                    "crs": s.get("crs", ""),
                    "distance_km": round(dist, 2),
                    "lat": s_lat,
                    "lon": s_lon,
                }

        return closest
    except Exception as e:
        print(f"[context] Error finding nearby station: {e}")
        return None


def get_next_trains(station_crs: str, rows: int = 3) -> list:
    """
    Fetch next departures from RTT API for a station.
    Requires RTT token from Miru's sms_service._get_rtt_token()
    Returns: [{"time": str, "destination": str, "platform": str}, ...]
    """
    try:
        from sms_service import _get_rtt_token

        access = _get_rtt_token()
        r = requests.get(
            "https://data.rtt.io/rtt/location",
            headers={"Authorization": f"Bearer {access}"},
            params={"code": f"gb-nr:{station_crs}"},
            timeout=10,
        )

        if r.status_code != 200:
            return []

        data = r.json()
        services = data.get("services", [])[:rows]
        departures = []

        for s in services:
            loc = s.get("locationDetail", {})
            dep_b = loc.get("gbttBookedDeparture", "")
            dep_r = loc.get("realtimeDeparture", dep_b)
            plat = loc.get("platform", "")

            def _fmt(t):
                t = str(t).strip()
                if len(t) == 4 and t.isdigit():
                    return t[:2] + ":" + t[2:]
                return t[:5] if len(t) >= 5 else t

            departures.append({
                "time": _fmt(dep_r or dep_b),
                "destination": s.get("destination", [{}])[-1].get("description", ""),
                "platform": plat,
                "operator": s.get("atocName", ""),
            })

        return departures
    except Exception as e:
        print(f"[context] Error fetching trains: {e}")
        return []


def get_nearby_fuel(postcode: str, max_distance_km: float = 10.0, limit: int = 3) -> list:
    """
    Fetch cheapest nearby fuel stations.
    Returns: [{"name": str, "distance_km": float, "petrol_pence": int, "diesel_pence": int}, ...]
    """
    try:
        coords = postcode_to_latlon(postcode)
        if not coords or len(coords) != 2:
            return []

        lat, lon = coords
        if not (lat and lon):
            return []

        all_stations = fetch_all_stations()
        if not all_stations:
            return []

        nearby = []
        for s in all_stations:
            s_lat = s.get("latitude")
            s_lon = s.get("longitude")
            if not (s_lat and s_lon):
                continue

            dist = haversine_km(lat, lon, s_lat, s_lon)
            if dist <= max_distance_km:
                nearby.append({
                    "name": s.get("name", ""),
                    "distance_km": round(dist, 1),
                    "petrol_pence": s.get("petrol_price"),
                    "diesel_pence": s.get("diesel_price"),
                    "brand": s.get("brand", ""),
                })

        nearby.sort(key=lambda x: x["distance_km"])
        return nearby[:limit]
    except Exception as e:
        print(f"[context] Error fetching fuel: {e}")
        return []


def get_local_mp(postcode: str) -> Optional[Dict]:
    """
    Fetch local MP for a postcode (uses Miru's existing MP API).
    Returns: {"name": str, "party": str, "email": str, "phone": str, "twitter": str, ...}
    """
    try:
        postcode_clean = postcode.strip().replace(" ", "").upper()

        # Step 1: Get constituency from postcodes.io
        pc_r = requests.get(f"https://api.postcodes.io/postcodes/{postcode_clean}", timeout=8)
        if not pc_r.ok:
            return None

        pc = pc_r.json().get("result") or {}
        constituency = pc.get("parliamentary_constituency_2024") or pc.get("parliamentary_constituency", "")
        if not constituency:
            return None

        # Step 2: Look up in Miru's mps table
        sb = lib._sb()
        rows = sb.table("mps").select("*") \
            .eq("constituency", constituency.lower()) \
            .execute().data or []

        if not rows:
            # Try fuzzy match
            all_mps = sb.table("mps").select("*").execute().data or []
            key_norm = constituency.lower().replace("&", "and").replace("  ", " ").strip()
            for row in all_mps:
                if row.get("constituency", "").replace("&", "and").replace("  ", " ").strip() == key_norm:
                    rows = [row]
                    break

        if not rows:
            return None

        row = rows[0]
        return {
            "name": row.get("name", ""),
            "party": row.get("party", ""),
            "constituency": row.get("constituency", ""),
            "email": row.get("email", ""),
            "phone": row.get("phone", ""),
            "website": row.get("website", ""),
            "twitter": row.get("twitter", ""),
            "parliament_url": row.get("parliament_url", ""),
        }
    except Exception as e:
        print(f"[context] Error fetching MP: {e}")
        return None


def get_local_council_info(postcode: str) -> Optional[Dict]:
    """
    Fetch local council info: council name, ward, election date, candidates.
    Uses Miru's elections data.
    Returns: {"council": str, "ward": str, "candidates": [...], "election_date": str}
    """
    try:
        from sms_service import _get_elections, _best_ward_match

        coords = postcode_to_latlon(postcode)
        if not coords:
            return None

        elections = _get_elections()
        if not elections:
            return None

        # Try postcode.io to get ward/council
        pc_r = requests.get(
            f"https://api.postcodes.io/postcodes/{postcode.strip().replace(' ', '').upper()}",
            timeout=8
        )
        if not pc_r.ok:
            return None

        pc = pc_r.json().get("result") or {}
        admin_ward = pc.get("admin_ward", "")
        council_area = pc.get("council_area") or pc.get("local_authority", "")

        if not admin_ward or not council_area:
            return None

        # Look up in elections CSV by GSS or slug
        by_gss = elections.get("by_gss", {})
        by_slug = elections.get("by_slug", {})

        # Try GSS first
        for gss, data in by_gss.items():
            if admin_ward.lower() in data.get("ward", "").lower():
                return {
                    "council": data.get("council", ""),
                    "ward": data.get("ward", ""),
                    "candidates": data.get("candidates", [])[:5],
                    "election_date": data.get("election_date", ""),
                    "gss": gss,
                }

        # Try slug (reorganised councils)
        for slug, wards in by_slug.items():
            best_ward = _best_ward_match(admin_ward, wards)
            if best_ward and best_ward in wards:
                data = wards[best_ward]
                return {
                    "council": data.get("council", ""),
                    "ward": data.get("ward", ""),
                    "candidates": data.get("candidates", [])[:5],
                    "election_date": data.get("election_date", ""),
                }

        return None
    except Exception as e:
        print(f"[context] Error fetching council info: {e}")
        return None


def enrich_postcode_context(postcode: str, device_id: str = "") -> Dict[str, Any]:
    """
    Main function: enrich postcode with all context.
    Caches result for 1 hour.
    Returns full context dict ready to store in Supabase.
    """
    postcode_clean = postcode.strip().upper()

    # Check cache
    if postcode_clean in _CONTEXT_CACHE:
        cached, ts = _CONTEXT_CACHE[postcode_clean]
        if time.time() - ts < _CONTEXT_CACHE_TTL:
            print(f"[context] Cache hit for {postcode_clean}")
            return cached

    print(f"[context] Enriching {postcode_clean}...")

    station = get_nearby_station(postcode_clean)
    trains = get_next_trains(station.get("crs", "")) if station else []
    fuel = get_nearby_fuel(postcode_clean)
    mp = get_local_mp(postcode_clean)
    council = get_local_council_info(postcode_clean)

    # Get pubs
    from pubs_finder import get_nearby_pubs
    pubs = get_nearby_pubs(postcode_clean, limit=5)

    coords = postcode_to_latlon(postcode_clean) or (None, None)

    context = {
        "postcode": postcode_clean,
        "lat": coords[0],
        "lon": coords[1],
        "station": station,
        "next_trains": trains,
        "fuel_nearby": fuel,
        "pubs_nearby": pubs,
        "mp": mp,
        "council": council,
        "enriched_at": datetime.utcnow().isoformat(),
    }

    # Cache it
    _CONTEXT_CACHE[postcode_clean] = (context, time.time())

    print(f"[context] Enriched {postcode_clean}: station={station.get('name') if station else None}, pubs={len(pubs)}, mp={mp.get('name') if mp else None}")

    return context


def save_enriched_context_to_db(device_id: str, postcode: str) -> bool:
    """
    Fetch enriched context and save to ma_details table.
    Called async from onboarding/postcode-change handlers.
    """
    try:
        context = enrich_postcode_context(postcode, device_id)

        sb = lib._sb()
        sb.table("ma_details").upsert({
            "device_id": device_id,
            "type": "postcode_context",
            "data": context
        }).execute()

        print(f"[context] Saved enriched context for {device_id}")
        return True
    except Exception as e:
        print(f"[context] Error saving enriched context: {e}")
        return False


def background_enrich_postcode(device_id: str, postcode: str):
    """
    Non-blocking background job to enrich postcode.
    Start in daemon thread from Flask route.
    """
    def _bg():
        try:
            save_enriched_context_to_db(device_id, postcode)
        except Exception as e:
            print(f"[context] Background job failed: {e}")

    thread = threading.Thread(target=_bg, daemon=True)
    thread.start()
    return thread


if __name__ == "__main__":
    # Test
    ctx = enrich_postcode_context("KT16 0DA")
    print(json.dumps(ctx, indent=2, default=str))
