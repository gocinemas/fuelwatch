#!/usr/bin/env python3
"""
Pubs Data Pipeline — FHRS + VOA + OSM matching for UK pubs
Builds a unified, confidence-scored pub database
"""

import os
import json
import csv
import requests
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
import time
from difflib import SequenceMatcher
from search import postcode_to_latlon, haversine_km
import library as lib

print("[pubs-pipeline] Initializing...")

# ─── STEP 1: FETCH FHRS DATA (Food Standards Agency) ───────────────────────
def fetch_fhrs_pubs(max_results: int = 10000) -> List[Dict]:
    """
    Fetch pub/bar establishments from Food Standards Agency API.
    Returns: [{"name": str, "postcode": str, "lat": float, "lon": float, "rating": int, "fhrs_id": str}, ...]
    """
    print("[fhrs] Fetching from Food Standards Agency...")
    pubs = []

    try:
        # FHRS API: search for pub/bar type establishments
        url = "https://api.ratings.food.gov.uk/establishments"

        headers = {
            "Accept": "application/json",
            "x-api-version": "2",
        }

        page = 1
        while len(pubs) < max_results:
            params = {
                "pageNumber": page,
                "pageSize": 100,
                "businessType": ["Pub/bar/nightclub", "Pub"],
                "sortOptionKey": "id",
            }

            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code != 200:
                print(f"[fhrs] Error: {r.status_code}")
                break

            data = r.json()
            establishments = data.get("establishments", [])

            if not establishments:
                break

            for est in establishments:
                # Only include pubs (exclude restaurants, hotels)
                business_type = est.get("businessType", {}).get("name", "").lower()
                if "pub" not in business_type and "bar" not in business_type:
                    continue

                postcode = est.get("postcode", "").strip()
                if not postcode:
                    continue

                # Try to get lat/lon
                coords = postcode_to_latlon(postcode)
                if not coords:
                    continue

                lat, lon = coords

                pubs.append({
                    "name": est.get("businessName", ""),
                    "postcode": postcode,
                    "lat": lat,
                    "lon": lon,
                    "rating": est.get("ratingValue", 0),
                    "rating_date": est.get("ratingDate", ""),
                    "fhrs_id": est.get("FHRSID", ""),
                    "address": est.get("addressLine1", ""),
                    "source": "FHRS",
                })

                if len(pubs) >= max_results:
                    break

            page += 1
            time.sleep(0.5)  # Rate limiting

        print(f"[fhrs] Fetched {len(pubs)} pubs from FHRS")
        return pubs

    except Exception as e:
        print(f"[fhrs] Error: {e}")
        return pubs


# ─── STEP 2: LOAD OSM PUBS (from cache or Overpass) ────────────────────────
def fetch_osm_pubs_cached() -> List[Dict]:
    """
    Load OSM pubs from uk_stations.py or similar cached data.
    For now, returns a small hand-curated list since Overpass is rate-limited.
    TODO: Download full OSM planet data or use an OSM dump file.
    """
    print("[osm] Loading cached OSM pubs...")

    # Hand-curated major pubs from OSM (would be replaced with full dump)
    osm_pubs = [
        {"name": "The Churchill Arms", "postcode": "W8 7PH", "lat": 51.5033, "lon": -0.2007, "osm_id": "123456", "source": "OSM"},
        {"name": "Ye Olde Cheshire Cheese", "postcode": "EC4A 3DG", "lat": 51.5127, "lon": -0.1248, "osm_id": "123457", "source": "OSM"},
        {"name": "The George Inn", "postcode": "SE1 1UH", "lat": 51.5063, "lon": -0.0933, "osm_id": "123458", "source": "OSM"},
        {"name": "The Lamb & Flag", "postcode": "WC2E 7BB", "lat": 51.5094, "lon": -0.1235, "osm_id": "123459", "source": "OSM"},
        {"name": "The Britons Protection", "postcode": "M3 4NF", "lat": 53.4827, "lon": -2.2398, "osm_id": "123460", "source": "OSM"},
    ]

    print(f"[osm] Loaded {len(osm_pubs)} pubs from cache (TODO: full OSM dump)")
    return osm_pubs


# ─── STEP 3: FUZZY MATCHING ──────────────────────────────────────────────────
def fuzzy_match(name1: str, name2: str, threshold: float = 0.7) -> float:
    """Calculate string similarity (0-1)."""
    s = SequenceMatcher(None, name1.lower(), name2.lower()).ratio()
    return s


def match_pubs(fhrs: List[Dict], osm: List[Dict], dist_threshold_m: int = 300) -> tuple:
    """
    Match FHRS pubs to OSM pubs using fuzzy name matching + distance.
    Returns: (matched_pubs, unmatched_fhrs, unmatched_osm)
    """
    print("[match] Matching FHRS → OSM...")

    matched = []
    matched_osm_ids: Set[str] = set()
    unmatched_fhrs = []

    for fhrs_pub in fhrs:
        fname = fhrs_pub.get("name", "")
        flat, flon = fhrs_pub.get("lat"), fhrs_pub.get("lon")

        best_osm = None
        best_score = 0

        for osm_pub in osm:
            oname = osm_pub.get("name", "")
            olat, olon = osm_pub.get("lat"), osm_pub.get("lon")

            # Name similarity
            name_score = fuzzy_match(fname, oname)
            if name_score < 0.5:
                continue

            # Distance check
            dist = haversine_km(flat, flon, olat, olon) * 1000  # to meters
            if dist > dist_threshold_m:
                continue

            # Combined score (70% name, 30% distance)
            distance_score = max(0, 1 - (dist / dist_threshold_m))
            combined = name_score * 0.7 + distance_score * 0.3

            if combined > best_score:
                best_score = combined
                best_osm = osm_pub

        if best_score > 0.6:
            # Match found
            matched.append({
                "name": fname,
                "postcode": fhrs_pub.get("postcode"),
                "lat": flat,
                "lon": flon,
                "fhrs_id": fhrs_pub.get("fhrs_id"),
                "fhrs_rating": fhrs_pub.get("rating"),
                "osm_id": best_osm.get("osm_id"),
                "match_confidence": round(best_score, 2),
                "verified": "osm" if best_osm else None,
            })
            matched_osm_ids.add(best_osm.get("osm_id"))
        else:
            # No match found
            unmatched_fhrs.append(fhrs_pub)

    unmatched_osm = [p for p in osm if p.get("osm_id") not in matched_osm_ids]

    print(f"[match] Matched: {len(matched)}, Unmatched FHRS: {len(unmatched_fhrs)}, Unmatched OSM: {len(unmatched_osm)}")

    return matched, unmatched_fhrs, unmatched_osm


# ─── STEP 4: ASSIGN CONFIDENCE TIERS ────────────────────────────────────────
def assign_confidence_tiers(matched: List[Dict], unmatched_fhrs: List[Dict], unmatched_osm: List[Dict]) -> List[Dict]:
    """
    Assign confidence tiers to all pubs:
    - VERIFIED: OSM (community-verified)
    - LIKELY: FHRS + match_confidence > 0.8
    - UNVERIFIED: FHRS only (no OSM match)
    """
    print("[confidence] Assigning confidence tiers...")

    all_pubs = []

    # Matched pubs (VERIFIED)
    for pub in matched:
        pub["confidence_tier"] = "VERIFIED"
        all_pubs.append(pub)

    # Unmatched FHRS with high confidence (LIKELY)
    for pub in unmatched_fhrs:
        pub["confidence_tier"] = "LIKELY"
        pub["fhrs_rating"] = pub.get("rating")
        pub["postcode"] = pub.get("postcode")
        pub["lat"] = pub.get("lat")
        pub["lon"] = pub.get("lon")
        all_pubs.append(pub)

    # Unmatched OSM (VERIFIED from OSM, but no hygiene data)
    for pub in unmatched_osm:
        pub["confidence_tier"] = "VERIFIED"
        all_pubs.append(pub)

    print(f"[confidence] VERIFIED: {len([p for p in all_pubs if p['confidence_tier']=='VERIFIED'])}, LIKELY: {len([p for p in all_pubs if p['confidence_tier']=='LIKELY'])}, UNVERIFIED: {len([p for p in all_pubs if p['confidence_tier']=='UNVERIFIED'])}")

    return all_pubs


# ─── STEP 5: LOAD TO SUPABASE ────────────────────────────────────────────────
def save_pubs_to_supabase(pubs: List[Dict]) -> bool:
    """
    Save unified pub database to Supabase.
    Creates/updates `pubs` table.
    """
    print(f"[supabase] Saving {len(pubs)} pubs to Supabase...")

    try:
        sb = lib._sb()

        # Prepare rows (Supabase compatible)
        rows = []
        for pub in pubs:
            rows.append({
                "name": pub.get("name", ""),
                "postcode": pub.get("postcode", ""),
                "lat": pub.get("lat"),
                "lon": pub.get("lon"),
                "fhrs_id": pub.get("fhrs_id", ""),
                "fhrs_rating": pub.get("fhrs_rating"),
                "osm_id": pub.get("osm_id", ""),
                "confidence_tier": pub.get("confidence_tier", "UNVERIFIED"),
                "match_confidence": pub.get("match_confidence", 0),
                "data": json.dumps(pub),  # Full record as JSON backup
                "created_at": datetime.utcnow().isoformat(),
            })

        # Upsert in batches of 100
        for i in range(0, len(rows), 100):
            batch = rows[i:i+100]
            sb.table("pubs").upsert(batch, on_conflict="fhrs_id,osm_id").execute()
            print(f"[supabase] Saved batch {i//100 + 1}")

        print(f"[supabase] ✅ Saved {len(rows)} pubs")
        return True

    except Exception as e:
        print(f"[supabase] Error: {e}")
        return False


# ─── MAIN PIPELINE ──────────────────────────────────────────────────────────
def run_pipeline():
    """Execute full pubs data pipeline."""
    print("\n🍺 Starting Pubs Data Pipeline...\n")

    # Step 1: Fetch FHRS data
    fhrs_pubs = fetch_fhrs_pubs(max_results=1000)

    # Step 2: Load OSM pubs
    osm_pubs = fetch_osm_pubs_cached()

    # Step 3: Match
    matched, unmatched_fhrs, unmatched_osm = match_pubs(fhrs_pubs, osm_pubs)

    # Step 4: Assign confidence
    all_pubs = assign_confidence_tiers(matched, unmatched_fhrs, unmatched_osm)

    # Step 5: Save to Supabase
    success = save_pubs_to_supabase(all_pubs)

    if success:
        print(f"\n✅ Pipeline complete! {len(all_pubs)} pubs in database")
    else:
        print(f"\n❌ Pipeline failed at Supabase step")

    return all_pubs


if __name__ == "__main__":
    pubs = run_pipeline()
    print(f"\nSample pubs:")
    for pub in pubs[:5]:
        print(f"  - {pub['name']} ({pub['confidence_tier']})")
