#!/usr/bin/env python3
"""
Enrich pubs database with phone, website, opening_hours from OpenStreetMap
"""

import os
import json
import requests
from supabase import create_client
import time

# Supabase setup
sb_url = os.getenv("SUPABASE_URL")
sb_key = os.getenv("SUPABASE_ANON_KEY")

print(f"SUPABASE_URL: {sb_url}")
print(f"SUPABASE_ANON_KEY: {sb_key[:20] if sb_key else 'NOT SET'}...")

if not sb_url or not sb_key:
    print("❌ SUPABASE_URL and SUPABASE_ANON_KEY required")
    exit(1)

sb = create_client(sb_url, sb_key)

print("\n🍺 Starting pubs data enrichment from OpenStreetMap...")

# Get all pubs from database
print("📊 Fetching all pubs from database...")
all_pubs = []
page = 0
page_size = 1000

while page < 50:
    rows = sb.table("pubs").select("*").limit(page_size).offset(page * page_size).execute().data or []
    if not rows:
        break
    all_pubs.extend(rows)
    page += 1
    print(f"   Page {page}: {len(all_pubs)} total")

print(f"✅ Loaded {len(all_pubs)} pubs\n")

# Fetch OSM data for pubs
print("🌍 Fetching full OpenStreetMap pub data with tags...")

# Use Overpass API to get pubs with all tags
overpass_query = """
[bbox:49.5,-8,56,-2];
(
  node["amenity"="pub"];
  way["amenity"="pub"];
  relation["amenity"="pub"];
);
out center;
"""

osm_data = {}
try:
    url = "https://overpass-api.de/api/interpreter"
    headers = {"User-Agent": "Miru-PubsEnricher/1.0"}

    print(f"   Querying Overpass API...")
    response = requests.post(url, data=overpass_query, headers=headers, timeout=60)

    if response.status_code == 200:
        data = response.json()
        elements = data.get("elements", [])
        print(f"   ✅ Got {len(elements)} OSM elements")

        # Build lookup by coordinates
        for elem in elements:
            if "center" in elem:
                lat = elem["center"]["lat"]
                lon = elem["center"]["lon"]
                tags = elem.get("tags", {})

                # Extract contact info
                phone = tags.get("phone", "").strip() or None
                website = tags.get("website", "").strip() or None
                hours = tags.get("opening_hours", "").strip() or None

                if phone or website or hours:
                    osm_data[(round(lat, 4), round(lon, 4))] = {
                        "phone": phone,
                        "website": website,
                        "opening_hours": hours
                    }
    else:
        print(f"   ⚠️ Overpass API returned {response.status_code}")
except Exception as e:
    print(f"   ⚠️ Error fetching OSM data: {e}")

print(f"📍 Found contact info for {len(osm_data)} locations\n")

# Update pubs with contact info
print("🔄 Updating pubs with contact information...")
updated = 0
skipped = 0

for pub in all_pubs:
    pub_lat = pub.get("lat")
    pub_lon = pub.get("lon")

    if not (pub_lat and pub_lon):
        continue

    # Look up OSM data by rounded coordinates
    key = (round(pub_lat, 4), round(pub_lon, 4))

    if key in osm_data:
        osm_info = osm_data[key]

        try:
            sb.table("pubs").update({
                "phone": osm_info.get("phone"),
                "website": osm_info.get("website"),
                "opening_hours": osm_info.get("opening_hours")
            }).eq("id", pub["id"]).execute()

            updated += 1
            if updated % 100 == 0:
                print(f"   Updated: {updated}")
        except Exception as e:
            print(f"   ❌ Error updating pub {pub['id']}: {e}")
    else:
        skipped += 1

print(f"\n✅ Complete!")
print(f"   Updated: {updated} pubs")
print(f"   Skipped: {skipped} pubs (no OSM match)")
print(f"\n🍺 Pubs now have phone, website, and opening hours populated!")
