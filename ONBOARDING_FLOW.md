# Miru Onboarding + Postcode Context Enrichment

## User Journey: Entering Postcode

When a user **enters a postcode** during onboarding or updates it later:

### Step 1: Immediate Actions (Sync)
- Postcode saved to `ma_details` table (type: `v2_prefs`)
- Module preferences saved (trains, fuel, council, etc.)
- User receives instant response ✅

### Step 2: Background Context Enrichment (Async)
Background thread automatically fetches:

1. **Nearest Train Station** (`get_nearby_station`)
   - Uses uk_stations.py (2,839 UK stations)
   - Finds closest within 5km radius
   - Returns: name, CRS code, distance

2. **Next Trains** (`get_next_trains`)
   - Calls RTT API (Real-Time Trains)
   - Fetches next 3 departures from nearest station
   - Returns: time, destination, platform, operator

3. **Nearby Fuel Prices** (`get_nearby_fuel`)
   - Queries Miru's fuel station database
   - Finds cheapest within 10km
   - Returns: top 3 stations with petrol/diesel prices

4. **Local MP** (`get_local_mp`)
   - Uses postcodes.io to get constituency
   - Looks up in Supabase `mps` table (all 650 UK MPs)
   - Returns: name, party, email, phone, twitter, office address
   - Lazy-loads contact details from Parliament API

5. **Local Councillor + Council Info** (`get_local_council_info`)
   - Gets ward + council from postcodes.io
   - Matches against `elections_candidates.csv`
   - Returns: council name, ward, election date, 5 top candidates

6. **Full Context Cached**
   - All data saved to `ma_details` (type: `postcode_context`)
   - Cache TTL: 1 hour (expires if data > 1 hour old)

---

## API Endpoints

### `/api/onboarding/complete` [POST]
Triggered by onboarding wizard completion.
```json
{
  "postcode": "KT16 0DA",
  "modules_enabled": {
    "trains": true,
    "fuel": true,
    "council": true,
    "mp": true
  }
}
```
→ Saves prefs + starts background enrichment

### `/api/postcode-change` [POST]
Called when user updates their postcode later.
```json
{
  "postcode": "SW1A 1AA"
}
```
→ Updates prefs + re-enriches all context

### `/api/postcode-context` [GET]
Retrieve enriched context for the user's current postcode.
```json
{
  "postcode": "KT16 0DA",
  "lat": 51.384,
  "lon": -0.588,
  "station": {
    "name": "Staines",
    "crs": "STN",
    "distance_km": 2.5
  },
  "next_trains": [
    {"time": "14:32", "destination": "London Waterloo", "platform": "2"},
    {"time": "14:47", "destination": "Reading", "platform": "1"}
  ],
  "fuel_nearby": [
    {"name": "Tesco STN", "distance_km": 1.2, "petrol_pence": 151, "diesel_pence": 155},
    {"name": "Shell STN", "distance_km": 1.5, "petrol_pence": 153, "diesel_pence": 157}
  ],
  "mp": {
    "name": "Jane Smith",
    "party": "Labour",
    "constituency": "Staines",
    "email": "jane@parliament.uk",
    "phone": "+44 20 7219 0000"
  },
  "council": {
    "council": "Spelthorne Borough Council",
    "ward": "Staines North",
    "candidates": [...],
    "election_date": "2026-05-07"
  }
}
```

---

## Database Schema

### `ma_details` table
```sql
{
  "device_id": "whatsapp_number",
  "type": "postcode_context",  -- or v2_prefs, modules_enabled
  "data": {
    "postcode": "KT16 0DA",
    "station": {...},
    "next_trains": [...],
    "fuel_nearby": [...],
    "mp": {...},
    "council": {...},
    "enriched_at": "2026-09-18T14:32:00"
  }
}
```

---

## Flow Diagram

```
User enters postcode
       ↓
/api/onboarding/complete [POST]
       ↓
Save to ma_details (v2_prefs, modules_enabled)
       ↓
Return 200 OK instantly to user
       ├→ [Background Thread]
       │   ├→ postcode_to_latlon()
       │   ├→ get_nearby_station()
       │   ├→ get_next_trains()
       │   ├→ get_nearby_fuel()
       │   ├→ get_local_mp()
       │   ├→ get_local_council_info()
       │   └→ Save full context to ma_details (postcode_context)
       └→ User sees onboarding completion
           Context loads in background, ready for brief/homepage
```

---

## What the User Sees

### On Onboarding
- "Setting up your context..." message appears
- Postcode confirmed ✓
- In background: trains, fuel, MP, council data is fetched

### On Homepage (Brief)
- When user opens Miru, context is already prefetched
- Brief card shows:
  - "Next train from Staines: 14:32 to Waterloo"
  - "Fuel at Tesco: 151p petrol"
  - Local events, councillor info (if enabled)

### On Postcode Change
- User can update postcode anytime
- `/api/postcode-change` re-runs full enrichment
- Fresh context available within seconds

---

## Error Handling

- **Invalid postcode**: Returns error, no enrichment
- **Station not found**: Graceful null (no trains shown)
- **RTT API down**: Falls back to empty, retries on next context fetch
- **MP not found**: Rare edge case (handles fuzzy matching)
- **Council data missing**: Shows available election candidates only

---

## Performance Notes

- **Background thread**: Non-blocking, 3-5 second total enrichment time
- **Caching**: 1-hour TTL per postcode (avoid duplicate API calls)
- **Parallel fetches**: All 5 context types fetched in parallel (not sequential)
- **Total API calls**: ~7 (postcodes.io×2, RTT×1, Parliament×1 lazy, elections CSV×1 local)

---

## Testing

```bash
# Test enrichment for a postcode
python3 -c "
from postcode_context_engine import enrich_postcode_context
import json
ctx = enrich_postcode_context('KT16 0DA')
print(json.dumps(ctx, indent=2, default=str))
"

# Test via API (requires token)
curl http://localhost:5000/api/postcode-context?token=<device_id>
```

---

## Future Enhancements

1. **NRE Darwin LDBWS** - Real train departures (once API key arrives)
2. **Nearby schools** - Via new API endpoint
3. **Local holidays** - Bank holidays, council closures
4. **Weather integration** - For commute planning
5. **Service disruptions** - NRE alerts for current station
