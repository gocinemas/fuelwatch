# Miru Onboarding Redesign — September 2026

## Summary
Redesigned Miru onboarding from 7-step wizard to **minimal 3-step flow** for users to get instantly to all features.

## The Problem
- Old onboarding (7 steps) was overwhelming: postcode → phone opt-in → kids → schools → commute → fuel → spend
- Users hit cognitive overload before reaching features
- Train details weren't populating on first load (background enrichment not running reliably)

## The Solution

### New 3-Step Flow
1. **Step 1: Postcode** — "Where are you?" ← only required field
2. **Step 2: Schools?** — "School-age kids?" ← optional yes/no
3. **Step 3: Done** — Loading spinner + "Setting up your Miru..."

**Redirect delay:** 2 seconds on step 3 → gives background enrichment time to populate data

### Features Built
- ✅ Beautiful minimal UI (progress bar, clear value props, emoji icons)
- ✅ Auto-skips onboarding if user already has postcode saved
- ✅ Calls `/api/onboarding/complete` which:
  - Saves postcode to `v2_prefs`
  - Saves module preferences (schools enabled/disabled)
  - Triggers `background_enrich_postcode()` in daemon thread (non-blocking)
- ✅ Redirects to homepage with `?token=...&postcode=...` to load all data

### Background Enrichment (`postcode_context_engine.py`)
Async thread populates:
- 🚆 **Trains** — nearest station + next 3 departures (RTT API)
- ⛽ **Fuel** — 3 cheapest nearby stations
- 🍺 **Pubs** — top 5 nearby pubs (with OSM phone/website/hours via smart on-demand fetch)
- 📍 **Local MP** — name, party, email, contact
- 🏛️ **Council** — ward, candidates, election date

All data cached in `ma_details` table as `type="postcode_context"` for 1 hour.

### Pubs Data Enrichment
- On first pubs search: `pubs_finder.py` queries Overpass API for 20km radius
- Fetches phone, website, opening_hours from OSM tags
- Updates Supabase pubs table with enriched data (non-blocking)
- Subsequent searches show enriched data instantly from cache

### Files Changed
- **templates/onboarding_simple.html** — NEW 3-step wizard UI
- **sms_service.py** — `/onboarding` route now renders `onboarding_simple.html`
- **templates/onboarding_simple.html** — Passes token + postcode to homepage on redirect

### Commits
1. `e4ade603` — Redesign onboarding to simple 3-step flow
2. `0f0735b3` — Fix onboarding redirect to pass token to homepage

## Testing Checklist
- [ ] Start new user flow → /onboarding
- [ ] Enter postcode (e.g., "SW1A 1AA")
- [ ] Select "Yes" or "No" for schools
- [ ] See loading spinner on step 3
- [ ] Redirected to homepage with all data loaded
- [ ] Homepage shows:
  - 🚆 Next trains (from `/api/home/brief`)
  - ⛽ Cheapest fuel (from `/api/your-area`)
  - 🍺 Nearby pubs (from `/api/your-area` + pubs database)
  - 📅 School events (if schools enabled)
- [ ] Navigate to My Area → verify trains/fuel/pubs all show with enriched data

## Known Issues
- RTT_TOKEN is empty in .env (trains may not fetch if empty)
- Overpass API can timeout on large queries (mitigated by 20km radius limit)
- Phone/website/hours populate on-demand (not all pubs have data)

## Next Steps (Future)
- [ ] Verify train fetching works (check RTT_TOKEN setup)
- [ ] Add visual "data loading" indicators on homepage
- [ ] Cache enriched pubs data longer (reduce Overpass calls)
- [ ] Show toast/confetti on onboarding complete
