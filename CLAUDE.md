# Miru / fuelwatch

## V2 — Agentic Architecture (started May 2026)
**Shift:** from ask-and-receive tiles → context-aware surface that knows what matters *now*

**Core concept:** Context Engine → Brief Card on home (replaces static greeting for returning users)

**V2 building blocks:**
- `/api/home/brief` — parallel-fetches trains + fuel + school events + spend → Groq writes 2-3 line narrative
- `/api/v2/prefs` GET/POST — user preference store (usual train route, fuel postcode, commute mode)
- Prefs stored in `ma_details` as `type="v2_prefs"` — single JSON blob per user
- Home brief card — shimmer while loading, narrative text replaces greeting, "Personalise" CTA
- WhatsApp morning push (Phase 2) — proactive brief at 7:30am for opted-in users
- Pattern learning (Phase 3) — detect repeated queries → offer to make default

**Deployed:** miru.humanagency.co (Railway, auto-deploys on `git push main`)
**Repo:** github.com/gocinemas/fuelwatch

## Railway Deployment
- Project: **zestful-education** | ID: `d114e3c5-e1e8-4e3c-9249-fa78f182bcda`
- Service: **web** | Environment: production
- Normal: `git push` → auto-deploys in ~2 min via GitHub webhook
- **If auto-deploy stops working** (webhook broken, trial expired, etc.):
  ```
  railway link --project d114e3c5-e1e8-4e3c-9249-fa78f182bcda
  railway up --service web --detach
  ```
- ⚠️ Dashboard "Redeploy" button reruns OLD code — does NOT pick up new commits. Always use `railway up` or fix the GitHub webhook instead.

## Stack
- `sms_service.py` — Flask routes + WhatsApp handler (Twilio)
- `school_service.py` — Gmail OAuth polling, Groq (llama-3.1-8b-instant) event extraction, Supabase storage
- `templates/index.html` — single-page web app
- Supabase: `school_profiles`, `school_events`, `ma_gmail_tokens`, `ma_provider_hints`, `ma_details`

## Three Products on this repo
- **Miru** — WhatsApp AI assistant (miru.humanagency.co)
- **Intel** — brand & company intelligence (intel.humanagency.co)
- **AI** — AI literacy (ai.humanagency.co)

## Identity Keys (localStorage)
- `miru_postcode` — identity bar postcode (no spaces, uppercase)
- `miru_phone` — linked phone number
- `_miruPostcode()` / `_miruPhone()` — JS getters
- `_maCurrentPostcode` — in-memory current My Area search postcode (separate from identity)

## My Area Architecture
- `_loadMyArea()` — called on every nav to My Area; early-returns if `_maCurrentPostcode` set but re-triggers `myAreaSearch()` if `_maAreaData` is null
- `myAreaSearch()` — clears data, fires all API calls via `_guard(searchGen)`
- `_maFetchCached(key, url, ttl, onData)` — LS-cached fetch
- Tabs: Places (weather+train), Services (GP+shops+pubs+schools), Civic (reps+council+crime), Accounts

## Gmail Scan
- `_MA_GMAIL_QUERIES` — (type, gmail_query) tuples including catch-all subject queries
- `_ma_gmail_scan_bg()` — background thread
- Fatal OAuth errors → clear tokens, set `scan_status="auth_error"`, show reconnect UI

## School Comms
- Riaan: Stanns Heath Junior School — scopay emails via `stannsheathjuniors-surrey@scopay.com`
- Inaaya: New Haw Junior School — `office@new-haw.surrey.sch.uk`
- Poll token: `miru-digest-2026` | Poll URL: `/api/school/poll?token=miru-digest-2026&days_back=7`
- Force re-poll: append `&force=true&days_back=30`

## Sub-agent model: prefer haiku for all file search/grep tasks in this repo
