# Cache Setup Checklist — Complete These 3 Steps

## ✅ Done (Already Deployed)
- [x] Disabled all Google APIs completely
- [x] Created `cache_layer.py` with Overpass integration
- [x] Updated brief to use cached places
- [x] Added `/api/cache/refresh` endpoint
- [x] Deployed to Railway

## 🔧 TO DO: Create the Cache Table

### Step 1: Go to Supabase Dashboard
1. Visit: https://app.supabase.com/
2. Select your Miru project
3. Click **SQL Editor** (left sidebar)
4. Click **New Query**

### Step 2: Paste This SQL

```sql
-- Create places_cache table
CREATE TABLE IF NOT EXISTS public.places_cache (
  id BIGSERIAL PRIMARY KEY,
  postcode TEXT NOT NULL UNIQUE,
  data JSONB NOT NULL,
  cached_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  expires_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for fast lookups
CREATE INDEX idx_places_cache_postcode ON public.places_cache(postcode);
CREATE INDEX idx_places_cache_expires ON public.places_cache(expires_at);

-- Enable row-level security
ALTER TABLE public.places_cache ENABLE ROW LEVEL SECURITY;

-- Allow read/write from app
CREATE POLICY "places_cache_public" ON public.places_cache
  FOR ALL USING (true);
```

### Step 3: Click **Run**

You should see: `Query successful`

---

## 📋 TO DO: Set Up Daily Cache Refresh

### Option A: Using Railway Cron (Recommended)

```bash
# Run this in your terminal:
curl -X POST https://miru.humanagency.co/api/cache/refresh \
  -H "Content-Type: application/json"
```

You should get back:
```json
{
  "postcodes_refreshed": 5,
  "postcodes_failed": 0,
  "total": 5
}
```

### Option B: Scheduled Trigger (Optional)

We can set up a cron job to refresh at 2 AM daily. Let me know if you want this.

---

## 🧪 TO DO: Test It Works

### Test 1: Check Cache Exists
```bash
curl "https://miru.humanagency.co/api/places/cached?postcode=KT160DA" | jq .
```

Should return empty `{}` initially (cache is empty until refreshed).

### Test 2: Refresh Cache
```bash
curl -X POST https://miru.humanagency.co/api/cache/refresh | jq .
```

Should show postcodes refreshed.

### Test 3: Check Cache Now Has Data
```bash
curl "https://miru.humanagency.co/api/places/cached?postcode=KT160DA" | jq .
```

Should now return:
```json
{
  "postcode": "KT16 0DA",
  "restaurants": [
    {"name": "...", "distance_mi": 0.5},
    {"name": "...", "distance_mi": 0.8}
  ],
  "cafes": [...],
  "bars": [...],
  "parks": [...]
}
```

---

## 📊 Cost Savings

**Before**: £73 for 8 days (Places API)
**After**: £0 (Overpass API is free)

**Monthly savings**: ~£300/month

---

## ❓ Troubleshooting

**Q: "places_cache table doesn't exist" error**
A: Run the SQL from Step 2 above

**Q: Cache is empty after refresh**
A: Check that users have postcodes set in their preferences

**Q: Overpass API is slow**
A: Normal (first run). Caching fixes this (future requests are instant)

---

## 🎯 Next: Monitor

Once daily for 3 days, check:
```bash
curl -X POST https://miru.humanagency.co/api/cache/refresh | jq .
```

Should consistently show a number > 0 for `postcodes_refreshed`.

That's it! Your Google API bill will now be **£0** forever.
