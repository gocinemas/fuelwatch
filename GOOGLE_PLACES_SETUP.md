# Google Places API Setup Guide

**Goal:** Enable Smart Places recommendations with strict credit monitoring

---

## 1️⃣ **Get Google Places API Key**

### Step 1: Create Google Cloud Project
```
1. Go to: https://console.cloud.google.com
2. Create new project: "Miru"
3. Wait for project creation (~30s)
```

### Step 2: Enable Places API
```
1. Search for "Places API"
2. Click "Enable"
3. Wait for service activation (~1-2 min)
```

### Step 3: Create API Key
```
1. Go to: Credentials (left sidebar)
2. Click: "Create Credentials" → "API Key"
3. Copy the API key
4. Restrict key:
   - Application restrictions: HTTP referrers
   - Website restrictions: miru.humanagency.co
   - API restrictions: Places API only
```

### Step 4: Add to Railway
```
Railway Console:
- Settings → Variables
- Add: GOOGLE_PLACES_API_KEY = [your-key]
- Redeploy
```

---

## 2️⃣ **Verify Setup**

```python
# Test in Python:
from smart_places import get_smart_places

sp = get_smart_places()
venues = sp.search_venues("KT16 0DA", "restaurant")
print(venues)

# Should return:
# [
#   {name: "Thai Palace", rating: 4.5, distance_km: 1.2, ...},
#   {name: "Bella Pasta", rating: 4.5, distance_km: 1.5, ...},
#   ...
# ]
```

---

## 3️⃣ **Monitor Usage**

### In Code
```python
sp = get_smart_places()
report = sp.get_usage_report()
print(report)

# Output:
# {
#   'today': 45,
#   'daily_quota': 333,
#   'daily_used_pct': 13.5,
#   'estimated_monthly': 4500,
#   'monthly_quota': 10000,
#   'monthly_used_pct': 45,
# }
```

### Safeguards Built-In
- ✅ Daily quota: 333 requests/day (~10,000/month)
- ✅ Request caching: 6-hour TTL (minimize API calls)
- ✅ Auto-alert at 80% quota
- ✅ Will refuse requests if daily limit hit

---

## 4️⃣ **Pricing**

| Tier | Requests/Month | Cost |
|------|--------|------|
| **Free** | 15,000 | $0 (free credit) |
| **You're safe at:** | 10,000 | $0 (conservative) |
| **Overage:** | 20,000+ | $320+/month ⚠️ |

**Bottom Line:** Stay under 10,000/month = $0 charge

---

## 5️⃣ **What Counts Toward Quota?**

### ❌ Cached requests (free)
```python
sp.search_venues("KT16 0DA", "restaurant")  # $0.032 (API call)
sp.search_venues("KT16 0DA", "restaurant")  # $0 (cached 6 hours)
```

### ❌ Failed requests (free)
```python
sp.search_venues("invalid", "restaurant")  # $0 (validation fails before API)
```

### ✅ Real searches
```python
sp.search_venues("SW1A 1AA", "cafe")  # $0.032 (API call)
sp.search_venues("M1 1AE", "pub")     # $0.032 (API call)
```

---

## ⚠️ **Cost Prevention**

### If you exceed daily quota:
```
❌ No more API calls that day
✅ Uses cached results instead
✅ Shows "not available" gracefully
```

### If you exceed monthly quota:
```
Email alert → Check usage_report()
Options:
1. Stop using Smart Places (fallback to OSM)
2. Wait for next month's $200 credit
3. Pay overage ($0.032 per request)
```

---

## 🆘 **Troubleshooting**

### API Key not working
```
Error: "API_KEY not configured"
Solution: Check Railway env vars, restart app
```

### "Approaching daily quota"
```
Warning: Daily quota 80%+ used
Action: Check get_usage_report()
```

### No results returned
```
Debug:
1. Check postcode is valid
2. Try different category (restaurant vs cafe)
3. Check 2km radius isn't too small
```

---

## 📊 **Expected Usage**

```
If 100 users, each loads brief 2x/day:
- 100 users × 2 requests × 30 days = 6,000/month ✅ Within quota

If 500 users, each loads brief 2x/day:
- 500 × 2 × 30 = 30,000/month ❌ Exceeds quota
- Solution: Enable caching, use fallback for some users
```

---

## ✅ **Checklist**

- [ ] Google Cloud project created
- [ ] Places API enabled
- [ ] API key generated
- [ ] Key restricted (HTTP referrers + Places API)
- [ ] Key added to Railway env
- [ ] App redeployed
- [ ] Test query works: `sp.search_venues("KT16 0DA", "restaurant")`
- [ ] Usage report shows requests tracked

---

Once setup, Smart Places will automatically replace the hardcoded mock data in Brief! 🎯
