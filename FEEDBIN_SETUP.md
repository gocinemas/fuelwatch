# Feedbin Integration — Stealth Mode Setup

**Status:** 🤫 Stealth (no UI, quietly adds to brief)  
**Feature:** Send 3 random starred links in morning WhatsApp brief

---

## 🔐 Get Feedbin API Token

### Step 1: Sign into Feedbin
```
1. Go: https://feedbin.com
2. Login with your subscription
```

### Step 2: Get API Token
```
1. Settings → Account
2. Scroll to "API Token"
3. Copy your token (looks like: abc123xyz...)
```

### Step 3: Add to Railway
```
Railway Console:
- Settings → Variables
- Add: FEEDBIN_API_TOKEN = [your-token]
- Redeploy
```

---

## ✨ How It Works

### Morning Brief
```
When you load your morning brief:

Your Brief
│
├─ Weather: 17°C
├─ Trains: Arriving 08:45
├─ School: Riaan swimming today
│
└─ 📖 Feedbin Picks (3 random starred links)
   📖 "How To Build AI Products" 
      https://...
   📖 "JavaScript Performance Tips"
      https://...
   📖 "Startup Funding Landscape 2026"
      https://...
```

### Stealth Details
- ✅ Quietly added to brief context
- ✅ No new UI element (appears naturally)
- ✅ Random 3 links each morning
- ✅ Fetched from your Feedbin subscription
- ✅ Auto-categorized by topic
- ✅ Cached 1 hour (avoid repeated API calls)

---

## 📚 Available Features

### Auto-Categorization
```
Links are categorized:
- 🔬 Science (arxiv, papers)
- 💻 Tech (GitHub, dev blogs)
- 📰 News (BBC, Reuters)
- 🎨 Design (Figma, UX)
- 📚 Learning (tutorials, courses)
- 🎬 Media (videos, podcasts)
- 💼 Business (startups, VC)
- 🌍 Travel (hotels, destinations)
- 🍔 Food (recipes, restaurants)
- ⚽ Sports (games, athletes)
```

### Search Links (Hidden API)
```python
from feedbin_sync import get_feedbin
fb = get_feedbin()
results = fb.search_entries(entries, "javascript")
# Returns all starred links mentioning "javascript"
```

### Browse by Category
```python
fb = get_feedbin()
entries = fb.sync_all_starred()
categorized = fb.categorize_all(entries)
# Returns:
# {
#   "💻 Tech": [...],
#   "📚 Learning": [...],
# }
```

### Get Random Links
```python
fb = get_feedbin()
entries = fb.sync_all_starred()
random_5 = fb.get_random_links(entries, count=5)
# Returns 5 random links
```

---

## 🔄 Sync Frequency

- **Morning brief:** Fetches fresh on every load (cached 1 hour)
- **Cache:** Local 1-hour TTL (avoid hammering Feedbin API)
- **Search:** Always fresh (no cache)

---

## 🤔 FAQ

### Why stealth mode?
- Clean brief (doesn't clutter with new UI)
- Groq naturally incorporates links into narrative
- Simple & elegant integration

### What if I don't have starred links?
- Brief shows nothing (graceful fallback)
- No error, no noise

### Can I turn it off?
- Just remove `FEEDBIN_API_TOKEN` from Railway
- Revert sms_service.py integration
- Brief works normally without it

### How many API calls does this use?
- ~30-60 per day (1 call per brief load, cached 1 hour)
- Feedbin has no strict rate limits (very generous)
- Totally fine with subscription

---

## 🛠️ Manual Testing

```bash
# Test connection
python3 << 'EOF'
from feedbin_sync import get_feedbin
import os

token = os.getenv("FEEDBIN_API_TOKEN")
if not token:
    print("❌ FEEDBIN_API_TOKEN not set")
else:
    fb = get_feedbin(token)
    entries = fb.sync_all_starred()
    print(f"✅ Found {len(entries)} starred entries")
    
    if entries:
        print(f"\nFirst entry: {entries[0].get('title')}")
        random = fb.get_random_links(entries, 3)
        print(f"\n3 Random links:")
        for link in random:
            print(f"  - {link.get('title')[:60]}")
EOF
```

---

## 📊 Integration Points

| Component | File | Line | Action |
|-----------|------|------|--------|
| Sync module | `feedbin_sync.py` | — | Fetch/categorize links |
| Brief integration | `sms_service.py` | ~16480 | Add links to context |
| Morning brief | WhatsApp | — | Display 3 random links |

---

## ✅ Checklist

- [ ] Get Feedbin API token from https://feedbin.com/settings/account
- [ ] Add FEEDBIN_API_TOKEN to Railway
- [ ] Redeploy
- [ ] Load morning brief and look for 📖 links section
- [ ] Test manual query: `get_feedbin().sync_all_starred()`

---

**Once set up:** Every morning brief will include 3 random links from your Feedbin starred items. No UI changes, just seamlessly integrated. 🤫
