# Intel Platform - API Summary

## ✅ What's Live & Working

### 1. Company Intelligence API
**Endpoint:** `GET /api/company/intelligence?name=Nike&country=US`

Returns real company data from EDGAR/SEC:
```json
{
  "name": "Nike",
  "hq": {
    "city": "BEAVERTON",
    "country": "United States"
  },
  "industry": "Rubber & Plastics Footwear",
  "ticker": "NKE",
  "cik": "0000320187",
  "source": "EDGAR"
}
```

**Features:**
- Real SEC EDGAR data (not estimated)
- CIK lookup for 15+ major US companies
- OpenCorporates fallback
- Crunchbase support (with API key)
- Country support (US, GB, others)
- Supabase caching

---

### 2. SKU Tracking System
**Endpoints:**
- `GET /api/brand/skus?name=Nike` - List tracked SKUs
- `POST /api/brand/skus` - Add new SKU
- `DELETE /api/brand/skus/<id>` - Remove SKU

**Data stored:**
```
company_name (Nike)
sku_name (Air Max 90)
category (Footwear)
price ($129.99)
created_at (timestamp)
last_checked (timestamp)
```

**Live example:**
- Nike: 1 SKU tracked (Air Max 90)

---

### 3. Competitor Monitoring
**Endpoints:**
- `GET /api/brand/competitors?name=Nike` - List tracked competitors
- `POST /api/brand/competitors` - Track new competitor
- `DELETE /api/brand/competitors/<id>` - Stop tracking

**Data stored:**
```
tracking_company (Nike)
competitor_name (Adidas)
created_at (timestamp)
last_checked (timestamp)
```

**Live example:**
- Nike: 1 competitor tracked (Adidas)

---

## Database Tables

All data persists in Supabase:
- `company_skus` - Product tracking
- `tracked_competitors` - Competitor monitoring
- `cik_lookup` - Company CIK cache
- `ai_cache` - Company intelligence cache

---

## Testing the APIs

```bash
# Company Intelligence
curl "https://miru.humanagency.co/api/company/intelligence?name=Nike"

# Add a SKU
curl -X POST "https://miru.humanagency.co/api/brand/skus" \
  -H "Content-Type: application/json" \
  -d '{"company_name":"Nike","sku_name":"Air Max 90","category":"Footwear","price":"$129.99"}'

# Track a competitor
curl -X POST "https://miru.humanagency.co/api/brand/competitors" \
  -H "Content-Type: application/json" \
  -d '{"tracking_company":"Nike","competitor_name":"Adidas"}'
```

---

## Known Limitations

- **Company data:** Only US public companies (EDGAR). International companies need local registry integration.
- **UI:** Intel mode in index.html is mixed with Miru code (messy). Consider separate frontend.
- **Crunchbase:** Requires API key to be set in Railway environment variables.
- **Founder/leadership data:** Not available from free tiers (would need premium APIs).

---

## Future Enhancements

1. **International expansion** - Add Companies House (UK), Handelsregister (Germany), etc.
2. **Auto-monitoring** - Cron job to periodically check competitor data
3. **Price tracking** - Monitor SKU price changes over time
4. **Market analysis** - Aggregate competitor data for market insights
5. **Clean UI** - Dedicated Intel frontend with modern design

---

**Status:** Production-ready backend. APIs stable and caching effectively.
