# Intel Phase 2 Requirements & Roadmap

## Status: MVP Live ✅

Current state: 85+ brands, demo pricing data, company directory

---

## Feature Requirements Parking Lot

### 1. Real-Time Pricing Data 🛒

| Requirement | Priority | Status | Notes |
|------------|----------|--------|-------|
| Amazon India pricing | HIGH | 🔴 Parked | Blocked by bot detection; 503 errors |
| BigBasket pricing | HIGH | 🔴 Parked | 403 Forbidden - active bot detection |
| Nykaa pricing | HIGH | 🔴 Parked | 403 Forbidden - active bot detection |
| Price history tracking | MEDIUM | 🟡 Ready | `price_scrapes` table created |
| Daily auto-update | MEDIUM | 🟡 Ready | Cron job infrastructure ready |

### 2. Data Source Options

#### Option A: Selenium Web Scraper
```
Status: 🟡 Built but needs testing
Cost: $0 (open source)
Effort: Medium
Reliability: Medium (70-80%)
Speed: Slow (5-10 min per brand)
Implementation: scraper_india_prices_selenium.py created

Pros:
- ✅ No API keys needed
- ✅ Works around bot detection
- ✅ Headless browser = more human-like

Cons:
- ❌ Slow (headless Chrome overhead)
- ❌ Fragile (HTML structure changes break it)
- ❌ High CPU usage on Railway
- ❌ Site updates break selectors frequently

Deployment: Deploy as Railway background worker
```

#### Option B: Official E-commerce APIs
```
Status: 🔴 Not implemented
Cost: $100-500/month
Effort: High (integration work)
Reliability: High (95%+)
Speed: Fast (1-2 min per brand)

Providers:
- Amazon Product Advertising API
  * Requires AWS account
  * Requires brand authorization
  * $0.005 per request (~$150/month for daily scrape)

- BigBasket API
  * Check if public API exists
  * May require partnership

- Nykaa API
  * Check if public API exists
  * May require partnership

Pros:
- ✅ Official data, most accurate
- ✅ Fast and reliable
- ✅ 99.9% uptime SLA
- ✅ No bot detection issues

Cons:
- ❌ Costs money
- ❌ Need API keys/credentials
- ❌ Rate limits apply
- ❌ Complex authentication
```

#### Option C: Third-Party Scraping Service
```
Status: 🔴 Not implemented
Cost: $20-200/month
Effort: Low (just API integration)
Reliability: High (90%+)
Speed: Medium (2-5 min per brand)

Services:
- ScrapingBee
  * $50/month for 250k requests
  * Handles bot detection automatically
  * Simple REST API

- Apify
  * $45/month for 200 actor runs
  * Pre-built e-commerce extractors
  * Excellent for Amazon/BigBasket

- ScrapeHero
  * $99/month base
  * Amazon/Nykaa integration
  * Best for fashion/beauty

Pros:
- ✅ Handles bot detection
- ✅ Fast setup
- ✅ Good reliability
- ✅ Affordable for startups

Cons:
- ❌ Monthly cost
- ❌ Vendor lock-in
- ❌ Limited customization
- ❌ API rate limits
```

---

## Implementation Timeline

### Phase 1 (Current) ✅
- [x] Brand database with 85+ brands
- [x] Company/category/brand search
- [x] Individual brand pages (structure ready)
- [x] Demo pricing data
- [x] Social media & news data
- [x] `price_scrapes` table created
- [x] Scraper code written (Selenium version)

### Phase 2 (When Revenue Arrives 💰)
- [ ] Choose data source option (A/B/C)
- [ ] Implement price scraping
- [ ] Set up daily cron job
- [ ] Update brand pages with real prices
- [ ] Add "Last updated" timestamp
- [ ] Email/PDF export with real data

### Phase 3 (Future)
- [ ] Market size data (Nielsen API)
- [ ] Competitor pricing comparison
- [ ] Price trend charts
- [ ] Alerts for price changes
- [ ] Market research data integration

---

## Recommendation

**Current (No Budget):** ✅ Keep demo data
- Intel is fully functional
- Users can explore brands
- Shows product value

**When You Get First Customer:** 💰
1. Start with **Option C (Scraping Service)** - fastest ROI
   - Deploy in 1 week
   - Cost: ~$50/month
   - Reliability: 90%+
   - Low maintenance

2. If successful, migrate to **Option B (Official APIs)**
   - Better long-term
   - Better reliability
   - Better accuracy
   - Scalable as you grow

3. Avoid **Option A (Selenium)** for production
   - Too maintenance-heavy
   - Breaks frequently
   - High CPU usage
   - Better for startups with dev team

---

## Blockers Currently
- 🔴 E-commerce sites blocking bot access (expected)
- 🔴 No budget for API subscriptions (wait for revenue)
- 🔴 Selenium too slow for many brands (future optimization)

## Next Steps When Revenue Arrives
1. Sign up for ScrapingBee/Apify ($50/month)
2. Update scraper with API credentials
3. Deploy cron job on Railway
4. Test with 5-10 brands
5. Monitor price accuracy
6. Expand to all brands once validated

---

## Cost Summary Table

| Option | Setup Cost | Monthly Cost | Effort | Speed | Reliability |
|--------|-----------|-------------|--------|-------|-------------|
| **A: Selenium** | $0 | $0 | Medium | Slow | 70% |
| **B: Official APIs** | $500 | $100-500 | High | Fast | 95%+ |
| **C: Scraping Service** | $0 | $20-200 | Low | Medium | 90% |
| **Demo Data** | $0 | $0 | Done | N/A | 100% (fake) |

---

**Parked on:** 2026-06-23
**Review when:** Revenue arrives or decision to invest in real data
**Owner:** Intel Product Team
