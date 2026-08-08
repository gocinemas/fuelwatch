# Miru Test Checklist — For New Users

Run through this before considering Miru production-ready for a user.

## 1. Ask Miru (Critical) ✅

### Receipt Queries
- [ ] "What did I order at [restaurant]?" → Returns correct items + date + price
- [ ] "When did I have [item]?" → Finds correct receipt chronologically
- [ ] "What did I order today?" → Returns today's receipt only
- [ ] "How much did I spend?" → Shows total + breakdown by merchant

### Edge Cases
- [ ] Ask without saving any receipt → Returns "not found" (not hallucination)
- [ ] Ask for non-existent item → Clear "didn't find" message
- [ ] Multiple receipts same merchant → Returns most recent
- [ ] Old receipts still accessible → Chronological sorting works

---

## 2. School Comms (Critical) ✅

### Gmail Integration
- [ ] Gmail OAuth connects without errors
- [ ] Comms appear in school section within 6 hours
- [ ] Can see both children's schools
- [ ] Permission letter tracking works (if applicable)

### Email Parsing
- [ ] Event titles extracted correctly
- [ ] Dates parsed right (handles various email formats)
- [ ] Notes/descriptions captured
- [ ] Holiday dates recognized

---

## 3. Fuel Prices (Critical) ✅

### Lookup
- [ ] Can search by postcode (with/without spaces)
- [ ] Shows 5 nearest stations
- [ ] Prices display in pence
- [ ] Updates every 30 min (cached)

### My Area
- [ ] Fuel appears in My Area services
- [ ] Prices reflect current cache

---

## 4. V2 Brief (Critical) ✅

### Context Loading
- [ ] Loads within 3 seconds
- [ ] Shows weather for postcode
- [ ] Trains display correct route
- [ ] School events appear
- [ ] Spend shows current month

### Cache Behavior
- [ ] Refreshing briefly shows "loading"
- [ ] ?refresh=1 bypasses cache
- [ ] 23:00-05:00 always fresh (goodnight hours)

---

## 5. Receipts & Spending (Important) 🟡

### PDF Imports
- [ ] Can upload receipt PDFs
- [ ] Items extract correctly
- [ ] Categorization is accurate (Dining/Coffee/Shopping)
- [ ] Duplicates detected (same amount, same day)

### Spending View
- [ ] Monthly breakdown shows all receipts
- [ ] "Last ordered at X" links work
- [ ] Can recategorize receipts
- [ ] Totals calculate correctly

---

## 6. School Events (Important) ✅

### Event Management
- [ ] Can create manual events
- [ ] Recurring activities save (e.g., swimming on Tuesdays)
- [ ] Holiday dates show correctly
- [ ] Events appear in brief

---

## 7. Personal Events (Important) 🟡

### Calendar Sync
- [ ] Can add personal events
- [ ] Time + date captured correctly
- [ ] Can set reminders/notes
- [ ] Events show in brief

---

## 8. Saves/Library (Secondary) 🔴

### Clipping
- [ ] Can save articles/links
- [ ] Categorize as Book/Article/Recipe/Other
- [ ] Search works (by title, category)
- [ ] Edit/delete functionality

---

## 9. Music ID (Secondary) 🟡

### Shazam Integration
- [ ] Can identify songs
- [ ] Can save to library
- [ ] Browse saved songs
- [ ] Metadata (artist, album) correct

---

## 10. Local Finder (Secondary) 🟡

### Venue Search
- [ ] Can search nearby pubs/restaurants/shops
- [ ] Results show location + phone
- [ ] Maps link works
- [ ] Hours/ratings display if available

---

## Post-Testing Checklist

- [ ] No console errors in browser (F12 → Console)
- [ ] No Railway errors in logs
- [ ] Response times reasonable (<3s for most queries)
- [ ] Tested from mobile browser
- [ ] Tested on different network (not just WiFi)

---

## Known Issues to Document

- Receipt OCR: Quality depends on image (no handwriting)
- School emails: Sensitive to format changes (may miss some)
- Fuel prices: 30-min cache (not live)
- Intel: Brand data still being collected (features limited)

---

## Performance Targets

| Feature | Target | Current |
|---------|--------|---------|
| Ask Miru | <2s | ✅ <1s |
| Brief Load | <3s | ✅ ~2s |
| Fuel Lookup | <2s | ✅ ~1s |
| Receipt Upload | <5s | ✅ ~3s |
| School Poll | 6h cron | ✅ Live |

---

## Rollout Strategy

### Phase 1: Friends (5 users)
- Run full checklist
- Gather feedback
- Fix critical bugs

### Phase 2: Early Access (20 users)
- Limited features (Ask Miru + Brief)
- Monitor crash reports
- Iterate UX

### Phase 3: Public Beta (All users)
- All features available
- Gradual rollout
- Support portal ready
