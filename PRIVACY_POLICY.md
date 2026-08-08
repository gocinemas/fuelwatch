# Miru Privacy Policy

**Last Updated:** 8 August 2026

---

## 📍 What Data We Collect

### Location Data
When you open Miru with GPS enabled, we collect:
- **Latitude/Longitude** — Your exact GPS coordinates
- **Place Name** — Reverse-geocoded location (e.g., "Slough")
- **Timestamp** — Date, time, day of week, hour
- **Postcode** — If available
- **Duration** — How long you used Miru at that location

**Stored in:** `place_visits` table  
**Retention:** Indefinite (no auto-delete)  
**Purpose:** Analytics, recommendations, pattern learning

### Receipt & Spending Data
- **Merchants** — Where you shop/eat
- **Items purchased** — What you buy
- **Amounts** — How much you spend
- **Dates** — When purchases happened
- **Payment method** — Type (Visa, Mastercard, etc.)

**Stored in:** `wa_saves`, `receipts` tables  
**Retention:** Indefinite  
**Purpose:** Ask Miru, spending analytics, recommendations

### School Communications
- **School emails** — Extracted events from school comms
- **Child names** — Linked to events
- **Event details** — Dates, times, notes
- **Holiday dates** — School calendar events

**Stored in:** `school_events` table  
**Retention:** Indefinite  
**Purpose:** Brief alerts, calendar sync

### Calendar & Events
- **Personal events** — Your calendar entries
- **Recurring activities** — Swimming, football, etc.
- **Timestamps** — When activities occur

**Stored in:** `personal_events` table  
**Retention:** Indefinite  
**Purpose:** Brief context, schedule tracking

### Search History (Implicit)
- **Questions to Ask Miru** — What you ask about
- **API lookups** — Fuel prices, venues searched

**Stored in:** Server logs  
**Retention:** 30 days  
**Purpose:** Debug, performance monitoring

---

## 🔐 How We Protect Your Data

✅ **Encryption** — All data in transit (HTTPS)  
✅ **Row-Level Security** — Only you can see your data  
✅ **No sharing** — Never shared with third parties  
✅ **No ads** — No ad targeting or profiling  
✅ **No selling** — Your data is never sold  

---

## 🎮 Your Privacy Controls

### Location
- **Browser setting:** Chrome → Settings → Privacy → Site Settings → Location
- **Phone setting:** Disable location permission for browser
- **Effect:** Miru will use postcode instead (recommendations still work)

### Search History
- Server logs auto-delete after 30 days
- You can request manual deletion

### Data Export
- Request export of all your data (CSV format)
- Contact: support@humanagency.co

### Data Deletion
- Request deletion of receipts, events, location history
- Contact: support@humanagency.co
- **Note:** Deletion is permanent and cannot be undone

---

## 📊 Potential Future Uses of Data

We collect location data for potential future features:

🟡 **Approved uses** (may implement):
- Location-based recommendations ("Popular on Fridays in Slough")
- Routine learning ("You visit Slough Tuesday evenings")
- Travel insights ("You spend £X per month traveling")
- Personalized alerts ("Traffic delays near your usual route")

🔴 **Prohibited uses** (will never do):
- Sell location data to third parties
- Target ads based on location
- Share with insurance/health companies
- Use for surveillance/tracking

---

## 💬 Questions?

**Privacy concerns?** Email: privacy@humanagency.co  
**Data request/deletion?** Email: support@humanagency.co  
**Found a bug?** File issue on GitHub

---

## 📋 Compliance

- ✅ GDPR compliant (EU residents have full rights)
- ✅ CCPA compliant (California residents)
- ✅ No cookies for tracking (only session auth)
- ✅ Accessible privacy controls

---

## Changes to This Policy

We'll notify you of major changes via email.  
Last modified: 8 August 2026
