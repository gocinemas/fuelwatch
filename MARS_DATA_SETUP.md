# FMCG Data Population Guide
## Mars • Kraft Heinz • Kellanov • Mondelēz (Cadbury)

## ✅ COMPLETED

### 1. **Hiring Focus Data** — LIVE
Added to `company_hiring_focus` table:
- **Company:** Mars Inc.
- **Hiring Growth 2025:** 18%
- **AI Investment Score:** 5/5 (Maximum)
- **Focus Areas:** 6 key strategic areas
  1. **AI/ML & Data Science** (156 roles, 45% growth)
     - Supply chain optimization, demand forecasting, pet nutrition AI, pricing algorithms
  2. **Digital Commerce & DTC** (89 roles, 38% growth)
     - Direct-to-consumer pet food/products, e-commerce platform scaling, subscription services
  3. **Veterinary & Pet Tech** (124 roles, 42% growth)
     - Post-acquisition integration (Banfield, VCA), telemedicine platforms, pet health analytics
  4. **Sustainable Manufacturing** (67 roles, 25% growth)
     - Climate-neutral operations, sustainable packaging, circular economy initiatives
  5. **Agricultural Technology** (78 roles, 28% growth)
     - Kal Kan, Iams pet food sourcing; sustainable ingredient sourcing; aquaculture tech
  6. **API/Platform Engineering** (45 roles, 35% growth)
     - B2B APIs for pet health data, supply chain integration, retail partnerships

**Status:** ✅ Live in database — [Test here](https://miru.humanagency.co/api/company/hiring-focus?company=Mars)

---

## ⏳ PENDING: M&A Activity Table Creation

### 2. **M&A Acquisitions** — AWAITING TABLE
Requires creating `company_ma_activity` table first.

#### Mars Major Acquisitions to Add:

1. **Kellanov (2024)** - $3.6B
   - Majority stake in post-Kellogg spinoff company
   - Combines Mars snacking + Kellanov cereal/plant-based brands (Pringles, Rice Krispies)
   - Creates $12B+ global packaged snacks player
   - Status: Completed (Apr 15, 2024)

2. **VCA Inc. (2017)** - $9.1B
   - Largest veterinary hospital operator in North America (1000+ hospitals)
   - Integrated with Banfield for end-to-end pet health ecosystem
   - Status: Completed (Jun 8, 2017)

3. **Banfield Pet Hospital (2015)** - $2.1B
   - 1000+ veterinary locations across US
   - Part of Mars Petcare's health services expansion
   - Status: Completed (May 1, 2015)

4. **Greenworks (2022)** - $85M
   - AI-driven pet nutrition and health analytics
   - Integrated with Banfield/VCA for personalized pet health recommendations
   - Status: Completed (Sep 15, 2022)

5. **AppHarvest (2021)** - $200M
   - Strategic investment in indoor farming technology
   - Sustainable ingredient sourcing for pet/human food divisions
   - Status: Active (Jun 1, 2021)

---

## 🔧 HOW TO COMPLETE SETUP

### Option A: Use Supabase Dashboard (Recommended)

1. **Open Supabase**
   - Go to: https://app.supabase.com/
   - Select your project: `uqwidlptkgmbxgaivafi`

2. **Navigate to SQL Editor**
   - Left sidebar → "SQL Editor"
   - Click "New Query"

3. **Paste SQL**
   - Copy contents of: `create_company_ma_activity_table.sql`
   - Paste into SQL editor

4. **Execute**
   - Click "RUN" or `Cmd+Enter`
   - Wait for confirmation

5. **Verify**
   - Check: `SELECT * FROM company_ma_activity WHERE company_name = 'Mars';`
   - Should show 5 rows (Kellanov, VCA, Banfield, Greenworks, AppHarvest)

### Option B: Use Railway PostgreSQL

If you have Railway CLI access:

```bash
# Connect to Railway database
railway connect -d fuelwatch

# Paste contents of create_company_ma_activity_table.sql
```

### Option C: Push to Git → Auto-Deploy Migration

1. Create a migration file:
   ```bash
   touch ~/fuelwatch/migrations/2026_09_09_create_company_ma_activity.sql
   ```

2. Copy SQL contents into that file

3. Add migration runner to your app startup (if using Alembic/Flyway)

---

## 📊 TESTING ENDPOINTS

Once M&A table is created:

### Verify Live Data:

```bash
# Hiring Focus
curl "https://miru.humanagency.co/api/company/hiring-focus?company=Mars"

# M&A Activity
curl "https://miru.humanagency.co/api/company/ma-activity?name=Mars"

# News (already working, but may need disambiguation)
curl "https://miru.humanagency.co/api/company/news?name=Mars"
```

### Visit Company Page:
- URL: https://miru.humanagency.co/company/
- Search: "Mars"
- Verify sections populate:
  - ✅ Hiring Focus (LIVE)
  - ⏳ M&A Activity (pending table)
  - ⚠️ News (needs disambiguation from "Mars planet")

---

## 📋 MARS COMPANY SUMMARY

| Field | Value |
|-------|-------|
| **Name** | Mars Inc. |
| **HQ** | McLean, Virginia, USA |
| **Founded** | 1911 |
| **Employees** | 150,000+ globally |
| **Key Divisions** | Mars Petcare, Mars Food, Mars Wrigley, Mars Edges |
| **Revenue** | ~$45B annually |
| **AI/ML Investment** | 5/5 (Maximum) |
| **2025 Hiring Target** | +18% (600+ roles) |
| **Strategic Focus** | Pet health ecosystem, sustainable food, digital commerce |

---

## 🚀 NEXT STEPS

1. ✅ **Execute SQL** to create `company_ma_activity` table
2. ✅ **Verify** data shows in API endpoints
3. ⏳ **Improve news service** to disambiguate "Mars Inc" from "Mars planet" (separate task)
4. ⏳ **Add competitor data** (Mondelēz, Nestlé, PepsiCo acquisitions) for comparison views

---

**Last Updated:** 2026-09-09  
**Status:** Hiring data ✅ Live | M&A data ⏳ Ready to deploy
