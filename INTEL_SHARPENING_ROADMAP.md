# Intel Sharpening Roadmap 🎯

**Goal:** Transform Intel from "functional" to "sharp & polished" — enterprise-grade visual design, 100-company database, working export functions.

---

## Phase 1: Visual Sharpening (2-3 days)
### Goal: Make entire site look sharp, consistent, professional

**1.1 Design System (Tokens)**
- [ ] Create unified color palette
- [ ] Typography hierarchy (headings, body, labels)
- [ ] Spacing scale (padding, margins, gaps)
- [ ] Component library (badges, cards, buttons, modals)
- [ ] Apply to all Intel pages

**1.2 Pages to Polish**
- [x] company_compare.html — DONE (heat-map, badges, timeline)
- [x] company_qa.html — DONE (badges, legend)
- [ ] intelligence_tabbed.html — Apply heat-map + badges
- [ ] intel_brand_intelligence.html — Add visual polish
- [ ] All modals & forms — Consistent styling
- [ ] Mobile responsive across all pages

**1.3 Visual Enhancements**
- [ ] Add micro-animations (smooth transitions, hover effects)
- [ ] Improve form inputs (better focus states, validation)
- [ ] Add loading states (skeleton screens, spinners)
- [ ] Improve empty states (helpful messaging)
- [ ] Add success/error notifications (toasts)

**Success Criteria:**
- All pages use same color palette
- All badges/badges consistent across site
- Mobile responsive (tested on iPhone 12+)
- No jarring visual inconsistencies

---

## Phase 2: 100-Company Database (3-5 days)
### Goal: Load 100 real companies with complete data

**2.1 Company Selection (by sector)**
```
Consumer Goods (20):
  Reckitt, Henkel, Unilever, Nestlé, P&G, Colgate, Clorox, 
  Mondelez, Danone, Keurig, Campbell, Kraft, J.M. Smucker, 
  Hershey, Beiersdorf, Revlon, Church & Dwight, Edgewell, 
  Energizer, Nu Skin

Pharma & Health (20):
  Pfizer, Moderna, J&J, Merck, AbbVie, Bristol, Eli Lilly,
  Amgen, Gilead, Biogen, Regeneron, Viatris, Vertex,
  Alexion, Seagen, Incyte, Celgene, Agios, Clovis, Inovio

Tech & Internet (20):
  Apple, Microsoft, Google, Amazon, Meta, Netflix, Nvidia,
  Intel, AMD, Broadcom, Qualcomm, Adobe, Salesforce, 
  ServiceNow, Databricks, Figma, Stripe, Canva, Notion, Slack

Food & Beverage (10):
  Coca-Cola, PepsiCo, Red Bull, Starbucks, Chipotle, 
  Yum! Brands, Restaurant Brands, Dine Brands, Wendy's, Dine Global

Financial Services (10):
  JPMorgan, Bank of America, Wells Fargo, Goldman Sachs,
  Morgan Stanley, Berkshire, BlackRock, Vanguard, Fidelity, CME

Fashion & Luxury (10):
  LVMH, Kering, Hermès, Richemont, Brunello Cucinelli,
  EssieLux, Tapestry, Ralph Lauren, Capri, Moncler

Automotive (5):
  Tesla, Ford, GM, BMW, VW

Energy (5):
  ExxonMobil, Chevron, Shell, ConocoPhillips, Equinor

Total: 100 companies
```

**2.2 Data per Company**
- [ ] Financial history (2021-2025): revenue, margin, employees, growth
- [ ] Stock data: current price, 52-week range, market cap, P/E
- [ ] M&A history (10 years): 3-5 deals per company minimum
- [ ] Brands/products: main portfolio items
- [ ] Description: 1-line sector, location, founded year

**2.3 Bootstrap Approach**
- Create `bootstrap_100_companies.py`
- Fetch data from multiple sources:
  - Financial data: Company filings (SEC 10-K for US, annual reports)
  - Stock data: Yahoo Finance API (live)
  - M&A data: Crunchbase, Refinitiv, PitchBook summaries
  - Company info: Wikipedia, company websites
- Batch insert into Supabase
- Validate: no null financials, all have at least 1 M&A deal

**Success Criteria:**
- 100 companies in database
- Each with complete financials (no "N/A" in core metrics)
- Each with 3+ M&A deals
- Load time <2 seconds on comparison page with 4 companies

---

## Phase 3: Email + PDF Export (2-3 days)
### Goal: Make sharing seamless

**3.1 Email Export**
- [x] Backend endpoint exists: `/api/company/send-comparison-email`
- [ ] Configure SMTP (Gmail or SendGrid)
- [ ] Test with real email
- [ ] Add UI to comparison page: "📧 Email this comparison"
- [ ] Send formatted HTML email with:
  - Company summary cards
  - Metrics table
  - M&A timeline (text summary)
  - Link back to live comparison
- [ ] Track sent emails (log in database)

**3.2 PDF Export**
- [x] Backend endpoint exists: `/api/company/generate-pdf`
- [ ] Verify PDF generation library installed (reportlab or puppeteer)
- [ ] Test PDF generation with real comparison
- [ ] Enhance PDF with:
  - Company logos (if available)
  - Charts (growth, margin trends)
  - M&A timeline visualization
  - Metrics heat-map (colored table)
  - Professional header/footer
- [ ] Add UI to comparison page: "📄 Download PDF"

**3.3 Sharing Features**
- [ ] Generate shareable link (snapshot of comparison)
- [ ] URL params: `?c1=Apple&c2=Microsoft&c3=Google&c4=Amazon`
- [ ] Short link option (URL shortener integration?)
- [ ] Copy-to-clipboard for easy sharing

**Success Criteria:**
- Click "📧 Email" → email arrives in inbox within 5 seconds
- Click "📄 PDF" → PDF downloads with proper formatting
- Email/PDF include all key metrics and visuals
- Comparison data is accurate (matches web version)

---

## Phase 4: Feature Suggestions (Brainstorm)
### What else can be added to Phase 2/3?

**4.1 Watchlist & Alerts** 🔔
- Save companies to personal watchlist
- Get weekly/monthly digest of:
  - Stock momentum changes
  - M&A announcements
  - Hiring rate changes
  - Quarterly earnings surprises
- Email/Slack notifications

**4.2 Industry Benchmarking** 📊
- Compare company vs sector averages
- "Above/below average in revenue growth?"
- "How efficient is this company vs peers?"
- Percentile rankings

**4.3 Custom Reports** 📋
- Template-based reports (Sales, Corp Dev, PE)
- Quarterly digest: "How did competitors move this quarter?"
- Competitor tracking over time: "Has Reckitt's hiring accelerated?"

**4.4 M&A Opportunity Engine** 🎯
- "Who should we acquire?" — machine learning ranking
- Factors: profitability, employee cost, competitor threat, synergy potential
- M&A scoring model

**4.5 Real-time Intel Dashboard** ⚡
- Live stock price tickers
- M&A deal feed (new deals appear in real-time)
- Hiring trends (job postings spike?)
- Earnings calendar (upcoming earnings dates)

**4.6 Competitor Analysis by Market** 🗺️
- "Who's winning in APAC?"
- "Which company dominates premium beauty?"
- Geographic/category breakdowns

**4.7 AI-Powered Insights** 🤖
- "What's Reckitt's acquisition strategy?"
- "Is Henkel's hiring for organic growth or M&A integration?"
- "Predict: Which company will acquire next?"
- Pattern recognition across deals

**4.8 Export to CRM/Salesforce** 🔗
- Push comparison to Salesforce as Deal Intelligence
- Update lead profiles with competitor context
- Sync watchlist to CRM

**4.9 Heatmap Customization** 🎨
- Allow users to pick metrics to compare
- Save custom comparison templates
- Export as branded PDF (logo, colors)

**4.10 Integration with Company Intelligence Hub** 🧠
- Link to Miru's Intelligence Hub
- "How did this company move in headlines?"
- "Social sentiment for this company?"
- "Press releases + M&A deals timeline"

---

## Timeline & Resource Allocation

| Phase | Duration | Effort | Priority | Start |
|-------|----------|--------|----------|-------|
| 1: Visual Sharpening | 2-3 days | Medium | **HIGH** | Today |
| 2: 100-Company DB | 3-5 days | High | **HIGH** | After Phase 1 |
| 3: Email + PDF | 2-3 days | Medium | **MEDIUM** | Parallel with Phase 2 |
| 4: Features | 1-2 days | Low | LOW | After Phase 3 |

**Critical Path:** Phase 1 → Phase 2 → Phase 3 (8-11 days total to "sharp + complete")

---

## Success Looks Like

**For Sales:**
- Shows Intel to prospect
- Prospect says: "This looks professional. How much does it cost?"

**For Users:**
- Loads comparison in <2 seconds
- All metrics visible, no N/A values
- Charts smooth, badges clear
- Can export via email/PDF
- Looks like enterprise product (not startup MVP)

**For Database:**
- 100 companies loaded
- Any 4-company comparison <1 second
- No gaps in data
- Verified financials/M&A

---

## Next: Which phase should we start?

**Recommendation:** Start Phase 1 (Visual Sharpening) immediately. It's the fastest high-impact work. Sets foundation for Phases 2-3.

Want me to start Phase 1? 🚀
