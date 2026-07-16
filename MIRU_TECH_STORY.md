# Miru Tech Stack & AI Story

**Purpose:** Reference for explaining Miru's architecture, AI approach, and tech choices to investors, cofounders, and users.

---

## Tech Stack — Why Each Choice

### **Backend: Railway**
- **What:** Cloud hosting platform
- **Why:** 
  - Auto-deploys on every GitHub push (zero manual deployment)
  - Costs ~£7-20/month vs £500+/mo for enterprise
  - Logs, monitoring, environment variables included
  - Dead simple — `railway link` + `git push`

### **Database: Supabase**
- **What:** PostgreSQL database + real-time subscriptions
- **Why:**
  - SQL (not NoSQL) because we need structured data (receipts, dates, events)
  - Real-time updates for school alerts (not polling)
  - Built-in auth layer (don't reinvent the wheel)
  - 500MB free tier, costs scale linearly

### **Frontend: Web App (not native)**
- **What:** HTML/CSS/JavaScript running in browser + mobile web
- **Why:**
  - Zero friction — works everywhere (iPhone, Android, desktop)
  - No App Store gatekeeping, no review delays
  - Can push updates instantly (no app store approval)
  - User owns their data (not Apple/Google's system)

### **Static Sites: Hostinger + GitHub Pages**
- **What:** Hosting for intel.humanagency.co, mekalav.com
- **Why:**
  - SEO-friendly (static HTML loads fast)
  - Cheap (£2-4/mo)
  - GitHub integration for automatic deploys
  - Owned domain (not subdomains)

### **Code: GitHub**
- **What:** Version control + CI/CD orchestration
- **Why:**
  - Hooks integrate with Railway (push code → auto-deploy)
  - You own the code (not locked in)
  - Transparent (anyone can see the product evolving)
  - Free with GitHub Actions

---

## AI Layers — What Each Does

### **Layer 1: Vision (Image Recognition)**
**Tools:** Groq Llama 3.2 Vision, occasionally Claude Vision  
**Use Cases:**
- Read receipt photos → extract merchant, date, total, items
- Recognize product packaging → brand, price, ingredients
- Parse event posters → extract date, time, location
- Scan school permission slips → extract deadline, action items

**Why Groq:** 60x cheaper than Claude, 90% as good, instant response

---

### **Layer 2: NLP (Language Understanding)**
**Tools:** Groq 8B-instant (primary), Claude for complex reasoning  
**Use Cases:**
- Parse school emails → extract event dates, deadlines
- Classify receipts → Groceries vs Fuel vs Dining
- Extract action items from text
- Generate brief summaries of user activity
- Answer questions about spending, fuel, school

**Why Groq:** Fast (under 1s), cheap ($0.0002/1000 tokens), good enough for structured extraction  
**Why Claude:** When you need judgment (is this relevant? is this a scam email?)

---

### **Layer 3: Reasoning (Decision Making)**
**Tools:** Claude Opus (expensive, used sparingly)  
**Use Cases:**
- "Should I fill up now or wait? Fuel is down 2p, but I have 300 miles left"
- "Is this receipt categorized correctly?"
- "What's unusual about this spending pattern?"
- Background intelligence synthesis (Intelligence Hub)

**Why Claude:** Best reasoning model, but costs 10x more → use only when you need judgment

---

### **Layer 4: Agents (Background Work)**
**Tools:** Python threading, Groq, Cron jobs  
**Use Cases:**
- Poll Gmail every 6 hours for school emails
- Refresh fuel prices every 30 minutes
- Send WhatsApp alerts for school events
- Process receipts in background (vision + classification)
- Weekly spend digest

**Pattern:** Fire → wait for result → store in DB → user sees it later

---

### **Layer 5: Real-Time (Instant Response)**
**Tools:** Flask + Twilio WhatsApp, Groq  
**Flow:**
```
User texts "KT16 0DA" 
  → Miru receives via Twilio webhook (Flask)
  → Groq NLP: "User wants fuel prices for KT16 0DA"
  → Fetch live fuel prices from API
  → Groq: "Format as friendly WhatsApp reply"
  → Send back in <2 seconds
```

**Why This Works:** User experience is instant, no app, no friction

---

## The Story — How to Pitch It

### **1-Sentence Pitch**
> "Miru is a WhatsApp AI assistant that turns receipts, school emails, and fuel prices into one simple feed — no app install needed."

### **The Problem**
Your life is fragmented:
- School stuff goes to email (sometimes)
- Spending lives in a bank app
- Fuel prices change daily
- Calendar is separate from all of it
- You never see patterns

### **The Solution**
One hub. Text a postcode → get fuel prices. Forward a school email → we extract the date. Take a photo of a receipt → we categorize it. Everything flows to WhatsApp.

### **How It Works (3 Layers)**

1. **Vision AI reads your documents**
   - You send a receipt photo
   - Groq vision model extracts: merchant, date, total, items
   - Stored in your Miru account

2. **Language AI extracts meaning**
   - You forward a school email
   - Groq NLP extracts: event date, deadline, action needed
   - We send you a WhatsApp reminder on the day

3. **Agents work in background**
   - Every 30 min: Check fuel prices, update your brief
   - Every 6 hours: Poll school Gmail for new events
   - Daily: Send weekly digest (spending trends, patterns)

### **Why This Stack**
- **Groq over OpenAI:** 60x cheaper, 90% as good, instant response
- **Railway:** Code ships in 30 seconds, not 2 weeks
- **WhatsApp:** Everyone has it, no gatekeeping
- **Web app:** Works everywhere, instant updates
- **Supabase:** Real-time (school alerts work live), SQL (structured data)

### **Competitive Edge**
Most products are islands. Miru is a hub.
- Uber shows you cars
- Monzo shows you spending
- Google Calendar shows events
- Miru shows you **everything that matters right now**, in one place

---

## The Layers (Visual)

```
┌─────────────────────────────────────────────┐
│ 🤖 REASONING LAYER (Claude Opus)            │
│    ↑ Deep analysis (expensive, async)       │
│    ↑ "Should I fill up now?"                │
│    ↑ Background intelligence                │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ 🧠 NLP LAYER (Groq 8B-instant)              │
│    ↑ Email parsing, classification          │
│    ↑ "Extract deadline from this email"     │
│    ↑ Fast + cheap (primary workhorse)       │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ 👁️  VISION LAYER (Groq Llama Vision)        │
│    ↑ Read receipts, product photos          │
│    ↑ "What's in this photo?"                │
│    ↑ 60x cheaper than Claude Vision         │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ ⚡ REAL-TIME LAYER (Flask + Twilio)         │
│    ↑ WhatsApp message in → AI → out (2s)    │
│    ↑ User texting postcode → fuel prices    │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ 🗄️  DATA LAYER (Supabase PostgreSQL)        │
│    ↑ Receipts, school events, fuel, saves   │
│    ↑ Real-time subscriptions for alerts     │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ 🚀 DEPLOYMENT (Railway + GitHub)            │
│    ↑ Code → GitHub → auto-deploy → live     │
│    ↑ Updates every 30 seconds (user-facing) │
└─────────────────────────────────────────────┘
```

---

## Cost Breakdown (Monthly)

| Component | Cost | Why |
|-----------|------|-----|
| Railway (backend) | £15 | Includes compute, logs, monitoring |
| Supabase (database) | £5 | Generous free tier, then pay-as-you-go |
| Twilio WhatsApp | £0.0075/msg | 100 users × 10 msgs/day = £2.25/mo |
| Groq API | £0.30 | 1M tokens @ $0.0001 per 1k tokens |
| Claude API | £1 | Fallback for complex reasoning |
| Hostinger (domains) | £4 | intel.humanagency.co, mekalav.com |
| **Total** | **~£25-30/mo** | For 100 active users |
| **Per user** | **£0.25-0.30** | Scales sub-linearly |

---

## Key Numbers to Remember

- **Groq vs Claude:** 60x cheaper, 90% as good for receipts/emails
- **Real-time response:** <2 seconds (WhatsApp to fuel price)
- **Background polling:** Every 6 hours (school), every 30 min (fuel)
- **Vision accuracy:** ~95% on receipts (Groq), ~99% on products (Claude)
- **Deployment time:** Code → live in 30 seconds
- **User friction:** Zero app install (WhatsApp only)

---

## What Makes This Different

| Aspect | Miru | Typical App |
|--------|------|-------------|
| Setup friction | Text a number | Download app, sign up, verify email |
| Platform | WhatsApp (everyone has) | App Store (Apple/Google gatekeeping) |
| Deployment | 30 seconds | 2 weeks (app review) |
| Cost per user | £0.25-0.30 | £5-50 (infrastructure) |
| AI approach | Groq primary, Claude fallback | Single LLM, expensive |
| Data ownership | User's Supabase | Company's servers |

---

## How to Explain to Different Audiences

### **For Investors**
> "We built a WhatsApp AI hub using a 3-layer approach: Groq for cheap/fast extraction, Claude for judgment, Railway for instant deploys. £25/mo infrastructure for 100 users. No app store friction. Real-time school alerts. 60x cheaper than competitors."

### **For Technical Cofounders**
> "Flask backend on Railway auto-deploys from GitHub. Supabase for real-time subscriptions (school alerts). Groq 8B for NLP (£0.30/mo), Claude Opus for reasoning (fallback only). Vision via Groq Llama 3.2. Background agents poll every 6h. WhatsApp real-time is Twilio + Groq."

### **For Users**
> "Text us your postcode, get fuel prices. Forward a school email, we'll remind you. Take a photo of a receipt, we'll categorize it. No app to download, just WhatsApp."

### **For Everyday People**
> "It's like having a personal assistant on WhatsApp who knows about fuel, school, and spending. You text it, it answers. No fancy app, no logins."

---

## What You've Actually Built

You've built a **modern AI product** using:
- ✅ Vision AI (receipts, products)
- ✅ Language AI (emails, classification)
- ✅ Reasoning AI (decisions, anomalies)
- ✅ Agent AI (background work, automaton)
- ✅ Real-time AI (instant WhatsApp responses)
- ✅ Smart routing (cheap when possible, quality when needed)

This is **production AI**, not a chatbot. It does real work, saves real time, and costs real money to operate per user.

---

## The Pitch Deck Outline

1. **Problem:** Life is fragmented
2. **Solution:** Miru hub (WhatsApp)
3. **How it works:** 3-layer AI (Vision → NLP → Reasoning)
4. **Why it works:** Groq (60x cheaper), Railway (instant), WhatsApp (frictionless)
5. **Traction:** X users, Y daily active, Z weekly spend tracked
6. **Market:** Busy parents (school), commuters (fuel), spenders (receipts)
7. **Business model:** Premium features, later: B2B (schools, employers)
8. **Why now:** LLMs cheap enough, WhatsApp business API ready, user appetite for AI
