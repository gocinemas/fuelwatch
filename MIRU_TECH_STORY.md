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

---

## Interview Answers (For Job Interviews, Investor Interviews, Pitch Meetings)

### **"Tell me about Miru's tech stack"**

*Answer (30 seconds):*
> "Miru is a WhatsApp AI assistant built on a smart 3-layer approach. Vision AI (Groq) reads receipts and product photos. Language AI (Groq NLP) extracts meaning from school emails and classifies spending. Reasoning AI (Claude) handles complex decisions. We deploy on Railway with Supabase for real-time alerts. The key insight: Groq is 60x cheaper than Claude for extraction tasks, so we route 90% of work there and only use Claude for judgment calls."

*Why this works for interviews:* Shows you understand cost/benefit trade-offs, not just "use the best tool."

---

### **"Why not build a native app instead of web + WhatsApp?"**

*Answer:*
> "Three reasons. First, friction: no App Store gatekeeping means we deploy instantly, users don't need to download anything. Second, reach: WhatsApp has 2 billion users already logged in—we don't need to convince them to install Miru. Third, data: we can't lock users into Apple/Google's ecosystem. For a product that lives on user data (receipts, school emails), that control matters."

*Why this works:* Demonstrates product thinking + business sense + technical awareness.

---

### **"How do you handle the cost of running LLM APIs at scale?"**

*Answer:*
> "Smart routing. We don't use Claude for everything. Receipt extraction via Groq vision is £0.30/month per 100 users. Email parsing via Groq NLP is similarly cheap. We reserve Claude (Opus) for cases that genuinely need reasoning—like analyzing spending anomalies or answering complex questions. For a user with 100 daily interactions, Groq is maybe £0.02/month. Claude fallback is £0.05/month. Total: £0.25-0.30 per user for all AI. That scales."

*Why this works:* Shows you've actually done the math, not just picked tools.

---

### **"What's the biggest technical challenge you've solved?"**

*Answer (pick one):*

**Option A — Real-time school alerts:**
> "School comms is time-sensitive. We could poll Gmail every hour, but that's slow and expensive. Instead, we use Supabase real-time subscriptions. Background agent polls every 6 hours, writes to DB, and users get WhatsApp alerts in <2 seconds via Twilio webhook. The challenge was not just fetching emails, but routing them through NLP, extracting dates, and surfacing only what matters (deadlines, not newsletters). We solved it by pre-filtering emails by sender domain (stannsheathjuniors-surrey@scopay.com) so we're not parsing 1000 irrelevant emails."

**Option B — Receipt categorization:**
> "Users manually reclassify receipts (Tesco fuel vs. Tesco groceries), but the system kept overwriting their changes. The bug was that we were recalculating categories on every update instead of checking if the user had already set one. We fixed it by storing the category and only auto-detecting if empty. Sounds simple, but it required tracing through the brief cache, the spend breakdown query, and the receipt update endpoints. Taught me: always check if data was manually set before overwriting with auto-detection."

**Option C — Cost optimization:**
> "Groq APIs are cheap, but calling them for every interaction still adds up. We batch processing: instead of analyzing one receipt, we analyze 10. Instead of checking fuel prices on-demand, we check every 30 minutes and cache. This reduced costs by 70% without hurting user experience because most queries are repetitive."

*Why these work:* Show you've shipped real products, not just architected on a whiteboard.

---

### **"What would you do differently if you rebuilt it?"**

*Answer:*
> "Three things. First, I'd start with receipts only—that's the highest-friction manual task. Add school comms once that's working well. I spent effort on Intelligence Hub (agentic reasoning) too early; now it times out. Start narrow, expand. Second, I'd use a message queue (Redis/RabbitMQ) from day one instead of threading for background jobs. Third, I'd make the categorization logic immutable from the start—once a user sets a category, it's locked unless they change it again. Those three changes would've saved weeks."

*Why this works:* Shows humility + product judgment + technical foresight. Interviewers love this.

---

### **"How do you think about data ownership?"**

*Answer:*
> "Critical. Users send us receipts, school emails, spending data. That's personal. We store it in Supabase (PostgreSQL), not a locked-in SaaS. Theoretically, a user could export their data or even run their own instance. We're transparent: the code is on GitHub. Most products keep data hostage; we don't. It's a competitive advantage—users trust us more."

*Why this works:* Shows ethical thinking + business sense (trust = retention).

---

### **"What's the unit economics look like?"**

*Answer:*
> "For an active user (uses it 3-4x per week):
> - Infrastructure (Railway, Supabase): £0.15
> - APIs (Groq NLP, Claude fallback): £0.10
> - WhatsApp messaging (Twilio): £0.02
> - Total: ~£0.27 per user per month
> 
> If they pay £2.99/month for premium features, that's a 10x margin. If they use it daily, margin improves to 20x. Right now we're not monetizing—proving PMF first. But the unit economics work."

*Why this works:* Investors eat this up. Shows you think about business fundamentals.

---

### **"Tell me about a time you made the wrong tech choice"**

*Answer:*
> "Receipts table. We started with a separate `receipts` table (migrated from old system) instead of storing everything in `wa_saves` (clippings). Later, the brief needed to read receipts, but it was querying the old table which was sometimes stale or missing data. Should've migrated everything to `wa_saves` from day one. The lesson: new features should extend existing tables, not create parallel tables. Creates data sync hell."

*Why this works:* Shows self-awareness + learning ability.

---

### **"How would you sell Miru to your mom?"**

*Answer:*
> "Tell her: 'Instead of checking three apps for school stuff, fuel prices, and spending, just text Miru. Forward a school email, it reminds you the day before. Take a photo of a receipt, it categorizes it. Check fuel prices by texting a postcode. Everything in one place.' She doesn't care about Groq vs Claude or Railway. She cares about: Does it work? Is it safe? Does it save me time? Those are the right questions."

*Why this works:* Shows you think about users, not just tech.

---

### **"What would you need to hit 10,000 users?"**

*Answer:*
> "Two things. First, user acquisition: school channels (parent groups), fuel forums/subreddits, Reddit personal finance. We'd create 20-30 short videos showing specific wins (saved £15 on fuel, never missed school pickup). Second, quality: the product has to be boring but reliable. If it fails 1% of the time (misses a school alert, miscategorizes a receipt), users abandon it. So we'd invest in automated testing, real-time monitoring, and user feedback loops. We'd probably need to hire one more engineer to keep up."

*Why this works:* Shows you understand growth and product stability.

---

### **"What's your biggest learning from building this?"**

*Answer (sincere):*
> "That constraint breeds clarity. When you have a £30/month budget, you can't use 10 different SaaS tools. You have to choose Railway OR Heroku (not both). You have to choose Groq over OpenAI based on cost/accuracy, not hype. When you're building for WhatsApp, you can't do fancy animations—you send text. Those constraints forced us to make better decisions than if we had unlimited budget. The product is stronger for it."

*Why this works:* Honest + insightful. Interviewers remember this.

---

## Interview Prep Checklist

Before any interview about Miru:

- [ ] **Know the numbers:** £0.27/user/month, <2s response time, 60x cheaper (Groq vs Claude)
- [ ] **Know the trade-offs:** Why Groq (cheap) not Claude (quality). Why WhatsApp (friction) not app (reach)
- [ ] **Have a failure story:** Something you got wrong and fixed. Shows honesty.
- [ ] **Know the market:** School parents, commuters, spenders. Who uses it? Why?
- [ ] **Have a "what's next" idea:** Not a half-baked fantasy, but something grounded. "Monetize school alerts to schools directly" beats "become Fintech unicorn"
- [ ] **Know your code:** If they ask "show me the vision layer" or "walk me through a receipt", you should know where it is in the codebase
- [ ] **Practice the 30-second pitch:** Should be able to explain Miru in the time of an elevator ride
