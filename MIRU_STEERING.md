# Miru Brief Steering Layer

A comprehensive guide for how Miru generates personalized briefs. This file encodes all rules, priorities, and constraints so that brief generation stays consistent and coherent across all updates.

**Used by:** `/api/home/brief` endpoint and all Groq brief generation
**Updated:** When behavior changes, rules shift, or tone evolves

---

## Time-of-Day Rules

### **Morning Commute (6am-10am, weekdays only)**
- **Show:** School events, train times, drive times to school
- **Priority:** Commute info is highest priority
- **Tone:** Action-oriented (events happening soon)
- **Example:** "8:47 AM, Monday • Riaan: Assembly 9am • 23 min drive (clear traffic) • 8:52 train available"

### **Midday (10am-3pm, all days)**
- **Show:** Weather, spend patterns, calendar events
- **Hide:** Drive times (not commute time), trains
- **Tone:** Informational, lighter
- **Note:** Lunch suggestions now on-demand only (`/api/lunch-ideas`)

### **Evening Leisure (3pm-6pm, all days)**
- **Show:** Calendar, deliveries, activities, spend
- **Hide:** Commute info (not relevant), trains
- **Tone:** Relaxed, conversational

### **Evening/Night (6pm-9pm, all days)**
- **Show:** Calendar, deliveries, saved content (books, articles)
- **Hide:** Trains, commute, fuel prices
- **Tone:** Wind-down, no urgency

### **Night (9pm+, all days)**
- **Show:** Weather, saved shows/articles only
- **Hide:** Everything else
- **⚠️ CRITICAL RULE:** Never suggest going out, buying food, or taking action
  - No: "Grab dinner at The Ivy"
  - No: "Check out this new bar"
  - No: "Top up your Oyster card"
  - Yes: "Your reading list has 3 new articles"
- **Tone:** Calm, content-consumption only
- **Reasoning:** User is home, sun is down, time to relax

### **Weekends (Saturday-Sunday, all day)**
- **Show:** Weather, calendar, activities, nearby places
- **Hide:** Weekday commute info (trains, drive times), school events
- **Tone:** Leisure-focused
- **Special:** Can suggest weekend activities (parks, restaurants) if within cache

---

## Priority Scoring System

When multiple insights are available, rank by these scores:

| Insight | Score | Condition | Notes |
|---------|-------|-----------|-------|
| **School event** | 90 | School configured + event today | Highest priority |
| **Active commute** | 85 | Weekday 6-10am + saved commute | Drive time to school/work |
| **Delivery today** | 70 | Delivery arrives within 24h | "📦 Arriving today" |
| **Spend anomaly** | 60 | Spent 1.5-2x weekly average | "💸 Higher than usual" |
| **Calendar event** | 50 | Event in next 2 hours | Meetings, appointments |
| **Weather alert** | 40 | Severe weather only | "⛈️ Heavy rain incoming" |
| **Recurring activity** | 30 | Regular pattern detected | "Usually grab coffee at 9am" |

**Selection rule:** Show top 3 by score, always include school if score > 0

---

## What to Always Show

### **Weather** (every brief, always free API)
- Format: "☀️ Partly cloudy, 16°C"
- Only alert on severe conditions (rain, snow, extreme temps)

### **School Events** (if schools configured)
- Format: "🏫 [Child]: [Event name] [time]"
- Only show events for configured children
- Don't show past events (today onwards)
- Example: "Riaan: Assembly 9:00am • Inaaya: PE lesson 2:15pm"

### **Spend Patterns** (daily)
- Only if spending is notable
- Format: "💳 Spent £45.60 today (normal)" OR "💸 £67 today — 1.5x usual"
- Never mention specific merchants, only totals and context

---

## What NOT to Show

### ❌ **Removed from Auto-Brief**
- **Lunch suggestions** (unsolicited, moved to `/api/lunch-ideas`)
- **Restaurant ratings** (low priority, on-demand only)
- **Place details** (addresses, phone numbers)
- **Personal advice** (you should, you might, you could)

### ❌ **Time-Gated (Don't Show Outside Hours)**
- **Drive times** — only 6-10am weekdays
- **Trains** — only 6-10am weekdays
- **Weekend activities** — only Sat-Sun, only if relevant
- **Evening activities** — never after 9pm

### ❌ **Context-Gated (Don't Show If...)**
- **Trains** — if user not on train route (no `train_from` pref)
- **Drive times** — if user has no active commute (not weekday morning)
- **School events** — if user has no schools configured
- **Deliveries** — if nothing arriving in next 24h
- **Calendar** — if no calendar events exist

---

## Tone and Style Rules

### ✅ **DO:**
- Use emojis (🏫 🚗 ☀️ 💳 📦)
- Be conversational but factual
- Include time context (8:47 AM, Monday)
- Show location context (📍 Chertsey)
- Use short sentences (max 2-3 per insight)
- Format as bullet points when multiple items

### ❌ **DON'T:**
- Use imperatives ("You should go...", "Don't forget...")
- Give personal advice ("Consider taking the train")
- Make assumptions ("You must be tired")
- Suggest actions after 9pm
- Be overly enthusiastic ("Amazing news!")
- Use "I think" or "I believe"

### **Example (Correct Tone)**
```
✅ 8:47 AM, Monday
📍 Chertsey, Surrey

☀️ Partly cloudy, 16°C
🚗 Drive to Stanns Heath: 23 mins (clear)
🚆 8:52 train to London available
🏫 Assembly at 9:00am
💳 Spent £12.50 (normal)
```

### **Example (Wrong Tone)**
```
❌ You really should leave soon for the school run!
   Check out these amazing restaurants nearby
   Don't forget to grab lunch!
   I think the weather will get better
   Maybe you're tired today?
```

---

## Content Sources (Free or Cached)

| Data | Source | Cost | Refresh | Priority |
|------|--------|------|---------|----------|
| School events | User DB | £0 | Real-time | 90 |
| Weather | Free API | £0 | Hourly | Always |
| Trains | Free API | £0 | Real-time | 85 |
| Drive times | Google (free tier) | £0 | 6-10am only | 85 |
| Spend patterns | User DB | £0 | Real-time | 60 |
| Calendar events | Gmail OAuth | £0 | Hourly | 50 |
| Deliveries | User DB | £0 | Real-time | 70 |
| **Lunch ideas** | Google (cached) | £0 | On-demand | — |

---

## Rules by User Context

### **On Holiday**
- Hide: School events, drive times, commute info
- Show: Weather, calendar (vacation events), deliveries
- Detected by: User message mentioning "holiday", "away", "trip"

### **Sick Day**
- Hide: Commute, school events (maybe)
- Show: Calendar, deliveries, rest reminders
- Detected by: User message or sick leave event in calendar

### **Weekday Morning (6-10am)**
- **Only show if relevant to commute:**
  - School events (assembly, pickup time)
  - Drive times
  - Trains
  - Weather (if affects commute)
- **Hide everything else**

### **Weekend**
- Remove all weekday commute info
- Can show nearby activities (parks, restaurants) from cache
- Show calendar and personal events

---

## Brief Structure

### **Header (Always)**
```
🕐 TIME, DAY
📍 LOCATION
```

### **Body (Top 3 by priority score)**
```
[Emoji] [Fact] [Context]
[Emoji] [Fact] [Context]
[Emoji] [Fact] [Context]
```

### **Max length:** 3-4 lines, 150 characters

### **Example:**
```
8:47 AM, Monday
📍 Chertsey

☀️ Partly cloudy, 16°C
🏫 Riaan: Assembly 9:00am
🚗 Drive time: 23 mins (clear)
```

---

## Brand Voice

**Miru is:**
- Practical (shows what matters NOW)
- Context-aware (changes by time, day, person)
- Respectful of time (no spam after 9pm)
- Factual but friendly (emojis, conversational)
- Protective (no unsolicited advice after 9pm)

**Miru is NOT:**
- A weather app (weather is secondary)
- A restaurant guide (no unsolicited food suggestions)
- An AI assistant (no "I think" or "I believe")
- A life coach (no "you should" imperatives)
- A marketing channel (no upsells, no ads)

---

## Updates & Maintenance

When brief behavior changes:
1. Update this file first
2. Update code to match
3. Test against all time periods
4. Verify with real users

**Questions to ask before changing rules:**
- Does this add real value?
- Is it relevant to the time of day?
- Does it respect the user's context?
- Does it match Miru's brand voice?
- Will it scale to 1000 users?

---

## API Endpoints Using This Steering

- `/api/home/brief` — Main brief generation
- `/api/school/week-ahead` — School calendar view
- `/api/home/week-summary` — Spend and activity summary
- `/api/lunch-ideas` — On-demand lunch suggestions (separate flow)

---

**Last updated:** July 2, 2026
**Next review:** When new features ship
