# Personal WhatsApp Assistant Bot

A WhatsApp bot that reads your messages, extracts TODOs, summarizes content, and responds intelligently using Claude AI.

## Features

✅ **Auto-read messages** — analyzes all incoming WhatsApp messages  
✅ **Extract TODOs** — automatically identifies action items with priorities  
✅ **Summarize** — creates summaries of messages and daily digests  
✅ **Smart responses** — replies when needed via Claude  
✅ **Persistent storage** — saves everything to Supabase  
✅ **API endpoints** — manage TODOs, view history, get digests  

## Architecture

```
Twilio WhatsApp ──▶ Flask Webhook
   ├─ Store raw message (durability first)
   ├─ Return immediate TwiML ack (👍 Got it)
   └─ Async: Process with Claude
        ├─ Extract: category, summary, TODOs
        ├─ Insert TODOs + processing log
        └─ Send follow-up message if needed
```

**Tech Stack:**
- Flask (existing fuelwatch server)
- Twilio WhatsApp API
- Supabase (PostgreSQL + RLS)
- Claude 3.5 Sonnet (AI processing)
- Railway (deployment)

---

## Setup Steps

### 1. Create Supabase Tables

Run the SQL schema in Supabase dashboard:

```bash
# Copy and paste into Supabase SQL Editor, then execute:
# File: personal_assistant_schema.sql
```

Or via CLI:
```bash
supabase db push --dry-run  # First, check what will happen
supabase db push            # Apply schema
```

**Tables created:**
- `messages` — raw inbound/outbound message log
- `message_processing` — Claude's structured extraction
- `todos` — extracted action items
- `auto_responses` — what the bot sent back
- `summaries` — daily/weekly digests
- `personal_bot_settings` — user preferences

### 2. Set Environment Variables

Add to Railway dashboard (or local `.env` for testing):

```env
# Supabase (source of truth for secrets)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key

# Twilio WhatsApp
TWILIO_ACCOUNT_SID=ACxxxxxx
TWILIO_AUTH_TOKEN=your-token
TWILIO_WHATSAPP_NUMBER=+1234567890  # Your Twilio WhatsApp number

# Claude AI
ANTHROPIC_API_KEY=sk-xxx

# API Protection (for /api/* endpoints)
PERSONAL_API_TOKEN=your-secret-token-here

# Optional: Cron digest token (for scheduled daily digest)
DIGEST_CRON_TOKEN=personal-digest-2026
```

**Get these values:**
- **Supabase**: Dashboard → Settings → API → Service Role Key
- **Twilio**: Twilio Console → Account → Auth Token + Sender WhatsApp Number
- **Claude**: Dashboard → Settings → API Keys
- **Tokens**: Generate any secure random string (e.g., `openssl rand -hex 16`)

### 3. Configure Twilio Webhook

1. Go to **Twilio Console** → **Messaging** → **Try it out** → **Send a WhatsApp message**
2. Click on your WhatsApp number (Sender)
3. Under **Webhook Settings**, set:
   - **When a message comes in**: `https://your-railway-app.up.railway.app/whatsapp/webhook`
   - Keep the default POST method
4. Save

### 4. Deploy to Railway

The bot runs on the existing Flask app in `sms_service.py`. Just push to `main`:

```bash
cd ~/fuelwatch
git add personal_assistant_bot.py personal_assistant_schema.sql PERSONAL_BOT_SETUP.md
git commit -m "feat: add personal WhatsApp assistant bot"
git push origin main
```

Railway auto-deploys on push. Check logs:
```bash
railway logs -f
```

### 5. Test the Bot

Send a WhatsApp message to your Twilio number:

```
Pick up milk from Sainsbury's tomorrow
```

Expected response:
```
👍 Got it
```

Then check the database:
```bash
# List TODOs (via Supabase dashboard or curl):
curl -H "Authorization: Bearer your-secret-token" \
  https://your-app.up.railway.app/api/todos
```

---

## API Reference

All `/api/*` endpoints require `Authorization: Bearer <PERSONAL_API_TOKEN>` header.

### GET /api/todos
List all TODOs with optional filtering.

**Query params:**
- `status`: "open" | "done" | "snoozed" | "cancelled"
- `priority`: "low" | "medium" | "high"

**Example:**
```bash
curl -H "Authorization: Bearer your-token" \
  "https://app.up.railway.app/api/todos?status=open&priority=high"
```

**Response:**
```json
[
  {
    "id": "uuid",
    "text": "Pick up milk",
    "due_date": "2026-09-15",
    "priority": "medium",
    "status": "open",
    "source_message_id": "uuid",
    "created_at": "2026-09-14T12:00:00Z",
    "updated_at": "2026-09-14T12:00:00Z"
  }
]
```

### PATCH /api/todos/<id>
Update a TODO.

**Body:**
```json
{
  "status": "done",
  "priority": "high",
  "due_date": "2026-09-16",
  "text": "Pick up milk from Sainsbury's"
}
```

**Response:** Updated TODO object

### GET /api/messages
List raw messages (for debugging).

**Query params:**
- `limit`: 1-100 (default 20)
- `offset`: pagination offset

**Response:** Array of message objects

### GET /api/summaries/daily
Fetch the latest daily summary.

**Response:**
```json
{
  "id": "uuid",
  "period_type": "daily",
  "period_start": "2026-09-14",
  "period_end": "2026-09-14",
  "summary_text": "You had 5 messages with 3 new TODOs. Main themes: shopping, reminders, questions.",
  "message_count": 5,
  "todo_count": 3,
  "created_at": "2026-09-14T23:00:00Z"
}
```

### POST /api/digest/run
Cron-triggered endpoint to build a daily digest.

**Required:** `X-Cron-Token` header or `token` query param (= `DIGEST_CRON_TOKEN`)

**Example (via cron):**
```bash
curl -X POST \
  -H "X-Cron-Token: personal-digest-2026" \
  https://app.up.railway.app/api/digest/run
```

**Response:**
```json
{
  "message": "Digest created",
  "summary": "...",
  "message_count": 5,
  "todo_count": 3
}
```

---

## Message Processing Flow

When you send a WhatsApp message:

1. **Webhook receives** → validates Twilio signature
2. **Durability first** → stores raw message in `messages` table (status='received')
3. **Sync response** → sends TwiML ack ("👍 Got it") to Twilio immediately
4. **Async processing** (background thread):
   - Calls Claude with structured extraction prompt
   - Extracts: category, summary, TODOs, needs_response, suggested_response
   - Inserts TODOs into `todos` table
   - Inserts processing log into `message_processing` table
   - If `needs_response=true`, sends follow-up WhatsApp message via Twilio REST API
   - Updates message status to 'processed' (or 'failed')

**Why this design?**
- Immediate ack keeps Twilio happy (doesn't timeout)
- Claude latency (1-3s) doesn't block webhook
- All messages persisted, even if Claude fails
- User gets sync ack + async response, clear communication

---

## Claude Extraction Prompt

The bot sends this to Claude:

```
Analyze this WhatsApp message and extract structured information.

Message: [user's text]

Return a JSON object with:
- category: "todo" | "question" | "note" | "request" | "fyi"
- summary: one or two sentence summary
- todos: array of { text, due_date (YYYY-MM-DD or null), priority ("low"|"medium"|"high") }
- needs_response: boolean (true if bot should send a follow-up)
- suggested_response: text to send back (if needs_response=true)

Only return valid JSON, no markdown or extra text.
```

**Examples:**

| Message | Extraction |
|---------|-----------|
| "Pick up milk tomorrow" | `{"category": "todo", "summary": "Need to buy milk", "todos": [{"text": "Pick up milk", "due_date": "2026-09-15", "priority": "medium"}], "needs_response": false}` |
| "What's the weather today?" | `{"category": "question", "summary": "User asking about weather", "todos": [], "needs_response": true, "suggested_response": "I can't check weather directly, but check weather.com or ask Siri!"}` |
| "Done with the project" | `{"category": "fyi", "summary": "Project completed", "todos": [], "needs_response": true, "suggested_response": "Awesome! 🎉"}` |

---

## Troubleshooting

### Messages not arriving
- ✅ Check Twilio webhook is pointing to correct URL
- ✅ Verify `TWILIO_AUTH_TOKEN` matches Twilio console
- ✅ Check Railway logs: `railway logs -f`

### TODOs not being extracted
- ✅ Check `ANTHROPIC_API_KEY` is set in Railway env vars
- ✅ Check Supabase tables exist: `supabase db list`
- ✅ Check `message_processing` table for errors in `raw_response`
- ✅ Verify Claude is receiving the message (logs show the prompt)

### API endpoints returning 401
- ✅ Check `Authorization: Bearer <token>` header is present
- ✅ Verify `PERSONAL_API_TOKEN` matches in Railway env vars
- ✅ Bearer token should NOT include "Bearer" prefix in the token itself

### Daily digest not running
- ✅ If using cron, check `DIGEST_CRON_TOKEN` is set
- ✅ Verify the cron service has access to Railway app
- ✅ Check logs for errors in digest generation

### Duplicate messages or messages lost
- ✅ Messages are deduplicated by `wa_message_sid` (Twilio MessageSid)
- ✅ If you see duplicates, check if Twilio is retrying (normal behavior)
- ✅ All messages are persisted even if processing fails

---

## Monitoring & Observability

### View all messages
```sql
select id, from_number, body, status, created_at 
from messages 
order by created_at desc 
limit 20;
```

### View today's TODOs
```sql
select text, priority, status, due_date 
from todos 
where created_at >= today() 
order by priority desc;
```

### View failed processing
```sql
select m.id, m.body, m.status, p.raw_response 
from messages m 
left join message_processing p on m.id = p.message_id 
where m.status = 'failed' 
order by m.created_at desc;
```

### Check API usage
```bash
# Fetch last 50 API calls (via logs)
railway logs -f --filter "Authorization Bearer" | tail -50
```

---

## Future Enhancements

- 📱 Support media uploads (images, documents, voice)
- 🎯 Smart due date parsing ("in 3 days", "next Monday")
- 📊 Weekly digest email
- 🔗 Integration with calendar/Notion/Slack
- 🤖 Context-aware responses (learn user's preferences)
- 💬 Conversation threading (group related messages)

---

## Support & Questions

For issues:
1. Check Railway logs: `railway logs -f`
2. Verify all env vars are set
3. Test with curl using the API endpoints
4. Check Supabase dashboard for data in tables

---

**Author:** Generated with Claude Code  
**Last Updated:** 2026-09-14
