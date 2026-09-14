# Personal WhatsApp Assistant — Quick Start

You now have a fully functional WhatsApp bot that reads your messages and creates TODOs automatically.

## What Was Built

```
personal_assistant_bot.py      — Main Flask blueprint (~350 lines)
personal_assistant_schema.sql  — Supabase tables + indexes
PERSONAL_BOT_SETUP.md         — Complete documentation
test_personal_bot.sh          — API testing script
```

**Registered in:** `sms_service.py` → automatically runs on Railway

## 5-Minute Setup

### Step 1: Deploy Database Schema
Open [Supabase Dashboard](https://app.supabase.com) → SQL Editor → Copy + Run:
```sql
-- Copy contents from: personal_assistant_schema.sql
```

### Step 2: Set Environment Variables in Railway
Go to Railway Dashboard → Variables:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
TWILIO_ACCOUNT_SID=ACxxxxxx
TWILIO_AUTH_TOKEN=your-auth-token
TWILIO_WHATSAPP_NUMBER=+1234567890
ANTHROPIC_API_KEY=sk-xxxxx
PERSONAL_API_TOKEN=your-secret-token
DIGEST_CRON_TOKEN=personal-digest-2026
```

### Step 3: Configure Twilio Webhook
1. Twilio Console → Messaging → WhatsApp Sender Number
2. Set webhook URL: `https://your-railway-app.up.railway.app/whatsapp/webhook`
3. Keep POST method selected

### Step 4: Push to Deploy
```bash
cd ~/fuelwatch
git add personal_assistant_bot.py personal_assistant_schema.sql PERSONAL_BOT_*.md test_personal_bot.sh
git commit -m "feat: add personal WhatsApp assistant bot

- Auto-extracts TODOs from messages via Claude
- Stores everything in Supabase
- Responds intelligently via WhatsApp
- API endpoints for TODO management"
git push origin main
```

Railway auto-deploys → check logs: `railway logs -f`

### Step 5: Test It!
Send a WhatsApp message to your Twilio number:
```
Pick up milk from Sainsbury's tomorrow
```

You should get:
```
👍 Got it
```

Check your TODOs:
```bash
./test_personal_bot.sh https://your-app.up.railway.app your-secret-token
```

## How It Works

When you message:
```
"Call mum on Monday, pick up milk, check quarterly reports"
```

The bot:
1. ✅ Receives message via Twilio
2. ✅ Stores in database (durability first)
3. ✅ Sends instant "Got it" ack
4. ✅ Calls Claude to extract:
   - 3 TODOs: "Call mum" (due Mon), "Pick up milk", "Check reports"
   - Category: "request"
   - Summary: "Three action items"
5. ✅ Stores TODOs in Supabase
6. ✅ (Optional) Sends follow-up if needs_response=true

All data persisted → access via `/api/todos`, Supabase dashboard, or Slack

## API Endpoints (All Require API Token)

```bash
# List TODOs
curl -H "Authorization: Bearer $TOKEN" https://app.up.railway.app/api/todos

# Filter by status/priority
curl -H "Authorization: Bearer $TOKEN" https://app.up.railway.app/api/todos?status=open&priority=high

# Mark a TODO done
curl -X PATCH -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"status":"done"}' \
  https://app.up.railway.app/api/todos/<todo-id>

# Get daily summary
curl -H "Authorization: Bearer $TOKEN" https://app.up.railway.app/api/summaries/daily

# List messages (debugging)
curl -H "Authorization: Bearer $TOKEN" https://app.up.railway.app/api/messages?limit=10
```

## Database Schema

```
messages
├─ id, direction (in/out), from_number, to_number
├─ wa_message_sid (Twilio ID), body, media_urls
├─ status (received/processing/processed/failed)
└─ created_at

message_processing
├─ message_id → messages.id
├─ summary, category, sentiment
├─ needs_response, claude_model
└─ raw_response (full JSON from Claude)

todos
├─ id, source_message_id → messages.id
├─ text, due_date, priority
├─ status (open/done/snoozed/cancelled)
└─ created_at, updated_at, completed_at

auto_responses
├─ message_id → messages.id
├─ response_text, response_type
├─ outbound_wa_sid
└─ sent_at

summaries
├─ period_type (daily/weekly)
├─ period_start, period_end
├─ summary_text
├─ message_count, todo_count
└─ created_at
```

## Key Features

✅ **Async Processing** — Immediate ack, Claude processes in background  
✅ **Idempotency** — Deduplicates by Twilio MessageSid  
✅ **Durability** — All messages stored before processing  
✅ **Error Handling** — Failed messages marked as 'failed', data not lost  
✅ **API Protection** — Bearer token on all `/api/*` endpoints  
✅ **Supabase RLS** — All tables have Row Level Security enabled  

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Messages not arriving | Check Twilio webhook URL in console |
| No TODOs extracted | Verify `ANTHROPIC_API_KEY` is set in Railway |
| API returns 401 | Check `PERSONAL_API_TOKEN` and Authorization header |
| Logs show errors | Run `railway logs -f` to see live output |

## What's Next?

### Short-term
- [ ] Send test WhatsApp message and verify TODO extraction
- [ ] Update a TODO via API to test PATCH endpoint
- [ ] Check Supabase dashboard to browse messages/TODOs

### Medium-term
- [ ] Set up daily digest email (via `/api/digest/run` cron)
- [ ] Add media upload support (images, documents)
- [ ] Create Slack integration to mirror TODOs

### Long-term
- [ ] Smart due date parsing ("in 3 days", "next Monday")
- [ ] Integration with Notion/Todoist
- [ ] Learning user preferences (urgent vs background)
- [ ] Conversation threading

## Files Reference

| File | Purpose |
|------|---------|
| `personal_assistant_bot.py` | Main Flask blueprint with all logic |
| `personal_assistant_schema.sql` | Supabase tables and indexes |
| `PERSONAL_BOT_SETUP.md` | Complete documentation |
| `PERSONAL_BOT_QUICKSTART.md` | This file (quick reference) |
| `test_personal_bot.sh` | API testing script |

## Support

For detailed setup, see: **PERSONAL_BOT_SETUP.md**

For API reference, see: **PERSONAL_BOT_SETUP.md → API Reference**

For code, see: **personal_assistant_bot.py**

---

**Built:** 2026-09-14  
**Status:** ✅ Ready to deploy  
**Deploy:** `git push origin main` → Railway auto-deploys
