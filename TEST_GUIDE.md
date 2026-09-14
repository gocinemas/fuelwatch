# Personal WhatsApp Bot — Testing Guide

**Goal:** Verify end-to-end that messages → Claude extraction → TODOs work correctly.

**Time estimate:** 20-30 minutes

---

## Step 1: Gather Credentials (5 min)

You'll need values for these env vars. Get them now:

### Supabase
1. Go to [Supabase Dashboard](https://app.supabase.com)
2. Select your project → **Settings** → **API**
3. Copy:
   - `Project URL` → `SUPABASE_URL`
   - `Service Role Secret` (NOT the anon key!) → `SUPABASE_KEY`

### Twilio
1. Go to [Twilio Console](https://console.twilio.com)
2. Copy:
   - **Account SID** → `TWILIO_ACCOUNT_SID`
   - **Auth Token** → `TWILIO_AUTH_TOKEN`
3. Go to **Messaging** → **Try it out** → **Send a WhatsApp message**
4. Click on your WhatsApp sender number
5. Copy the phone number (e.g., `+1234567890`) → `TWILIO_WHATSAPP_NUMBER`

### Claude API
1. Go to [Claude Dashboard](https://console.anthropic.com)
2. **Settings** → **API Keys** → **Create Key**
3. Copy → `ANTHROPIC_API_KEY`

### Generate Secret Tokens
```bash
# Generate two random tokens:
openssl rand -hex 32  # Copy → PERSONAL_API_TOKEN
openssl rand -hex 32  # Copy → DIGEST_CRON_TOKEN
```

---

## Step 2: Set Environment Variables in Railway (5 min)

1. Go to [Railway Dashboard](https://railway.app)
2. Select your Miru/FuelWatch project
3. Click **Variables** tab
4. Add these 8 variables:

```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=eyJhbGc...  (your service role key)
TWILIO_ACCOUNT_SID=ACxxxxxxx
TWILIO_AUTH_TOKEN=your-token
TWILIO_WHATSAPP_NUMBER=+1234567890
ANTHROPIC_API_KEY=sk-ant-...
PERSONAL_API_TOKEN=your-generated-token-1
DIGEST_CRON_TOKEN=your-generated-token-2
```

5. Click **Deploy** (if it shows the button)

**Note:** Railway will redeploy the app with new env vars. This takes 1-2 minutes.

---

## Step 3: Deploy Supabase Schema (3 min)

1. Go to [Supabase Dashboard](https://app.supabase.com) → Your project
2. Click **SQL Editor** (left sidebar)
3. Click **+ New Query**
4. Copy the entire contents of `personal_assistant_schema.sql` from the repo
5. Paste into the query editor
6. Click **Run**

**Expected output:** Green checkmark, no errors

**Tables created:**
- ✅ messages
- ✅ message_processing
- ✅ todos
- ✅ auto_responses
- ✅ summaries
- ✅ personal_bot_settings

---

## Step 4: Configure Twilio Webhook (3 min)

1. Go to [Twilio Console](https://console.twilio.com)
2. **Messaging** → **Try it out** → **Send a WhatsApp message**
3. Click your WhatsApp sender number
4. Under **Webhook Settings** → **When a message comes in**:
   - **URL:** `https://your-railway-app.up.railway.app/whatsapp/webhook`
   - **Method:** POST (default)
5. Click **Save**

**Note:** You can find your Railway app URL:
- Railway Dashboard → Project → Settings → Domain

---

## Step 5: Test the Webhook (2 min)

Send a WhatsApp message to your Twilio number:

```
Pick up milk from Sainsbury's tomorrow
```

**Expected response (instant):**
```
👍 Got it
```

This is the sync TwiML ack. Claude is processing in the background.

**Wait 2-3 seconds** for Claude to process.

---

## Step 6: Verify TODOs Were Extracted (5 min)

### Option A: Via Supabase Dashboard
1. Go to [Supabase](https://app.supabase.com) → Your project
2. **Table Editor** → Click **todos** table
3. You should see a row:
   - `text: "Pick up milk from Sainsbury's"`
   - `priority: "medium"`
   - `due_date: "2026-09-15"` (tomorrow's date)
   - `status: "open"`

### Option B: Via API
```bash
API_TOKEN="your-secret-token"
APP_URL="https://your-railway-app.up.railway.app"

curl -H "Authorization: Bearer $API_TOKEN" \
  "$APP_URL/api/todos"
```

**Expected response:**
```json
[
  {
    "id": "uuid",
    "text": "Pick up milk from Sainsbury's",
    "due_date": "2026-09-15",
    "priority": "medium",
    "status": "open",
    "source_message_id": "uuid",
    "created_at": "2026-09-14T12:34:56Z"
  }
]
```

### Option C: Via Supabase SQL
```sql
select text, priority, due_date, status 
from todos 
where created_at > now() - interval '5 minutes'
order by created_at desc;
```

---

## Step 7: Send More Test Messages (5 min)

Test various message types to verify extraction quality:

**Test 1: Simple task**
```
Call John at 2pm
```
Expected: 1 TODO "Call John at 2pm", no due_date (relative), priority medium

**Test 2: Multiple tasks**
```
Go grocery shopping, pay electricity bill, submit report
```
Expected: 3 TODOs

**Test 3: Question (should get a response)**
```
What's the best time to buy tech stocks?
```
Expected: Message category="question", needs_response=true, bot replies with a suggestion

**Test 4: Urgent task**
```
URGENT: Fix the production server NOW
```
Expected: TODO with priority="high"

**Test 5: Event/Note**
```
Meeting with Sarah scheduled for Friday 3pm
```
Expected: Category="note", due_date="2026-09-19" (next Friday)

---

## Step 8: Check Message Processing Log (3 min)

See what Claude extracted:

### Via Supabase
1. Go to **message_processing** table
2. Find your test messages
3. Check `raw_response` JSON column to see Claude's full extraction

### Via Supabase SQL
```sql
select m.body, mp.summary, mp.category, mp.raw_response
from messages m
join message_processing mp on m.id = mp.message_id
order by m.created_at desc
limit 5;
```

---

## ✅ Verification Checklist

- [ ] Env vars set in Railway
- [ ] Supabase schema deployed (6 tables exist)
- [ ] Twilio webhook URL configured
- [ ] Sent test message, got "👍 Got it" ack
- [ ] TODO appeared in Supabase todos table
- [ ] API `/api/todos` returns data
- [ ] Claude extraction looks good (summary, category, todos array)
- [ ] Multiple test messages worked
- [ ] No errors in Railway logs

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| No "Got it" response | Check Twilio webhook URL is correct |
| No TODO in database | Check Railway logs: `railway logs -f` |
| API returns 401 | Verify `Authorization: Bearer <token>` header |
| Empty todos table | Check if message_processing has errors in `raw_response` |
| Claude errors | Check `ANTHROPIC_API_KEY` is valid |
| Supabase connection fails | Check `SUPABASE_URL` and `SUPABASE_KEY` are correct |

---

## 📊 Sample Test Results

After sending 5 test messages, your database should look like:

**messages table:**
```
| from_number | body | status | created_at |
|---|---|---|---|
| whatsapp:+447700900123 | Pick up milk tomorrow | processed | 2:34 PM |
| whatsapp:+447700900123 | Call John at 2pm | processed | 2:35 PM |
| whatsapp:+447700900123 | What's the best time... | processed | 2:36 PM |
```

**todos table:**
```
| text | priority | due_date | status | source_message_id |
|---|---|---|---|---|
| Pick up milk | medium | 2026-09-15 | open | uuid-1 |
| Call John | medium | NULL | open | uuid-2 |
```

**message_processing table:**
```
| message_id | category | summary | needs_response |
|---|---|---|---|
| uuid-1 | todo | Need to buy milk | false |
| uuid-2 | todo | Call John at 2pm | false |
| uuid-3 | question | Asking for investment advice | true |
```

---

## 🎉 Success Criteria

✅ **You're good to go if:**
1. Messages arrive and get instant ack
2. TODOs are extracted and stored in Supabase
3. API returns TODO list correctly
4. Claude extraction is accurate (99% correct categories/summaries)
5. No errors in Railway logs

---

## Next Steps After Testing

Once testing is successful:

1. **Document for others** — Create simple setup guide for beta users
2. **Share credentials** — Give others Railway/Supabase/Twilio access (or provide setup instructions)
3. **Monitor logs** — Watch for errors over 24 hours
4. **Tune prompt** — Adjust Claude's extraction prompt if needed based on real usage
5. **Roll out to all** — Once stable, open to all users

---

**Testing started:** [Date/Time]  
**Status:** 🟡 In Progress
