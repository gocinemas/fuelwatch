# Brand Research Worker Setup

## Overview
The brand research worker is a **background service** that processes brand requests independently from Flask. This is much more reliable than daemon threads because it survives app restarts and doesn't block web requests.

## Architecture

```
User submits brand request → Flask stores in DB → Worker picks up → Processes → Updates DB → Sends email
```

**Flow:**
1. **Flask API** (`/api/intel/request-brand`) → stores request with `status="pending"`
2. **Worker loop** → polls database every 5 seconds
3. **Agent** → researches brand via Groq LLM
4. **Database** → adds brand across UK/USA/India
5. **Email** → notifies user when done
6. **Status** → updates to `"collected"` or `"failed"`

## Running Locally

```bash
# Terminal 1: Start Flask web server
python sms_service.py

# Terminal 2: Start brand worker (in same directory)
python brand_worker.py
```

Output:
```
🚀 Brand Worker Started
   Supabase: https://...
   Poll interval: 5s
   Started: 2026-06-23 12:34:56

[CYCLE 1] No pending requests (12:34:56)
[CYCLE 2] No pending requests (12:35:01)
[CYCLE 3] 1 pending request(s) found
============================================================
🔄 PROCESSING: Nutella (snacks)
   Email: user@example.com
   Created: 2026-06-23
============================================================
🤖 [AGENT] Starting process_request: Nutella (category=snacks, email=user@example.com)
🤖 [AGENT] Researching Nutella...
🤖 [AGENT] Research complete: snacks brand
🤖 [AGENT] Adding Nutella to database...
🤖 [AGENT] Nutella added across UK, USA, India
✅ SUCCESS: Nutella added to database
```

## Running on Railway

### Option 1: Add as separate service (Recommended for production)

1. **Push the code:**
   ```bash
   git push origin main
   ```

2. **In Railway dashboard:**
   - Go to: `zestful-education` project
   - Click "New Service"
   - Connect to: `gocinemas/fuelwatch` repo
   - Set **Start Command:** `python brand_worker.py`
   - Environment variables are already shared from the web service
   - Deploy ✅

3. **Verify it's running:**
   - Logs should show: `🚀 Brand Worker Started`
   - Check cycles every 5 seconds
   - When requests come in, you'll see `🔄 PROCESSING:` logs

### Option 2: Add to existing Procfile (Simpler)

The Procfile has been updated:
```
web: gunicorn sms_service:app ...
worker: python brand_worker.py
```

On Railway, this will automatically spin up both services.

## Environment Variables

The worker uses these env vars (already set on Railway):

```env
SUPABASE_URL=...
SUPABASE_KEY=...
GROQ_API_KEY=...
BRAND_WORKER_INTERVAL=5          # Poll every 5 seconds (optional)
BRAND_WORKER_MAX_RETRIES=3       # Max retries per request (optional)
SENDER_EMAIL=...                  # For email notifications (optional)
SENDER_PASSWORD=...               # Gmail app password (optional)
```

## Monitoring

### Real-time logs on Railway:
```
Dashboard → zestful-education → Services → worker → Logs
```

Look for:
- `🚀 Brand Worker Started` — worker is alive
- `🔄 PROCESSING:` — working on a brand
- `✅ SUCCESS:` — brand added successfully
- `❌ ERROR:` — something failed

### Check request status:
```bash
# Local (with env vars set)
python monitor_brand_agent.py --watch --interval=2

# On Railway (via psql or UI)
SELECT * FROM brand_data_requests ORDER BY created_at DESC;
```

## Testing

### Submit test requests:
```bash
python test_brand_agent.py
# Submits 5 brands: Nutella, Tesla, Dyson, Lululemon, Airbnb
```

### Debug a single brand:
```bash
python debug_agent.py
# Manually triggers agent on Nutella
```

### Monitor progress:
```bash
python monitor_brand_agent.py        # Single status check
python monitor_brand_agent.py --watch  # Live monitoring
```

## Troubleshooting

### Worker not starting?
Check logs for:
- ❌ `Supabase credentials not set` → env vars not loaded
- ❌ `GROQ_API_KEY not set` → can't research brands
- ❌ Connection errors → Railway networking issue

### Requests stuck on "pending"?
- Check worker logs for `ERROR` or `FAILED`
- Verify Groq API is responding: `debug_agent.py`
- Check if worker process is actually running

### Request shows "failed"?
- Worker caught an exception
- Check logs for `❌ ERROR processing`
- Common issues:
  - Groq LLM returned bad JSON
  - Database insert failed (duplicate?)
  - Email send failed (credentials wrong?)

## Performance Notes

- **Poll interval:** 5 seconds (configurable via `BRAND_WORKER_INTERVAL`)
  - Lower = faster processing, higher CPU/DB load
  - Higher = slower processing, lower resource usage
- **Concurrency:** Worker processes 1 request at a time (no parallelization)
  - This prevents Groq rate limiting
  - Prevents database lock contention
- **Memory:** ~150MB (Python + Groq client + Supabase client)
- **CPU:** Minimal when idle, ~20-30% when processing

## Best Practices

✅ **DO:**
- Monitor logs regularly for errors
- Set up alerts for `❌ ERROR` in logs
- Test new features locally with `brand_worker.py`
- Use email notifications so users know when done

❌ **DON'T:**
- Run multiple workers on same database (they'll conflict)
- Kill the worker process without draining pending requests
- Change poll interval while requests are processing
- Assume email is working without testing

## Rollback

If the worker is causing issues:

1. **Disable without deleting:**
   ```bash
   # Comment out in Procfile
   # worker: python brand_worker.py
   git push
   ```
   Railway will stop the worker service

2. **Re-enable daemon threads (fallback):**
   - Revert to using daemon threads in Flask
   - Less reliable but no extra service needed

## Next Steps

- [ ] Deploy worker to Railway
- [ ] Submit test brands via UI
- [ ] Monitor logs for processing
- [ ] Verify brands appear in search
- [ ] Test email notifications
