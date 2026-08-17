# Phase 1b Integration Guide

**Objective:** Wire motivation features into sms_service.py

**Files created:**
- `miru/motivation/handlers.py` — WhatsApp keyword handlers
- `miru/motivation/endpoints.py` — Flask endpoints  
- `weekly_savings_summary.py` — Weekly savings computation
- `miru/motivation/nudges.py` — Copy/formatting functions
- `migrations/phase3_motivation_layer.sql` — Database schema

---

## Step 1: Register endpoints in `sms_service.py`

**Location:** Near the top of sms_service.py, after `app = Flask(__name__)`

```python
# Add this AFTER the Flask app is created:
from miru.motivation.endpoints import register_motivation_endpoints
register_motivation_endpoints(app)
```

---

## Step 2: Add WhatsApp keyword handlers

**Location:** In `_whatsapp_reply_inner()`, find the TGTG handlers (line ~27237)

**Add AFTER the TGTG handlers (around line 27330):**

```python
    # ── Motivation: Price alerts ──
    if body_lower in ("price alert", "price alerts", "alert me", "fuel alert"):
        from miru.motivation.handlers import handle_price_alert_setup
        reply = handle_price_alert_setup(from_number, body)
        resp.message(reply)
        return str(resp)

    if body_lower in ("alerts off", "stop alerts", "stop fuel"):
        from miru.motivation.handlers import handle_alerts_off
        reply = handle_alerts_off(from_number)
        resp.message(reply)
        return str(resp)

    if body_lower in ("beat", "beat target"):
        from miru.motivation.handlers import handle_beat_target
        reply = handle_beat_target(from_number)
        resp.message(reply)
        return str(resp)

    if body_lower in ("weekly", "weekly summary"):
        from miru.motivation.handlers import handle_weekly_summary_toggle
        reply = handle_weekly_summary_toggle(from_number, True)
        resp.message(reply)
        return str(resp)
```

---

## Step 3: Modify `/api/fuel/check-drops` endpoint

**Location:** Search for `def.*fuel/check-drops` in sms_service.py

**Add this after the message is sent (around line 7760):**

```python
    # ── Log to savings_events ledger ──
    try:
        # Calculate savings estimate
        saving_pence = int((last_price - new_price) * 55 * 10)  # 55L tank, in pence
        
        # Insert event
        lib._sb().table("savings_events").insert({
            "wa": wa,
            "event_type": "fuel_drop",
            "amount_pence": saving_pence,
            "description": f"Fuel {fuel_type} dropped {drop_ppl:.1f}p/L in {postcode}"
        }).execute()
        
        # Update running total on fuel_alerts
        lib._sb().table("fuel_alerts").update({
            "total_saved_pence": (last_alert.get('total_saved_pence', 0) or 0) + saving_pence
        }).eq("id", alert_id).execute()
        
    except Exception as e:
        app.logger.error(f"[fuel-check-drops] Error logging savings event: {e}")
```

---

## Step 4: Brief integration (optional, Phase 1b-extended)

**Location:** In `/api/home/brief` endpoint around line 15109

**Add this to the parallel fetcher list:**

```python
    # In the parallel fetcher list, add:
    "savings": _v2_fetch_savings_summary(from_number),
```

**Add this new function anywhere in sms_service.py:**

```python
def _v2_fetch_savings_summary(from_number: str) -> dict:
    """Fetch weekly savings for brief integration."""
    try:
        from weekly_savings_summary import get_weekly_savings
        return get_weekly_savings(from_number)
    except Exception as e:
        app.logger.error(f"[savings-summary] Error: {e}")
        return {"status": "error"}
```

**Then in the Groq brief narrative context, add:**

```python
    # Existing context dict building:
    brief_context = {
        "school": school_data,
        "trains": trains_data,
        "spend": spend_data,
        "savings": savings_data,  # ADD THIS
        ...
    }
```

---

## Step 5: Create scheduled cron job for weekly summary

**Manual option (before automating):**

Test the endpoint locally:
```bash
curl "http://localhost:5000/api/cron/weekly-savings?token=miru-digest-2026"
```

**Production (Railway):**

Set up a cron job to call:
```
GET https://miru.humanagency.co/api/cron/weekly-savings?token=<YOUR_CRON_TOKEN>
```

Every Sunday at 18:00 (6pm).

Use: https://cron-job.org or https://www.easycron.com

---

## Checklist before shipping:

- [ ] Step 1: Register endpoints
- [ ] Step 2: Add WhatsApp handlers
- [ ] Step 3: Modify fuel/check-drops
- [ ] Step 4 (optional): Brief integration
- [ ] Run migration: `psql <db-url> < migrations/phase3_motivation_layer.sql`
- [ ] Test locally:
  - Send "price alert KT16 0DA" to WhatsApp
  - Check motivation_prefs table
  - Send "beat" and verify weekly target stored
- [ ] Push to git
- [ ] Railway auto-deploys
- [ ] Verify tables exist in Supabase
- [ ] Set internal testers: Set Riaan/Inaaya to `is_internal_tester=true`
- [ ] Set up cron job for weekly summary

---

## Testing checklist

**Local testing (before deploy):**
1. Send "price alert KT16 0HY" → confirm opt-in message
2. Send "alerts off" → confirm pause message
3. Send "beat" → confirm target set message
4. Test `/api/motivation/prefs` endpoint with POST request
5. Test `/api/motivation/savings-summary/whatsapp:+44...` with GET request

**Post-deploy:**
1. Add Riaan + Inaaya's parents to internal testers
2. Message them: "price alert"
3. Wait for weekly summary (Sunday 18:00) if summary cron is set
4. Measure: reply rate to keywords within 48 hours

---

## Troubleshooting

**Issue:** WhatsApp keyword not recognized
- Check body_lower matching (case-insensitive, trimmed)
- Ensure handler is added before the default "no match" fallback

**Issue:** Endpoint 404
- Verify import is at the top: `from miru.motivation.endpoints import register_motivation_endpoints`
- Verify `register_motivation_endpoints(app)` is called after Flask app init

**Issue:** motivation_prefs insert fails
- Check table was created by migration
- Verify `wa` is not null/empty
- Confirm Supabase credentials are valid

**Issue:** Weekly summary doesn't send
- Check cron job is firing (logs)
- Verify users have `weekly_summary_enabled=true`
- Test endpoint manually: `/api/cron/weekly-savings?token=...`
