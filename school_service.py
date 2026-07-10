from __future__ import annotations
"""
School Comms — Miru module
==========================
Monitors Gmail for school communications, extracts events/reminders,
and delivers a weekly WhatsApp digest.

Supabase tables (run once):
─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS school_profiles (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  from_number     text NOT NULL,
  child_name      text NOT NULL DEFAULT '',
  school_name     text NOT NULL,
  year_group      text NOT NULL DEFAULT '',
  class_name      text NOT NULL DEFAULT '',
  teacher_name    text NOT NULL DEFAULT '',
  address         text NOT NULL DEFAULT '',
  phone           text NOT NULL DEFAULT '',
  class_wa_group  text NOT NULL DEFAULT '',
  sender_emails   jsonb NOT NULL DEFAULT '[]',
  shared_with     jsonb NOT NULL DEFAULT '[]',
  active          boolean NOT NULL DEFAULT true,
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE(from_number, school_name)
);
-- Migration: ALTER TABLE school_profiles ADD COLUMN IF NOT EXISTS class_wa_group text NOT NULL DEFAULT '';
-- Migration: ALTER TABLE school_profiles ADD COLUMN IF NOT EXISTS gmail_refresh_token text;
-- Migration: ALTER TABLE school_profiles ADD COLUMN IF NOT EXISTS gmail_token_error boolean NOT NULL DEFAULT false;
-- Migration: ALTER TABLE school_profiles ADD COLUMN IF NOT EXISTS shared_with jsonb NOT NULL DEFAULT '[]';
-- Migration: ALTER TABLE school_profiles ADD CONSTRAINT IF NOT EXISTS school_profiles_unique_user_school UNIQUE(from_number, school_name);
CREATE INDEX ON school_profiles(from_number);

CREATE TABLE IF NOT EXISTS school_events (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  profile_id    uuid REFERENCES school_profiles(id) ON DELETE CASCADE,
  from_number   text NOT NULL,
  event_date    date,
  event_title   text NOT NULL,
  event_type    text NOT NULL DEFAULT 'event',
  description   text NOT NULL DEFAULT '',
  action_needed text NOT NULL DEFAULT '',
  deadline      date,
  gmail_msg_id  text UNIQUE,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON school_events(from_number, event_date);
─────────────────────────────────────────────────────────────────────

Env vars needed (add to .env / Railway):
  GMAIL_CLIENT_ID
  GMAIL_CLIENT_SECRET
  GMAIL_REFRESH_TOKEN
  TWILIO_ACCOUNT_SID   (already set)
  TWILIO_AUTH_TOKEN    (already set)
  TWILIO_WHATSAPP_FROM (already set)
  SUPABASE_URL         (already set)
  SUPABASE_KEY         (already set)
  GROQ_API_KEY         (already set)

One-time Gmail auth setup:  python3 school_auth.py
"""

import json
import os
import re
import time
from datetime import date, datetime, timedelta
from threading import Lock

import requests

import library as lib

# ── Groq Rate Limiter ──────────────────────────────────────────────────────────
class GroqRateLimiter:
  """Global queue to prevent Groq rate limit hits by processing emails serially."""
  def __init__(self):
    self._lock = Lock()
    self._last_request_time = 0
    self._min_delay = 0.5  # 500ms minimum between requests (stay well under 6000 TPM)

  def wait_if_needed(self):
    """Enforce minimum delay between requests."""
    with self._lock:
      elapsed = time.time() - self._last_request_time
      if elapsed < self._min_delay:
        time.sleep(self._min_delay - elapsed)
      self._last_request_time = time.time()

  def handle_rate_limit(self, error_msg: str) -> float:
    """Extract suggested wait time from Groq rate limit error and update delay."""
    import re
    match = re.search(r'Please try again in ([\d.]+)s', error_msg)
    if match:
      wait_seconds = float(match.group(1)) + 1.0  # Add 1s buffer
      with self._lock:
        self._min_delay = max(self._min_delay, wait_seconds / 6)  # Spread over 6 requests
      return wait_seconds
    return 0

_groq_limiter = GroqRateLimiter()

# ── Claude Fallback for accuracy ───────────────────────────────────────────────
def _claude_parse_events(subject: str, body: str, school_name: str, year_group: str,
                         sent_date: str = "") -> list[dict]:
  """Fallback to Claude for complex/new schools. Higher accuracy, used sparingly."""
  try:
    ref = date.fromisoformat(sent_date) if sent_date else date.today()
  except ValueError:
    ref = date.today()

  ref_str = ref.isoformat()
  weekday = ref.strftime("%A")
  days_map = {}
  for i, d in enumerate(["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]):
    delta = (i - ref.weekday()) % 7
    days_map[d] = (ref + timedelta(days=delta if delta else 7)).isoformat()
  days_hint = "  ".join(f"this {d} = {v}" for d, v in days_map.items())

  body_truncated = body[:3000] if len(body) > 3000 else body
  prompt = f"""School: {school_name}  Year group: {year_group}
Email sent: {ref_str} ({weekday})
Relative dates: {days_hint}

Subject: {subject}
Body:
{body_truncated}

Extract all school events/reminders as JSON array. Return ONLY the array, no markdown.
Each item: {{event_title, event_type, event_date, description, action_needed, deadline, link_url}}
Types: activity|reminder|club|dinner|newsletter|info
"""

  try:
    import anthropic
    client = anthropic.Anthropic()
    msg = client.messages.create(
      model="claude-3-5-sonnet-20241022",
      max_tokens=1500,
      messages=[{"role": "user", "content": prompt}]
    )
    raw = msg.content[0].text.strip()
    raw = re.sub(r"^```[a-z]*\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw)
    events = json.loads(raw)
    return events if isinstance(events, list) else []
  except Exception as e:
    print(f"[school] claude fallback error: {e}")
    return []

# ── Gmail OAuth ────────────────────────────────────────────────────────────────

_GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GMAIL_API_BASE  = "https://gmail.googleapis.com/gmail/v1/users/me"

def _gmail_access_token(refresh_token: str = None) -> str:
    """Exchange refresh token for a short-lived access token.
    Per-user tokens (from OAuth signup) use GMAIL_WEB_CLIENT_ID/SECRET.
    The legacy env-var token uses GMAIL_CLIENT_ID/SECRET (desktop app)."""
    rtok = refresh_token or os.environ.get("GMAIL_REFRESH_TOKEN", "")
    # Per-user tokens were issued by the web client; legacy token by desktop client
    if refresh_token:
        cid  = os.environ.get("GMAIL_WEB_CLIENT_ID")  or os.environ.get("GMAIL_CLIENT_ID", "")
        csec = os.environ.get("GMAIL_WEB_CLIENT_SECRET") or os.environ.get("GMAIL_CLIENT_SECRET", "")
    else:
        cid  = os.environ.get("GMAIL_CLIENT_ID", "")
        csec = os.environ.get("GMAIL_CLIENT_SECRET", "")
    if not all([cid, csec, rtok]):
        missing = [k for k, v in [("client_id", cid), ("client_secret", csec), ("refresh_token", rtok)] if not v]
        raise RuntimeError(f"Missing Gmail credentials: {', '.join(missing)}")
    r = requests.post(_GMAIL_TOKEN_URL, data={
        "client_id":     cid,
        "client_secret": csec,
        "refresh_token": rtok,
        "grant_type":    "refresh_token",
    }, timeout=10)
    r.raise_for_status()
    rj = r.json()
    if "access_token" not in rj:
        raise RuntimeError(f"Gmail token exchange failed: {rj}")
    return rj["access_token"]


def _gmail_get(path: str, params: dict = None, refresh_token: str = None, access_token: str = None) -> dict:
    token = access_token or _gmail_access_token(refresh_token)
    r = requests.get(
        f"{_GMAIL_API_BASE}/{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


# ── Email fetching ─────────────────────────────────────────────────────────────

def _build_gmail_query(sender_emails: list[str], days_back: int = 7) -> str:
    after = (date.today() - timedelta(days=days_back)).strftime("%Y/%m/%d")
    froms = " OR ".join(f"from:{e}" for e in sender_emails)
    return f"in:inbox ({froms}) after:{after}"


def _extract_pdf_text(msg_id: str, att_id: str, filename: str, refresh_token: str = None) -> str:
    """Fetch a Gmail attachment and extract text via PyMuPDF. Returns up to 4000 chars."""
    try:
        import base64, fitz  # fitz = PyMuPDF
        data = _gmail_get(f"messages/{msg_id}/attachments/{att_id}", refresh_token=refresh_token).get("data", "")
        if not data:
            return ""
        pdf_bytes = base64.urlsafe_b64decode(data + "==")
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = "\n".join(page.get_text() for page in doc)
        doc.close()
        text = re.sub(r"\n{3,}", "\n\n", text.strip())
        print(f"[school] attachment {filename}: {len(text)} chars extracted")
        return text[:4000]
    except Exception as e:
        print(f"[school] attachment extract error {filename}: {e}")
        return ""


def _extract_url_text(url: str) -> str:
    """Fetch and extract text from a newsletter URL. Returns up to 3000 chars."""
    try:
        from html.parser import HTMLParser

        # Safelist: only fetch from common school domains and trusted newsletter hosts
        safelist = [
            'chartersschool.org.uk', 'stannshealth.co.uk', 'newhaw.co.uk',
            'docs.google.com', 'notion.so', 'mailchimp.com', 'campaignmonitor.com',
            'getresponse.com', 'constant-contact.com'
        ]
        if not any(safe in url.lower() for safe in safelist):
            print(f"[school] URL {url} not on safelist, skipping")
            return ""

        r = requests.get(url, timeout=10, allow_redirects=True)
        r.raise_for_status()

        if r.status_code != 200:
            return ""

        html = r.text

        # Strip scripts, styles, and HTML tags
        html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.S | re.I)
        html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.S | re.I)
        html = re.sub(r"<(?:br|p|div|tr|li|h[1-6])[^>]*>", "\n", html, flags=re.I)
        html = re.sub(r"<[^>]+>", "", html)

        # Decode entities
        for ent, ch in [("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                        ("&quot;", '"'), ("&#39;", "'")]:
            html = html.replace(ent, ch)

        text = re.sub(r"\n{3,}", "\n\n", html.strip())
        text = re.sub(r"[ \t]{2,}", " ", text)

        print(f"[school] fetched URL {url}: {len(text)} chars extracted")
        return text[:3000]
    except Exception as e:
        print(f"[school] URL fetch error {url}: {e}")
        return ""


def _extract_email_text(msg: dict, msg_id: str = "", refresh_token: str = None) -> tuple[str, str, str]:
    """Return (subject, body_text, sent_date_iso) from a Gmail message resource.
    Reads text/plain, falls back to stripped HTML, then appends any PDF attachments."""
    import base64
    from email.utils import parsedate_to_datetime

    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    subject = headers.get("subject", "")

    # Extract send date from email headers
    sent_date = ""
    raw_date = headers.get("date", "")
    if raw_date:
        try:
            sent_date = parsedate_to_datetime(raw_date).date().isoformat()
        except Exception:
            sent_date = date.today().isoformat()

    plain_parts, html_parts = [], []

    def _walk(parts):
        for part in parts:
            if part.get("parts"):
                _walk(part["parts"])
            mime = part.get("mimeType", "")
            data = part.get("body", {}).get("data", "")
            if not data:
                continue
            decoded = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")
            if mime == "text/plain":
                plain_parts.append(decoded)
            elif mime == "text/html":
                html_parts.append(decoded)

    payload = msg.get("payload", {})
    if payload.get("parts"):
        _walk(payload["parts"])
    else:
        data = payload.get("body", {}).get("data", "")
        if data:
            decoded = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")
            mime = payload.get("mimeType", "")
            if mime == "text/html":
                html_parts.append(decoded)
            else:
                plain_parts.append(decoded)

    if plain_parts:
        body = "\n".join(plain_parts)
    elif html_parts:
        # Strip HTML — remove boilerplate blocks then tags
        html = "\n".join(html_parts)
        body = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.S | re.I)
        body = re.sub(r"<script[^>]*>.*?</script>", "", body, flags=re.S | re.I)
        body = re.sub(r"<head[^>]*>.*?</head>", "", body, flags=re.S | re.I)
        # Preserve line breaks from block elements
        body = re.sub(r"<(?:br|p|div|tr|li|h[1-6])[^>]*>", "\n", body, flags=re.I)
        body = re.sub(r"<[^>]+>", "", body)
        # Decode common entities
        for ent, ch in [("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"),
                        ("&gt;", ">"), ("&quot;", '"'), ("&#39;", "'"),
                        ("&ldquo;", '"'), ("&rdquo;", '"'), ("&ndash;", "-"),
                        ("&mdash;", "-"), ("&lsquo;", "'"), ("&rsquo;", "'")]:
            body = body.replace(ent, ch)
        body = re.sub(r"[ \t]{2,}", " ", body)
    else:
        body = ""

    body = re.sub(r"\n{3,}", "\n\n", body.strip())

    # Follow Google Docs links — newsletters often link to a public Google Doc
    gdoc_ids = re.findall(r'docs\.google\.com/document/d/([A-Za-z0-9_-]{20,})', body)
    for doc_id in gdoc_ids[:2]:  # cap at 2 docs per email
        try:
            export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
            r = requests.get(export_url, timeout=15, allow_redirects=True)
            if r.status_code == 200 and r.text.strip():
                doc_text = re.sub(r"\n{3,}", "\n\n", r.text.strip())[:5000]
                body = (body + f"\n\n[Google Doc content]\n{doc_text}").strip()
                print(f"[school] fetched gdoc {doc_id}: {len(doc_text)} chars")
        except Exception as e:
            print(f"[school] gdoc fetch error {doc_id}: {e}")

    # Follow newsletter URLs — schools often send emails linking to website newsletters
    newsletter_urls = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]*(?:/\d+)?/newsletter[^\s<>"{}|\\^`\[\]]*', body, re.I)
    for url in newsletter_urls[:2]:  # cap at 2 URLs per email
        url_text = _extract_url_text(url)
        if url_text:
            body = (body + f"\n\n[Newsletter from {re.sub(r'https?://([^/]+)/.*', r'\\1', url)}]\n{url_text}").strip()

    # Append text from PDF attachments (newsletters often arrive as attached PDFs)
    if msg_id:
        att_texts = []
        def _find_atts(parts):
            for part in parts:
                if part.get("parts"):
                    _find_atts(part["parts"])
                mime = part.get("mimeType", "")
                fname = part.get("filename", "")
                att_id = part.get("body", {}).get("attachmentId", "")
                if att_id and (mime == "application/pdf" or fname.lower().endswith(".pdf")):
                    txt = _extract_pdf_text(msg_id, att_id, fname, refresh_token=refresh_token)
                    if txt:
                        att_texts.append(f"[Attachment: {fname}]\n{txt}")
        _find_atts(payload.get("parts", []))
        if att_texts:
            body = (body + "\n\n" + "\n\n".join(att_texts)).strip()

    return subject, body[:12000], sent_date


# ── Groq event parsing ─────────────────────────────────────────────────────────

def _groq_parse_events(subject: str, body: str, school_name: str, year_group: str,
                       sent_date: str = "") -> list[dict]:
    """
    Extract events/reminders from school emails using Groq.
    Returns list of: {event_title, event_type, event_date, description, action_needed, deadline}
    """
    # Use the email's actual send date for relative date resolution
    try:
        ref = date.fromisoformat(sent_date) if sent_date else date.today()
    except ValueError:
        ref = date.today()

    ref_str = ref.isoformat()
    weekday = ref.strftime("%A")
    # Map "this Monday/Friday/..." relative to the email send date
    days_map = {}
    for i, d in enumerate(["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]):
        delta = (i - ref.weekday()) % 7
        days_map[d] = (ref + timedelta(days=delta if delta else 7)).isoformat()
    days_hint = "  ".join(f"this {d} = {v}" for d, v in days_map.items())

    system = (
        "You are a school communication parser. Extract all events, deadlines, reminders, "
        "and important dates from school emails. Return ONLY valid JSON, no markdown fences."
    )
    prompt = f"""School: {school_name}  Year group: {year_group}
Email sent: {ref_str} ({weekday})
Relative dates from send date: {days_hint}

Email subject: {subject}
Email body:
{body}

Extract every item a parent should know about. Return a JSON array of objects, each with:
  event_title   : short title (max 10 words)
  event_type    : classify as exactly one of:
                  "activity"   — trips, sports days, shows, assemblies, specific school events with a date
                  "reminder"   — deadlines, payments, consent forms, things parent must do
                  "club"       — after-school or lunchtime clubs
                  "dinner"     — school dinner menus, meal choices
                  "newsletter" — ONLY use this for the top-level summary item of a newsletter/bulletin email; never for individual events
                  "info"       — general info, policy updates, term dates, no action needed
  event_date    : ISO date (YYYY-MM-DD) or null — look hard for dates; convert "Thursday 8th May" → "2026-05-08"
  description   : 1-2 sentence plain summary
  action_needed : what the parent must do, or empty string
  deadline      : ISO date by which action is needed, or null
  link_url      : the most relevant URL from the email for this item (form link, booking page, sign-up), or null

Rules:
- ALWAYS extract every specific event, trip, deadline, reminder, club, or dinner mentioned.
- If the email is a newsletter/bulletin:
    * Create ONE item of type "newsletter": event_title = subject, description = 2-sentence overall summary
    * Then create SEPARATE items for EVERY specific event, reminder, deadline, or action inside it
    * Even if dates are not given, create items for clubs, trips, or recurring reminders
- For non-newsletters: one item per distinct event/reminder/action
- Do NOT create duplicate items for the same event
- Prefer to over-extract than under-extract — a parent would rather see too much than miss something

If nothing relevant, return [].
JSON array:"""

    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return []

    # Truncate body to reduce token usage
    body_truncated = body[:4000] if len(body) > 4000 else body
    prompt = prompt.replace(body, body_truncated)

    for attempt in range(3):
        try:
            _groq_limiter.wait_if_needed()
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {groq_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.1-8b-instant",
                    "max_tokens": 2000,
                    "temperature": 0.5,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": prompt},
                    ],
                },
                timeout=30,
            )
            rj = r.json()
            if "choices" not in rj or not rj["choices"]:
                err_msg = rj.get("error", {}).get("message", str(rj))
                print(f"[school] groq parse error (attempt {attempt+1}): {err_msg}")
                if "rate limit" in err_msg.lower() and attempt < 2:
                    wait = _groq_limiter.handle_rate_limit(err_msg)
                    print(f"[school] rate limit hit, waiting {wait:.1f}s before retry")
                    time.sleep(wait)
                    continue
                if "overloaded" in err_msg.lower() and attempt < 2:
                    time.sleep(10 * (attempt + 1))
                    continue
                return []
            raw = rj["choices"][0]["message"]["content"].strip()
            raw = re.sub(r"^```[a-z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
            events = json.loads(raw)
            if isinstance(events, list) and len(events) > 0:
                return events
            # If Groq returned empty, try Claude fallback for accuracy
            print(f"[school] groq returned empty, trying claude fallback for '{subject[:40]}'")
            claude_events = _claude_parse_events(subject, body, school_name, year_group, sent_date)
            if claude_events:
                print(f"[school] claude fallback succeeded: {len(claude_events)} events")
            return claude_events
        except json.JSONDecodeError as e:
            print(f"[school] groq json error (attempt {attempt+1}): {e}, trying claude fallback")
            claude_events = _claude_parse_events(subject, body, school_name, year_group, sent_date)
            if claude_events:
                print(f"[school] claude fallback succeeded: {len(claude_events)} events")
            return claude_events
        except Exception as e:
            print(f"[school] groq parse error (attempt {attempt+1}): {e}")
            if attempt < 2:
                time.sleep(5)
    # Final fallback: try Claude
    print(f"[school] groq exhausted retries, trying claude fallback")
    claude_events = _claude_parse_events(subject, body, school_name, year_group, sent_date)
    if claude_events:
        print(f"[school] claude fallback succeeded: {len(claude_events)} events")
    return claude_events


# ── Supabase helpers ───────────────────────────────────────────────────────────

def _get_profiles(from_number: str = None) -> list[dict]:
    """Get school profiles owned by or shared with the user."""
    if not from_number:
        q = lib._sb().table("school_profiles").select("*").eq("active", True)
        return q.execute().data or []

    # Normalize phone number (remove whatsapp: prefix if present)
    phone = from_number.replace("whatsapp:", "").strip()
    normalized_wa = f"whatsapp:{phone}" if not from_number.startswith("whatsapp:") else from_number

    try:
        # Get schools owned by user OR shared with user
        all_schools = lib._sb().table("school_profiles").select("*").eq("active", True).execute().data or []
        owned = [s for s in all_schools if s.get("from_number") == normalized_wa or s.get("from_number") == phone]
        shared = [s for s in all_schools if phone in (s.get("shared_with") or []) or normalized_wa in (s.get("shared_with") or [])]

        # Merge, dedup by id
        seen = {s["id"] for s in owned}
        result = owned + [s for s in shared if s["id"] not in seen]
        return result
    except:
        # Fallback to simple query
        q = lib._sb().table("school_profiles").select("*").eq("active", True).eq("from_number", normalized_wa)
        return q.execute().data or []


def _store_events(profile: dict, events: list[dict], gmail_msg_id: str, sent_date: str = "") -> list[dict]:
    """Insert new events; return list of newly inserted rows (skipping duplicates)."""
    # Check which (gmail_msg_id, event_title) pairs already exist
    try:
        existing = lib._sb().table("school_events") \
            .select("event_title") \
            .eq("gmail_msg_id", gmail_msg_id) \
            .execute().data or []
        existing_titles = {r["event_title"].lower().strip() for r in existing}
    except Exception:
        existing_titles = set()

    _stale_types = {"reminder", "activity", "club", "dinner"}
    newly_inserted = []
    for ev in events:
        title = (ev.get("event_title") or "").strip()
        if not title:
            continue
        if title.lower() in existing_titles:
            continue
        # Skip time-sensitive past events — no point showing March reminders in May
        raw_type = (ev.get("event_type", "other") or "other").lower().strip()
        ev_date  = ev.get("event_date")
        if ev_date and raw_type in _stale_types:
            try:
                if date.fromisoformat(ev_date) < date.today() - timedelta(days=7):
                    continue
            except ValueError:
                pass
        # If this is a reschedule/cancel notice, delete the superseded original event
        if any(kw in title.lower() for kw in ("reschedul", "postpone", "cancel")):
            sig = [w for w in re.split(r'\W+', re.sub(r'reschedul\w*|postpone\w*|cancel\w*', '', title.lower())) if len(w) > 3]
            if sig:
                try:
                    old_evs = lib._sb().table("school_events") \
                        .select("id, event_title") \
                        .eq("from_number", profile["from_number"]) \
                        .execute().data or []
                    for old_ev in old_evs:
                        old_t = (old_ev.get("event_title") or "").lower()
                        if any(kw in old_t for kw in ("reschedul", "postpone", "cancel")):
                            continue
                        if sum(1 for w in sig if w in old_t) >= min(2, len(sig)):
                            lib._sb().table("school_events").delete().eq("id", old_ev["id"]).execute()
                            print(f"[school] deleted superseded event '{old_ev['event_title']}' → '{title}'")
                except Exception as _ce:
                    print(f"[school] reschedule cleanup: {_ce}")
        try:
            desc = ev.get("description", "") or ""
            link = ev.get("link_url") or ""
            if link.startswith("http"):
                desc = (desc + "\n" + link).strip()
            # For newsletters without a specific event date, use the email's sent date
            ev_date = ev.get("event_date") or None
            if not ev_date and raw_type == "newsletter" and sent_date:
                ev_date = sent_date
            row = {
                "profile_id":    profile["id"],
                "from_number":   profile["from_number"],
                "event_title":   title[:200],
                "event_type":    raw_type.lower().strip(),
                "event_date":    ev_date,
                "description":   desc[:500],
                "action_needed": ev.get("action_needed", "")[:300],
                "deadline":      ev.get("deadline") or None,
                "gmail_msg_id":  gmail_msg_id,
            }
            lib._sb().table("school_events").insert(row).execute()
            existing_titles.add(title.lower())
            newly_inserted.append({**row, "child_name": profile.get("child_name", ""), "school_name": profile.get("school_name", "")})
        except Exception as e:
            if "unique" not in str(e).lower():
                print(f"[school] insert error: {e}")
    return newly_inserted


# Action-needed types that warrant an immediate WhatsApp alert
_ALERT_TYPES = {"reminder", "activity", "trip", "deadline", "payment", "event"}
_INFO_TYPES  = {"info"}

_TYPE_EMOJI = {
    "reminder": "⏰", "activity": "🎨", "event": "📅",
    "trip": "🚌", "deadline": "🔴", "payment": "💳",
}


def _fmt_date(d):
    if not d:
        return ""
    try:
        from datetime import date as _date
        dt = _date.fromisoformat(d)
        return dt.strftime("%-d %b")
    except Exception:
        return d


def _school_twilio_send(from_number: str, msg: str) -> None:
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    auth_token  = os.environ.get("TWILIO_AUTH_TOKEN", "")
    from_wa     = os.environ.get("TWILIO_WHATSAPP_FROM", "")
    if not all([account_sid, auth_token, from_wa]):
        print("[school] notify: Twilio env vars missing")
        return
    from twilio.rest import Client
    Client(account_sid, auth_token).messages.create(
        body=msg,
        from_=f"whatsapp:{from_wa}",
        to=from_number,
    )


def _school_quiet_hours() -> bool:
    """True if current UK time is outside 7am–9pm — don't send alerts now."""
    import zoneinfo as _zi
    from datetime import datetime as _dt
    hour = _dt.now(_zi.ZoneInfo("Europe/London")).hour
    return hour < 7 or hour >= 21


def _school_queue_alerts(from_number: str, events: list[dict]) -> None:
    """Store pending alerts in ma_details to be sent at next poll inside quiet hours."""
    try:
        import json as _j
        key = f"school_alert_queue:{from_number}"
        existing_row = lib._sb().table("ma_details").select("id,data") \
            .eq("device_id", from_number).eq("type", "school_alert_queue").limit(1).execute().data or []
        queued = (existing_row[0].get("data") or {}).get("events", []) if existing_row else []
        # Merge — deduplicate by event_title
        existing_titles = {e.get("event_title","").lower() for e in queued}
        for ev in events:
            if ev.get("event_title","").lower() not in existing_titles:
                queued.append(ev)
        payload = {"events": queued}
        if existing_row:
            lib._sb().table("ma_details").update({"data": payload}).eq("id", existing_row[0]["id"]).execute()
        else:
            lib._sb().table("ma_details").insert({"device_id": from_number, "type": "school_alert_queue", "label": "school_alert_queue", "data": payload}).execute()
        print(f"[school] quiet hours — queued {len(events)} events for {from_number}")
    except Exception as e:
        print(f"[school] queue error: {e}")


def _school_flush_queue(from_number: str) -> list[dict]:
    """Return queued alerts and clear the queue. Called at start of morning poll."""
    try:
        rows = lib._sb().table("ma_details").select("id,data") \
            .eq("device_id", from_number).eq("type", "school_alert_queue").limit(1).execute().data or []
        if not rows:
            return []
        queued = (rows[0].get("data") or {}).get("events", [])
        lib._sb().table("ma_details").delete().eq("id", rows[0]["id"]).execute()
        return queued
    except Exception:
        return []


def _notify_new_school_events(from_number: str, new_events: list[dict]) -> None:
    """Push newly found school events via WhatsApp.
    - Quiet hours (before 7am or after 9pm): queue for morning delivery
    - Groups action items from the same email into one message line
    - Applies reschedule/cancel suppression before sending
    """
    import re as _re

    # Deduplicate and suppress reschedule/cancelled events before notifying
    _stops = {"a","an","the","and","or","for","to","of","in","on","at","is","with","about","from","your"}
    def _sig(t):
        return [w for w in _re.sub(r'[^a-z0-9]',' ',(t or '').lower()).split() if len(w)>3 and w not in _stops]
    _resc = _re.compile(r'\b(reschedul|postpone|cancel)\w*', _re.I)
    suppressed, seen_titles = set(), set()
    for rev in new_events:
        if not _resc.search(rev.get("event_title","")): continue
        rev_words = set(_sig(rev["event_title"]))
        for ev in new_events:
            if ev is rev or id(ev) in suppressed: continue
            if _resc.search(ev.get("event_title","")): continue
            ev_words = _sig(ev.get("event_title",""))
            if ev_words and sum(1 for w in ev_words if w in rev_words) >= min(2, len(rev_words)):
                suppressed.add(id(ev))
    clean = []
    for ev in new_events:
        if id(ev) in suppressed: continue
        key = _re.sub(r'[^a-z0-9]','', (ev.get("event_title") or "").lower())
        if key in seen_titles: continue
        seen_titles.add(key)
        clean.append(ev)

    actionable = [e for e in clean if (e.get("event_type") or "").lower() in _ALERT_TYPES]
    info_only  = [e for e in clean if (e.get("event_type") or "").lower() in _INFO_TYPES]

    if not actionable and not info_only:
        return

    # Quiet hours — queue and return; morning poll will flush
    if _school_quiet_hours():
        _school_queue_alerts(from_number, actionable + info_only)
        return

    try:
        if actionable:
            # Group events from the same email (gmail_msg_id) to avoid fragmented reminders
            from collections import defaultdict as _dd
            by_msg = _dd(list)
            ungrouped = []
            for ev in actionable:
                mid = ev.get("gmail_msg_id", "")
                if mid:
                    by_msg[mid].append(ev)
                else:
                    ungrouped.append(ev)

            lines = ["🏫 *New from school*\n"]
            rendered = 0
            for mid, evs in by_msg.items():
                if rendered >= 5: break
                if len(evs) == 1:
                    ev = evs[0]
                    emoji = _TYPE_EMOJI.get(ev.get("event_type",""), "📌")
                    line  = f"{emoji} *{ev.get('event_title','')}*"
                    dt    = _fmt_date(ev.get("event_date"))
                    child = ev.get("child_name","")
                    if dt:     line += f" — {dt}"
                    if child:  line += f" ({child})"
                    if ev.get("action_needed"): line += f"\n   ↳ {ev['action_needed']}"
                    lines.append(line)
                else:
                    # Multiple items from same email — group under a heading
                    child = evs[0].get("child_name","")
                    # Find a common theme (non-reminder event if present, else first title)
                    anchor = next((e for e in evs if e.get("event_type") not in ("reminder",)), evs[0])
                    heading = anchor.get("event_title","")
                    dt = _fmt_date(anchor.get("event_date"))
                    line = f"🏫 *{heading}*"
                    if dt:    line += f" — {dt}"
                    if child: line += f" ({child})"
                    reminders = [e for e in evs if e.get("event_type") == "reminder" and e.get("action_needed")]
                    if reminders:
                        actions = " · ".join(e["action_needed"] for e in reminders[:3])
                        line += f"\n   ↳ {actions}"
                    lines.append(line)
                rendered += 1
            for ev in ungrouped[:max(0, 5 - rendered)]:
                emoji = _TYPE_EMOJI.get(ev.get("event_type",""), "📌")
                line  = f"{emoji} *{ev.get('event_title','')}*"
                dt    = _fmt_date(ev.get("event_date"))
                child = ev.get("child_name","")
                if dt:    line += f" — {dt}"
                if child: line += f" ({child})"
                if ev.get("action_needed"): line += f"\n   ↳ {ev['action_needed']}"
                lines.append(line)
            lines.append("\nmiru.humanagency.co/?screen=school")
            _school_twilio_send(from_number, "\n".join(lines))
            print(f"[school] alert sent to {from_number}: {len(actionable)} action events")

        if info_only:
            school_name = (info_only[0].get("school_name") or "school").strip()
            child_name  = (info_only[0].get("child_name") or "").strip()
            who = f"{child_name}'s school" if child_name else school_name
            lines = [f"📬 *New from {who}*\n"]
            for ev in info_only[:3]:
                lines.append(f"• {ev.get('event_title','').strip()}")
            if len(info_only) > 3:
                lines.append(f"• … and {len(info_only) - 3} more")
            lines.append("\nmiru.humanagency.co/?screen=school")
            _school_twilio_send(from_number, "\n".join(lines))
            print(f"[school] info nudge sent to {from_number}: {len(info_only)} info events")

    except Exception as e:
        print(f"[school] alert send error for {from_number}: {e}")


def _get_events(from_number: str, days_ahead: int = 30, days_back: int = 14) -> list[dict]:
    """Fetch dated events within window + all undated items from last days_back days."""
    past    = (date.today() - timedelta(days=days_back)).isoformat()
    horizon = (date.today() + timedelta(days=days_ahead)).isoformat()
    dated = (
        lib._sb().table("school_events")
        .select("*")
        .eq("from_number", from_number)
        .gte("event_date", past)
        .lte("event_date", horizon)
        .execute()
        .data or []
    )
    undated = (
        lib._sb().table("school_events")
        .select("*")
        .eq("from_number", from_number)
        .is_("event_date", "null")
        .gte("created_at", past)
        .execute()
        .data or []
    )
    return dated + undated


def _get_upcoming_events(from_number: str, days: int = 14) -> list[dict]:
    return _get_events(from_number, days_ahead=days, days_back=14)


def _get_this_week_events(from_number: str) -> list[dict]:
    today = date.today()
    start = today - timedelta(days=today.weekday())  # Monday this week
    # Include from last Monday (14 days) to end of next week
    return _get_events(from_number, days_ahead=7, days_back=14)


# ── Email polling ──────────────────────────────────────────────────────────────

def _flag_token_error(from_number: str, profiles: list, on_error=None):
    """Mark all profiles for this parent as having a bad token, then notify via callback."""
    ids = [p["id"] for p in profiles]
    try:
        lib._sb().table("school_profiles").update({"gmail_token_error": True}) \
            .in_("id", ids).execute()
    except Exception as e:
        print(f"[school] flag token error DB write failed: {e}")
    if callable(on_error):
        try:
            on_error(from_number, profiles)
        except Exception as e:
            print(f"[school] on_error callback failed: {e}")


def poll_all_profiles(days_back: int = 7, force: bool = False, profile_ids: list = None, on_error=None, skip_error_flag: bool = False) -> dict:
    """
    For every active school profile (optionally filtered to profile_ids),
    fetch emails from school senders using each parent's own Gmail token.
    force=True deletes existing events before re-parsing.
    skip_error_flag=True prevents flagging the token as errored (used after fresh OAuth).
    Returns summary dict.
    """
    profiles = _get_profiles()
    if profile_ids:
        profiles = [p for p in profiles if p["id"] in set(profile_ids)]
    if not profiles:
        return {"profiles": 0, "emails": 0, "events": 0}

    if force:
        for p in profiles:
            try:
                lib._sb().table("school_events").delete() \
                    .eq("profile_id", p["id"]).execute()
                print(f"[school] cleared events for profile {p['id']} ({p.get('child_name','')})")
            except Exception as e:
                print(f"[school] clear error: {e}")

    # Group profiles by from_number — same parent = same Gmail account
    by_parent: dict[str, list] = {}
    for p in profiles:
        by_parent.setdefault(p["from_number"], []).append(p)

    total_emails = total_events = 0
    # Collect newly inserted events per parent for WhatsApp alerts
    new_by_parent: dict[str, list[dict]] = {}

    for from_number, parent_profiles in by_parent.items():
        # Per-profile token (issued by web client OAuth) takes priority.
        # For legacy profiles (Vikram's), per-profile token is None — pass None so
        # _gmail_access_token uses the desktop client creds + GMAIL_REFRESH_TOKEN env var.
        # IMPORTANT: do NOT pass the env var token as refresh_token — web client
        # creds cannot exchange a desktop-client-issued refresh token.
        gmail_token = next(
            (p.get("gmail_refresh_token") for p in parent_profiles if p.get("gmail_refresh_token")),
            None
        )
        if gmail_token is None and not os.environ.get("GMAIL_REFRESH_TOKEN"):
            print(f"[school] No Gmail token for {from_number}, skipping — needs OAuth")
            continue

        # Fetch access token once for all API calls in this parent's loop
        try:
            access_token = _gmail_access_token(gmail_token)
        except Exception as e:
            print(f"[school] Gmail auth error for {from_number}: {e}")
            if not skip_error_flag:
                _flag_token_error(from_number, parent_profiles, on_error)
            continue

        # Collect all sender emails across this parent's schools
        all_senders: list[str] = []
        for p in parent_profiles:
            all_senders.extend(p.get("sender_emails") or [])
        all_senders = list(set(all_senders))
        if not all_senders:
            continue

        query = _build_gmail_query(all_senders, days_back=days_back)
        try:
            res = _gmail_get("messages", {"q": query, "maxResults": 100}, access_token=access_token)
        except Exception as e:
            print(f"[school] Gmail list error for {from_number}: {e}")
            if not skip_error_flag and ("400" in str(e) or "401" in str(e)):
                _flag_token_error(from_number, parent_profiles, on_error)
            continue

        msg_stubs = res.get("messages", [])
        total_emails += len(msg_stubs)

        # Pre-build sender→profile lookup (O(1) instead of O(n²))
        sender_to_profile = {}
        for p in parent_profiles:
            for se in (p.get("sender_emails") or []):
                sender_to_profile[se.lower()] = p

        # Parallel fetch of full messages (10 concurrent)
        from concurrent.futures import ThreadPoolExecutor
        def _fetch_msg(stub):
            msg_id = stub["id"]
            try:
                msg = _gmail_get(f"messages/{msg_id}", {"format": "full"}, access_token=access_token)
                headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
                sender = headers.get("from", "").lower()

                matched_profile = None
                for registered_sender, profile in sender_to_profile.items():
                    if registered_sender in sender:
                        matched_profile = profile
                        break
                if not matched_profile:
                    return None

                subject, body, sent_date = _extract_email_text(msg, msg_id=msg_id, refresh_token=gmail_token)
                if not body.strip():
                    return None

                skip_keywords = ("payment successful", "thank you", "receipt", "invoice", "confirmation", "unsubscribe")
                if any(kw in subject.lower() for kw in skip_keywords):
                    return None

                return (msg_id, subject, body, sent_date, matched_profile)
            except Exception as e:
                print(f"[school] Gmail fetch error {msg_id}: {e}")
                return None

        with ThreadPoolExecutor(max_workers=10) as executor:
            parsed_emails = list(executor.map(_fetch_msg, msg_stubs, timeout=15))

        # Process valid emails
        for result in parsed_emails:
            if not result:
                continue

            msg_id, subject, body, sent_date, matched_profile = result

            if force:
                try:
                    lib._sb().table("school_events").delete().eq("gmail_msg_id", msg_id).execute()
                except Exception as e:
                    print(f"[school] force-delete error {msg_id}: {e}")

            events = _groq_parse_events(
                subject, body,
                matched_profile["school_name"],
                matched_profile.get("year_group", ""),
                sent_date=sent_date,
            )
            print(f"[school] {msg_id} subject={subject!r} sent={sent_date} → {len(events)} events")
            if events:
                inserted = _store_events(matched_profile, events, gmail_msg_id=msg_id, sent_date=sent_date)
                total_events += len(events)
                if inserted:
                    new_by_parent.setdefault(from_number, []).extend(inserted)

    # Flush any queued alerts from quiet-hours polls (send now if daytime)
    if not _school_quiet_hours():
        all_numbers = {p["from_number"] for p in profiles}
        for fn in all_numbers:
            queued = _school_flush_queue(fn)
            if queued:
                new_by_parent.setdefault(fn, [])
                existing_titles = {e.get("event_title","").lower() for e in new_by_parent[fn]}
                for ev in queued:
                    if ev.get("event_title","").lower() not in existing_titles:
                        new_by_parent[fn].append(ev)

    # Send WhatsApp alerts for action-needed new events
    for from_number, new_events in new_by_parent.items():
        _notify_new_school_events(from_number, new_events)

    return {"profiles": len(profiles), "emails": total_emails, "events": total_events}


# ── Digest formatting ──────────────────────────────────────────────────────────

# Section definitions: (event_types, header, emoji)
_SECTIONS = [
    ({"reminder"},              "⏰ Reminders & Actions"),
    ({"activity"},              "📅 Upcoming Activities"),
    ({"club"},                  "⚽ Clubs"),
    ({"dinner"},                "🍽️ School Dinners"),
    ({"newsletter"},            "📰 Newsletter"),
    ({"info", "other", "meeting", "event", "trip", "deadline"}, "ℹ️ General Info"),
]

def _format_date(d: str | None) -> str:
    if not d:
        return ""
    try:
        return datetime.fromisoformat(d).strftime("%-d %b")
    except Exception:
        return d


def format_digest(events: list[dict], title: str = "School update") -> str:
    """Compact WhatsApp digest — title + date only, max 20 events, stays under 4096 chars."""
    if not events:
        return f"🏫 *{title}*\n\nNothing coming up from school right now."

    # Sort: dated events first (by date), then undated
    def _sort_key(e):
        return (e.get("event_date") or "9999-12-31", e.get("event_title") or "")
    sorted_events = sorted(events, key=_sort_key)

    # Deduplicate by title
    seen, deduped = set(), []
    for ev in sorted_events:
        k = (ev.get("event_title") or "").lower().strip()
        if k and k not in seen:
            seen.add(k)
            deduped.append(ev)

    lines = [f"🏫 *{title}*\n"]
    shown = 0
    for ev in deduped:
        if shown >= 20:
            remaining = len(deduped) - shown
            lines.append(f"\n_…and {remaining} more — see miru.humanagency.co/?screen=school_")
            break
        title_text = (ev.get("event_title") or "Untitled")[:80]
        d = _format_date(ev.get("event_date"))
        action = ev.get("action_needed", "")
        dl = ev.get("deadline", "")

        line = f"• {title_text}"
        if d:
            line += f" — {d}"
        if action:
            deadline_str = f" by {_format_date(dl)}" if dl else ""
            line += f"\n  ✏️ {action[:60]}{deadline_str}"
        lines.append(line)
        shown += 1

    msg = "\n".join(lines)
    # Hard safety truncation for WhatsApp 4096-char limit
    if len(msg) > 3900:
        msg = msg[:3897] + "…"
    return msg


# ── WhatsApp digest send ───────────────────────────────────────────────────────

def send_digest(from_number: str, days: int = 14) -> bool:
    """Send upcoming events digest to a parent via WhatsApp. Returns True on success."""
    events = _get_upcoming_events(from_number, days=days)
    message = format_digest(events, title=f"School events — next {days} days")

    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    auth_token  = os.environ.get("TWILIO_AUTH_TOKEN", "")
    from_wa     = os.environ.get("TWILIO_WHATSAPP_FROM", "")
    if not all([account_sid, auth_token, from_wa]):
        print("[school] Twilio env vars missing")
        return False

    try:
        from twilio.rest import Client
        client = Client(account_sid, auth_token)
        # Split if long
        chunks, current = [], ""
        for line in message.split("\n"):
            if len(current) + len(line) + 1 > 3800:
                chunks.append(current.strip())
                current = line + "\n"
            else:
                current += line + "\n"
        if current.strip():
            chunks.append(current.strip())
        for chunk in chunks:
            client.messages.create(body=chunk, from_=f"whatsapp:{from_wa}", to=from_number)
        return True
    except Exception as e:
        print(f"[school] send error: {e}")
        return False


def send_weekly_digest_all() -> dict:
    """Send weekly digest to every active parent. Call Sunday evening via cron."""
    profiles = _get_profiles()
    parents  = list({p["from_number"] for p in profiles})
    sent = 0
    for number in parents:
        if send_digest(number, days=7):
            sent += 1
    return {"total_parents": len(parents), "sent": sent}


# ── Google Places school lookup ────────────────────────────────────────────────

def _lookup_school(name: str) -> dict:
    """Search Google Places for a UK school. Returns {address, phone, place_name} or {}."""
    api_key = os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        return {}
    try:
        # Text search
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": f"{name} school UK", "key": api_key, "type": "school"},
            timeout=8,
        )
        results = r.json().get("results", [])
        if not results:
            return {}
        place = results[0]
        place_id = place.get("place_id", "")
        address  = place.get("formatted_address", "")
        found_name = place.get("name", name)

        # Place Details for phone number
        phone = ""
        if place_id:
            d = requests.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={"place_id": place_id, "fields": "formatted_phone_number", "key": api_key},
                timeout=8,
            )
            phone = d.json().get("result", {}).get("formatted_phone_number", "")

        return {"place_name": found_name, "address": address, "phone": phone}
    except Exception as e:
        print(f"[school] places lookup error: {e}")
        return {}


# ── WhatsApp conversation handler ──────────────────────────────────────────────

# Multi-step setup state: persisted to Supabase ma_details table (type="school_setup_state")
def _load_setup_state(from_number: str) -> dict:
    """Load school setup state from Supabase."""
    try:
        rows = lib._sb().table("ma_details").select("data").eq("device_id", from_number).eq("type", "school_setup_state").limit(1).execute().data or []
        return rows[0]["data"] if rows else None
    except Exception:
        return None

def _save_setup_state(from_number: str, state: dict) -> None:
    """Save school setup state to Supabase."""
    try:
        rows = lib._sb().table("ma_details").select("id").eq("device_id", from_number).eq("type", "school_setup_state").limit(1).execute().data or []
        if rows:
            lib._sb().table("ma_details").update({"data": state}).eq("id", rows[0]["id"]).execute()
        else:
            lib._sb().table("ma_details").insert({"device_id": from_number, "type": "school_setup_state", "data": state}).execute()
    except Exception:
        pass

def _delete_setup_state(from_number: str) -> None:
    """Delete school setup state from Supabase."""
    try:
        lib._sb().table("ma_details").delete().eq("device_id", from_number).eq("type", "school_setup_state").execute()
    except Exception:
        pass

_SETUP_STEPS = ["child_name", "school_name", "class_name", "teacher_name", "year_group", "sender_emails"]
_SETUP_PROMPTS = {
    "child_name":    "What's your child's name?",
    "school_name":   "What's the school name? (e.g. *Greenway Academy*)",
    "class_name":    "Which class are they in? (e.g. *5B* or *Year 5 Maple*)",
    "teacher_name":  "Who is the class teacher? (e.g. *Miss Smith*) — or reply *skip*",
    "year_group":    "Which year group? (e.g. *Year 5*)",
    "sender_emails": (
        "What email address does the school send from?\n"
        "e.g. admin@greenway.sch.uk\n\n"
        "You can add multiple separated by commas."
    ),
}


def _next_setup_prompt(state: dict) -> str:
    step = state["step"]
    return _SETUP_PROMPTS.get(step, "")


def handle_wa_school(from_number: str, text: str) -> str:
    """
    Entry point called from sms_service.py when message starts with 'school'.
    Returns the reply string (or empty string if nothing to send).
    """
    text = text.strip()
    cmd  = text.lower()

    # ── Resume setup if in progress ───────────────────────────────────────────
    state = _load_setup_state(from_number)
    if state:
        step  = state["step"]

        if cmd in ("cancel", "stop", "quit"):
            _delete_setup_state(from_number)
            return "Setup cancelled. Reply *school* to start again."

        # Store answer for current step
        if step == "sender_emails":
            emails = [e.strip().lower() for e in text.replace(" ", "").split(",") if "@" in e]
            if not emails:
                return "Please enter a valid email address (e.g. admin@yourschool.sch.uk)."
            state["data"]["sender_emails"] = emails
        elif step == "teacher_name" and cmd in ("skip", "no", "-", "n/a"):
            state["data"]["teacher_name"] = ""
        elif step == "school_name":
            state["data"]["school_name"] = text.strip()
            # Auto-lookup address and phone in background
            info = _lookup_school(text.strip())
            if info:
                state["data"]["address"]    = info.get("address", "")
                state["data"]["phone"]      = info.get("phone", "")
                state["data"]["place_name"] = info.get("place_name", text.strip())
        else:
            state["data"][step] = text.strip()

        # Advance step
        idx = _SETUP_STEPS.index(step)
        if idx + 1 < len(_SETUP_STEPS):
            state["step"] = _SETUP_STEPS[idx + 1]
            _save_setup_state(from_number, state)
            prompt = _next_setup_prompt(state)
            # After school name — show what was found
            if step == "school_name" and state["data"].get("address"):
                addr = state["data"]["address"]
                prompt = f"📍 Found: *{state['data'].get('place_name','')}*\n{addr}\n\n" + prompt
            return prompt
        else:
            # All steps done — save profile
            data = state["data"]
            _delete_setup_state(from_number)
            try:
                lib._sb().table("school_profiles").insert({
                    "from_number":   from_number,
                    "child_name":    data.get("child_name", ""),
                    "school_name":   data.get("school_name", ""),
                    "class_name":    data.get("class_name", ""),
                    "teacher_name":  data.get("teacher_name", ""),
                    "year_group":    data.get("year_group", ""),
                    "address":       data.get("address", ""),
                    "phone":         data.get("phone", ""),
                    "sender_emails": data.get("sender_emails", []),
                }).execute()
            except Exception as e:
                return f"Sorry, couldn't save your school profile: {e}"

            # Kick off a background poll so events appear immediately
            import threading
            threading.Thread(target=poll_all_profiles, kwargs={"days_back": 30}, daemon=True).start()

            school  = data.get("school_name", "your school")
            child   = data.get("child_name", "")
            cls     = data.get("class_name", "")
            teacher = data.get("teacher_name", "")
            detail  = ", ".join(filter(None, [cls, teacher]))
            return (
                f"✅ Done! Watching *{school}*"
                + (f" for *{child}*" if child else "")
                + (f" ({detail})" if detail else "")
                + ".\n\n"
                "Fetching the last 30 days of emails now — check the web in a minute.\n\n"
                "You can also ask anytime:\n"
                "• *school week* — this week + last week\n"
                "• *school upcoming* — next 30 days\n"
                "• *school setup* — add another school"
            )

    # ── Top-level commands ────────────────────────────────────────────────────
    if cmd in ("school", "school help", "school menu"):
        profiles = _get_profiles(from_number)
        schools  = ", ".join(p["school_name"] for p in profiles) if profiles else "none set up yet"
        return (
            "🏫 *School Comms*\n"
            f"Tracking: {schools}\n\n"
            "Reply with:\n"
            "• *school week* — this week's events\n"
            "• *school upcoming* — next 14 days\n"
            "• *school setup* — add a school\n"
            "• *school list* — show your schools"
        )

    if cmd == "school setup":
        # Encode the WhatsApp number so the web form can pre-fill it
        import urllib.parse
        wa_param = urllib.parse.quote(from_number.replace("whatsapp:", ""))
        signup_url = f"https://miru.humanagency.co/school/signup?wa={wa_param}"
        return (
            "🏫 *Add a school*\n\n"
            f"Easiest via the web form — takes 30 seconds:\n👉 {signup_url}\n\n"
            "Or reply *school chat* to set up here on WhatsApp instead."
        )

    if cmd == "school chat":
        _save_setup_state(from_number, {"step": "child_name", "data": {}})
        return (
            "🏫 *Add a school* (reply *cancel* at any time)\n\n"
            + _SETUP_PROMPTS["child_name"]
        )

    if cmd == "school debug":
        profiles = _get_profiles(from_number)
        events_all = _get_events(from_number, days_ahead=60, days_back=30)
        return (
            f"from_number: {from_number}\n"
            f"profiles found: {len(profiles)}\n"
            f"events found: {len(events_all)}\n"
            + (f"first profile from_number: {profiles[0].get('from_number','?')}" if profiles else "no profiles")
        )

    def _resolve_child(words):
        """Return (profile_id_or_None, child_label) from extra words in the command."""
        profiles = _get_profiles(from_number)
        if not profiles:
            return None, ""
        hint = " ".join(words).strip().lower()
        if not hint:
            return None, ""
        match = next((p for p in profiles if hint in p.get("child_name", "").lower()), None)
        if match:
            return match["id"], match["child_name"]
        return None, ""

    def _events_for(profile_id, days_ahead=30, days_back=3):
        horizon = (date.today() + timedelta(days=days_ahead)).isoformat()
        past_dated = (date.today() - timedelta(days=days_back)).isoformat()
        past_undated = (date.today() - timedelta(days=14)).isoformat()
        q_base = lib._sb().table("school_events").select("*").eq("from_number", from_number)
        if profile_id:
            q_base = q_base.eq("profile_id", profile_id)
        dated = (q_base.gte("event_date", past_dated)
                 .lte("event_date", horizon).execute().data or [])
        undated = (q_base.is_("event_date", "null")
                   .gte("created_at", past_undated).execute().data or [])
        all_events = dated + undated
        return sorted(all_events, key=lambda e: (e.get("event_date") or "9999", e.get("event_title") or ""))

    if cmd in ("school news", "school today", "school update", "school updates") or cmd.startswith("school news"):
        events = _events_for(None, days_ahead=14, days_back=7)
        return format_digest(events, title="School updates")

    if cmd.startswith("school week"):
        extra = cmd[len("school week"):].split()
        pid, child = _resolve_child(extra)
        label = f"{child}'s week" if child else "This week"
        events = _events_for(pid, days_ahead=7, days_back=7)
        return format_digest(events, title=label)

    if cmd.startswith("school upcoming") or cmd.startswith("school next"):
        prefix = "school upcoming" if cmd.startswith("school upcoming") else "school next"
        extra = cmd[len(prefix):].split()
        pid, child = _resolve_child(extra)
        label = f"{child} — coming up" if child else "Coming up"
        events = _events_for(pid, days_ahead=30, days_back=0)
        return format_digest(events, title=label)

    if cmd == "school list":
        profiles = _get_profiles(from_number)
        if not profiles:
            return "No schools set up yet. Reply *school setup* to add one."
        lines = ["🏫 *Your schools:*\n"]
        for p in profiles:
            child = f" ({p['child_name']})" if p.get("child_name") else ""
            emails = ", ".join(p.get("sender_emails") or [])
            lines.append(f"• *{p['school_name']}*{child} — {p.get('year_group','')}")
            lines.append(f"  Watching: {emails}")
        return "\n".join(lines)

    # school add email <address> [for <child>]
    # e.g. "school add email stannsheathjuniors-surrey@scopay.com for Riaan"
    if cmd.startswith("school add email"):
        parts = text.strip().split()
        # find the email address (contains @)
        new_email = next((p.lower() for p in parts if "@" in p), "")
        if not new_email:
            return "Please include the email address, e.g.:\n*school add email office@school.sch.uk*\nor\n*school add email office@school.sch.uk for Riaan*"
        # optional child name after "for"
        child_hint = ""
        if " for " in text.lower():
            child_hint = text.lower().split(" for ", 1)[1].strip()
        profiles = _get_profiles(from_number)
        if not profiles:
            return "No schools set up yet. Reply *school setup* first."
        # pick profile matching child hint, or the first one
        target = next((p for p in profiles if child_hint and child_hint in p.get("child_name","").lower()), profiles[0])
        current = target.get("sender_emails") or []
        if new_email in [e.lower() for e in current]:
            return f"That email is already being watched for *{target.get('child_name','your child')}*."
        updated = current + [new_email]
        try:
            lib._sb().table("school_profiles").update({"sender_emails": updated}).eq("id", target["id"]).execute()
        except Exception as e:
            return f"Couldn't update: {e}"
        import threading
        threading.Thread(target=poll_all_profiles, kwargs={"days_back": 30}, daemon=True).start()
        return (
            f"✅ Added *{new_email}* to *{target.get('child_name','your child')}*'s school.\n"
            "Fetching emails now — check the web in a minute."
        )

    # ── school note [text] / school note for [child]: [text] ─────────────────
    # User forwards a class WhatsApp group message to Miru.
    # Groq parses dates/type so it lands in the right dashboard section.
    if cmd.startswith("school note"):
        raw = text[len("school note"):].lstrip(": ").strip()
        # Optional child routing: "school note for Riaan: play rehearsal Thu"
        target_pid = None
        target_name = ""
        if raw.lower().startswith("for ") and ":" in raw:
            child_hint, raw = raw[4:].split(":", 1)
            child_hint = child_hint.strip().lower()
            raw = raw.strip()
            profiles_all = _get_profiles(from_number)
            match = next((p for p in profiles_all if child_hint in p.get("child_name","").lower()), None)
            if match:
                target_pid  = match["id"]
                target_name = match["child_name"]
        if not raw:
            return (
                "Forward any class WhatsApp message to me with:\n"
                "*school note: [paste message here]*\n"
                "or for a specific child:\n"
                "*school note for Riaan: [paste message]*"
            )
        profiles_all = _get_profiles(from_number)
        if not profiles_all:
            return "No school set up yet. Reply *school setup* first."
        profile = next((p for p in profiles_all if p["id"] == target_pid), profiles_all[0])

        # Use Groq to parse the forwarded message — same pipeline as email
        parsed = _groq_parse_events(
            subject=raw[:120],
            body=raw,
            school_name=profile.get("school_name", ""),
            year_group=profile.get("year_group", ""),
            sent_date=date.today().isoformat(),
        )

        if parsed:
            # Store each parsed event directly (skip gmail_msg_id since this is WhatsApp)
            saved = 0
            for ev in parsed:
                title = (ev.get("event_title") or "").strip()
                if not title:
                    continue
                try:
                    lib._sb().table("school_events").insert({
                        "profile_id":    profile["id"],
                        "from_number":   from_number,
                        "event_title":   title[:200],
                        "event_type":    (ev.get("event_type") or "info").lower(),
                        "event_date":    ev.get("event_date") or None,
                        "description":   (ev.get("description") or raw)[:500],
                        "action_needed": (ev.get("action_needed") or "")[:300],
                        "deadline":      ev.get("deadline") or None,
                    }).execute()
                    saved += 1
                except Exception as e:
                    if "unique" not in str(e).lower():
                        print(f"[school note] insert error: {e}")
        else:
            # Groq found nothing — save raw text as info so nothing is lost
            try:
                lib._sb().table("school_events").insert({
                    "profile_id":    profile["id"],
                    "from_number":   from_number,
                    "event_title":   raw[:200],
                    "event_type":    "info",
                    "description":   raw[:500],
                    "action_needed": "",
                }).execute()
                saved = 1
            except Exception as e:
                return f"Couldn't save: {e}"

        child_label = target_name or profile.get("child_name", "")
        label = f" for *{child_label}*" if child_label else ""
        return f"✅ Saved{label} — check your school dashboard."

    # ── school wa group [group_name] [for child] ──────────────────────────────
    # Saves the class WhatsApp group name against a child's profile
    if cmd.startswith("school wa group"):
        rest = text[len("school wa group"):].strip()
        child_hint = ""
        group_name = rest
        if " for " in rest.lower():
            parts = rest.lower().split(" for ", 1)
            group_name = rest[:rest.lower().index(" for ")].strip()
            child_hint = parts[1].strip()
        if not group_name:
            return (
                "Tell me the WhatsApp group name:\n"
                "*school wa group Year 4 Parents*\n"
                "or for a specific child:\n"
                "*school wa group Year 4 Parents for Riaan*"
            )
        profiles_all = _get_profiles(from_number)
        if not profiles_all:
            return "No school set up yet. Reply *school setup* first."
        profile = next(
            (p for p in profiles_all if child_hint and child_hint in p.get("child_name","").lower()),
            profiles_all[0],
        )
        try:
            lib._sb().table("school_profiles").update({"class_wa_group": group_name}).eq("id", profile["id"]).execute()
        except Exception as e:
            return f"Couldn't save: {e}"
        child_label = profile.get("child_name", "")
        label = f" for *{child_label}*" if child_label else ""
        return (
            f"✅ Class WhatsApp group saved{label}: *{group_name}*\n\n"
            "Now when you forward messages from that group, send them here with:\n"
            f"*school note for {child_label}: [paste the message]*"
        )

    # Unknown sub-command
    return (
        "🏫 *School Comms*\n"
        "Commands: *school week* | *school upcoming* | *school setup* | *school list*\n"
        "• *school add email office@school.sch.uk for Riaan* — add a sender\n"
        "• *school note: Play rehearsal Thu* — save a note from class WhatsApp\n"
        "• *school wa group Year 4 Parents for Riaan* — link class WA group"
    )
