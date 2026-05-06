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
  active          boolean NOT NULL DEFAULT true,
  created_at      timestamptz NOT NULL DEFAULT now()
);
-- Migration: ALTER TABLE school_profiles ADD COLUMN IF NOT EXISTS class_wa_group text NOT NULL DEFAULT '';
-- Migration: ALTER TABLE school_profiles ADD COLUMN IF NOT EXISTS gmail_refresh_token text;
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
from datetime import date, datetime, timedelta

import requests

import library as lib

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


def _gmail_get(path: str, params: dict = None, refresh_token: str = None) -> dict:
    token = _gmail_access_token(refresh_token)
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
    return f"({froms}) after:{after}"


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
    Ask Groq to extract events/reminders from an email.
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

    # Truncate body to avoid hitting context limits
    body_truncated = body[:12000] if len(body) > 12000 else body
    prompt = prompt.replace(body, body_truncated)

    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.1-8b-instant",
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user",   "content": prompt},
                    ],
                    "max_tokens": 2000,
                    "temperature": 0.1,
                },
                timeout=30,
            )
            rj = r.json()
            if "choices" not in rj:
                err_msg = rj.get("error", {}).get("message", str(rj))
                print(f"[school] groq no choices (attempt {attempt+1}): {err_msg}")
                if "rate" in err_msg.lower() and attempt < 2:
                    import time as _time
                    _time.sleep(10 * (attempt + 1))
                    continue
                return []
            raw = rj["choices"][0]["message"]["content"].strip()
            raw = re.sub(r"^```[a-z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
            events = json.loads(raw)
            if isinstance(events, list):
                return events
            return []
        except Exception as e:
            print(f"[school] groq parse error (attempt {attempt+1}): {e}")
            if attempt < 2:
                import time as _time
                _time.sleep(5)
    return []


# ── Supabase helpers ───────────────────────────────────────────────────────────

def _get_profiles(from_number: str = None) -> list[dict]:
    q = lib._sb().table("school_profiles").select("*").eq("active", True)
    if from_number:
        q = q.eq("from_number", from_number)
    return q.execute().data or []


def _store_events(profile: dict, events: list[dict], gmail_msg_id: str, sent_date: str = ""):
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
        try:
            desc = ev.get("description", "") or ""
            link = ev.get("link_url") or ""
            if link.startswith("http"):
                desc = (desc + "\n" + link).strip()
            # For newsletters without a specific event date, use the email's sent date
            ev_date = ev.get("event_date") or None
            if not ev_date and raw_type == "newsletter" and sent_date:
                ev_date = sent_date
            lib._sb().table("school_events").insert({
                "profile_id":    profile["id"],
                "from_number":   profile["from_number"],
                "event_title":   title[:200],
                "event_type":    raw_type.lower().strip(),
                "event_date":    ev_date,
                "description":   desc[:500],
                "action_needed": ev.get("action_needed", "")[:300],
                "deadline":      ev.get("deadline") or None,
                "gmail_msg_id":  gmail_msg_id,
            }).execute()
            existing_titles.add(title.lower())
        except Exception as e:
            if "unique" not in str(e).lower():
                print(f"[school] insert error: {e}")


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

def poll_all_profiles(days_back: int = 7, force: bool = False, profile_ids: list = None) -> dict:
    """
    For every active school profile (optionally filtered to profile_ids),
    fetch emails from school senders using each parent's own Gmail token.
    force=True deletes existing events before re-parsing.
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

        # Collect all sender emails across this parent's schools
        all_senders: list[str] = []
        for p in parent_profiles:
            all_senders.extend(p.get("sender_emails") or [])
        all_senders = list(set(all_senders))
        if not all_senders:
            continue

        query = _build_gmail_query(all_senders, days_back=days_back)
        try:
            res = _gmail_get("messages", {"q": query, "maxResults": 50}, refresh_token=gmail_token)
        except Exception as e:
            print(f"[school] Gmail list error for {from_number}: {e}")
            continue

        msg_stubs = res.get("messages", [])
        total_emails += len(msg_stubs)

        for stub in msg_stubs:
            msg_id = stub["id"]
            try:
                msg = _gmail_get(f"messages/{msg_id}", {"format": "full"}, refresh_token=gmail_token)
            except Exception as e:
                print(f"[school] Gmail fetch error {msg_id}: {e}")
                continue

            # Determine which profile this sender belongs to
            headers = {h["name"].lower(): h["value"]
                       for h in msg.get("payload", {}).get("headers", [])}
            sender = headers.get("from", "").lower()

            matched_profile = None
            for p in parent_profiles:
                for se in (p.get("sender_emails") or []):
                    if se.lower() in sender:
                        matched_profile = p
                        break
                if matched_profile:
                    break
            if not matched_profile:
                matched_profile = parent_profiles[0]  # fallback

            if force:
                # Wipe existing events for this email so we reparse fresh
                try:
                    lib._sb().table("school_events").delete().eq("gmail_msg_id", msg_id).execute()
                except Exception as e:
                    print(f"[school] force-delete error {msg_id}: {e}")

            subject, body, sent_date = _extract_email_text(msg, msg_id=msg_id, refresh_token=gmail_token)
            if not body.strip():
                print(f"[school] empty body for msg {msg_id} subject={subject!r}")
                continue

            import time as _time; _time.sleep(2)  # stay well under 30 req/min Groq limit
            events = _groq_parse_events(
                subject, body,
                matched_profile["school_name"],
                matched_profile.get("year_group", ""),
                sent_date=sent_date,
            )
            print(f"[school] {msg_id} subject={subject!r} sent={sent_date} → {len(events)} events")
            if events:
                _store_events(matched_profile, events, gmail_msg_id=msg_id, sent_date=sent_date)
                total_events += len(events)

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

# Multi-step setup state: from_number → {step, data}
_SETUP_STATE: dict = {}

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
    if from_number in _SETUP_STATE:
        state = _SETUP_STATE[from_number]
        step  = state["step"]

        if cmd in ("cancel", "stop", "quit"):
            del _SETUP_STATE[from_number]
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
            prompt = _next_setup_prompt(state)
            # After school name — show what was found
            if step == "school_name" and state["data"].get("address"):
                addr = state["data"]["address"]
                prompt = f"📍 Found: *{state['data'].get('place_name','')}*\n{addr}\n\n" + prompt
            return prompt
        else:
            # All steps done — save profile
            data = state["data"]
            del _SETUP_STATE[from_number]
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
        _SETUP_STATE[from_number] = {"step": "child_name", "data": {}}
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
