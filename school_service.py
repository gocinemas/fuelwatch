"""
School Comms — Miru module
==========================
Monitors Gmail for school communications, extracts events/reminders,
and delivers a weekly WhatsApp digest.

Supabase tables (run once):
─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS school_profiles (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  from_number   text NOT NULL,
  child_name    text NOT NULL DEFAULT '',
  school_name   text NOT NULL,
  year_group    text NOT NULL DEFAULT '',
  sender_emails jsonb NOT NULL DEFAULT '[]',
  active        boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now()
);
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

def _gmail_access_token() -> str:
    """Exchange refresh token for a short-lived access token."""
    r = requests.post(_GMAIL_TOKEN_URL, data={
        "client_id":     os.environ["GMAIL_CLIENT_ID"],
        "client_secret": os.environ["GMAIL_CLIENT_SECRET"],
        "refresh_token": os.environ["GMAIL_REFRESH_TOKEN"],
        "grant_type":    "refresh_token",
    }, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def _gmail_get(path: str, params: dict = None) -> dict:
    token = _gmail_access_token()
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


def _extract_email_text(msg: dict) -> tuple[str, str]:
    """Return (subject, plain_text_body) from a Gmail message resource."""
    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    subject = headers.get("subject", "")

    def _parts_text(parts):
        text = ""
        for part in parts:
            if part.get("parts"):
                text += _parts_text(part["parts"])
            elif part.get("mimeType") == "text/plain":
                data = part.get("body", {}).get("data", "")
                if data:
                    import base64
                    text += base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")
        return text

    payload = msg.get("payload", {})
    if payload.get("parts"):
        body = _parts_text(payload["parts"])
    else:
        import base64
        data = payload.get("body", {}).get("data", "")
        body = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore") if data else ""

    # Strip excessive whitespace
    body = re.sub(r"\n{3,}", "\n\n", body.strip())
    return subject, body[:4000]  # cap at 4k chars for Groq


# ── Groq event parsing ─────────────────────────────────────────────────────────

def _groq_parse_events(subject: str, body: str, school_name: str, year_group: str) -> list[dict]:
    """
    Ask Groq to extract events/reminders from an email.
    Returns list of: {event_title, event_type, event_date, description, action_needed, deadline}
    """
    today_str = date.today().isoformat()
    system = (
        "You are a school communication parser. Extract all events, deadlines, reminders, "
        "and important dates from school emails. Return ONLY valid JSON, no markdown fences."
    )
    prompt = f"""School: {school_name}  Year group: {year_group}  Today: {today_str}

Email subject: {subject}
Email body:
{body}

Extract all events/reminders. Return a JSON array of objects, each with:
  event_title   : short title (max 10 words)
  event_type    : one of: event | trip | dinner | club | deadline | meeting | newsletter | other
  event_date    : ISO date (YYYY-MM-DD) or null if no specific date
  description   : 1-2 sentence summary
  action_needed : what the parent must do, or empty string
  deadline      : ISO date by which action is needed, or null

If nothing actionable, return [].
JSON array:"""

    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return []

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
                "max_tokens": 800,
                "temperature": 0.1,
            },
            timeout=20,
        )
        raw = r.json()["choices"][0]["message"]["content"].strip()
        # Strip markdown fences if model ignores instructions
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        events = json.loads(raw)
        if isinstance(events, list):
            return events
    except Exception as e:
        print(f"[school] groq parse error: {e}")
    return []


# ── Supabase helpers ───────────────────────────────────────────────────────────

def _get_profiles(from_number: str = None) -> list[dict]:
    q = lib._sb().table("school_profiles").select("*").eq("active", True)
    if from_number:
        q = q.eq("from_number", from_number)
    return q.execute().data or []


def _store_events(profile: dict, events: list[dict], gmail_msg_id: str):
    for ev in events:
        if not ev.get("event_title"):
            continue
        try:
            lib._sb().table("school_events").insert({
                "profile_id":    profile["id"],
                "from_number":   profile["from_number"],
                "event_title":   ev.get("event_title", "")[:200],
                "event_type":    ev.get("event_type", "other"),
                "event_date":    ev.get("event_date") or None,
                "description":   ev.get("description", "")[:500],
                "action_needed": ev.get("action_needed", "")[:300],
                "deadline":      ev.get("deadline") or None,
                "gmail_msg_id":  gmail_msg_id,
            }).execute()
        except Exception as e:
            # Unique constraint on gmail_msg_id — already processed
            if "unique" not in str(e).lower():
                print(f"[school] insert error: {e}")


def _get_upcoming_events(from_number: str, days: int = 14) -> list[dict]:
    today = date.today().isoformat()
    horizon = (date.today() + timedelta(days=days)).isoformat()
    return (
        lib._sb().table("school_events")
        .select("*")
        .eq("from_number", from_number)
        .gte("event_date", today)
        .lte("event_date", horizon)
        .order("event_date")
        .execute()
        .data or []
    )


def _get_this_week_events(from_number: str) -> list[dict]:
    today = date.today()
    # Mon–Sun of current week
    start = today - timedelta(days=today.weekday())
    end   = start + timedelta(days=6)
    return (
        lib._sb().table("school_events")
        .select("*")
        .eq("from_number", from_number)
        .gte("event_date", start.isoformat())
        .lte("event_date", end.isoformat())
        .order("event_date")
        .execute()
        .data or []
    )


# ── Email polling ──────────────────────────────────────────────────────────────

def poll_all_profiles(days_back: int = 7) -> dict:
    """
    For every active school profile, fetch emails from school senders,
    parse events, and store. Call this on a schedule (e.g. every 6h).
    Returns summary dict.
    """
    if not os.environ.get("GMAIL_REFRESH_TOKEN"):
        return {"error": "GMAIL_REFRESH_TOKEN not set"}

    profiles = _get_profiles()
    if not profiles:
        return {"profiles": 0, "emails": 0, "events": 0}

    # Group profiles by from_number so we only fetch Gmail once per parent
    by_parent: dict[str, list] = {}
    for p in profiles:
        by_parent.setdefault(p["from_number"], []).append(p)

    total_emails = total_events = 0

    for from_number, parent_profiles in by_parent.items():
        # Collect all sender emails across this parent's schools
        all_senders: list[str] = []
        for p in parent_profiles:
            all_senders.extend(p.get("sender_emails") or [])
        all_senders = list(set(all_senders))
        if not all_senders:
            continue

        query = _build_gmail_query(all_senders, days_back=days_back)
        try:
            res = _gmail_get("messages", {"q": query, "maxResults": 50})
        except Exception as e:
            print(f"[school] Gmail list error for {from_number}: {e}")
            continue

        msg_stubs = res.get("messages", [])
        total_emails += len(msg_stubs)

        for stub in msg_stubs:
            msg_id = stub["id"]
            try:
                msg = _gmail_get(f"messages/{msg_id}", {"format": "full"})
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

            subject, body = _extract_email_text(msg)
            if not body.strip():
                continue

            events = _groq_parse_events(
                subject, body,
                matched_profile["school_name"],
                matched_profile.get("year_group", ""),
            )
            if events:
                _store_events(matched_profile, events, gmail_msg_id=msg_id)
                total_events += len(events)

    return {"profiles": len(profiles), "emails": total_emails, "events": total_events}


# ── Digest formatting ──────────────────────────────────────────────────────────

_TYPE_EMOJI = {
    "event":      "📅",
    "trip":       "🚌",
    "dinner":     "🍽️",
    "club":       "⚽",
    "deadline":   "⏰",
    "meeting":    "👥",
    "newsletter": "📰",
    "other":      "📌",
}

def _format_date(d: str | None) -> str:
    if not d:
        return ""
    try:
        return datetime.fromisoformat(d).strftime("%-d %b")  # e.g. "7 May"
    except Exception:
        return d


def format_digest(events: list[dict], title: str = "Upcoming school events") -> str:
    if not events:
        return f"✅ *{title}*\n\nNothing on the school calendar right now."

    lines = [f"🏫 *{title}*\n"]
    current_date = None
    for ev in events:
        d = _format_date(ev.get("event_date"))
        if d and d != current_date:
            lines.append(f"\n*{d}*")
            current_date = d
        emoji = _TYPE_EMOJI.get(ev.get("event_type", "other"), "📌")
        lines.append(f"{emoji} {ev['event_title']}")
        if ev.get("description"):
            lines.append(f"   _{ev['description']}_")
        if ev.get("action_needed"):
            lines.append(f"   ✏️ Action: {ev['action_needed']}")
            if ev.get("deadline"):
                lines.append(f"   ⏰ By: {_format_date(ev['deadline'])}")

    return "\n".join(lines)


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


# ── WhatsApp conversation handler ──────────────────────────────────────────────

# Multi-step setup state: from_number → {step, data}
_SETUP_STATE: dict = {}

_SETUP_STEPS = ["child_name", "school_name", "year_group", "sender_emails"]
_SETUP_PROMPTS = {
    "child_name":    "What's your child's name?",
    "school_name":   "What's the school name? (e.g. *Greenway Academy*)",
    "year_group":    "Which year group? (e.g. *Year 4*)",
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
        else:
            state["data"][step] = text.strip()

        # Advance step
        idx = _SETUP_STEPS.index(step)
        if idx + 1 < len(_SETUP_STEPS):
            state["step"] = _SETUP_STEPS[idx + 1]
            return _next_setup_prompt(state)
        else:
            # All steps done — save profile
            data = state["data"]
            del _SETUP_STATE[from_number]
            try:
                lib._sb().table("school_profiles").insert({
                    "from_number":   from_number,
                    "child_name":    data.get("child_name", ""),
                    "school_name":   data.get("school_name", ""),
                    "year_group":    data.get("year_group", ""),
                    "sender_emails": data.get("sender_emails", []),
                }).execute()
            except Exception as e:
                return f"Sorry, couldn't save your school profile: {e}"

            school = data.get("school_name", "your school")
            child  = data.get("child_name", "")
            return (
                f"✅ Done! I'll watch for emails from *{school}*"
                + (f" for *{child}*" if child else "")
                + ".\n\n"
                "I'll send you a digest every Sunday evening. You can also ask anytime:\n"
                "• *school week* — this week's events\n"
                "• *school upcoming* — next 2 weeks\n"
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
        _SETUP_STATE[from_number] = {"step": "child_name", "data": {}}
        return (
            "🏫 *Add a school* (reply *cancel* at any time)\n\n"
            + _SETUP_PROMPTS["child_name"]
        )

    if cmd == "school week":
        events = _get_this_week_events(from_number)
        return format_digest(events, title="This week at school")

    if cmd in ("school upcoming", "school next"):
        events = _get_upcoming_events(from_number, days=14)
        return format_digest(events, title="Upcoming — next 14 days")

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

    # Unknown sub-command
    return (
        "🏫 *School Comms*\n"
        "Commands: *school week* | *school upcoming* | *school setup* | *school list*"
    )
