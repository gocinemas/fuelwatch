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
    """Return (subject, body_text) from a Gmail message resource.
    Prefers text/plain; falls back to stripped HTML."""
    import base64

    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    subject = headers.get("subject", "")

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
    return subject, body[:5000]


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

Extract every item a parent should know about. Return a JSON array of objects, each with:
  event_title   : short title (max 10 words)
  event_type    : classify as exactly one of:
                  "activity"   — trips, sports days, shows, school events with a date
                  "reminder"   — deadlines, payments, consent forms, things parent must do
                  "club"       — after-school or lunchtime clubs
                  "dinner"     — school dinner menus, meal choices
                  "newsletter" — a newsletter or bulletin summary entry
                  "info"       — general info, policy updates, term dates, no action needed
  event_date    : ISO date (YYYY-MM-DD) or null if no specific date
  description   : 1-2 sentence plain summary
  action_needed : what the parent must do, or empty string
  deadline      : ISO date by which action is needed, or null

Rules:
- If the email subject contains "bulletin", "newsletter", "weekly update" or similar:
    * FIRST create one item of type "newsletter" with event_title = the subject line,
      and description = a 2-sentence summary of the whole bulletin
    * THEN also create separate items for any specific activities or reminders inside it
- For all other emails: create one item per distinct event/reminder/action
- Do NOT create duplicate items for the same event

If nothing relevant, return [].
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
                "max_tokens": 1200,
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
            raw_type = ev.get("event_type", "other") or "other"
            lib._sb().table("school_events").insert({
                "profile_id":    profile["id"],
                "from_number":   profile["from_number"],
                "event_title":   ev.get("event_title", "")[:200],
                "event_type":    raw_type.lower().strip(),
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
    if not events:
        return f"🏫 *{title}*\n\nNothing new from school right now."

    # Sort by date (nulls last), then title
    def _sort_key(e):
        return (e.get("event_date") or "9999-12-31", e.get("event_title", ""))
    sorted_events = sorted(events, key=_sort_key)

    # Group into sections
    used = set()
    section_map: dict[str, list] = {h: [] for _, h in _SECTIONS}
    for ev in sorted_events:
        etype = ev.get("event_type", "other")
        for types, header in _SECTIONS:
            if etype in types:
                section_map[header].append(ev)
                break
        used.add(ev.get("id"))

    lines = [f"🏫 *{title}*"]
    for types, header in _SECTIONS:
        bucket = section_map[header]
        if not bucket:
            continue
        lines.append(f"\n*{header}*")
        for ev in bucket:
            d = _format_date(ev.get("event_date"))
            date_str = f" — {d}" if d else ""
            lines.append(f"• {ev['event_title']}{date_str}")
            if ev.get("description"):
                lines.append(f"  _{ev['description']}_")
            if ev.get("action_needed"):
                dl = f" (by {_format_date(ev['deadline'])})" if ev.get("deadline") else ""
                lines.append(f"  ✏️ {ev['action_needed']}{dl}")

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
                "I'll send you a digest every Sunday evening. You can also ask anytime:\n"
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
