"""
Personal Events Service — Monitor emails for event details and extract to homepage.
"""
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
import json
import re
import os
import base64
import requests
from groq import Groq

import library as lib

groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# Gmail API helper
def _gmail_get(resource: str, params: dict = None, access_token: str = None):
    """Call Gmail API."""
    if not access_token:
        access_token = os.environ.get("GMAIL_ACCESS_TOKEN", "")

    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://www.googleapis.com/gmail/v1/users/me/{resource}"
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def _extract_email_text(msg: dict) -> tuple[str, str, str]:
    """Extract subject, body, sent_date from Gmail message."""
    from email.utils import parsedate_to_datetime

    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    subject = headers.get("subject", "")

    # Extract date
    sent_date = ""
    raw_date = headers.get("date", "")
    if raw_date:
        try:
            sent_date = parsedate_to_datetime(raw_date).date().isoformat()
        except:
            sent_date = date.today().isoformat()

    # Extract body
    payload = msg.get("payload", {})
    body = ""

    def _walk(parts):
        nonlocal body
        for part in parts:
            if part.get("parts"):
                _walk(part["parts"])
            mime = part.get("mimeType", "")
            data = part.get("body", {}).get("data", "")
            if not data:
                continue
            try:
                decoded = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")
                if mime == "text/plain":
                    body = decoded
                elif mime == "text/html" and not body:
                    body = decoded
            except:
                pass

    if payload.get("parts"):
        _walk(payload["parts"])
    else:
        data = payload.get("body", {}).get("data", "")
        if data:
            try:
                body = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")
            except:
                pass

    return subject, body, sent_date

def parse_event_email(subject: str, body: str, sent_date: str = "") -> Optional[Dict]:
    """
    Parse email for event details (date, time, location, description).
    Returns dict with event_title, event_date, event_time, location, description, etc.
    """
    try:
        ref = date.fromisoformat(sent_date) if sent_date else date.today()
    except ValueError:
        ref = date.today()

    ref_str = ref.isoformat()
    weekday = ref.strftime("%A")

    prompt = f"""Extract event details from this personal email. Email sent: {ref_str} ({weekday})

Subject: {subject}
Body:
{body}

Return ONLY valid JSON (no markdown):
{{
  "event_title": "Event name (max 50 words)",
  "event_date": "ISO date (YYYY-MM-DD) or null",
  "event_time": "HH:MM in 24h format or null",
  "location": "Full address or venue name or null",
  "description": "2-3 sentence summary",
  "is_event": true or false
}}

If this is NOT an event email (e.g., receipt, newsletter, no actionable event), set is_event to false.
Extract dates carefully: "tomorrow" = {(date.fromisoformat(ref_str) + timedelta(days=1)).isoformat()}, "next Monday" = first Monday after {ref_str}.
"""

    try:
        message = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        response_text = message.choices[0].message.content.strip()

        # Extract JSON
        start = response_text.find('{')
        end = response_text.rfind('}') + 1
        if start >= 0 and end > start:
            json_str = response_text[start:end]
            parsed = json.loads(json_str)
            if parsed.get("is_event"):
                return parsed
    except Exception as e:
        print(f"[personal-events] Groq parse error: {e}")

    return None


def store_personal_event(email_id: str, email_from: str, event: Dict) -> bool:
    """Store parsed event in database."""
    try:
        sb = lib._sb()

        # Check if already stored
        existing = sb.table("personal_events").select("id") \
            .eq("gmail_msg_id", email_id) \
            .execute().data or []

        if existing:
            return True  # Already stored

        sb.table("personal_events").insert({
            "gmail_msg_id": email_id,
            "email_from": email_from,
            "event_title": event.get("event_title", ""),
            "event_date": event.get("event_date"),
            "event_time": event.get("event_time"),
            "location": event.get("location"),
            "description": event.get("description", ""),
            "created_at": datetime.utcnow().isoformat(),
        }).execute()

        print(f"[personal-events] Stored: {event.get('event_title')}")
        return True
    except Exception as e:
        print(f"[personal-events] Store error: {e}")
        return False


def get_personal_events(days_ahead: int = 30) -> List[Dict]:
    """Get upcoming personal events."""
    try:
        sb = lib._sb()
        today = date.today().isoformat()
        future = (date.today() + timedelta(days=days_ahead)).isoformat()

        rows = sb.table("personal_events").select("*") \
            .gte("event_date", today) \
            .lte("event_date", future) \
            .order("event_date") \
            .execute().data or []

        return rows
    except Exception as e:
        print(f"[personal-events] Fetch error: {e}")
        return []


def get_directions_link(location: str) -> str:
    """Generate Waze directions link."""
    if not location:
        return ""
    encoded = location.replace(" ", "%20")
    return f"https://waze.com/ul?q={encoded}"


def ensure_table_exists():
    """Create personal_events table if it doesn't exist."""
    try:
        sb = lib._sb()
        # Try to query - if table doesn't exist, this will error
        sb.table("personal_events").select("id").limit(1).execute()
        print("[personal-events] Table exists")
        return True
    except:
        # Table doesn't exist, create it
        try:
            print("[personal-events] Creating table...")
            # Note: This requires direct SQL execution via Supabase
            # For now, return False and user can create manually or use Supabase UI
            return False
        except Exception as e:
            print(f"[personal-events] Table creation error: {e}")
            return False


def scan_and_parse_emails(access_token: str = None, days_back: int = 7) -> List[Dict]:
    """
    Scan mekala@gmail.com for emails from reddyaemalla@gmail.com.
    Parse for events and store in database.
    """
    if not access_token:
        access_token = os.environ.get("GMAIL_ACCESS_TOKEN", "")

    if not access_token:
        print("[personal-events] No Gmail access token available")
        return []

    try:
        # Search for emails from reddyaemalla@gmail.com to mekala@gmail.com from last week
        after_date = (date.today() - timedelta(days=days_back)).strftime("%Y/%m/%d")
        query = f'from:reddyaemalla@gmail.com to:mekala@gmail.com after:{after_date}'

        print(f"[personal-events] Scanning Gmail: {query}")

        res = _gmail_get("messages", {"q": query, "maxResults": 50}, access_token=access_token)
        msg_stubs = res.get("messages", [])
        print(f"[personal-events] Found {len(msg_stubs)} emails")

        stored_events = []

        for stub in msg_stubs:
            msg_id = stub["id"]
            try:
                # Fetch full message
                msg = _gmail_get(f"messages/{msg_id}", {"format": "full"}, access_token=access_token)
                subject, body, sent_date = _extract_email_text(msg)

                print(f"[personal-events] Parsing: {subject[:60]}")

                # Parse for event details
                event = parse_event_email(subject, body, sent_date)

                if event:
                    # Store in database
                    if store_personal_event(msg_id, "reddyaemalla@gmail.com", event):
                        stored_events.append(event)
                        print(f"[personal-events] ✅ Stored: {event.get('event_title')}")

            except Exception as e:
                print(f"[personal-events] Error processing {msg_id}: {e}")

        return stored_events

    except Exception as e:
        print(f"[personal-events] Scan error: {e}")
        return []
