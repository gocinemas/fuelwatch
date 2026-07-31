"""
Personal Events Service — Monitor emails for event details and extract to homepage.
"""
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
import json
import re
import os
import time
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

        # Validate events: filter out any with dates that don't make sense
        # (e.g., stored with incorrect date from months ago that still fall within 30 days)
        valid_events = []
        for row in rows:
            event_date_str = row.get("event_date", "")
            try:
                event_date = date.fromisoformat(event_date_str)
                # Check if event was created more than 90 days ago but still hasn't passed
                # This filters out stale events with wrong future dates
                created_str = row.get("created_at", "")
                if created_str:
                    created_date = date.fromisoformat(created_str[:10])
                    days_old = (date.today() - created_date).days
                    # If event is >60 days old (created) and still marked as future, likely a bad entry
                    if days_old > 60 and (event_date - date.today()).days > 0:
                        continue
                valid_events.append(row)
            except (ValueError, TypeError):
                # Skip events with invalid dates
                continue

        return valid_events
    except Exception as e:
        print(f"[personal-events] Fetch error: {e}")
        return []


def get_directions_link(location: str) -> str:
    """Generate Waze directions link."""
    if not location:
        return ""
    encoded = location.replace(" ", "%20")
    return f"https://www.waze.com/?q={encoded}&navigate=yes"


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


def parse_natural_event_text(user_text: str, from_number: str) -> dict:
    """Parse natural language event from user input (e.g., 'Inaaya dance class today 3pm at Studio XYZ')."""
    try:
        ref = date.today()
        ref_str = ref.isoformat()
        weekday = ref.strftime("%A")

        prompt = f"""Extract event details from this natural language text. Today is {ref_str} ({weekday}).

User text: "{user_text}"

Return ONLY valid JSON (no markdown):
{{
  "event_title": "Event name",
  "event_date": "ISO date (YYYY-MM-DD) or null if unclear",
  "event_time": "HH:MM in 24h format or null if unclear",
  "location": "Full address or venue or null",
  "description": "Summary of the event",
  "is_event": true or false
}}

Handle relative dates: "today" = {ref_str}, "tomorrow" = {(ref + timedelta(days=1)).isoformat()}.
Extract times: "3pm" = 15:00, "3:30pm" = 15:30, etc.
If this is NOT an event (just random text), set is_event to false."""

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
                # Store in database
                try:
                    sb = lib._sb()
                    sb.table("personal_events").insert({
                        "gmail_msg_id": f"whatsapp_{from_number}_{int(time.time())}",
                        "email_from": from_number,
                        "event_title": parsed.get("event_title", "Event"),
                        "event_date": parsed.get("event_date"),
                        "event_time": parsed.get("event_time"),
                        "location": parsed.get("location"),
                        "description": parsed.get("description", ""),
                        "created_at": datetime.utcnow().isoformat(),
                    }).execute()
                    print(f"[event-parse] Stored: {parsed.get('event_title')}")
                    return {"success": True, "event": parsed}
                except Exception as e:
                    print(f"[event-parse] Store error: {e}")
                    return {"success": False, "error": f"Failed to save event: {str(e)}"}

        return {"success": False, "error": "Could not parse as event"}
    except Exception as e:
        print(f"[event-parse] Parse error: {e}")
        return {"success": False, "error": str(e)}
