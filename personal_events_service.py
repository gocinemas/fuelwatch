"""
Personal Events Service — Monitor emails for event details and extract to homepage.
"""
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
import json
import re
import os
from groq import Groq

import library as lib

groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

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
    """Generate Google Maps directions link."""
    if not location:
        return ""
    encoded = location.replace(" ", "+")
    return f"https://maps.google.com/?q={encoded}"
