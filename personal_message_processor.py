"""
Personal Message Processor — Smart WhatsApp Message Router
============================================================

Reads incoming WhatsApp messages and intelligently routes them to:
- School communications module
- Personal events/calendar
- Personal TODOs
- Receipt scanning (existing)
- Direct responses

Architecture:
  WhatsApp Message → /whatsapp endpoint
    ↓
  personal_message_processor.process_message()
    ↓
  Claude extracts: category, summary, TODOs, needs_response
    ↓
  Smart routing:
    - school → school_events table
    - event → personal_events table
    - todo → personal_todos table
    - question → direct response
    - receipt → existing receipt scanner
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import anthropic
from supabase import create_client

logger = logging.getLogger(__name__)

def get_supabase():
    """Get Supabase client."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY required")
    return create_client(url, key)

def get_claude():
    """Get Claude client."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY required")
    return anthropic.Anthropic(api_key=api_key)

# ─────────────────────────────────────────────────────────────────────
# 1. SMART MESSAGE EXTRACTION & ROUTING
# ─────────────────────────────────────────────────────────────────────

def extract_message_intent(body: str) -> Dict[str, Any]:
    """
    Call Claude to extract structured intent from message.

    Returns:
    {
      "category": "school|todo|event|question|note|receipt|fyi",
      "summary": "one liner",
      "todos": [{"text": "...", "due_date": "...", "priority": "..."}],
      "event": {"title": "...", "date": "...", "description": "..."},
      "school_related": bool,
      "needs_response": bool,
      "suggested_response": "text to send back"
    }
    """

    claude = get_claude()

    prompt = f"""Analyze this WhatsApp message and extract structured intent.

Message: {body}

Return JSON with:
- category: "school"|"todo"|"event"|"question"|"note"|"receipt"|"fyi"
- summary: one sentence summary
- school_related: true if mentions school/homework/teacher/class
- event: {{title, date (YYYY-MM-DD or null), description}} if it's an event
- todos: [{{text, due_date, priority}}] if it contains action items
- needs_response: boolean
- suggested_response: text if needs_response=true
- confidence: 0-1 (how confident are you in the categorization?)

Only return valid JSON, no markdown."""

    response = claude.messages.create(
        model="claude-opus-5",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )

    try:
        return json.loads(response.content[0].text)
    except json.JSONDecodeError:
        logger.error(f"Claude returned invalid JSON: {response.content[0].text}")
        return {
            "category": "note",
            "summary": body[:100],
            "todos": [],
            "needs_response": False,
            "confidence": 0
        }


def process_message(
    message_id: str,
    from_number: str,
    body: str,
    media_urls: list = None
) -> Dict[str, Any]:
    """
    Process incoming WhatsApp message and route to appropriate systems.

    Args:
        message_id: UUID from messages table
        from_number: WhatsApp number (whatsapp:+44...)
        body: Message text
        media_urls: List of media URLs (images, documents)

    Returns:
        Processing result with routing decisions
    """

    if not media_urls:
        media_urls = []

    try:
        sb = get_supabase()
        logger.info(f"Processing message {message_id}: {body[:50]}")

        # 1. Extract intent using Claude
        extraction = extract_message_intent(body)
        category = extraction.get("category", "note")
        confidence = extraction.get("confidence", 0.5)

        logger.info(f"Extracted category={category}, confidence={confidence}")

        # 2. Store extraction in database
        sb.table("message_processing").insert({
            "message_id": message_id,
            "summary": extraction.get("summary"),
            "category": category,
            "needs_response": extraction.get("needs_response", False),
            "claude_model": "claude-opus-5",
            "raw_response": extraction
        }).execute()

        # 3. Route based on category
        result = {
            "message_id": message_id,
            "category": category,
            "routes": []
        }

        # Route: School-related messages
        if extraction.get("school_related") or category == "school":
            result["routes"].append("school_events")
            route_to_school(from_number, body, extraction)

        # Route: Events/Calendar
        if category == "event" and extraction.get("event"):
            result["routes"].append("personal_events")
            route_to_event(from_number, extraction.get("event"), body)

        # Route: TODOs
        if extraction.get("todos"):
            result["routes"].append("personal_todos")
            route_to_todos(message_id, extraction.get("todos"))

        # Route: Needs response
        if extraction.get("needs_response"):
            result["routes"].append("response")
            # Response would be sent back via Twilio (handled by caller)

        logger.info(f"✅ Routed message to: {result['routes']}")
        return result

    except Exception as e:
        logger.exception(f"Error processing message {message_id}: {e}")
        return {
            "message_id": message_id,
            "error": str(e),
            "routes": []
        }


# ─────────────────────────────────────────────────────────────────────
# 2. ROUTING FUNCTIONS TO INTEGRATE WITH EXISTING MODULES
# ─────────────────────────────────────────────────────────────────────

def route_to_school(from_number: str, body: str, extraction: Dict):
    """
    Route school-related messages to school_events table.

    Integrates with existing school_comms module.
    """

    try:
        sb = get_supabase()

        # Get school profile for this user
        profile_result = sb.table("school_profiles").select("id").eq(
            "from_number", from_number
        ).limit(1).execute()

        if not profile_result.data:
            logger.warning(f"No school profile for {from_number}, skipping school routing")
            return

        profile_id = profile_result.data[0]["id"]

        # Extract event details from message
        event_data = {
            "profile_id": profile_id,
            "from_number": from_number,
            "event_title": extraction.get("summary", body[:100]),
            "event_type": "message",
            "description": body,
            "action_needed": "Review message from school",
            "source": "whatsapp_personal_processor"
        }

        # Add event date if extracted
        if extraction.get("event", {}).get("date"):
            event_data["event_date"] = extraction["event"]["date"]

        # Insert into school_events
        sb.table("school_events").insert(event_data).execute()
        logger.info(f"✅ Routed to school_events for {from_number}")

    except Exception as e:
        logger.exception(f"Error routing to school: {e}")


def route_to_event(from_number: str, event_data: Dict, original_message: str):
    """
    Route event messages to personal_events.

    Integrates with personal_events_service module.
    """

    try:
        sb = get_supabase()

        # Insert into personal_events (or equivalent table)
        # This assumes personal_events table exists in Supabase
        event_record = {
            "from_number": from_number,
            "event_date": event_data.get("date"),
            "event_title": event_data.get("title", "Event from WhatsApp"),
            "description": event_data.get("description", original_message),
            "source": "whatsapp_personal_processor",
            "original_message": original_message
        }

        # Check if table exists, if not log warning
        try:
            sb.table("personal_events").insert(event_record).execute()
            logger.info(f"✅ Routed to personal_events for {from_number}")
        except Exception as e:
            if "does not exist" in str(e).lower():
                logger.warning("personal_events table not found, skipping event routing")
            else:
                raise

    except Exception as e:
        logger.exception(f"Error routing to event: {e}")


def route_to_todos(message_id: str, todos: list):
    """
    Route extracted TODOs to personal_todos table.

    Creates TODO records for personal task management.
    """

    try:
        sb = get_supabase()

        for todo in todos:
            todo_record = {
                "source_message_id": message_id,
                "text": todo.get("text"),
                "due_date": todo.get("due_date"),
                "priority": todo.get("priority", "medium"),
                "status": "open"
            }

            sb.table("todos").insert(todo_record).execute()

        logger.info(f"✅ Created {len(todos)} TODOs from message {message_id}")

    except Exception as e:
        logger.exception(f"Error routing to todos: {e}")


# ─────────────────────────────────────────────────────────────────────
# 3. CLASSIFICATION HELPER
# ─────────────────────────────────────────────────────────────────────

def is_likely_receipt(body: str, media_count: int) -> bool:
    """
    Quick heuristic: is this message likely a receipt?
    Prevents unnecessary Claude calls for obvious receipts.
    """

    if media_count > 0:
        # If there are images, probably a receipt
        return True

    receipt_keywords = [
        "receipt", "invoice", "order", "purchase", "paid", "price",
        "total", "amount", "£", "$", "€", "qty", "item", "transaction"
    ]

    body_lower = body.lower()
    return any(keyword in body_lower for keyword in receipt_keywords)


def is_likely_school(body: str) -> bool:
    """
    Quick heuristic: is this likely school-related?
    """

    school_keywords = [
        "school", "homework", "teacher", "class", "lesson", "exam",
        "uniform", "permission", "trip", "sports day", "parents evening",
        "pickup", "drop off", "year group", "form tutor"
    ]

    body_lower = body.lower()
    return any(keyword in body_lower for keyword in school_keywords)
