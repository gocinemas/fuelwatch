"""
Personal Assistant Bot — Webhook Handler
==========================================

Receives WhatsApp messages and routes them to personal_message_processor
for intelligent extraction and routing to appropriate modules (school_comms,
personal_events, personal TODOs, etc.)

Workflow:
1. Receive message via Twilio webhook
2. Validate Twilio signature
3. Check idempotency
4. Store raw message (durability first)
5. Return immediate TwiML ack
6. Async: Call personal_message_processor.process_message()
   → Routes to school_comms, personal_events, TODOs, etc.
7. Send follow-up response if needed
"""

import os
import logging
import threading
from flask import Blueprint, request, jsonify
from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator
from supabase import create_client
from personal_message_processor import process_message

logger = logging.getLogger(__name__)

def get_supabase():
    """Get Supabase client."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY required")
    return create_client(url, key)

personal_bot_bp = Blueprint('personal_bot', __name__)

# ─────────────────────────────────────────────────────────────────────
# WEBHOOK ENTRY POINT
# ─────────────────────────────────────────────────────────────────────

@personal_bot_bp.route('/personal-bot/whatsapp', methods=['POST'])
def personal_bot_whatsapp_webhook():
    """
    Twilio inbound WhatsApp webhook for personal message processing.

    Receives all WhatsApp messages, validates, stores, and routes to
    appropriate systems (school, events, TODOs, etc).
    """

    # 1. Validate Twilio signature
    twilio_token = os.environ.get("TWILIO_AUTH_TOKEN")
    if not twilio_token:
        logger.error("TWILIO_AUTH_TOKEN not set")
        return jsonify({"error": "Server misconfiguration"}), 500

    validator = RequestValidator(twilio_token)
    if not validator.validate_request(
        request.base_url,
        request.form,
        request.headers.get('X-Twilio-Signature', '')
    ):
        logger.warning("Invalid Twilio signature")
        return jsonify({"error": "Unauthorized"}), 403

    # 2. Extract message data
    from_number = request.form.get('From')
    to_number = request.form.get('To')
    body = request.form.get('Body', '')
    message_sid = request.form.get('MessageSid')
    num_media = int(request.form.get('NumMedia', 0))

    media_urls = []
    for i in range(num_media):
        media_url = request.form.get(f'MediaUrl{i}')
        if media_url:
            media_urls.append(media_url)

    try:
        sb = get_supabase()

        # 3. Check idempotency
        existing = sb.table('messages').select('id').eq('wa_message_sid', message_sid).execute()
        if existing.data:
            logger.info(f"Message {message_sid} already processed")
            return MessagingResponse().to_xml()

        # 4. Store raw message
        msg_result = sb.table('messages').insert({
            'direction': 'inbound',
            'from_number': from_number,
            'to_number': to_number,
            'wa_message_sid': message_sid,
            'body': body,
            'media_urls': media_urls,
            'num_media': num_media,
            'status': 'received'
        }).execute()

        message_id = msg_result.data[0]['id']
        logger.info(f"Stored message {message_id}: {body[:50]}")

        # 5. Return immediate TwiML ack
        resp = MessagingResponse()
        resp.message("👍 Got it")

        # 6. Fire background processing
        thread = threading.Thread(
            target=process_message_async,
            args=(message_id, from_number, body, media_urls)
        )
        thread.daemon = True
        thread.start()

        return resp.to_xml()

    except Exception as e:
        logger.exception(f"Webhook error: {e}")
        return jsonify({"error": "Server error"}), 500


def process_message_async(message_id: str, from_number: str, body: str, media_urls: list):
    """
    Async background processing: route message to appropriate systems.

    Calls personal_message_processor which:
    - Extracts intent with Claude
    - Routes to school_comms if school-related
    - Routes to personal_events if event
    - Routes to personal TODOs if action items
    - Sends follow-up response if needed
    """

    try:
        sb = get_supabase()

        # Update status
        sb.table('messages').update({'status': 'processing'}).eq('id', message_id).execute()

        # Call the message processor (does all the routing)
        result = process_message(message_id, from_number, body, media_urls)

        logger.info(f"✅ Routed message to: {result.get('routes', [])}")

        # Update status to processed
        sb.table('messages').update({'status': 'processed'}).eq('id', message_id).execute()

    except Exception as e:
        logger.exception(f"Error processing message {message_id}: {e}")
        try:
            sb.table('messages').update({'status': 'failed'}).eq('id', message_id).execute()
        except:
            pass


# ─────────────────────────────────────────────────────────────────────
# 3. API ENDPOINTS FOR MANAGING TODOs
# ─────────────────────────────────────────────────────────────────────

def get_api_token_from_header():
    """Extract and validate the API token from Authorization header."""
    auth_header = request.headers.get('Authorization', '')
    expected_token = os.environ.get('PERSONAL_API_TOKEN')

    if not expected_token:
        raise ValueError("PERSONAL_API_TOKEN not configured")

    # Expected format: "Bearer <token>"
    parts = auth_header.split()
    if len(parts) != 2 or parts[0] != 'Bearer':
        raise ValueError("Invalid authorization header")

    if parts[1] != expected_token:
        raise ValueError("Invalid token")


@personal_bot_bp.route('/api/todos', methods=['GET'])
def list_todos():
    """
    List all TODOs with optional filtering.

    Query params:
    - status: "open" | "done" | "snoozed" | "cancelled"
    - priority: "low" | "medium" | "high"
    """

    try:
        get_api_token_from_header()
        sb = get_supabase()

        # Build query
        query = sb.table('todos').select('*')

        if request.args.get('status'):
            query = query.eq('status', request.args.get('status'))

        if request.args.get('priority'):
            query = query.eq('priority', request.args.get('priority'))

        # Order by due_date
        result = query.order('due_date', desc=False).execute()

        return jsonify(result.data), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        logger.exception(f"Error listing todos: {e}")
        return jsonify({"error": "Internal server error"}), 500


@personal_bot_bp.route('/api/todos/<todo_id>', methods=['PATCH'])
def update_todo(todo_id: str):
    """Update a TODO (status, priority, due_date, etc.)."""

    try:
        get_api_token_from_header()
        sb = get_supabase()

        data = request.get_json()
        update_data = {}

        # Only allow specific fields to be updated
        allowed_fields = ['status', 'priority', 'due_date', 'text']
        for field in allowed_fields:
            if field in data:
                update_data[field] = data[field]

        if field == 'status' and data.get('status') == 'done':
            update_data['completed_at'] = datetime.utcnow().isoformat()

        update_data['updated_at'] = datetime.utcnow().isoformat()

        result = sb.table('todos').update(update_data).eq('id', todo_id).execute()

        if not result.data:
            return jsonify({"error": "TODO not found"}), 404

        return jsonify(result.data[0]), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        logger.exception(f"Error updating todo: {e}")
        return jsonify({"error": "Internal server error"}), 500


@personal_bot_bp.route('/api/messages', methods=['GET'])
def list_messages():
    """List raw messages (for debugging)."""

    try:
        get_api_token_from_header()
        sb = get_supabase()

        # Paginated query
        limit = int(request.args.get('limit', 20))
        offset = int(request.args.get('offset', 0))

        result = sb.table('messages').select('*').order(
            'created_at', desc=True
        ).range(offset, offset + limit - 1).execute()

        return jsonify(result.data), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        logger.exception(f"Error listing messages: {e}")
        return jsonify({"error": "Internal server error"}), 500


@personal_bot_bp.route('/api/summaries/daily', methods=['GET'])
def get_daily_summary():
    """Fetch the latest daily summary."""

    try:
        get_api_token_from_header()
        sb = get_supabase()

        result = sb.table('summaries').select('*').eq(
            'period_type', 'daily'
        ).order('period_start', desc=True).limit(1).execute()

        if not result.data:
            return jsonify({"error": "No summary found"}), 404

        return jsonify(result.data[0]), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        logger.exception(f"Error getting summary: {e}")
        return jsonify({"error": "Internal server error"}), 500


@personal_bot_bp.route('/api/digest/run', methods=['POST'])
def run_daily_digest():
    """
    Cron-triggered endpoint to build a daily digest.

    Requires digest cron token (like miru-digest-2026).
    """

    try:
        # Validate cron token
        token = request.headers.get('X-Cron-Token') or request.args.get('token')
        expected_token = os.environ.get('DIGEST_CRON_TOKEN')

        if not expected_token or token != expected_token:
            return jsonify({"error": "Invalid cron token"}), 401

        sb = get_supabase()
        claude = get_claude()

        # Get today's messages
        from datetime import date
        today = date.today()

        messages_result = sb.table('messages').select('*').eq(
            'status', 'processed'
        ).gte('created_at', f"{today}T00:00:00").execute()

        messages = messages_result.data

        # Get today's todos
        todos_result = sb.table('todos').select('*').gte(
            'created_at', f"{today}T00:00:00"
        ).execute()

        todos = todos_result.data

        if not messages and not todos:
            return jsonify({"message": "No messages or todos today"}), 200

        # Build digest with Claude
        digest_prompt = f"""Create a brief daily digest summary.

Messages ({len(messages)}):
{json.dumps([m['body'] for m in messages[:10]], indent=2)}

TODOs ({len(todos)}):
{json.dumps([t['text'] for t in todos[:10]], indent=2)}

Provide a 2-3 sentence summary."""

        response = claude.messages.create(
            model="claude-opus-5",
            max_tokens=300,
            messages=[
                {"role": "user", "content": digest_prompt}
            ]
        )

        summary_text = response.content[0].text

        # Store in summaries table
        sb.table('summaries').insert({
            'period_type': 'daily',
            'period_start': today,
            'period_end': today,
            'summary_text': summary_text,
            'message_count': len(messages),
            'todo_count': len(todos)
        }).execute()

        return jsonify({
            "message": "Digest created",
            "summary": summary_text,
            "message_count": len(messages),
            "todo_count": len(todos)
        }), 200

    except Exception as e:
        logger.exception(f"Error running digest: {e}")
        return jsonify({"error": "Internal server error"}), 500


def register_personal_bot_endpoints(app):
    """Register the personal bot blueprint with the Flask app."""
    app.register_blueprint(personal_bot_bp)
    logger.info("✅ Personal WhatsApp Assistant Bot registered")
