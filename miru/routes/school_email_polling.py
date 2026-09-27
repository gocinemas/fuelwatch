"""
School Email Polling — Monitor verified school emails via IMAP.

Flow:
1. Onboarding sends verified email to POST /api/school/email-verified
2. We store encrypted credentials + IMAP server
3. Background cron polls every 6 hours
4. Extract events, alerts, dates
5. Add to morning brief
"""

from flask import Blueprint, request, jsonify
import logging
import imaplib
import email
from datetime import datetime, timedelta
from email.header import decode_header
import re

logger = logging.getLogger(__name__)
bp = Blueprint('school_email_polling', __name__, url_prefix='/api/school')


def _get_imap_server(email_address):
    """Detect IMAP server based on email domain."""
    domain = email_address.split('@')[1].lower()

    imap_servers = {
        'gmail.com': 'imap.gmail.com',
        'yahoo.com': 'imap.mail.yahoo.com',
        'hotmail.com': 'imap-mail.outlook.com',
        'outlook.com': 'imap-mail.outlook.com',
        'scopay.com': 'mail.scopay.com',
        'protonmail.com': 'imap.protonmail.com',
    }

    # Check if domain is in our map
    if domain in imap_servers:
        return imap_servers[domain]

    # Generic fallback - try imap.domain.com
    return f"imap.{domain}"


def _extract_school_email_info(email_msg):
    """Extract key info from school email (events, dates, alerts)."""
    try:
        subject = email_msg.get('Subject', '').lower()
        sender = email_msg.get('From', '').lower()

        # Get email body
        body = ""
        if email_msg.is_multipart():
            for part in email_msg.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True).decode('utf-8', errors='ignore').lower()
                    break
        else:
            body = email_msg.get_payload(decode=True).decode('utf-8', errors='ignore').lower()

        full_text = subject + " " + body

        # Detect alert type
        alert_type = "update"
        if any(w in full_text for w in ["urgent", "alert", "emergency", "closure", "closed"]):
            alert_type = "urgent"
        elif any(w in full_text for w in ["event", "trip", "day", "sports", "assembly"]):
            alert_type = "event"
        elif any(w in full_text for w in ["payment", "fee", "lunch", "cost", "invoice"]):
            alert_type = "payment"
        elif any(w in full_text for w in ["newsletter", "news", "update"]):
            alert_type = "newsletter"

        # Extract date mentions
        dates = []
        date_patterns = [
            r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b',  # DD/MM/YYYY
            r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}\b',
            r'\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b',
        ]
        for pattern in date_patterns:
            dates.extend(re.findall(pattern, full_text, re.IGNORECASE))

        return {
            "subject": email_msg.get('Subject', ''),
            "from": email_msg.get('From', ''),
            "date": email_msg.get('Date', ''),
            "alert_type": alert_type,
            "dates_mentioned": dates[:3],  # Top 3 dates
        }
    except Exception as e:
        logger.error(f"Error extracting email info: {e}")
        return None


def _poll_imap(email_address, app_password, days_back=7):
    """Poll IMAP inbox for school emails."""
    try:
        imap_server = _get_imap_server(email_address)

        # Connect to IMAP
        imap = imaplib.IMAP4_SSL(imap_server, 993)
        imap.login(email_address, app_password)
        imap.select('INBOX')

        # Search for emails from last N days
        since_date = (datetime.now() - timedelta(days=days_back)).strftime('%d-%b-%Y')
        status, messages = imap.search(None, f'SINCE {since_date}')

        emails = []
        msg_ids = messages[0].split()[-10:]  # Last 10 emails

        for msg_id in msg_ids:
            status, msg_data = imap.fetch(msg_id, '(RFC822)')
            email_msg = email.message_from_bytes(msg_data[0][1])

            info = _extract_school_email_info(email_msg)
            if info:
                emails.append(info)

        imap.close()
        imap.logout()

        return {
            "success": True,
            "emails_found": len(emails),
            "emails": emails
        }

    except Exception as e:
        logger.error(f"IMAP polling failed: {e}")
        return {
            "success": False,
            "error": str(e)
        }


@bp.route('/email-verified', methods=['POST'])
def email_verified():
    """
    Onboarding Step 5 callback: Store verified school email + start polling.
    Body: {school_id, email, imap_password, user_phone}
    """
    data = request.get_json() or {}
    email_address = (data.get('email') or '').strip().lower()
    imap_password = (data.get('imap_password') or '').strip()
    school_id = (data.get('school_id') or '').strip()
    user_phone = (data.get('user_phone') or '').strip()

    if not all([email_address, imap_password, school_id, user_phone]):
        return jsonify({"ok": False, "error": "missing fields"}), 400

    try:
        from sms_service import lib

        # Test IMAP connection first
        imap_server = _get_imap_server(email_address)
        test_imap = imaplib.IMAP4_SSL(imap_server, 993)
        test_imap.login(email_address, imap_password)
        test_imap.logout()

        # Store in database
        lib._sb().table("school_email_credentials").insert({
            "device_id": user_phone,
            "school_id": school_id,
            "email": email_address,
            "imap_password_encrypted": _encrypt_password(imap_password),
            "imap_server": imap_server,
            "verified": True,
            "created_at": datetime.utcnow().isoformat(),
            "last_polled_at": None,
        }).execute()

        logger.info(f"[school-email] Stored verified email: {email_address} for {school_id}")

        return jsonify({
            "ok": True,
            "email": email_address,
            "polling_interval": "6 hours"
        }), 200

    except Exception as e:
        logger.error(f"[school-email] Error storing email: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route('/emails', methods=['GET'])
def get_school_emails():
    """Get recent school emails for a user."""
    user_phone = request.args.get('phone', '').strip()
    school_id = request.args.get('school_id', '').strip()

    if not user_phone:
        return jsonify({"ok": False, "error": "phone required"}), 400

    try:
        from sms_service import lib

        # Get verified emails for this user
        query = lib._sb().table("school_email_credentials").select("*") \
            .eq("device_id", user_phone)

        if school_id:
            query = query.eq("school_id", school_id)

        rows = query.execute().data or []

        all_emails = []
        for row in rows:
            # Poll this email
            result = _poll_imap(row['email'], _decrypt_password(row['imap_password_encrypted']))
            if result.get('success'):
                all_emails.extend(result.get('emails', []))

            # Update last_polled
            lib._sb().table("school_email_credentials").update({
                "last_polled_at": datetime.utcnow().isoformat()
            }).eq("id", row['id']).execute()

        return jsonify({
            "ok": True,
            "emails": all_emails,
            "count": len(all_emails)
        }), 200

    except Exception as e:
        logger.error(f"[school-email] Error fetching emails: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route('/poll-all', methods=['POST'])
def poll_all_emails():
    """Background cron: Poll all verified emails."""
    token = request.headers.get('X-Cron-Token', '')
    if token != "miru-digest-2026":
        return jsonify({"error": "Forbidden"}), 403

    try:
        from sms_service import lib

        # Get all verified emails
        rows = lib._sb().table("school_email_credentials").select("*") \
            .eq("verified", True).execute().data or []

        total_emails = 0
        for row in rows:
            result = _poll_imap(row['email'], _decrypt_password(row['imap_password_encrypted']))
            if result.get('success'):
                total_emails += result.get('emails_found', 0)

            # Update last_polled
            lib._sb().table("school_email_credentials").update({
                "last_polled_at": datetime.utcnow().isoformat()
            }).eq("id", row['id']).execute()

        logger.info(f"[school-email] Cron poll complete: {len(rows)} emails, {total_emails} messages found")

        return jsonify({
            "ok": True,
            "emails_polled": len(rows),
            "messages_found": total_emails
        }), 200

    except Exception as e:
        logger.error(f"[school-email] Cron poll error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


def _encrypt_password(password):
    """Encrypt password (TODO: proper encryption)."""
    import base64
    return base64.b64encode(password.encode()).decode()[::-1]


def _decrypt_password(encrypted):
    """Decrypt password (TODO: proper decryption)."""
    import base64
    try:
        return base64.b64decode(encrypted[::-1].encode()).decode()
    except:
        return encrypted


def register_school_email_endpoints(app):
    """Register school email polling endpoints."""
    app.register_blueprint(bp)
    logger.info("[school-email] School email polling endpoints registered")
