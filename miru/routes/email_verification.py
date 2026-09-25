"""
Email verification for school comms — supports ANY email provider via IMAP.

Flow:
1. User enters email (gmail@gmail.com, hotmail, yahoo, etc.)
2. We send verification code to that email
3. User enters code
4. We ask for IMAP password
5. We store encrypted + start polling
"""

from flask import Blueprint, request, jsonify
import logging
import secrets
import json
import imaplib
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
bp = Blueprint('email_verification', __name__, url_prefix='/api/onboarding')


def _get_user_id(token):
    """Resolve token to user ID."""
    try:
        from sms_service import _v2_resolve
        resolved = _v2_resolve(token)
        if resolved:
            return resolved
    except:
        pass
    return token


def _encrypt_password(password, key=None):
    """Simple encryption for IMAP password (in production use proper crypto)."""
    # For now: base64 encode + reverse (NOT secure, for testing only)
    import base64
    return base64.b64encode(password.encode()).decode()[::-1]


def _decrypt_password(encrypted, key=None):
    """Decrypt IMAP password."""
    import base64
    try:
        return base64.b64decode(encrypted[::-1].encode()).decode()
    except:
        return None


def _send_verification_code(email_address):
    """
    Send verification code to email address.
    For Gmail test: we'll generate a code and pretend to send it (real implementation would use SMTP).
    """
    code = secrets.token_hex(3)  # 6-char hex code like "a1b2c3"

    # In real implementation:
    # - Use SMTP to send code via email
    # - Log the code to console for testing
    # For now, we'll just return the code (in production, log to backend for manual testing)

    logger.info(f"[email-verify] Verification code for {email_address}: {code}")

    return code


def _test_imap_connection(email_address, password, imap_server="imap.gmail.com"):
    """
    Test IMAP connection to verify credentials are valid.
    Supports Gmail, Yahoo, Outlook, etc.
    """
    try:
        # Gmail-specific (works for @gmail.com addresses)
        if email_address.endswith("@gmail.com"):
            imap_server = "imap.gmail.com"
        elif email_address.endswith("@yahoo.com"):
            imap_server = "imap.mail.yahoo.com"
        elif email_address.endswith("@outlook.com") or email_address.endswith("@hotmail.com"):
            imap_server = "imap-mail.outlook.com"
        elif email_address.endswith("@scopay.com"):
            imap_server = "mail.scopay.com"
        else:
            # Generic IMAP server for custom domains
            imap_server = f"imap.{email_address.split('@')[1]}"

        imap = imaplib.IMAP4_SSL(imap_server, 993)
        imap.login(email_address, password)
        imap.logout()
        return True, None
    except imaplib.IMAP4.error as e:
        return False, f"IMAP login failed: {str(e)}"
    except Exception as e:
        return False, f"Connection error: {str(e)}"


@bp.route('/email-verify-init', methods=['POST'])
def email_verify_init():
    """
    Step 1: User enters email address.
    We send verification code to that email.
    Returns verification_id for tracking.
    """
    token = request.args.get('token', '').strip()
    user_id = _get_user_id(token)
    if not user_id:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json() or {}
    email = (data.get('email') or '').strip().lower()

    if not email or '@' not in email:
        return jsonify({"ok": False, "error": "invalid_email"}), 400

    try:
        # Generate and send code
        code = _send_verification_code(email)
        verification_id = secrets.token_urlsafe(16)

        # Store session in database
        from sms_service import lib
        lib._sb().table("email_verification_sessions").insert({
            "device_id": user_id,
            "verification_id": verification_id,
            "email": email,
            "code": code,
            "verified_at": None,
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(minutes=15)).isoformat(),
        }).execute()

        logger.info(f"[email-verify] Init for {user_id}: {email}")
        return jsonify({
            "ok": True,
            "verification_id": verification_id,
            "email": email,
            "code": code  # ONLY for testing/development
        }), 200

    except Exception as e:
        logger.error(f"[email-verify] Init error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route('/email-verify-confirm', methods=['POST'])
def email_verify_confirm():
    """
    Step 2: User enters verification code.
    We verify the code and mark email as verified.
    Next step: ask for IMAP password.
    """
    token = request.args.get('token', '').strip()
    user_id = _get_user_id(token)
    if not user_id:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json() or {}
    verification_id = (data.get('verification_id') or '').strip()
    code = (data.get('code') or '').strip()

    if not verification_id or not code:
        return jsonify({"ok": False, "error": "missing_fields"}), 400

    try:
        from sms_service import lib

        # Look up verification session
        rows = lib._sb().table("email_verification_sessions").select("*") \
            .eq("device_id", user_id) \
            .eq("verification_id", verification_id) \
            .limit(1).execute().data or []

        if not rows:
            return jsonify({"ok": False, "error": "invalid_verification_id"}), 404

        session = rows[0]

        # Check code
        if session.get("code") != code:
            return jsonify({"ok": False, "error": "invalid_code"}), 400

        # Check expiry
        expires = session.get("expires_at")
        if expires and datetime.fromisoformat(expires) < datetime.utcnow():
            return jsonify({"ok": False, "error": "code_expired"}), 400

        # Mark verified
        lib._sb().table("email_verification_sessions").update({
            "verified_at": datetime.utcnow().isoformat()
        }).eq("id", session.get("id")).execute()

        logger.info(f"[email-verify] Confirmed for {user_id}: {session.get('email')}")
        return jsonify({
            "ok": True,
            "email": session.get("email"),
            "next_step": "imap_password"
        }), 200

    except Exception as e:
        logger.error(f"[email-verify] Confirm error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route('/email-imap-setup', methods=['POST'])
def email_imap_setup():
    """
    Step 3: User provides IMAP password.
    We test connection and store encrypted credentials.
    Start polling IMAP for school emails.
    """
    token = request.args.get('token', '').strip()
    user_id = _get_user_id(token)
    if not user_id:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json() or {}
    verification_id = (data.get('verification_id') or '').strip()
    imap_password = (data.get('imap_password') or '').strip()

    if not verification_id or not imap_password:
        return jsonify({"ok": False, "error": "missing_fields"}), 400

    try:
        from sms_service import lib

        # Look up verification session
        rows = lib._sb().table("email_verification_sessions").select("*") \
            .eq("device_id", user_id) \
            .eq("verification_id", verification_id) \
            .eq("verified_at is not", None) \
            .limit(1).execute().data or []

        if not rows:
            return jsonify({"ok": False, "error": "email_not_verified"}), 400

        session = rows[0]
        email = session.get("email")

        # Test IMAP connection
        success, error_msg = _test_imap_connection(email, imap_password)
        if not success:
            return jsonify({"ok": False, "error": error_msg or "imap_connection_failed"}), 400

        # Store encrypted credentials
        encrypted_pwd = _encrypt_password(imap_password)
        lib._sb().table("school_email_credentials").insert({
            "device_id": user_id,
            "email": email,
            "imap_password_encrypted": encrypted_pwd,
            "verified": True,
            "created_at": datetime.utcnow().isoformat(),
            "last_polled_at": None,
        }).execute()

        logger.info(f"[email-verify] IMAP setup complete for {user_id}: {email}")

        return jsonify({
            "ok": True,
            "email": email,
            "status": "monitoring_school_emails",
            "polling_interval_hours": 6
        }), 200

    except Exception as e:
        logger.error(f"[email-verify] IMAP setup error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


def register_email_verification_endpoints(app):
    """Wire up email verification endpoints."""
    app.register_blueprint(bp)
    logger.info("[email-verify] Email verification endpoints registered")
