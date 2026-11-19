"""Code-based sign-in for Miru.

Flow:
  1. POST /api/auth/request-code  {phone}        -> WhatsApp code (3 per phone per hour)
  2. POST /api/auth/verify        {phone, code}  -> 30-day session cookie
  3. GET  /api/auth/me                           -> phone (masked) if signed in
  4. POST /api/auth/logout                       -> clears the session

Codes and session tokens are stored only as SHA-256 hashes.
Tables (create in Supabase, see auth_tables.sql): auth_codes, auth_sessions.
"""
import hashlib
import hmac
import os
import re
import secrets
from datetime import datetime, timedelta, timezone

from flask import jsonify, make_response, request

CODE_TTL_MIN = 10
CODE_MAX_PER_HOUR = 3
CODE_MAX_ATTEMPTS = 5
SESSION_DAYS = 30
COOKIE_NAME = "miru_session"
WA_FROM = "whatsapp:+447488883378"  # Miru's WhatsApp number (same as the morning brief)


def _now():
    return datetime.now(timezone.utc)


def _pepper():
    # Secret used to hash codes and tokens. Set AUTH_PEPPER on Railway.
    return os.environ.get("AUTH_PEPPER", "") or os.environ.get("CRON_SECRET", "") or "miru-auth"


def _hash(value: str) -> str:
    return hmac.new(_pepper().encode(), value.encode(), hashlib.sha256).hexdigest()


def normalise_phone(raw: str) -> str:
    """Return +44... for UK numbers, or "" if it doesn't look like a phone number."""
    s = re.sub(r"[^\d+]", "", (raw or "").replace("whatsapp:", ""))
    if s.startswith("00"):
        s = "+" + s[2:]
    if s.startswith("0") and len(s) >= 10:
        s = "+44" + s[1:]
    if not s.startswith("+"):
        s = "+" + s
    return s if re.fullmatch(r"\+\d{10,15}", s) else ""


def _mask(phone: str) -> str:
    return phone[:4] + " ••• ••" + phone[-2:] if phone else ""


def _sb():
    import library  # the existing Supabase helper module (imported as `lib` in sms_service)
    return library._sb()


def _send_code_whatsapp(phone: str, code: str) -> bool:
    from twilio.rest import Client
    sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    if not sid or not token:
        return False
    sender = os.environ.get("TWILIO_WHATSAPP_FROM", "").strip() or WA_FROM
    if not sender.startswith("whatsapp:"):
        sender = "whatsapp:" + sender.lstrip("+").join(["+", ""]) if False else "whatsapp:" + (sender if sender.startswith("+") else "+" + sender)
    Client(sid, token).messages.create(
        from_=sender,
        to=f"whatsapp:{phone}",
        body=f"Your Miru sign-in code is {code}. It expires in {CODE_TTL_MIN} minutes. If you didn't ask for it, ignore this message.",
    )
    return True


def session_phone():
    """Phone number of the signed-in user, or None. Other routes can call this."""
    raw = request.cookies.get(COOKIE_NAME, "")
    if not raw:
        return None
    try:
        rows = (_sb().table("auth_sessions").select("phone,expires_at")
                .eq("token_hash", _hash(raw)).limit(1).execute().data or [])
    except Exception:
        return None
    if not rows:
        return None
    exp = datetime.fromisoformat(rows[0]["expires_at"].replace("Z", "+00:00"))
    return rows[0]["phone"] if exp > _now() else None


FACTORY_ORIGIN = "https://humanagency.co"
CROSS_SITE_PATHS = ("/api/auth/", "/api/factory/me/")


def _cors_for_factory(resp):
    """Let the Factory page (humanagency.co) use the sign-in cookie. Only that origin is allowed."""
    if request.path.startswith(CROSS_SITE_PATHS) and request.headers.get("Origin", "") == FACTORY_ORIGIN:
        resp.headers["Access-Control-Allow-Origin"] = FACTORY_ORIGIN
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Vary"] = "Origin"
    return resp


def register_auth(app):
    app.after_request(_cors_for_factory)

    @app.route("/api/factory/me/messages", methods=["GET"])
    def factory_me_messages():
        phone = session_phone()
        if not phone:
            return jsonify({"error": "Sign in first"}), 401
        try:
            import school_service  # already loaded by the app
            events = school_service._get_events(f"whatsapp:{phone}", days_ahead=30, days_back=14)
        except Exception as e:
            app.logger.warning(f"[me/messages] {e}")
            return jsonify({"error": "Could not load messages"}), 500
        items = sorted(events, key=lambda e: e.get("event_date") or "9999-12-31")[:5]
        return jsonify({"messages": [{
            "title": e.get("event_title"), "date": e.get("event_date"),
            "child": e.get("child_name"), "school": e.get("school_name"),
        } for e in items]})

    @app.route("/api/auth/request-code", methods=["POST"])
    def auth_request_code():
        body = request.get_json(force=True, silent=True) or {}
        phone = normalise_phone(body.get("phone", ""))
        if not phone:
            return jsonify({"error": "Enter a UK mobile number, for example 07700 900123."}), 400
        try:
            since = (_now() - timedelta(hours=1)).isoformat()
            recent = (_sb().table("auth_codes").select("id").eq("phone", phone)
                      .gte("created_at", since).execute().data or [])
            if len(recent) >= CODE_MAX_PER_HOUR:
                return jsonify({"error": "Too many codes requested. Try again in an hour."}), 429
            code = f"{secrets.randbelow(1000000):06d}"
            _sb().table("auth_codes").insert({
                "phone": phone,
                "code_hash": _hash(phone + ":" + code),
                "attempts": 0,
                "used": False,
                "expires_at": (_now() + timedelta(minutes=CODE_TTL_MIN)).isoformat(),
            }).execute()
            if not _send_code_whatsapp(phone, code):
                return jsonify({"error": "Sign-in is not set up yet. Try again later."}), 503
        except Exception as e:
            app.logger.warning(f"[auth] request-code failed: {e}")
            return jsonify({"error": "Could not send a code. Try again."}), 500
        return jsonify({"ok": True, "sent_to": _mask(phone)})

    @app.route("/api/auth/verify", methods=["POST"])
    def auth_verify():
        body = request.get_json(force=True, silent=True) or {}
        phone = normalise_phone(body.get("phone", ""))
        code = re.sub(r"\D", "", str(body.get("code", "")))
        if not phone or len(code) != 6:
            return jsonify({"error": "Enter the six-digit code."}), 400
        try:
            rows = (_sb().table("auth_codes").select("id,code_hash,attempts,expires_at,used")
                    .eq("phone", phone).eq("used", False).order("created_at", desc=True)
                    .limit(1).execute().data or [])
            if not rows:
                return jsonify({"error": "That code has expired. Request a new one."}), 400
            row = rows[0]
            exp = datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00"))
            if exp <= _now():
                return jsonify({"error": "That code has expired. Request a new one."}), 400
            if row["attempts"] >= CODE_MAX_ATTEMPTS:
                return jsonify({"error": "Too many wrong attempts. Request a new code."}), 429
            if not hmac.compare_digest(row["code_hash"], _hash(phone + ":" + code)):
                _sb().table("auth_codes").update({"attempts": row["attempts"] + 1}).eq("id", row["id"]).execute()
                return jsonify({"error": "That code is not right."}), 400
            _sb().table("auth_codes").update({"used": True}).eq("id", row["id"]).execute()
            token = secrets.token_urlsafe(32)
            _sb().table("auth_sessions").insert({
                "token_hash": _hash(token),
                "phone": phone,
                "expires_at": (_now() + timedelta(days=SESSION_DAYS)).isoformat(),
            }).execute()
        except Exception as e:
            app.logger.warning(f"[auth] verify failed: {e}")
            return jsonify({"error": "Could not sign you in. Try again."}), 500
        resp = make_response(jsonify({"ok": True, "phone": _mask(phone)}))
        resp.set_cookie(COOKIE_NAME, token, max_age=SESSION_DAYS * 86400,
                        httponly=True, secure=True, samesite="Lax", path="/")
        return resp

    @app.route("/api/auth/me", methods=["GET"])
    def auth_me():
        phone = session_phone()
        return jsonify({"signed_in": bool(phone), "phone": _mask(phone) if phone else None})

    @app.route("/api/auth/logout", methods=["POST"])
    def auth_logout():
        raw = request.cookies.get(COOKIE_NAME, "")
        if raw:
            try:
                _sb().table("auth_sessions").delete().eq("token_hash", _hash(raw)).execute()
            except Exception:
                pass
        resp = make_response(jsonify({"ok": True}))
        resp.delete_cookie(COOKIE_NAME, path="/")
        return resp
