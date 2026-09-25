"""
Module Gating System — Let users control which features they want

Unified module schema:
- trains: Live train times
- fuel: Fuel price tracking
- schools: School events & comms
- expense: Receipt scanning & spend tracking
- local: Pubs, cafes, restaurants nearby
- research: Area research (council, MPs, crime, attractions)
- library: Save articles & books
- gigs: Concerts & events
- weather: Hourly forecast
"""

from flask import request, jsonify
import logging
import json

logger = logging.getLogger(__name__)

# Default: all modules enabled (users can opt-out)
DEFAULT_MODULES = {
    "trains": True,
    "fuel": True,
    "schools": True,
    "expense": True,
    "local": True,
    "research": True,
    "library": True,
    "gigs": True,
    "weather": True
}


def _resolve_user_phone(token):
    """Get phone number from token. Returns phone or None."""
    if not token:
        return None
    try:
        from sms_service import _v2_resolve
        return _v2_resolve(token)
    except:
        # For local dev, accept any token
        return token if token else None


def _get_user_modules(phone_number):
    """Load user's module preferences from database."""
    if not phone_number:
        return DEFAULT_MODULES.copy()

    try:
        import library as lib

        result = lib._sb().table("ma_details").select("data").eq("type", "modules_enabled").eq("device_id", f"whatsapp:{phone_number}").execute()

        if result.data and len(result.data) > 0:
            stored_modules = result.data[0].get("data", {})
            # Merge with defaults (in case new modules were added)
            merged = DEFAULT_MODULES.copy()
            merged.update(stored_modules)
            return merged
        else:
            return DEFAULT_MODULES.copy()
    except Exception as e:
        logger.error(f"[modules] Error loading user modules: {e}")
        return DEFAULT_MODULES.copy()


def _set_user_modules(phone_number, modules_dict):
    """Save user's module preferences to database."""
    if not phone_number:
        return False

    try:
        import library as lib

        # Validate: only accept known modules
        valid_modules = {k: v for k, v in modules_dict.items() if k in DEFAULT_MODULES}

        lib._sb().table("ma_details").upsert({
            "device_id": f"whatsapp:{phone_number}",
            "type": "modules_enabled",
            "data": valid_modules,
            "label": "user_module_preferences"
        }).execute()

        return True
    except Exception as e:
        logger.error(f"[modules] Error saving user modules: {e}")
        return False


def api_user_modules_get():
    """GET /api/user/modules?token=TOKEN — Get user's enabled modules."""
    token = request.args.get("token", "").strip()
    phone = _resolve_user_phone(token)

    if not phone:
        return jsonify({"ok": False, "error": "token required"}), 401

    modules = _get_user_modules(phone)

    return jsonify({
        "ok": True,
        "modules": modules,
        "count_enabled": sum(1 for v in modules.values() if v),
        "count_total": len(modules)
    }), 200


def api_user_modules_post():
    """POST /api/user/modules?token=TOKEN — Set user's enabled modules."""
    token = request.args.get("token", "").strip()
    phone = _resolve_user_phone(token)

    if not phone:
        return jsonify({"ok": False, "error": "token required"}), 401

    try:
        data = request.get_json() or {}

        # Accept either raw module dict or wrapped in "modules" key
        modules_to_save = data.get("modules", data)

        if not modules_to_save:
            return jsonify({"ok": False, "error": "no modules provided"}), 400

        # Validate structure
        if not isinstance(modules_to_save, dict):
            return jsonify({"ok": False, "error": "modules must be object"}), 400

        # Save
        success = _set_user_modules(phone, modules_to_save)

        if success:
            # Return updated modules
            updated = _get_user_modules(phone)
            return jsonify({
                "ok": True,
                "modules": updated,
                "count_enabled": sum(1 for v in updated.values() if v),
                "count_total": len(updated)
            }), 200
        else:
            return jsonify({"ok": False, "error": "failed to save"}), 500

    except Exception as e:
        logger.error(f"[modules] Error in POST: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


def is_module_enabled(phone_number, module_name):
    """Check if a specific module is enabled for a user. Used by Phase 2 endpoints."""
    if not module_name or module_name not in DEFAULT_MODULES:
        return True  # Unknown module = allow access

    modules = _get_user_modules(phone_number)
    return modules.get(module_name, True)


def register_module_gating_endpoints(app):
    """Wire up module gating endpoints."""
    app.add_url_rule("/api/user/modules", "api_user_modules_get", api_user_modules_get, methods=["GET"])
    app.add_url_rule("/api/user/modules", "api_user_modules_post", api_user_modules_post, methods=["POST"])
    logger.info("[modules] Module gating endpoints registered")
