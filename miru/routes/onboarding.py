"""User onboarding endpoints - set up preferences, schools, features."""

from flask import Blueprint, request, jsonify
import logging

logger = logging.getLogger(__name__)

bp = Blueprint('onboarding', __name__, url_prefix='/api/user')


def _get_user_id(token):
    """Resolve token to user ID. Import here to avoid circular deps."""
    from sms_service import _v2_resolve
    return _v2_resolve(token)


@bp.route('/setup-status', methods=['GET'])
def get_setup_status():
    """Check onboarding completion status."""
    token = request.args.get('token', '').strip()

    try:
        from_number = _get_user_id(token)
        if not from_number:
            return jsonify({"error": "unauthorized"}), 401

        from sms_service import lib

        # Check if prefs exist
        try:
            prefs_rows = lib._sb().table("ma_details").select("id") \
                .eq("device_id", from_number).eq("type", "v2_prefs").limit(1).execute().data or []
        except Exception as e:
            logger.warning(f"[onboarding] Prefs query error: {e}")
            prefs_rows = []

        # Check if schools exist
        try:
            school_rows = lib._sb().table("school_profiles").select("id") \
                .eq("from_number", from_number).limit(1).execute().data or []
        except Exception as e:
            logger.warning(f"[onboarding] Schools query error: {e}")
            school_rows = []

        completed = len(prefs_rows) > 0 or len(school_rows) > 0
        missing = []
        if not prefs_rows:
            missing.append('prefs')

        return jsonify({
            "completed": completed,
            "missing": missing,
            "has_prefs": len(prefs_rows) > 0,
            "has_schools": len(school_rows) > 0,
        })
    except Exception as e:
        logger.error(f"[onboarding] Status error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@bp.route('/prefs', methods=['GET'])
def get_prefs():
    """Get user preferences."""
    token = request.args.get('token', '').strip()
    from_number = _get_user_id(token)

    if not from_number:
        return jsonify({"error": "unauthorized"}), 401

    try:
        from sms_service import lib

        rows = lib._sb().table("ma_details").select("data") \
            .eq("device_id", from_number).eq("type", "v2_prefs").limit(1).execute().data or []

        prefs = rows[0]["data"] if rows else {}

        return jsonify({
            "train_from": prefs.get("train_from", ""),
            "train_to": prefs.get("train_to", ""),
            "fuel_postcode": prefs.get("fuel_postcode", ""),
            "morning_push": prefs.get("morning_push", False),
            "school_alerts": prefs.get("school_alerts", False),
        })
    except Exception as e:
        logger.error(f"[onboarding] Prefs get error: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route('/prefs', methods=['POST'])
def save_prefs():
    """Save user preferences."""
    token = request.args.get('token', '').strip()
    from_number = _get_user_id(token)

    if not from_number:
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}

    try:
        from sms_service import lib

        prefs = {
            "train_from": (body.get("train_from") or "").strip().upper(),
            "train_to": (body.get("train_to") or "").strip().upper(),
            "fuel_postcode": (body.get("fuel_postcode") or "").strip().upper(),
            "morning_push": body.get("morning_push", False),
            "school_alerts": body.get("school_alerts", False),
        }

        # Remove empty fields
        prefs = {k: v for k, v in prefs.items() if v or isinstance(v, bool)}

        # Upsert into ma_details
        lib._sb().table("ma_details").upsert({
            "device_id": from_number,
            "type": "v2_prefs",
            "label": "user_preferences",
            "data": prefs,
        }).execute()

        logger.info(f"[onboarding] Prefs saved for {from_number}")
        return jsonify({"ok": True, "prefs": prefs})
    except Exception as e:
        logger.error(f"[onboarding] Prefs save error: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route('/schools', methods=['POST'])
def add_school():
    """Add a child's school."""
    token = request.args.get('token', '').strip()
    from_number = _get_user_id(token)

    if not from_number:
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}

    try:
        from sms_service import lib

        child_name = (body.get("child_name") or "").strip()
        school_name = (body.get("school_name") or "").strip()
        school_email = (body.get("school_email") or "").strip().lower()

        if not child_name or not school_name:
            return jsonify({"error": "child_name and school_name required"}), 400

        # Insert school profile
        result = lib._sb().table("school_profiles").insert({
            "from_number": from_number,
            "child_name": child_name,
            "school_name": school_name,
            "sender_emails": [school_email] if school_email else [],
        }).execute()

        logger.info(f"[onboarding] School added for {from_number}: {child_name} @ {school_name}")
        return jsonify({"ok": True, "school": result.data[0] if result.data else {}})
    except Exception as e:
        logger.error(f"[onboarding] School add error: {e}")
        return jsonify({"error": str(e)}), 500


DEFAULT_MODULES = {
    "myarea": True,
    "commute": True,
    "school": False,
    "spend": True,
    "saves": True,
    "library": True,
}


@bp.route('/modules', methods=['GET'])
def get_modules():
    """Get user's enabled/disabled modules (Smart Home Screen)."""
    token = request.args.get('token', '').strip()
    from_number = _get_user_id(token)

    if not from_number:
        return jsonify({"error": "unauthorized"}), 401

    try:
        from sms_service import lib

        rows = lib._sb().table("ma_details").select("data") \
            .eq("device_id", from_number).eq("type", "modules_enabled").limit(1).execute().data or []

        saved = rows[0].get("data") or {} if rows else {}
        modules = {**DEFAULT_MODULES, **saved}

        return jsonify({"modules_enabled": modules})
    except Exception as e:
        logger.error(f"[onboarding] Modules get error: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route('/modules', methods=['POST'])
def save_modules():
    """Save user's enabled/disabled modules (Settings screen)."""
    token = request.args.get('token', '').strip()
    from_number = _get_user_id(token)

    if not from_number:
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    new_modules = body.get("modules_enabled", body)

    if not isinstance(new_modules, dict):
        return jsonify({"error": "modules_enabled must be an object"}), 400

    # Only accept known module keys, coerced to bool
    updates = {k: bool(v) for k, v in new_modules.items() if k in DEFAULT_MODULES}

    try:
        from sms_service import lib

        sb = lib._sb()
        rows = sb.table("ma_details").select("id,data") \
            .eq("device_id", from_number).eq("type", "modules_enabled").limit(1).execute().data or []

        if rows:
            merged = {**DEFAULT_MODULES, **(rows[0].get("data") or {}), **updates}
            sb.table("ma_details").update({"data": merged}).eq("id", rows[0]["id"]).execute()
        else:
            merged = {**DEFAULT_MODULES, **updates}
            sb.table("ma_details").insert({
                "device_id": from_number,
                "type": "modules_enabled",
                "label": "user_modules",
                "data": merged,
            }).execute()

        logger.info(f"[onboarding] Modules saved for {from_number}: {merged}")
        return jsonify({"ok": True, "modules_enabled": merged})
    except Exception as e:
        logger.error(f"[onboarding] Modules save error: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route('/schools', methods=['GET'])
def get_schools():
    """Get user's schools."""
    token = request.args.get('token', '').strip()
    from_number = _get_user_id(token)

    if not from_number:
        return jsonify({"error": "unauthorized"}), 401

    try:
        from sms_service import lib

        schools = lib._sb().table("school_profiles").select("id,child_name,school_name") \
            .eq("from_number", from_number).execute().data or []

        return jsonify({"schools": schools})
    except Exception as e:
        logger.error(f"[onboarding] Schools get error: {e}")
        return jsonify({"error": str(e)}), 500
