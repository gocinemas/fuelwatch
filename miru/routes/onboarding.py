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
                .eq("user_phone", from_number).limit(1).execute().data or []
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
            "user_phone": from_number,
            "child_name": child_name,
            "school_name": school_name,
            "email": school_email if school_email else None,
            "status": "active",
        }).execute()

        logger.info(f"[onboarding] School added for {from_number}: {child_name} @ {school_name}")
        return jsonify({"ok": True, "school": result.data[0] if result.data else {}})
    except Exception as e:
        logger.error(f"[onboarding] School add error: {e}")
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

        schools = lib._sb().table("school_profiles").select("id,child_name,school_name,email") \
            .eq("user_phone", from_number).execute().data or []

        return jsonify({"schools": schools})
    except Exception as e:
        logger.error(f"[onboarding] Schools get error: {e}")
        return jsonify({"error": str(e)}), 500
