"""Phase 2: Miru Public Data APIs (v1)"""

from flask import request, jsonify
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)


def _resolve_postcode_v1(postcode_str):
    """Normalize postcode. Returns (postcode, lat, lon, formatted) or None."""
    try:
        from search import postcode_to_latlon
        postcode = postcode_str.strip().upper().replace(" ", "")
        if not postcode or len(postcode) < 5:
            return None
        latlon = postcode_to_latlon(postcode)
        if not latlon:
            return None
        lat, lon = latlon
        pc_fmt = f"{postcode[:-3]} {postcode[-3:]}" if len(postcode) >= 5 else postcode
        return postcode, lat, lon, pc_fmt
    except Exception as e:
        logger.error(f"[api_v1] Postcode resolve error: {e}")
        return None


def _auth_token_or_403(token_str):
    """Accept any non-empty token (local dev mode)."""
    return True if token_str else False


def _build_response(ok, postcode, data=None, error=None, cached=False):
    """Standard v1 response envelope."""
    resp = {
        "ok": ok,
        "postcode": postcode,
        "timestamp": datetime.utcnow().isoformat(),
    }
    if data:
        resp["data"] = data
    if error:
        resp["error"] = error
    if cached:
        resp["cached"] = True
    return resp


# Endpoint 1: Calendar (bank holidays, school holidays)
def api_v1_calendar(postcode_str):
    """GET /api/v1/calendar/{postcode}?token=TOKEN"""
    token = request.args.get("token", "").strip()
    if not _auth_token_or_403(token):
        return jsonify(_build_response(False, postcode_str, error="token required")), 401

    resolved = _resolve_postcode_v1(postcode_str)
    if not resolved:
        return jsonify(_build_response(False, postcode_str, error="postcode not found")), 404

    postcode, lat, lon, pc_fmt = resolved

    # Bank holidays 2026
    bank_holidays = [
        {"date": "2026-01-01", "name": "New Year's Day"},
        {"date": "2026-04-10", "name": "Good Friday"},
        {"date": "2026-04-13", "name": "Easter Monday"},
        {"date": "2026-05-04", "name": "Early May Bank Holiday"},
        {"date": "2026-05-25", "name": "Spring Bank Holiday"},
        {"date": "2026-08-31", "name": "Summer Bank Holiday"},
        {"date": "2026-12-25", "name": "Christmas Day"},
        {"date": "2026-12-28", "name": "Boxing Day (observed)"}
    ]

    # School holidays (England)
    school_holidays = [
        {"start": "2026-02-16", "end": "2026-02-20", "name": "Half-term"},
        {"start": "2026-04-03", "end": "2026-04-20", "name": "Easter holidays"},
        {"start": "2026-07-22", "end": "2026-09-01", "name": "Summer holidays"},
        {"start": "2026-10-26", "end": "2026-10-30", "name": "Half-term"},
        {"start": "2026-12-21", "end": "2027-01-08", "name": "Christmas holidays"}
    ]

    result = {
        "bank_holidays": bank_holidays,
        "school_holidays": school_holidays
    }

    return jsonify(_build_response(True, postcode, data=result))


# Endpoint 2: Bills & Utilities (council tax, water)
def api_v1_bills(postcode_str):
    """GET /api/v1/bills/{postcode}?token=TOKEN"""
    token = request.args.get("token", "").strip()
    if not _auth_token_or_403(token):
        return jsonify(_build_response(False, postcode_str, error="token required")), 401

    resolved = _resolve_postcode_v1(postcode_str)
    if not resolved:
        return jsonify(_build_response(False, postcode_str, error="postcode not found")), 404

    postcode, lat, lon, pc_fmt = resolved

    result = {
        "council_tax": {
            "status": "not_available",
            "note": "Council tax band lookup requires council authority data — not yet implemented"
        },
        "water": {
            "status": "not_available",
            "note": "Water authority data not available in current API version"
        },
        "utilities": {
            "status": "available_from_user_history",
            "note": "Scan receipts to track your own bills via /api/expense/scan"
        }
    }

    return jsonify(_build_response(True, postcode, data=result))


# Endpoint 3: Transport (trains, tubes, buses)
def api_v1_transport(postcode_str):
    """GET /api/v1/transport/{postcode}?token=TOKEN"""
    token = request.args.get("token", "").strip()
    if not _auth_token_or_403(token):
        return jsonify(_build_response(False, postcode_str, error="token required")), 401

    resolved = _resolve_postcode_v1(postcode_str)
    if not resolved:
        return jsonify(_build_response(False, postcode_str, error="postcode not found")), 404

    postcode, lat, lon, pc_fmt = resolved

    try:
        from search import haversine_km
        from uk_stations import UK_STATIONS

        trains = []

        # Find nearest train stations within 2km
        for crs, station_data in list(UK_STATIONS.items())[:100]:
            try:
                dist = haversine_km(lat, lon, station_data.get("lat", 0), station_data.get("lon", 0))
                if dist <= 2:
                    trains.append({
                        "name": station_data.get("name", crs),
                        "crs": crs,
                        "distance_km": round(dist, 1),
                        "lat": station_data.get("lat"),
                        "lon": station_data.get("lon")
                    })
            except:
                pass

        result = {
            "trains": sorted(trains, key=lambda x: x["distance_km"])[:5],
            "tubes": [],
            "buses": [],
            "note": "Live departure times available via /api/trains/{station} endpoint"
        }

        return jsonify(_build_response(True, postcode, data=result))

    except Exception as e:
        logger.error(f"[api_v1] Transport error: {e}")
        result = {"trains": [], "tubes": [], "buses": [], "error": str(e)}
        return jsonify(_build_response(True, postcode, data=result))


# Endpoint 4: Parking & EV Charging
def api_v1_parking(postcode_str):
    """GET /api/v1/parking/{postcode}?token=TOKEN"""
    token = request.args.get("token", "").strip()
    if not _auth_token_or_403(token):
        return jsonify(_build_response(False, postcode_str, error="token required")), 401

    resolved = _resolve_postcode_v1(postcode_str)
    if not resolved:
        return jsonify(_build_response(False, postcode_str, error="postcode not found")), 404

    postcode, lat, lon, pc_fmt = resolved

    result = {
        "car_parks": [],
        "ev_chargers": [],
        "note": "Car park and EV charging data not available in local dev mode"
    }

    return jsonify(_build_response(True, postcode, data=result))


# Endpoint 5: Health Services (GPs, dentists, A&E)
def api_v1_health_services(postcode_str):
    """GET /api/v1/health-services/{postcode}?token=TOKEN"""
    token = request.args.get("token", "").strip()
    if not _auth_token_or_403(token):
        return jsonify(_build_response(False, postcode_str, error="token required")), 401

    resolved = _resolve_postcode_v1(postcode_str)
    if not resolved:
        return jsonify(_build_response(False, postcode_str, error="postcode not found")), 404

    postcode, lat, lon, pc_fmt = resolved

    result = {
        "gps": [],
        "dentists": [],
        "ae": [],
        "note": "Health services lookup not available in local dev mode"
    }

    return jsonify(_build_response(True, postcode, data=result))


# Register all endpoints with Flask app
def register_api_v1_endpoints(app):
    """Called from sms_service.py to wire up all v1 routes."""
    app.add_url_rule("/api/v1/calendar/<postcode_str>", "api_v1_calendar", api_v1_calendar, methods=["GET"])
    app.add_url_rule("/api/v1/bills/<postcode_str>", "api_v1_bills", api_v1_bills, methods=["GET"])
    app.add_url_rule("/api/v1/transport/<postcode_str>", "api_v1_transport", api_v1_transport, methods=["GET"])
    app.add_url_rule("/api/v1/parking/<postcode_str>", "api_v1_parking", api_v1_parking, methods=["GET"])
    app.add_url_rule("/api/v1/health-services/<postcode_str>", "api_v1_health_services", api_v1_health_services, methods=["GET"])
    logger.info("[api_v1] All 5 Phase 2 endpoints registered")
