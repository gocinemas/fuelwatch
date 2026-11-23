"""
Web endpoints for the receipts summary card and category budgets.

GET  /api/v2/receipts/summary?token=<v2 token>&period=week|month[&month=YYYY-MM]
GET  /api/v2/receipts/budgets?token=<v2 token>
POST /api/v2/receipts/budgets  {"token": "...", "bucket": "Groceries", "amount": 250}
     amount 0 clears that budget.

Uses the same token resolver as the other /api/v2 routes (sms_service._v2_resolve).
"""
import sys

from flask import jsonify, request

import library as lib
from miru.receipts_summary import build_summary, get_budgets, set_budget


def _resolve_token(token: str):
    # sms_service defines _v2_resolve; look it up at request time to avoid a circular import
    mod = sys.modules.get("sms_service") or sys.modules.get("__main__")
    resolver = getattr(mod, "_v2_resolve", None)
    if resolver is None:
        return None
    return resolver(token)


def register_receipts_summary_endpoints(app):
    @app.route("/api/v2/receipts/summary", methods=["GET"])
    def api_v2_receipts_summary():
        try:
            token = request.args.get("token", "").strip()
            from_number = _resolve_token(token)
            if not from_number:
                return jsonify({"error": "Unauthorized"}), 401

            period = request.args.get("period", "week")
            if period not in ("week", "month"):
                return jsonify({"error": "period must be week or month"}), 400

            result = build_summary(lib, from_number, period, month=request.args.get("month"))
            # text is for WhatsApp; the app uses the structured fields
            result.pop("text", None)
            return jsonify({"status": "ok", **result})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/v2/receipts/budgets", methods=["GET", "POST"])
    def api_v2_receipts_budgets():
        try:
            data = request.get_json(silent=True) or {}
            token = (request.args.get("token") or data.get("token") or "").strip()
            from_number = _resolve_token(token)
            if not from_number:
                return jsonify({"error": "Unauthorized"}), 401

            if request.method == "GET":
                return jsonify({"status": "ok", "budgets": get_budgets(lib, from_number)})

            bucket = data.get("bucket", "")
            try:
                amount = float(data.get("amount", 0))
            except (TypeError, ValueError):
                return jsonify({"error": "amount must be a number"}), 400
            try:
                budgets = set_budget(lib, from_number, bucket, amount)
            except ValueError as e:
                return jsonify({"error": str(e)}), 400
            return jsonify({"status": "ok", "budgets": budgets})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
