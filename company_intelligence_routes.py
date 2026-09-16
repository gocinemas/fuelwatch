"""
Company Intelligence Routes — company_details data layer for the unified
company dashboard.

  https://miru.humanagency.co/company/Ikea

This module owns the `company_details` Supabase table: get-or-create lookup,
staleness checks, and firing off company_data_populator.populate_company_data()
on a background daemon thread (fire-and-forget, mirrors
brand_intelligence_service.py's _background_enrich_brand() — never blocks the
calling request).

The unified HTML dashboard (`/company/<name>`, also reachable at the legacy
`/intelligence/<name>` URL) is rendered by sms_service.py's
`company_intelligence_tabbed()` view, which imports `ensure_company_row()`
from here to fetch/create the company_details row (basics + enrichment
status) and merges it with the existing 5signals/sentiment/comparison
intelligence into a single `intelligence_tabbed.html` render. That keeps this
module focused on the data layer while the tabbed template stays the one
place company pages are rendered.

Registered into the main app the same way as the other feature modules in
sms_service.py:

    from company_intelligence_routes import register_company_intelligence_endpoints
    register_company_intelligence_endpoints(app)

Route map (this module):
  GET  /api/company/<company_name>         — JSON profile (triggers fetch if missing)
  GET  /api/company/<company_name>/status  — lightweight poll target for the pending badge
  GET  /api/companies/search?q=            — search across enriched companies

The shareable HTML page itself — GET /company/<company_name>, also served at
GET /intelligence/<company_name> — lives in sms_service.py so it can reuse
the existing tabbed-dashboard machinery (get_5_signals, TabbedIntelligenceService,
etc.) without duplicating it here.
"""

import os
import re
import threading
from datetime import datetime, timezone

from flask import request, jsonify
from supabase import create_client

from company_data_populator import populate_company_data, STALE_AFTER_DAYS

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

_sb_client = None


def _sb():
    global _sb_client
    if _sb_client is None:
        _sb_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb_client


def slugify(name: str) -> str:
    """Turn 'Ikea' / 'IKEA UK' into a stable, URL + lookup-safe slug. Must match
    company_data_populator.slugify() exactly, since both read/write by slug."""
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unknown"


def _get_company(slug: str):
    res = _sb().table("company_details").select("*").eq("slug", slug).limit(1).execute()
    rows = res.data or []
    return rows[0] if rows else None


def _is_stale(row) -> bool:
    if not row or row.get("status") != "ready":
        return True
    last = row.get("last_enriched_at")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
    except Exception:
        return True
    return (datetime.now(timezone.utc) - last_dt).days > STALE_AFTER_DAYS


def _trigger_background_fetch(company_name: str, slug: str, requested_by: str = None):
    """Fire-and-forget enrichment — mirrors brand_intelligence_service.py's
    _background_enrich_brand(). Never blocks the calling request."""
    thread = threading.Thread(
        target=populate_company_data,
        args=(company_name, slug),
        kwargs={"requested_by": requested_by},
        daemon=True,
    )
    thread.start()


def _ensure_row(company_name: str, requested_by: str = None):
    """
    Idempotent "get or create" for a company page view / API hit / WhatsApp
    command. Returns (row, just_created: bool). Always triggers a background
    fetch when the row is missing or stale.
    """
    slug = slugify(company_name)
    row = _get_company(slug)

    if row is None:
        display_name = company_name.strip().title()
        print(f"[company_routes] Creating new row for {display_name}")
        try:
            _sb().table("company_details").insert(
                {
                    "company_name": display_name,
                    "slug": slug,
                    "status": "pending",
                    "requested_by": requested_by,
                }
            ).execute()
            print(f"[company_routes] ✓ Inserted {slug}")
        except Exception as e:
            print(f"[company_routes] ✗ Insert failed for {slug}: {e}")
        row = _get_company(slug) or {
            "company_name": display_name,
            "slug": slug,
            "status": "pending",
        }
        print(f"[company_routes] Triggering background fetch for {display_name}")
        _trigger_background_fetch(display_name, slug, requested_by=requested_by)
        return row, True

    if _is_stale(row):
        _trigger_background_fetch(row.get("company_name", company_name), slug)

    return row, False


# Public entry point for sms_service.py's unified /company/<name> ↔
# /intelligence/<name> view — get-or-create the company_details row and
# kick off background enrichment when missing/stale. Never blocks.
def ensure_company_row(company_name: str, requested_by: str = None):
    display_name = (company_name or "").replace("-", " ").replace("_", " ").strip()
    row, just_created = _ensure_row(display_name, requested_by=requested_by)

    if not just_created and row.get("status") == "ready":
        try:
            _sb().table("company_details").update(
                {"view_count": (row.get("view_count") or 0) + 1}
            ).eq("slug", row["slug"]).execute()
        except Exception:
            pass  # view counting is best-effort, never block the page on it

    return row, just_created


def register_company_intelligence_endpoints(app):

    @app.route("/api/company/<company_name>")
    def api_company_detail(company_name):
        row, just_created = _ensure_row(
            company_name.replace("-", " ").replace("_", " ").strip(),
            requested_by=request.remote_addr,
        )
        status_code = 202 if row.get("status") in ("pending", "enriching") else 200
        return jsonify(row), status_code

    @app.route("/api/company/<company_name>/status")
    def api_company_status(company_name):
        """Cheap polling target for the pending page — avoid re-selecting * every 3s."""
        slug = slugify(company_name.replace("-", " ").replace("_", " ").strip())
        res = (
            _sb()
            .table("company_details")
            .select("status,updated_at")
            .eq("slug", slug)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            return jsonify({"status": "not_found"}), 404
        return jsonify(rows[0])

    @app.route("/api/companies/search")
    def api_company_search():
        q = (request.args.get("q") or "").strip()
        if not q:
            return jsonify({"results": []})
        res = (
            _sb()
            .table("company_details")
            .select("company_name,slug,industry,description,logo_url,status,view_count")
            .ilike("company_name", f"%{q}%")
            .eq("status", "ready")
            .order("view_count", desc=True)
            .limit(20)
            .execute()
        )
        return jsonify({"results": res.data or []})


def handle_company_lookup_command(company_query: str, from_number: str) -> str:
    """
    Used by the WhatsApp `/company <name>` command in sms_service.py.
    Returns plain text suitable for resp.message(...).
    """
    row, just_created = _ensure_row(company_query.strip(), requested_by=from_number)
    link = f"https://miru.humanagency.co/company/{row['slug']}"

    if row.get("status") == "ready" and not just_created:
        desc = (row.get("description") or "").strip()
        preview = f"\n\n{desc[:180]}{'…' if len(desc) > 180 else ''}" if desc else ""
        return f"🏢 *{row.get('company_name')}*{preview}\n\n🔗 {link}"

    return (
        f"🏢 Gathering intelligence on *{row.get('company_name', company_query)}*…\n\n"
        f"Your shareable page is ready now and will fill in over the next"
        f" ~30 seconds:\n🔗 {link}"
    )

