"""
Company Intelligence Routes — shareable company profile pages.

  https://miru.humanagency.co/company/Ikea

Fast path: look up `company_details` in Supabase. If it exists and is fresh,
render immediately with OpenGraph/Twitter meta tags so the link previews
nicely when shared in WhatsApp/iMessage/Slack/etc. If it's missing or stale,
serve a lightweight "gathering intelligence" page INSTANTLY (no blocking on
enrichment) and kick off company_data_populator.populate_company_data() on a
background daemon thread — the same fire-and-forget pattern used by
brand_intelligence_service.py's _background_enrich_brand(). The pending page
polls /api/company/<name>/status every few seconds and refreshes itself once
ready.

Registered into the main app the same way as the other feature modules in
sms_service.py:

    from company_intelligence_routes import register_company_intelligence_endpoints
    register_company_intelligence_endpoints(app)

Route map:
  GET  /company/<company_name>          — shareable HTML profile page
  GET  /api/company/<company_name>      — JSON profile (triggers fetch if missing)
  GET  /api/company/<company_name>/status  — lightweight poll target for the pending page
  GET  /api/companies/search?q=         — search across enriched companies

Note: this intentionally does NOT touch the existing `/company`, `/company/`,
`/company/compare` or `/intelligence/<company_name>` routes already defined
in sms_service.py (Flask/Werkzeug always prefers a static rule like
`/company/compare` over a dynamic one like `/company/<company_name>`, so
there's no collision) — this is a separate, self-contained feature backed by
its own `company_details` table.
"""

import os
import re
import threading
from datetime import datetime, timezone

from flask import request, jsonify, render_template_string
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
        try:
            _sb().table("company_details").insert(
                {
                    "company_name": display_name,
                    "slug": slug,
                    "status": "pending",
                    "requested_by": requested_by,
                }
            ).execute()
        except Exception as e:
            # Race: two requests for the same new company at once — the unique
            # slug constraint will reject the second insert. Just re-read.
            print(f"[company_routes] insert race for slug={slug}: {e}")
        row = _get_company(slug) or {
            "company_name": display_name,
            "slug": slug,
            "status": "pending",
        }
        _trigger_background_fetch(display_name, slug, requested_by=requested_by)
        return row, True

    if _is_stale(row):
        _trigger_background_fetch(row.get("company_name", company_name), slug)

    return row, False


def register_company_intelligence_endpoints(app):

    @app.route("/company/<company_name>")
    def company_detail_page(company_name):
        display_name = company_name.replace("-", " ").replace("_", " ").strip()
        row, just_created = _ensure_row(
            display_name, requested_by=request.headers.get("X-Forwarded-For", request.remote_addr)
        )

        if not just_created and row.get("status") == "ready":
            try:
                _sb().table("company_details").update(
                    {"view_count": (row.get("view_count") or 0) + 1}
                ).eq("slug", row["slug"]).execute()
            except Exception:
                pass  # view counting is best-effort, never block the page on it

        return render_template_string(
            COMPANY_PAGE_TEMPLATE,
            company=row,
            share_url=request.url,
            is_ready=(row.get("status") == "ready"),
        )

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


# ─────────────────────────────────────────────────────────────────────────
# Shareable HTML page — OG/Twitter meta tags make links preview nicely when
# pasted into WhatsApp/iMessage/Slack. Auto-polls /status while pending.
# ─────────────────────────────────────────────────────────────────────────
COMPANY_PAGE_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ company.company_name }} — Company Intelligence | Miru</title>
<meta name="description" content="{{ company.description or ('Company profile for ' + company.company_name + ', generated by Miru.') }}">

<!-- OpenGraph / shareable link preview -->
<meta property="og:type" content="website">
<meta property="og:title" content="{{ company.company_name }} — Company Intelligence">
<meta property="og:description" content="{{ company.description or 'AI-generated company profile — description, industry, website, and key facts.' }}">
<meta property="og:url" content="{{ share_url }}">
{% if company.logo_url %}<meta property="og:image" content="{{ company.logo_url }}">{% endif %}

<!-- Twitter card -->
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="{{ company.company_name }} — Company Intelligence">
<meta name="twitter:description" content="{{ company.description or 'AI-generated company profile.' }}">
{% if company.logo_url %}<meta name="twitter:image" content="{{ company.logo_url }}">{% endif %}

<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 640px;
         margin: 0 auto; padding: 32px 20px 64px; background: #fafafa; color: #1a1a1a; }
  @media (prefers-color-scheme: dark) { body { background: #111; color: #eee; } .card { background: #1c1c1c !important; border-color: #2a2a2a !important; } }
  .card { background: #fff; border: 1px solid #e5e5e5; border-radius: 16px; padding: 28px; }
  h1 { font-size: 1.6rem; margin: 0 0 4px; }
  .industry { color: #888; font-size: 0.95rem; margin-bottom: 18px; }
  .desc { line-height: 1.55; margin-bottom: 20px; }
  .facts { list-style: none; padding: 0; margin: 0 0 20px; }
  .facts li { padding: 8px 0; border-top: 1px solid #eee; }
  .meta { display: flex; flex-wrap: wrap; gap: 8px 20px; font-size: 0.9rem; color: #666; margin-bottom: 20px; }
  .social a { margin-right: 14px; font-size: 0.9rem; }
  .pending { text-align: center; padding: 40px 0; }
  .spinner { width: 28px; height: 28px; margin: 0 auto 16px; border: 3px solid #ddd; border-top-color: #666;
             border-radius: 50%; animation: spin 0.8s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .footer { text-align: center; color: #999; font-size: 0.8rem; margin-top: 24px; }
  a { color: #0a6cff; }
</style>
</head>
<body>
  <div class="card" id="company-card">
    {% if is_ready %}
      <h1>{{ company.company_name }}</h1>
      {% if company.industry %}<div class="industry">{{ company.industry }}</div>{% endif %}
      {% if company.description %}<p class="desc">{{ company.description }}</p>{% endif %}

      <div class="meta">
        {% if company.website %}<span>🌐 <a href="{{ company.website }}" target="_blank" rel="noopener">{{ company.website }}</a></span>{% endif %}
        {% if company.headquarters %}<span>📍 {{ company.headquarters }}</span>{% endif %}
        {% if company.founded_year %}<span>📅 Founded {{ company.founded_year }}</span>{% endif %}
        {% if company.employee_count %}<span>👥 {{ company.employee_count }}</span>{% endif %}
      </div>

      {% if company.key_facts %}
      <ul class="facts">
        {% for fact in company.key_facts %}<li>• {{ fact }}</li>{% endfor %}
      </ul>
      {% endif %}

      {% if company.social_links %}
      <div class="social">
        {% for platform, url in company.social_links.items() %}
          {% if url %}<a href="{{ url }}" target="_blank" rel="noopener">{{ platform|capitalize }}</a>{% endif %}
        {% endfor %}
      </div>
      {% endif %}
    {% else %}
      <div class="pending" id="pending-block">
        <div class="spinner"></div>
        <h1>{{ company.company_name }}</h1>
        <p>Gathering company intelligence… this usually takes under a minute.</p>
      </div>
    {% endif %}
  </div>
  <div class="footer">Generated by <a href="https://miru.humanagency.co">Miru</a> — shareable company intelligence</div>

  {% if not is_ready %}
  <script>
    // Poll for completion, then reload to render the finished profile.
    const slug = {{ company.slug|tojson }};
    const poll = setInterval(async () => {
      try {
        const r = await fetch(`/api/company/${slug}/status`);
        const data = await r.json();
        if (data.status === 'ready' || data.status === 'failed') {
          clearInterval(poll);
          location.reload();
        }
      } catch (e) { /* keep polling */ }
    }, 3000);
  </script>
  {% endif %}
</body>
</html>"""
