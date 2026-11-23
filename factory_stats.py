"""Visitor stats for the Factory page (humanagency.co/factory).

No cookies and nothing personal: the page makes a random visitor id, kept in that browser,
and sends small events ("view", "open:fuel", "remind:bins"...). No IP address or postcode is stored.

  POST /api/factory/hit       body {"v": visitor id, "e": event, "d": optional detail}  (sent with sendBeacon)
  GET  /factory/stats?key=..  simple stats page. key = CRON_SECRET

Table: run migrations/011_factory_events.sql once in Supabase.
"""
import html
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from flask import Response, jsonify, request

EVENTS = re.compile(r"^(view|open|pin|hide|share|code|place|install|installed|remind|fuel|bins|whatsapp|talk|today|how|promo|trains)(:[a-z0-9-]{1,24})?$")
VISITOR = re.compile(r"^[a-z0-9]{8,40}$")
FACTORY_ORIGIN = "https://humanagency.co"


def _sb():
    import library
    return library._sb()


def register_factory_stats(app):

    @app.route("/api/factory/hit", methods=["POST", "OPTIONS"])
    def factory_hit():
        if request.method == "OPTIONS":
            resp = Response(status=204)
        else:
            try:
                body = json.loads(request.get_data(as_text=True) or "{}")
            except Exception:
                body = {}
            v, e = str(body.get("v") or ""), str(body.get("e") or "")
            if VISITOR.match(v) and EVENTS.match(e):
                try:
                    _sb().table("factory_events").insert({
                        "visitor": v, "event": e, "detail": str(body.get("d") or "")[:40] or None,
                    }).execute()
                except Exception as ex:
                    print(f"[factory-stats] {str(ex)[:160]}")
            resp = Response(status=204)
        if request.headers.get("Origin", "") == FACTORY_ORIGIN:
            resp.headers["Access-Control-Allow-Origin"] = FACTORY_ORIGIN
            resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
            resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp

    @app.route("/factory/stats", methods=["GET"])
    def factory_stats_page():
        secret = os.environ.get("CRON_SECRET", "")
        if not secret or request.args.get("key", "") != secret:
            return jsonify({"error": "Unauthorized"}), 401
        since = (datetime.now(timezone.utc) - timedelta(days=35)).isoformat()
        rows, start = [], 0
        while True:   # page through, 1000 at a time
            batch = (_sb().table("factory_events").select("at,visitor,event")
                     .gte("at", since).order("at").range(start, start + 999).execute().data or [])
            rows += batch
            if len(batch) < 1000 or len(rows) >= 50000:
                break
            start += 1000
        return Response(_render(rows), mimetype="text/html")


def summarise(rows, today=None):
    """Numbers for the stats page. rows: [{at, visitor, event}]"""
    today = today or datetime.now(timezone.utc).date()
    days_by_visitor = defaultdict(set)
    events_7d = Counter()
    for r in rows:
        d = datetime.fromisoformat(str(r["at"]).replace("Z", "+00:00")).date()
        if r["event"] == "view":
            days_by_visitor[r["visitor"]].add(d)
        if (today - d).days < 7 and r["event"] != "view":
            events_7d[r["event"]] += 1
    first = {v: min(ds) for v, ds in days_by_visitor.items()}
    daily = []
    for i in range(13, -1, -1):
        day = today - timedelta(days=i)
        vs = [v for v, ds in days_by_visitor.items() if day in ds]
        returning = sum(1 for v in vs if first[v] < day)
        daily.append({"day": day, "visitors": len(vs), "new": len(vs) - returning, "returning": returning})
    week = {v for v, ds in days_by_visitor.items() if any((today - d).days < 7 for d in ds)}
    week_returning = {v for v in week if first[v] < today - timedelta(days=6) or len(days_by_visitor[v]) > 1}
    # Of people whose first visit was 7 to 34 days ago, how many came back within 7 days
    cohort = [v for v, f in first.items() if 7 <= (today - f).days <= 34]
    came_back = [v for v in cohort if any(0 < (d - first[v]).days <= 7 for d in days_by_visitor[v])]
    return {
        "daily": daily,
        "week_visitors": len(week),
        "week_returning": len(week_returning),
        "cohort": len(cohort),
        "came_back": len(came_back),
        "events": events_7d.most_common(20),
    }


def _render(rows):
    s = summarise(rows)
    pct = lambda a, b: f"{round(100 * a / b)}%" if b else "–"
    peak = max([d["visitors"] for d in s["daily"]] + [1])
    bars = "".join(
        f"<tr><td>{d['day'].strftime('%a %d %b')}</td><td><div class='bar'><i style='width:{100 * d['new'] / peak:.0f}%'></i>"
        f"<b style='width:{100 * d['returning'] / peak:.0f}%'></b></div></td><td>{d['visitors']}</td><td>{d['returning']}</td></tr>"
        for d in s["daily"])
    events = "".join(f"<tr><td>{html.escape(e)}</td><td>{n}</td></tr>" for e, n in s["events"]) or "<tr><td colspan=2>Nothing yet</td></tr>"
    return f"""<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Factory stats</title>
<style>body{{font:15px/1.4 system-ui,sans-serif;max-width:760px;margin:30px auto;padding:0 16px;color:#1d1630;background:#fff8f1}}
h1{{font-size:24px}} .k{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:16px 0 26px}}
.k div{{background:#fff;border:2px solid #1d1630;border-radius:14px;padding:12px}} .k b{{display:block;font-size:28px}}
table{{width:100%;border-collapse:collapse;margin-bottom:26px}} td,th{{padding:6px 4px;border-bottom:1px solid #eadfd2;text-align:left}}
.bar{{display:flex;height:14px;background:#f3ece4;border-radius:7px;overflow:hidden;min-width:160px}} .bar i{{background:#5aa9ff}} .bar b{{background:#2fd1a4}}
small{{color:#6c6485}}</style>
<h1>Factory stats</h1>
<div class="k">
 <div><small>Visitors, last 7 days</small><b>{s['week_visitors']}</b></div>
 <div><small>Of those, came back</small><b>{pct(s['week_returning'], s['week_visitors'])}</b></div>
 <div><small>Returned within 7 days of first visit</small><b>{pct(s['came_back'], s['cohort'])}</b><small>{s['came_back']} of {s['cohort']} people</small></div>
</div>
<h2>Last 14 days</h2><small><span style="color:#5aa9ff">■</span> new &nbsp; <span style="color:#2fd1a4">■</span> returning</small>
<table><tr><th>Day</th><th></th><th>Visitors</th><th>Returning</th></tr>{bars}</table>
<h2>What people did, last 7 days</h2><table><tr><th>Event</th><th>Count</th></tr>{events}</table>
<small>No cookies, no IP addresses, no postcodes. Visitor = a random id kept in that browser.</small>"""
