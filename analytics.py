#!/usr/bin/env python3
"""PostgreSQL analytics: log searches, expose stats for admin dashboard."""

import os

_db_ok = False
_conn = None
_last_error = ""


_DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:PRzSjmduvkVKfEQlqaRoqaWUOKCLBimk@postgres.railway.internal:5432/railway"
)

def _get_conn():
    global _conn, _last_error
    try:
        import psycopg2
        db_url = _DB_URL
        if not db_url:
            _last_error = "DATABASE_URL not set"
            return None
        if _conn is None or _conn.closed:
            _conn = psycopg2.connect(db_url, connect_timeout=5)
            _conn.autocommit = True
        return _conn
    except Exception as e:
        _last_error = str(e)
        print(f"[analytics] DB connect error: {e}")
        _conn = None
        return None


def init_db():
    global _db_ok, _last_error
    conn = _get_conn()
    if not conn:
        print(f"[analytics] Disabled: {_last_error}")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS searches (
                    id          SERIAL PRIMARY KEY,
                    search_type VARCHAR(20),
                    query       TEXT,
                    ip          TEXT,
                    user_agent  TEXT,
                    created_at  TIMESTAMP DEFAULT NOW()
                )
            """)
        _db_ok = True
        print("[analytics] DB ready")
    except Exception as e:
        _last_error = str(e)
        print(f"[analytics] init_db error: {e}")


def ensure_ready():
    """Try to connect and init if not already done (handles late env var injection)."""
    global _db_ok
    if not _db_ok:
        init_db()


def log_search(search_type: str, query: str, ip: str = None, user_agent: str = None):
    ensure_ready()
    if not _db_ok:
        return
    conn = _get_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO searches (search_type, query, ip, user_agent) VALUES (%s, %s, %s, %s)",
                (search_type, query, ip, (user_agent or "")[:200]),
            )
    except Exception as e:
        print(f"[analytics] log_search error: {e}")


def get_stats() -> dict:
    ensure_ready()
    conn = _get_conn()
    if not conn or not _db_ok:
        return {"error": _last_error or "DB unavailable"}
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS total FROM searches")
            total = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) AS today FROM searches WHERE created_at >= CURRENT_DATE")
            today = cur.fetchone()["today"]

            cur.execute("SELECT COUNT(*) AS week FROM searches WHERE created_at >= NOW() - INTERVAL '7 days'")
            week = cur.fetchone()["week"]

            cur.execute("SELECT search_type, COUNT(*) AS cnt FROM searches GROUP BY search_type ORDER BY cnt DESC")
            by_type = [{"type": r["search_type"], "count": r["cnt"]} for r in cur.fetchall()]

            cur.execute("""
                SELECT UPPER(query) AS query, COUNT(*) AS cnt
                FROM searches WHERE search_type IN ('fuel', 'area')
                GROUP BY UPPER(query) ORDER BY cnt DESC LIMIT 10
            """)
            top_postcodes = [{"query": r["query"], "count": r["cnt"]} for r in cur.fetchall()]

            cur.execute("""
                SELECT query, COUNT(*) AS cnt FROM searches
                WHERE search_type = 'company'
                GROUP BY query ORDER BY cnt DESC LIMIT 10
            """)
            top_companies = [{"query": r["query"], "count": r["cnt"]} for r in cur.fetchall()]

            cur.execute("""
                SELECT DATE(created_at) AS day, COUNT(*) AS cnt
                FROM searches WHERE created_at >= NOW() - INTERVAL '14 days'
                GROUP BY day ORDER BY day
            """)
            daily = [{"day": str(r["day"]), "count": r["cnt"]} for r in cur.fetchall()]

            cur.execute("""
                SELECT search_type, query, ip, created_at
                FROM searches ORDER BY created_at DESC LIMIT 20
            """)
            recent = [
                {
                    "type": r["search_type"],
                    "query": r["query"],
                    "ip": (r["ip"] or "")[:15],
                    "at": r["created_at"].strftime("%d %b %H:%M"),
                }
                for r in cur.fetchall()
            ]

        return {
            "total": total, "today": today, "week": week,
            "by_type": by_type,
            "top_postcodes": top_postcodes,
            "top_companies": top_companies,
            "daily": daily,
            "recent": recent,
        }
    except Exception as e:
        print(f"[analytics] get_stats error: {e}")
        return {"error": str(e)}
