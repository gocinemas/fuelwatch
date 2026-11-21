"""
Books API for the Factory books page.

  GET    /api/books/readers?household=H              readers on this device
  POST   /api/books/readers        {household,name}  add a reader
  PATCH  /api/books/readers/<id>   {household,theme,avatar}  change a reader's theme or avatar
  POST   /api/books/read-slip      photo (image)    read a library slip: titles and due dates
  POST   /api/books/link-code      {household}      make a 6-digit code to join from another device
  POST   /api/books/link           {code}           use a code: returns the shared household
  DELETE /api/books/readers/<id>?household=H         remove a reader and their books
  GET    /api/books/list?household=H&reader=ID       one reader's books
  POST   /api/books/list           {household,reader,...}  add or update a book
  DELETE /api/books/list/<id>?household=H            remove a book

Every call checks that the reader and the book belong to this household.
Tables: book_readers, book_list (see books_tables.sql and books_readers.sql).
"""
import os, re, requests
from flask import jsonify, request

SB = "https://wagbceigsvctbdthfczf.supabase.co/rest/v1"
SHELVES = {"reading", "read", "tb", "wish"}
THEMES = {"sunny", "ocean", "jungle", "sunset", "space", "candy"}


def _key():
    return os.environ.get("BOOKS_SUPABASE_KEY", "")


def _h(prefer=None):
    k = _key()
    h = {"apikey": k, "Authorization": f"Bearer {k}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    return h


def _household(raw):
    raw = (raw or "").strip()
    return raw if re.fullmatch(r"[A-Za-z0-9_-]{16,64}", raw) else None


def _owns_reader(household, reader_id):
    r = requests.get(f"{SB}/book_readers", headers=_h(), params={"select": "id", "id": f"eq.{reader_id}", "household": f"eq.{household}"}, timeout=15)
    return r.status_code == 200 and len(r.json()) == 1


def register_books_api(app):
    import secrets
    from datetime import datetime, timedelta, timezone

    @app.route("/api/books/read-slip", methods=["POST", "OPTIONS"])
    def books_read_slip():
        """Read a photo of a library slip: returns the titles and due-back dates it lists (the page asks her to confirm)."""
        if request.method == "OPTIONS":
            return ("", 204)
        f = request.files.get("image")
        if not f:
            return jsonify({"error": "Take a photo of the slip."}), 400
        import base64, json
        raw = f.read()
        if len(raw) > 6 * 1024 * 1024:
            return jsonify({"error": "That photo is too big. Try a closer one."}), 400
        media = f.mimetype if f.mimetype in ("image/jpeg", "image/png", "image/webp", "image/gif") else "image/jpeg"
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
            resp = client.messages.create(
                model="claude-sonnet-4-6", max_tokens=1200,
                messages=[{"role": "user", "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media, "data": base64.b64encode(raw).decode()}},
                    {"type": "text", "text": "This is a library receipt or loan slip. List every book it shows. Return only JSON: "
                     '{"books":[{"title":"...","author":"...","due":"YYYY-MM-DD"}]}. Use null for anything you cannot read clearly. '
                     "Do not guess titles or dates."}]}])
            text = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
            m = re.search(r"\{[\s\S]*\}", text)
            data = json.loads(m.group(0)) if m else {"books": []}
        except Exception as e:
            print(f"[books] read-slip failed: {e}", flush=True)
            return jsonify({"error": "Couldn't read that photo. Try a clearer one, or type the books in."}), 502
        books = []
        for b in (data.get("books") or [])[:60]:
            t = (b.get("title") or "").strip()
            if t:
                books.append({"title": t[:300], "author": (b.get("author") or "")[:200], "due": b.get("due") if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(b.get("due") or "")) else None})
        return jsonify({"books": books}), 200

    @app.route("/api/books/link-code", methods=["POST", "OPTIONS"])
    def books_link_code():
        """Make a 6-digit code (valid 10 minutes) that lets another device join this household."""
        if request.method == "OPTIONS":
            return ("", 204)
        household = _household((request.get_json(silent=True) or {}).get("household"))
        if not household:
            return jsonify({"error": "This device isn't set up for books yet."}), 400
        code = f"{secrets.randbelow(1000000):06d}"
        expires = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
        requests.delete(f"{SB}/book_link_codes", headers=_h(), params={"household": f"eq.{household}"}, timeout=15)
        r = requests.post(f"{SB}/book_link_codes", headers=_h(), json={"code": code, "household": household, "expires_at": expires}, timeout=15)
        if not r.ok:
            return jsonify({"error": "Could not make a code. Try again."}), 502
        return jsonify({"code": code, "minutes": 10}), 201

    @app.route("/api/books/link", methods=["POST", "OPTIONS"])
    def books_link_use():
        """Use a code on a second device: returns the shared household so this device sees the same lists."""
        if request.method == "OPTIONS":
            return ("", 204)
        body = request.get_json(silent=True) or {}
        code = re.sub(r"\D", "", str(body.get("code") or ""))
        if len(code) != 6:
            return jsonify({"error": "Type the 6-digit code."}), 400
        r = requests.get(f"{SB}/book_link_codes", headers=_h(), params={"select": "code,household,expires_at", "code": f"eq.{code}"}, timeout=15)
        rows = r.json() if r.ok else []
        if not rows:
            return jsonify({"error": "That code isn't right. Check it and try again."}), 404
        row = rows[0]
        if datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00")) < datetime.now(timezone.utc):
            requests.delete(f"{SB}/book_link_codes", headers=_h(), params={"code": f"eq.{code}"}, timeout=15)
            return jsonify({"error": "That code has run out. Make a new one on the other device."}), 410
        requests.delete(f"{SB}/book_link_codes", headers=_h(), params={"code": f"eq.{code}"}, timeout=15)
        return jsonify({"household": row["household"]}), 200

    @app.after_request
    def _books_cors(resp):
        if request.path.startswith("/api/books/"):
            origin = request.headers.get("Origin", "")
            if origin in ("https://humanagency.co", "https://www.humanagency.co"):
                resp.headers["Access-Control-Allow-Origin"] = origin
                resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"
                resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
                resp.headers["Vary"] = "Origin"
        return resp

    @app.route("/api/books/readers", methods=["GET", "POST", "OPTIONS"])
    def books_readers():
        if request.method == "OPTIONS":
            return ("", 204)
        body = request.get_json(silent=True) or {}
        household = _household(request.args.get("household") or body.get("household"))
        if not household:
            return jsonify({"error": "This device isn't set up for books yet."}), 400
        if request.method == "GET":
            r = requests.get(f"{SB}/book_readers", headers=_h(), params={"select": "id,name", "household": f"eq.{household}", "order": "added.asc"}, timeout=15)
            return jsonify(r.json() if r.ok else []), (200 if r.ok else 502)
        name = (body.get("name") or "").strip()[:30]
        if not name:
            return jsonify({"error": "Type a name."}), 400
        # One name per device: if it's already there, return that reader instead of making a second one
        existing = requests.get(f"{SB}/book_readers", headers=_h(), params={"select": "id,name", "household": f"eq.{household}", "name": f"ilike.{name}"}, timeout=15)
        if existing.ok and existing.json():
            row = existing.json()[0]
            return jsonify({"id": row["id"], "name": row["name"]}), 200
        r = requests.post(f"{SB}/book_readers", headers=_h("return=representation"), json={"household": household, "name": name}, timeout=15)
        if not r.ok:
            return jsonify({"error": "Could not add that name."}), 502
        row = r.json()[0]
        return jsonify({"id": row["id"], "name": row["name"]}), 201

    @app.route("/api/books/readers/<reader_id>", methods=["PATCH", "DELETE", "OPTIONS"])
    def books_reader_delete(reader_id):
        if request.method == "OPTIONS":
            return ("", 204)
        household = _household(request.args.get("household") or (request.get_json(silent=True) or {}).get("household"))
        if not household or not _owns_reader(household, reader_id):
            return jsonify({"error": "Not found."}), 404
        if request.method == "PATCH":
            body = request.get_json(silent=True) or {}
            change = {}
            if body.get("theme") in THEMES:
                change["theme"] = body["theme"]
            if body.get("avatar") and len(str(body["avatar"])) <= 8:
                change["avatar"] = str(body["avatar"])
            if body.get("name") and 1 <= len(str(body["name"]).strip()) <= 30:
                change["name"] = str(body["name"]).strip()
            if not change:
                return jsonify({"error": "Nothing to change."}), 400
            r = requests.patch(f"{SB}/book_readers", headers=_h("return=representation"), params={"id": f"eq.{reader_id}"}, json=change, timeout=15)
            return (jsonify(r.json()[0]), 200) if r.ok and r.json() else (jsonify({"error": "Could not save."}), 502)
        requests.delete(f"{SB}/book_readers", headers=_h(), params={"id": f"eq.{reader_id}"}, timeout=15)
        return jsonify({"ok": True})

    @app.route("/api/books/list", methods=["GET", "POST", "OPTIONS"])
    def books_list():
        if request.method == "OPTIONS":
            return ("", 204)
        body = request.get_json(silent=True) or {}
        household = _household(request.args.get("household") or body.get("household"))
        if not household:
            return jsonify({"error": "This device isn't set up for books yet."}), 400
        if request.method == "GET":
            reader = (request.args.get("reader") or "").strip()
            if not _owns_reader(household, reader):
                return jsonify({"error": "Not found."}), 404
            r = requests.get(f"{SB}/book_list", headers=_h(), params={
                "select": "id,book_id,title,author,cover,shelf,rating,finished,notes,added,due",
                "reader_id": f"eq.{reader}", "household": f"eq.{household}", "order": "added.desc"}, timeout=15)
            return jsonify(r.json() if r.ok else []), (200 if r.ok else 502)
        reader = (body.get("reader") or "").strip()
        if not _owns_reader(household, reader):
            return jsonify({"error": "Not found."}), 404
        shelf = body.get("shelf") if body.get("shelf") in SHELVES else "tb"
        rating = min(max(int(body.get("rating") or 0), 0), 5)
        row = {
            "household": household, "reader_id": reader,
            "book_id": (body.get("book_id") or None), "title": (body.get("title") or "Untitled")[:300],
            "author": (body.get("author") or "")[:200], "cover": (body.get("cover") or "")[:500],
            "shelf": shelf, "rating": rating, "finished": (body.get("finished") or None),
            "notes": (body.get("notes") or "")[:2000],
            "due": (body.get("due") or None),
        }
        if body.get("id"):   # update an existing book of this reader
            r = requests.patch(f"{SB}/book_list", headers=_h("return=representation"), params={"id": f"eq.{body['id']}", "reader_id": f"eq.{reader}", "household": f"eq.{household}"}, json=row, timeout=15)
        else:
            r = requests.post(f"{SB}/book_list", headers=_h("return=representation"), json=row, timeout=15)
        if not r.ok or not r.json():
            return jsonify({"error": "Could not save that book."}), 502
        return jsonify(r.json()[0]), 200

    @app.route("/api/books/list/<book_id>", methods=["DELETE", "OPTIONS"])
    def books_list_delete(book_id):
        if request.method == "OPTIONS":
            return ("", 204)
        household = _household(request.args.get("household"))
        if not household:
            return jsonify({"error": "Not found."}), 404
        requests.delete(f"{SB}/book_list", headers=_h(), params={"id": f"eq.{book_id}", "household": f"eq.{household}"}, timeout=15)
        return jsonify({"ok": True})
