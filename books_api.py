"""
Books API for the Factory books page.

  GET    /api/books/readers?household=H              readers on this device
  POST   /api/books/readers        {household,name}  add a reader
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
    @app.after_request
    def _books_cors(resp):
        if request.path.startswith("/api/books/"):
            origin = request.headers.get("Origin", "")
            if origin in ("https://humanagency.co", "https://www.humanagency.co"):
                resp.headers["Access-Control-Allow-Origin"] = origin
                resp.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
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
        r = requests.post(f"{SB}/book_readers", headers=_h("return=representation"), json={"household": household, "name": name}, timeout=15)
        if not r.ok:
            return jsonify({"error": "Could not add that name."}), 502
        row = r.json()[0]
        return jsonify({"id": row["id"], "name": row["name"]}), 201

    @app.route("/api/books/readers/<reader_id>", methods=["DELETE", "OPTIONS"])
    def books_reader_delete(reader_id):
        if request.method == "OPTIONS":
            return ("", 204)
        household = _household(request.args.get("household"))
        if not household or not _owns_reader(household, reader_id):
            return jsonify({"error": "Not found."}), 404
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
                "select": "id,book_id,title,author,cover,shelf,rating,finished,notes,added",
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
