"""
Document Library — RAG-ready document indexing and search.

Scans specified folders, extracts text, stores in database with full-text search.
Supports: PDF, DOCX, MD, TXT, DOC
"""

from flask import Blueprint, request, jsonify, render_template
import logging
import os
from datetime import datetime
import json

logger = logging.getLogger(__name__)
bp = Blueprint('document_library', __name__, url_prefix='/api/library')

# Safe folders to index (user-approved)
SAFE_FOLDERS = [
    os.path.expanduser("~/Downloads/Books"),
    os.path.expanduser("~/Downloads/Research_Reports"),
    os.path.expanduser("~/Downloads/Reports"),
    os.path.expanduser("~/Downloads/Content-Hub"),
    os.path.expanduser("~/Downloads/Web_Downloads"),
    os.path.expanduser("~/Documents/Books"),
]


def _extract_text_from_file(filepath):
    """Extract text from various file types. Returns (title, text) or (None, None)."""
    try:
        ext = os.path.splitext(filepath)[1].lower()
        filename = os.path.basename(filepath)

        if ext == '.txt':
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
            return filename, text

        elif ext == '.md':
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
            return filename, text

        elif ext == '.pdf':
            try:
                import PyPDF2
                with open(filepath, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    text = '\n'.join([page.extract_text() for page in reader.pages[:50]])  # First 50 pages
                return filename, text
            except ImportError:
                logger.warning(f"PyPDF2 not installed, skipping {filename}")
                return None, None

        elif ext in ['.docx']:
            try:
                from docx import Document
                doc = Document(filepath)
                text = '\n'.join([p.text for p in doc.paragraphs])
                return filename, text
            except ImportError:
                logger.warning(f"python-docx not installed, skipping {filename}")
                return None, None

        elif ext in ['.doc']:
            # .doc support requires python-docx or other library
            logger.warning(f"Legacy .doc format not supported yet: {filename}")
            return None, None

        else:
            return None, None

    except Exception as e:
        logger.error(f"Error extracting text from {filepath}: {e}")
        return None, None


def _scan_folder(folder_path):
    """Scan a folder and return list of (filepath, title, text) tuples."""
    documents = []

    if not os.path.exists(folder_path):
        logger.warning(f"Folder not found: {folder_path}")
        return documents

    for root, dirs, files in os.walk(folder_path):
        for filename in files:
            filepath = os.path.join(root, filename)
            ext = os.path.splitext(filename)[1].lower()

            if ext in ['.pdf', '.docx', '.doc', '.txt', '.md']:
                title, text = _extract_text_from_file(filepath)
                if text:
                    documents.append({
                        'filepath': filepath,
                        'filename': filename,
                        'folder': folder_path,
                        'title': title or filename,
                        'text': text,
                        'file_type': ext[1:] if ext else 'unknown',
                        'file_size': os.path.getsize(filepath)
                    })

    return documents


@bp.route('/search-ui', methods=['GET'])
def search_ui():
    """Serve the library search UI page."""
    return render_template('library_search.html')


@bp.route('/index-status', methods=['GET'])
def get_index_status():
    """Get status of document indexing."""
    try:
        from sms_service import lib

        rows = lib._sb().table("document_index_status").select("*").execute().data or []

        return jsonify({
            "ok": True,
            "status": rows,
            "total_folders": len(SAFE_FOLDERS),
            "safe_folders": SAFE_FOLDERS
        }), 200

    except Exception as e:
        logger.error(f"[library] Status error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route('/index-scan', methods=['POST'])
def index_scan():
    """
    Scan all safe folders and index documents.
    Endpoint: POST /api/library/index-scan
    """
    try:
        from sms_service import lib

        total_indexed = 0
        results = {}

        for folder in SAFE_FOLDERS:
            logger.info(f"[library] Scanning {folder}...")

            # Update status
            lib._sb().table("document_index_status").upsert({
                "folder_path": folder,
                "status": "indexing"
            }).execute()

            # Scan folder
            docs = _scan_folder(folder)

            # Store in database
            for doc in docs:
                excerpt = doc['text'][:200] + "..." if len(doc['text']) > 200 else doc['text']

                try:
                    lib._sb().table("documents").insert({
                        "filename": doc['filename'],
                        "folder_path": doc['folder'],
                        "file_type": doc['file_type'],
                        "title": doc['title'],
                        "excerpt": excerpt,
                        "full_text": doc['text'],
                        "file_size_bytes": doc['file_size'],
                    }).execute()
                    total_indexed += 1
                except Exception as e:
                    logger.warning(f"[library] Failed to index {doc['filename']}: {e}")

            # Update status
            lib._sb().table("document_index_status").update({
                "doc_count": len(docs),
                "last_indexed_at": datetime.utcnow().isoformat(),
                "status": "complete"
            }).eq("folder_path", folder).execute()

            results[folder] = len(docs)

        logger.info(f"[library] Indexed {total_indexed} documents total")

        return jsonify({
            "ok": True,
            "total_indexed": total_indexed,
            "by_folder": results
        }), 200

    except Exception as e:
        logger.error(f"[library] Scan error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route('/search', methods=['GET'])
def search_documents():
    """
    Full-text search documents.
    Query: GET /api/library/search?q=artificial+intelligence&limit=10
    """
    query = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 20))

    if not query or len(query) < 2:
        return jsonify({"ok": False, "error": "query too short (min 2 chars)"}), 400

    try:
        from sms_service import lib

        # Full-text search using PostgreSQL tsvector
        rows = lib._sb().table("documents").select("id,filename,title,excerpt,folder_path,file_type") \
            .textSearch("ts_vector", query) \
            .limit(limit) \
            .execute().data or []

        return jsonify({
            "ok": True,
            "query": query,
            "results": rows,
            "count": len(rows)
        }), 200

    except Exception as e:
        logger.error(f"[library] Search error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route('/by-folder', methods=['GET'])
def documents_by_folder():
    """Get documents from a specific folder."""
    folder = request.args.get('folder', '').strip()
    limit = int(request.args.get('limit', 50))

    if not folder:
        return jsonify({"ok": False, "error": "folder required"}), 400

    try:
        from sms_service import lib

        rows = lib._sb().table("documents").select("id,filename,title,excerpt,file_type") \
            .eq("folder_path", folder) \
            .limit(limit) \
            .execute().data or []

        return jsonify({
            "ok": True,
            "folder": folder,
            "documents": rows,
            "count": len(rows)
        }), 200

    except Exception as e:
        logger.error(f"[library] Folder search error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


def register_document_library_endpoints(app):
    """Wire up document library endpoints."""
    app.register_blueprint(bp)
    logger.info("[library] Document library endpoints registered")
