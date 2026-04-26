import os
import uuid

from supabase import create_client
try:
    from algoliasearch.search_client import SearchClient
    _ALGOLIA_OK = True
except ImportError:
    _ALGOLIA_OK = False

_SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
_ALGOLIA_APP  = os.environ.get("ALGOLIA_APP_ID", "")
_ALGOLIA_KEY  = os.environ.get("ALGOLIA_API_KEY", "")
_INDEX_NAME   = "library_chunks"


def _sb():
    return create_client(_SUPABASE_URL, _SUPABASE_KEY)


def _idx():
    if not _ALGOLIA_OK:
        raise RuntimeError("algoliasearch not installed")
    if not _ALGOLIA_APP or not _ALGOLIA_KEY:
        raise RuntimeError("Algolia credentials not set")
    client = SearchClient.create(_ALGOLIA_APP, _ALGOLIA_KEY)
    return client.init_index(_INDEX_NAME)


def chunk_text(text: str, chunk_size: int = 350, overlap: int = 50) -> list:
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunks.append(" ".join(words[i : i + chunk_size]))
        i += chunk_size - overlap
    return chunks


def upload_document(title: str, text: str, doc_type: str = "note", page_count: int = 0) -> dict:
    sb = _sb()
    share_id = str(uuid.uuid4())
    doc = sb.table("library_docs").insert({
        "title":        title,
        "doc_type":     doc_type,
        "text_content": text[:60000],
        "page_count":   page_count,
        "char_count":   len(text),
        "share_id":     share_id,
    }).execute()
    doc_record = doc.data[0]
    doc_id = doc_record["id"]

    chunks = chunk_text(text)
    if chunks:
        # Store in Supabase
        sb.table("library_chunks").insert([
            {"doc_id": doc_id, "chunk_index": i, "content": c}
            for i, c in enumerate(chunks)
        ]).execute()

        # Index in Algolia
        try:
            idx = _idx()
            idx.save_objects([{
                "objectID":    f"{share_id}_{i}",
                "share_id":    share_id,
                "doc_id":      doc_id,
                "doc_title":   title,
                "doc_type":    doc_type,
                "chunk_index": i,
                "content":     c,
            } for i, c in enumerate(chunks)])
        except Exception:
            pass  # Algolia indexing failure doesn't block upload

    return doc_record


def list_documents() -> list:
    sb = _sb()
    return sb.table("library_docs") \
        .select("id,share_id,title,doc_type,page_count,char_count,created_at") \
        .order("created_at", desc=True).execute().data


def get_document(share_id: str) -> dict | None:
    sb = _sb()
    rows = sb.table("library_docs").select("*").eq("share_id", share_id).execute().data
    if not rows:
        return None
    doc = rows[0]
    chunks = sb.table("library_chunks") \
        .select("chunk_index,content") \
        .eq("doc_id", doc["id"]) \
        .order("chunk_index").execute().data
    doc["chunks"] = chunks
    return doc


def search_library(query: str, n: int = 10) -> list:
    """Full-text search across all documents. Tries Algolia first, falls back to Supabase."""
    # Try Algolia
    try:
        idx = _idx()
        res = idx.search(query, {"hitsPerPage": n * 2, "attributesToRetrieve": [
            "share_id", "doc_title", "doc_type", "content", "chunk_index"
        ]})
        hits = res.get("hits", [])
        if hits:
            seen, results = set(), []
            for h in hits:
                if h["share_id"] not in seen:
                    seen.add(h["share_id"])
                    results.append({
                        "share_id": h["share_id"],
                        "title":    h["doc_title"],
                        "doc_type": h["doc_type"],
                        "snippet":  h["content"][:200],
                    })
            return results[:n]
    except Exception:
        pass
    # Fallback: keyword-scored search via Supabase
    chunks = _search_all_chunks_supabase(query, n)
    seen, results = set(), []
    for c in chunks:
        if c["share_id"] not in seen:
            seen.add(c["share_id"])
            results.append({
                "share_id": c["share_id"],
                "title":    c["title"],
                "doc_type": "document",
                "snippet":  c["content"][:200],
            })
    return results


def _search_all_chunks_supabase(query: str, n: int = 8) -> list:
    """Keyword-scored fallback search across all Supabase chunks."""
    try:
        sb = _sb()
        docs_data = sb.table("library_docs").select("id,share_id,title").execute().data
        if not docs_data:
            return []
        doc_map = {d["id"]: d for d in docs_data}

        q_words = [w.lower() for w in query.split() if len(w) > 3]

        if q_words:
            # Fetch chunks containing the first keyword, then score all words
            chunks = (sb.table("library_chunks")
                      .select("doc_id,content")
                      .ilike("content", f"%{q_words[0]}%")
                      .limit(300)
                      .execute().data)
            if not chunks:
                # Broaden: any chunk from these docs
                chunks = (sb.table("library_chunks")
                          .select("doc_id,content")
                          .limit(200)
                          .execute().data)
        else:
            chunks = (sb.table("library_chunks")
                      .select("doc_id,content")
                      .limit(n)
                      .execute().data)

        scored = sorted(chunks, key=lambda c: -sum(1 for w in q_words if w in c["content"].lower()))
        results = []
        for c in scored[:n]:
            meta = doc_map.get(c["doc_id"])
            if meta:
                results.append({
                    "title":    meta["title"],
                    "share_id": meta["share_id"],
                    "content":  c["content"],
                })
        return results
    except Exception:
        return []


def search_all_chunks(query: str, n: int = 8) -> list:
    """Return top-n chunks across all docs for RAG. Tries Algolia first, falls back to Supabase."""
    try:
        idx = _idx()
        res = idx.search(query, {"hitsPerPage": n, "attributesToRetrieve": [
            "share_id", "doc_title", "content"
        ]})
        hits = res.get("hits", [])
        if hits:
            return [{"title": h["doc_title"], "share_id": h["share_id"], "content": h["content"]}
                    for h in hits]
    except Exception:
        pass
    # Algolia unavailable or returned nothing — fall back to Supabase keyword search
    return _search_all_chunks_supabase(query, n)


def search_chunks(doc_id: str, query: str, n: int = 6) -> list:
    """Keyword search within a single doc (fallback if Algolia unavailable)."""
    sb = _sb()
    chunks = sb.table("library_chunks").select("content").eq("doc_id", doc_id).execute().data
    if not chunks:
        return []
    q_words = set(w for w in query.lower().split() if len(w) > 2)
    scored = sorted(chunks, key=lambda c: -sum(1 for w in q_words if w in c["content"].lower()))
    return [c["content"] for c in scored[:n]]


def reindex_all() -> dict:
    """Re-index all documents from Supabase into Algolia. Call once after integration."""
    sb = _sb()
    idx = _idx()
    docs = sb.table("library_docs").select("id,share_id,title,doc_type").execute().data
    total, indexed = len(docs), 0
    for doc in docs:
        chunks = sb.table("library_chunks").select("chunk_index,content") \
            .eq("doc_id", doc["id"]).order("chunk_index").execute().data
        if chunks:
            idx.save_objects([{
                "objectID":    f"{doc['share_id']}_{c['chunk_index']}",
                "share_id":    doc["share_id"],
                "doc_id":      doc["id"],
                "doc_title":   doc["title"],
                "doc_type":    doc["doc_type"],
                "chunk_index": c["chunk_index"],
                "content":     c["content"],
            } for c in chunks])
            indexed += 1
    return {"total_docs": total, "indexed": indexed}


def delete_document(share_id: str) -> bool:
    sb = _sb()
    result = sb.table("library_docs").delete().eq("share_id", share_id).execute()
    # Remove from Algolia index
    try:
        idx = _idx()
        idx.delete_by({"filters": f"share_id:{share_id}"})
    except Exception:
        pass
    return bool(result.data)
