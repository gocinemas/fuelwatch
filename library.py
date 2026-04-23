import os
import uuid

from supabase import create_client

_SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


def _sb():
    return create_client(_SUPABASE_URL, _SUPABASE_KEY)


def chunk_text(text: str, chunk_size: int = 600, overlap: int = 80) -> list:
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
    doc_id = doc.data[0]["id"]

    chunks = chunk_text(text)
    if chunks:
        sb.table("library_chunks").insert([
            {"doc_id": doc_id, "chunk_index": i, "content": c}
            for i, c in enumerate(chunks)
        ]).execute()

    return doc.data[0]


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


def search_chunks(doc_id: str, query: str, n: int = 6) -> list:
    sb = _sb()
    chunks = sb.table("library_chunks").select("content").eq("doc_id", doc_id).execute().data
    if not chunks:
        return []
    q_words = set(w for w in query.lower().split() if len(w) > 2)
    scored = sorted(chunks, key=lambda c: -sum(1 for w in q_words if w in c["content"].lower()))
    return [c["content"] for c in scored[:n]]


def delete_document(share_id: str) -> bool:
    sb = _sb()
    return bool(sb.table("library_docs").delete().eq("share_id", share_id).execute().data)
