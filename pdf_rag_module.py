"""
Local RAG system for personal PDFs using sentence-transformers
Ingests PDFs, chunks content, generates embeddings, stores in Supabase
"""
import os
import json
from pathlib import Path
from typing import List, Dict, Optional
import PyPDF2
from sentence_transformers import SentenceTransformer
import library as lib


class PDFRAGEngine:
    """Local RAG for personal research PDFs"""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """Initialize with local embedding model"""
        print(f"Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.chunk_size = 500  # tokens
        self.chunk_overlap = 50

    def ingest_pdf(self, pdf_path: str, metadata: Dict = None) -> int:
        """Ingest a single PDF, chunk it, embed, and store"""
        try:
            pdf_path = Path(pdf_path)
            if not pdf_path.exists():
                print(f"❌ PDF not found: {pdf_path}")
                return 0

            print(f"📄 Ingesting: {pdf_path.name}")

            # Extract text from PDF
            text = self._extract_pdf_text(pdf_path)
            if not text:
                print(f"❌ No text extracted from {pdf_path.name}")
                return 0

            # Chunk text
            chunks = self._chunk_text(text)
            print(f"   Chunks: {len(chunks)}")

            # Generate embeddings
            embeddings = self.model.encode(chunks, convert_to_tensor=False)

            # Store in Supabase
            stored_count = self._store_chunks(pdf_path.name, chunks, embeddings, metadata or {})
            print(f"   ✅ Stored {stored_count} chunks")
            return stored_count

        except Exception as e:
            print(f"❌ Error ingesting {pdf_path}: {e}")
            return 0

    def ingest_folder(self, folder_path: str, metadata: Dict = None) -> int:
        """Ingest all PDFs from a folder"""
        folder = Path(folder_path)
        if not folder.exists():
            print(f"❌ Folder not found: {folder_path}")
            return 0

        total_chunks = 0
        for pdf_file in folder.glob("*.pdf"):
            chunks = self.ingest_pdf(str(pdf_file), metadata)
            total_chunks += chunks

        print(f"\n✅ Total chunks stored: {total_chunks}")
        return total_chunks

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        """Search PDFs using RAG"""
        try:
            # Embed query
            query_embedding = self.model.encode(query, convert_to_tensor=False).tolist()

            # Search in Supabase
            results = self._search_embeddings(query_embedding, top_k)
            return results

        except Exception as e:
            print(f"❌ Search error: {e}")
            return []

    # ─── Private methods ───

    def _extract_pdf_text(self, pdf_path: Path) -> str:
        """Extract text from PDF"""
        text = []
        try:
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages:
                    text.append(page.extract_text())
            return "\n".join(text)
        except Exception as e:
            print(f"   Error extracting text: {e}")
            return ""

    def _chunk_text(self, text: str) -> List[str]:
        """Split text into chunks with overlap"""
        words = text.split()
        chunks = []

        for i in range(0, len(words), self.chunk_size - self.chunk_overlap):
            chunk = " ".join(words[i:i + self.chunk_size])
            if chunk.strip():
                chunks.append(chunk)

        return chunks

    def _store_chunks(self, filename: str, chunks: List[str], embeddings: List, metadata: Dict) -> int:
        """Store chunks and embeddings in Supabase"""
        try:
            sb = lib._sb()
            stored = 0

            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                sb.table("pdf_chunks").insert({
                    "filename": filename,
                    "chunk_index": i,
                    "content": chunk,
                    "embedding": embedding,
                    "metadata": json.dumps(metadata or {}),
                }).execute()
                stored += 1

            return stored

        except Exception as e:
            print(f"   Error storing chunks: {e}")
            return 0

    def _search_embeddings(self, query_embedding: List, top_k: int) -> List[Dict]:
        """Search for similar chunks using embeddings"""
        try:
            sb = lib._sb()

            # Note: This is a simple similarity search
            # For production, use pgvector extension in Supabase for vector search
            chunks = sb.table("pdf_chunks").select("*").limit(100).execute().data or []

            # Calculate similarity
            import numpy as np
            results = []
            for chunk in chunks:
                if chunk.get("embedding"):
                    similarity = np.dot(query_embedding, chunk["embedding"])
                    results.append({
                        "score": float(similarity),
                        "filename": chunk["filename"],
                        "content": chunk["content"],
                    })

            # Sort by score and return top_k
            results.sort(key=lambda x: x["score"], reverse=True)
            return results[:top_k]

        except Exception as e:
            print(f"Search error: {e}")
            return []


if __name__ == "__main__":
    # Example usage
    rag = PDFRAGEngine()

    # Ingest PDFs
    pdf_folder = os.path.expanduser("~/Documents/Research")
    rag.ingest_folder(pdf_folder)

    # Search
    results = rag.search("machine learning best practices", top_k=3)
    for r in results:
        print(f"✓ {r['filename']}: {r['content'][:100]}...")
