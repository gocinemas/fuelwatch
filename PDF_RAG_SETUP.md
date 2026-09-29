# PDF RAG Setup Guide

Your personal research PDFs are now searchable through Ask Miru! 📚🔍

## Setup Steps

### 1. Install Dependencies
```bash
pip install PyPDF2 sentence-transformers numpy
```

### 2. Create Supabase Table
Run `setup_pdf_rag.sql` in your Supabase dashboard:
- Go to SQL Editor
- Paste the contents of `setup_pdf_rag.sql`
- Execute

### 3. Point to Your PDF Folder
Edit `pdf_rag_module.py` line 215:
```python
pdf_folder = os.path.expanduser("~/Documents/Research")  # Change this path
```

Use your actual folder path, e.g.:
- `~/Documents/Research`
- `~/Downloads/PDFs`
- `~/Desktop/Papers`

### 4. Ingest Your PDFs
```bash
python -c "from pdf_rag_module import PDFRAGEngine; rag = PDFRAGEngine(); rag.ingest_folder(os.path.expanduser('~/Documents/Research'))"
```

Or create a simple script:
```python
from pdf_rag_module import PDFRAGEngine
import os

rag = PDFRAGEngine()
pdf_folder = os.path.expanduser("~/Documents/Research")
rag.ingest_folder(pdf_folder)
```

## Usage

Now ask Ask Miru about your research:

**"What's in my PDFs about machine learning?"**
```
📚 From your research PDFs:
1. ML-Best-Practices (relevance: 0.87)
   Machine learning involves supervised and unsupervised learning...

2. Neural-Networks-Intro (relevance: 0.82)
   Neural networks are inspired by biological neurons...
```

## Supported Queries

The system recognizes:
- "What's in my research about X?"
- "Find PDFs on Y"
- "What did I learn about Z?"
- "Show me papers on W"
- Any question containing: `pdf`, `research`, `document`, `paper`, `study`, `notes`, `knowledge`, `learned`

## How It Works

1. **PDF Ingestion**: Extracts text from PDFs and chunks it
2. **Embedding**: Uses `sentence-transformers` to create local embeddings (all-MiniLM-L6-v2)
3. **Storage**: Stores chunks + embeddings in Supabase `pdf_chunks` table
4. **Search**: Finds relevant chunks using similarity matching
5. **Display**: Shows top results with relevance scores

## Privacy

✅ **Local embeddings** - no cloud AI service calls
✅ **Your data stays in Supabase** - you control it
✅ **No third-party APIs** - sentence-transformers runs locally

## Advanced: Vector Search (Optional)

For faster search on large PDF collections, enable pgvector in Supabase:

1. In Supabase Dashboard → Database → Extensions
2. Enable `pgvector`
3. Uncomment the vector index line in `setup_pdf_rag.sql`
4. Redeploy

Then the search will use true vector similarity instead of approximate matching.

## Troubleshooting

**"PDF RAG not available"**
- Check dependencies: `pip install PyPDF2 sentence-transformers`
- First run takes time (downloads embedding model ~80MB)

**No results found**
- Make sure PDFs are in correct folder
- Run ingestion again: `rag.ingest_folder(path)`
- Check Supabase: `SELECT COUNT(*) FROM pdf_chunks`

**Slow search**
- Enable pgvector for vector indexes (see Advanced section above)
- Or reduce `top_k` parameter in search calls
