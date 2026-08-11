-- Create company_documents table for storing links, notes, PDFs, and analysis
CREATE TABLE IF NOT EXISTS public.company_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name TEXT NOT NULL,
    doc_type TEXT NOT NULL CHECK (doc_type IN ('link', 'note', 'pdf', 'analysis')),
    title TEXT NOT NULL,
    content TEXT,
    url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for faster lookups by company
CREATE INDEX IF NOT EXISTS idx_company_documents_company_name ON public.company_documents(company_name);

-- Create index for sorting by date
CREATE INDEX IF NOT EXISTS idx_company_documents_created_at ON public.company_documents(created_at DESC);

-- Enable RLS (Row Level Security) - allow public read/write for now
ALTER TABLE public.company_documents ENABLE ROW LEVEL SECURITY;

-- Allow all operations (demo mode)
CREATE POLICY "Allow all operations" ON public.company_documents
    FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- Add comment
COMMENT ON TABLE public.company_documents IS 'Stores reference links, notes, PDFs, and analysis for companies in Intel';
COMMENT ON COLUMN public.company_documents.doc_type IS 'Type of document: link, note, pdf, or analysis';
COMMENT ON COLUMN public.company_documents.url IS 'URL for link-type documents';
COMMENT ON COLUMN public.company_documents.content IS 'Text content for note, pdf, or analysis types';
