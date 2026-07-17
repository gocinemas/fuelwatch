-- Create receipts table for storing receipt/spending data
-- This table stores receipt information extracted from PDF uploads and manual entry
CREATE TABLE IF NOT EXISTS public.receipts (
  id BIGSERIAL PRIMARY KEY,
  phone TEXT NOT NULL,
  merchant TEXT,
  total NUMERIC(10, 2),
  shop_date TEXT,
  items JSONB DEFAULT '[]',
  raw_summary TEXT,
  category TEXT DEFAULT 'Other',
  restaurant_type TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_receipts_phone_created ON public.receipts(phone, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_receipts_phone_merchant ON public.receipts(phone, merchant);

-- Grant permissions for authenticated and anonymous users
GRANT ALL ON public.receipts TO authenticated;
GRANT ALL ON public.receipts TO anon;
