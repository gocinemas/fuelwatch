-- Create company_hiring_focus table for AI investment and hiring strategy tracking
CREATE TABLE IF NOT EXISTS public.company_hiring_focus (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name TEXT NOT NULL UNIQUE,
    hiring_growth_2025 FLOAT,
    ai_investment_score INT CHECK (ai_investment_score >= 0 AND ai_investment_score <= 5),
    strategic_direction TEXT,
    focus_areas JSONB, -- Array of {area, growth, roles, reason}
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for company lookup
CREATE INDEX IF NOT EXISTS idx_hiring_focus_company ON public.company_hiring_focus(company_name);

-- Enable RLS
ALTER TABLE public.company_hiring_focus ENABLE ROW LEVEL SECURITY;

-- Allow all operations
CREATE POLICY "Allow all operations" ON public.company_hiring_focus
    FOR ALL USING (TRUE) WITH CHECK (TRUE);

-- Add comments
COMMENT ON TABLE public.company_hiring_focus IS 'Tracks hiring growth, AI investment, and strategic focus areas for companies';
COMMENT ON COLUMN public.company_hiring_focus.ai_investment_score IS 'Scale 1-5: 1=Low, 5=Aggressive AI investment';
COMMENT ON COLUMN public.company_hiring_focus.focus_areas IS 'JSON array of {area, growth%, roles, reason}';
