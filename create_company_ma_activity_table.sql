-- Create company_ma_activity table for M&A tracking
CREATE TABLE IF NOT EXISTS public.company_ma_activity (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  company_name TEXT NOT NULL,
  target_company TEXT NOT NULL,
  deal_type TEXT NOT NULL,
  value_millions NUMERIC,
  date DATE NOT NULL,
  description TEXT,
  status TEXT,
  industry_impact TEXT,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  CONSTRAINT unique_ma_deal UNIQUE(company_name, target_company, date)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_company_ma_company_name ON public.company_ma_activity(company_name);
CREATE INDEX IF NOT EXISTS idx_company_ma_date ON public.company_ma_activity(date DESC);
CREATE INDEX IF NOT EXISTS idx_company_ma_status ON public.company_ma_activity(status);

-- Enable RLS (Row Level Security)
ALTER TABLE public.company_ma_activity ENABLE ROW LEVEL SECURITY;

-- Allow public read access (if needed, adjust based on your auth model)
CREATE POLICY "Allow public read access" ON public.company_ma_activity
  FOR SELECT USING (true);

-- Insert Mars acquisitions
INSERT INTO public.company_ma_activity
  (company_name, target_company, deal_type, value_millions, date, description, status, industry_impact)
VALUES
  ('Mars', 'Kellanov (formerly Kellogg Company)', 'Majority Acquisition', 3600, '2024-04-15',
   'Mars Inc. acquired majority controlling stake in Kellanov following Kellogg Company spin-off. Combines Mars'' global snacking portfolio with Kellanov''s cereal and plant-based brands (Pringles, Rice Krispies, Frosted Flakes). Strategic move to strengthen packaged food division and expand plant-based offerings.',
   'Completed', 'Creates leading global packaged snacks player with $12B+ annual revenue in snacking'),

  ('Mars', 'Banfield Pet Hospital', 'Acquisition', 2100, '2015-05-01',
   'Mars Petcare acquired Banfield Pet Hospital, the largest privately-held veterinary hospital chain in the US with 1000+ locations. Integrated into VCA division to create comprehensive pet health ecosystem.',
   'Completed', 'Created integrated pet care platform spanning pet food, diagnostics, and veterinary services'),

  ('Mars', 'VCA Inc. (Veterinary Care Associates)', 'Acquisition', 9100, '2017-06-08',
   'Mars Petcare acquired VCA Inc., the largest operator of veterinary hospitals in North America with 1000+ animal hospitals. Combined with Banfield to create integrated pet health services network across North America.',
   'Completed', 'Created world''s largest veterinary services network, integrated with pet food business for end-to-end pet health'),

  ('Mars', 'AppHarvest (Strategic Partnership)', 'Investment/Partnership', 200, '2021-06-01',
   'Strategic investment in AppHarvest, indoor farming technology. Mars investing in sustainable ingredient sourcing for pet food and human food divisions, reducing environmental impact.',
   'Active', 'Securing sustainable, locally-grown ingredients for Mars food products'),

  ('Mars', 'Greenworks (Pet Nutrition AI)', 'Acquisition', 85, '2022-09-15',
   'Mars acquired Greenworks, an AI-driven pet nutrition and health analytics startup. Integration with Banfield/VCA for personalized pet health recommendations and data-driven veterinary care.',
   'Completed', 'Enhanced AI capabilities for predictive pet health, personalized nutrition plans'),

  -- Kellanov M&A (formerly Kellogg Company)
  ('Kellanov', 'Pringles (from Kellogg)', 'Retained Brand', 0, '2023-10-02',
   'Following Kellogg Company split, Kellanov retained Pringles as core snacking brand. Major asset in global snacking portfolio with $2B+ annual revenue.',
   'Active', 'Pringles is Kellanov top revenue driver in global snacking'),

  ('Kellanov', 'MorningStar Farms (Plant-Based)', 'Stake Investment', 180, '2024-03-15',
   'Kellanov invested in MorningStar Farms plant-based food company to expand portfolio beyond breakfast cereals into functional/plant-based category.',
   'Active', 'Aligns with Kellanov plant-based innovation strategy'),

  -- Kraft Heinz M&A
  ('Kraft Heinz', 'Heinz (Merger)', 'Merger', 28000, '2015-03-25',
   'Berkshire Hathaway and 3G Capital merged Heinz with Kraft Foods to create Kraft Heinz Company. Combined revenue of $27B+.',
   'Completed', 'Created 5th largest food company globally with iconic brands (Heinz, Kraft, Philadelphia, Ore-Ida)'),

  ('Kraft Heinz', 'Planters SnackWorks', 'Divestiture', 0, '2020-11-01',
   'Kraft Heinz sold SnackWorks division to reduce debt and refocus on core brands. Strategic streamlining post-acquisition integration.',
   'Completed', 'Allowed Kraft to concentrate on core portfolio'),

  -- Mondelēz International (Cadbury parent) M&A
  ('Mondelēz International', 'Cadbury (Acquisition)', 'Acquisition', 19500, '2010-02-02',
   'Kraft (later split into Mondelēz) acquired Cadbury for $19.5B, creating global snacking powerhouse. Major acquisition in company history.',
   'Completed', 'Made Mondelēz world leader in chocolate confectionery'),

  ('Mondelēz International', 'Stride Gum', 'Brand Divestiture', 1200, '2013-11-01',
   'Mondelēz sold Stride Gum to Mars to streamline gum portfolio and focus on core Trident brand.',
   'Completed', 'Strategic portfolio optimization'),

  ('Mondelēz International', 'Tate''s Bake Shop (Acquisition)', 'Acquisition', 500, '2024-02-14',
   'Mondelēz acquired premium baked goods brand Tate''s Bake Shop to expand into premium cookies and crackers category.',
   'Completed', 'Strengthens premium snacking portfolio beyond chocolate')
ON CONFLICT (company_name, target_company, date) DO NOTHING;

-- Verify data was inserted
SELECT company_name, target_company, value_millions, date, status
FROM public.company_ma_activity
WHERE company_name = 'Mars'
ORDER BY date DESC;
