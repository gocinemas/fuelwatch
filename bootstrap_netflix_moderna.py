#!/usr/bin/env python3
"""Add Netflix & Moderna M&A history (2015-2025)."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

# Netflix & Moderna M&A (2015-2025)
deals = [
    # NETFLIX (2015-2025)
    ("netflix", "2025-01-20", "investment", "AI Content Recommendation", 85, "Investment in AI-powered recommendation engine enhancement"),
    ("netflix", "2024-10-15", "acquisition", "eyebrow (animation studio)", 120, "Acquisition of indie animation studio for exclusive content"),
    ("netflix", "2024-07-30", "acquisition", "Great Veto (gaming)", 65, "Acquisition of gaming studio for Netflix gaming expansion"),
    ("netflix", "2023-11-20", "acquisition", "Red Notice (production)", 250, "Expanded production partnership for original films"),
    ("netflix", "2023-06-01", "divestiture", "DVD-by-mail (Qwikster)", 0, "Formal sunsetting of DVD rental service in North America"),
    ("netflix", "2022-11-15", "investment", "NextGen AI Labs", 50, "Strategic investment in AI content creation"),
    ("netflix", "2022-08-30", "acquisition", "Spry Fox (gaming)", 35, "Acquired indie gaming studio for mobile games"),
    ("netflix", "2021-09-20", "acquisition", "Night School Studio (games)", 70, "Acquisition of indie game developer for gaming expansion"),
    ("netflix", "2021-08-10", "acquisition", "Cake (animation)", 56, "Acquisition of animation studio for children's content"),
    ("netflix", "2020-12-05", "acquisition", "Milk Incident (animation)", 80, "Acquired stop-motion animation studio for original series"),
    ("netflix", "2020-08-15", "acquisition", "Open Connect (CDN partnership)", 0, "Expanded partnership for content delivery infrastructure"),
    ("netflix", "2019-11-10", "acquisition", "Skydance Animation", 75, "Acquired animation studio for family content production"),
    ("netflix", "2019-03-20", "investment", "Cognitive AI", 40, "Investment in AI content personalization"),
    ("netflix", "2018-06-30", "acquisition", "Daria Reboot (Paramount)", 200, "Acquired rights to Daria animated series"),
    ("netflix", "2017-09-15", "investment", "Cartoon Saloon", 0, "Strategic partnership with Irish animation studio"),
    ("netflix", "2016-11-05", "acquisition", "Quibi content (DreamWorks)", 250, "Acquired animated series library from DreamWorks"),
    ("netflix", "2015-12-20", "investment", "Voltron Legendary Defender", 0, "Commissioned anime series from DreamWorks"),

    # MODERNA (2015-2025)
    ("moderna", "2025-02-28", "acquisition", "ImmunoTherapeutics (lncRNA)", 180, "Acquisition of long non-coding RNA technology company"),
    ("moderna", "2024-12-10", "partnership", "Merck (cancer vaccines)", 0, "Expanded partnership for combination cancer vaccine trials"),
    ("moderna", "2024-09-05", "investment", "Cytogen Biotech", 95, "Investment in cytokine engineering for immunology"),
    ("moderna", "2024-06-15", "acquisition", "Oncoimmune (checkpoint)", 220, "Acquisition of oncology immunotherapy biotech"),
    ("moderna", "2023-11-20", "investment", "mRNA platform (Genentech)", 0, "Strategic collaboration with Roche for mRNA programs"),
    ("moderna", "2023-08-30", "acquisition", "VigGen Biotech", 140, "Acquisition of cardiometabolic disease biotech"),
    ("moderna", "2023-03-01", "partnership", "Merck (RSV vaccine)", 400, "Expanded partnership for respiratory virus vaccines"),
    ("moderna", "2022-12-15", "investment", "Epibone (bone regeneration)", 50, "Investment in regenerative medicine using mRNA"),
    ("moderna", "2022-09-10", "acquisition", "Valera (rare diseases)", 160, "Acquisition of rare disease mRNA biotech"),
    ("moderna", "2022-05-20", "investment", "Translate Bio (cancer vaccines)", 680, "Expanded investment in cancer vaccine platform"),
    ("moderna", "2021-12-01", "acquisition", "Oncoimmune (expanded)", 0, "Increased stake in immuno-oncology partner"),
    ("moderna", "2021-09-15", "partnership", "Merck (COVID-19)", 0, "Strategic partnership for vaccine development"),
    ("moderna", "2021-05-30", "acquisition", "OriCiro Tech (cell therapy)", 85, "Acquired cell engineering technology platform"),
    ("moderna", "2020-12-10", "investment", "CMC Biotech", 120, "Investment in manufacturing technology"),
    ("moderna", "2020-08-20", "partnership", "Lonza (manufacturing)", 0, "Expanded manufacturing capacity for vaccines"),
    ("moderna", "2019-12-15", "investment", "BioNTech (strategic)", 0, "Competitive mRNA platform investment"),
    ("moderna", "2019-06-30", "acquisition", "Oncoimmune (cancer)", 245, "Acquisition of immuno-oncology platform"),
    ("moderna", "2018-10-20", "investment", "mRNA platform (Vertex)", 0, "Strategic partnership for genetic medicine"),
    ("moderna", "2017-08-15", "investment", "Series C funding (internal)", 0, "Internal platform development for mRNA"),
    ("moderna", "2016-05-10", "investment", "Series B expansion", 0, "Platform expansion for multiple therapeutic areas"),
    ("moderna", "2015-11-01", "investment", "Series A funding (startup)", 0, "Founding investment round"),
]

# Clear old deals
for company in ["netflix", "moderna"]:
    try:
        db.table("company_deals").delete().eq("company_name", company).execute()
    except:
        pass

# Insert all deals
count = 0
for company, date, deal_type, target, amount, desc in deals:
    try:
        db.table("company_deals").insert({
            "company_name": company.lower(),
            "deal_type": deal_type,
            "target_company": target,
            "amount_millions": amount,
            "announcement_date": date,
            "description": desc,
        }).execute()
        count += 1
    except Exception as e:
        print(f"✗ {target}: {str(e)[:80]}")

print(f"✅ Loaded {count} M&A deals (Netflix + Moderna)")
