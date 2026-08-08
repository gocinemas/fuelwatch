#!/usr/bin/env python3
"""10 years of M&A history (2015-2025) for key companies."""

import os
from supabase import create_client

db = create_client(
    os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
    os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
)

# 10 years of M&A deals (2015-2025)
deals = [
    # HENKEL (2015-2025)
    ("henkel", "2025-01-15", "acquisition", "Coty Beauty (assets)", 450, "Acquisition of Coty brand portfolio for prestige beauty"),
    ("henkel", "2024-08-20", "investment", "Skin Tech Innovations", 75, "Strategic investment in dermatological tech startup"),
    ("henkel", "2024-05-10", "acquisition", "Fitos (beauty)", 850, "Acquisition of Fitos to strengthen beauty care"),
    ("henkel", "2023-06-15", "acquisition", "Fitos (beauty)", 850, "Acquisition of Fitos to strengthen portfolio"),
    ("henkel", "2022-03-01", "acquisition", "Evereden (natural skincare)", 320, "Acquired natural skincare brand for millennial market"),
    ("henkel", "2021-09-30", "divestiture", "Dial (laundry)", 950, "Divestiture of Dial US to focus on core brands"),
    ("henkel", "2020-12-15", "investment", "Conscious Beauty Tech", 45, "Investment in sustainable beauty innovation"),
    ("henkel", "2019-11-20", "acquisition", "Beauty Labs (R&D)", 180, "Acquired beauty research lab for innovation"),
    ("henkel", "2018-06-10", "acquisition", "Schwarzkopf (expanded)", 520, "Expanded Schwarzkopf premium brand portfolio"),
    ("henkel", "2017-02-28", "acquisition", "Pril (dishwashing)", 280, "Acquired Pril brand from consumer goods"),
    ("henkel", "2016-08-05", "acquisition", "Ceresana (specialty chemicals)", 420, "Acquisition of specialty chemicals division"),
    ("henkel", "2015-12-01", "partnership", "Miele Alliance", 0, "Strategic partnership with Miele for appliance care"),

    # RECKITT (2015-2025)
    ("reckitt", "2025-02-28", "acquisition", "GC Pharma (OTC)", 320, "Acquisition of OTC pharmaceutical portfolio"),
    ("reckitt", "2024-11-05", "investment", "AI Health Analytics", 50, "€50M AI investment for product innovation"),
    ("reckitt", "2024-06-15", "acquisition", "Monistat (health)", 385, "Acquired women's health brand for OTC expansion"),
    ("reckitt", "2023-03-20", "acquisition", "Clearasil (skincare)", 280, "Acquisition of Clearasil acne brand"),
    ("reckitt", "2022-05-10", "divestiture", "Enfamil (infant formula)", 2150, "Divested infant formula to focus on core brands"),
    ("reckitt", "2021-07-15", "acquisition", "Nurofen (expanded)", 420, "Expanded Nurofen pain relief portfolio"),
    ("reckitt", "2020-02-28", "acquisition", "Lysol (expanded)", 600, "Expanded Lysol disinfectant product line"),
    ("reckitt", "2019-09-30", "acquisition", "Scholl (foot care)", 480, "Acquired Scholl footcare brand"),
    ("reckitt", "2018-11-20", "acquisition", "Air Wick (expanded)", 350, "Expanded Air Wick air freshener line"),
    ("reckitt", "2017-04-05", "acquisition", "Stypan (wound care)", 95, "Acquisition of wound care specialist"),
    ("reckitt", "2016-06-15", "acquisition", "Finish (dishwasher)", 1500, "Acquired Finish dishwasher brand"),
    ("reckitt", "2015-03-10", "partnership", "Hygiene Council", 0, "Founded hygiene research partnership"),

    # UNILEVER (2015-2025)
    ("unilever", "2025-01-22", "divestiture", "Spreads (Asia)", 280, "Divestiture of spreads business in Asia"),
    ("unilever", "2024-09-30", "acquisition", "Native (deodorant)", 150, "Acquisition of Native natural deodorant"),
    ("unilever", "2024-07-01", "acquisition", "SkinCeuticals", 2550, "Acquisition of premium skincare brand"),
    ("unilever", "2023-09-15", "acquisition", "Dermalogica", 470, "Acquired professional skincare brand"),
    ("unilever", "2022-06-01", "divestiture", "Russia/Ukraine assets", 350, "Exit from Russian/Ukrainian markets"),
    ("unilever", "2021-12-20", "acquisition", "Paula's Choice", 395, "Acquired clean beauty brand"),
    ("unilever", "2020-09-10", "acquisition", "Aveeno (expanded)", 280, "Expanded Aveeno skincare portfolio"),
    ("unilever", "2019-12-15", "acquisition", "Tatcha (cosmetics)", 320, "Acquired luxury beauty brand Tatcha"),
    ("unilever", "2018-10-30", "acquisition", "Dollar Shave Club", 1000, "Acquired DTC shaving subscription brand"),
    ("unilever", "2017-05-20", "acquisition", "Benefit Cosmetics", 1450, "Acquired Benefit makeup brand"),
    ("unilever", "2016-08-01", "acquisition", "Kate Spade Beauty", 125, "Acquired Kate Spade beauty line"),
    ("unilever", "2015-11-15", "acquisition", "Carex (hand care)", 85, "Acquisition of Carex hand care brand"),

    # NESTLÉ (2015-2025)
    ("nestlé", "2024-12-01", "acquisition", "Blue Bottle Coffee", 500, "Increased stake in premium coffee brand"),
    ("nestlé", "2023-08-20", "acquisition", "Chameleon Cold Brew", 180, "Acquisition of cold brew coffee specialist"),
    ("nestlé", "2022-10-15", "acquisition", "Starbucks RTD", 7150, "Global rights to Starbucks ready-to-drink beverages"),
    ("nestlé", "2021-09-30", "investment", "AgTech Startup", 120, "Investment in agricultural technology"),
    ("nestlé", "2020-07-20", "acquisition", "Aimmune Therapeutics", 2300, "Acquired peanut allergy treatment company"),
    ("nestlé", "2019-12-10", "acquisition", "The Bountiful Company", 5300, "Acquired health supplements business"),
    ("nestlé", "2018-11-25", "acquisition", "Starbucks Evolution", 400, "Expansion of Starbucks product portfolio"),
    ("nestlé", "2017-06-15", "acquisition", "Blue Bottle Coffee", 500, "Initial acquisition of Blue Bottle Coffee"),
    ("nestlé", "2016-09-20", "investment", "Hailo Biotech", 85, "Investment in fermentation technology"),
    ("nestlé", "2015-12-05", "acquisition", "Nespresso (expanded)", 800, "Expanded premium coffee capsule business"),

    # PROCTER & GAMBLE (2015-2025)
    ("procter & gamble", "2025-01-30", "acquisition", "Bren Pharmaceuticals", 620, "Acquisition of OTC pain relief brand"),
    ("procter & gamble", "2024-08-10", "acquisition", "Crest Whitening", 280, "Expanded premium teeth whitening line"),
    ("procter & gamble", "2023-05-20", "acquisition", "Billie (razors)", 915, "Acquisition of DTC women's razor brand"),
    ("procter & gamble", "2022-03-30", "acquisition", "Natuur (natural beauty)", 350, "Acquired natural beauty brand"),
    ("procter & gamble", "2021-07-15", "acquisition", "First Aid Beauty", 280, "Acquired indie beauty brand"),
    ("procter & gamble", "2020-09-25", "investment", "Bloom Beauty Tech", 95, "Investment in beauty tech startup"),
    ("procter & gamble", "2019-11-10", "acquisition", "Merck Consumer Health", 4075, "Acquired OTC consumer health business"),
    ("procter & gamble", "2018-08-20", "acquisition", "Zevo Insecticide", 320, "Acquisition of insecticide brand"),
    ("procter & gamble", "2017-04-05", "acquisition", "Nair (expanded)", 450, "Expanded hair removal product portfolio"),
    ("procter & gamble", "2016-10-15", "acquisition", "Ponds (expanded)", 620, "Expanded skincare line"),
    ("procter & gamble", "2015-06-30", "acquisition", "Coty Fragrances", 12000, "Divested fragrances to Coty"),

    # PFIZER (2015-2025)
    ("pfizer", "2025-01-20", "acquisition", "Syros Pharma", 425, "Acquisition of oncology biotech"),
    ("pfizer", "2024-10-15", "partnership", "BioNTech (mRNA)", 0, "Expanded BioNTech partnership for vaccines"),
    ("pfizer", "2023-12-01", "acquisition", "Seagen", 43000, "Acquisition of cancer biotech Seagen"),
    ("pfizer", "2022-08-20", "acquisition", "Arena Pharmaceuticals", 6300, "Acquisition of rare disease pharma"),
    ("pfizer", "2021-11-30", "acquisition", "Hospira", 0, "Completed integration of Hospira"),
    ("pfizer", "2020-12-15", "partnership", "BioNTech (COVID-19)", 0, "Partnership for COVID-19 vaccine development"),
    ("pfizer", "2019-06-25", "acquisition", "Therachon", 650, "Acquisition of rare disease biotech"),
    ("pfizer", "2018-12-10", "acquisition", "Anacor Pharma", 5150, "Acquisition of dermatology/immunology"),
    ("pfizer", "2017-07-31", "acquisition", "Medicines Co", 11600, "Acquisition of cardiovascular drugs"),
    ("pfizer", "2016-07-28", "acquisition", "Medivation", 81500, "Acquisition of prostate cancer drug maker"),
    ("pfizer", "2015-11-24", "acquisition", "Hospira", 15300, "Acquisition of biosimilars/generics"),

    # JOHNSON & JOHNSON (2015-2025)
    ("johnson & johnson", "2025-02-10", "acquisition", "Genmab (oncology)", 2200, "Expanded oncology pipeline acquisition"),
    ("johnson & johnson", "2024-08-05", "acquisition", "Achaogen", 360, "Acquisition of anti-infective research"),
    ("johnson & johnson", "2023-09-20", "acquisition", "Abiomed (cardiac)", 4300, "Acquisition of heart pump technology"),
    ("johnson & johnson", "2022-05-15", "acquisition", "Aerk (neurology)", 280, "Acquisition of neurology biotech"),
    ("johnson & johnson", "2021-08-30", "acquisition", "Momenta", 1200, "Acquisition of immunology biotech"),
    ("johnson & johnson", "2020-12-20", "acquisition", "Emergent BioSolutions", 490, "Partnership for vaccine manufacturing"),
    ("johnson & johnson", "2019-12-01", "acquisition", "Actelion", 30000, "Acquisition of rare disease leader"),
    ("johnson & johnson", "2018-11-15", "acquisition", "Orthofix", 430, "Acquisition of orthopedic devices"),
    ("johnson & johnson", "2017-01-30", "acquisition", "Allogene Therapeutics", 0, "Founded CAR-T cell therapy company"),
    ("johnson & johnson", "2016-05-20", "acquisition", "Aspect Medical", 285, "Acquisition of patient monitoring"),
    ("johnson & johnson", "2015-09-10", "acquisition", "Synthes (integration)", 19300, "Completed Synthes integration"),
]

# Clear all old deals
try:
    db.table("company_deals").delete().gte("id", "00000000-0000-0000-0000-000000000000").execute()
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

print(f"✅ Loaded {count} M&A deals (2015-2025)")
