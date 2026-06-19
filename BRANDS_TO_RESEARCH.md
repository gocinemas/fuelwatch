# 100 Brands Research List
## Phase 1b: Expansion Dataset

**Structure:** 25 skincare + 25 beverages × 3 markets (UK, USA, India) = 150 records

---

## SKINCARE (25 brands)

### Already Have (10) ✅
1. Neutrogena
2. Dove
3. CeraVe
4. Garnier
5. Cetaphil
6. L'Oréal
7. Estée Lauder
8. The Ordinary
9. Olay Regenerist
10. Clinique

### Need to Research (15) 🔍

#### Premium Tier
11. Nykaa (India-native)
12. Himalaya (India-native)
13. MAC (Makeup)
14. Revlon (Color cosmetics)
15. Lancôme (Luxury)
16. SK-II (Ultra-premium)
17. Shiseido (Premium Asian)

#### Mid-market Tier
18. Simple (Natural/sensitive)
19. Eucerin (Derma)
20. Avon (Direct-to-consumer)
21. Oriflame (Direct-to-consumer)
22. Tupperware (Multi-level)
23. Herbalife (Wellness)
24. Amway (Multi-level)

#### Budget Tier
25. Ponds (India classic)

---

## BEVERAGES (25 brands)

### Already Have (10) ✅
1. Pepsi
2. Sprite
3. Fanta
4. Monster Energy
5. Mountain Dew
6. Thums Up (India)
7. Limca (India)
8. Perrier (Premium water)
9. Tropicana (Juice)
10. Minute Maid (Juice)

### Need to Research (15) 🔍

#### Cola/Soft Drinks
11. Coca-Cola (Global leader)
12. Red Bull (Energy)
13. 7UP (Lemon-lime)
14. Fanta Orange (variant focus)
15. Sting (Asia energy)

#### Premium Water/Sparkling
16. San Pellegrino (Sparkling water)
17. Topo Chico (Sparkling water)
18. Fiji Water (Premium water)

#### Juice/Functional
19. Real (Fruit juice - India)
20. Appy Fizz (Juice drink - India)
21. Maaza (Mango juice - India)
22. Ocean Spray (Cranberry)

#### Tea/Coffee/Other
23. Nestlé Pure Life (Water brand)
24. Bisleri (Water - India)
25. Gatorade (Sports drink)

---

## Research Template for Each Brand

For each brand × market combination, gather:

```json
{
  "brand_name": "Brand Name",
  "category": "skincare" or "beverages",
  "market_country": "UK" or "USA" or "India",
  
  "fundamentals": {
    "founded_year": 1950,
    "headquarters_city": "City",
    "headquarters_country": "Country",
    "official_website": "website.com",
    "parent_company": "Parent Corp"
  },
  
  "positioning": {
    "positioning_tier": "economy|mass-market|mass-prestige|premium|luxury",
    "direct_competitor_1": "Brand A",
    "direct_competitor_2": "Brand B",
    "direct_competitor_3": "Brand C",
    "positioning_summary": "Brief positioning statement"
  },
  
  "segment": {
    "target_demographic": "Women 30-50, urban",
    "target_income_tier": "lower-middle|middle|upper-middle|affluent|high",
    "segment_size_millions": 15
  },
  
  "pricing": {
    "price_local": 5.99,
    "price_currency": "GBP" or "USD" or "INR",
    "ppp_index": 1.0 or 0.25
  },
  
  "market": {
    "category_growth_cagr_3yr": 3.5,
    "market_status": "mature|emerging|high_growth",
    "growth_driver": "premiumization, online retail"
  },
  
  "distribution": {
    "distribution_channels": ["channel1", "channel2"],
    "distribution_strategy": "mass_market|selective|exclusive"
  },
  
  "marketing": {
    "marketing_channels": ["TV", "digital", "social"],
    "marketing_tone": "scientific|playful|luxury|natural"
  }
}
```

---

## Data Sources

- **Fundamentals:** Wikipedia, Wikidata, company websites
- **Positioning:** Brand websites, industry reports
- **Pricing:** Retailer websites (Tesco, Amazon, Flipkart, Nykaa)
- **Market Data:** Statista, Euromonitor, World Bank
- **Distribution:** Retailer listings, brand official channels
- **Marketing:** Brand social media, ads, press releases

---

## Priority Order

1. **Coca-Cola, Red Bull** (beverage category leaders)
2. **MAC, Revlon** (makeup competition)
3. **Himalaya, Nykaa** (India-specific dynamics)
4. **Gatorade, Ocean Spray** (functional beverages)
5. Rest of list

---

## Status Tracking

- [ ] Coca-Cola (UK, USA, India)
- [ ] Red Bull (UK, USA, India)
- [ ] MAC (UK, USA, India)
- [ ] Revlon (UK, USA, India)
- [ ] Himalaya (UK, USA, India)
- [ ] Nykaa (UK, USA, India)
- [ ] Gatorade (UK, USA, India)
- [ ] Ocean Spray (UK, USA, India)
- [ ] ... (rest of 25 brands)

**Target:** Research + insert 50 additional brands by [DATE]
