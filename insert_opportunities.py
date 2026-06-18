#!/usr/bin/env python3
import sys
sys.path.insert(0, '/Users/srevi/fuelwatch')
from library import _sb

sb = _sb()

opportunities = [
    ('Red Bull', 'market_gap', 'Budget Energy ($1-2 segment)', 'Energy drink market dominated by premium. Mintel data shows 34% consumer interest in "affordable energy".', '$8.2B', 7.8, 'Red Bull dominates premium; Coca-Cola Energy gap at budget.', 'Launch budget line via discount retailers (Aldi, Costco).', 'Mintel Energy Drink Report 2024'),
    ('Red Bull', 'growth_adjacency', 'Functional Energy (adaptogenic)', 'McKinsey: Functional beverage growing 18% YoY. Adaptogens (ashwagandha, rhodiola) emerging with premium positioning.', '$3.1B', 8.2, 'Adaptogenic category emerging with Gen-Z willing to pay 40% premium for wellness.', 'Develop Red Bull Vitality (adaptogenic) at $4.99 for wellness retailers.', 'McKinsey Functional Beverages 2024'),
    ('Red Bull', 'growth_adjacency', 'Asian Market Juice Premiumization', 'Euromonitor: Asian juice market growing 22% CAGR. Middle-class expansion driving premium demand.', '$12.4B', 7.5, 'Asia premium juice willing to pay 3x markup. Distribution exists.', 'Launch Fiji Juice (exotic fruit blends) at premium tier.', 'Euromonitor Asia Juice Market 2024'),
    ('Olay', 'market_gap', 'Clinical DTC Personalized Skincare', 'HBR: Beauty DTC disruptors growing 40% CAGR. Traditional incumbents losing share.', '$7.8B', 8.3, 'Opportunity to own clinical DTC positioning.', 'Launch Olay+ digital with AI skin assessment + subscription at $49/mo.', 'HBR Beauty DTC Disruption 2024'),
    ('Olay', 'growth_adjacency', 'Male Skincare (beyond shaving)', 'Euromonitor: Male skincare growing 18% CAGR globally, 28% in Asia.', '$4.2B', 8.0, 'Male consumers perceive skincare as feminine; opportunity to destigmatize.', 'Develop Olay Men Expert Premium at $35/50ml for fitness/wellness.', 'Euromonitor Male Skincare 2024'),
    ('Olay', 'growth_adjacency', 'Clean Beauty Premium Tier', 'McKinsey: Clean beauty growing 35% CAGR. Consumers pay 2-3x for clean formulations.', '$3.5B', 7.7, 'Mass-market perception conflicts with clean; create premium sub-brand.', 'Launch Olay Pure (clean, dermatologist-tested, vegan) at $24/oz.', 'McKinsey Clean Beauty 2024'),
]

inserted = 0
for brand, gap_type, gap_name, description, market_size, score, why, recommendation, source in opportunities:
    try:
        sb.table('brand_market_opportunities').insert({
            'brand_name': brand,
            'gap_type': gap_type,
            'gap_name': gap_name,
            'gap_description': description,
            'market_size': market_size,
            'opportunity_score': score,
            'why_opportunity': why,
            'brand_recommendation': recommendation,
            'source_reference': source
        }).execute()
        inserted += 1
        print(f"✓ {brand}: {gap_name}")
    except Exception as e:
        print(f"✗ {brand}: {str(e)[:80]}")

print(f"\n✅ Inserted {inserted} opportunities")
