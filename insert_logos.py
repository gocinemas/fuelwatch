#!/usr/bin/env python3
import sys
sys.path.insert(0, '/Users/srevi/fuelwatch')
from library import _sb

sb = _sb()

# Brand logos from Wikimedia Commons & reliable CDNs
# Format: (brand_name, logo_url)
logos = [
    # BEVERAGES
    ('Red Bull', 'https://upload.wikimedia.org/wikipedia/commons/thumb/4/46/Red_Bull_Logo.svg/200px-Red_Bull_Logo.svg.png'),
    ('Monster Energy', 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/5e/Monster_Energy_logo.svg/200px-Monster_Energy_logo.svg.png'),
    ('PepsiCo', 'https://upload.wikimedia.org/wikipedia/commons/thumb/7/79/PepsiCo_logo.svg/200px-PepsiCo_logo.svg.png'),
    ('Sprite', 'https://upload.wikimedia.org/wikipedia/commons/thumb/b/b6/Sprite_logo.svg/200px-Sprite_logo.svg.png'),
    ('Gatorade', 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/e5/Gatorade.svg/200px-Gatorade.svg.png'),
    ('Tropicana', 'https://upload.wikimedia.org/wikipedia/commons/thumb/4/41/Tropicana_logo.svg/200px-Tropicana_logo.svg.png'),
    ('Nescafé', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a7/Nescafe_logo.svg/200px-Nescafe_logo.svg.png'),
    ('Danone', 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/eb/Danone_2009_logo.svg/200px-Danone_2009_logo.svg.png'),
    ('Fiji Water', 'https://upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Fiji_Water_logo.svg/200px-Fiji_Water_logo.svg.png'),
    ('Perrier', 'https://upload.wikimedia.org/wikipedia/commons/thumb/b/b5/Perrier_logo.svg/200px-Perrier_logo.svg.png'),

    # SNACKS
    ('Mars Inc', 'https://upload.wikimedia.org/wikipedia/commons/thumb/7/73/Mars%2C_Incorporated_logo.svg/200px-Mars%2C_Incorporated_logo.svg.png'),
    ('Nestlé Confectionery', 'https://upload.wikimedia.org/wikipedia/commons/thumb/7/78/Nestl%C3%A9.svg/200px-Nestl%C3%A9.svg.png'),
    ('Mondelēz', 'https://upload.wikimedia.org/wikipedia/commons/thumb/b/b5/Mondelez_International_2012_logo.svg/200px-Mondelez_International_2012_logo.svg.png'),
    ('Ferrero', 'https://upload.wikimedia.org/wikipedia/commons/thumb/8/85/Ferrero_SpA_logo.svg/200px-Ferrero_SpA_logo.svg.png'),
    ('Lindt', 'https://upload.wikimedia.org/wikipedia/commons/thumb/6/6e/Lindt_%26_Spr%C3%BCngli_logo.svg/200px-Lindt_%26_Spr%C3%BCngli_logo.svg.png'),
    ('Haribo', 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/e3/Haribo_logo.svg/200px-Haribo_logo.svg.png'),
    ('Cadbury', 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/06/Cadbury_logo.svg/200px-Cadbury_logo.svg.png'),
    ('Hershey', 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/The_Hershey_Company_logo.svg/200px-The_Hershey_Company_logo.svg.png'),
    ('Lay''s', 'https://upload.wikimedia.org/wikipedia/commons/thumb/c/c1/Lay%27s_logo.svg/200px-Lay%27s_logo.svg.png'),
    ('Doritos', 'https://upload.wikimedia.org/wikipedia/commons/thumb/6/63/Doritos_logo.svg/200px-Doritos_logo.svg.png'),

    # PERSONAL CARE
    ('Olay', 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Olay_logo.svg/200px-Olay_logo.svg.png'),
    ('Gillette', 'https://upload.wikimedia.org/wikipedia/commons/thumb/4/4c/Gillette_logo.svg/200px-Gillette_logo.svg.png'),
    ('Dove', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a2/Dove_logo.svg/200px-Dove_logo.svg.png'),
    ('L''Oréal', 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0f/LOr%C3%A9al_logo.svg/200px-LOr%C3%A9al_logo.svg.png'),
    ('Estée Lauder', 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Estee_Lauder_Companies_logo.svg/200px-Estee_Lauder_Companies_logo.svg.png'),
    ('Revlon', 'https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/Revlon_logo.svg/200px-Revlon_logo.svg.png'),
    ('CoverGirl', 'https://upload.wikimedia.org/wikipedia/commons/thumb/9/99/Covergirl_logo.svg/200px-Covergirl_logo.svg.png'),
    ('Maybelline', 'https://upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Maybelline_logo.svg/200px-Maybelline_logo.svg.png'),
    ('Neutrogena', 'https://upload.wikimedia.org/wikipedia/commons/thumb/7/7c/Neutrogena_logo.svg/200px-Neutrogena_logo.svg.png'),
    ('Clinique', 'https://upload.wikimedia.org/wikipedia/commons/thumb/7/78/Clinique_logo.svg/200px-Clinique_logo.svg.png'),

    # HOUSEHOLD
    ('Tide', 'https://upload.wikimedia.org/wikipedia/commons/thumb/4/4a/Tide_logo.svg/200px-Tide_logo.svg.png'),
    ('Dawn', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a9/Dawn_%28brand%29_logo.svg/200px-Dawn_%28brand%29_logo.svg.png'),
    ('Lysol', 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Lysol_logo.svg/200px-Lysol_logo.svg.png'),
    ('Clorox', 'https://upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Clorox_logo.svg/200px-Clorox_logo.svg.png'),
    ('Colgate', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a6/Colgate_palmolive_logo.svg/200px-Colgate_palmolive_logo.svg.png'),
    ('SC Johnson', 'https://upload.wikimedia.org/wikipedia/commons/thumb/4/40/SC_Johnson_logo.svg/200px-SC_Johnson_logo.svg.png'),
    ('Henkel', 'https://upload.wikimedia.org/wikipedia/commons/thumb/3/38/Henkel_logo.svg/200px-Henkel_logo.svg.png'),
    ('Method', 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0e/Method_logo.svg/200px-Method_logo.svg.png'),
    ('Ecos', 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f5/Ecos_logo.svg/200px-Ecos_logo.svg.png'),
    ('Surf', 'https://upload.wikimedia.org/wikipedia/commons/thumb/8/8a/Surf_logo.svg/200px-Surf_logo.svg.png'),

    # QSR
    ('McDonald''s', 'https://upload.wikimedia.org/wikipedia/commons/thumb/3/36/McDonald%27s_logo.svg/200px-McDonald%27s_logo.svg.png'),
    ('Subway', 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0b/Subway_logo.svg/200px-Subway_logo.svg.png'),
    ('Domino''s', 'https://upload.wikimedia.org/wikipedia/commons/thumb/9/9a/Domino%27s_pizza_logo.svg/200px-Domino%27s_pizza_logo.svg.png'),
    ('KFC', 'https://upload.wikimedia.org/wikipedia/commons/thumb/b/b7/KFC_logo.svg/200px-KFC_logo.svg.png'),
    ('Chipotle', 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/e9/Chipotle_logo.svg/200px-Chipotle_logo.svg.png'),
    ('Wendy''s', 'https://upload.wikimedia.org/wikipedia/commons/thumb/9/99/Wendy%27s_logo.svg/200px-Wendy%27s_logo.svg.png'),
    ('Burger King', 'https://upload.wikimedia.org/wikipedia/commons/thumb/4/43/Burger_King_logo.svg/200px-Burger_King_logo.svg.png'),
    ('Taco Bell', 'https://upload.wikimedia.org/wikipedia/commons/thumb/8/80/Taco_Bell_logo.svg/200px-Taco_Bell_logo.svg.png'),
    ('Pizza Hut', 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f5/Pizza_Hut_logo.svg/200px-Pizza_Hut_logo.svg.png'),
    ('Panera Bread', 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Panera_Bread_logo.svg/200px-Panera_Bread_logo.svg.png'),

    # PACKAGED FOOD
    ('Kraft Heinz', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a0/Kraft_Heinz_logo.svg/200px-Kraft_Heinz_logo.svg.png'),
    ('General Mills', 'https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/General_Mills_logo.svg/200px-General_Mills_logo.svg.png'),
    ('Kellogg''s', 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/e6/Kellogg%27s_logo.svg/200px-Kellogg%27s_logo.svg.png'),
    ('Nestlé Food', 'https://upload.wikimedia.org/wikipedia/commons/thumb/7/78/Nestl%C3%A9.svg/200px-Nestl%C3%A9.svg.png'),
    ('Lactalis', 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a4/Lactalis_logo.svg/200px-Lactalis_logo.svg.png'),
    ('ConAgra', 'https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/ConAgra_Foods_logo.svg/200px-ConAgra_Foods_logo.svg.png'),
    ('Campbell Soup', 'https://upload.wikimedia.org/wikipedia/commons/thumb/c/cc/Campbell_Soup_Company_logo.svg/200px-Campbell_Soup_Company_logo.svg.png'),
    ('Unilever Food', 'https://upload.wikimedia.org/wikipedia/commons/thumb/3/3f/Unilever_logo.svg/200px-Unilever_logo.svg.png'),
    ('Tyson Foods', 'https://upload.wikimedia.org/wikipedia/commons/thumb/8/83/Tyson_Foods_logo.svg/200px-Tyson_Foods_logo.svg.png'),
    ('Ben & Jerry''s', 'https://upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Ben_%26_Jerry%27s_logo.svg/200px-Ben_%26_Jerry%27s_logo.svg.png'),
]

print(f"Updating logos for {len(logos)} brands...\n")

updated = 0
for brand_name, logo_url in logos:
    try:
        # Update existing brand with logo_url
        sb.table('brand_profile').update({
            'logo_url': logo_url
        }).eq('name', brand_name).execute()
        updated += 1
        print(f"  ✓ {brand_name}")
    except Exception as e:
        print(f"  ✗ {brand_name}: {str(e)[:60]}")

print(f"\n✅ Updated {updated} logos out of {len(logos)} brands")
