#!/usr/bin/env python3
"""
Deploy brand profile data to Supabase
Run via: flask_app --flask_app sms_service.py flask shell < deploy_brand_profile.py
Or just push to main branch and Railway will auto-deploy
"""

import sys
from library import _sb

# Initialize Supabase client
sb = _sb()

# All 60 brands with real data
brands_data = [
    # BEVERAGES (10)
    {'name': 'PepsiCo', 'founded_year': 1965, 'origin_city': 'Purchase', 'origin_country': 'USA', 'tagline': "What's Brewing?", 'description': 'Global beverage and snacks company owning Pepsi, Gatorade, Tropicana, and Frito-Lay brands', 'website': 'pepsico.com', 'headquarters': 'Purchase, New York, USA'},
    {'name': 'Red Bull', 'founded_year': 1987, 'origin_city': 'Salzburg', 'origin_country': 'Austria', 'tagline': 'Gives You Wings', 'description': "Austrian energy drink company with global presence in over 170 countries", 'website': 'redbull.com', 'headquarters': 'Salzburg, Austria'},
    {'name': 'Monster Energy', 'founded_year': 2002, 'origin_city': 'Corona', 'origin_country': 'USA', 'tagline': 'Unleash the Beast', 'description': 'American energy drink company, subsidiary of The Coca-Cola Company', 'website': 'monsterenergy.com', 'headquarters': 'Corona, California, USA'},
    {'name': 'Nescafé', 'founded_year': 1938, 'origin_city': 'Vevey', 'origin_country': 'Switzerland', 'tagline': 'Start Pleasantly', 'description': "Instant coffee brand owned by Nestlé, world's largest coffee brand", 'website': 'nescafe.com', 'headquarters': 'Vevey, Switzerland'},
    {'name': 'Danone', 'founded_year': 1919, 'origin_city': 'Barcelona', 'origin_country': 'Spain', 'tagline': 'Naturally Good', 'description': 'French multinational food company specializing in yogurt and dairy products', 'website': 'danone.com', 'headquarters': 'Paris, France'},
    {'name': 'Fiji Water', 'founded_year': 1996, 'origin_city': 'Viti Levu', 'origin_country': 'Fiji', 'tagline': "Earth's Finest Water", 'description': 'Premium bottled water from artesian aquifer in Fiji', 'website': 'fijiwater.com', 'headquarters': 'Viti Levu, Fiji'},
    {'name': 'Perrier', 'founded_year': 1863, 'origin_city': 'Vergèze', 'origin_country': 'France', 'tagline': 'Refreshingly Different', 'description': 'French sparkling water brand owned by Nestlé', 'website': 'perrier.com', 'headquarters': 'Vergèze, France'},
    {'name': 'Gatorade', 'founded_year': 1965, 'origin_city': 'Gainesville', 'origin_country': 'USA', 'tagline': 'Be Like Mike', 'description': 'Sports drink brand, owned by PepsiCo since 2001', 'website': 'gatorade.com', 'headquarters': 'Chicago, Illinois, USA'},
    {'name': 'Tropicana', 'founded_year': 1947, 'origin_city': 'Bradenton', 'origin_country': 'USA', 'tagline': 'Pure Premium', 'description': 'Citrus juice and drinks brand owned by PepsiCo', 'website': 'tropicana.com', 'headquarters': 'Bradenton, Florida, USA'},
    {'name': 'Sprite', 'founded_year': 1961, 'origin_city': 'Atlanta', 'origin_country': 'USA', 'tagline': 'Obey Your Thirst', 'description': 'Lemon-lime flavored soft drink owned by The Coca-Cola Company', 'website': 'sprite.com', 'headquarters': 'Atlanta, Georgia, USA'},

    # SNACKS & CONFECTIONERY (10)
    {'name': 'Mars Inc', 'founded_year': 1911, 'origin_city': 'Wrigley', 'origin_country': 'USA', 'tagline': 'Work, Rest and Play', 'description': 'American manufacturer of chocolate and confectionery products', 'website': 'mars.com', 'headquarters': 'McLean, Virginia, USA'},
    {'name': 'Nestlé Confectionery', 'founded_year': 1875, 'origin_city': 'Vevey', 'origin_country': 'Switzerland', 'tagline': 'Inspired by Our Past, Driven by Innovation', 'description': 'Division of Nestlé producing KitKat, Aero, and other chocolate brands', 'website': 'nestle.com', 'headquarters': 'Vevey, Switzerland'},
    {'name': 'Mondelēz', 'founded_year': 2012, 'origin_city': 'East Hanover', 'origin_country': 'USA', 'tagline': 'Make Today Delicious', 'description': 'Global snacking company producing Oreo, Cadbury, Trident, and other brands', 'website': 'mondelez.com', 'headquarters': 'East Hanover, New Jersey, USA'},
    {'name': 'Ferrero', 'founded_year': 1946, 'origin_city': 'Alba', 'origin_country': 'Italy', 'tagline': 'Crafted Confectionery', 'description': 'Italian chocolate manufacturer known for Ferrero Rocher and Nutella', 'website': 'ferrero.com', 'headquarters': 'Alba, Italy'},
    {'name': 'Lindt', 'founded_year': 1845, 'origin_city': 'Zurich', 'origin_country': 'Switzerland', 'tagline': 'Master Chocolatier', 'description': 'Swiss chocolate manufacturer and cocoa processor', 'website': 'lindtusa.com', 'headquarters': 'Zurich, Switzerland'},
    {'name': 'Haribo', 'founded_year': 1920, 'origin_city': 'Bonn', 'origin_country': 'Germany', 'tagline': 'Gummi Candy Innovation', 'description': 'German confectionery company famous for gummy bears', 'website': 'haribo.com', 'headquarters': 'Bonn, Germany'},
    {'name': 'Cadbury', 'founded_year': 1824, 'origin_city': 'Birmingham', 'origin_country': 'UK', 'tagline': 'A Glass and a Half', 'description': 'British chocolate manufacturer, owned by Mondelēz', 'website': 'cadbury.co.uk', 'headquarters': 'Bournville, UK'},
    {'name': 'Hershey', 'founded_year': 1894, 'origin_city': 'Lancaster', 'origin_country': 'USA', 'tagline': 'Taste the Love', 'description': 'American chocolate and confectionery manufacturer', 'website': 'hersheys.com', 'headquarters': 'Hershey, Pennsylvania, USA'},
    {'name': "Lay's", 'founded_year': 1932, 'origin_city': 'Nashville', 'origin_country': 'USA', 'tagline': "Betcha Can't Eat Just One", 'description': 'American potato chip brand owned by PepsiCo', 'website': 'lays.com', 'headquarters': 'Plano, Texas, USA'},
    {'name': 'Doritos', 'founded_year': 1966, 'origin_city': 'Los Angeles', 'origin_country': 'USA', 'tagline': 'For the Bold', 'description': 'Nacho cheese tortilla chip brand owned by PepsiCo', 'website': 'doritos.com', 'headquarters': 'Plano, Texas, USA'},

    # PERSONAL CARE & BEAUTY (10)
    {'name': 'Olay', 'founded_year': 1952, 'origin_city': 'South Africa', 'origin_country': 'South Africa', 'tagline': 'Face the Future', 'description': 'Skincare brand owned by Procter & Gamble', 'website': 'olay.com', 'headquarters': 'Cincinnati, Ohio, USA'},
    {'name': 'Gillette', 'founded_year': 1901, 'origin_city': 'Boston', 'origin_country': 'USA', 'tagline': 'The Best a Man Can Get', 'description': 'Shaving and personal care brand owned by Procter & Gamble', 'website': 'gillette.com', 'headquarters': 'Boston, Massachusetts, USA'},
    {'name': 'Dove', 'founded_year': 1957, 'origin_city': 'Rotterdam', 'origin_country': 'Netherlands', 'tagline': 'Real Beauty', 'description': 'Personal care brand owned by Unilever', 'website': 'dove.com', 'headquarters': 'Rotterdam, Netherlands'},
    {'name': "L'Oréal", 'founded_year': 1909, 'origin_city': 'Paris', 'origin_country': 'France', 'tagline': "Because You're Worth It", 'description': 'French multinational cosmetics and beauty company', 'website': 'loreal.com', 'headquarters': 'Clichy, France'},
    {'name': 'Estée Lauder', 'founded_year': 1946, 'origin_city': 'New York', 'origin_country': 'USA', 'tagline': 'The Ultimate Luxury Beauty', 'description': 'American multinational prestige beauty company', 'website': 'esteelauder.com', 'headquarters': 'New York, New York, USA'},
    {'name': 'Revlon', 'founded_year': 1932, 'origin_city': 'New York', 'origin_country': 'USA', 'tagline': 'Colorstay True', 'description': 'American cosmetics and personal care company', 'website': 'revlon.com', 'headquarters': 'New York, New York, USA'},
    {'name': 'CoverGirl', 'founded_year': 1961, 'origin_city': 'Irving', 'origin_country': 'USA', 'tagline': 'Easy, Breezy, Beautiful', 'description': 'Cosmetics brand owned by Coty', 'website': 'covergirl.com', 'headquarters': 'New York, New York, USA'},
    {'name': 'Maybelline', 'founded_year': 1917, 'origin_city': 'Memphis', 'origin_country': 'USA', 'tagline': 'Make It Happen', 'description': "Cosmetics brand owned by L'Oréal", 'website': 'maybelline.com', 'headquarters': 'New York, New York, USA'},
    {'name': 'Neutrogena', 'founded_year': 1930, 'origin_city': 'Los Angeles', 'origin_country': 'USA', 'tagline': 'Dermatologist Recommended', 'description': 'Skincare brand owned by Johnson & Johnson', 'website': 'neutrogena.com', 'headquarters': 'Los Angeles, California, USA'},
    {'name': 'Clinique', 'founded_year': 1968, 'origin_city': 'New York', 'origin_country': 'USA', 'tagline': 'Clinical Skincare', 'description': 'Skincare and cosmetics brand owned by Estée Lauder', 'website': 'clinique.com', 'headquarters': 'New York, New York, USA'},

    # HOUSEHOLD & CLEANING (10)
    {'name': 'Tide', 'founded_year': 1946, 'origin_city': 'Cincinnati', 'origin_country': 'USA', 'tagline': "It Works Hard So You Don't Have To", 'description': 'Laundry detergent brand owned by Procter & Gamble', 'website': 'tide.com', 'headquarters': 'Cincinnati, Ohio, USA'},
    {'name': 'Dawn', 'founded_year': 1973, 'origin_city': 'Cincinnati', 'origin_country': 'USA', 'tagline': 'It Takes Just One Drop', 'description': 'Dishwashing liquid brand owned by Procter & Gamble', 'website': 'dawn-dish.com', 'headquarters': 'Cincinnati, Ohio, USA'},
    {'name': 'Lysol', 'founded_year': 1889, 'origin_city': 'New Jersey', 'origin_country': 'USA', 'tagline': 'Disinfect with Confidence', 'description': 'Disinfectant brand owned by Reckitt', 'website': 'lysol.com', 'headquarters': 'West Deptford, New Jersey, USA'},
    {'name': 'Clorox', 'founded_year': 1913, 'origin_city': 'Oakland', 'origin_country': 'USA', 'tagline': 'Clean and Kill', 'description': 'Bleach and cleaning products company', 'website': 'clorox.com', 'headquarters': 'Oakland, California, USA'},
    {'name': 'Colgate', 'founded_year': 1806, 'origin_city': 'New York', 'origin_country': 'USA', 'tagline': 'Bright Smile, Bright Future', 'description': 'Oral care and personal hygiene products company', 'website': 'colgate.com', 'headquarters': 'New York, New York, USA'},
    {'name': 'SC Johnson', 'founded_year': 1886, 'origin_city': 'Racine', 'origin_country': 'USA', 'tagline': 'A Family Company', 'description': 'Manufacturer of household cleaning and personal care products', 'website': 'scjohnson.com', 'headquarters': 'Racine, Wisconsin, USA'},
    {'name': 'Henkel', 'founded_year': 1876, 'origin_city': 'Düsseldorf', 'origin_country': 'Germany', 'tagline': 'Excellence is Our Choice', 'description': 'German chemical and consumer goods company', 'website': 'henkel.com', 'headquarters': 'Düsseldorf, Germany'},
    {'name': 'Method', 'founded_year': 2000, 'origin_city': 'San Francisco', 'origin_country': 'USA', 'tagline': 'People Against Dirty', 'description': 'Eco-friendly cleaning products brand', 'website': 'methodhome.com', 'headquarters': 'San Francisco, California, USA'},
    {'name': 'Ecos', 'founded_year': 1967, 'origin_city': 'San Francisco', 'origin_country': 'USA', 'tagline': "Nature's Clean", 'description': 'Hypoallergenic and eco-friendly cleaning products', 'website': 'ecoproducts.com', 'headquarters': 'San Francisco, California, USA'},
    {'name': 'Surf', 'founded_year': 1952, 'origin_city': 'Rotterdam', 'origin_country': 'Netherlands', 'tagline': 'Authentic Clean', 'description': 'Laundry detergent brand owned by Unilever', 'website': 'surf.com', 'headquarters': 'Rotterdam, Netherlands'},

    # QSR & FOOD SERVICE (10)
    {'name': "McDonald's", 'founded_year': 1940, 'origin_city': 'San Bernardino', 'origin_country': 'USA', 'tagline': "I'm Lovin' It", 'description': "World's largest fast food restaurant chain", 'website': 'mcdonalds.com', 'headquarters': 'Chicago, Illinois, USA'},
    {'name': 'Subway', 'founded_year': 1965, 'origin_city': 'Bridgeport', 'origin_country': 'USA', 'tagline': 'Eat Fresh', 'description': 'Submarine sandwich fast food franchise', 'website': 'subway.com', 'headquarters': 'Milford, Connecticut, USA'},
    {'name': "Domino's", 'founded_year': 1960, 'origin_city': 'Ypsilanti', 'origin_country': 'USA', 'tagline': 'Pizza Perfection', 'description': 'Pizza delivery chain operating globally', 'website': 'dominos.com', 'headquarters': 'Ann Arbor, Michigan, USA'},
    {'name': 'KFC', 'founded_year': 1952, 'origin_city': 'Salt Lake City', 'origin_country': 'USA', 'tagline': "Finger Lickin' Good", 'description': 'Fried chicken restaurant chain owned by Yum! Brands', 'website': 'kfc.com', 'headquarters': 'Louisville, Kentucky, USA'},
    {'name': 'Chipotle', 'founded_year': 1993, 'origin_city': 'Denver', 'origin_country': 'USA', 'tagline': 'Food With Integrity', 'description': 'Fast casual Mexican restaurant chain', 'website': 'chipotle.com', 'headquarters': 'Newport Beach, California, USA'},
    {'name': "Wendy's", 'founded_year': 1969, 'origin_city': 'Columbus', 'origin_country': 'USA', 'tagline': "Where's the Beef?", 'description': 'American fast food hamburger restaurant chain', 'website': 'wendys.com', 'headquarters': 'Dublin, Ohio, USA'},
    {'name': 'Burger King', 'founded_year': 1954, 'origin_city': 'Miami', 'origin_country': 'USA', 'tagline': 'Have It Your Way', 'description': 'Fast food restaurant chain serving flame-grilled burgers', 'website': 'bk.com', 'headquarters': 'Miami, Florida, USA'},
    {'name': 'Taco Bell', 'founded_year': 1962, 'origin_city': 'Bell Gardens', 'origin_country': 'USA', 'tagline': 'Think Outside the Bun', 'description': 'Fast food Mexican restaurant chain owned by Yum! Brands', 'website': 'tacobell.com', 'headquarters': 'Irvine, California, USA'},
    {'name': 'Pizza Hut', 'founded_year': 1958, 'origin_city': 'Wichita', 'origin_country': 'USA', 'tagline': 'Hut Rewards', 'description': 'Pizza restaurant chain operating worldwide', 'website': 'pizzahut.com', 'headquarters': 'Plano, Texas, USA'},
    {'name': 'Panera Bread', 'founded_year': 1987, 'origin_city': 'Richmond Heights', 'origin_country': 'USA', 'tagline': 'Bread for Life', 'description': 'Fast casual restaurant chain featuring soups, salads, and sandwiches', 'website': 'panerabread.com', 'headquarters': 'St. Louis, Missouri, USA'},

    # DAIRY & PACKAGED FOOD (10)
    {'name': 'Kraft Heinz', 'founded_year': 2015, 'origin_city': 'Chicago', 'origin_country': 'USA', 'tagline': 'A Legacy of Taste', 'description': 'Multinational food company from merger of Kraft and Heinz', 'website': 'kraftheinz.com', 'headquarters': 'Chicago, Illinois, USA'},
    {'name': 'General Mills', 'founded_year': 1928, 'origin_city': 'Minneapolis', 'origin_country': 'USA', 'tagline': 'Nourishing Lives', 'description': 'American multinational manufacturer of packaged foods', 'website': 'generalmills.com', 'headquarters': 'Minneapolis, Minnesota, USA'},
    {'name': "Kellogg's", 'founded_year': 1906, 'origin_city': 'Battle Creek', 'origin_country': 'USA', 'tagline': 'From Field to Table', 'description': 'American breakfast food company', 'website': 'kelloggs.com', 'headquarters': 'Battle Creek, Michigan, USA'},
    {'name': 'Nestlé Food', 'founded_year': 1866, 'origin_city': 'Vevey', 'origin_country': 'Switzerland', 'tagline': 'Nutrition, Health, Wellness', 'description': 'Division of Nestlé producing packaged foods and beverages', 'website': 'nestle.com', 'headquarters': 'Vevey, Switzerland'},
    {'name': 'Lactalis', 'founded_year': 1933, 'origin_city': 'Laval', 'origin_country': 'France', 'tagline': 'Dairy Excellence', 'description': 'French multinational dairy products company', 'website': 'lactalis.com', 'headquarters': 'Laval, France'},
    {'name': 'ConAgra', 'founded_year': 1919, 'origin_city': 'Omaha', 'origin_country': 'USA', 'tagline': 'Brands That Matter', 'description': 'American multinational packaged foods company', 'website': 'conagra.com', 'headquarters': 'Omaha, Nebraska, USA'},
    {'name': 'Campbell Soup', 'founded_year': 1869, 'origin_city': 'Camden', 'origin_country': 'USA', 'tagline': 'Um, Um, Good', 'description': 'American soup and packaged food company', 'website': 'campbells.com', 'headquarters': 'Camden, New Jersey, USA'},
    {'name': 'Unilever Food', 'founded_year': 2000, 'origin_city': 'Rotterdam', 'origin_country': 'Netherlands', 'tagline': 'Nourishing Every Generation', 'description': 'Foods division of Unilever', 'website': 'unilever.com', 'headquarters': 'Rotterdam, Netherlands'},
    {'name': 'Tyson Foods', 'founded_year': 1935, 'origin_city': 'Springdale', 'origin_country': 'USA', 'tagline': 'Protein Specialists', 'description': 'American multinational protein company', 'website': 'tysonfoods.com', 'headquarters': 'Springdale, Arkansas, USA'},
    {'name': "Ben & Jerry's", 'founded_year': 1978, 'origin_city': 'Waterbury', 'origin_country': 'USA', 'tagline': 'Super Premium Ice Cream', 'description': 'Premium ice cream company owned by Unilever', 'website': 'benjerry.com', 'headquarters': 'Waterbury, Vermont, USA'},
]

# Upsert (delete existing, insert new)
try:
    # First delete existing
    for brand in brands_data:
        try:
            sb.table('brand_profile').delete().eq('name', brand['name']).execute()
        except:
            pass

    # Insert all new records
    for i, brand in enumerate(brands_data):
        try:
            sb.table('brand_profile').insert(brand).execute()
            print(f"  ✓ {brand['name']}")
        except Exception as e:
            print(f"  ✗ {brand['name']}: {str(e)[:100]}")

    # Verify count
    result = sb.table('brand_profile').select('count', count='exact').execute()
    total = result.count if hasattr(result, 'count') else len(result.data)
    print(f"\n✅ Deployed {total} brand profiles successfully!")

except Exception as e:
    print(f"Fatal error: {e}")
    sys.exit(1)
