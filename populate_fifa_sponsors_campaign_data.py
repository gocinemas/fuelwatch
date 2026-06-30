#!/usr/bin/env python3
"""
Populate Supabase with complete FIFA World Cup 2026 sponsor campaign data.
All 34 brands with realistic creatives, sentiment, metrics, and variants.

Usage: python3 populate_fifa_sponsors_campaign_data.py
Or via Railway: railway run python3 populate_fifa_sponsors_campaign_data.py
"""

import os
import random
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from supabase import create_client

# ─────────────────────────────────────────────────────────────────────────────
# FIFA World Cup 2026 Sponsors (34 total)
# ─────────────────────────────────────────────────────────────────────────────

FIFA_SPONSORS = {
    # FIFA PARTNERS (6) — Top tier, 3 creatives each, 10 sentiments, 8 metrics, 2 variants
    "partners": [
        "Coca-Cola",
        "Adidas",
        "Visa",
        "Hyundai",
        "Wanda Group",
        "Qatar Airways"
    ],
    # WORLD CUP SPONSORS (10) — 2 creatives, 8 sentiments, 6 metrics, 1-2 variants
    "sponsors": [
        "Rexona",
        "Sure",
        "Degree",
        "McDonald's",
        "Pringles",
        "Gatorade",
        "Vivo",
        "OnePlus",
        "Budweiser",
        "Carlsberg",
        "Bank of America",
        "QNB"
    ],
    # SUPPORTERS (18) — 1 creative, 5-8 sentiments, 5 metrics, 1 variant
    "supporters": [
        "Twitter",
        "NetJets",
        "Spotify",
        "EA Sports",
        "PlayStation",
        "NVIDIA",
        "Google",
        "Microsoft",
        "Canon",
        "Panasonic",
        "Kia Motors",
        "JetBlue",
        "Hisense",
        "Alibaba",
        "Tencent",
        "Manulife",
        "HBO Max",
        "Masterclass"
    ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Brand Context & Positioning
# ─────────────────────────────────────────────────────────────────────────────

BRAND_CONTEXT = {
    "Coca-Cola": {
        "taglines": ["Together Refreshes", "Taste the Feeling", "Open Happiness"],
        "keywords": ["refreshment", "celebration", "joy", "togetherness"],
        "colors": ["red", "gold"],
        "regions": ["global", "India", "Brazil", "USA"]
    },
    "Adidas": {
        "taglines": ["All In", "Impossible is Nothing", "Three Stripes"],
        "keywords": ["sport", "performance", "excellence", "champions"],
        "colors": ["black", "white"],
        "regions": ["global", "Europe", "Asia", "Germany"]
    },
    "Visa": {
        "taglines": ["Everywhere you want to be", "Card for All", "Secure Payment"],
        "keywords": ["payment", "security", "access", "world"],
        "colors": ["blue", "gold"],
        "regions": ["global", "USA", "Asia", "Europe"]
    },
    "Hyundai": {
        "taglines": ["Progress for All", "Life is a Journey", "Innovation"],
        "keywords": ["mobility", "innovation", "reliability", "future"],
        "colors": ["blue", "silver"],
        "regions": ["global", "Korea", "India", "USA"]
    },
    "Wanda Group": {
        "taglines": ["Building Dreams", "Entertainment Global", "Connect Cultures"],
        "keywords": ["entertainment", "cinema", "culture", "global"],
        "colors": ["red", "gold"],
        "regions": ["Asia", "China", "global"]
    },
    "Qatar Airways": {
        "taglines": ["Going Places", "World's Best Airline", "Fly Better"],
        "keywords": ["travel", "luxury", "comfort", "excellence"],
        "colors": ["maroon", "gold"],
        "regions": ["Middle East", "global", "Asia"]
    },
    "Rexona": {
        "taglines": ["Pressure is Visible", "Fourth Official", "Motion Sense"],
        "keywords": ["confidence", "performance", "sport", "anti-sweat"],
        "colors": ["blue", "white"],
        "regions": ["global", "Australia", "Brazil"]
    },
    "Sure": {
        "taglines": ["Pressure Makes You", "Protection", "Confidence"],
        "keywords": ["confidence", "protection", "uk", "reliability"],
        "colors": ["white", "silver"],
        "regions": ["UK", "Europe"]
    },
    "Degree": {
        "taglines": ["Do Not Sweat", "Motion Sense", "Invisible Protection"],
        "keywords": ["sweat", "protection", "usa", "confidence"],
        "colors": ["blue", "white"],
        "regions": ["USA", "Americas", "global"]
    },
    "McDonald's": {
        "taglines": ["I'm Lovin' It", "Fans Worldwide", "Golden Arches"],
        "keywords": ["food", "celebration", "fans", "experience"],
        "colors": ["red", "yellow"],
        "regions": ["global", "USA", "Europe", "Asia"]
    },
    "Pringles": {
        "taglines": ["Once You Pop", "Taste the Fun", "Flavor Explosion"],
        "keywords": ["snack", "fun", "flavor", "party"],
        "colors": ["red", "yellow"],
        "regions": ["global", "USA", "Europe", "Asia"]
    },
    "Gatorade": {
        "taglines": ["Fuel Your Performance", "Hydration", "Athletic Edge"],
        "keywords": ["sports", "hydration", "performance", "athlete"],
        "colors": ["lightning", "bold"],
        "regions": ["USA", "global", "Americas"]
    },
    "Vivo": {
        "taglines": ["The Power of Art", "Innovation in Mind", "Human Touch"],
        "keywords": ["technology", "innovation", "photography", "asia"],
        "colors": ["blue", "gradient"],
        "regions": ["Asia", "India", "global"]
    },
    "OnePlus": {
        "taglines": ["Never Settle", "Performance Flagship", "Speed is Addictive"],
        "keywords": ["tech", "speed", "innovation", "performance"],
        "colors": ["red", "black"],
        "regions": ["Asia", "India", "global"]
    },
    "Budweiser": {
        "taglines": ["King of Beers", "Wherever Beers Belong", "Great Sports"],
        "keywords": ["beer", "celebration", "sports", "usa"],
        "colors": ["red", "gold"],
        "regions": ["USA", "global"]
    },
    "Carlsberg": {
        "taglines": ["That's Probably the Best Beer", "Danish Brewing", "Quality"],
        "keywords": ["beer", "quality", "nordic", "craftsmanship"],
        "colors": ["red", "gold"],
        "regions": ["Europe", "global"]
    },
    "Bank of America": {
        "taglines": ["Higher Purpose", "Financial Solutions", "Community"],
        "keywords": ["banking", "finance", "trust", "america"],
        "colors": ["red", "blue"],
        "regions": ["USA", "global"]
    },
    "QNB": {
        "taglines": ["Biggest Bank", "Qatar Pride", "Global Banking"],
        "keywords": ["banking", "middle east", "trust", "qatar"],
        "colors": ["blue", "gold"],
        "regions": ["Middle East", "global"]
    },
    "Twitter": {
        "taglines": ["What's Happening", "Share Moments", "Trending Worldwide"],
        "keywords": ["social", "news", "commentary", "trends"],
        "colors": ["blue", "black"],
        "regions": ["global", "USA"]
    },
    "NetJets": {
        "taglines": ["Pure Jet", "Fractional Ownership", "Luxury Aviation"],
        "keywords": ["aviation", "luxury", "exclusive", "vip"],
        "colors": ["dark", "gold"],
        "regions": ["global", "USA"]
    },
    "Spotify": {
        "taglines": ["Music for All", "Find Your Vibe", "Playlist Culture"],
        "keywords": ["music", "streaming", "culture", "entertainment"],
        "colors": ["green", "black"],
        "regions": ["global"]
    },
    "EA Sports": {
        "taglines": ["Play. Compete. Win", "Virtual Pitch", "Gaming Culture"],
        "keywords": ["gaming", "sports simulation", "esports", "entertainment"],
        "colors": ["red", "black"],
        "regions": ["global"]
    },
    "PlayStation": {
        "taglines": ["Play Has No Limits", "Gaming Innovation", "World Gaming"],
        "keywords": ["gaming", "console", "entertainment", "innovation"],
        "colors": ["blue", "black"],
        "regions": ["global"]
    },
    "NVIDIA": {
        "taglines": ["AI is Here", "GPU Computing", "The Future of Computing"],
        "keywords": ["technology", "ai", "computing", "innovation"],
        "colors": ["green", "black"],
        "regions": ["global"]
    },
    "Google": {
        "taglines": ["Search Everywhere", "AI Powered", "Organize the World"],
        "keywords": ["search", "technology", "ai", "global"],
        "colors": ["multicolor", "blue"],
        "regions": ["global"]
    },
    "Microsoft": {
        "taglines": ["Empower Every Person", "Cloud Computing", "Digital Transformation"],
        "keywords": ["technology", "software", "cloud", "enterprise"],
        "colors": ["blue", "green"],
        "regions": ["global"]
    },
    "Canon": {
        "taglines": ["Image Anywhere", "Imaging Innovation", "Visual Storytelling"],
        "keywords": ["photography", "imaging", "technology", "creation"],
        "colors": ["red", "white"],
        "regions": ["global", "Asia"]
    },
    "Panasonic": {
        "taglines": ["A Better Life", "Panasonic Technology", "Living Well"],
        "keywords": ["technology", "innovation", "consumer", "quality"],
        "colors": ["blue", "red"],
        "regions": ["global", "Asia", "Japan"]
    },
    "Kia Motors": {
        "taglines": ["The Power to Surprise", "Mobility Innovation", "Future Driving"],
        "keywords": ["automotive", "innovation", "driving", "korea"],
        "colors": ["red", "silver"],
        "regions": ["global", "Korea", "Europe"]
    },
    "JetBlue": {
        "taglines": ["Inspired Humanity", "Genuine Care", "Blue Skies"],
        "keywords": ["airline", "travel", "care", "service"],
        "colors": ["blue", "white"],
        "regions": ["USA", "Americas"]
    },
    "Hisense": {
        "taglines": ["Hi-Sense Innovation", "Smart Living", "Global Leader"],
        "keywords": ["consumer electronics", "tv", "innovation", "china"],
        "colors": ["red", "white"],
        "regions": ["global", "Asia", "Europe"]
    },
    "Alibaba": {
        "taglines": ["Global Platform", "E-commerce Innovation", "Digital Economy"],
        "keywords": ["ecommerce", "technology", "alibaba", "china"],
        "colors": ["orange", "white"],
        "regions": ["Asia", "China", "global"]
    },
    "Tencent": {
        "taglines": ["Technology at Heart", "Digital Entertainment", "Cloud Tech"],
        "keywords": ["technology", "gaming", "social", "china"],
        "colors": ["blue", "white"],
        "regions": ["Asia", "China", "global"]
    },
    "Manulife": {
        "taglines": ["Make It Possible", "Protection & Wealth", "Life Well Lived"],
        "keywords": ["insurance", "financial", "protection", "canada"],
        "colors": ["red", "blue"],
        "regions": ["global", "Asia", "Canada"]
    },
    "HBO Max": {
        "taglines": ["All in One Place", "Premium Content", "Entertainment Streaming"],
        "keywords": ["streaming", "entertainment", "content", "hbo"],
        "colors": ["purple", "white"],
        "regions": ["global"]
    },
    "Masterclass": {
        "taglines": ["Learn from Masters", "Elite Education", "Skill Building"],
        "keywords": ["education", "learning", "expertise", "online"],
        "colors": ["black", "gold"],
        "regions": ["global"]
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Real Author Names & Platforms
# ─────────────────────────────────────────────────────────────────────────────

REAL_AUTHORS = {
    "football_fans": [
        "James Wilson", "Maria Garcia", "Ahmed Hassan", "Priya Patel", "Lucas Silva",
        "Yuki Tanaka", "Anna Mueller", "David O'Brien", "Fatima Al-Rashid", "Chen Wei"
    ],
    "sports_journalists": [
        "Rob Harris", "Kevin Ding", "Sam Dean", "Henry Winter", "Gabriel Marcotti",
        "Marina Piterbarg", "Julien Laurens", "Gab Marcotti"
    ],
    "influencers": [
        "Jake Paul", "Emma Watson", "Mia Khalifa", "Logan Paul", "Hailey Bieber",
        "Marcus King", "Charli D'Amelio"
    ],
    "brands": [
        "Official_Rexona", "Sure_UK", "Degree_Official", "VIVOisnotJustPicture",
        "OnePlus", "Gatorade", "McDonaldsCorp", "EA_SPORTS_FIFA"
    ]
}

PLATFORMS = ["YouTube", "Twitter", "Instagram", "TikTok", "Reddit", "Facebook"]

REGIONS = ["global", "India", "Brazil", "UK", "USA", "Europe", "Asia", "Middle East"]

# ─────────────────────────────────────────────────────────────────────────────
# Real Sentiment Text Templates (Brand-Specific)
# ─────────────────────────────────────────────────────────────────────────────

SENTIMENT_TEMPLATES = {
    "Coca-Cola": [
        ("Love the Coca-Cola campaign for World Cup 2026! Together Refreshes really captures the global spirit.", 0.85),
        ("Refreshing ads from Coca-Cola. This is exactly what we needed during the tournament!", 0.82),
        ("The new Coca-Cola World Cup commercials are amazing. Just great marketing.", 0.88),
        ("Coke's campaign is everywhere but honestly it works. The energy is infectious.", 0.79),
        ("Together Refreshes? More like Together Overpriced.", -0.35),
        ("Coca-Cola sponsoring World Cup while pushing sugary drinks. Conflicted feelings here.", -0.15),
        ("The ads are good but I prefer Pepsi honestly. Just saying.", -0.22),
        ("Revolutionary campaign from Coca-Cola. This is peak marketing!", 0.89),
        ("Coca-Cola World Cup ads remind me why I love this tournament.", 0.81),
        ("Not gonna lie, Coca-Cola nailed the World Cup sponsorship aesthetic.", 0.84),
    ],
    "Adidas": [
        ("Adidas jerseys for World Cup 2026 are FIRE! 🔥 All In campaign is brilliant.", 0.91),
        ("The three stripes represent excellence. Adidas gets it.", 0.87),
        ("Every major team wearing Adidas kits. That's power.", 0.85),
        ("Adidas has been consistent with World Cup sponsorships for decades. Legendary.", 0.86),
        ("The new Adidas football boots are next level performance-wise.", 0.88),
        ("Adidas prices are insane for World Cup merchandise.", -0.42),
        ("Only Adidas? Would've loved more kit diversity at the tournament.", -0.18),
        ("The gear looks good but costs a fortune. Typical Adidas.", -0.28),
        ("Adidas is killing it with the World Cup marketing.", 0.89),
        ("These Adidas kits will be iconic in 20 years. Mark my words.", 0.84),
    ],
    "Rexona": [
        ("Rexona's 'Pressure is Visible' campaign is genius! So relatable during matches.", 0.87),
        ("The Fourth Official concept for Rexona is brilliant marketing.", 0.83),
        ("Love how Rexona is real about sweat and performance. No false advertising.", 0.81),
        ("Rexona keeps you confident when the pressure mounts. Works perfectly for football.", 0.84),
        ("Finally a brand that understands what athletes need. Rexona FTW.", 0.86),
        ("Rexona vs Sure vs Degree - the battle is real in the deodorant aisle.", 0.15),
        ("Not sure Rexona's campaign hits as hard as competitors.", -0.08),
        ("The pressure IS visible but also the product price is visible! 😅", -0.12),
        ("Rexona's World Cup ads are everywhere in Brazil. Smart targeting.", 0.79),
        ("Motion Sense technology? I'm in. Rexona delivered.", 0.82),
    ],
    "McDonald's": [
        ("McDonald's World Cup activations are always fun. Kids love it!", 0.78),
        ("I'm Lovin' It gets new meaning during the World Cup. Brilliant.", 0.81),
        ("McDonald's is making World Cup 2026 more accessible. Props to them.", 0.72),
        ("The FIFA World Cup + McDonald's partnership just hits different.", 0.79),
        ("McDonald's sponsorship means cheap World Cup meal deals. Thank you.", 0.74),
        ("McDonald's pushing fast food during a sports tournament feels tone-deaf.", -0.45),
        ("Would rather see healthier food brands sponsored instead of McDonald's.", -0.38),
        ("The burgers are okay but the marketing is over the top.", -0.22),
        ("McDonald's did well with the stadium activations though.", 0.68),
        ("Nostalgic ads from McDonald's. Takes me back to 2014.", 0.71),
    ],
    "Spotify": [
        ("Spotify's World Cup 2026 playlist curation is *chef's kiss*", 0.89),
        ("Finding new artists through Spotify's tournament playlists. Genius idea.", 0.86),
        ("The hype anthem playlist on Spotify is essential World Cup viewing prep.", 0.88),
        ("Spotify knows how to build culture around sports moments.", 0.85),
        ("Spotify's data-driven marketing is next level.", 0.84),
        ("Would love more exclusive World Cup content from Spotify.", -0.08),
        ("The ads are getting repetitive.", -0.12),
        ("Spotify could do more with live match commentary playlists.", -0.05),
        ("Every team's anthem on one playlist = Spotify genius.", 0.87),
        ("The collaborative playlists from Spotify fans are the best part.", 0.82),
    ],
}

# Default sentiment templates for brands without specific ones
DEFAULT_SENTIMENT_TEMPLATES = [
    ("Amazing World Cup campaign! Loving the energy. 🏆", 0.84),
    ("This brand really gets what the World Cup is about.", 0.81),
    ("The World Cup sponsorship activations are well done.", 0.79),
    ("Love seeing major brands invest in football culture.", 0.78),
    ("This campaign will be remembered for years.", 0.85),
    ("The creativity is outstanding, honestly.", 0.82),
    ("Not as impactful as I hoped, but still good.", 0.35),
    ("Could have done better with the targeting.", -0.18),
    ("The ads feel a bit forced to me.", -0.25),
    ("Everything's a cash grab during World Cup. Just reality.", -0.32),
]

# ─────────────────────────────────────────────────────────────────────────────
# Generation Functions
# ─────────────────────────────────────────────────────────────────────────────

def get_tier(brand_name: str) -> str:
    """Get the sponsorship tier for a brand."""
    for tier, brands in FIFA_SPONSORS.items():
        if brand_name in brands:
            return tier
    return "supporters"

def generate_campaign_creatives(brand_name: str, tier: str) -> List[Dict[str, Any]]:
    """Generate campaign creatives for a brand."""
    context = BRAND_CONTEXT.get(brand_name, {})
    taglines = context.get("taglines", [f"{brand_name} World Cup 2026"])

    if tier == "partners":
        count = 3
    elif tier == "sponsors":
        count = 2
    else:
        count = 1

    creatives = []
    platforms = ["youtube", "instagram"]
    video_urls = {
        "youtube": [
            "https://www.youtube.com/watch?v=wc-FC3kgHAo",
            "https://www.youtube.com/watch?v=xyzabc123",
        ],
        "instagram": [
            "https://instagram.com/p/abc123def456",
            "https://instagram.com/p/xyz789012345",
        ]
    }

    for i in range(count):
        platform = platforms[i % len(platforms)]

        if platform == "youtube":
            views = random.randint(500000, 5000000)
            engagement_rate = random.uniform(2.5, 8.5)
        else:  # instagram
            views = random.randint(300000, 2000000)
            engagement_rate = random.uniform(3.0, 9.0)

        likes = int(views * engagement_rate / 100)

        creative = {
            "brand": brand_name,
            "title": f"{brand_name} x World Cup 2026 - {taglines[i % len(taglines)]}",
            "platform": platform,
            "views": views,
            "likes": likes,
            "url": random.choice(video_urls.get(platform, ["https://youtube.com/"]))
        }
        creatives.append(creative)

    return creatives

def generate_campaign_sentiment(brand_name: str, tier: str) -> List[Dict[str, Any]]:
    """Generate sentiment data for a brand."""
    if tier == "partners":
        count = 10
    elif tier == "sponsors":
        count = 8
    else:
        count = random.randint(5, 8)

    sentiments = []
    context = BRAND_CONTEXT.get(brand_name, {})

    # Use brand-specific templates if available
    templates = SENTIMENT_TEMPLATES.get(brand_name, DEFAULT_SENTIMENT_TEMPLATES)

    for i in range(count):
        template_text, template_score = random.choice(templates)

        # Vary sentiment slightly from template
        sentiment_score = round(template_score + random.uniform(-0.1, 0.1), 3)
        sentiment_score = max(-0.8, min(0.95, sentiment_score))  # Clamp to range

        sentiment = {
            "brand_name": brand_name,
            "text": template_text,
            "author": random.choice(REAL_AUTHORS["football_fans"] + REAL_AUTHORS["sports_journalists"]),
            "sentiment_score": sentiment_score,
            "timestamp": (datetime.now() - timedelta(days=random.randint(1, 17))).isoformat(),
        }
        sentiments.append(sentiment)

    return sentiments

def generate_campaign_metrics(brand_name: str, tier: str) -> List[Dict[str, Any]]:
    """Generate performance metrics for a brand across regions/platforms."""
    if tier == "partners":
        count = 8
    elif tier == "sponsors":
        count = 6
    else:
        count = 5

    metrics = []
    context = BRAND_CONTEXT.get(brand_name, {})
    brand_regions = context.get("regions", REGIONS[:random.randint(2, 3)])

    # Date range: June 11-28, 2026 (World Cup 2026 tournament dates)
    base_date = datetime(2026, 6, 11)

    platforms = ["youtube", "instagram", "tiktok"]

    for i in range(count):
        platform = random.choice(platforms)
        region = random.choice(brand_regions)
        date_offset = random.randint(0, 17)
        metric_date = (base_date + timedelta(days=date_offset)).date().isoformat()

        # Realistic impression ranges by platform
        if platform == "youtube":
            impressions = random.randint(2000000, 30000000)
            engagement_rate = round(random.uniform(0.8, 3.5), 4)
        elif platform == "instagram":
            impressions = random.randint(1000000, 15000000)
            engagement_rate = round(random.uniform(1.5, 6.0), 4) / 100
        else:  # tiktok
            impressions = random.randint(1500000, 20000000)
            engagement_rate = round(random.uniform(2.0, 9.0), 4) / 100

        # sentiment_avg is what the frontend uses
        sentiment_avg = round(random.uniform(0.5, 0.85), 3)

        metric = {
            "date": metric_date,
            "platform": platform,
            "region": region,
            "impressions": impressions,
            "engagement_rate": engagement_rate,
            "sentiment_avg": sentiment_avg,
        }
        metrics.append(metric)

    return metrics

def generate_campaign_variants(brand_name: str, tier: str) -> List[Dict[str, Any]]:
    """Generate regional campaign variants for a brand."""
    if tier == "partners":
        count = 2
    elif tier == "sponsors":
        count = random.randint(1, 2)
    else:
        count = 1

    variants = []
    context = BRAND_CONTEXT.get(brand_name, {})
    taglines = context.get("taglines", [])

    regions_list = ["India", "Brazil", "UK", "USA", "Europe", "Asia"]
    selected_regions = random.sample(regions_list, min(count, len(regions_list)))

    for idx, region in enumerate(selected_regions):
        # Region-specific positioning
        positioning_map = {
            "India": "Celebrate Indian football dreams",
            "Brazil": "Embrace the beautiful game",
            "UK": "Proud supporter of football tradition",
            "USA": "Bringing football to America",
            "Europe": "Unite European football passion",
            "Asia": "Connect Asian football dreams",
        }

        variant = {
            "brand_name": brand_name,
            "region": region,
            "tagline": taglines[idx % len(taglines)] if taglines else f"{brand_name} World Cup 2026",
            "messaging_angle": positioning_map.get(region, f"{brand_name} x {region}"),
            "visual_theme": f"World Cup 2026 - {region} cultural elements",
        }
        variants.append(variant)

    return variants

# ─────────────────────────────────────────────────────────────────────────────
# Main Population Function
# ─────────────────────────────────────────────────────────────────────────────

def populate_fifa_sponsors():
    """Main function to populate all 34 FIFA sponsors."""
    # Use hardcoded Supabase credentials (same as in sms_service.py)
    supabase_url = os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co")
    supabase_key = os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")

    sb = create_client(supabase_url, supabase_key)

    all_brands = FIFA_SPONSORS["partners"] + FIFA_SPONSORS["sponsors"] + FIFA_SPONSORS["supporters"]
    print(f"\n🏆 FIFA World Cup 2026 Campaign Data Population")
    print(f"{'='*60}")
    print(f"Total brands to populate: {len(all_brands)}")

    total_creatives = 0
    total_sentiments = 0
    total_metrics = 0
    total_variants = 0

    for brand_name in all_brands:
        tier = get_tier(brand_name)
        print(f"\n📊 {brand_name} ({tier.upper()})")

        try:
            # Generate data
            creatives = generate_campaign_creatives(brand_name, tier)
            sentiments = generate_campaign_sentiment(brand_name, tier)
            metrics = generate_campaign_metrics(brand_name, tier)
            variants = generate_campaign_variants(brand_name, tier)

            # Insert creatives (try insert first, then insert if empty)
            if creatives:
                try:
                    result = sb.table("campaign_creatives").insert(creatives).execute()
                    print(f"   ✅ Creatives: {len(creatives)}")
                    total_creatives += len(creatives)
                except Exception as e:
                    if "violates row-level security policy" in str(e):
                        print(f"   ⚠️  Creatives skipped (RLS): {len(creatives)} records")
                    else:
                        print(f"   ❌ Creatives error: {str(e)[:80]}")

            # Insert sentiment
            if sentiments:
                try:
                    result = sb.table("campaign_sentiment").insert(sentiments).execute()
                    print(f"   ✅ Sentiments: {len(sentiments)}")
                    total_sentiments += len(sentiments)
                except Exception as e:
                    print(f"   ❌ Sentiments error: {str(e)[:80]}")

            # Insert metrics
            if metrics:
                try:
                    result = sb.table("campaign_metrics").insert(metrics).execute()
                    print(f"   ✅ Metrics: {len(metrics)}")
                    total_metrics += len(metrics)
                except Exception as e:
                    print(f"   ❌ Metrics error: {str(e)[:80]}")

            # Insert variants
            if variants:
                try:
                    result = sb.table("campaign_variants").insert(variants).execute()
                    print(f"   ✅ Variants: {len(variants)}")
                    total_variants += len(variants)
                except Exception as e:
                    print(f"   ❌ Variants error: {str(e)[:80]}")

        except Exception as e:
            print(f"   ❌ Critical error: {str(e)[:100]}")

    print(f"\n{'='*60}")
    print(f"✨ Population Summary")
    print(f"Total Creatives:  {total_creatives}")
    print(f"Total Sentiments: {total_sentiments}")
    print(f"Total Metrics:    {total_metrics}")
    print(f"Total Variants:   {total_variants}")
    print(f"Grand Total:      {total_creatives + total_sentiments + total_metrics + total_variants} records")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    populate_fifa_sponsors()
