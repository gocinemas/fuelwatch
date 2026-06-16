#!/usr/bin/env python3
"""
Intel Scheduler - Runs daily scraping agents
Scheduled via Railway cron or APScheduler
"""

from scraping_agents import (
    scrape_amazon_skus,
    scrape_tesco_skus,
    store_scraped_skus_in_supabase,
    fetch_company_news,
    track_competitor_skus,
    fetch_earnings_call_transcript
)
from datetime import datetime
import library as lib
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Brands to track
TRACKED_BRANDS = {
    "Nike": {
        "competitors": ["Adidas", "Puma", "New Balance"],
        "ticker": "NKE",
        "category": "athletic_wear"
    },
    "Apple": {
        "competitors": ["Samsung", "Microsoft", "Google"],
        "ticker": "AAPL",
        "category": "consumer_electronics"
    },
    "Tesla": {
        "competitors": ["Ford", "GM", "Volkswagen"],
        "ticker": "TSLA",
        "category": "automotive"
    },
    "Coca-Cola": {
        "competitors": ["PepsiCo", "Monster", "Red Bull"],
        "ticker": "KO",
        "category": "beverages"
    }
}

def schedule_daily_scraping():
    """Main scheduler function - runs all agents daily"""
    logger.info("🤖 Starting daily Intel scraping agents...")

    for brand_name, brand_config in TRACKED_BRANDS.items():
        logger.info(f"\n📊 Processing {brand_name}")

        try:
            # 1. Scrape retail SKUs
            logger.info(f"  🛒 Scraping SKUs from Amazon...")
            amazon_skus = scrape_amazon_skus(brand_name, brand_config["category"])
            if amazon_skus:
                store_scraped_skus_in_supabase(brand_name, amazon_skus, "Amazon")
                logger.info(f"  ✓ Stored {len(amazon_skus)} SKUs from Amazon")

            # 2. Scrape Tesco (if UK brand)
            if brand_config["category"] in ["beverages", "food_cpg"]:
                logger.info(f"  🛒 Scraping SKUs from Tesco...")
                tesco_skus = scrape_tesco_skus(brand_name)
                if tesco_skus:
                    store_scraped_skus_in_supabase(brand_name, tesco_skus, "Tesco")
                    logger.info(f"  ✓ Stored {len(tesco_skus)} SKUs from Tesco")

            # 3. Fetch news
            logger.info(f"  📰 Fetching news...")
            news = fetch_company_news(brand_name)
            if news and news.get('articles'):
                logger.info(f"  ✓ Found {len(news['articles'])} news articles")
                # Store latest article
                if news['articles']:
                    latest_article = news['articles'][0]
                    store_news_in_supabase(brand_name, latest_article)

            # 4. Track competitors
            logger.info(f"  🏆 Tracking competitors...")
            competitors = brand_config.get("competitors", [])
            if competitors:
                comp_data = track_competitor_skus(brand_name, competitors)
                if comp_data['competitors']:
                    logger.info(f"  ✓ Tracked {len(comp_data['competitors'])} competitors")
                    store_competitor_data_in_supabase(comp_data)

            # 5. Fetch earnings call (quarterly, not daily)
            logger.info(f"  📞 Checking earnings call transcript...")
            earnings = fetch_earnings_call_transcript(brand_name, brand_config["ticker"])
            if earnings:
                logger.info(f"  ✓ Found earnings call")

            logger.info(f"  ✅ {brand_name} complete")

        except Exception as e:
            logger.error(f"  ❌ Error processing {brand_name}: {e}")
            continue

    logger.info("\n✅ Daily scraping complete!")

def store_news_in_supabase(brand_name, article):
    """Store news article in Supabase"""
    try:
        lib._sb().table("brand_intelligence_insights").insert({
            "brand_name": brand_name,
            "tracked_date": datetime.now().date().isoformat(),
            "strategic_direction": article.get('title'),
            "source": article.get('source'),
            "source_url": article.get('url'),
            "confidence_score": 0.7
        }).execute()
    except Exception as e:
        logger.error(f"Error storing news: {e}")

def store_competitor_data_in_supabase(comp_data):
    """Store competitor comparison in Supabase"""
    try:
        primary_brand = comp_data['primary_brand']

        for competitor_info in comp_data['competitors']:
            competitor_name = competitor_info['name']

            lib._sb().table("competitor_comparison_history").insert({
                "primary_brand_name": primary_brand,
                "competitor_name": competitor_name,
                "tracked_date": datetime.now().date().isoformat(),
                "primary_social_spend": 0,  # Will be populated from other sources
                "competitor_social_spend": 0
            }).execute()
    except Exception as e:
        logger.error(f"Error storing competitor data: {e}")

# Integration points:
# Railway: Add to Procfile as: `scheduler: python intel_scheduler.py`
# Or use APScheduler:
#
# from apscheduler.schedulers.background import BackgroundScheduler
# scheduler = BackgroundScheduler()
# scheduler.add_job(schedule_daily_scraping, 'cron', hour=2)  # Run at 2 AM daily
# scheduler.start()

if __name__ == "__main__":
    schedule_daily_scraping()
