#!/usr/bin/env python3
"""
Fetch social media data for newly added brands.
Gets YouTube, Twitter, Instagram, LinkedIn follower counts and engagement metrics.
Runs as final step after brand population.

Usage:
    python3 fetch_brand_social_media.py [brand_name] [--all]
    (or) railway run python3 fetch_brand_social_media.py --all
"""

import os
import sys
import json
import time
from datetime import datetime
from typing import Dict, Optional, List


def get_brand_list_for_social_fetch() -> List[str]:
    """Get brands that need social media data (not in brand_social_media table yet)."""
    try:
        import library as lib
        sb = lib._sb()

        # Get all brands from brand_phase1_intelligence
        brands_result = sb.table("brand_phase1_intelligence").select("brand_name").execute()
        all_brands = list(set(b["brand_name"] for b in brands_result.data))

        # Get brands already with social media data
        social_result = sb.table("brand_social_media").select("brand_name").execute()
        has_social = set(s["brand_name"] for s in social_result.data)

        # Return brands missing social data
        missing = [b for b in all_brands if b not in has_social]
        print(f"📊 Brands needing social media data: {len(missing)}/{len(all_brands)}")
        return missing

    except Exception as e:
        print(f"❌ Failed to get brand list: {e}")
        return []


def fetch_youtube_channel_data(brand_name: str) -> Optional[Dict]:
    """Fetch YouTube channel data for a brand."""
    try:
        api_key = os.environ.get("YOUTUBE_API_KEY")
        if not api_key:
            return None

        import requests
        from urllib.parse import quote

        # Search for official brand channel
        search_url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "q": f"{brand_name} official",
            "type": "channel",
            "maxResults": 1,
            "key": api_key,
        }

        response = requests.get(search_url, params=params, timeout=5)
        if response.status_code != 200:
            return None

        data = response.json()
        if not data.get("items"):
            return None

        channel_id = data["items"][0]["id"].get("channelId")
        if not channel_id:
            return None

        # Get channel statistics
        channel_url = "https://www.googleapis.com/youtube/v3/channels"
        channel_params = {
            "part": "statistics,snippet",
            "id": channel_id,
            "key": api_key,
        }

        channel_response = requests.get(channel_url, params=channel_params, timeout=5)
        if channel_response.status_code != 200:
            return None

        channel_data = channel_response.json()
        if not channel_data.get("items"):
            return None

        channel_info = channel_data["items"][0]
        stats = channel_info.get("statistics", {})
        snippet = channel_info.get("snippet", {})

        return {
            "platform": "youtube",
            "channel_name": snippet.get("title", brand_name),
            "channel_url": f"https://www.youtube.com/channel/{channel_id}",
            "subscribers": stats.get("subscriberCount", "0"),
            "view_count": stats.get("viewCount", "0"),
            "video_count": stats.get("videoCount", "0"),
            "data_source": "YouTube Data API",
            "fetched_at": datetime.now().isoformat(),
        }
    except Exception as e:
        print(f"  ⚠️  YouTube fetch failed for {brand_name}: {e}")
        return None


def fetch_twitter_data(brand_name: str) -> Optional[Dict]:
    """Fetch Twitter/X data for a brand (if API available)."""
    try:
        api_key = os.environ.get("TWITTER_API_KEY")
        if not api_key:
            return None

        # Twitter API v2 bearer token required
        import requests
        headers = {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "BrandIntelligence/1.0"
        }

        # Search for brand account
        search_url = "https://api.twitter.com/2/tweets/search/recent"
        query = f"from:{brand_name.lower().replace(' ', '')} -is:retweet"

        # Note: This requires proper auth setup
        # For now, returning None as this needs credential setup
        return None

    except Exception as e:
        print(f"  ⚠️  Twitter fetch failed for {brand_name}: {e}")
        return None


def fetch_instagram_data(brand_name: str) -> Optional[Dict]:
    """Fetch Instagram data via web scraping (limited without API)."""
    try:
        # Instagram doesn't provide public API for follower counts without business account
        # Would need Instagrapi or similar library
        # For now, return None - can be enhanced with proper setup
        return None
    except Exception as e:
        print(f"  ⚠️  Instagram fetch failed for {brand_name}: {e}")
        return None


def fetch_linkedin_data(brand_name: str) -> Optional[Dict]:
    """Fetch LinkedIn company data."""
    try:
        api_key = os.environ.get("LINKEDIN_API_KEY")
        if not api_key:
            return None

        # LinkedIn requires OAuth - simplified placeholder
        return None

    except Exception as e:
        print(f"  ⚠️  LinkedIn fetch failed for {brand_name}: {e}")
        return None


def store_social_media_data(brand_name: str, social_data: Dict) -> bool:
    """Store social media data in database."""
    try:
        import library as lib
        sb = lib._sb()

        # Check if already exists
        existing = sb.table("brand_social_media").select("*").eq("brand_name", brand_name).execute().data

        record = {
            "brand_name": brand_name,
            "platform": social_data.get("platform"),
            "channel_name": social_data.get("channel_name", brand_name),
            "channel_url": social_data.get("channel_url", ""),
            "followers": social_data.get("subscribers") or social_data.get("followers", "0"),
            "engagement_rate": social_data.get("engagement_rate", "0%"),
            "reach": social_data.get("reach", "0"),
            "monthly_ad_spend": social_data.get("ad_spend", "0"),
            "data_source": social_data.get("data_source", ""),
            "fetched_at": social_data.get("fetched_at", datetime.now().isoformat()),
        }

        if existing:
            # Update
            sb.table("brand_social_media").update(record).eq("brand_name", brand_name).execute()
        else:
            # Insert
            sb.table("brand_social_media").insert(record).execute()

        return True
    except Exception as e:
        print(f"  ❌ Failed to store social data for {brand_name}: {e}")
        return False


def fetch_all_missing_social_data(batch_size: int = 10):
    """Fetch social media data for all brands missing it."""
    brands = get_brand_list_for_social_fetch()

    if not brands:
        print("✅ All brands already have social media data")
        return

    print(f"\n📱 Fetching social media data for {len(brands)} brands...\n")

    successful = 0
    failed = 0

    for i, brand in enumerate(brands[:batch_size], 1):
        print(f"[{i}/{min(len(brands), batch_size)}] {brand}...", end=" ", flush=True)

        # Try YouTube (most reliable)
        yt_data = fetch_youtube_channel_data(brand)
        if yt_data and store_social_media_data(brand, yt_data):
            print("✅ YouTube")
            successful += 1
        else:
            # Try others if YouTube fails
            for fetch_fn in [fetch_twitter_data, fetch_instagram_data, fetch_linkedin_data]:
                data = fetch_fn(brand)
                if data and store_social_media_data(brand, data):
                    print(f"✅ {data.get('platform', 'Unknown')}")
                    successful += 1
                    break
            else:
                print("⏭️  Skipped (no data)")
                failed += 1

        # Rate limiting
        time.sleep(0.5)

    print(f"\n" + "="*70)
    print(f"✅ SOCIAL MEDIA FETCH COMPLETE")
    print(f"   Successful: {successful}/{min(len(brands), batch_size)}")
    print(f"   Skipped: {failed}/{min(len(brands), batch_size)}")
    print(f"="*70 + "\n")


def main():
    print("\n" + "="*70)
    print("BRAND SOCIAL MEDIA DATA FETCHER")
    print("="*70)

    args = sys.argv[1:]

    if "--all" in args:
        # Fetch for all missing brands
        fetch_all_missing_social_data(batch_size=100)
    elif args:
        # Fetch for specific brand
        brand_name = " ".join(args).strip("--all")
        print(f"\nFetching data for: {brand_name}\n")

        yt_data = fetch_youtube_channel_data(brand_name)
        if yt_data:
            if store_social_media_data(brand_name, yt_data):
                print(f"✅ Stored YouTube data for {brand_name}")
            else:
                print(f"❌ Failed to store data for {brand_name}")
        else:
            print(f"⚠️  No YouTube data found for {brand_name}")
    else:
        # Fetch for recently added brands (last 50)
        print("\nFetching social media for recently added brands...\n")
        fetch_all_missing_social_data(batch_size=50)


if __name__ == "__main__":
    main()
