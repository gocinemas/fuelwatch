#!/usr/bin/env python3
"""
Refresh Intel campaign data in Supabase with realistic incremental updates.
Simulates live World Cup campaign performance across 34 brands.

Run: railway run python3 refresh_campaign_data.py
"""

import os
import random
from datetime import date

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
TODAY = date.today().isoformat()

from supabase import create_client

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
print(f"Connected to Supabase | refresh date: {TODAY}")

# All 34 FIFA World Cup 2026 sponsors
FIFA_SPONSORS = {
    # FIFA Partners (top tier)
    "Coca-Cola", "Adidas", "Visa", "Wanda Group", "Qatar Airways", "Hyundai",
    # World Cup Sponsors
    "Rexona", "Sure", "Degree", "McDonald's", "Pringles", "Gatorade",
    "Vivo", "OnePlus", "Budweiser", "Carlsberg", "Bank of America", "QNB",
    # Supporters
    "Twitter", "NetJets", "Spotify", "EA Sports", "Playstation", "NVIDIA",
    "Google", "Microsoft", "Canon", "Panasonic", "Kia Motors", "JetBlue",
    "Hisense", "Alibaba", "Tencent", "Manulife",
}
FIFA_SPONSORS_LOWER = {s.lower() for s in FIFA_SPONSORS}

# Brand identifier fields per table (order = priority)
BRAND_FIELDS = ["brand_name", "brand", "brand_variant", "name"]


def clamp(val, lo, hi):
    return max(lo, min(hi, val))


def rand_mult(lo=0.05, hi=0.15):
    return 1.0 + random.uniform(lo, hi)


def bump_int(val):
    if val is None:
        return None
    return max(0, int(float(val) * rand_mult()))


def bump_float(val, cap=None):
    if val is None:
        return None
    result = float(val) * rand_mult()
    if cap is not None:
        result = clamp(result, 0, cap)
    return round(result, 6)


def vary_sentiment(val):
    if val is None:
        return None
    return round(clamp(float(val) + random.uniform(-0.05, 0.05), -1.0, 1.0), 4)


def sentiment_trend(old_avg, new_avg):
    if old_avg is None or new_avg is None:
        return None
    diff = float(new_avg) - float(old_avg)
    if diff > 0.01:
        return "rising"
    elif diff < -0.01:
        return "falling"
    return "stable"


SENTIMENT_SCORE_FIELDS = [
    "sentiment_score", "positive_score", "negative_score",
    "neutral_score", "sentiment", "score", "overall_sentiment", "sentiment_avg",
]
VOLUME_INT_FIELDS = [
    "impressions", "reach", "clicks", "views", "view_count",
    "likes", "like_count", "shares", "share_count", "comments",
    "comment_count", "saves", "volume",
]
RATE_FIELDS = ["engagement_rate", "ctr", "click_through_rate", "conversion_rate"]
MONEY_FIELDS = ["spend", "cost", "revenue", "cpc", "cpm", "roas"]
DATE_FIELDS = ["date", "metric_date", "tracked_date", "updated_at", "last_updated"]


def get_brand(row):
    for f in BRAND_FIELDS:
        v = row.get(f)
        if v:
            return str(v).lower()
    return None


def is_fifa_sponsor(row):
    brand = get_brand(row)
    if brand is None:
        return True  # no brand field → include (don't silently drop)
    # partial match: e.g. "coca-cola" matches "Coca-Cola"
    return any(brand in s or s in brand for s in FIFA_SPONSORS_LOWER)


def build_patch(row, sentiment=False, volumes=True, rates=True, money=False, dates=True):
    patch = {}
    new_sentiment_avg = None

    if sentiment:
        for f in SENTIMENT_SCORE_FIELDS:
            if f in row and row[f] is not None:
                new_val = vary_sentiment(row[f])
                patch[f] = new_val
                if f == "sentiment_avg":
                    new_sentiment_avg = new_val

        # Recalculate sentiment_trend when sentiment_avg is present
        if "sentiment_trend" in row:
            old_avg = row.get("sentiment_avg")
            trend = sentiment_trend(old_avg, new_sentiment_avg or old_avg)
            if trend is not None:
                patch["sentiment_trend"] = trend

    if volumes:
        for f in VOLUME_INT_FIELDS:
            if f in row and row[f] is not None:
                patch[f] = bump_int(row[f])

    if rates:
        for f in RATE_FIELDS:
            if f in row and row[f] is not None:
                cap = 1.0 if float(row[f]) <= 1.0 else 100.0
                patch[f] = bump_float(row[f], cap=cap)

    if money:
        for f in MONEY_FIELDS:
            if f in row and row[f] is not None:
                patch[f] = round(float(row[f]) * rand_mult(), 4)

    if dates:
        for f in DATE_FIELDS:
            if f in row:
                patch[f] = TODAY

    return patch


def refresh_table(table, sentiment=False, volumes=True, rates=True, money=False):
    print(f"\n--- {table} ---")
    rows = sb.table(table).select("*").execute().data or []
    if not rows:
        print("  0 rows — skipped")
        return 0, set()

    print(f"  {len(rows)} rows fetched | fields: {list(rows[0].keys())}")

    updated = 0
    brands_updated = set()
    skipped = 0

    for row in rows:
        if not is_fifa_sponsor(row):
            skipped += 1
            continue

        patch = build_patch(row, sentiment=sentiment, volumes=volumes,
                            rates=rates, money=money)
        if patch:
            sb.table(table).update(patch).eq("id", row["id"]).execute()
            updated += 1
            brand = get_brand(row)
            if brand:
                brands_updated.add(brand)

    if skipped:
        print(f"  Skipped {skipped} non-FIFA rows")
    print(f"  Updated {updated} rows across {len(brands_updated)} brand(s)")
    return updated, brands_updated


total_rows = 0
all_brands: set = set()

_, b = refresh_table("campaign_sentiment", sentiment=True, volumes=True)
all_brands |= b

_, b = refresh_table("campaign_metrics", volumes=True, rates=True, money=True)
all_brands |= b

_, b = refresh_table("campaign_creatives", volumes=True, rates=True)
all_brands |= b

_, b = refresh_table("campaign_variants", sentiment=True, volumes=True, rates=True)
all_brands |= b

print(f"\nDone — {len(all_brands)} brand(s) refreshed for {TODAY}")
if all_brands:
    print(f"Brands: {', '.join(sorted(all_brands))}")
