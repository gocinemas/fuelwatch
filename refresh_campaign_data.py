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


SENTIMENT_SCORE_FIELDS = ["sentiment_score", "positive_score", "negative_score",
                          "neutral_score", "sentiment", "score", "overall_sentiment"]
VOLUME_INT_FIELDS = ["impressions", "reach", "clicks", "views", "view_count",
                     "likes", "like_count", "shares", "share_count", "comments",
                     "comment_count", "saves", "volume"]
RATE_FIELDS = ["engagement_rate", "ctr", "click_through_rate", "conversion_rate"]
MONEY_FIELDS = ["spend", "cost", "revenue", "cpc", "cpm", "roas"]
DATE_FIELDS = ["date", "metric_date", "tracked_date", "updated_at", "last_updated"]


def build_patch(row, sentiment=False, volumes=True, rates=True, money=False, dates=True):
    patch = {}
    if sentiment:
        for f in SENTIMENT_SCORE_FIELDS:
            if f in row and row[f] is not None:
                patch[f] = vary_sentiment(row[f])
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
    print(f"  {len(rows)} rows | fields: {list(rows[0].keys()) if rows else 'none'}")
    updated = 0
    for row in rows:
        patch = build_patch(row, sentiment=sentiment, volumes=volumes,
                            rates=rates, money=money)
        if patch:
            sb.table(table).update(patch).eq("id", row["id"]).execute()
            updated += 1
    print(f"  Updated {updated} rows")
    return updated


total = 0
total += refresh_table("campaign_sentiment", sentiment=True, volumes=True)
total += refresh_table("campaign_metrics", volumes=True, rates=True, money=True)
total += refresh_table("campaign_creatives", volumes=True, rates=True)
total += refresh_table("campaign_variants", sentiment=True, volumes=True, rates=True)

print(f"\nDone — {total} total rows refreshed for {TODAY}")
