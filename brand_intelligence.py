"""
UNIFIED Brand Intelligence Platform

Tracks everything about a brand:
1. AI Search Visibility (ChatGPT, Claude, Perplexity mentions)
2. Competitor Pricing & Product Changes
3. Social Intelligence (mentions, sentiment, trends)
4. Growth Signals (hiring, funding, expansion)
5. D2C Catalog Intelligence (SKU tracking, inventory, reviews)

Data sources (all free-tier / low-cost, no paid enterprise APIs required):
- NewsAPI.org (free tier: 100 req/day) — AI visibility proxy + growth signals
- Reddit public search JSON endpoint (free, no key) — social mentions
- Twitter/X API v2 recent search (requires TWITTER_BEARER_TOKEN; note the
  official "free" tier does not include search/read access as of 2023 — this
  only activates if a working bearer token is configured, e.g. Basic tier+)
- SerpAPI Google Shopping engine (free tier: 100 searches/month) — pricing +
  catalog/SKU intelligence
- hiring_signals_fetcher.HiringSignalsFetcher (Adzuna free API, if
  ADZUNA_API_KEY configured) — hiring activity, reused from existing repo code

Every function fails soft: missing API keys or upstream errors return the
same schema with empty/placeholder values plus a human-readable
`data_status` explaining what's missing, instead of raising or faking data.
"""

import requests
import json
import os
import re
import math
import logging
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 8

# ---------------------------------------------------------------------------
# API credentials — read from environment only. NEWS_API_KEY falls back to
# the NewsAPI key already committed/in-use elsewhere in this repo
# (news_service.py) so this module works out of the box in this codebase
# without introducing a *new* hardcoded secret.
# ---------------------------------------------------------------------------
try:
    from news_service import NEWSAPI_KEY as _REPO_NEWSAPI_KEY
except Exception:
    _REPO_NEWSAPI_KEY = ""

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "") or _REPO_NEWSAPI_KEY
TWITTER_BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN", "")
SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY", "") or os.environ.get("SERP_API_KEY", "")
# Reddit's public read-only JSON endpoints (reddit.com/*.json without auth)
# now hard-403 everything as of Reddit's 2023+ API lockdown — even simple
# subreddit listings, not just search. Real access now requires a free
# Reddit "script" app (https://www.reddit.com/prefs/apps) and OAuth via the
# client_credentials grant. That's still free (non-commercial, <100 QPM).
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "brand-intelligence/1.0")

# NewsAPI.org's free/developer plan only allows querying the trailing ~1
# month of articles (older `from` dates get a 426 "upgrade required" error).
# Clamp everything through this module to stay inside that window.
NEWSAPI_MAX_DAYS_BACK = 29


# ---------------------------------------------------------------------------
# Lightweight lexicon-based sentiment + theme extraction.
# No NLP dependency (textblob/vaderSentiment aren't installed in this repo's
# requirements.txt) — keyword scoring is cheap, dependency-free and good
# enough for headline/tweet/post-level sentiment at this scale.
# ---------------------------------------------------------------------------
POSITIVE_WORDS = {
    "great", "excellent", "love", "loves", "loved", "best", "amazing", "growth",
    "success", "successful", "innovative", "award", "awarded", "launch", "launches",
    "expand", "expands", "expanding", "popular", "favorite", "favourite", "top",
    "strong", "boost", "boosts", "win", "wins", "winning", "impressive", "delicious",
    "fresh", "quality", "sustainable", "recommend", "recommended", "praise", "praised",
    "celebrate", "celebrates", "thrive", "thriving", "record", "milestone", "surge",
    "soar", "soars", "profit", "profits", "opens", "opening", "flagship", "beloved",
    "iconic", "premium", "delight", "delightful",
}
NEGATIVE_WORDS = {
    "closure", "closures", "closed", "closing", "layoff", "layoffs", "lawsuit",
    "recall", "recalled", "fraud", "decline", "declines", "declining", "loss",
    "losses", "controversy", "criticism", "criticised", "criticized", "boycott",
    "complaint", "complaints", "fail", "fails", "failure", "failing", "scandal",
    "fire", "fired", "fined", "poor", "worst", "struggle", "struggling",
    "cut", "cuts", "cutting", "shutdown", "bankrupt", "bankruptcy", "warning",
    "risk", "concern", "concerns", "backlash", "sued", "suing", "downturn",
    "slump", "disappointing", "angry", "outrage", "bad", "avoid", "waste",
    "wasted", "expensive", "overpriced", "rude", "slow", "terrible", "awful",
    "disappointed", "disappointing", "rip-off", "ripoff",
}
# Neutral markers — social/review chatter that shouldn't be scored either way
# (e.g. Reddit/Twitter text like "it's fine" or "pretty average" should not
# swing sentiment negative just because it superficially overlaps with news
# words like "fined"). These aren't folded into the +/- tally; they exist so
# callers/tests can recognise genuinely neutral language explicitly.
NEUTRAL_WORDS = {
    "ok", "okay", "average", "fine", "decent", "alright", "mediocre", "moderate",
}

THEME_KEYWORDS = {
    "Expansion": ["expand", "expansion", "new store", "new location", "opens its",
                  "opening a", "flagship store", "new branch"],
    "Funding & Investment": ["funding", "raises", "investment", "investor",
                              "series a", "series b", "series c", "valuation",
                              "venture capital"],
    "Product Launch": ["launches", "launch", "unveils", "introduces", "debuts",
                        "new product", "new range", "new menu"],
    "Partnership": ["partnership", "partners with", "collaboration", "teams up",
                     "team up", "tie-up"],
    "Sustainability": ["sustainable", "sustainability", "eco-friendly", "carbon",
                        "organic", "ethical", "compostable", "recyclable"],
    "Awards & Recognition": ["award", "recognition", "named", "ranked", "wins the",
                             "best in"],
    "Pricing & Value": ["price", "pricing", "discount", "cost of", "affordable",
                         "expensive", "price rise", "price hike"],
    "Quality & Reviews": ["quality", "review", "rating", "taste", "delicious",
                           "customers say"],
    "Leadership": ["ceo", "founder", "co-founder", "executive", "appoints", "appointed"],
    "Controversy": ["controversy", "criticism", "backlash", "lawsuit", "recall",
                     "boycott", "scandal"],
}


def _lexicon_sentiment(texts):
    """Score a list of text snippets -1..1 using a positive/negative keyword lexicon.
    Returns (label, score). Cheap, dependency-free stand-in for real NLP sentiment."""
    if not texts:
        return "—", 0.0

    pos = neg = 0
    for t in texts:
        if not t:
            continue
        words = re.findall(r"[a-z']+", t.lower())
        for w in words:
            if w in POSITIVE_WORDS:
                pos += 1
            elif w in NEGATIVE_WORDS:
                neg += 1

    total = pos + neg
    if total == 0:
        return "Neutral", 0.0

    score = (pos - neg) / total
    if score > 0.35:
        label = "Very Positive"
    elif score > 0.1:
        label = "Positive"
    elif score < -0.35:
        label = "Very Negative"
    elif score < -0.1:
        label = "Negative"
    else:
        label = "Neutral"
    return label, round(score, 2)


def _extract_themes(texts, top_n=5):
    """Bucket a list of text snippets into recurring business themes."""
    if not texts:
        return []
    counts = defaultdict(int)
    lowered = [t.lower() for t in texts if t]
    for theme, keywords in THEME_KEYWORDS.items():
        for text in lowered:
            if any(kw in text for kw in keywords):
                counts[theme] += 1
    ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return [theme for theme, count in ranked[:top_n] if count > 0]


# ---------------------------------------------------------------------------
# NewsAPI helper (shared by ai_visibility + growth_signals)
# ---------------------------------------------------------------------------
def _newsapi_search(query, days_back=30, page_size=30, sort_by="publishedAt"):
    """Search NewsAPI.org /v2/everything. Returns (articles, total_results).
    Returns (None, 0) on missing key / rate-limit / error so callers can tell
    'no key configured' apart from 'zero real mentions found'."""
    if not NEWS_API_KEY:
        logger.info("[brand_intel] NEWS_API_KEY not configured; skipping NewsAPI call")
        return None, 0

    try:
        days_back = min(days_back, NEWSAPI_MAX_DAYS_BACK)
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": query,
                "from": from_date,
                "language": "en",
                "sortBy": sort_by,
                "pageSize": min(page_size, 100),
                "apiKey": NEWS_API_KEY,
            },
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 429:
            logger.warning("[brand_intel] NewsAPI rate limit hit")
            return None, 0
        if resp.status_code != 200:
            logger.warning(f"[brand_intel] NewsAPI error {resp.status_code}: {resp.text[:200]}")
            return None, 0

        data = resp.json()
        return data.get("articles", []), data.get("totalResults", 0)

    except requests.exceptions.RequestException as e:
        logger.warning(f"[brand_intel] NewsAPI request failed: {e}")
        return None, 0
    except Exception as e:
        logger.warning(f"[brand_intel] NewsAPI parse failed: {e}")
        return None, 0


def fetch_brand_intelligence(brand_name: str, market: str = "GB") -> dict:
    """
    Unified brand intelligence dashboard.

    Returns all brand data across 5 dimensions.
    """

    result = {
        "name": brand_name,
        "market": market.upper(),
        "timestamp": datetime.now().isoformat(),
        "overall_score": 0,

        # 1. AI Search Visibility
        "ai_visibility": {
            "chatgpt_mentions": [],
            "claude_mentions": [],
            "perplexity_mentions": [],
            "google_ai_mentions": [],
            "sentiment": "—",
            "visibility_score": 0,
            "key_themes": []
        },

        # 2. Competitor Pricing
        "pricing_intelligence": {
            "current_price": "—",
            "price_range": "—",
            "price_changes_30d": [],
            "competitor_prices": [],
            "price_position": "—",  # Premium/Mid/Budget
            "skus_tracked": 0
        },

        # 3. Social Intelligence
        "social_signals": {
            "reddit_mentions": [],
            "twitter_sentiment": "—",
            "tiktok_presence": False,
            "instagram_followers": "—",
            "mentions_30d": 0,
            "trending": False,
            "overall_sentiment": "—",
            "top_topics": []
        },

        # 4. Growth Signals
        "growth_signals": {
            "hiring_activity": "—",
            "recent_hires": 0,
            "funding_rounds": [],
            "store_openings": [],
            "expansion_markets": [],
            "product_launches": [],
            "growth_score": 0
        },

        # 5. D2C Catalog Intelligence
        "catalog_intelligence": {
            "total_skus": 0,
            "new_products_30d": [],
            "inventory_status": "—",
            "review_average": 0,
            "review_count": 0,
            "top_products": [],
            "catalog_velocity": "—"  # Fast/Medium/Slow
        },

        "data_sources": [],
        "last_updated": datetime.now().isoformat()
    }

    try:
        # Fetch from real data sources only (no fallback demo data)
        # 1. AI Search Visibility
        logger.info(f"[brand_intel] Fetching AI visibility for {brand_name}...")
        ai_data = _fetch_ai_visibility(brand_name)
        if ai_data:
            result["ai_visibility"] = ai_data
            result["data_sources"].append("NewsAPI (visibility proxy)")

        # 2. Competitor Pricing
        logger.info(f"[brand_intel] Fetching pricing data for {brand_name}...")
        pricing_data = _fetch_pricing_intelligence(brand_name, market)
        if pricing_data:
            result["pricing_intelligence"] = pricing_data
            result["data_sources"].append("Google Shopping (SerpAPI)")

        # 3. Social Intelligence
        logger.info(f"[brand_intel] Fetching social signals for {brand_name}...")
        social_data = _fetch_social_intelligence(brand_name)
        if social_data:
            result["social_signals"] = social_data
            result["data_sources"].append("Reddit / Twitter")

        # 4. Growth Signals
        logger.info(f"[brand_intel] Fetching growth signals for {brand_name}...")
        growth_data = _fetch_growth_signals(brand_name)
        if growth_data:
            result["growth_signals"] = growth_data
            result["data_sources"].append("NewsAPI / Adzuna")

        # 5. D2C Catalog Intelligence
        logger.info(f"[brand_intel] Fetching catalog data for {brand_name}...")
        catalog_data = _fetch_catalog_intelligence(brand_name, market)
        if catalog_data:
            result["catalog_intelligence"] = catalog_data
            result["data_sources"].append("Google Shopping (SerpAPI)")

        # Calculate overall brand health score (0-100)
        result["overall_score"] = _calculate_brand_score(result)

        return result

    except Exception as e:
        logger.error(f"[brand_intelligence] ERROR: {e}", exc_info=True)
        result["error"] = str(e)
        return result


def _fetch_ai_visibility(brand_name: str) -> dict:
    """Approximate AI/search visibility for a brand.

    There is no public API that reports "what ChatGPT/Claude/Perplexity say"
    about a brand — those assistants don't expose mention logs. The closest
    real, freely-available signal is web/news mention volume, which strongly
    correlates with what a brand's training-data & search footprint looks
    like. We use NewsAPI mention count (last 30 days) as that proxy, plus
    real sentiment + theme extraction from the actual headlines returned.
    """
    result = {
        "chatgpt_mentions": [],
        "claude_mentions": [],
        "perplexity_mentions": [],
        "google_ai_mentions": [],
        "sentiment": "—",
        "visibility_score": 0,
        "key_themes": [],
    }
    try:
        articles, total_results = _newsapi_search(f'"{brand_name}"', days_back=30, page_size=50)

        if articles is None:
            result["data_status"] = (
                "AI visibility unavailable — NEWS_API_KEY missing or NewsAPI request failed"
            )
            return result

        texts = [f"{a.get('title', '')} {a.get('description', '')}".strip()
                 for a in articles if a.get("title")]

        # Log-scaled 0-100: 0 mentions -> 0, ~10 -> ~21, ~100 -> ~40, ~1000 -> ~60, ~10000 -> ~80
        result["visibility_score"] = min(100, round(20 * math.log10(total_results + 1)))

        sentiment_label, sentiment_score = _lexicon_sentiment(texts)
        result["sentiment"] = sentiment_label
        result["key_themes"] = _extract_themes(texts)
        result["mentions_30d"] = total_results
        result["sentiment_score"] = sentiment_score
        result["sample_headlines"] = [a.get("title") for a in articles[:5] if a.get("title")]
        result["data_status"] = (
            f"Live NewsAPI mention volume used as visibility proxy "
            f"({total_results} articles matched in last 30 days)"
        )

        logger.info(
            f"[ai_visibility] {brand_name}: mentions={total_results}, "
            f"score={result['visibility_score']}, sentiment={sentiment_label}"
        )
        return result

    except Exception as e:
        logger.error(f"[ai_visibility] Error for {brand_name}: {e}", exc_info=True)
        result["data_status"] = f"Error: {e}"
        return result


# ---------------------------------------------------------------------------
# Social intelligence — Reddit (free, no key) + Twitter/X v2 (if a working
# bearer token is configured).
# ---------------------------------------------------------------------------
_reddit_token_cache = {"token": None, "expires_at": None}


def _get_reddit_access_token():
    """OAuth client_credentials grant for a Reddit 'script' app.
    Reddit's read-only JSON endpoints (reddit.com/*.json) now 403 without
    auth — this is the current free, real way to read public Reddit data
    (register a free app at https://www.reddit.com/prefs/apps, type=script).
    Token is cached in-process until shortly before it expires."""
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        return None

    cached = _reddit_token_cache.get("token")
    expires_at = _reddit_token_cache.get("expires_at")
    if cached and expires_at and datetime.now() < expires_at:
        return cached

    try:
        resp = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": REDDIT_USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning(f"[social] Reddit OAuth token request failed: {resp.status_code} {resp.text[:200]}")
            return None

        data = resp.json()
        token = data.get("access_token")
        if not token:
            return None

        expires_in = data.get("expires_in", 3600)
        _reddit_token_cache["token"] = token
        _reddit_token_cache["expires_at"] = datetime.now() + timedelta(seconds=max(60, expires_in - 60))
        return token

    except requests.exceptions.RequestException as e:
        logger.warning(f"[social] Reddit OAuth request failed: {e}")
        return None
    except Exception as e:
        logger.warning(f"[social] Reddit OAuth parse failed: {e}")
        return None


def _fetch_reddit_mentions(brand_name, days_back=30):
    """Real Reddit mentions via OAuth-authenticated search (see
    _get_reddit_access_token — Reddit's unauthenticated JSON endpoints are
    fully blocked as of their 2023+ API policy). Requires REDDIT_CLIENT_ID +
    REDDIT_CLIENT_SECRET (free 'script' app); fails soft to an empty list
    otherwise."""
    token = _get_reddit_access_token()
    if not token:
        logger.info(
            "[social] Reddit unavailable — set REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET "
            "(free script app at reddit.com/prefs/apps) for real Reddit mentions"
        )
        return []

    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": REDDIT_USER_AGENT,
        }
        resp = requests.get(
            "https://oauth.reddit.com/search",
            params={"q": brand_name, "sort": "new", "limit": 50, "t": "month"},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 429:
            logger.warning("[social] Reddit rate limited")
            return []
        if resp.status_code != 200:
            logger.warning(f"[social] Reddit error {resp.status_code}")
            return []

        data = resp.json()
        posts = data.get("data", {}).get("children", [])
        cutoff = datetime.now() - timedelta(days=days_back)
        mentions = []
        for p in posts:
            d = p.get("data", {})
            created_utc = d.get("created_utc")
            if not created_utc:
                continue
            created = datetime.fromtimestamp(created_utc)
            if created < cutoff:
                continue
            mentions.append({
                "title": d.get("title", ""),
                "subreddit": d.get("subreddit_name_prefixed", ""),
                "score": d.get("score", 0),
                "num_comments": d.get("num_comments", 0),
                "url": f"https://reddit.com{d.get('permalink', '')}",
                "created": created.isoformat(),
            })
        return mentions

    except requests.exceptions.RequestException as e:
        logger.warning(f"[social] Reddit request failed: {e}")
        return []
    except Exception as e:
        logger.warning(f"[social] Reddit parse failed: {e}")
        return []


def _fetch_twitter_mentions(brand_name):
    """Twitter/X API v2 recent search. Requires TWITTER_BEARER_TOKEN.

    Note: as of the 2023 API pricing changes, the free tier does NOT include
    read/search access (only posting) — recent-search requires at least the
    paid Basic tier. This will simply no-op (return None) unless a bearer
    token with search access is configured; it does not fake data."""
    if not TWITTER_BEARER_TOKEN:
        return None

    try:
        headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
        query = f'"{brand_name}" -is:retweet lang:en'
        resp = requests.get(
            "https://api.twitter.com/2/tweets/search/recent",
            headers=headers,
            params={
                "query": query,
                "max_results": 100,
                "tweet.fields": "created_at,public_metrics",
            },
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 429:
            logger.warning("[social] Twitter rate limited")
            return None
        if resp.status_code == 403:
            logger.info("[social] Twitter API 403 — token likely lacks recent-search read access")
            return None
        if resp.status_code != 200:
            logger.warning(f"[social] Twitter error {resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json()
        tweets = data.get("data", [])
        return {
            "count": data.get("meta", {}).get("result_count", len(tweets)),
            "texts": [t.get("text", "") for t in tweets],
        }

    except requests.exceptions.RequestException as e:
        logger.warning(f"[social] Twitter request failed: {e}")
        return None
    except Exception as e:
        logger.warning(f"[social] Twitter parse failed: {e}")
        return None


def _fetch_social_intelligence(brand_name: str) -> dict:
    """Track social media mentions and sentiment from real, free sources."""
    result = {
        "reddit_mentions": [],
        "twitter_sentiment": "—",
        "tiktok_presence": False,
        "instagram_followers": "—",
        "mentions_30d": 0,
        "trending": False,
        "overall_sentiment": "—",
        "top_topics": [],
    }
    try:
        reddit_mentions = _fetch_reddit_mentions(brand_name, days_back=30)
        twitter_data = _fetch_twitter_mentions(brand_name)

        all_texts = [m["title"] for m in reddit_mentions if m.get("title")]
        reddit_count = len(reddit_mentions)
        twitter_count = 0

        if twitter_data is not None:
            twitter_count = twitter_data.get("count", 0) or 0
            all_texts.extend(twitter_data.get("texts", []))
            t_label, _ = _lexicon_sentiment(twitter_data.get("texts", []))
            result["twitter_sentiment"] = t_label

        result["reddit_mentions"] = sorted(
            reddit_mentions, key=lambda m: m.get("score", 0), reverse=True
        )[:5]
        result["mentions_30d"] = reddit_count + twitter_count

        overall_label, overall_score = _lexicon_sentiment(all_texts)
        result["overall_sentiment"] = overall_label
        result["sentiment_score"] = overall_score
        result["top_topics"] = _extract_themes(all_texts)

        # Trending: last-7-day Reddit velocity vs. the prior weeks' average velocity
        recent_cutoff = datetime.now() - timedelta(days=7)
        recent_count = sum(
            1 for m in reddit_mentions
            if m.get("created") and datetime.fromisoformat(m["created"]) >= recent_cutoff
        )
        older_count = reddit_count - recent_count
        older_weekly_avg = older_count / (23 / 7) if older_count else 0
        result["trending"] = bool(recent_count > 0 and recent_count > older_weekly_avg * 1.5)

        sources = []
        notes = []

        if REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET:
            sources.append("Reddit (OAuth search)")
        else:
            notes.append(
                "set REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET for Reddit coverage "
                "(free script app at reddit.com/prefs/apps — unauthenticated "
                "reddit.com/*.json endpoints are fully blocked as of 2023+)"
            )

        if twitter_data is not None:
            sources.append("Twitter/X API v2")
        elif TWITTER_BEARER_TOKEN:
            notes.append("Twitter request failed or token lacks access this run")
        else:
            notes.append(
                "set TWITTER_BEARER_TOKEN for Twitter/X coverage "
                "(the free API tier has no search/read access; requires paid Basic tier+)"
            )

        status = f"Live data from: {', '.join(sources)}" if sources else "No social sources configured"
        if notes:
            status += " — " + "; ".join(notes)
        result["data_status"] = status

        logger.info(
            f"[social] {brand_name}: mentions_30d={result['mentions_30d']} "
            f"(reddit={reddit_count}, twitter={twitter_count}), sentiment={overall_label}"
        )
        return result

    except Exception as e:
        logger.error(f"[social] Error for {brand_name}: {e}", exc_info=True)
        result["data_status"] = f"Error: {e}"
        return result


# ---------------------------------------------------------------------------
# Pricing + catalog intelligence — both backed by SerpAPI's Google Shopping
# engine. Shared + briefly cached so a single fetch_brand_intelligence() call
# only spends one SerpAPI credit per brand (free tier is 100 searches/month).
# ---------------------------------------------------------------------------
_serpapi_cache = {}
_SERPAPI_CACHE_TTL_SECONDS = 300

_PRICE_RE = re.compile(r"[£$€]\s?([\d,]+\.?\d*)")


def _serpapi_google_shopping(brand_name: str, market: str) -> dict:
    cache_key = f"{brand_name.lower()}::{(market or '').lower()}"
    cached = _serpapi_cache.get(cache_key)
    if cached and (datetime.now() - cached["fetched_at"]).total_seconds() < _SERPAPI_CACHE_TTL_SECONDS:
        logger.debug(f"[brand_intel] Using cached SerpAPI Google Shopping result for {brand_name}")
        return cached["data"]

    if not SERPAPI_API_KEY:
        logger.info("[brand_intel] SERPAPI_API_KEY not configured; skipping Google Shopping lookup")
        return {}

    try:
        resp = requests.get(
            "https://serpapi.com/search.json",
            params={
                "engine": "google_shopping",
                "q": brand_name,
                "gl": (market or "gb").lower(),
                "hl": "en",
                "api_key": SERPAPI_API_KEY,
            },
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 429:
            logger.warning("[brand_intel] SerpAPI rate limited")
            return {}
        if resp.status_code != 200:
            logger.warning(f"[brand_intel] SerpAPI error {resp.status_code}: {resp.text[:200]}")
            return {}

        data = resp.json()
        _serpapi_cache[cache_key] = {"data": data, "fetched_at": datetime.now()}
        return data

    except requests.exceptions.RequestException as e:
        logger.warning(f"[brand_intel] SerpAPI request failed: {e}")
        return {}
    except Exception as e:
        logger.warning(f"[brand_intel] SerpAPI parse failed: {e}")
        return {}


def _parse_price(price_str):
    if not price_str:
        return None
    m = _PRICE_RE.search(str(price_str))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _fetch_pricing_intelligence(brand_name: str, market: str) -> dict:
    """Track real product pricing via Google Shopping (SerpAPI free tier)."""
    result = {
        "current_price": "—",
        "price_range": "—",
        "price_changes_30d": [],
        "competitor_prices": [],
        "price_position": "—",
        "skus_tracked": 0,
    }
    try:
        data = _serpapi_google_shopping(brand_name, market)
        if not data:
            result["data_status"] = (
                "Pricing unavailable — SERPAPI_API_KEY missing or Google Shopping request failed "
                "(SerpAPI free tier: 100 searches/month)"
            )
            return result

        listings = data.get("shopping_results", [])
        if not listings:
            result["data_status"] = f"No Google Shopping listings found for '{brand_name}' in {market}"
            return result

        priced = []
        for item in listings:
            price = _parse_price(item.get("price", ""))
            if price is None:
                continue
            priced.append({
                "title": item.get("title", ""),
                "source": item.get("source", "Unknown"),
                "price": price,
                "rating": item.get("rating"),
                "reviews": item.get("reviews"),
                "link": item.get("link") or item.get("product_link", ""),
            })

        result["skus_tracked"] = len(listings)

        if not priced:
            result["data_status"] = "Listings found but no parsable prices in this Google Shopping response"
            return result

        prices = [p["price"] for p in priced]
        currency_symbol = "£" if (market or "GB").upper() == "GB" else "$"
        result["current_price"] = f"{currency_symbol}{min(prices):.2f}"
        result["price_range"] = f"{currency_symbol}{min(prices):.2f} – {currency_symbol}{max(prices):.2f}"

        # Cheapest listing per retailer, sorted low to high
        by_source = {}
        for p in priced:
            src = p["source"]
            if src not in by_source or p["price"] < by_source[src]["price"]:
                by_source[src] = p
        result["competitor_prices"] = [
            {
                "retailer": src,
                "price": f"{currency_symbol}{v['price']:.2f}",
                "product": v["title"][:80],
            }
            for src, v in sorted(by_source.items(), key=lambda kv: kv[1]["price"])
        ][:6]

        avg_price = sum(prices) / len(prices)
        cheapest = min(prices)
        if cheapest >= avg_price * 1.15:
            result["price_position"] = "Premium"
        elif cheapest <= avg_price * 0.85:
            result["price_position"] = "Budget"
        else:
            result["price_position"] = "Mid-market"

        result["data_status"] = (
            f"Live Google Shopping data via SerpAPI ({len(priced)} priced listings). "
            "price_changes_30d requires day-over-day snapshots, not yet tracked."
        )

        logger.info(
            f"[pricing] {brand_name}: {len(priced)} priced listings, "
            f"range={result['price_range']}, position={result['price_position']}"
        )
        return result

    except Exception as e:
        logger.error(f"[pricing] Error for {brand_name}: {e}", exc_info=True)
        result["data_status"] = f"Error: {e}"
        return result


def _fetch_growth_signals(brand_name: str) -> dict:
    """Track hiring, funding, expansion from NewsAPI + Adzuna (existing repo helper)."""
    result = {
        "hiring_activity": "—",
        "recent_hires": 0,
        "funding_rounds": [],
        "store_openings": [],
        "expansion_markets": [],
        "product_launches": [],
        "growth_score": 0,
    }
    try:
        # Hiring activity — reuse the existing Adzuna-backed hiring signals
        # fetcher already in this repo (hiring_signals_fetcher.py). No-ops
        # cleanly if ADZUNA_API_KEY isn't configured.
        try:
            from hiring_signals_fetcher import HiringSignalsFetcher
            hiring = HiringSignalsFetcher().fetch_hiring_signals(brand_name)
            open_roles = hiring.get("overview", {}).get("total_open_roles", 0) or 0
            if open_roles:
                result["recent_hires"] = open_roles
                result["hiring_activity"] = f"Active — {open_roles} open roles"
        except Exception as e:
            logger.debug(f"[growth] Hiring signals unavailable for {brand_name}: {e}")

        query = (
            f'"{brand_name}" AND (opens OR "new store" OR expansion OR expands OR '
            f'launches OR unveils OR "raises funding" OR "funding round" OR investment OR '
            f'"flagship store" OR "new location" OR "series a" OR "series b")'
        )
        # NEWSAPI_MAX_DAYS_BACK (29d) — the free plan's lookback ceiling, not a
        # deliberately short window; growth signals would ideally span ~90d.
        articles, total_results = _newsapi_search(query, days_back=NEWSAPI_MAX_DAYS_BACK, page_size=50, sort_by="publishedAt")

        if articles is None:
            result["data_status"] = "Growth signals unavailable — NEWS_API_KEY missing or NewsAPI request failed"
            return result

        store_kw = ["new store", "flagship", "opens its", "opens a", "opening a", "opening its"]
        expansion_kw = ["expand", "expansion", "launches in", "enters the", "new market", "new location"]
        launch_kw = ["launches", "unveils", "introduces", "debuts", "new product", "new range", "new menu"]
        funding_kw = ["raises", "funding round", "series a", "series b", "series c",
                      "investment from", "backed by investors"]

        seen_titles = set()
        for a in articles:
            title = a.get("title") or ""
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            text = title.lower()
            item = {
                "headline": title,
                "source": (a.get("source") or {}).get("name", "Unknown"),
                "date": (a.get("publishedAt") or "")[:10],
                "url": a.get("url", ""),
            }
            if any(k in text for k in funding_kw):
                result["funding_rounds"].append(item)
            elif any(k in text for k in store_kw):
                result["store_openings"].append(item)
            elif any(k in text for k in expansion_kw):
                result["expansion_markets"].append(item)
            elif any(k in text for k in launch_kw):
                result["product_launches"].append(item)

        for key in ("funding_rounds", "store_openings", "expansion_markets", "product_launches"):
            result[key] = result[key][:5]

        score = 0
        score += min(30, len(result["funding_rounds"]) * 15)
        score += min(25, len(result["store_openings"]) * 8)
        score += min(20, len(result["expansion_markets"]) * 10)
        score += min(15, len(result["product_launches"]) * 5)
        score += min(10, result["recent_hires"])
        result["growth_score"] = min(100, score)

        result["data_status"] = (
            f"Live NewsAPI growth signals ({total_results} matching articles, "
            f"last {NEWSAPI_MAX_DAYS_BACK} days — free-tier lookback ceiling)"
            + ("; hiring via Adzuna" if result["recent_hires"] else "")
        )

        logger.info(
            f"[growth] {brand_name}: score={result['growth_score']}, "
            f"funding={len(result['funding_rounds'])}, stores={len(result['store_openings'])}, "
            f"expansion={len(result['expansion_markets'])}, launches={len(result['product_launches'])}"
        )
        return result

    except Exception as e:
        logger.error(f"[growth] Error for {brand_name}: {e}", exc_info=True)
        result["data_status"] = f"Error: {e}"
        return result


def _fetch_catalog_intelligence(brand_name: str, market: str) -> dict:
    """Track SKUs, inventory, reviews via Google Shopping (SerpAPI free tier).

    Shares the same underlying SerpAPI call as _fetch_pricing_intelligence
    (via the short-lived cache in _serpapi_google_shopping) so a single
    fetch_brand_intelligence() run doesn't double-spend SerpAPI quota.
    """
    result = {
        "total_skus": 0,
        "new_products_30d": [],
        "inventory_status": "—",
        "review_average": 0,
        "review_count": 0,
        "top_products": [],
        "catalog_velocity": "—",
    }
    try:
        data = _serpapi_google_shopping(brand_name, market)
        if not data:
            result["data_status"] = (
                "Catalog data unavailable — SERPAPI_API_KEY missing or Google Shopping request failed"
            )
            return result

        listings = data.get("shopping_results", [])
        if not listings:
            result["data_status"] = f"No products found for '{brand_name}' in {market} Google Shopping"
            return result

        result["total_skus"] = len(listings)

        rated = [l for l in listings if l.get("rating")]
        if rated:
            total_reviews = sum((l.get("reviews") or 0) for l in rated)
            if total_reviews > 0:
                weighted = sum((l.get("rating") or 0) * (l.get("reviews") or 0) for l in rated)
                result["review_average"] = round(weighted / total_reviews, 2)
            else:
                result["review_average"] = round(sum(l["rating"] for l in rated) / len(rated), 2)
            result["review_count"] = total_reviews

        in_stock_count = sum(
            1 for l in listings if str(l.get("in_stock", "true")).lower() != "false"
        )
        ratio = in_stock_count / len(listings) if listings else 0
        if ratio > 0.8:
            result["inventory_status"] = "In Stock"
        elif ratio > 0.3:
            result["inventory_status"] = "Limited Stock"
        else:
            result["inventory_status"] = "Low Stock"

        top = sorted(listings, key=lambda l: (l.get("reviews") or 0), reverse=True)[:5]
        result["top_products"] = [
            {
                "title": (t.get("title") or "")[:100],
                "price": t.get("price", "—"),
                "rating": t.get("rating"),
                "reviews": t.get("reviews"),
                "source": t.get("source", ""),
            }
            for t in top
        ]

        result["data_status"] = (
            f"Live Google Shopping catalog snapshot via SerpAPI ({len(listings)} listings). "
            "new_products_30d and catalog_velocity require day-over-day snapshots, not yet tracked."
        )

        logger.info(
            f"[catalog] {brand_name}: total_skus={result['total_skus']}, "
            f"review_average={result['review_average']}, review_count={result['review_count']}"
        )
        return result

    except Exception as e:
        logger.error(f"[catalog] Error for {brand_name}: {e}", exc_info=True)
        result["data_status"] = f"Error: {e}"
        return result


def _calculate_brand_score(data: dict) -> int:
    """Calculate overall brand health score (0-100)."""
    try:
        scores = []

        # AI Visibility Score
        if data.get("ai_visibility", {}).get("visibility_score"):
            scores.append(data["ai_visibility"]["visibility_score"])

        # Growth Score
        if data.get("growth_signals", {}).get("growth_score"):
            scores.append(data["growth_signals"]["growth_score"])

        # Social Sentiment
        sentiment_map = {"Very Positive": 90, "Positive": 75, "Neutral": 50, "Negative": 25}
        social = data.get("social_signals", {}).get("overall_sentiment", "")
        if social in sentiment_map:
            scores.append(sentiment_map[social])

        # Catalog Health
        if data.get("catalog_intelligence", {}).get("review_average"):
            review_score = (data["catalog_intelligence"]["review_average"] / 5) * 100
            scores.append(review_score)

        if scores:
            return int(sum(scores) / len(scores))
        return 0

    except Exception as e:
        logger.debug(f"[score_calc] Error: {e}")
        return 0
