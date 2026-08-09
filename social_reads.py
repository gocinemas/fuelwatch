"""
Social Reads — Aggregate tweets, articles, and links for morning brief
Combines Feedbin + Twitter/social content into beautiful brief snippet
"""
import random
from typing import List, Dict, Optional
from datetime import datetime


class SocialReads:
    """
    Aggregate social content (tweets, articles) for morning brief
    - Fetch random Feedbin links
    - Fetch random tweets
    - Format nicely for WhatsApp/brief
    """

    def __init__(self, feedbin_sync=None, twitter_api=None):
        self.feedbin = feedbin_sync
        self.twitter = twitter_api

    def get_morning_snippet(self, count_links: int = 2, count_tweets: int = 3) -> str:
        """
        Get formatted morning social reads snippet

        Returns beautiful text like:
        📖 Today's Reading
        • "How to Build AI" → https://...
        • "JavaScript Tips" → https://...

        💬 Trending Tweets
        • "Just shipped..." - @user
        • "Excited about..." - @user
        """

        lines = []

        # Feedbin section
        try:
            if self.feedbin:
                entries = self.feedbin.sync_all_starred()
                random_links = self.feedbin.get_random_links(entries, count_links)

                if random_links:
                    lines.append("📖 Today's Reading")
                    for link in random_links:
                        title = link.get("title", "Untitled")[:50]
                        url = link.get("url", "")
                        # Shorten URL
                        url_short = url.replace("https://", "").split("?")[0][:30]
                        lines.append(f"  • {title}")
                        lines.append(f"    {url_short}...")
                    lines.append("")
        except Exception as e:
            print(f"[SocialReads] Feedbin error: {e}")

        # Twitter section (placeholder for now)
        tweets = self.get_sample_tweets(count_tweets)
        if tweets:
            lines.append("💬 Trending Tweets")
            for tweet in tweets:
                text = tweet.get("text", "")[:80]
                author = tweet.get("author", "User")
                lines.append(f"  • {text}...")
                lines.append(f"    @{author}")
            lines.append("")

        return "\n".join(lines).strip()

    def get_sample_tweets(self, count: int = 3) -> List[Dict]:
        """Get sample tweets (placeholder - integrate with Twitter API later)"""
        # Mock data for now - replace with real Twitter API integration
        sample_tweets = [
            {
                "text": "Just shipped a new feature that will blow your mind 🚀",
                "author": "shippy",
                "likes": 245,
                "timestamp": datetime.now().isoformat()
            },
            {
                "text": "AI is becoming more powerful every day. Excited to see what's next!",
                "author": "aitrendings",
                "likes": 1204,
                "timestamp": datetime.now().isoformat()
            },
            {
                "text": "Building in public is the best way to learn and grow 📈",
                "author": "builder",
                "likes": 567,
                "timestamp": datetime.now().isoformat()
            },
            {
                "text": "Coffee + Coding = Productivity ☕",
                "author": "devlife",
                "likes": 892,
                "timestamp": datetime.now().isoformat()
            },
            {
                "text": "The future is decentralized 🔗",
                "author": "web3dev",
                "likes": 445,
                "timestamp": datetime.now().isoformat()
            },
        ]

        random.shuffle(sample_tweets)
        return sample_tweets[:count]

    def format_for_whatsapp(self, snippet: str) -> str:
        """Format snippet for WhatsApp (shorter, emoji-heavy)"""
        return snippet  # WhatsApp-friendly format


def get_social_reads(feedbin=None, twitter=None) -> SocialReads:
    """Get or create SocialReads instance"""
    return SocialReads(feedbin_sync=feedbin, twitter_api=twitter)
