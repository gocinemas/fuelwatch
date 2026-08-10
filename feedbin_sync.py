"""
Feedbin Sync — Pull starred links, categorize, and integrate into Miru
Stealth mode: Quietly adds to morning brief without user knowing
"""
import requests
import json
import base64
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import hashlib
import random


class FeedbinSync:
    """
    Sync starred links from Feedbin subscription
    - Fetch starred entries (articles)
    - Auto-categorize by URL/content
    - Cache locally (avoid repeated API calls)
    - Randomize for morning brief
    """

    API_BASE = "https://api.feedbin.com/v2"
    CACHE_TTL = 3600  # Cache for 1 hour

    def __init__(self, feedbin_token: str = None, feedbin_email: str = None, feedbin_password: str = None):
        """
        Initialize with Feedbin credentials.
        Prefer token over email/password.
        """
        self.token = feedbin_token
        self.email = feedbin_email
        self.password = feedbin_password

        if not (feedbin_token or (feedbin_email and feedbin_password)):
            raise ValueError("Need either Feedbin API token or email+password")

        self.cache = {}  # postcode → {entries, cached_at}
        self.categories_cache = {}  # Auto-detected categories

    def _get_auth_header(self) -> Dict:
        """Get Authorization header"""
        if self.token:
            return {"Authorization": f"Bearer {self.token}"}
        else:
            # Basic auth
            creds = base64.b64encode(f"{self.email}:{self.password}".encode()).decode()
            return {"Authorization": f"Basic {creds}"}

    def fetch_starred_entries(self) -> List[Dict]:
        """Fetch last 50 starred entries from Feedbin"""
        try:
            url = f"{self.API_BASE}/starred_entries.json"
            headers = self._get_auth_header()

            # Get first page of starred IDs (~100)
            response = requests.get(url, headers=headers, params={"page": 1}, timeout=10)
            response.raise_for_status()

            entry_ids = response.json()
            print(f"[Feedbin] Fetched {len(entry_ids)} starred entry IDs, taking first 50")

            # Only use first 50 to avoid URL length issues
            entry_ids = entry_ids[:50]

            if not entry_ids:
                return []

            # Fetch full entry details for all IDs in one request
            entries_url = f"{self.API_BASE}/entries.json"
            entries_params = {"ids": ",".join(str(id) for id in entry_ids)}
            entries_response = requests.get(entries_url, headers=headers, params=entries_params, timeout=10)
            entries_response.raise_for_status()

            entries = entries_response.json()
            print(f"[Feedbin] Got {len(entries)} full entry details")
            return entries

        except Exception as e:
            print(f"[Feedbin] Error fetching entries: {e}")
            return []

    def sync_all_starred(self) -> List[Dict]:
        """Fetch last 100 starred entries"""
        entries = self.fetch_starred_entries()
        print(f"[Feedbin] Synced {len(entries)} starred entries")
        return entries

    def categorize_entry(self, entry: Dict) -> str:
        """Auto-categorize entry by URL/content"""
        url = entry.get("url", "").lower()
        title = entry.get("title", "").lower()
        content = entry.get("summary", "").lower()

        # Check URL patterns
        patterns = {
            "🔬 Science": ["arxiv", "science", "research", "study", "paper"],
            "💻 Tech": ["github", "dev", "code", "programming", "tech", "python", "javascript"],
            "📰 News": ["news", "times", "bbc", "cnn", "reuters"],
            "🎨 Design": ["design", "ux", "ui", "figma", "dribbble"],
            "📚 Learning": ["tutorial", "course", "learn", "education"],
            "🎬 Media": ["youtube", "video", "podcast", "spotify"],
            "💼 Business": ["startup", "venture", "business", "market"],
            "🌍 Travel": ["travel", "hotel", "flight", "destination"],
            "🍔 Food": ["recipe", "food", "restaurant", "cook"],
            "⚽ Sports": ["sports", "soccer", "basketball", "cricket"],
        }

        text = f"{url} {title} {content}"

        for category, keywords in patterns.items():
            if any(kw in text for kw in keywords):
                return category

        return "📌 Saved"  # Default

    def extract_source(self, entry: Dict) -> str:
        """Extract newsletter/source name from entry"""
        url = entry.get("url", "").lower()
        feed_title = entry.get("feed_title", "").lower()
        author = entry.get("author", "").lower()

        # Try to extract from feed title first
        if feed_title:
            return feed_title.split(" - ")[0].strip()[:30]

        # Extract domain from URL
        if url:
            import re
            match = re.search(r'https?://(?:www\.)?([^/]+)', url)
            if match:
                domain = match.group(1)
                # Clean up domain
                domain = domain.replace("www.", "").split(".")[0]
                return domain.capitalize()

        # Fallback to author
        if author:
            return author[:30]

        return "Unknown"

    def categorize_all(self, entries: List[Dict]) -> Dict[str, List[Dict]]:
        """Group entries by category"""
        categorized = {}

        for entry in entries:
            category = self.categorize_entry(entry)
            if category not in categorized:
                categorized[category] = []

            categorized[category].append({
                "id": entry.get("id"),
                "title": entry.get("title", "Untitled"),
                "url": entry.get("url", ""),
                "summary": entry.get("summary", "")[:200],  # Truncate
                "author": entry.get("author", ""),
                "published": entry.get("published", ""),
                "category": category,
                "source": self.extract_source(entry),
            })

        return categorized

    def get_random_links(self, entries: List[Dict], count: int = 3) -> List[Dict]:
        """Get N random links from entries"""
        if not entries:
            return []

        # Extract URLs from all entries
        links = []
        for entry in entries:
            if entry.get("url"):
                links.append(entry)

        # Shuffle and return top N
        random.shuffle(links)
        return links[:count]

    def search_entries(self, entries: List[Dict], query: str) -> List[Dict]:
        """Search entries by title/summary/author"""
        q = query.lower()
        results = []

        for entry in entries:
            title = entry.get("title", "").lower()
            summary = entry.get("summary", "").lower()
            author = entry.get("author", "").lower()

            if q in title or q in summary or q in author:
                results.append(entry)

        return results

    def get_morning_brief_links(self, count: int = 3) -> List[Dict]:
        """Get random links for morning brief"""
        try:
            # Fetch fresh from Feedbin
            entries = self.sync_all_starred()

            if not entries:
                print("[Feedbin] No starred entries found")
                return []

            # Get random selection
            random_links = self.get_random_links(entries, count)

            print(f"[Feedbin] Selected {len(random_links)} random links for brief")
            return random_links

        except Exception as e:
            print(f"[Feedbin] Error getting morning links: {e}")
            return []


# Singleton
_feedbin = None


def get_feedbin(feedbin_token: str = None) -> Optional[FeedbinSync]:
    """Get or create Feedbin sync instance"""
    global _feedbin
    import os

    if not _feedbin:
        token = feedbin_token or os.getenv("FEEDBIN_API_TOKEN")
        email = os.getenv("FEEDBIN_EMAIL")
        password = os.getenv("FEEDBIN_PASSWORD")

        if token:
            try:
                _feedbin = FeedbinSync(feedbin_token=token)
            except Exception as e:
                print(f"[Feedbin] Failed to initialize with token: {e}")
                return None
        elif email and password:
            try:
                _feedbin = FeedbinSync(feedbin_email=email, feedbin_password=password)
            except Exception as e:
                print(f"[Feedbin] Failed to initialize with email/password: {e}")
                return None

    return _feedbin
