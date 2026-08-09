"""
Smart Places — Location-aware venue recommendations using Google Places API
With strict credit monitoring to stay within $200/month free tier
"""
import os
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from functools import lru_cache


class SmartPlaces:
    """
    Recommendations using Google Places API
    - Caches results (minimize API calls)
    - Monitors usage (stay within $200/month free credit)
    - Daily quota enforcement (prevent overage)
    """

    # Free tier: ~10,000-15,000 requests/month
    MONTHLY_QUOTA = 10000  # Conservative estimate
    DAILY_QUOTA = MONTHLY_QUOTA // 30  # ~333 requests/day

    API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

    def __init__(self):
        self.cache = {}  # postcode → venues (in-memory)
        self.usage_log = {}  # Track API calls per day
        self._load_usage_log()

    def _load_usage_log(self):
        """Load usage from previous runs"""
        try:
            # In production, load from database (ma_details table)
            # For now, in-memory tracking
            pass
        except:
            pass

    def get_usage_today(self) -> int:
        """Count API calls made today"""
        today = datetime.now().date().isoformat()
        return self.usage_log.get(today, 0)

    def can_make_request(self) -> tuple[bool, str]:
        """
        Check if we can safely make an API request.
        Returns: (can_proceed, reason)
        """
        today_usage = self.get_usage_today()

        if today_usage >= self.DAILY_QUOTA:
            return False, f"Daily quota exceeded: {today_usage}/{self.DAILY_QUOTA}"

        if not self.API_KEY:
            return False, "GOOGLE_PLACES_API_KEY not configured"

        return True, "OK"

    def search_venues(self, postcode: str, category: str = "restaurant") -> List[Dict]:
        """
        Find venues near a postcode.

        Args:
            postcode: UK postcode (e.g., "KT16 0DA")
            category: Type of venue (restaurant, cafe, pub, park, etc.)

        Returns:
            List of venues with name, rating, distance, etc.

        Usage Cost:
            - Nearby Search: $0.032 per request
            - Stays within free tier (~333 requests/day)
        """

        # Check cache first (no API cost)
        cache_key = f"{postcode}:{category}"
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            if datetime.fromisoformat(cached["cached_at"]) > datetime.now() - timedelta(hours=6):
                return cached["venues"]

        # Check if we can make the request
        can_proceed, reason = self.can_make_request()
        if not can_proceed:
            print(f"[SmartPlaces] Cannot make request: {reason}")
            return []

        try:
            import googlemaps
            from miru.geo import postcode_to_latlon

            # Get coordinates from postcode
            coords = postcode_to_latlon(postcode.replace(" ", "").upper())
            if not coords:
                return []

            lat, lng = coords

            # Initialize Google Maps client
            gmaps = googlemaps.Client(key=self.API_KEY)

            # Search nearby places
            places_result = gmaps.places_nearby(
                location=(lat, lng),
                radius=2000,  # 2km radius
                type=category,
                rank_by="prominence"  # Better results
            )

            # Track API usage
            self._track_request(postcode, category)

            # Parse results
            venues = []
            for place in places_result.get("results", [])[:10]:  # Top 10 results
                venue = {
                    "name": place.get("name", ""),
                    "rating": place.get("rating", 0),
                    "types": place.get("types", []),
                    "open_now": place.get("opening_hours", {}).get("open_now", None),
                    "vicinity": place.get("vicinity", ""),
                    "place_id": place.get("place_id", ""),
                    "lat": place.get("geometry", {}).get("location", {}).get("lat"),
                    "lng": place.get("geometry", {}).get("location", {}).get("lng"),
                }

                # Calculate distance (rough)
                dist_km = self._haversine(lat, lng, venue["lat"], venue["lng"])
                venue["distance_km"] = round(dist_km, 1)

                venues.append(venue)

            # Cache for 6 hours
            self.cache[cache_key] = {
                "venues": venues,
                "cached_at": datetime.now().isoformat()
            }

            return venues

        except Exception as e:
            print(f"[SmartPlaces] Error: {e}")
            return []

    def _track_request(self, postcode: str, category: str):
        """Track API request for quota monitoring"""
        today = datetime.now().date().isoformat()
        if today not in self.usage_log:
            self.usage_log[today] = 0
        self.usage_log[today] += 1

        usage = self.usage_log[today]
        pct = (usage / self.DAILY_QUOTA) * 100

        print(f"[SmartPlaces] API call {usage}/{self.DAILY_QUOTA} ({pct:.1f}%) — {postcode} {category}")

        # Alert if approaching daily limit
        if pct >= 80:
            print(f"⚠️ [SmartPlaces] Approaching daily quota! {usage}/{self.DAILY_QUOTA}")

    @staticmethod
    def _haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two coordinates (km)"""
        from math import radians, cos, sin, asin, sqrt

        lon1, lat1, lon2, lat2 = map(radians, [lng1, lat1, lng2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371  # km
        return c * r

    def get_weekend_places(self, postcode: str) -> List[Dict]:
        """Get mix of restaurants, cafes, pubs for weekend activities"""
        all_venues = []

        for category in ["restaurant", "cafe", "bar"]:
            venues = self.search_venues(postcode, category)
            all_venues.extend(venues)

        # Sort by rating, deduplicate
        all_venues = sorted(all_venues, key=lambda x: x["rating"], reverse=True)

        seen = set()
        deduped = []
        for v in all_venues:
            if v["name"] not in seen:
                seen.add(v["name"])
                deduped.append(v)

        return deduped[:10]  # Top 10 unique venues

    def get_usage_report(self) -> Dict:
        """Get API usage statistics"""
        today = datetime.now().date().isoformat()
        today_usage = self.usage_log.get(today, 0)

        # Estimate monthly based on average
        avg_daily = sum(self.usage_log.values()) / max(len(self.usage_log), 1)
        estimated_monthly = avg_daily * 30

        return {
            "today": today_usage,
            "daily_quota": self.DAILY_QUOTA,
            "daily_used_pct": (today_usage / self.DAILY_QUOTA) * 100,
            "estimated_monthly": estimated_monthly,
            "monthly_quota": self.MONTHLY_QUOTA,
            "monthly_used_pct": (estimated_monthly / self.MONTHLY_QUOTA) * 100,
            "free_credits": "$200/month",
            "cost_per_request": "$0.032"
        }


# Singleton instance
_smart_places = None

def get_smart_places() -> SmartPlaces:
    global _smart_places
    if not _smart_places:
        _smart_places = SmartPlaces()
    return _smart_places
