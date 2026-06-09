"""Data caching layer with TTL support."""

from typing import Any, Callable, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DataCache:
    """Simple TTL-based cache for fetched data."""

    def __init__(self):
        self._cache = {}

    def get_or_fetch(
        self,
        key: str,
        fetcher_func: Callable,
        ttl_seconds: int,
        **kwargs
    ) -> Any:
        """Get cached data or fetch fresh.

        Args:
            key: Cache key (e.g., "fuel_KT15")
            fetcher_func: Function to call if not cached
            ttl_seconds: Time-to-live (0 = always fetch)
            **kwargs: Arguments to pass to fetcher_func

        Returns:
            Cached or fresh data (or None if fetch fails)
        """
        # Never cache
        if ttl_seconds == 0:
            return self._fetch(key, fetcher_func, **kwargs)

        # Check cache
        if key in self._cache:
            cached_data, cached_time = self._cache[key]
            age_seconds = (datetime.now() - cached_time).total_seconds()

            if age_seconds < ttl_seconds:
                logger.debug(f"Cache HIT: {key} ({age_seconds:.0f}s old)")
                return cached_data

            logger.debug(f"Cache MISS: {key} ({age_seconds:.0f}s old, TTL={ttl_seconds}s)")

        # Fetch fresh
        return self._fetch_and_cache(key, fetcher_func, ttl_seconds, **kwargs)

    def _fetch(self, key: str, fetcher_func: Callable, **kwargs) -> Any:
        """Fetch without caching."""
        try:
            return fetcher_func(**kwargs)
        except Exception as e:
            logger.error(f"Fetch error for {key}: {e}")
            return None

    def _fetch_and_cache(
        self,
        key: str,
        fetcher_func: Callable,
        ttl_seconds: int,
        **kwargs
    ) -> Any:
        """Fetch and store in cache."""
        result = self._fetch(key, fetcher_func, **kwargs)

        if result is not None:
            self._cache[key] = (result, datetime.now())
            logger.debug(f"Cached: {key} (TTL={ttl_seconds}s)")

        return result

    def invalidate(self, key: str) -> None:
        """Clear cache for a key."""
        if key in self._cache:
            del self._cache[key]
            logger.debug(f"Cache invalidated: {key}")

    def clear(self) -> None:
        """Clear entire cache."""
        self._cache.clear()
        logger.debug("Cache cleared")

    def stats(self) -> dict:
        """Cache statistics."""
        return {
            "cached_keys": len(self._cache),
            "keys": list(self._cache.keys()),
        }


# Global cache instance
_global_cache = DataCache()


def get_cache() -> DataCache:
    """Get global cache instance."""
    return _global_cache
