"""Data fetchers — real-time and cached data sources."""

from abc import ABC, abstractmethod
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


class DataFetcher(ABC):
    """Base class for all data fetchers.

    Attributes:
        cache_ttl_seconds: 0 = never cache, 3600 = cache 1 hour
        is_realtime: True = never cache, False = respects TTL
        timeout_seconds: Max time to wait for fetch
    """

    cache_ttl_seconds: int = 0
    is_realtime: bool = False
    timeout_seconds: int = 6

    @abstractmethod
    def fetch(self, **kwargs) -> Any:
        """Fetch data. Return None or empty on failure, never throw."""
        pass


class TrainFetcher(DataFetcher):
    """Real-time train departures."""

    is_realtime = True
    timeout_seconds = 6

    def fetch(self, from_code: str, to_code: str) -> Optional[list]:
        """Fetch train departures.

        Args:
            from_code: Station code (e.g., "SUR")
            to_code: Station code (e.g., "WAT")

        Returns:
            List of Train objects, or None on failure
        """
        try:
            from ..core.types import Train
            # TODO: Call actual train API
            # For now, return empty (will be wired to existing _v2_fetch_trains)
            return []
        except Exception as e:
            logger.error(f"TrainFetcher error: {e}")
            return None


class FuelFetcher(DataFetcher):
    """Real-time fuel prices."""

    is_realtime = True
    timeout_seconds = 6

    def fetch(self, postcode: str) -> Optional[list]:
        """Fetch fuel prices near postcode.

        Args:
            postcode: UK postcode

        Returns:
            List of Fuel objects, or None on failure
        """
        try:
            from ..core.types import Fuel
            # TODO: Call actual fuel API
            # For now, return empty
            return []
        except Exception as e:
            logger.error(f"FuelFetcher error: {e}")
            return None


class WeatherFetcher(DataFetcher):
    """Real-time weather."""

    is_realtime = True
    timeout_seconds = 6

    def fetch(self, postcode: str) -> Optional[dict]:
        """Fetch weather for postcode.

        Args:
            postcode: UK postcode

        Returns:
            Weather object, or None on failure
        """
        try:
            from ..core.types import Weather
            # TODO: Call actual weather API
            return None
        except Exception as e:
            logger.error(f"WeatherFetcher error: {e}")
            return None


class SchoolEventsFetcher(DataFetcher):
    """Cached school events."""

    cache_ttl_seconds = 900  # 15 minutes
    is_realtime = False
    timeout_seconds = 30

    def fetch(self, from_number: str, days_ahead: int = 30) -> Optional[list]:
        """Fetch upcoming school events.

        Args:
            from_number: User phone number
            days_ahead: How many days to look ahead

        Returns:
            List of SchoolEvent objects, or None on failure
        """
        try:
            from ..core.types import SchoolEvent
            # TODO: Call Supabase school_events table
            return []
        except Exception as e:
            logger.error(f"SchoolEventsFetcher error: {e}")
            return None


class SpendFetcher(DataFetcher):
    """Cached monthly spending."""

    cache_ttl_seconds = 900  # 15 minutes
    is_realtime = False
    timeout_seconds = 10

    def fetch(self, from_number: str, month_offset: int = 0) -> Optional[dict]:
        """Fetch monthly spending.

        Args:
            from_number: User phone number
            month_offset: 0 = current month, -1 = last month

        Returns:
            Spend object, or None on failure
        """
        try:
            from ..core.types import Spend
            # TODO: Call Supabase receipts table & aggregate
            return None
        except Exception as e:
            logger.error(f"SpendFetcher error: {e}")
            return None
