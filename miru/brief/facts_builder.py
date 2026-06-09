"""FactsBuilder — orchestrate parallel data fetching."""

from typing import Optional
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from ..core.types import BriefContext
from ..data.cache import get_cache
from ..data.fetchers import (
    TrainFetcher,
    FuelFetcher,
    WeatherFetcher,
    SchoolEventsFetcher,
    SpendFetcher,
)
import logging

logger = logging.getLogger(__name__)


class FactsBuilder:
    """Build brief context by fetching data in parallel.

    Fetches with timeouts so one slow API doesn't block everything.
    """

    TOTAL_TIMEOUT = 8  # seconds for all fetches combined

    @staticmethod
    def build(
        from_number: str,
        postcode: str,
        mode: str = "office",
    ) -> BriefContext:
        """Build brief context with parallel data fetching.

        Args:
            from_number: User phone number
            postcode: User's postcode
            mode: "wfh", "office", "out"

        Returns:
            BriefContext with all available data
        """
        context = BriefContext()
        cache = get_cache()

        # Launch all fetches in parallel
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                "trains": executor.submit(
                    cache.get_or_fetch,
                    f"trains_{postcode}",
                    lambda: TrainFetcher().fetch(postcode, postcode),
                    ttl_seconds=300,
                ),
                "fuel": executor.submit(
                    cache.get_or_fetch,
                    f"fuel_{postcode}",
                    lambda: FuelFetcher().fetch(postcode),
                    ttl_seconds=300,
                ),
                "weather": executor.submit(
                    cache.get_or_fetch,
                    f"weather_{postcode}",
                    lambda: WeatherFetcher().fetch(postcode),
                    ttl_seconds=600,
                ),
                "school": executor.submit(
                    cache.get_or_fetch,
                    f"school_{from_number}",
                    lambda: SchoolEventsFetcher().fetch(from_number),
                    ttl_seconds=900,
                ),
                "spend": executor.submit(
                    cache.get_or_fetch,
                    f"spend_{from_number}",
                    lambda: SpendFetcher().fetch(from_number),
                    ttl_seconds=900,
                ),
            }

            # Wait for all with timeout
            done, pending = wait(
                futures.values(),
                timeout=FactsBuilder.TOTAL_TIMEOUT,
            )

            # Collect results
            for name, future in futures.items():
                if future in done:
                    try:
                        result = future.result(timeout=1)
                        setattr(context, name, result)
                        logger.debug(f"Fetched {name}: {type(result).__name__}")
                    except Exception as e:
                        logger.warning(f"Error fetching {name}: {e}")
                else:
                    logger.warning(f"Timeout/pending: {name}")

        return context
