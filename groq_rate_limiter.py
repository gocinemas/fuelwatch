"""
Groq Rate Limiter - Prevent 429 errors by batching requests

The problem: School comms tries to parse 5-10 emails at once → hits 30k TPM limit
The solution: Queue + delay between requests to stay under limit
"""

import time
import threading
from collections import deque
from typing import Callable, Any, Optional
from datetime import datetime, timedelta

class GroqRateLimiter:
    """Rate limiter for Groq API calls (30k TPM limit)."""

    def __init__(self, tokens_per_minute: int = 30000, buffer_percent: int = 80):
        """
        Initialize rate limiter.

        Args:
            tokens_per_minute: TPM limit (30000 for free tier)
            buffer_percent: Stay under this % of limit (80 = use max 24k TPM)
        """
        self.tpm_limit = tokens_per_minute
        self.safe_limit = int(tokens_per_minute * (buffer_percent / 100))
        self.request_queue = deque()
        self.tokens_used_this_minute = 0
        self.minute_start = datetime.now()
        self.lock = threading.Lock()
        self.last_request_time = 0

    def reset_minute_counter(self):
        """Reset token counter every minute."""
        now = datetime.now()
        if (now - self.minute_start).total_seconds() >= 60:
            self.tokens_used_this_minute = 0
            self.minute_start = now

    def wait_if_needed(self, estimated_tokens: int = 1000):
        """
        Wait if necessary to avoid rate limit.

        Args:
            estimated_tokens: Estimated tokens for next request
        """
        with self.lock:
            self.reset_minute_counter()

            # If this request would exceed limit, wait
            while self.tokens_used_this_minute + estimated_tokens > self.safe_limit:
                time_since_minute_start = (datetime.now() - self.minute_start).total_seconds()
                time_to_wait = 60 - time_since_minute_start + 1

                if time_to_wait > 0:
                    print(f"[groq-limiter] ⏳ Rate limit approaching. Waiting {time_to_wait:.1f}s...")
                    time.sleep(min(time_to_wait, 2))  # Max 2s wait per check
                    self.reset_minute_counter()
                else:
                    break

            # Add delay between requests (minimum 0.5s between calls)
            now = time.time()
            time_since_last = now - self.last_request_time
            if time_since_last < 0.5:
                time.sleep(0.5 - time_since_last)

            self.last_request_time = time.time()
            self.tokens_used_this_minute += estimated_tokens

    def record_tokens(self, tokens_used: int):
        """Record actual tokens used after request."""
        with self.lock:
            self.tokens_used_this_minute += max(0, tokens_used - 1000)  # Already counted estimate

    def call_with_rate_limit(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        estimated_tokens: int = 1000,
        max_retries: int = 3
    ) -> Optional[Any]:
        """
        Call function with automatic rate limiting and retry.

        Args:
            func: Function to call (e.g., Groq API call)
            args: Positional arguments
            kwargs: Keyword arguments
            estimated_tokens: Estimated tokens for this request
            max_retries: Retry on rate limit errors

        Returns:
            Function result or None if failed
        """
        kwargs = kwargs or {}
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Wait if necessary
                self.wait_if_needed(estimated_tokens)

                # Make the call
                result = func(*args, **kwargs)

                # Record actual tokens (if available in response)
                if hasattr(result, 'usage') and hasattr(result.usage, 'total_tokens'):
                    self.record_tokens(result.usage.total_tokens)

                return result

            except Exception as e:
                error_str = str(e)

                # Check if it's a rate limit error
                if "429" in error_str or "rate_limit" in error_str.lower():
                    retry_count += 1
                    wait_time = min(60, 2 ** retry_count)  # Exponential backoff: 2s, 4s, 8s
                    print(
                        f"[groq-limiter] ⚠️  Rate limited (attempt {retry_count}/{max_retries}). "
                        f"Waiting {wait_time}s before retry..."
                    )
                    time.sleep(wait_time)
                    continue

                # Not a rate limit error, re-raise
                raise

        print(f"[groq-limiter] ❌ Failed after {max_retries} retries due to rate limiting")
        return None


# Global rate limiter instance
_groq_limiter = GroqRateLimiter()


def rate_limited_groq_call(func, *args, **kwargs):
    """
    Convenience wrapper for rate-limited Groq calls.

    Usage:
        result = rate_limited_groq_call(client.messages.create,
                                       model="...",
                                       max_tokens=...,
                                       estimated_tokens=2500)
    """
    estimated_tokens = kwargs.pop("estimated_tokens", 1000)
    return _groq_limiter.call_with_rate_limit(func, args, kwargs, estimated_tokens)


def batch_groq_calls(
    requests: list,
    func: Callable,
    batch_size: int = 1,
    delay_between_batches: float = 2.0
) -> list:
    """
    Process multiple Groq requests with batching and delays.

    Args:
        requests: List of request kwargs (dicts)
        func: Groq function to call (e.g., client.messages.create)
        batch_size: How many requests per batch (1 = sequential)
        delay_between_batches: Delay in seconds between batches

    Returns:
        List of results (None for failed requests)
    """
    results = []

    for i in range(0, len(requests), batch_size):
        batch = requests[i : i + batch_size]

        print(f"[groq-limiter] Processing batch {i // batch_size + 1}/ {(len(requests) + batch_size - 1) // batch_size}...")

        for request_kwargs in batch:
            estimated = request_kwargs.pop("estimated_tokens", 1000)
            result = _groq_limiter.call_with_rate_limit(
                func,
                kwargs=request_kwargs,
                estimated_tokens=estimated
            )
            results.append(result)

        # Delay between batches
        if i + batch_size < len(requests):
            print(f"[groq-limiter] ⏳ Waiting {delay_between_batches}s before next batch...")
            time.sleep(delay_between_batches)

    return results


# Example usage
if __name__ == "__main__":
    print("Groq Rate Limiter Demo")
    print("=" * 50)
    print("This module prevents 429 rate limit errors by:")
    print("  1. Tracking tokens per minute (30k limit)")
    print("  2. Delaying requests if approaching limit")
    print("  3. Retrying with exponential backoff on 429")
    print("  4. Batching multiple requests sequentially")
    print()
    print("Usage in school_service.py:")
    print("  from groq_rate_limiter import batch_groq_calls")
    print("  results = batch_groq_calls(email_list, groq_parse_func)")
