"""SmartContext — minimal, bulletproof intelligent inference.

Runs key agents in parallel, each returns 1 clean sentence max.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


def smart_weather(weather: Dict, now: datetime) -> Optional[str]:
    """One sentence of practical weather advice."""
    if not weather:
        return None

    temp = weather.get("temp")
    desc = (weather.get("desc") or "").lower()

    # Just the most critical thing
    if "rain" in desc:
        return "Rainy — bring umbrella"
    if temp and temp < 10:
        return "Cold — wrap up warm"
    if temp and temp > 20 and "sunny" in desc:
        return f"Sunny {temp}°C — good weather"

    return None


def smart_event(events: List[Dict], now: datetime, location: Optional[str] = None) -> Optional[str]:
    """Next event with countdown, location-aware."""
    if not events:
        return None

    today = now.strftime("%Y-%m-%d")

    for ev in events:
        if ev.get("event_date") != today:
            continue

        try:
            event_time_str = ev.get("time", "")  # e.g. "3:52pm"
            event_time = datetime.strptime(event_time_str, "%I:%M%p").time()
            event_dt = datetime.combine(now.date(), event_time)

            mins_until = (event_dt - now).total_seconds() / 60

            # Show if 0 < mins < 180 (next 3 hours)
            if 0 < mins_until < 180:
                child = ev.get("child_name", "").split()[0]  # First name only
                title = ev.get("event_title", "")
                location_str = ev.get("location", "")

                # Add context if at same location
                if location and location.lower() in (location_str or "").lower():
                    return f"At {location}. {child}'s {title} in {int(mins_until)} mins — need to leave soon."

                # If location nearby
                if location_str:
                    return f"{child}'s {title} in {int(mins_until)} mins at {location_str.split(',')[0]}"

                return f"{child}'s {title} in {int(mins_until)} mins"
        except (ValueError, AttributeError):
            continue

    return None


def smart_location_context(location: Optional[str]) -> Optional[str]:
    """Simply state where user is."""
    if location:
        return f"You are at {location}"
    return None


def smart_time(now: datetime, location: Optional[str]) -> Optional[str]:
    """Time-of-day context."""
    hour = now.hour

    # School pickup windows
    if 14 <= hour < 17:
        return "School pickup time coming up"

    # Dinner time
    if 17 <= hour < 21:
        return "Dinner time"

    return None


def smart_location(location: Optional[str], receipts: List[Dict]) -> Optional[str]:
    """What they bought at this location last time."""
    if not location or not receipts:
        return None

    location_lower = location.lower()

    # Find receipts at this location
    for receipt in receipts:
        merchant = (receipt.get("merchant") or "").lower()
        if location_lower in merchant:
            items = receipt.get("items", [])
            if items:
                items_str = ", ".join(items[:2])  # Top 2 items
                return f"Last time at {location}: {items_str}"

    return None


def smart_context_brief(
    now: datetime,
    weather: Dict,
    events: List[Dict],
    receipts: List[Dict],
    location: Optional[str],
) -> str:
    """Run key agents in parallel, return combined smart brief.

    Returns:
        1-3 sentence brief combining weather + event + location context
    """

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            "weather": executor.submit(smart_weather, weather, now),
            "event": executor.submit(smart_event, events, now, location),
            "location": executor.submit(smart_location, location, receipts),
            "location_context": executor.submit(smart_location_context, location),
            "time": executor.submit(smart_time, now, location),
        }

        results = {}
        for name, future in futures.items():
            try:
                result = future.result(timeout=1)
                if result:
                    results[name] = result
            except Exception as e:
                logger.debug(f"[smart_context] {name} failed: {e}")

    # Priority: event > location_context > location_history > time > weather
    parts = []
    for priority in ["event", "location_context", "location", "time", "weather"]:
        if priority in results:
            parts.append(results[priority])

    combined = ". ".join(parts[:3])
    if combined and not combined.endswith("."):
        combined += "."

    return combined
