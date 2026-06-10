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


def smart_event_prep(event_title: str, weather: Dict) -> Optional[str]:
    """Suggest practical prep based on event type + weather (facts only)."""
    if not event_title or not weather:
        return None

    title_lower = event_title.lower()
    temp = weather.get("temp")
    desc = (weather.get("desc") or "").lower()

    # OUTDOOR EVENTS
    if any(w in title_lower for w in ["walk", "trip", "field", "sports", "pe", "outdoor", "playground"]):
        if "rain" in desc:
            return "🌧️ Rainy walk — bring waterproof jacket"
        if temp and temp < 10:
            return "🥶 Cold walk — wear extra layer"
        if temp and temp > 20 and "sunny" in desc:
            return "☀️ Sunny activity — bring sunscreen & water"
        if "wind" in desc:
            return "💨 Windy day — secure any loose items"

    # ASSEMBLY/INDOOR (general)
    if any(w in title_lower for w in ["assembly", "meeting", "presentation", "class"]):
        if temp and temp > 22:
            return "Warm indoors — lighter clothing works"

    return None


def format_event_with_prep(child_name: str, event_title: str, event_date: str, weather: Optional[Dict] = None) -> str:
    """Format school event with weather-based prep suggestion.

    Args:
        child_name: Child's name
        event_title: Event title (e.g., "Geography Field Trip")
        event_date: Event date (e.g., "2026-06-09")
        weather: Weather dict (optional) - if provided, adds prep suggestion

    Returns:
        Formatted event string with prep suggestion if relevant
        Example: "Inaaya has Geography Field Trip on 09/06. 🌧️ Rainy walk — bring waterproof jacket"
    """
    # Format date as DD/MM/YY
    try:
        if len(event_date) >= 10:
            parts = event_date[:10].split("-")
            date_display = f"{parts[2]}/{parts[1]}/{parts[0][-2:]}"
        else:
            date_display = event_date
    except:
        date_display = event_date

    msg = f"{child_name} has {event_title} on {date_display}"

    # Add weather prep if available
    if weather:
        prep = smart_event_prep(event_title, weather)
        if prep:
            msg += f". {prep}"

    return msg


def smart_event(events: List[Dict], now: datetime, location: Optional[str] = None, weather: Optional[Dict] = None) -> Optional[str]:
    """Next event with countdown, location-aware, + transit context + weather prep. Only future events."""
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

            # ONLY show if event is FUTURE (mins_until > 0) and within next 3 hours
            if 0 < mins_until < 180:
                child = ev.get("child_name", "").split()[0]  # First name only
                title = ev.get("event_title", "")
                location_str = ev.get("location", "")

                # Try to add transit context
                transit = None
                if location_str:
                    try:
                        from miru.brief.transit_context import transit_context_for_event
                        # Get lat/lon from event or location lookup
                        lat = ev.get("lat")
                        lon = ev.get("lon")
                        if lat and lon:
                            transit = transit_context_for_event(location_str, lat, lon)
                    except Exception:
                        pass

                # Try to add event prep suggestions (weather-based)
                prep = smart_event_prep(title, weather=weather or {})

                # Build message with transit + prep
                msg_parts = []

                # Add context if at same location
                if location and location.lower() in (location_str or "").lower():
                    msg_parts.append(f"At {location}. {child}'s {title} in {int(mins_until)} mins — need to leave soon.")
                elif location_str:
                    msg_parts.append(f"{child}'s {title} in {int(mins_until)} mins at {location_str.split(',')[0]}")
                else:
                    msg_parts.append(f"{child}'s {title} in {int(mins_until)} mins")

                # Add transit + prep suggestions
                if transit:
                    msg_parts.append(transit)
                if prep:
                    msg_parts.append(prep)

                return ". ".join(msg_parts)
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
            "event": executor.submit(smart_event, events, now, location, weather),
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
