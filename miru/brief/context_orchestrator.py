"""ContextOrchestrator — parallel intelligent inference from real data.

Runs 5 agents in parallel:
1. WeatherAgent — practical weather advice based on conditions
2. TimeAgent — time-of-day context (meals, school pickups, activities)
3. LocationAgent — location-based history from receipts
4. EventAgent — upcoming events + timing/logistics
5. ServiceAgent — relevant nearby services based on time/weather/location
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class WeatherAgent:
    """Practical weather advice from actual conditions."""

    @staticmethod
    def infer(weather: Dict, time_mode: str) -> Optional[str]:
        """Generate weather-based advice.

        Args:
            weather: {"temp": 17, "desc": "Rainy", "wind": 24}
            time_mode: "morning_commute", "daytime", "evening", "night"

        Returns:
            Practical advice or None
        """
        if not weather or not weather.get("temp"):
            return None

        temp = weather["temp"]
        desc = (weather.get("desc") or "").lower()
        wind = weather.get("wind", 0)

        advice = []

        # Rainy/wet
        if any(w in desc for w in ["rain", "shower", "drizzle", "wet"]):
            advice.append("Rainy — bring umbrella")

        # Cold
        if temp < 10:
            advice.append("Cold — wrap up warm")
        elif temp < 15:
            advice.append("Chilly — layers recommended")

        # Wind
        if wind > 20:
            advice.append(f"Windy {wind}km/h — watch for delays")

        # Sunny/good weather
        if any(w in desc for w in ["sunny", "clear"]) and temp > 15:
            advice.append(f"Nice weather {temp}°C — good for outdoors")

        return ". ".join(advice) if advice else None


class TimeAgent:
    """Time-of-day context — meals, pickups, activities."""

    @staticmethod
    def infer(
        now: datetime,
        events: List[Dict],
        time_mode: str,
    ) -> Optional[str]:
        """Generate time-based context.

        Args:
            now: current datetime
            events: upcoming school/activity events
            time_mode: "morning_commute", "daytime", "evening"

        Returns:
            Time-based context or None
        """
        hour = now.hour
        advice = []

        # Morning (7-10am) - school run
        if 7 <= hour < 10:
            # Check for school events today
            for ev in events:
                if ev.get("event_date") == now.strftime("%Y-%m-%d"):
                    return f"School run coming up — {ev.get('event_title')} today"

        # Midday (11am-2pm) - lunch
        if 11 <= hour < 14:
            advice.append("Lunch time")

        # Afternoon (2-5pm) - school pickups
        if 14 <= hour < 17:
            # Find events happening soon (within next 2 hours)
            for ev in events:
                if ev.get("event_date") == now.strftime("%Y-%m-%d"):
                    event_time_str = ev.get("time", "")
                    try:
                        # Parse time like "3:52pm"
                        event_time = datetime.strptime(event_time_str, "%I:%M%p").time()
                        mins_until = (
                            datetime.combine(now.date(), event_time) - now
                        ).total_seconds() / 60
                        if 0 < mins_until < 120:
                            advice.append(
                                f"{ev.get('child_name')}'s {ev.get('event_title')} in {int(mins_until)} mins"
                            )
                    except (ValueError, AttributeError):
                        pass

        # Evening (5-9pm) - dinner
        if 17 <= hour < 21:
            advice.append("Dinner time — nearby restaurants/takeaways")

        return ". ".join(advice) if advice else None


class LocationAgent:
    """Location-based history from receipts."""

    @staticmethod
    def infer(location: Optional[str], receipts: List[Dict]) -> Optional[str]:
        """Show what they've bought at this location before.

        Args:
            location: current location name (e.g. "Costa")
            receipts: list of receipt records with merchant + items

        Returns:
            History string or None
        """
        if not location or not receipts:
            return None

        # Find receipts at this location
        location_lower = location.lower()
        matching = [r for r in receipts if location_lower in (r.get("merchant") or "").lower()]

        if not matching:
            return None

        # Get most recent
        most_recent = matching[0]  # Assume sorted by date
        items = most_recent.get("items", [])
        if not items:
            return None

        items_str = ", ".join(items[:3])  # Top 3 items
        return f"Last time at {location}: {items_str}"


class EventAgent:
    """Upcoming events + logistics."""

    @staticmethod
    def infer(
        now: datetime,
        events: List[Dict],
        location: Optional[Dict] = None,
    ) -> Optional[str]:
        """Show critical upcoming events + travel time.

        Args:
            now: current datetime
            events: upcoming events
            location: current location {"lat": x, "lng": y}

        Returns:
            Event logistics or None
        """
        if not events:
            return None

        # Find next event within next 6 hours
        for ev in events:
            event_date = ev.get("event_date", "")
            event_time = ev.get("time", "")

            try:
                event_dt = datetime.strptime(
                    f"{event_date} {event_time}", "%Y-%m-%d %I:%M%p"
                )
            except (ValueError, AttributeError):
                continue

            mins_until = (event_dt - now).total_seconds() / 60

            # Show if within next 6 hours and > 0 (not past)
            if 0 < mins_until < 360:
                location_str = ev.get("location", "")
                title = ev.get("event_title", "Event")
                child = ev.get("child_name", "")

                # Estimate drive time (rough: 1 min per km)
                drive_estimate = "~10 mins" if location_str else ""

                if drive_estimate:
                    return f"{child}'s {title} in {int(mins_until)} mins at {location_str.split(',')[0]}"
                else:
                    return f"{child}'s {title} in {int(mins_until)} mins"

        return None


class ServiceAgent:
    """Relevant services based on time/weather/location."""

    @staticmethod
    def infer(
        time_mode: str,
        weather: Dict,
        location: Optional[str],
        nearby_services: List[Dict],
    ) -> Optional[str]:
        """Show relevant services.

        Args:
            time_mode: "morning_commute", "daytime", "evening"
            weather: weather dict
            location: current location name
            nearby_services: list of nearby places

        Returns:
            Service recommendation or None
        """
        if not nearby_services:
            return None

        advice = []

        # Lunch time → show restaurants
        if time_mode == "daytime":
            restaurants = [s for s in nearby_services if "restaurant" in s.get("category", "").lower()]
            if restaurants:
                top = restaurants[0]
                advice.append(f"Lunch: {top.get('name')} nearby")

        # Evening → show dinner/pubs
        if time_mode in ("evening_leisure", "night"):
            food = [s for s in nearby_services if any(c in s.get("category", "").lower() for c in ["restaurant", "pub", "takeaway"])]
            if food:
                top = food[0]
                advice.append(f"Dinner: {top.get('name')} nearby")

        # Rainy → show indoor activities
        if weather and "rain" in (weather.get("desc") or "").lower():
            indoor = [s for s in nearby_services if any(c in s.get("category", "").lower() for c in ["cinema", "museum", "cafe"])]
            if indoor:
                top = indoor[0]
                advice.append(f"Rainy — {top.get('name')} nearby")

        return ". ".join(advice) if advice else None


class ContextOrchestrator:
    """Run all inference agents in parallel."""

    @staticmethod
    def orchestrate(
        now: datetime,
        weather: Dict,
        location: Optional[str],
        events: List[Dict],
        receipts: List[Dict],
        nearby_services: List[Dict],
        mode: str = "daytime",
    ) -> str:
        """Run all agents in parallel, collect results.

        Returns:
            Combined intelligent context (1-3 sentences)
        """
        time_mode = ContextOrchestrator._get_time_mode(now.hour)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                "weather": executor.submit(
                    WeatherAgent.infer, weather, time_mode
                ),
                "time": executor.submit(
                    TimeAgent.infer, now, events, time_mode
                ),
                "location": executor.submit(
                    LocationAgent.infer, location, receipts
                ),
                "event": executor.submit(
                    EventAgent.infer, now, events, None
                ),
                "service": executor.submit(
                    ServiceAgent.infer,
                    time_mode,
                    weather,
                    location,
                    nearby_services,
                ),
            }

            results = {}
            for agent_name, future in futures.items():
                try:
                    result = future.result(timeout=2)
                    if result:
                        results[agent_name] = result
                        logger.debug(f"[{agent_name}] {result}")
                except Exception as e:
                    logger.warning(f"[{agent_name}] failed: {e}")

        # Orchestrate into brief (priority order)
        brief_parts = []

        # 1. Event (time-critical)
        if "event" in results:
            brief_parts.append(results["event"])

        # 2. Location (where they are)
        if "location" in results:
            brief_parts.append(results["location"])

        # 3. Weather + practical advice
        if "weather" in results:
            brief_parts.append(results["weather"])

        # 4. Time-based context
        if "time" in results and "event" not in results:
            brief_parts.append(results["time"])

        # 5. Services (lowest priority)
        if "service" in results and len(brief_parts) < 3:
            brief_parts.append(results["service"])

        # Combine max 3 pieces
        combined = ". ".join(brief_parts[:3])
        if combined and not combined.endswith("."):
            combined += "."

        return combined

    @staticmethod
    def _get_time_mode(hour: int) -> str:
        """Classify time of day."""
        if 5 <= hour < 10:
            return "morning_commute"
        elif 10 <= hour < 17:
            return "daytime"
        elif 17 <= hour < 21:
            return "evening_leisure"
        elif hour >= 23 or hour < 5:
            return "goodnight"
        else:
            return "night"
