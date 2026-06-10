"""Transit context - nearest station for event locations."""

import math
from typing import Optional, Tuple, Dict

# UK Tube & National Rail stations with lat/lon
STATIONS = [
    # London Underground (sample major stations)
    {"name": "King's Cross St Pancras", "line": "Circle/Metropolitan/Piccadilly", "lat": 51.5308, "lon": -0.1185, "type": "tube"},
    {"name": "Waterloo", "line": "Jubilee/Northern", "lat": 51.5033, "lon": -0.1127, "type": "tube"},
    {"name": "Victoria", "line": "Circle/District", "lat": 51.4935, "lon": -0.1449, "type": "tube"},
    {"name": "Paddington", "line": "Circle/Hammersmith", "lat": 51.5155, "lon": -0.1772, "type": "tube"},
    {"name": "Liverpool Street", "line": "Circle/Hammersmith", "lat": 51.5175, "lon": -0.0821, "type": "tube"},
    {"name": "Bank", "line": "Northern/District", "lat": 51.5127, "lon": -0.0886, "type": "tube"},

    # National Rail Stations (sample)
    {"name": "Guildford", "line": "South Western Railway", "lat": 51.2369, "lon": -0.5742, "type": "rail"},
    {"name": "Woking", "line": "South Western Railway", "lat": 51.3179, "lon": -0.5566, "type": "rail"},
    {"name": "Virginia Water", "line": "South Western Railway", "lat": 51.3835, "lon": -0.4969, "type": "rail"},
    {"name": "Weybridge", "line": "South Western Railway", "lat": 51.3707, "lon": -0.4607, "type": "rail"},
    {"name": "Walton-on-Thames", "line": "South Western Railway", "lat": 51.3866, "lon": -0.4018, "type": "rail"},
    {"name": "Chertsey", "line": "South Western Railway", "lat": 51.4130, "lon": -0.5126, "type": "rail"},
    {"name": "Staines", "line": "South Western Railway", "lat": 51.4381, "lon": -0.4936, "type": "rail"},
    {"name": "Windsor", "line": "South Western Railway", "lat": 51.4755, "lon": -0.6294, "type": "rail"},
    {"name": "Slough", "line": "South Western Railway", "lat": 51.5085, "lon": -0.5927, "type": "rail"},
    {"name": "Hayes & Harlington", "line": "Great Western Railway", "lat": 51.4885, "lon": -0.4373, "type": "rail"},
    {"name": "Ealing Broadway", "line": "District Line/Underground", "lat": 51.5163, "lon": -0.3016, "type": "tube"},
    {"name": "Heathrow Terminal 5", "line": "Piccadilly Line", "lat": 51.4609, "lon": -0.4893, "type": "rail"},
]


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km between two points."""
    R = 6371  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def estimate_walk_time(distance_km: float) -> int:
    """Estimate walking time in minutes (avg 1.4 km/h = 1 min per 0.023 km)."""
    return max(1, int(distance_km / 0.023))


def find_nearest_station(lat: float, lon: float, max_distance_km: float = 5.0) -> Optional[Dict]:
    """Find nearest Tube/Rail station to given coordinates.

    Args:
        lat: Latitude
        lon: Longitude
        max_distance_km: Maximum search radius (default 5km)

    Returns:
        Dict with station info or None if none found within range
    """
    if not lat or not lon:
        return None

    nearest = None
    min_distance = max_distance_km

    for station in STATIONS:
        dist = haversine_distance(lat, lon, station["lat"], station["lon"])
        if dist < min_distance:
            min_distance = dist
            nearest = {
                **station,
                "distance_km": round(dist, 2),
                "walk_time_mins": estimate_walk_time(dist),
            }

    return nearest


def transit_context_for_event(event_location: Optional[str], lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    """Generate transit context for an event.

    Args:
        event_location: Event location name (e.g., "Stanns Heath School")
        lat: Latitude (from postcode lookup)
        lon: Longitude (from postcode lookup)

    Returns:
        Transit context string or None
        Example: "🚂 Virginia Water Station (8 min walk)"
    """
    if not lat or not lon:
        return None

    station = find_nearest_station(lat, lon)
    if not station:
        return None

    # Choose icon based on type
    icon = "🚆" if station["type"] == "tube" else "🚂"

    # Format: "🚂 Station Name (X min walk)"
    return f"{icon} {station['name']} ({station['walk_time_mins']} min walk)"
