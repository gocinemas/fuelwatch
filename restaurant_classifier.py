"""
Restaurant type classifier for spend categorization.
Maps restaurant names to types (takeaway, delivery, dine-in, cafe, etc).
"""

RESTAURANT_TYPES = {
    # Takeaway
    "kokoro": "takeaway",
    "chaiiwala": "takeaway",
    "renaizzance": "takeaway",
    "renaizance": "takeaway",
    "wagamama": "takeaway",
    "leon": "takeaway",
    "subway": "takeaway",
    "taco bell": "takeaway",
    "kfc": "takeaway",
    "mcdonald": "takeaway",
    "burger king": "takeaway",
    "chipotle": "takeaway",
    "five guys": "takeaway",
    "nandos": "takeaway",

    # Dine-in restaurants
    "dishoom": "dine-in",
    "cote": "dine-in",
    "barbounia": "dine-in",
    "granger & co": "dine-in",
    "saravanaa bhavan": "dine-in",
    "saravana bhavan": "dine-in",

    # Coffee & Cafes
    "starbucks": "coffee",
    "costa": "coffee",
    "greggs": "coffee",
    "pret": "coffee",
    "pret a manger": "coffee",
    "blacksheep": "coffee",
    "blacksheep coffee": "coffee",
    "caffe nero": "coffee",
    "nero": "coffee",
    "itsu": "coffee",
}

def classify_restaurant(name: str) -> str:
    """
    Classify a restaurant by name.
    Returns: "takeaway", "dine-in", "cafe", "delivery", or "unknown"
    """
    if not name:
        return "unknown"

    name_lower = name.lower().strip()

    # Exact match
    if name_lower in RESTAURANT_TYPES:
        return RESTAURANT_TYPES[name_lower]

    # Partial match (check if any known restaurant is in the name)
    for restaurant, rtype in RESTAURANT_TYPES.items():
        if restaurant in name_lower:
            return rtype

    return "unknown"

def add_restaurant_type(name: str, rtype: str):
    """Add a new restaurant classification."""
    RESTAURANT_TYPES[name.lower().strip()] = rtype

# Add Vikram's favorites
add_restaurant_type("kokoro", "takeaway")
add_restaurant_type("chaiiwala", "takeaway")

if __name__ == "__main__":
    test_names = ["Kokoro", "Chaiiwala", "Wagamama", "Dishoom", "Starbucks", "Random Place"]
    for name in test_names:
        print(f"{name}: {classify_restaurant(name)}")
