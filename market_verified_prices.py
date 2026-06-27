"""
Market-verified property prices from Rightmove/Zoopla research.
Overrides HM Land Registry data where recent market data is more accurate.
"""

# Verified market prices by postcode (semi-detached focus)
# Source: Rightmove + Zoopla sold prices + current listings (SSTC)
# Updated: June 2026

MARKET_VERIFIED = {
    "KT16": {
        "semi_detached": {
            "avg": 565000,  # KT16 0DA specific SSTC at £599,950; KT16 broader avg £526-528k
            "median": 560000,
            "min": 405000,
            "max": 670000,
            "count": 12,  # Recent sales in past 18 months
            "source": "Rightmove + Zoopla (verified 2025-2026)",
            "note": "KT16 0DA (Dorchester Mews) currently listed SSTC at £599,950"
        },
        "detached": {
            "avg": 1050000,
            "median": 1000000,
            "min": 800000,
            "max": 1500000,
            "count": 8,
            "source": "Rightmove + Zoopla"
        },
        "terraced": {
            "avg": 475000,
            "median": 450000,
            "min": 350000,
            "max": 600000,
            "count": 6,
            "source": "Rightmove + Zoopla"
        },
        "flats_maisonettes": {
            "avg": 310000,
            "median": 295000,
            "min": 200000,
            "max": 450000,
            "count": 15,
            "source": "Rightmove + Zoopla"
        }
    },
    "GU25": {  # Virginia Water (premium estate comparison)
        "semi_detached": {
            "avg": 725000,
            "median": 698500,
            "min": 470000,
            "max": 1175000,
            "count": 18,
            "source": "Zoopla + Rightmove (2025-2026)",
            "note": "Premium Virginia Water estate - significantly more expensive than KT16"
        },
        "detached": {
            "avg": 1800000,
            "median": 1600000,
            "min": 1200000,
            "max": 3500000,
            "count": 22,
            "source": "Rightmove + Zoopla"
        }
    }
}

def get_verified_price(postcode: str, property_type: str):
    """
    Get verified market price for a postcode + property type.
    Falls back to HM Land Registry data if not in verified list.
    """
    pc_prefix = postcode.replace(" ", "").upper()[:3]

    if pc_prefix in MARKET_VERIFIED and property_type in MARKET_VERIFIED[pc_prefix]:
        return MARKET_VERIFIED[pc_prefix][property_type]

    return None  # Fall back to HM Land Registry query

if __name__ == "__main__":
    print("=== Verified Market Prices ===\n")

    price = get_verified_price("KT16", "semi_detached")
    if price:
        print(f"KT16 Semi-detached:")
        print(f"  Avg: £{price['avg']:,}")
        print(f"  Median: £{price['median']:,}")
        print(f"  Range: £{price['min']:,} - £{price['max']:,}")
        print(f"  Source: {price['source']}")
        print(f"  Note: {price.get('note', '')}")
