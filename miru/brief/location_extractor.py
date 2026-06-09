"""LocationExtractor — learn user location from natural conversation."""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class LocationExtractor:
    """Extract location mentions from user questions."""

    # Common UK places user might ask about
    LOCATIONS = {
        "costa": "Costa Coffee",
        "tesco": "Tesco",
        "sainsbury": "Sainsbury's",
        "asda": "Asda",
        "morrisons": "Morrisons",
        "waitrose": "Waitrose",
        "boots": "Boots",
        "boots pharmacy": "Boots",
        "lloyds": "Lloyds Pharmacy",
        "pret": "Pret A Manger",
        "starbucks": "Starbucks",
        "mcdonald": "McDonald's",
        "burger king": "Burger King",
        "kfc": "KFC",
        "subway": "Subway",
        "domino": "Dominos",
        "pizza hut": "Pizza Hut",
        "nando": "Nando's",
        "wagamama": "Wagamama",
        "zizzi": "Zizzi",
        "frankie": "Frankie & Benny's",
    }

    @staticmethod
    def extract(question: str) -> Optional[str]:
        """Extract location from user question.

        Args:
            question: User's question/statement

        Returns:
            Location name if found, else None
        """
        if not question:
            return None

        question_lower = question.lower().strip()

        # Pattern: "at X", "in X", "at X yesterday", etc.
        patterns = [
            r"(?:at|in)\s+([a-z\s&]+?)(?:\s+(?:today|yesterday|last|this|when|what|did|on|with)|\?|$)",
            r"([a-z\s&]+?)\s+(?:coffee|supermarket|shop|store|restaurant)",
        ]

        for pattern in patterns:
            match = re.search(pattern, question_lower)
            if match:
                potential_location = match.group(1).strip()

                # Check against known locations
                for key, canonical_name in LocationExtractor.LOCATIONS.items():
                    if key in potential_location:
                        logger.info(f"[location] Extracted: {canonical_name} from '{question[:50]}'")
                        return canonical_name

        return None

    @staticmethod
    def normalize(location: str) -> str:
        """Normalize location name to canonical form."""
        location_lower = location.lower().strip()

        for key, canonical in LocationExtractor.LOCATIONS.items():
            if key in location_lower:
                return canonical

        return location
