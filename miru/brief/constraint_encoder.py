"""Encode data constraints into Groq prompt to prevent hallucinations."""

from typing import List, Set
from miru.brief.fact_schema import FactWithSource


class ConstraintEncoder:
    """Build negative constraints showing what data IS NOT available."""

    @staticmethod
    def encode(facts_with_source: List[FactWithSource]) -> str:
        """Generate constraint rules based on what data is present vs. absent.

        Args:
            facts_with_source: List of facts with source traceability

        Returns:
            Constraint string for Groq prompt explaining boundaries
        """
        sources_present: Set[str] = set(f.source_type for f in facts_with_source)

        # Build lists of what we HAVE and what we DON'T HAVE
        have = []
        must_not_mention = []

        # MAP: source_type → what we can confidently suggest
        source_to_data = {
            "fuel": ("fuel prices", None),
            "school": ("school events (use exact titles only)", None),
            "weather": ("current weather (temp, conditions)", None),
            "spend": ("recent shopping receipts", None),
            "trains": ("train departures and times", None),
            "transit": ("nearest stations", None),
            "calendar": ("calendar events", None),
            "saves": ("saved articles and places", None),
        }

        # Build HAVE list
        for source, (desc, _) in source_to_data.items():
            if source in sources_present:
                have.append(desc)

        # Build DON'T HAVE list (inverse)
        all_sources = set(source_to_data.keys())
        missing = all_sources - sources_present

        if "fuel" in missing:
            must_not_mention.append("petrol prices or fuel")
        if "school" not in sources_present:
            must_not_mention.append("school events or pickup times")
        if "weather" not in sources_present:
            must_not_mention.append("weather or seasonal advice")
        if "saves" not in sources_present:
            must_not_mention.append("restaurants, cafes, or places")
        if "calendar" not in sources_present:
            must_not_mention.append("personal appointments or activities")

        # Build constraint string
        if have:
            have_text = f"KNOWN DATA: {'; '.join(have)}."
        else:
            have_text = "KNOWN DATA: Minimal context available."

        if must_not_mention:
            must_not_text = f"DO NOT mention or invent: {'; '.join(must_not_mention)}."
        else:
            must_not_text = "All key data is available."

        return f"{have_text} {must_not_text}"

    @staticmethod
    def get_allowed_entities(facts_with_source: List[FactWithSource]) -> Set[str]:
        """Extract all entity names (merchants, stations, people) from facts.

        Used by OutputValidator to check if Groq invented entities.
        """
        allowed = set()

        for fact in facts_with_source:
            data = fact.source_data or {}

            # Extract from various source types
            if fact.source_type == "fuel":
                if data.get("merchant"):
                    allowed.add(data["merchant"].lower())
            elif fact.source_type == "school":
                if data.get("child_name"):
                    allowed.add(data["child_name"].lower())
                if data.get("school_name"):
                    allowed.add(data["school_name"].lower())
            elif fact.source_type == "trains":
                if data.get("destination"):
                    allowed.add(data["destination"].lower())
                if data.get("station"):
                    allowed.add(data["station"].lower())
            elif fact.source_type == "transit":
                if data.get("station_name"):
                    allowed.add(data["station_name"].lower())
            elif fact.source_type == "spend":
                if data.get("merchant"):
                    allowed.add(data["merchant"].lower())

        return allowed
