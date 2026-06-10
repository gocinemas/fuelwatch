"""Validate Groq output against source data to remove hallucinations."""

import re
from typing import List, Set
from miru.brief.fact_schema import FactWithSource
from miru.brief.constraint_encoder import ConstraintEncoder


class OutputValidator:
    """Post-Groq validation: allow smart data-backed inference, block hallucinations."""

    # UNGROUNDED suggestions (no data backing) — always block
    UNGROUNDED_PATTERNS = [
        # Invented activities without context
        r"(?:pick up|grab|get|buy)\s+(?:fuel|petrol|gas|lunch|coffee|beer)",
        r"(?:head to|visit|go to|pop by|check out)\s+[A-Z]\w+",  # Invented place suggestion
        r"(?:stop by|call|text)\s+",

        # Weak suggestion patterns
        r"you\s+(?:should probably|might want to|could try|may want to)",
        r"(?:why not|perhaps you should|maybe you could)",

        # Pure wishy-washy with no grounding
        r"(?:it would be nice|it\s+might be good|you probably should)",
    ]

    # ALLOWED patterns (smart inference grounded in data)
    ALLOWED_PATTERNS = [
        r"it's\s+(?:\d+(?:am|pm)|morning|afternoon|evening)",  # Time context
        r"(?:rainy|sunny|cold|warm|windy)",  # Weather
        r"in\s+\d+\s+(?:min|hour|mins|hours)",  # Time delta (event countdown)
        r"at\s+[A-Z]\w+(?:\s+station)?",  # Known location
        r"bring\s+(?:umbrella|jacket|water|sunscreen|waterproof)",  # Weather-based prep
        r"(?:field trip|pickup|event|class|assembly)",  # School events (OK to mention)
        r"(?:station|walk|train|transport|journey)",  # Transit context
    ]

    @staticmethod
    def validate(brief_text: str, facts_with_source: List[FactWithSource]) -> str:
        """Remove sentences that mention invented entities or pattern-match hallucinations.

        Args:
            brief_text: Raw Groq output
            facts_with_source: Source facts that are allowed

        Returns:
            Cleaned brief with hallucinations removed
        """
        if not brief_text:
            return ""

        # Get allowed entity names from source data
        allowed_entities = ConstraintEncoder.get_allowed_entities(facts_with_source)

        # Split into sentences
        sentences = [s.strip() for s in brief_text.split(".") if s.strip()]

        valid_sentences = []

        for sent in sentences:
            sent_lower = sent.lower().strip()

            # RULE 1: Block ungrounded suggestions (invented activities/places)
            is_ungrounded = any(
                re.search(pattern, sent_lower)
                for pattern in OutputValidator.UNGROUNDED_PATTERNS
            )
            if is_ungrounded:
                continue

            # RULE 2: Check if sentence mentions unknown entities
            # Allow if entity is in allowed_entities OR it's a known pattern
            mentions_unknown = False

            # Extract place/merchant names mentioned
            place_pattern = r"\b(?:at|to|in|near|by)\s+([A-Z][a-zA-Z\s&]+)"
            matches = re.findall(place_pattern, sent)
            for match in matches:
                match_lower = match.lower()
                # Allow if: it's a known entity OR it's a generic location (station, café, home)
                if match_lower not in allowed_entities and \
                   match_lower not in ("station", "cafe", "café", "home", "office", "school"):
                    mentions_unknown = True
                    break

            if mentions_unknown:
                continue

            # RULE 3: Allow smart inference IF it has 2+ data anchors
            # (e.g., time + weather, event + location, weather + activity)
            has_time = any(p in sent_lower for p in ["am", "pm", "morning", "afternoon", "evening", "min", "hour"])
            has_weather = any(p in sent_lower for p in ["rainy", "sunny", "cold", "warm", "windy", "rain"])
            has_location = any(p in sent_lower for p in allowed_entities)
            has_event = any(p in sent_lower for p in ["pickup", "event", "meeting", "appointment", "class"])

            data_anchors = sum([has_time, has_weather, has_location, has_event])

            # If sentence starts with "you" but has multiple data anchors, allow it
            # (it's smart inference, not hallucination)
            if sent_lower.startswith(("you ", "you've", "you're")):
                if data_anchors < 2:
                    # Not enough grounding
                    continue

            # Sentence passed validation
            valid_sentences.append(sent)

        # Rejoin sentences
        result = ". ".join(valid_sentences)
        if result and not result.endswith("."):
            result += "."

        return result

    @staticmethod
    def get_validation_report(
        original: str, validated: str, facts_with_source: List[FactWithSource]
    ) -> dict:
        """Return a report of what was blocked and why (for logging).

        Args:
            original: Raw Groq output
            validated: Cleaned output
            facts_with_source: Source facts

        Returns:
            Report dict with blocked sentences and reasons
        """
        original_sentences = {s.strip() for s in original.split(".") if s.strip()}
        validated_sentences = {s.strip() for s in validated.split(".") if s.strip()}
        blocked = original_sentences - validated_sentences

        allowed_entities = ConstraintEncoder.get_allowed_entities(facts_with_source)

        report = {
            "original_count": len(original_sentences),
            "validated_count": len(validated_sentences),
            "blocked_count": len(blocked),
            "blocked_sentences": list(blocked),
            "allowed_entities": list(allowed_entities),
        }

        return report
