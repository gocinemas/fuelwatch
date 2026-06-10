"""Validate Groq output against source data to remove hallucinations."""

import re
from typing import List, Set
from miru.brief.fact_schema import FactWithSource
from miru.brief.constraint_encoder import ConstraintEncoder


class OutputValidator:
    """Post-Groq validation: ensure brief only mentions things from source data."""

    HALLUCINATION_PATTERNS = [
        r"(?:pick up|grab|get|buy)\s+(?:fuel|petrol|gas)",  # Invented fuel suggestion
        r"(?:head to|visit|go to|pop by)\s+\w+",  # Invented place suggestion
        r"(?:you should|you might|you could)\s+(?:consider|try|visit)",  # Weak inference
        r"(?:it would be|it\s+is)\s+(?:nice|good|great)\s+(?:to|for)",  # Invented opinions
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
            sent_lower = sent.lower()

            # RULE 1: Check against hallucination patterns
            is_hallucination = any(
                re.search(pattern, sent_lower)
                for pattern in OutputValidator.HALLUCINATION_PATTERNS
            )
            if is_hallucination:
                continue

            # RULE 2: Check if sentence mentions unknown entities
            mentions_unknown = False
            for entity in allowed_entities:
                if entity in sent_lower:
                    # This entity is known, so it's safe
                    break
            else:
                # Check for suspiciously specific place/merchant names that aren't in allowed_entities
                # (e.g., "head to Starbucks" when no Starbucks in facts)
                place_pattern = r"\b(?:at|to|in|near|by)\s+([A-Z][a-zA-Z\s&]+)"
                matches = re.findall(place_pattern, sent)
                for match in matches:
                    if match.lower() not in allowed_entities and match.lower() != "the":
                        mentions_unknown = True
                        break

            if mentions_unknown:
                continue

            # RULE 3: Block generic "you should" suggestions without grounding
            if sent_lower.startswith(("you should", "you might", "you could")) and \
               "because" not in sent_lower and "since" not in sent_lower:
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
