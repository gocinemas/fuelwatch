"""Validate Groq output against source data to remove hallucinations."""

import re
from typing import List, Set
from miru.brief.fact_schema import FactWithSource
from miru.brief.constraint_encoder import ConstraintEncoder


class OutputValidator:
    """Post-Groq validation: ensure brief only mentions things from source data."""

    HALLUCINATION_PATTERNS = [
        # Pronoun starters (inference)
        r"^you've\s+got",
        r"^you\s+can\s+",
        r"^you\s+could\s+",
        r"^you\s+should\s+",
        r"^you\s+might\s+",
        r"^you\s+have\s+",
        r"^you\s+are\s+",
        r"^you're\s+",
        r"^you'll\s+",

        # Action verbs (invention)
        r"(?:pick up|grab|get|buy)\s+(?:fuel|petrol|gas)",
        r"(?:head to|visit|go to|pop by)\s+",
        r"(?:stop by|check out|look in)\s+",
        r"(?:take a|have a|grab a)\s+(?:break|rest|moment)",

        # Wishy-washy language
        r"\bperhaps\b",
        r"\bmight\s+be\b",
        r"\bcould\s+be\b",
        r"\bmaybe\b",
        r"\bprobably\b",

        # Opinions/suggestions
        r"(?:it would be|it\s+is)\s+(?:nice|good|great|perfect|ideal)",
        r"(?:why not|how about|what about)\s+",
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

            # RULE 0: Block if starts with "you" or "i" (pronouns = inference)
            if sent_lower.startswith(("you ", "you've", "you're", "you'll", "you'd",
                                     "i ", "i've", "i'm", "it's", "they ", "we ")):
                continue

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
