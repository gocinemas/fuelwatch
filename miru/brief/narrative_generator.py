"""NarrativeGenerator — safe Groq integration with strict constraints."""

from typing import List
import requests
import os
import logging

logger = logging.getLogger(__name__)


class NarrativeGenerator:
    """Generate brief narrative from validated facts using Groq.

    Constraints enforced via prompt (not in Groq config):
    - Only facts in the list
    - No suggestions or inferences
    - Under 40 words
    - No greetings or bullet points
    """

    GROQ_MODEL = "llama-3.1-8b-instant"
    MAX_TOKENS = 80
    TIMEOUT = 8

    @staticmethod
    def generate(
        facts: List[str],
        mode: str,
        dow: str,
        tod: str,
        kids: List[str],
    ) -> str:
        """Generate brief narrative from facts.

        Args:
            facts: List of validated facts (from InferenceGuard)
            mode: "wfh", "office", "out"
            dow: Day of week (Monday, Tuesday, etc.)
            tod: Time of day (morning, afternoon, evening)
            kids: List of children's names (NOT passed to Groq to prevent inference)

        Returns:
            Brief narrative (1-2 sentences, under 40 words)
        """
        mode_note = {
            "wfh": "working from home",
            "office": "going into the office",
            "out": "out and about",
        }.get(mode, "at home")

        facts_text = "; ".join(facts) if facts else "no specific updates"

        system_prompt = (
            "You are a factual UK assistant. ONLY state facts provided. "
            "ABSOLUTELY FORBIDDEN: should, probably, might, may, could, think, believe, "
            "want, relax, unwind, looking forward, heading to, have time, take a look, "
            "you've got, you need, suggest, consider, ensure. "
            "ONLY FACTS FROM THE LIST."
        )

        # DO NOT mention kids names - they cause inference
        user_prompt = f"""Facts (state ONLY these, nothing else):
{facts_text}

Write 1-2 sentences max. State facts only. NO suggestions, NO inferences, NO opinions."""

        try:
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {os.environ.get('GROQ_API_KEY', '')}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": NarrativeGenerator.GROQ_MODEL,
                    "max_tokens": NarrativeGenerator.MAX_TOKENS,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                },
                timeout=NarrativeGenerator.TIMEOUT,
            )

            if response.status_code != 200:
                logger.error(f"Groq error: {response.status_code} {response.text}")
                return ""

            text = response.json()["choices"][0]["message"]["content"].strip()

            # POST-VALIDATION: Remove any inferred sentences
            text = NarrativeGenerator._validate_output(text)

            # REJECT if narrative is just day/time info (useless)
            # e.g., "Wednesday evening. Thursday tomorrow." or "Tuesday afternoon."
            text_lower = text.lower()
            days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            times = ["morning", "afternoon", "evening", "night", "today", "tomorrow"]
            is_just_datetime = all(
                any(day in text_lower for day in days) or
                any(time in text_lower for time in times)
                for _ in [1]  # Always true, just for structure
            ) and not any(
                word in text_lower for word in [
                    "train", "£", "school", "rain", "sunny", "event", "coffee", "lunch",
                    "meeting", "deadline", "flight", "appointment", "birthday", "open",
                    "closed", "cancelled", "delayed", "due", "available", "free"
                ]
            )
            if is_just_datetime:
                logger.warning(f"[validate] Rejected useless datetime-only narrative: {text}")
                return ""

            logger.debug(f"Generated narrative: {text[:100]}...")
            return text

        except requests.Timeout:
            logger.error("Groq timeout")
            return ""
        except Exception as e:
            logger.error(f"NarrativeGenerator error: {e}")
            return ""

    @staticmethod
    def _validate_output(text: str) -> str:
        """POST-VALIDATE output to remove inferences Groq snuck in.

        Removes sentences that:
        - Start with pronouns (You, I, They)
        - Contain inference words (probably, might, could, think, suggest)
        - Contain suggestions (want, need, relax, unwind, heading)
        """
        if not text:
            return ""

        sentences = [s.strip() for s in text.split(".") if s.strip()]
        valid_sentences = []

        for sent in sentences:
            sent_lower = sent.lower()

            # Block sentences starting with pronouns
            if any(sent_lower.startswith(p) for p in ["you ", "i ", "they ", "we "]):
                logger.warning(f"[validate] Blocked pronoun-start: {sent[:50]}")
                continue

            # Block sentences with inference/suggestion words
            blocked_words = [
                "probably", "might", "may", "could", "should", "think",
                "believe", "know", "suggest", "consider", "want", "need",
                "relax", "unwind", "forward", "heading", "visit", "go to",
                "take a look", "pick up", "looking forward", "a bit of time"
            ]
            if any(word in sent_lower for word in blocked_words):
                logger.warning(f"[validate] Blocked inference: {sent[:50]}")
                continue

            valid_sentences.append(sent)

        result = ". ".join(valid_sentences)
        if result:
            result += "."

        logger.info(f"[validate] {len(sentences)} → {len(valid_sentences)} sentences")
        return result
