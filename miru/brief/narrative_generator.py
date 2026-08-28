"""NarrativeGenerator — Groq integration with personality, insight and context.

Turns validated facts into a brief that reads like a sharp, caring friend
texting you what matters — not a dashboard read out loud. Still grounded
strictly in the facts it's given (no invented places, numbers or events);
what changed is the TONE and the JUDGEMENT layered on top of those facts.
"""

from typing import List
import requests
import os
import logging

logger = logging.getLogger(__name__)


class NarrativeGenerator:
    """Generate brief narrative from validated facts using Groq.

    Constraints enforced via prompt (not in Groq config):
    - Only facts in the list — never invent a place, number, or event
    - Personality + insight allowed: celebrate wins, flag risks, one
      actionable recommendation grounded in the facts
    - 2-3 sentences, conversational, British, practical
    - No corporate-speak, no bullet points, no greetings
    """

    # Same model used by the other brief prompts in sms_service.py — the
    # smaller "llama-3.1-8b-instant" this used to call was returning non-200
    # responses in production (likely deprecated/retired on Groq's side),
    # which silently produced empty briefs since nothing here surfaced the error.
    GROQ_MODEL = "llama-3.3-70b-versatile"
    MAX_TOKENS = 140
    TIMEOUT = 8

    @staticmethod
    def generate(
        facts: List[str],
        mode: str,
        dow: str,
        tod: str,
        kids: List[str],
        is_weekend: bool = False,
        is_bank_holiday_tomorrow: bool = False,
        is_long_weekend: bool = False,
    ) -> str:
        """Generate brief narrative from facts.

        Args:
            facts: List of validated facts (from InferenceGuard)
            mode: "wfh", "office", "out"
            dow: Day of week (Monday, Tuesday, etc.)
            tod: Time of day (morning, afternoon, evening)
            kids: List of children's names (NOT passed to Groq to prevent inference)
            is_weekend: True if Saturday/Sunday
            is_bank_holiday_tomorrow: True if tomorrow is a UK bank holiday
            is_long_weekend: True if this is a Fri/Sat/Sun leading into a bank holiday Monday

        Returns:
            Brief narrative (2-3 sentences, conversational)
        """
        mode_note = {
            "wfh": "working from home",
            "office": "going into the office",
            "out": "out and about",
        }.get(mode, "at home")

        facts_text = "; ".join(facts) if facts else "no specific updates today"

        context_notes = []
        if is_long_weekend:
            context_notes.append("This is a long weekend — a bank holiday Monday is coming up.")
        elif is_bank_holiday_tomorrow:
            context_notes.append("Tomorrow is a UK bank holiday.")
        if is_weekend:
            context_notes.append("It's the weekend.")
        context_str = " ".join(context_notes)

        system_prompt = (
            "You are Miru — a sharp, warm British friend who texts a quick, useful brief. "
            "Write like a person, not a dashboard. Weave the facts together into 2-3 short, "
            "conversational sentences with real personality: British, optimistic, practical, "
            "never corporate or robotic. "
            "\n\nHow to add value beyond the raw facts:"
            "\n- Spot the context: Friday plus a bank holiday tomorrow is a long weekend — say so. "
            "A quiet day with nothing urgent is worth naming as an easy one."
            "\n- Celebrate genuine wins in the facts (spend down, savings, a good deal, on-time trains) — "
            "briefly and warmly, don't gush."
            "\n- Flag genuine risks in the facts (spend up, price spikes, delays, bad weather) plainly and "
            "without judgement — never guilt-trip, just note it."
            "\n- End with ONE short, practical recommendation or forward-looking line, grounded only in "
            "the facts given — never invent an activity, place, or plan that isn't in the facts."
            "\n\nHard rules:"
            "\n- ONLY reference facts explicitly given below. Never invent a place, event, number, name, "
            "or plan that isn't in the list."
            "\n- Do not repeat every fact — pick what matters most and connect it with insight."
            "\n- Address the user directly as 'you' — this should read like a text from a friend."
            "\n- Max 1-2 emoji, used naturally, not decoratively."
            "\n- No greetings ('Good morning!'), no bullet points, no corporate phrases "
            "('as you can see', 'please note', 'I hope this finds you well')."
            "\n- 2-3 sentences max. Output ONLY the brief text — no labels, no preamble."
        )

        user_prompt = (
            f"It's {dow} {tod}. They're {mode_note}. "
            f"{context_str}\n\n"
            f"Facts to weave in (only use what's here — nothing else):\n{facts_text}\n\n"
            f"Write the brief now."
        )

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
                    "temperature": 0.6,
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

            # Light post-validation — trims stray labels/quotes, does NOT strip
            # personality or direct address (that's the whole point now).
            text = NarrativeGenerator._validate_output(text)

            # REJECT if narrative is just day/time info (useless)
            # e.g., "Wednesday evening. Thursday tomorrow." or "Tuesday afternoon."
            text_lower = text.lower()
            days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            times = ["morning", "afternoon", "evening", "night", "today", "tomorrow"]
            is_just_datetime = (
                any(day in text_lower for day in days) or any(time in text_lower for time in times)
            ) and not any(
                word in text_lower for word in [
                    "train", "£", "school", "rain", "sunny", "event", "coffee", "lunch",
                    "meeting", "deadline", "flight", "appointment", "birthday", "open",
                    "closed", "cancelled", "delayed", "due", "available", "free", "weekend",
                    "holiday", "spend", "spent", "saved", "fuel", "weather", "warm", "cold"
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
        """Light cleanup of the Groq output.

        This used to strip any sentence starting with 'you' or containing soft
        language like 'should'/'might'/'could' — which is exactly the personality,
        recommendation and direct address the brief is supposed to have. Now it
        only strips stray formatting artifacts (labels, wrapping quotes) and
        drops literal meta-commentary Groq occasionally leaks.
        """
        if not text:
            return ""

        text = text.strip()

        # Strip wrapping quotes Groq sometimes adds around the whole brief
        if len(text) >= 2 and text[0] in "\"'" and text[-1] in "\"'":
            text = text[1:-1].strip()

        # Drop a leading label line like "Brief:" or "Here's the brief:"
        for prefix in ("brief:", "here's the brief:", "here is the brief:", "text:"):
            if text.lower().startswith(prefix):
                text = text[len(prefix):].strip()

        sentences = [s.strip() for s in text.split(".") if s.strip()]
        valid_sentences = []

        for sent in sentences:
            sent_lower = sent.lower()

            # Block genuine meta-commentary about being an AI/assistant/model —
            # never legitimate content for a brief.
            meta_markers = (
                "as an ai", "as a language model", "i'm an ai", "i am an ai",
                "as your assistant", "note:", "disclaimer:",
            )
            if any(marker in sent_lower for marker in meta_markers):
                logger.warning(f"[validate] Blocked meta-commentary: {sent[:50]}")
                continue

            valid_sentences.append(sent)

        result = ". ".join(valid_sentences)
        if result and not result.endswith((".", "!", "?")):
            result += "."

        logger.info(f"[validate] {len(sentences)} → {len(valid_sentences)} sentences")
        return result
