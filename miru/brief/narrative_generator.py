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
            kids: List of children's names

        Returns:
            Brief narrative (1-2 sentences, under 40 words)
        """
        mode_note = {
            "wfh": "working from home",
            "office": "going into the office",
            "out": "out and about",
        }.get(mode, "at home")

        kids_str = " and ".join(kids) if kids else "none mentioned"
        facts_text = "; ".join(facts) if facts else "no specific updates"

        system_prompt = (
            "You are a UK personal assistant. Generate warm, factual briefings from provided data only. "
            "No suggestions, no inferences, no hallucinations."
        )

        user_prompt = f"""Generate a brief for someone who is {mode_note} on {dow} {tod}.
Children: {kids_str}.

Facts (mention ONLY what's listed, nothing else):
{facts_text}

Write 1-2 sentences max, under 40 words. Reference only facts above. No suggestions or inferences."""

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
            logger.debug(f"Generated narrative: {text[:100]}...")
            return text

        except requests.Timeout:
            logger.error("Groq timeout")
            return ""
        except Exception as e:
            logger.error(f"NarrativeGenerator error: {e}")
            return ""
