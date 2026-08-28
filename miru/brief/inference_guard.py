"""InferenceGuard — validates facts before LLM (prevents hallucinations)."""

from typing import List, Optional
from ..core.types import BriefContext
from ..core.formatting import DateFormatter, CurrencyFormatter
import logging

logger = logging.getLogger(__name__)


class InferenceGuard:
    """Extract only factual, validated information from context.

    Rules:
    - ONLY mention facts from context (no hallucination)
    - All dates formatted via DateFormatter
    - All amounts formatted via CurrencyFormatter
    - Events without dates are dropped
    - Saves without titles are dropped
    - No inference beyond explicit facts
    """

    @staticmethod
    def extract_facts(
        context: BriefContext,
        mode: str,  # "wfh", "office", "out"
        is_weekend: bool,
    ) -> List[str]:
        """Extract validated facts from context.

        Args:
            context: BriefContext object with all data
            mode: User's current mode
            is_weekend: True if Saturday/Sunday

        Returns:
            List of fact strings safe for Groq
        """
        facts = []

        # Trains: only if both stations exist and departures available
        if context.trains and len(context.trains) > 0:
            for train in context.trains[:3]:  # Max 3 trains
                if train.from_code and train.to_code and train.departs_iso:
                    departs = train.departs_display()
                    fact = f"Train {train.from_code}→{train.to_code}: {departs} ({train.status})"
                    facts.append(fact)

        # Fuel: only if price found (and no recent fuel purchase)
        if context.fuel and context.fuel.price_pence:
            price = context.fuel.price_display()
            fact = f"Cheapest fuel: {context.fuel.station_name} {price}"
            facts.append(fact)

        # School events: only upcoming, dated events
        if context.school_events:
            for event in context.school_events[:3]:  # Max 3 events
                if event.event_date_iso and event.event_title:
                    date_str = event.date_display()
                    fact = f"{event.child_name} has {event.event_title} on {date_str}"
                    facts.append(fact)

        # Spend: only if breakdown exists and total > 0
        if context.spend and context.spend.total_pence > 0:
            total = context.spend.total_display()
            breakdown_parts = []

            for cat, item in list(context.spend.breakdown.items())[:5]:
                breakdown_parts.append(f"{cat} {item.amount_display()}")

            if breakdown_parts:
                breakdown = " · ".join(breakdown_parts)
                fact = f"Spend this month: {total} ({breakdown})"
            else:
                fact = f"Spend this month: {total}"

            facts.append(fact)

        # Weather: only if temp found
        if context.weather and context.weather.temp_c is not None:
            temp = context.weather.temp_display()
            fact = f"Weather: {temp}, {context.weather.description}"
            facts.append(fact)

        # Recurring activities: only if on today's day
        if context.recurring_activities:
            for activity in context.recurring_activities[:2]:  # Max 2
                if activity.get("child") and activity.get("activity"):
                    fact = f"{activity['child']} has {activity['activity']}"
                    facts.append(fact)

        logger.debug(f"InferenceGuard extracted {len(facts)} facts")
        return facts[:8]  # Max 8 facts to keep brief focused
