"""
Question intent classification for Intel Q&A.
Determines what the user is really asking about.
"""

import logging

logger = logging.getLogger(__name__)

# Define question types and their identifying patterns
INTENT_PATTERNS = {
    "comparison": {
        "required": ["vs", "versus", "compare", "better", "trending vs", "vs compet"],
        "excluded": [],
        "priority": 12,  # Higher priority to catch "vs competitors" questions
    },
    "market_share": {
        "required": ["market", "share", "rank", "size", "largest", "position", "competitive", "advantage", "moat", "strength"],
        "excluded": ["growth", "hiring", "employee"],
        "priority": 10,
    },
    "competitor": {
        "required": ["compet", "rival", "who compete"],
        "excluded": ["vs", "versus", "compare", "trending"],  # Exclude if it's actually a comparison
        "priority": 9,
    },
    "brands": {
        "required": ["brand", "what do"],
        "excluded": ["market", "share", "revenue", "price", "cost"],
        "priority": 8,
    },
    "strategy": {
        "required": ["strategy", "acquisition", "acquire", "growth", "focus", "direction"],
        "excluded": ["hiring", "talent", "recruitment", "employ", "headcount"],
        "priority": 7,
    },
    "financial": {
        "required": ["revenue", "margin", "profit", "ebitda", "earnings", "cash", "financial", "health", "trajectory", "growth", "fcf", "dividend"],
        "excluded": ["market"],
        "priority": 6,
    },
    "hiring": {
        "required": ["hiring", "employ", "headcount", "workforce", "staff", "team", "talent", "recruitment", "expand", "reduce", "trending"],
        "excluded": [],
        "priority": 6,
    },
    "geographic": {
        "required": ["country", "countries", "expand", "geographic", "region", "market", "which"],
        "excluded": ["share", "position", "competitive"],
        "priority": 5,
    },
    "general": {
        "required": ["tell", "about", "what", "who", "how"],
        "excluded": [],
        "priority": 1,
    },
}


def detect_intent(question: str) -> str:
    """
    Classify question into intent type.
    Uses pattern matching with priority ordering.

    Args:
        question: The user's question

    Returns:
        One of: "market_share", "competitor", "brands", "strategy",
                "financial", "hiring", "comparison", "general"
    """
    if not question or not question.strip():
        return "general"

    q_lower = question.lower().strip()

    # Score each intent type
    scores = {}
    for intent_type, pattern in INTENT_PATTERNS.items():
        required_words = pattern["required"]
        excluded_words = pattern["excluded"]
        priority = pattern["priority"]

        # Check if all required words present
        has_required = any(word in q_lower for word in required_words)

        # Check if any excluded words present
        has_excluded = any(word in q_lower for word in excluded_words)

        if has_required and not has_excluded:
            scores[intent_type] = priority

    # Return highest priority match
    if scores:
        best_intent = max(scores, key=scores.get)
        logger.info(f"[Intent] Classified '{question[:50]}...' as '{best_intent}'")
        return best_intent

    logger.info(f"[Intent] No pattern match, defaulting to 'general'")
    return "general"


def get_answer_strategy(intent: str, question: str = "") -> list:
    """
    Get the fallback strategy for this intent type.
    Returns list of (source, handler) tuples to try in order.
    """
    strategies = {
        "comparison": [
            ("database", "compare_companies"),
            ("groq", "ask_groq"),
        ],
        "competitor": [
            ("database", "query_competitors"),
            ("groq", "ask_groq"),
        ],
        "brands": [
            ("database", "query_brands"),
            ("groq", "ask_groq"),
        ],
        "strategy": [
            ("database", "infer_strategy_from_ma"),
            ("database", "infer_strategy_from_growth"),
            ("groq", "ask_groq"),
        ],
        "financial": [
            ("database", "query_financial_health"),
            ("groq", "ask_groq"),
        ],
        "market_share": [
            ("database", "query_market_position"),
            ("groq", "ask_groq"),
        ],
        "hiring": [
            ("database", "query_hiring_strategy"),
            ("groq", "ask_groq"),
        ],
        "geographic": [
            ("database", "query_geographic_expansion"),
            ("groq", "ask_groq"),
        ],
        "general": [
            ("database", "fetch_company_overview"),
            ("groq", "ask_groq"),
        ],
    }

    return strategies.get(intent, strategies["general"])
