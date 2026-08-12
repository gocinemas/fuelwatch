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
        "excluded": ["growth", "hiring", "employee", "stock", "price"],
        "priority": 10,
    },
    "comparison": {
        "required": ["compare", "vs", "versus", "compared to", "how do we compare"],
        "excluded": [],
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
    "financial": {
        "required": ["revenue", "margin", "profit", "ebitda", "earnings", "cash", "financial", "health", "trajectory", "fcf", "dividend"],
        "excluded": ["market"],
        "priority": 8,
    },
    "stock": {
        "required": ["stock", "price", "driver", "movement", "catalyst"],
        "excluded": ["market"],
        "priority": 11,
    },
    "growth": {
        "required": ["growth", "pricing", "volume", "composition", "led", "driven"],
        "excluded": ["hiring", "employee"],
        "priority": 6,
    },
    "regional": {
        "required": ["about", "how", "uk", "india", "japan", "china", "france", "germany", "brazil", "mexico", "specific"],
        "excluded": [],
        "priority": 8,
    },
    "geographic": {
        "required": ["country", "countries", "geographic", "region", "expand"],
        "excluded": ["share", "position", "competitive"],
        "priority": 7,
    },
    "acquisition": {
        "required": ["acquisition", "acquire", "acquir", "m&a", "merge", "deal", "deal"],
        "excluded": [],
        "priority": 8,
    },
    "strategy": {
        "required": ["strategy", "focus", "direction", "roadmap"],
        "excluded": ["hiring", "talent", "recruitment", "employ", "headcount", "financial", "health", "acquisition"],
        "priority": 7,
    },
    "hiring": {
        "required": ["hiring", "employ", "headcount", "workforce", "staff", "team", "talent", "recruitment", "trending"],
        "excluded": ["country", "countries", "geographic"],
        "priority": 6,
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
        "comparison": [
            ("database", "query_competitor_comparison"),
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
        "acquisition": [
            ("database", "query_acquisition_strategy"),
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
        "stock": [
            ("database", "query_stock_price_drivers"),
            ("groq", "ask_groq"),
        ],
        "growth": [
            ("database", "query_growth_composition"),
            ("groq", "ask_groq"),
        ],
        "regional": [
            ("database", "query_regional_strategy"),
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
