"""
Query Orchestrator — Central routing layer for all Miru handlers
Routes: Shopping, Life Advice, Receipts, Events, Commute, Fuel, Research, etc.
"""
from typing import Dict, Callable, Optional, List
from enum import Enum


class QueryType(Enum):
    RECEIPT = "receipt"          # Receipt scanning (HIGH PRIORITY)
    SHOPPING = "shopping"         # Should I buy this?
    LIFE_ADVICE = "life_advice"  # Help me with...
    RESEARCH = "research"         # Tell me about...
    COMMUTE = "commute"          # Next train / my commute
    FUEL = "fuel"                # Fuel prices
    EVENT = "event"              # Event management
    UNKNOWN = "unknown"


class QueryOrchestrator:
    """Route queries to appropriate handlers"""

    def __init__(self):
        self.handlers: Dict[QueryType, Callable] = {}

    def register_handler(self, query_type: QueryType, handler: Callable):
        """Register a handler for a query type"""
        self.handlers[query_type] = handler

    def route(self, message: str, from_number: str, media_urls: List[str] = None, urls: List[str] = None, request_form=None) -> Optional[Dict]:
        """
        Route message to appropriate handler
        Returns: (handled: bool, response: str) or None if no handler matched
        """
        media_urls = media_urls or []
        urls = urls or []

        # PRIORITY ORDER (most critical first)
        # 1. Receipts (scanning photos/parsing)
        if media_urls and request_form:
            # Try receipt handler first if media present
            query_type = self._classify_as_receipt(message, media_urls)
            if query_type == QueryType.RECEIPT:
                return self._call_handler(QueryType.RECEIPT, message, from_number, media_urls, urls, request_form)

        # 2. Shopping/Life Advice/Research (Miru Assistant)
        query_type = self._classify_query(message, media_urls, urls)
        if query_type in [QueryType.SHOPPING, QueryType.LIFE_ADVICE, QueryType.RESEARCH]:
            return self._call_handler(query_type, message, from_number, media_urls, urls, request_form)

        # 3. Commute/Fuel/Events (existing handlers)
        if query_type in [QueryType.COMMUTE, QueryType.FUEL, QueryType.EVENT]:
            return self._call_handler(query_type, message, from_number, media_urls, urls, request_form)

        # No handler matched
        return None

    def _classify_as_receipt(self, message: str, media_urls: List[str]) -> QueryType:
        """Check if this is a receipt scanning request"""
        # Receipt handler checks for image + context
        # Don't classify here — let receipt handler decide
        # This is just to indicate we should try the receipt handler
        return QueryType.RECEIPT

    def _classify_query(self, message: str, media_urls: List[str], urls: List[str]) -> QueryType:
        """Classify query type"""
        m = message.lower()

        # LIFE ADVICE (check first — most important)
        if any(x in m for x in ["frustrated", "help me", "help with", "how do i", "what should i", "advice", "worried", "stressed", "upset", "anxious", "depressed", "job", "work", "relationship", "family"]):
            return QueryType.LIFE_ADVICE

        # Shopping queries
        if any(x in m for x in ["should i buy", "is this worth", "good price", "good deal", "compare", "vs "]):
            return QueryType.SHOPPING

        # Research queries
        if any(x in m for x in ["tell me about", "explain", "what is", "who is"]):
            return QueryType.RESEARCH

        # Commute queries
        if any(x in m for x in ["train", "commute", "transport", "next", "waterloo"]):
            return QueryType.COMMUTE

        # Fuel queries
        if any(x in m for x in ["fuel", "petrol", "diesel", "price"]):
            return QueryType.FUEL

        # Event queries
        if any(x in m for x in ["event", "calendar", "upcoming"]):
            return QueryType.EVENT

        return QueryType.UNKNOWN

    def _call_handler(self, query_type: QueryType, message: str, from_number: str, media_urls: List[str], urls: List[str], request_form=None) -> Optional[Dict]:
        """Call the registered handler for this query type"""
        if query_type not in self.handlers:
            return None

        handler = self.handlers[query_type]

        try:
            # Call handler with appropriate parameters
            result = handler(
                message=message,
                from_number=from_number,
                media_urls=media_urls,
                urls=urls,
                request_form=request_form
            )
            return result
        except Exception as e:
            print(f"[orchestrator] Handler error for {query_type}: {e}")
            return None


# Global orchestrator instance
_orchestrator = None


def get_orchestrator() -> QueryOrchestrator:
    """Get or create orchestrator"""
    global _orchestrator
    if not _orchestrator:
        _orchestrator = QueryOrchestrator()
    return _orchestrator


def register_handler(query_type: QueryType, handler: Callable):
    """Register a handler"""
    get_orchestrator().register_handler(query_type, handler)
