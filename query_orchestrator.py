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
        import sys
        media_urls = media_urls or []
        urls = urls or []

        print(f"\n[orchestrator.route] ═══ ROUTING START ═══", flush=True)
        sys.stdout.flush()
        print(f"[orchestrator.route] Message: {message[:60]}", flush=True)
        print(f"[orchestrator.route] Media: {len(media_urls)}, URLs: {len(urls)}", flush=True)
        sys.stdout.flush()

        # PRIORITY ORDER (most critical first)
        # 1. Receipts (scanning photos/parsing)
        if media_urls and request_form:
            # Try receipt handler first if media present
            query_type = self._classify_as_receipt(message, media_urls)
            if query_type == QueryType.RECEIPT:
                print(f"[orchestrator.route] → Classified as RECEIPT")
                return self._call_handler(QueryType.RECEIPT, message, from_number, media_urls, urls, request_form)

        # 2. Shopping/Life Advice/Research (Miru Assistant)
        query_type = self._classify_query(message, media_urls, urls)
        print(f"[orchestrator.route] → Classified as: {query_type}", flush=True)
        sys.stdout.flush()

        if query_type in [QueryType.SHOPPING, QueryType.LIFE_ADVICE, QueryType.RESEARCH]:
            print(f"[orchestrator.route] → Has handler, calling...", flush=True)
            sys.stdout.flush()
            result = self._call_handler(query_type, message, from_number, media_urls, urls, request_form)
            print(f"[orchestrator.route] → Handler returned: {result is not None}", flush=True)
            sys.stdout.flush()
            return result

        # 3. Commute/Fuel/Events (existing handlers)
        if query_type in [QueryType.COMMUTE, QueryType.FUEL, QueryType.EVENT]:
            print(f"[orchestrator.route] → Has handler for {query_type}")
            return self._call_handler(query_type, message, from_number, media_urls, urls, request_form)

        # No handler matched
        print(f"[orchestrator.route] → No matching handler")
        print(f"[orchestrator.route] ═══ ROUTING END (NONE) ═══\n")
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
        life_advice_keywords = ["frustrated", "help me", "help with", "how do i", "what should i", "advice", "worried", "stressed", "upset", "anxious", "depressed", "job", "work", "relationship", "family"]
        if any(x in m for x in life_advice_keywords):
            matched = [x for x in life_advice_keywords if x in m]
            print(f"[orchestrator._classify] LIFE_ADVICE (matched: {matched})")
            return QueryType.LIFE_ADVICE

        # Shopping queries
        shopping_keywords = ["should i buy", "is this worth", "good price", "good deal", "compare", "vs "]
        if any(x in m for x in shopping_keywords):
            matched = [x for x in shopping_keywords if x in m]
            print(f"[orchestrator._classify] SHOPPING (matched: {matched})")
            return QueryType.SHOPPING

        # Research queries
        research_keywords = ["tell me about", "explain", "what is", "who is"]
        if any(x in m for x in research_keywords):
            matched = [x for x in research_keywords if x in m]
            print(f"[orchestrator._classify] RESEARCH (matched: {matched})")
            return QueryType.RESEARCH

        # Commute queries
        commute_keywords = ["train", "commute", "transport", "next", "waterloo"]
        if any(x in m for x in commute_keywords):
            matched = [x for x in commute_keywords if x in m]
            print(f"[orchestrator._classify] COMMUTE (matched: {matched})")
            return QueryType.COMMUTE

        # Fuel queries
        fuel_keywords = ["fuel", "petrol", "diesel", "price"]
        if any(x in m for x in fuel_keywords):
            matched = [x for x in fuel_keywords if x in m]
            print(f"[orchestrator._classify] FUEL (matched: {matched})")
            return QueryType.FUEL

        # Event queries
        event_keywords = ["event", "calendar", "upcoming"]
        if any(x in m for x in event_keywords):
            matched = [x for x in event_keywords if x in m]
            print(f"[orchestrator._classify] EVENT (matched: {matched})")
            return QueryType.EVENT

        print(f"[orchestrator._classify] UNKNOWN: '{message[:50]}'")
        return QueryType.UNKNOWN

    def _call_handler(self, query_type: QueryType, message: str, from_number: str, media_urls: List[str], urls: List[str], request_form=None) -> Optional[Dict]:
        """Call the registered handler for this query type"""
        if query_type not in self.handlers:
            print(f"[orchestrator] No handler registered for {query_type}")
            return None

        handler = self.handlers[query_type]
        print(f"[orchestrator] Found handler for {query_type}, calling...")

        try:
            # Call handler with appropriate parameters
            result = handler(
                message=message,
                from_number=from_number,
                media_urls=media_urls,
                urls=urls,
                request_form=request_form
            )
            print(f"[orchestrator] Handler returned: {result}")
            return result
        except Exception as e:
            print(f"[orchestrator] Handler error for {query_type}: {e}")
            import traceback
            traceback.print_exc()
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
