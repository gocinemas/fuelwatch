"""
Miru Assistant — Personal Intelligence Agent
Shopping decisions, life advice, research queries
"""
import os
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum


class QueryType(Enum):
    SHOPPING = "shopping"      # Should I buy X?
    COMPARISON = "comparison"  # Compare X vs Y
    LIFE_ADVICE = "life_advice"  # How do I handle X?
    RESEARCH = "research"      # Tell me about X
    UNKNOWN = "unknown"


class MiruAssistant:
    """Route queries to appropriate agents"""

    def __init__(self, user_phone: str = None):
        self.user_phone = user_phone
        self.judgments = []  # Track user decisions to learn taste

    def process_query(self, query: str, media_urls: List[str] = None, urls: List[str] = None) -> Dict:
        """Route query to appropriate agent with optional media/URLs"""
        # Extract info from media
        media_info = ""
        if media_urls:
            media_info = self._analyze_images(media_urls)
            query = f"{query}\n[Image analysis: {media_info}]"

        # Extract info from URLs
        url_info = ""
        if urls:
            url_info = self._extract_url_info(urls)
            query = f"{query}\n[URL info: {url_info}]"

        query_type = self._classify_query(query)

        if query_type == QueryType.SHOPPING:
            return self._shopping_agent(query)
        elif query_type == QueryType.COMPARISON:
            return self._comparison_agent(query)
        elif query_type == QueryType.LIFE_ADVICE:
            return self._life_advisor_agent(query)
        elif query_type == QueryType.RESEARCH:
            return self._research_agent(query)
        else:
            return {"error": "I didn't understand. Try: 'should I buy...', 'compare...', 'how do I...', or 'tell me about...'"}

    def _classify_query(self, query: str) -> QueryType:
        """Classify query type (context-aware)"""
        q = query.lower().strip()

        # Single word? Probably not a shopping query — needs full context
        if len(q.split()) <= 2 and not any(x in q for x in ["should", "compare", "vs", "worth", "price"]):
            # Could be life advice follow-up or research
            if any(x in q for x in ["help", "advice", "frustrated", "upset", "worried", "stressed"]):
                return QueryType.LIFE_ADVICE
            return QueryType.RESEARCH

        if any(x in q for x in ["should i buy", "is this worth", "worth buying", "good price", "good deal", "should i get"]):
            return QueryType.SHOPPING

        if any(x in q for x in ["compare", "vs", "versus", "which is better", "better", "best"]):
            return QueryType.COMPARISON

        if any(x in q for x in ["how do i", "what should i", "help with", "advice", "should i do", "how should i", "frustrated", "upset", "worried", "stressed", "anxious"]):
            return QueryType.LIFE_ADVICE

        if any(x in q for x in ["tell me", "about", "explain", "what is", "who is", "why"]):
            return QueryType.RESEARCH

        return QueryType.UNKNOWN

    def _shopping_agent(self, query: str) -> Dict:
        """Analyze shopping decisions"""
        # Extract product name and price if mentioned
        product = self._extract_product(query)

        return {
            "type": "shopping",
            "product": product,
            "analysis": {
                "price_range": "Research via Google Shopping",
                "specs": "Parse from query or search",
                "alternatives": ["Alternative 1", "Alternative 2"],
                "value_score": 7.5,  # Out of 10
                "recommendation": "Good choice at this price"
            },
            "next_steps": [
                "Check reviews on Trustpilot",
                "Compare with alternatives",
                "Check return policy"
            ]
        }

    def _comparison_agent(self, query: str) -> Dict:
        """Compare products/options"""
        items = self._extract_items_to_compare(query)

        return {
            "type": "comparison",
            "items": items,
            "analysis": {
                "item_1": {"score": 8, "pros": [], "cons": []},
                "item_2": {"score": 7, "pros": [], "cons": []},
                "winner": "Item 1",
                "reasoning": "Better value and features"
            }
        }

    def _life_advisor_agent(self, query: str) -> Dict:
        """Help with life decisions"""
        situation = query

        return {
            "type": "life_advice",
            "situation": situation,
            "analysis": {
                "key_factors": ["Factor 1", "Factor 2", "Factor 3"],
                "options": [
                    {"option": "Option A", "pros": [], "cons": [], "risk": "low"},
                    {"option": "Option B", "pros": [], "cons": [], "risk": "medium"}
                ],
                "recommendation": "Based on your situation, consider Option A because...",
                "questions_to_ask": [
                    "What's your timeline?",
                    "What's your budget?"
                ]
            }
        }

    def _research_agent(self, query: str) -> Dict:
        """Research/informational queries"""
        topic = self._extract_topic(query)

        return {
            "type": "research",
            "topic": topic,
            "summary": "Research summary here",
            "details": {
                "overview": "...",
                "key_points": ["Point 1", "Point 2"],
                "sources": []
            }
        }

    def _extract_product(self, query: str) -> str:
        """Extract product name from query"""
        # Simple extraction - can be enhanced with NLP
        words = query.lower().split()
        if "buy" in words:
            idx = words.index("buy")
            return " ".join(words[idx+1:idx+4])
        return "Unknown product"

    def _extract_items_to_compare(self, query: str) -> List[str]:
        """Extract items to compare"""
        if " vs " in query.lower():
            return query.lower().split(" vs ")
        return []

    def _extract_topic(self, query: str) -> str:
        """Extract research topic"""
        return query.replace("tell me about", "").replace("what is", "").strip()

    def _analyze_images(self, media_urls: List[str]) -> str:
        """Analyze images asynchronously — return placeholder for now"""
        if not media_urls:
            return ""

        # TODO: Implement Claude vision analysis in background
        # For now, just indicate image was received
        return f"[{len(media_urls)} image(s) attached for analysis]"

    def _extract_url_info(self, urls: List[str]) -> str:
        """Extract product info from URLs"""
        info = []
        for url in urls:
            try:
                # Try to fetch and parse
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(url, headers=headers, timeout=5)

                # Simple extraction
                if "amazon" in url.lower():
                    info.append(f"Amazon link: {url}")
                elif "ebay" in url.lower():
                    info.append(f"eBay link: {url}")
                elif "price" in url.lower() or "shop" in url.lower():
                    info.append(f"Shopping link: {url}")
                else:
                    # Extract title if possible
                    if "<title>" in response.text:
                        title = response.text.split("<title>")[1].split("</title>")[0]
                        info.append(f"Page: {title[:100]}")

            except Exception as e:
                info.append(f"URL: {url}")

        return " | ".join(info) if info else "Could not extract URL info"

    def record_judgment(self, query: str, outcome: str, feedback: str = ""):
        """Record user judgment to learn taste"""
        self.judgments.append({
            "query": query,
            "outcome": outcome,  # "helpful", "not_helpful", "agree", "disagree"
            "feedback": feedback,
            "timestamp": datetime.now().isoformat(),
            "user": self.user_phone
        })
        # TODO: Save to database and use for future recommendations


def get_miru_assistant(phone: str = None) -> MiruAssistant:
    """Get or create Miru Assistant instance"""
    return MiruAssistant(user_phone=phone)
