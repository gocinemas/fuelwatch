"""
Intelligence Tabbed Service
Aggregates and transforms company data for the two-tab interface
Tab 1: SIGNAL (3 quick metrics per company)
Tab 2: INTELLIGENCE (deep dive sections)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class TabbedIntelligenceService:
    """Transforms raw intelligence data into tabbed format."""

    # Competitor mapping (add more as needed)
    COMPETITOR_MAP = {
        "reckitt": ["Henkel", "Unilever", "SC Johnson"],
        "henkel": ["Reckitt", "Unilever", "SC Johnson"],
        "unilever": ["Reckitt", "Henkel", "SC Johnson"],
        "sc johnson": ["Reckitt", "Henkel", "Unilever"],
    }

    @staticmethod
    def get_available_competitors(company_name: str) -> List[str]:
        """Get list of competitors for a company."""
        return TabbedIntelligenceService.COMPETITOR_MAP.get(
            company_name.lower(), []
        )

    @staticmethod
    def build_signal_metrics(company_name: str, raw_signals: Dict) -> Dict:
        """
        Transform raw signals into SIGNAL tab metrics (3 only).

        Returns:
        {
            "market_direction": "up|down|flat",
            "risk_level": "high|medium|low",
            "watch_item": "string or None"
        }
        """
        try:
            # 1. MARKET DIRECTION (from stock movement)
            market_direction = "flat"
            stock = raw_signals.get("stock", {})
            if stock.get("direction") == "up":
                market_direction = "up"
            elif stock.get("direction") == "down":
                market_direction = "down"

            # 2. RISK LEVEL (derived from sentiment + news)
            risk_level = "medium"  # Default
            sentiment_score = raw_signals.get("sentiment", {}).get("score", 50)
            news_count = raw_signals.get("news", {}).get("count", 0)

            # Risk calculation:
            # Low risk: high sentiment (>60) + stable/positive stock
            # High risk: low sentiment (<40) + negative news/stock
            if sentiment_score > 60 and market_direction != "down":
                risk_level = "low"
            elif sentiment_score < 40 or (market_direction == "down" and news_count > 10):
                risk_level = "high"

            # 3. WATCH ITEM (critical news or exec change)
            watch_item = None
            news_signals = raw_signals.get("news_signals", {})

            # Check for lawsuit news
            categories = news_signals.get("categories", {})
            if categories.get("lawsuit", 0) > 0:
                watch_item = "Lawsuit filed"
            # Check for acquisition
            elif categories.get("acquisition", 0) > 0:
                watch_item = "Acquisition activity"
            # Check for earnings miss (negative stock movement)
            elif market_direction == "down" and news_count > 5:
                watch_item = "Negative outlook"
            # Check for high hiring (could be expansion or filling gaps)
            elif raw_signals.get("hiring", {}).get("direction") == "up" and market_direction == "down":
                watch_item = "Expanding amid downturn"

            return {
                "market_direction": market_direction,
                "risk_level": risk_level,
                "watch_item": watch_item,
            }

        except Exception as e:
            logger.error(f"[signal_metrics] Error: {e}")
            return {
                "market_direction": "flat",
                "risk_level": "medium",
                "watch_item": None,
            }

    @staticmethod
    def build_intelligence_data(company_name: str, raw_signals: Dict) -> Dict:
        """
        Transform raw signals into INTELLIGENCE tab sections.

        Returns:
        {
            "recent_news": [...],
            "sentiment": {...},
            "hiring": {...},
            "leadership": [...],
        }
        """
        try:
            result = {}

            # RECENT NEWS
            news_signals = raw_signals.get("news_signals", {})
            recent_news = []
            if news_signals.get("top_articles"):
                for article in news_signals["top_articles"][:5]:
                    recent_news.append({
                        "title": article.get("title", "—"),
                        "source": article.get("source", "Unknown"),
                        "published": article.get("published", ""),
                    })
            result["recent_news"] = recent_news

            # MARKET SENTIMENT
            sentiment = {}
            sentiment_signals = raw_signals.get("sentiment", {})

            # Trustpilot score (0-100)
            trustpilot_data = sentiment_signals.get("trustpilot", {})
            if trustpilot_data.get("company_rating", {}).get("rating"):
                rating = trustpilot_data["company_rating"]["rating"]
                sentiment["trustpilot_score"] = int((rating / 5) * 100)
            else:
                sentiment["trustpilot_score"] = 50

            # Hacker News sentiment (0-100)
            hacker_news_data = sentiment_signals.get("reddit", {})  # HN is in 'reddit' key
            sentiment["hacker_news_score"] = hacker_news_data.get("sentiment_score", 50)

            result["sentiment"] = sentiment

            # HIRING
            hiring = {}
            hiring_signals = raw_signals.get("hiring_signals", {})
            hiring["total_roles"] = hiring_signals.get("linkedin_job_count", "—")
            hiring["growth_pct"] = hiring_signals.get("trend", "—")
            hiring["top_regions"] = "US, UK, EMEA"  # Default; could be enhanced
            result["hiring"] = hiring

            # LEADERSHIP
            leadership = []
            exec_changes = raw_signals.get("executive_changes", {})
            if exec_changes.get("recent_changes"):
                for exec in exec_changes["recent_changes"][:5]:
                    leadership.append({
                        "name": exec.get("name", "Unknown"),
                        "title": exec.get("title", "Executive"),
                    })

            # Fallback to competitive position if available
            if not leadership:
                comp_pos = raw_signals.get("competitive_position", {})
                # This is a limitation of current data; we'll note it
                pass

            result["leadership"] = leadership

            return result

        except Exception as e:
            logger.error(f"[intelligence_data] Error: {e}")
            return {
                "recent_news": [],
                "sentiment": {"trustpilot_score": 50, "hacker_news_score": 50},
                "hiring": {"total_roles": "—", "growth_pct": "—", "top_regions": "—"},
                "leadership": [],
            }

    @staticmethod
    def aggregate_for_both_tabs(
        company_name: str,
        competitor_name: str,
        company_signals: Dict,
        competitor_signals: Dict,
    ) -> Dict:
        """
        Prepare complete data for both tabs.

        Returns:
        {
            "company": "Reckitt",
            "competitor": "Henkel",
            "signals": {
                "Reckitt": {...},
                "Henkel": {...}
            },
            "intelligence": {
                "Reckitt": {...},
                "Henkel": {...}
            },
            "available_competitors": [...],
            "timestamp": "2026-08-04T..."
        }
        """
        try:
            return {
                "company": company_name,
                "competitor": competitor_name,
                "signals": {
                    company_name: TabbedIntelligenceService.build_signal_metrics(
                        company_name, company_signals
                    ),
                    competitor_name: TabbedIntelligenceService.build_signal_metrics(
                        competitor_name, competitor_signals
                    ),
                },
                "intelligence": {
                    company_name: TabbedIntelligenceService.build_intelligence_data(
                        company_name, company_signals
                    ),
                    competitor_name: TabbedIntelligenceService.build_intelligence_data(
                        competitor_name, competitor_signals
                    ),
                },
                "available_competitors": TabbedIntelligenceService.get_available_competitors(
                    company_name
                ),
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error(f"[aggregate] Error: {e}")
            return {"error": str(e)}
