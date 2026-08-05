"""
Persistent Company Intelligence Database
Stores all queries, answers, and builds company profiles over time.
"""

import json
from datetime import datetime
import library as lib
import logging

logger = logging.getLogger(__name__)


class CompanyKnowledgeBase:
    """Store and retrieve company intelligence from Supabase."""

    @staticmethod
    def save_query(company_name: str, question: str, answer: str, source: str = "claude_api"):
        """Save Q&A to database."""
        try:
            # Insert into company_queries table
            lib._sb().table("company_queries").insert({
                "company_name": company_name.lower().strip(),
                "question": question,
                "answer": answer,
                "source": source,
                "created_at": datetime.utcnow().isoformat(),
            }).execute()

            logger.info(f"[knowledge] Stored query for {company_name}: {question[:50]}")

            # Update company profile
            CompanyKnowledgeBase._update_profile(company_name, question, answer)

        except Exception as e:
            logger.error(f"[knowledge] Failed to save query: {e}")

    @staticmethod
    def get_company_history(company_name: str, limit: int = 10) -> list:
        """Get recent queries about a company."""
        try:
            results = lib._sb().table("company_queries") \
                .select("*") \
                .eq("company_name", company_name.lower().strip()) \
                .order("created_at", desc=True) \
                .limit(limit) \
                .execute()

            return results.data or []

        except Exception as e:
            logger.error(f"[knowledge] Failed to get history: {e}")
            return []

    @staticmethod
    def _update_profile(company_name: str, question: str, answer: str):
        """Update company profile with new insight."""
        try:
            company_lower = company_name.lower().strip()

            # Get or create profile
            existing = lib._sb().table("company_profiles") \
                .select("*") \
                .eq("company_name", company_lower) \
                .limit(1) \
                .execute()

            profile = existing.data[0]["data"] if existing.data else {
                "company_name": company_name,
                "total_queries": 0,
                "recent_topics": [],
                "ai_mentions": 0,
                "hiring_mentions": 0,
                "strategy_mentions": 0,
                "risk_mentions": 0,
                "created_at": datetime.utcnow().isoformat(),
            }

            # Update counters
            profile["total_queries"] = profile.get("total_queries", 0) + 1
            profile["last_updated"] = datetime.utcnow().isoformat()

            # Track topic mentions
            question_lower = question.lower()
            if any(word in question_lower for word in ["ai", "artificial intelligence", "machine learning", "genai"]):
                profile["ai_mentions"] = profile.get("ai_mentions", 0) + 1

            if any(word in question_lower for word in ["hire", "hiring", "recruitment", "talent", "employee"]):
                profile["hiring_mentions"] = profile.get("hiring_mentions", 0) + 1

            if any(word in question_lower for word in ["strategy", "position", "competitive", "win"]):
                profile["strategy_mentions"] = profile.get("strategy_mentions", 0) + 1

            if any(word in question_lower for word in ["risk", "threat", "challenge", "problem"]):
                profile["risk_mentions"] = profile.get("risk_mentions", 0) + 1

            # Add to recent topics
            topic = question[:60]
            if topic not in profile.get("recent_topics", []):
                profile["recent_topics"] = [topic] + profile.get("recent_topics", [])[:9]

            # Save profile
            if existing.data:
                lib._sb().table("company_profiles") \
                    .update({"data": profile}) \
                    .eq("company_name", company_lower) \
                    .execute()
            else:
                lib._sb().table("company_profiles").insert({
                    "company_name": company_lower,
                    "data": profile,
                }).execute()

            logger.info(f"[knowledge] Updated profile for {company_name}")

        except Exception as e:
            logger.error(f"[knowledge] Failed to update profile: {e}")

    @staticmethod
    def get_profile(company_name: str) -> dict:
        """Get company profile with all stored intelligence."""
        try:
            company_lower = company_name.lower().strip()

            results = lib._sb().table("company_profiles") \
                .select("*") \
                .eq("company_name", company_lower) \
                .limit(1) \
                .execute()

            if results.data:
                return results.data[0]["data"]

            return {}

        except Exception as e:
            logger.error(f"[knowledge] Failed to get profile: {e}")
            return {}

    @staticmethod
    def get_trends(company_name: str) -> dict:
        """Get trends from stored data (what people ask about)."""
        try:
            company_lower = company_name.lower().strip()

            # Get last 30 queries
            history = lib._sb().table("company_queries") \
                .select("question") \
                .eq("company_name", company_lower) \
                .order("created_at", desc=True) \
                .limit(30) \
                .execute()

            if not history.data:
                return {}

            questions = [q["question"] for q in history.data]

            # Count mentions
            ai_count = sum(1 for q in questions if any(w in q.lower() for w in ["ai", "ml", "genai"]))
            hiring_count = sum(1 for q in questions if any(w in q.lower() for w in ["hire", "talent", "employee"]))
            strategy_count = sum(1 for q in questions if any(w in q.lower() for w in ["strategy", "competitive"]))
            risk_count = sum(1 for q in questions if any(w in q.lower() for w in ["risk", "threat"]))

            return {
                "total_queries": len(questions),
                "ai_interest": f"{(ai_count/len(questions)*100):.0f}%",
                "hiring_interest": f"{(hiring_count/len(questions)*100):.0f}%",
                "strategy_interest": f"{(strategy_count/len(questions)*100):.0f}%",
                "risk_interest": f"{(risk_count/len(questions)*100):.0f}%",
                "top_questions": questions[:5],
            }

        except Exception as e:
            logger.error(f"[knowledge] Failed to get trends: {e}")
            return {}
