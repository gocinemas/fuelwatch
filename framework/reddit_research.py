"""
FrameWork Phase 2a: Reddit Problem Validation
Search Reddit for problem mentions, sentiment, user needs.

Returns validation score + citations.
"""

import requests
import re
from datetime import datetime, timedelta
from collections import Counter


def infer_category(app_analysis: dict) -> str:
    """
    Infer product category from app analysis.
    Used to search relevant subreddits.

    Examples:
    - Productivity app → r/productivity, r/startups, r/saas
    - Design tool → r/design, r/UX_Design, r/web_design
    - Writing tool → r/writing, r/AuthorTherapy, r/Blogging
    - Fitness app → r/fitness, r/EverythingFitness
    """
    value_prop = (app_analysis.get("value_prop") or "").lower()
    features = " ".join([f.lower() for f in app_analysis.get("features", [])])
    combined = value_prop + " " + features

    # Category mapping
    categories = {
        "productivity": ["productivity", "task", "todo", "project management", "workflow", "efficiency"],
        "design": ["design", "ui", "ux", "graphic", "visual", "illustration", "figma"],
        "writing": ["writing", "blog", "content", "copywriting", "novel", "author"],
        "fitness": ["fitness", "workout", "exercise", "health", "gym"],
        "finance": ["finance", "money", "budget", "investment", "crypto", "payment"],
        "social": ["social", "community", "network", "messaging", "chat"],
        "ai": ["ai", "machine learning", "neural", "gpt", "llm", "automation"],
        "developer": ["developer", "coding", "programming", "api", "devops", "backend"],
        "saas": ["saas", "business", "enterprise", "b2b", "company"],
    }

    best_category = "saas"  # Default
    best_score = 0

    for category, keywords in categories.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > best_score:
            best_score = score
            best_category = category

    return best_category


def get_subreddits_for_category(category: str) -> list:
    """
    Get relevant subreddits to search based on category.
    """
    mapping = {
        "productivity": ["r/productivity", "r/startups", "r/saas", "r/LifeProTips", "r/GetStudying"],
        "design": ["r/design", "r/UX_Design", "r/web_design", "r/UI_Design", "r/graphic_design"],
        "writing": ["r/writing", "r/AuthorTherapy", "r/Blogging", "r/NaNoWriMo", "r/Screenwriting"],
        "fitness": ["r/fitness", "r/EverythingFitness", "r/WeightLossAdvice", "r/Stronglifts5x5"],
        "finance": ["r/personalfinance", "r/investing", "r/Cryptocurrency", "r/Frugal"],
        "social": ["r/socialskills", "r/CasualConversation", "r/NoStupidQuestions"],
        "ai": ["r/MachineLearning", "r/LanguageModels", "r/OpenAI", "r/ArtificialIntelligence"],
        "developer": ["r/learnprogramming", "r/webdev", "r/node", "r/Python"],
        "saas": ["r/startups", "r/saas", "r/Entrepreneur", "r/smallbusiness"],
    }

    return mapping.get(category, ["r/startups", "r/saas", "r/Entrepreneur"])


def search_reddit_for_problem(problem_keywords: str, subreddits: list = None, limit: int = 100) -> dict:
    """
    Search Reddit for problem mentions and sentiment.

    Uses Pushshift/Reddit API to search posts mentioning the problem.
    Returns: List of threads with sentiment analysis.

    NOTE: Using Pushshift alternative or Reddit PRAW library in production.
    For MVP, using basic keyword search + sentiment heuristics.
    """
    if not subreddits:
        subreddits = ["r/startups", "r/saas"]

    threads_found = []
    total_mentions = 0
    sentiment_scores = []

    # In production: Use PRAW (Python Reddit API Wrapper)
    # For MVP: Return mock data with realistic structure
    # Real implementation would hit Reddit API or Pushshift

    mock_threads = _get_mock_reddit_data(problem_keywords, subreddits)

    for thread in mock_threads:
        sentiment = _analyze_sentiment(thread["title"] + " " + thread["body"])
        threads_found.append({
            "title": thread["title"],
            "subreddit": thread["subreddit"],
            "url": thread["url"],
            "score": thread["score"],
            "sentiment": sentiment,  # -1 to +1
            "comments": thread.get("comments", 0),
        })
        sentiment_scores.append(sentiment)
        total_mentions += 1

    # Calculate aggregate metrics
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
    positive_mentions = sum(1 for s in sentiment_scores if s > 0.3)

    return {
        "problem_keywords": problem_keywords,
        "threads_analyzed": len(threads_found),
        "total_mentions": total_mentions,
        "average_sentiment": avg_sentiment,  # -1 to +1
        "positive_mentions": positive_mentions,
        "threads": threads_found[:5],  # Top 5
        "validation_score": _calculate_validation_score(
            total_mentions, avg_sentiment, positive_mentions
        )
    }


def _analyze_sentiment(text: str) -> float:
    """
    Simple sentiment analysis using keyword heuristics.
    Returns -1 to +1 score.

    Production: Use Groq API or transformers library.
    """
    text_lower = text.lower()

    positive_words = ["love", "amazing", "great", "excellent", "need", "want", "help", "finally", "solved"]
    negative_words = ["hate", "terrible", "bad", "frustrat", "broken", "useless", "avoid"]
    problem_words = ["problem", "issue", "struggle", "difficult", "wish", "would be nice"]

    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)
    prob_count = sum(1 for word in problem_words if word in text_lower)

    # Problem mentions are slightly positive (means there's demand)
    score = (pos_count - neg_count + prob_count * 0.5) / max(1, len(text_lower) / 100)
    return min(1, max(-1, score))  # Clamp to -1..+1


def _calculate_validation_score(mentions: int, sentiment: float, positive: int) -> int:
    """
    Calculate idea validation score (0-100) based on Reddit analysis.
    """
    # Scoring logic
    mention_score = min(100, (mentions / 5) * 100)  # Max at 5+ mentions
    sentiment_score = max(0, ((sentiment + 1) / 2) * 100)  # Convert -1..+1 to 0..100
    positive_score = min(100, (positive / max(1, mentions)) * 100)  # % positive

    # Weight: mentions (40%), sentiment (40%), positive ratio (20%)
    weighted = (mention_score * 0.4) + (sentiment_score * 0.4) + (positive_score * 0.2)

    return int(weighted)


def _get_mock_reddit_data(problem_keywords: str, subreddits: list) -> list:
    """
    Mock Reddit data for MVP.
    In production, replace with real Reddit API calls.
    """
    mock_data = [
        {
            "title": f"Need better solution for {problem_keywords}",
            "body": "I've been struggling with this for months. There has to be a better way.",
            "subreddit": subreddits[0] if subreddits else "r/startups",
            "url": "https://reddit.com/r/startups/...",
            "score": 342,
            "comments": 67,
        },
        {
            "title": f"Does anyone else have trouble with {problem_keywords}?",
            "body": "Feeling frustrated. Existing solutions are lacking. Would love to see...",
            "subreddit": subreddits[1] if len(subreddits) > 1 else "r/saas",
            "url": "https://reddit.com/r/saas/...",
            "score": 189,
            "comments": 42,
        },
        {
            "title": f"Built a solution for {problem_keywords} - feedback needed",
            "body": "After months of work, finally shipped. Early validation is amazing.",
            "subreddit": subreddits[0] if subreddits else "r/startups",
            "url": "https://reddit.com/r/startups/...",
            "score": 523,
            "comments": 89,
        },
    ]

    return mock_data[:3]


# Main entry point
def validate_problem(app_analysis: dict) -> dict:
    """
    Full pipeline: infer category → search subreddits → analyze sentiment → score.
    """
    try:
        category = infer_category(app_analysis)
        subreddits = get_subreddits_for_category(category)
        problem_keywords = app_analysis.get("value_prop", "solution").split()[0:3]
        problem_keywords_str = " ".join(problem_keywords) or "this"

        result = search_reddit_for_problem(problem_keywords_str, subreddits)

        return {
            "status": "ok",
            "category": category,
            "subreddits_searched": subreddits,
            "problem_validation": result
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}
