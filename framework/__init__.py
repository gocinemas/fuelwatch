"""
FrameWork: Deep App Idea Validator

4-phase research pipeline:
1. App Scraping (extract positioning, features, design)
2. Market Research (Reddit + competitors + trends)
3. Framework Analysis (score across 4 dimensions)
4. Report Generation (improvements, pivots, risks)
"""

from framework.reddit_research import validate_problem
from framework.competitor_analysis import analyze_competition
from framework.scoring import (
    score_idea_validation,
    score_market_potential,
    score_design_quality,
    score_execution_risk,
    calculate_overall_score,
    generate_improvements,
    generate_pivots,
    generate_risk_assessment,
)

__all__ = [
    'validate_problem',
    'analyze_competition',
    'score_idea_validation',
    'score_market_potential',
    'score_design_quality',
    'score_execution_risk',
    'calculate_overall_score',
    'generate_improvements',
    'generate_pivots',
    'generate_risk_assessment',
]
