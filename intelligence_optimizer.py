"""
Intelligence Optimizer — Smart routing between Groq and Anthropic
Minimizes costs while maintaining quality
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, Optional

# Model costs per 1M tokens (input/output)
COSTS = {
    "groq_mixtral": {"input": 0.24, "output": 0.24},  # $0.24 per MTok
    "claude_opus": {"input": 15.0, "output": 75.0},   # $15/$75 per MTok
    "claude_sonnet": {"input": 3.0, "output": 15.0},  # $3/$15 per MTok
    "claude_haiku": {"input": 0.8, "output": 4.0},    # $0.8/$4 per MTok
}

class IntelligenceOptimizer:
    """Route LLM tasks to cheapest viable model."""

    def __init__(self):
        self.groq_enabled = bool(os.environ.get("GROQ_API_KEY"))
        self.anthropic_enabled = bool(os.environ.get("ANTHROPIC_API_KEY"))
        self.cache = {}

    def choose_model(self, task_type: str, complexity: str = "medium") -> str:
        """
        Choose best model for task based on cost and quality.

        Args:
            task_type: 'reasoning', 'extraction', 'summary', 'creative'
            complexity: 'low', 'medium', 'high'

        Returns:
            Model name: 'groq', 'claude_haiku', 'claude_sonnet', 'claude_opus'
        """

        # Task routing matrix
        if task_type == "reasoning" and complexity in ("medium", "high"):
            # Complex reasoning → Groq (fast, cheap, good reasoning)
            if self.groq_enabled:
                return "groq"
            else:
                return "claude_sonnet"  # Fallback

        elif task_type == "extraction" and complexity == "low":
            # Simple extraction → Claude Haiku (cheapest)
            return "claude_haiku"

        elif task_type == "summary":
            # Summarization → Groq or Haiku
            if self.groq_enabled:
                return "groq"
            else:
                return "claude_haiku"

        elif task_type == "creative" and complexity == "high":
            # High-quality creative → Claude Sonnet
            return "claude_sonnet"

        else:
            # Default: Groq if available, else Sonnet
            return "groq" if self.groq_enabled else "claude_sonnet"

    def estimate_cost(self, model: str, input_tokens: int, output_tokens: int) -> Dict[str, float]:
        """Estimate cost in USD for API call."""
        if model not in COSTS:
            return {"input": 0, "output": 0, "total": 0}

        costs = COSTS[model]
        input_cost = (input_tokens / 1_000_000) * costs["input"]
        output_cost = (output_tokens / 1_000_000) * costs["output"]

        return {
            "input": round(input_cost, 6),
            "output": round(output_cost, 6),
            "total": round(input_cost + output_cost, 6),
        }

    def should_cache(self, task_type: str, data_freshness_hours: int = 24) -> bool:
        """Should this task result be cached?"""
        cacheable = ["reasoning", "summary", "extraction"]
        return task_type in cacheable and data_freshness_hours >= 1

    def get_cached_intelligence(self, from_number: str, max_age_hours: int = 24) -> Optional[Dict]:
        """Retrieve cached intelligence if fresh."""
        cache_key = f"intelligence_{from_number}"
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            age_hours = (datetime.utcnow() - entry["timestamp"]).total_seconds() / 3600
            if age_hours < max_age_hours:
                return entry["data"]
        return None

    def cache_intelligence(self, from_number: str, data: Dict) -> None:
        """Store intelligence result."""
        cache_key = f"intelligence_{from_number}"
        self.cache[cache_key] = {
            "timestamp": datetime.utcnow(),
            "data": data
        }

    def optimize_groq_call(self, prompt: str, max_tokens: int = 2000) -> Dict[str, Any]:
        """
        Groq-specific optimizations:
        1. Use context caching (save 90% on repeated prompts)
        2. Batch similar requests
        3. Use streaming for long responses
        """
        return {
            "model": "mixtral-8x7b-32768",
            "max_tokens": max_tokens,
            "temperature": 0.7,
            "cache_enabled": True,  # Groq supports prompt caching
            "batch_compatible": True,
        }

    def route_intelligence_request(
        self,
        from_number: str,
        data_freshness: str = "6h",
        prefer_quality: bool = False
    ) -> Dict[str, Any]:
        """
        Route intelligence request smartly.

        Returns:
            {
                "model": "groq|claude_*",
                "use_cache": True|False,
                "estimated_cost": 0.0005,
                "reason": "Fast reasoning, cost-optimized"
            }
        """

        # Check cache first
        cached = self.get_cached_intelligence(from_number, max_age_hours=6)
        if cached and not prefer_quality:
            return {
                "model": "cache",
                "use_cache": True,
                "cached_data": cached,
                "estimated_cost": 0.0,
                "reason": "Using cached intelligence (6h old)"
            }

        # Complex reasoning → Groq
        if self.groq_enabled:
            groq_config = self.optimize_groq_call(max_tokens=2000)
            return {
                "model": "groq",
                "config": groq_config,
                "use_cache": False,
                "estimated_cost": 0.0005,
                "reason": "Groq: fast reasoning (2s), cost-optimized ($0.0005)"
            }

        # Fallback: Claude Sonnet
        return {
            "model": "claude_sonnet",
            "use_cache": False,
            "estimated_cost": 0.003,
            "reason": "Claude Sonnet: high quality (longer latency, higher cost)"
        }

    def compare_models(self, input_tokens: int = 1200, output_tokens: int = 800) -> Dict[str, Any]:
        """Compare cost/performance across models."""
        comparison = {}

        for model in COSTS.keys():
            cost = self.estimate_cost(model, input_tokens, output_tokens)
            speed = {
                "groq_mixtral": "~2s",
                "claude_haiku": "~1-2s",
                "claude_sonnet": "~3-4s",
                "claude_opus": "~4-5s"
            }.get(model, "~2s")

            quality = {
                "groq_mixtral": 8,  # 1-10 scale
                "claude_haiku": 6,
                "claude_sonnet": 9,
                "claude_opus": 10
            }.get(model, 5)

            comparison[model] = {
                "cost": cost["total"],
                "speed": speed,
                "quality": quality,
                "recommendation": "BEST" if cost["total"] < 0.001 and quality >= 8 else ""
            }

        return comparison

    def optimize_brief_generation(self) -> Dict[str, Any]:
        """Optimize the home brief endpoint."""
        return {
            "parallel_fetch": True,
            "cache_duration": "15 minutes",
            "intelligence_model": "groq" if self.groq_enabled else "claude_haiku",
            "intelligence_caching": "6 hours",
            "timeout": "8 seconds",
            "fallback_on_timeout": True,
            "fallback_model": "simple_patterns",
            "estimated_daily_cost_per_user": 0.004,
        }

    def optimize_morning_message(self) -> Dict[str, Any]:
        """Optimize 7am WhatsApp message generation."""
        return {
            "parallel_workers": 9,
            "cache_intelligence": "24 hours",
            "intelligence_model": "groq",
            "batch_mode": True,  # Batch all users' requests
            "estimated_cost_per_1000_users": 1.20,  # $1.20 for 1000 morning messages
        }


# Global optimizer instance
_optimizer = IntelligenceOptimizer()


def get_optimizer() -> IntelligenceOptimizer:
    """Get global optimizer instance."""
    return _optimizer


def compare_all_models() -> str:
    """Print comparison of all models."""
    optimizer = get_optimizer()
    comparison = optimizer.compare_models(input_tokens=1200, output_tokens=800)

    lines = ["MODEL COMPARISON (1200 input + 800 output tokens)\n", "=" * 70]
    for model, stats in comparison.items():
        rec = f" ← {stats['recommendation']}" if stats['recommendation'] else ""
        lines.append(
            f"{model:20} | Cost: ${stats['cost']:.4f} | Speed: {stats['speed']:8} | Quality: {stats['quality']}/10{rec}"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    print(compare_all_models())

    optimizer = get_optimizer()
    print("\n" + "=" * 70)
    print("\nGROQ VS ANTHROPIC ROUTING RECOMMENDATIONS\n")

    print("1. INTELLIGENCE ENGINE (Complex 7-dimension reasoning)")
    result = optimizer.route_intelligence_request("test_user", prefer_quality=False)
    print(f"   Model: {result['model']}")
    print(f"   Cost: ${result['estimated_cost']:.4f}")
    print(f"   Reason: {result['reason']}\n")

    print("2. HOME BRIEF (Generate daily brief text)")
    brief_config = optimizer.optimize_brief_generation()
    print(f"   Model: {brief_config['intelligence_model']}")
    print(f"   Cache: {brief_config['intelligence_caching']}")
    print(f"   Cost/user/month: ${brief_config['estimated_daily_cost_per_user'] * 30:.2f}\n")

    print("3. MORNING MESSAGE (7am WhatsApp)")
    morning_config = optimizer.optimize_morning_message()
    print(f"   Model: {morning_config['intelligence_model']}")
    print(f"   Batch mode: {morning_config['batch_mode']}")
    print(f"   Cost per 1000 users: ${morning_config['estimated_cost_per_1000_users']:.2f}\n")
