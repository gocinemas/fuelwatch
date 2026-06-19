"""
Smart AI Router - Hybrid Together AI + Groq + Anthropic

Routes tasks to the optimal AI provider based on:
- Task type (NLP vs synthesis vs creative)
- Provider rate limits & availability
- Cost optimization (Together AI cheapest → Groq → Anthropic)
- Response time requirements

Together AI: Cheapest ($0.50/1M tokens), good for synthesis
Groq: Free tier but quota limits, good for real-time
Anthropic: Best quality, use for complex analysis only
"""

import os
from enum import Enum
from datetime import datetime

# Together AI setup (PRIMARY - cheapest)
TOGETHER_API_KEY = os.getenv('TOGETHER_API_KEY')
together_client = None
try:
    import together
    if TOGETHER_API_KEY:
        together_client = together.Together(api_key=TOGETHER_API_KEY)
        print("[router] Together AI client initialized")
except:
    print("[router] Together AI initialization failed")

# Groq setup (SECONDARY - free tier)
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
groq_client = None
try:
    import groq as groq_module
    if GROQ_API_KEY:
        groq_client = groq_module.Groq(api_key=GROQ_API_KEY)
        print("[router] Groq client initialized")
except:
    print("[router] Groq initialization failed")

# Anthropic setup (TERTIARY - best quality, reserve for complex tasks)
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
anthropic_client = None
try:
    import anthropic as anthropic_module
    if ANTHROPIC_API_KEY:
        anthropic_client = anthropic_module.Anthropic(api_key=ANTHROPIC_API_KEY)
        print("[router] Anthropic client initialized")
except:
    print("[router] Anthropic initialization failed")


class TaskType(Enum):
    """Task categories for intelligent routing"""
    SYNTHESIS = "synthesis"  # Quick data synthesis → Groq
    NLP = "nlp"  # Natural language processing → Anthropic
    ANALYSIS = "analysis"  # Complex analysis → Anthropic
    CREATIVE = "creative"  # Creative writing → Anthropic
    EXTRACTION = "extraction"  # Data extraction → Anthropic
    FAST = "fast"  # Real-time, low latency → Groq


class AIRouter:
    """Route AI tasks to optimal provider"""

    def __init__(self):
        self.together_rate_limited = False
        self.groq_rate_limited = False
        self.anthropic_rate_limited = False
        self.usage_log = []

    def route(self, task_type: TaskType, prompt: str, max_tokens: int = 500) -> dict:
        """
        Route task to optimal provider (cost-optimized).

        Priority: Together AI (cheapest) → Groq (free tier) → Anthropic (best quality)

        Returns: {
            "response": "...",
            "provider": "together" | "groq" | "anthropic",
            "tokens_used": 150,
            "cost": 0.0005,
            "latency_ms": 234
        }
        """
        import time
        start_time = time.time()

        # Routing logic - try Together AI first (cheapest), fallback to Groq, then Anthropic
        if task_type == TaskType.SYNTHESIS:
            result = self._use_together(prompt, max_tokens) or self._use_groq(prompt, max_tokens)
        elif task_type == TaskType.NLP:
            result = self._use_together(prompt, max_tokens) or self._use_anthropic(prompt, max_tokens)
        elif task_type == TaskType.ANALYSIS:
            result = self._use_anthropic(prompt, max_tokens) or self._use_together(prompt, max_tokens)
        elif task_type == TaskType.EXTRACTION:
            result = self._use_anthropic(prompt, max_tokens) or self._use_together(prompt, max_tokens)
        elif task_type == TaskType.FAST:
            result = self._use_together(prompt, max_tokens) or self._use_groq(prompt, max_tokens)
        else:
            # Fallback: try all in order
            result = self._use_together(prompt, max_tokens) or self._use_groq(prompt, max_tokens) or self._use_anthropic(prompt, max_tokens)

        latency = (time.time() - start_time) * 1000

        if result:
            result["latency_ms"] = latency
            self.usage_log.append(result)
            print(f"[router] {result['provider'].upper()}: {result['tokens_used']} tokens, ${result['cost']:.4f}")
            return result

        return {"error": "All providers unavailable", "provider": "none"}

    def _use_together(self, prompt: str, max_tokens: int) -> dict:
        """Try Together AI first (cheapest)"""
        if not together_client or self.together_rate_limited:
            return None

        try:
            response = together_client.chat.completions.create(
                model="meta-llama/Llama-3.3-70B-Instruct-Turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=0.3,
                timeout=10
            )

            text = response.choices[0].message.content.strip()
            tokens = response.usage.total_tokens if hasattr(response, 'usage') else len(prompt.split()) + max_tokens
            # Together AI: ~$0.50/1M tokens for 70B model
            cost = (tokens / 1000000) * 0.0005

            return {
                "response": text,
                "provider": "together",
                "tokens_used": tokens,
                "cost": cost
            }

        except Exception as e:
            error_msg = str(e)
            if "rate_limit" in error_msg.lower() or "quota" in error_msg.lower():
                self.together_rate_limited = True
                print(f"[router] Together AI rate limited: {e}")
            else:
                print(f"[router] Together AI error: {e}")
            return None

    def _use_groq(self, prompt: str, max_tokens: int) -> dict:
        """Try Groq second (free tier, quota limits)"""
        if not groq_client or self.groq_rate_limited:
            return None

        try:
            response = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=max_tokens,
                timeout=5
            )

            text = response.choices[0].message.content.strip()
            tokens = response.usage.total_tokens
            # Groq free tier is effectively cheap
            cost = (tokens / 1000000) * 0.0001

            return {
                "response": text,
                "provider": "groq",
                "tokens_used": tokens,
                "cost": cost
            }

        except Exception as e:
            error_msg = str(e)
            if "rate_limit" in error_msg.lower():
                self.groq_rate_limited = True
                print(f"[router] Groq rate limited, falling back to Anthropic")
            return None

    def _use_anthropic(self, prompt: str, max_tokens: int) -> dict:
        """Fallback to Anthropic (better quality)"""
        if not anthropic_client or self.anthropic_rate_limited:
            return None

        try:
            response = anthropic_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )

            text = response.content[0].text.strip()
            tokens = response.usage.input_tokens + response.usage.output_tokens
            # Claude 3.5 Sonnet: $3/$15 per 1M tokens
            cost = (response.usage.input_tokens / 1000000) * 0.003 + (response.usage.output_tokens / 1000000) * 0.015

            return {
                "response": text,
                "provider": "anthropic",
                "tokens_used": tokens,
                "cost": cost
            }

        except Exception as e:
            error_msg = str(e)
            if "rate_limit" in error_msg.lower():
                self.anthropic_rate_limited = True
                print(f"[router] Anthropic rate limited")
            return None

    def reset_rate_limits(self):
        """Reset rate limit flags (after rate limit window expires)"""
        self.together_rate_limited = False
        self.groq_rate_limited = False
        self.anthropic_rate_limited = False
        print("[router] Rate limit flags reset")

    def get_usage_summary(self):
        """Get usage statistics"""
        if not self.usage_log:
            return {"total_calls": 0, "total_cost": 0}

        total_cost = sum(log.get("cost", 0) for log in self.usage_log)
        together_calls = len([l for l in self.usage_log if l.get("provider") == "together"])
        groq_calls = len([l for l in self.usage_log if l.get("provider") == "groq"])
        anthropic_calls = len([l for l in self.usage_log if l.get("provider") == "anthropic"])

        return {
            "total_calls": len(self.usage_log),
            "together_calls": together_calls,
            "groq_calls": groq_calls,
            "anthropic_calls": anthropic_calls,
            "total_cost": total_cost,
            "avg_cost_per_call": total_cost / len(self.usage_log) if self.usage_log else 0
        }


# Global router instance
router = AIRouter()

if __name__ == "__main__":
    # Test
    test_prompt = "Score this brand's health 0-100 based on growth, profitability, market position, innovation, attractiveness. Brand: Test Corp. Respond ONLY with a number."

    # Test synthesis (should use Groq)
    print("\n=== Testing SYNTHESIS (Groq) ===")
    result = router.route(TaskType.SYNTHESIS, test_prompt, max_tokens=50)
    print(f"Response: {result.get('response')[:100]}")

    # Test NLP (should use Anthropic)
    print("\n=== Testing NLP (Anthropic) ===")
    nlp_prompt = "Extract the top 3 risks from this brand description: 'American skin care brand owned by P&G with mature market position.'"
    result = router.route(TaskType.NLP, nlp_prompt, max_tokens=200)
    print(f"Response: {result.get('response')[:100]}")

    # Show usage
    print("\n=== Usage Summary ===")
    print(router.get_usage_summary())
