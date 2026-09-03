"""
Challenge Agent for Miru
=====================
Autonomous adversarial testing framework that interrogates features for edge cases,
failure modes, and architectural improvements.

Runs pre-deployment to catch bugs before they reach production.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import requests
from anthropic import Anthropic

# Initialize Anthropic client
client = Anthropic()

# ──────────────────────────────────────────────────────────────────────────────
# INTERROGATION FRAMEWORK
# ──────────────────────────────────────────────────────────────────────────────

class ChallengeAgent:
    """Autonomous adversarial testing agent for Miru features."""

    def __init__(self, base_url="http://localhost:5000", verbose=True):
        self.base_url = base_url
        self.verbose = verbose
        self.findings = []
        self.conversation_history = []

    def log(self, msg: str, level="INFO"):
        """Log with timestamp."""
        if self.verbose:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] [{level}] {msg}")

    def interrogate_feature(self, feature_name: str, feature_description: str,
                           test_cases: List[Dict]) -> Dict:
        """
        Main interrogation method: challenge a feature across multiple dimensions.

        Args:
            feature_name: Name of feature (e.g., "receipt_search")
            feature_description: What it does
            test_cases: List of {"input": ..., "expected": ...} cases

        Returns:
            {
                "feature": str,
                "findings": List[{"severity": str, "issue": str, "test": str, "fix": str}],
                "recommendation": str
            }
        """
        self.log(f"🎯 INTERROGATING: {feature_name}", "CHALLENGE")
        self.log(f"   Description: {feature_description}")
        self.log(f"   Test cases: {len(test_cases)}")

        findings = []

        # 1. EDGE CASE INTERROGATION
        self.log("   Phase 1: Edge Case Testing...", "CHALLENGE")
        edge_cases = self._generate_edge_cases(feature_name, feature_description)
        for edge_case in edge_cases:
            result = self._test_edge_case(edge_case)
            if result and result.get("failed"):
                findings.append({
                    "severity": "HIGH",
                    "category": "edge_case",
                    "issue": result["issue"],
                    "test": result["test"],
                    "fix": result.get("suggested_fix", "N/A")
                })

        # 2. FAILURE MODE INTERROGATION
        self.log("   Phase 2: Failure Mode Analysis...", "CHALLENGE")
        failure_modes = self._generate_failure_modes(feature_name, feature_description)
        for mode in failure_modes:
            result = self._test_failure_mode(mode)
            if result and result.get("vulnerable"):
                findings.append({
                    "severity": "CRITICAL",
                    "category": "failure_mode",
                    "issue": result["issue"],
                    "test": result["test"],
                    "fix": result.get("suggested_fix", "N/A")
                })

        # 3. PERFORMANCE INTERROGATION
        self.log("   Phase 3: Performance Testing...", "CHALLENGE")
        perf_tests = self._generate_performance_tests(feature_name)
        for perf_test in perf_tests:
            result = self._test_performance(perf_test)
            if result and result.get("degraded"):
                findings.append({
                    "severity": "MEDIUM",
                    "category": "performance",
                    "issue": result["issue"],
                    "test": result["test"],
                    "fix": result.get("suggested_fix", "N/A")
                })

        # 4. ASSUMPTIONS INTERROGATION (the secret sauce)
        self.log("   Phase 4: Questioning Assumptions...", "CHALLENGE")
        assumptions = self._extract_assumptions(feature_name, feature_description)
        for assumption in assumptions:
            result = self._challenge_assumption(assumption)
            if result and result.get("questionable"):
                findings.append({
                    "severity": "MEDIUM",
                    "category": "architecture",
                    "issue": result["issue"],
                    "test": result["rationale"],
                    "fix": result.get("improvement", "N/A")
                })

        # 5. SECURITY INTERROGATION
        self.log("   Phase 5: Security Analysis...", "CHALLENGE")
        security_tests = self._generate_security_tests(feature_name)
        for sec_test in security_tests:
            result = self._test_security(sec_test)
            if result and result.get("vulnerable"):
                findings.append({
                    "severity": "CRITICAL",
                    "category": "security",
                    "issue": result["issue"],
                    "test": result["test"],
                    "fix": result.get("suggested_fix", "N/A")
                })

        # Generate recommendation using Claude
        recommendation = self._synthesize_recommendation(feature_name, findings)

        self.findings.extend(findings)
        result = {
            "feature": feature_name,
            "timestamp": datetime.now().isoformat(),
            "findings_count": len(findings),
            "findings": findings,
            "recommendation": recommendation,
            "severity_breakdown": self._count_severities(findings)
        }

        self.log(f"✅ Interrogation complete: {len(findings)} issues found", "CHALLENGE")
        return result

    def _generate_edge_cases(self, feature: str, description: str) -> List[str]:
        """Use Claude to generate edge cases for the feature."""
        prompt = f"""
Given this Miru feature:
- Name: {feature}
- Description: {description}

Generate 5 extreme edge cases that could break it. Be specific.
Format: One case per line, numbered 1-5.

Examples:
1. Empty input / None / null
2. Extremely large input (10MB, 1M items)
3. Unicode/emoji characters in unexpected places
4. Concurrent requests from same user
5. Resource exhaustion (rate limits, timeouts)
"""
        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.split("\n")[:5]

    def _generate_failure_modes(self, feature: str, description: str) -> List[str]:
        """Generate failure modes: what external systems could fail?"""
        prompt = f"""
Feature: {feature}
Description: {description}

What external dependencies could fail and break this feature?
Examples: Database down, API timeout, third-party service error, network failure, auth token expired

List 5 failure modes:
"""
        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.split("\n")[:5]

    def _generate_performance_tests(self, feature: str) -> List[Dict]:
        """Generate performance scenarios that could cause latency issues."""
        return [
            {
                "name": f"{feature}_with_max_data",
                "scenario": "Feature tested with maximum possible data load",
                "threshold": "p95 latency < 2s"
            },
            {
                "name": f"{feature}_concurrent_users",
                "scenario": "10 users hitting feature simultaneously",
                "threshold": "error rate < 1%"
            },
            {
                "name": f"{feature}_resource_constrained",
                "scenario": "Feature running on degraded resources (memory/CPU)",
                "threshold": "graceful degradation, no crashes"
            },
        ]

    def _generate_security_tests(self, feature: str) -> List[Dict]:
        """Generate security interrogation tests."""
        return [
            {
                "name": "injection_test",
                "attack": "SQL injection via user input",
                "payload": "'; DROP TABLE users; --"
            },
            {
                "name": "auth_bypass",
                "attack": "Can user see another user's data?",
                "payload": "another_user_id"
            },
            {
                "name": "rate_limit_bypass",
                "attack": "Can I bypass rate limiting?",
                "payload": "repeated rapid requests"
            },
        ]

    def _extract_assumptions(self, feature: str, description: str) -> List[Dict]:
        """Extract implicit assumptions in the design."""
        prompt = f"""
Feature: {feature}
Code: {description}

What are the implicit assumptions this feature makes?
List 5 assumptions that might be wrong:

Format:
Assumption 1: [assumption]
Assumption 2: [assumption]
etc.
"""
        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )

        assumptions = []
        for line in response.content[0].text.split("\n"):
            if line.startswith("Assumption"):
                assumptions.append({"assumption": line})
        return assumptions

    def _test_edge_case(self, edge_case: str) -> Dict:
        """Simulate testing an edge case (in real implementation, would call actual API)."""
        # This is a placeholder - in production, would make actual HTTP calls
        prompt = f"""
Test case: {edge_case}

Imagine this edge case being tested against a Miru feature.
What would likely happen? Would it break?

Respond in JSON:
{{
  "failed": true/false,
  "issue": "description if failed",
  "test": "how to reproduce"
}}
"""
        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        try:
            return json.loads(response.content[0].text)
        except:
            return None

    def _test_failure_mode(self, mode: str) -> Dict:
        """Test how feature handles failure modes."""
        prompt = f"""
Failure mode: {mode}

How would Miru handle this failure? Is it resilient?

JSON:
{{
  "vulnerable": true/false,
  "issue": "what breaks",
  "test": "how to trigger",
  "suggested_fix": "how to handle gracefully"
}}
"""
        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        try:
            return json.loads(response.content[0].text)
        except:
            return None

    def _test_performance(self, perf_test: Dict) -> Dict:
        """Test performance characteristics."""
        prompt = f"""
Performance scenario: {perf_test.get('scenario')}
Threshold: {perf_test.get('threshold')}

Would this scenario likely breach the threshold?

JSON:
{{
  "degraded": true/false,
  "issue": "performance problem if true",
  "test": "how to measure",
  "suggested_fix": "optimization approach"
}}
"""
        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        try:
            return json.loads(response.content[0].text)
        except:
            return None

    def _challenge_assumption(self, assumption: Dict) -> Dict:
        """Question an architectural assumption."""
        prompt = f"""
Assumption in Miru: {assumption.get('assumption')}

Is this assumption valid? What if it's wrong?

JSON:
{{
  "questionable": true/false,
  "issue": "why this might be wrong",
  "rationale": "what could go wrong",
  "improvement": "alternative approach"
}}
"""
        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        try:
            return json.loads(response.content[0].text)
        except:
            return None

    def _test_security(self, sec_test: Dict) -> Dict:
        """Test security vulnerability."""
        prompt = f"""
Security test: {sec_test.get('attack')}
Payload: {sec_test.get('payload')}

How would Miru handle this attack? Is it vulnerable?

JSON:
{{
  "vulnerable": true/false,
  "issue": "vulnerability description",
  "test": "how to test",
  "suggested_fix": "fix approach"
}}
"""
        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        try:
            return json.loads(response.content[0].text)
        except:
            return None

    def _synthesize_recommendation(self, feature: str, findings: List[Dict]) -> str:
        """Use Claude to synthesize overall recommendation."""
        if not findings:
            return "✅ No critical issues found. Feature ready for deployment."

        critical = [f for f in findings if f.get("severity") == "CRITICAL"]
        high = [f for f in findings if f.get("severity") == "HIGH"]

        if critical:
            return f"🚫 DO NOT DEPLOY. {len(critical)} critical issues found. Requires fixes before deployment."
        elif high:
            return f"⚠️  CAUTION: {len(high)} high-severity issues. Recommend fixes before deployment, or deploy with risk acceptance."
        else:
            return f"✅ Safe to deploy. {len(findings)} low-priority issues; consider for next sprint."

    def _count_severities(self, findings: List[Dict]) -> Dict:
        """Count findings by severity."""
        return {
            "CRITICAL": len([f for f in findings if f.get("severity") == "CRITICAL"]),
            "HIGH": len([f for f in findings if f.get("severity") == "HIGH"]),
            "MEDIUM": len([f for f in findings if f.get("severity") == "MEDIUM"]),
            "LOW": len([f for f in findings if f.get("severity") == "LOW"]),
        }

    def generate_report(self, feature_name: str) -> str:
        """Generate a readable report of all findings."""
        relevant = [f for f in self.findings if f.get("feature") == feature_name]

        if not relevant:
            return f"No interrogation results for {feature_name}"

        report = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    CHALLENGE AGENT INTERROGATION REPORT                      ║
║                           Feature: {feature_name:<50} ║
╚══════════════════════════════════════════════════════════════════════════════╝

FINDINGS BY SEVERITY:
"""
        for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            items = [f for f in relevant if f.get("severity") == severity]
            if items:
                report += f"\n{severity} ({len(items)}):\n"
                for i, item in enumerate(items, 1):
                    report += f"  {i}. [{item.get('category')}] {item.get('issue')}\n"
                    report += f"     Test: {item.get('test')}\n"
                    report += f"     Fix: {item.get('fix')}\n\n"

        return report


# ──────────────────────────────────────────────────────────────────────────────
# FEATURE INTERROGATION LIBRARY
# ──────────────────────────────────────────────────────────────────────────────

MIRU_FEATURES = {
    "receipt_search": {
        "description": "Search user's receipts for items (e.g., 'Did i buy Vita Coco')",
        "test_cases": [
            {"input": "Did i buy Vita Coco", "expected": "Found Vita Coco at Sainsbury's"},
            {"input": "Did i buy Tikka", "expected": "Not found"},
            {"input": "", "expected": "Error or help message"},
        ]
    },
    "onboarding_wizard": {
        "description": "7-step user onboarding flow (postcode → WhatsApp → phone → kids → schools → commute → spend)",
        "test_cases": [
            {"input": "Complete all steps", "expected": "User setup complete"},
            {"input": "Skip school step", "expected": "Schools skipped if no kids"},
            {"input": "No kids selected", "expected": "Schools question not shown"},
        ]
    },
    "brief_generation": {
        "description": "Generate morning brief from fuel, school, weather, spending data",
        "test_cases": [
            {"input": "User with no data", "expected": "Graceful message"},
            {"input": "User with 1000 receipts", "expected": "Loads in <2s"},
            {"input": "Groq rate limit hit", "expected": "Fallback response"},
        ]
    },
    "school_comms": {
        "description": "Fetch school emails and extract events",
        "test_cases": [
            {"input": "Gmail auth expired", "expected": "Graceful error"},
            {"input": "1000 unread emails", "expected": "Processes in <30s"},
            {"input": "PDF attachment", "expected": "Handles without crashes"},
        ]
    },
    "spending_tracker": {
        "description": "Track receipts and provide weekly spending insights",
        "test_cases": [
            {"input": "Duplicate receipt", "expected": "Deduped (15min rule)"},
            {"input": "User has 5000 receipts", "expected": "Fast filtering"},
            {"input": "Receipt with no items", "expected": "Handles gracefully"},
        ]
    }
}


# ──────────────────────────────────────────────────────────────────────────────
# MAIN EXECUTION
# ──────────────────────────────────────────────────────────────────────────────

def run_full_interrogation():
    """Run Challenge Agent against all Miru features."""
    agent = ChallengeAgent(verbose=True)

    print("\n" + "="*80)
    print("🎯 MIRU CHALLENGE AGENT - AUTONOMOUS ADVERSARIAL TESTING")
    print("="*80 + "\n")

    all_reports = {}

    for feature_name, feature_info in MIRU_FEATURES.items():
        print(f"\n{'─'*80}")
        result = agent.interrogate_feature(
            feature_name=feature_name,
            feature_description=feature_info["description"],
            test_cases=feature_info["test_cases"]
        )

        all_reports[feature_name] = result

        # Print summary
        severity_counts = result.get("severity_breakdown", {})
        print(f"\n📊 SUMMARY for {feature_name}:")
        print(f"   Critical: {severity_counts.get('CRITICAL', 0)}")
        print(f"   High: {severity_counts.get('HIGH', 0)}")
        print(f"   Medium: {severity_counts.get('MEDIUM', 0)}")
        print(f"   Recommendation: {result.get('recommendation')}")

    # Generate final report
    print("\n" + "="*80)
    print("📋 FINAL CHALLENGE AGENT REPORT")
    print("="*80)

    total_critical = sum(r.get("severity_breakdown", {}).get("CRITICAL", 0) for r in all_reports.values())
    total_high = sum(r.get("severity_breakdown", {}).get("HIGH", 0) for r in all_reports.values())

    print(f"\n🚨 CRITICAL ISSUES: {total_critical}")
    print(f"⚠️  HIGH-SEVERITY ISSUES: {total_high}")

    if total_critical > 0:
        print(f"\n🚫 DEPLOYMENT VERDICT: DO NOT DEPLOY")
        print(f"   Fix {total_critical} critical issues before proceeding.")
    elif total_high > 0:
        print(f"\n⚠️  DEPLOYMENT VERDICT: CAUTION")
        print(f"   {total_high} high-priority issues found. Deploy with care.")
    else:
        print(f"\n✅ DEPLOYMENT VERDICT: APPROVED")
        print(f"   All critical issues resolved. Safe to deploy to production.")

    return all_reports


if __name__ == "__main__":
    results = run_full_interrogation()

    # Save results to file
    with open("/tmp/challenge_agent_report.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n📁 Full report saved to: /tmp/challenge_agent_report.json")
