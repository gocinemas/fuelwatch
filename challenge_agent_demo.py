"""
Challenge Agent - DEMO VERSION
Shows architecture and findings without needing API keys
"""

import json
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# DEMO FINDINGS - What Challenge Agent Would Find
# ──────────────────────────────────────────────────────────────────────────────

INTERROGATION_RESULTS = {
    "receipt_search": {
        "feature": "receipt_search",
        "timestamp": datetime.now().isoformat(),
        "findings": [
            {
                "severity": "CRITICAL",
                "category": "correctness",
                "issue": "Algolia fallback still matching loose 'Tikka' to 'Southern Fried Chicken'",
                "test": "Query 'Did i buy Tikka' returns Sainsbury's with no Tikka items",
                "fix": "Remove Algolia fallback for item searches - must have strict word match"
            },
            {
                "severity": "HIGH",
                "category": "edge_case",
                "issue": "Searches only 200 receipts - misses older purchases",
                "test": "User with 500+ receipts can't find items from 6+ months ago",
                "fix": "Increase receipt search limit to 500, or add date filter parameter"
            },
            {
                "severity": "HIGH",
                "category": "performance",
                "issue": "Receipt search doesn't use indexing - O(n) scan of 200 items",
                "test": "User with 500 receipts × 20 items = 10k item comparisons per query",
                "fix": "Add database index on receipt items, use SQL ILIKE for faster matching"
            },
            {
                "severity": "MEDIUM",
                "category": "architecture",
                "issue": "Why split search between wa_saves AND receipts table?",
                "test": "User's clippings and structured receipts are duplicate data sources",
                "fix": "Consolidate: all receipt data should go into ONE receipts table with consistent schema"
            },
            {
                "severity": "MEDIUM",
                "category": "edge_case",
                "issue": "No handling for special characters - emoji, accents",
                "test": "Query 'Did i buy café' might fail if DB has 'cafe' (no accent)",
                "fix": "Normalize strings: lowercase + remove accents before comparison"
            }
        ],
        "recommendation": "🚫 DO NOT DEPLOY. Critical correctness bug: Algolia fallback still active. Feature is unreliable.",
        "severity_breakdown": {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 2, "LOW": 0}
    },

    "onboarding_wizard": {
        "feature": "onboarding_wizard",
        "timestamp": datetime.now().isoformat(),
        "findings": [
            {
                "severity": "CRITICAL",
                "category": "routing",
                "issue": "Onboarding route still caught by catch-all /<company_slug> route",
                "test": "Visit /onboarding → shows Intel brand search instead of wizard",
                "fix": "Move @app.route('/onboarding') to END of sms_service.py (after catch-all)"
            },
            {
                "severity": "HIGH",
                "category": "edge_case",
                "issue": "No validation on phone number format",
                "test": "User enters 'abc123' → accepted, but WhatsApp won't work",
                "fix": "Validate UK +44 format OR international E.164 standard"
            },
            {
                "severity": "MEDIUM",
                "category": "architecture",
                "issue": "WhatsApp group link is hardcoded placeholder",
                "test": "Success screen shows 'https://chat.whatsapp.com/YOUR_GROUP_LINK'",
                "fix": "Store group link in database/config, or generate dynamically"
            },
            {
                "severity": "MEDIUM",
                "category": "ux",
                "issue": "No progress indication on long form steps",
                "test": "User can't tell how many steps left without counting",
                "fix": "Progress bar already exists - good! But add 'Step N of 7' to each page"
            }
        ],
        "recommendation": "🚫 DO NOT DEPLOY. Critical routing bug blocks page entirely. Fix urgently.",
        "severity_breakdown": {"CRITICAL": 1, "HIGH": 1, "MEDIUM": 2, "LOW": 0}
    },

    "brief_generation": {
        "feature": "brief_generation",
        "timestamp": datetime.now().isoformat(),
        "findings": [
            {
                "severity": "HIGH",
                "category": "performance",
                "issue": "Groq rate limit hits during parallel brief requests",
                "test": "5 users hitting brief API simultaneously → 429 errors for 3 of them",
                "fix": "Implement token bucket rate limiter, or batch Groq calls"
            },
            {
                "severity": "HIGH",
                "category": "failure_mode",
                "issue": "No fallback when Groq fails - brief doesn't render",
                "test": "Groq down → user sees blank brief card",
                "fix": "Cache previous brief, or generate lightweight fallback (without Groq)"
            },
            {
                "severity": "MEDIUM",
                "category": "architecture",
                "issue": "Brief depends on 20+ parallel API calls - any one failure causes timeout",
                "test": "TrainAPI slow → whole brief takes 5+ seconds",
                "fix": "Timeout each sub-call at 1s, show 'data not available' if missing"
            },
            {
                "severity": "MEDIUM",
                "category": "correctness",
                "issue": "Brief ignores user's time-of-day rules (9pm no 'go out' suggestions)",
                "test": "9:30pm brief suggests 'Check nearby restaurants'",
                "fix": "Check hour in brief generation, apply STEERING_LAYER rules per MIRU_STEERING.md"
            }
        ],
        "recommendation": "⚠️  CAUTION. High-severity issues but feature is functional. Recommend fixing Groq rate limiting before wider rollout.",
        "severity_breakdown": {"CRITICAL": 0, "HIGH": 2, "MEDIUM": 2, "LOW": 0}
    },

    "school_comms": {
        "feature": "school_comms",
        "timestamp": datetime.now().isoformat(),
        "findings": [
            {
                "severity": "HIGH",
                "category": "correctness",
                "issue": "Gmail auth expiration not handled - silently fails",
                "test": "Gmail token expires → emails stop fetching, no error to user",
                "fix": "Check token refresh response, set error flag, notify user to re-auth"
            },
            {
                "severity": "HIGH",
                "category": "performance",
                "issue": "Groq parsing ALL emails instead of filtering first",
                "test": "School sends 1000 emails/week → 30k TPM rate limit hit daily",
                "fix": "Use smart email filter (_should_parse_email) BEFORE sending to Groq"
            },
            {
                "severity": "MEDIUM",
                "category": "edge_case",
                "issue": "PDF attachments extracted as text - causes Groq 413 errors",
                "test": "Email with 5MB PDF attachment → Groq request too large",
                "fix": "Truncate extracted PDF text to 2000 chars (already done, verify)"
            }
        ],
        "recommendation": "⚠️  CAUTION. Email filtering helps but auth error handling critical. Fix before production.",
        "severity_breakdown": {"CRITICAL": 0, "HIGH": 2, "MEDIUM": 1, "LOW": 0}
    },

    "spending_tracker": {
        "feature": "spending_tracker",
        "timestamp": datetime.now().isoformat(),
        "findings": [
            {
                "severity": "HIGH",
                "category": "correctness",
                "issue": "Dedup rule (15min, same location) has edge case with multiple payment types",
                "test": "User pays at Tesco twice with different cards in 10min → might not dedup",
                "fix": "Dedup should match: merchant + amount + timestamp, ignoring payment method"
            },
            {
                "severity": "MEDIUM",
                "category": "architecture",
                "issue": "Spend toggle in settings is broken (per user feedback)",
                "test": "User enables Spend in settings → doesn't appear on homepage",
                "fix": "Verify spending toggle wires to modules_enabled correctly"
            },
            {
                "severity": "MEDIUM",
                "category": "performance",
                "issue": "'View →' link on spending tip navigates to spend with date filter",
                "test": "User clicks tip link → page reloads to apply filter (jarring UX)",
                "fix": "Instead of reload, use client-side filtering (AJAX)"
            }
        ],
        "recommendation": "✅ Safe to deploy with caveats. Dedup edge case is low-probability. Monitor user feedback.",
        "severity_breakdown": {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 0}
    }
}


def print_report():
    """Print formatted interrogation report."""
    print("\n" + "="*90)
    print("🎯 MIRU CHALLENGE AGENT - INTERROGATION FINDINGS")
    print("="*90 + "\n")

    total_critical = 0
    total_high = 0
    total_medium = 0

    for feature, results in INTERROGATION_RESULTS.items():
        severity = results["severity_breakdown"]
        total_critical += severity.get("CRITICAL", 0)
        total_high += severity.get("HIGH", 0)
        total_medium += severity.get("MEDIUM", 0)

        print(f"\n{'─'*90}")
        print(f"📋 FEATURE: {feature.upper()}")
        print(f"{'─'*90}")

        findings = results.get("findings", [])
        if not findings:
            print("✅ No issues found")
            continue

        for i, finding in enumerate(findings, 1):
            severity_icon = {
                "CRITICAL": "🚨",
                "HIGH": "⚠️ ",
                "MEDIUM": "💡",
                "LOW": "ℹ️ "
            }.get(finding["severity"], "❓")

            print(f"\n{severity_icon} [{finding['severity']}] {finding['category'].upper()}")
            print(f"   Issue: {finding['issue']}")
            print(f"   Test:  {finding['test']}")
            print(f"   Fix:   {finding['fix']}")

        print(f"\n   📊 Severity: {severity}")
        print(f"   ✏️  Recommendation: {results['recommendation']}")

    # FINAL DEPLOYMENT VERDICT
    print("\n" + "="*90)
    print("🚀 DEPLOYMENT VERDICT")
    print("="*90 + "\n")

    print(f"📊 TOTAL ISSUES FOUND:")
    print(f"   🚨 Critical: {total_critical}")
    print(f"   ⚠️  High:     {total_high}")
    print(f"   💡 Medium:   {total_medium}")

    if total_critical >= 2:
        verdict = "🚫 HOLD - DO NOT DEPLOY"
        reason = f"{total_critical} critical bugs blocking production. Fix required."
    elif total_critical == 1:
        verdict = "⛔ CAUTION - DEPLOY WITH RISK"
        reason = f"1 critical issue (onboarding routing). High risk of user impact."
    elif total_high >= 3:
        verdict = "⚠️  REVIEW - CONDITIONAL DEPLOY"
        reason = f"{total_high} high-severity issues. Deploy only if mitigated or accepted."
    elif total_high > 0:
        verdict = "✅ APPROVED - MONITOR CLOSELY"
        reason = f"{total_high} high-severity issues. Safe to deploy but monitor user feedback."
    else:
        verdict = "✅ APPROVED - CLEAR"
        reason = "All critical/high issues resolved."

    print(f"\n{verdict}")
    print(f"Reason: {reason}")

    print(f"\n📋 ACTION ITEMS (prioritized):")
    print(f"   1. URGENT: Fix onboarding route (critical routing bug)")
    print(f"   2. URGENT: Remove Algolia fallback from receipt search (correctness bug)")
    print(f"   3. HIGH: Implement phone number validation in onboarding")
    print(f"   4. HIGH: Add Gmail auth error handling in school_comms")
    print(f"   5. HIGH: Implement Groq rate limiting for brief generation")
    print(f"   6. MEDIUM: Consolidate receipt data sources (wa_saves + receipts)")
    print(f"   7. MEDIUM: Fix spending toggle wiring")

    print(f"\n" + "="*90 + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# INTEGRATION: Pre-Deployment Gate
# ──────────────────────────────────────────────────────────────────────────────

def check_deployment_gate(verdict_override=None):
    """
    This function would be called by CI/CD before deploy.
    Returns True if safe to deploy, False otherwise.
    """
    total_critical = sum(r["severity_breakdown"].get("CRITICAL", 0)
                        for r in INTERROGATION_RESULTS.values())

    # GATE 1: No critical issues
    if total_critical > 0:
        print(f"❌ DEPLOYMENT BLOCKED: {total_critical} critical issues found")
        return False

    # GATE 2: All high-severity issues triaged
    total_high = sum(r["severity_breakdown"].get("HIGH", 0)
                    for r in INTERROGATION_RESULTS.values())
    if total_high > 2 and not verdict_override:
        print(f"⚠️  DEPLOYMENT CAUTION: {total_high} high-severity issues")
        print(f"   Override with: check_deployment_gate(verdict_override=True)")
        return False

    print("✅ DEPLOYMENT GATE PASSED - Safe to deploy")
    return True


if __name__ == "__main__":
    # Print full report
    print_report()

    # Check deployment gate
    print("\n💳 CHECKING DEPLOYMENT GATE...")
    can_deploy = check_deployment_gate()

    if can_deploy:
        print("\n✅ You can deploy with: git push main")
    else:
        print("\n❌ Fix critical issues before deploying")
