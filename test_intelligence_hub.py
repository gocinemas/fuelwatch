#!/usr/bin/env python3
"""
Intelligence Hub Integration Test
Verifies all intelligence endpoints and agentic reasoning
"""

import requests
import json
import os
from datetime import datetime

BASE_URL = os.environ.get("TEST_BASE_URL", "http://localhost:8080")
TEST_PHONE = os.environ.get("TEST_PHONE", "whatsapp:447911123456")

def test_insights_full():
    """Test: Full intelligence engine across all modules."""
    print("\n🧠 TEST: Full Intelligence Report")
    print("─" * 50)

    try:
        r = requests.get(f"{BASE_URL}/api/insights/full?wa={TEST_PHONE}", timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            print(f"❌ Failed: {data.get('error')}")
            return False

        insights = data.get("insights", {})
        summary = data.get("data_summary", {})

        print(f"✅ Engine generated insights")
        print(f"   Timestamp: {data.get('timestamp')}")
        print(f"   Spend this week: £{summary.get('spend_total', 0):.2f}")
        print(f"   Last week: £{summary.get('last_week_spend', 0):.2f}")

        # Verify all insight dimensions
        dimensions = ["fuel", "spend", "location", "school", "lifestyle", "anomalies", "recommendations", "forecast"]
        for dim in dimensions:
            if dim in insights:
                print(f"   ✓ {dim.capitalize()}: {str(insights[dim])[:60]}...")
            else:
                print(f"   ⚠️  {dim.capitalize()}: missing")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_insights_week():
    """Test: Your Week enhanced with insights."""
    print("\n📊 TEST: Your Week with Intelligence")
    print("─" * 50)

    try:
        r = requests.get(f"{BASE_URL}/api/insights/week?wa={TEST_PHONE}", timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            print(f"❌ Failed: {data.get('error')}")
            return False

        week = data.get("week", {})
        insights = data.get("insights", {})

        print(f"✅ Your Week loaded with intelligence")
        print(f"   This week spend: £{week.get('this_week', {}).get('spend', 0):.2f}")

        if week.get("forecast"):
            print(f"   Forecast: {week['forecast'].get('next_week_spend', 0)}")

        if week.get("anomalies"):
            print(f"   Anomalies: {len(week['anomalies'])} detected")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_insights_receipts():
    """Test: Receipts with spend intelligence."""
    print("\n💳 TEST: Receipts with Spend Intelligence")
    print("─" * 50)

    try:
        r = requests.get(f"{BASE_URL}/api/insights/receipts?wa={TEST_PHONE}", timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            print(f"❌ Failed: {data.get('error')}")
            return False

        insights = data.get("insights", {})
        spend = insights.get("spend", {})
        location = insights.get("location", {})

        print(f"✅ Receipts enhanced with intelligence")
        print(f"   Spend trend: {spend.get('trend', '—').upper()}")
        print(f"   Forecast: £{spend.get('forecast_next_week', 0):.2f} next week")
        print(f"   Top location: {location.get('most_visited', '—')}")
        print(f"   Potential savings: {location.get('savings', '—')}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_insights_fuel():
    """Test: Fuel with price intelligence."""
    print("\n⛽ TEST: Fuel with Price Intelligence")
    print("─" * 50)

    try:
        r = requests.get(f"{BASE_URL}/api/insights/fuel?wa={TEST_PHONE}", timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            print(f"❌ Failed: {data.get('error')}")
            return False

        insights = data.get("insights", {})
        fuel = insights.get("fuel", {})

        print(f"✅ Fuel module enhanced with intelligence")
        print(f"   Price trend: {fuel.get('price_trend', '—').upper()} {fuel.get('percent_change', 0)}%")
        print(f"   Next refill: {fuel.get('next_fill_days', '?')} days")
        print(f"   Recommendation: {fuel.get('recommendation', '—')[:50]}...")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_notifications():
    """Test: Smart notifications."""
    print("\n🔔 TEST: Smart Notifications")
    print("─" * 50)

    try:
        r = requests.get(f"{BASE_URL}/api/insights/notifications?wa={TEST_PHONE}", timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            print(f"❌ Failed: {data.get('error')}")
            return False

        notifications = data.get("notifications", [])
        count = data.get("count", 0)

        print(f"✅ Generated {count} notifications")

        for notif in notifications[:3]:
            priority = notif.get("priority", "—")
            print(f"   [{priority.upper()}] {notif.get('title')}: {notif.get('message')[:40]}...")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_html_ui():
    """Test: Intelligence Hub UI loads."""
    print("\n🎨 TEST: Intelligence Hub UI")
    print("─" * 50)

    try:
        r = requests.get(BASE_URL, timeout=10)
        r.raise_for_status()
        html = r.text

        # Check for screen-intelligence
        if "screen-intelligence" not in html:
            print("❌ screen-intelligence div not found")
            return False

        # Check for sidebar button
        if "sb-intelligence" not in html:
            print("❌ 🧠 Intelligence button not in sidebar")
            return False

        # Check for key elements
        elements = [
            "intel-fuel-trend",
            "intel-spend-trend",
            "intel-lifestyle-change",
            "intel-rec-top",
            "intel-actions"
        ]

        missing = [e for e in elements if e not in html]

        if missing:
            print(f"❌ Missing elements: {', '.join(missing)}")
            return False

        print(f"✅ Intelligence Hub UI fully integrated")
        print(f"   ✓ Screen div found")
        print(f"   ✓ Sidebar button (🧠) present")
        print(f"   ✓ All {len(elements)} data elements found")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "="*50)
    print("🚀 INTELLIGENCE HUB INTEGRATION TEST SUITE")
    print("="*50)
    print(f"\nBase URL: {BASE_URL}")
    print(f"Test phone: {TEST_PHONE}\n")

    tests = [
        ("HTML UI", test_html_ui),
        ("Full Intelligence", test_insights_full),
        ("Your Week", test_insights_week),
        ("Receipts", test_insights_receipts),
        ("Fuel", test_insights_fuel),
        ("Notifications", test_notifications),
    ]

    results = {}
    for name, test_fn in tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            print(f"\n❌ CRASH in {name}: {e}")
            results[name] = False

    # Summary
    print("\n" + "="*50)
    print("📋 TEST RESULTS")
    print("="*50)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")

    print(f"\nTotal: {passed}/{total} passed")

    if passed == total:
        print("\n🎉 All tests passed! Intelligence Hub is fully operational.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check logs above.")
        return 1


if __name__ == "__main__":
    exit(main())
