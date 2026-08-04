"""
Test the new two-tab intelligence interface
"""

import sys
from intelligence_5signals import get_5_signals
from intelligence_tabbed_service import TabbedIntelligenceService


def test_signal_computation():
    """Test SIGNAL tab metric computation."""
    print("\n=== Testing SIGNAL Tab Computation ===")

    # Fetch sample data
    company_signals = get_5_signals("Reckitt")
    competitor_signals = get_5_signals("Henkel")

    print(f"\nReckitt signals: {company_signals.keys()}")
    print(f"Henkel signals: {competitor_signals.keys()}")

    # Build signal metrics
    company_metrics = TabbedIntelligenceService.build_signal_metrics(
        "Reckitt", company_signals
    )
    competitor_metrics = TabbedIntelligenceService.build_signal_metrics(
        "Henkel", competitor_signals
    )

    print(f"\nReckitt SIGNAL metrics:")
    print(f"  Market: {company_metrics['market_direction']}")
    print(f"  Risk: {company_metrics['risk_level']}")
    print(f"  Watch: {company_metrics['watch_item']}")

    print(f"\nHenkel SIGNAL metrics:")
    print(f"  Market: {competitor_metrics['market_direction']}")
    print(f"  Risk: {competitor_metrics['risk_level']}")
    print(f"  Watch: {competitor_metrics['watch_item']}")

    # Validate structure
    assert company_metrics["market_direction"] in ["up", "down", "flat"]
    assert company_metrics["risk_level"] in ["low", "medium", "high"]
    print("\n✓ SIGNAL metrics structure validated")


def test_intelligence_computation():
    """Test INTELLIGENCE tab data aggregation."""
    print("\n=== Testing INTELLIGENCE Tab Computation ===")

    company_signals = get_5_signals("Reckitt")

    # Build intelligence data
    intel_data = TabbedIntelligenceService.build_intelligence_data(
        "Reckitt", company_signals
    )

    print(f"\nReckitt INTELLIGENCE sections:")
    print(f"  Recent news articles: {len(intel_data.get('recent_news', []))}")
    print(f"  Sentiment scores: {intel_data.get('sentiment', {})}")
    print(f"  Hiring data: {intel_data.get('hiring', {})}")
    print(f"  Leadership count: {len(intel_data.get('leadership', []))}")

    # Validate structure
    assert isinstance(intel_data.get("recent_news", []), list)
    assert "trustpilot_score" in intel_data.get("sentiment", {})
    assert "hacker_news_score" in intel_data.get("sentiment", {})
    assert "total_roles" in intel_data.get("hiring", {})
    print("\n✓ INTELLIGENCE data structure validated")


def test_aggregate_both_tabs():
    """Test full aggregation for both tabs."""
    print("\n=== Testing Full Aggregation ===")

    company_signals = get_5_signals("Reckitt")
    competitor_signals = get_5_signals("Henkel")

    # Aggregate for both tabs
    tabbed_data = TabbedIntelligenceService.aggregate_for_both_tabs(
        "Reckitt", "Henkel", company_signals, competitor_signals
    )

    print(f"\nTabbed data keys: {tabbed_data.keys()}")
    print(f"Company: {tabbed_data['company']}")
    print(f"Competitor: {tabbed_data['competitor']}")
    print(f"Available competitors: {tabbed_data['available_competitors']}")

    # Validate structure
    assert tabbed_data["company"] == "Reckitt"
    assert tabbed_data["competitor"] == "Henkel"
    assert "Reckitt" in tabbed_data["signals"]
    assert "Henkel" in tabbed_data["signals"]
    assert "Reckitt" in tabbed_data["intelligence"]
    assert "Henkel" in tabbed_data["intelligence"]

    print("\n✓ Full aggregation validated")
    print(f"\nReady for template rendering:")
    print(f"  Template variables: {', '.join(tabbed_data.keys())}")


def test_competitor_mapping():
    """Test competitor selection logic."""
    print("\n=== Testing Competitor Mapping ===")

    competitors = TabbedIntelligenceService.get_available_competitors("Reckitt")
    print(f"\nReckitt competitors: {competitors}")

    assert len(competitors) > 0
    assert all(isinstance(c, str) for c in competitors)
    print("✓ Competitor mapping validated")


if __name__ == "__main__":
    try:
        test_signal_computation()
        test_intelligence_computation()
        test_aggregate_both_tabs()
        test_competitor_mapping()

        print("\n" + "="*50)
        print("✓ All tests passed!")
        print("="*50)
        print("\nImplementation ready for deployment:")
        print("  - Template: /templates/intelligence_tabbed.html")
        print("  - Service: intelligence_tabbed_service.py")
        print("  - Route: /intelligence/<company_name>")
        print("  - Query param: ?vs=<competitor>")

    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
