#!/usr/bin/env python3
"""
Manually trigger agent to test end-to-end
Helpful for debugging what's failing
"""

import os
import sys
from brand_research_agent import BrandResearchAgent

def test_single_brand():
    """Test agent with a single brand"""
    print("🔧 Debug Mode: Manual Agent Test\n")

    agent = BrandResearchAgent()

    print(f"Groq API Key: {'✅' if os.environ.get('GROQ_API_KEY') else '❌'}")
    print(f"Supabase URL: {'✅' if os.environ.get('SUPABASE_URL') else '❌'}")
    print(f"Supabase Key: {'✅' if os.environ.get('SUPABASE_KEY') else '❌'}\n")

    # Test with Nutella
    result = agent.process_request(
        brand_name="Nutella",
        category_hint="snacks",
        email="test@example.com"
    )

    print(f"\n✅ Result: {result}")

if __name__ == "__main__":
    test_single_brand()
