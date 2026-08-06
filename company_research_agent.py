#!/usr/bin/env python3
"""
Background research agent for company intelligence.
Automatically researches and populates database for any company.

Usage:
  python3 company_research_agent.py --companies "Reckitt,Henkel,Netflix"
  Or via API: GET /api/company/research/Reckitt
"""

import os
import sys
import json
import logging
from datetime import datetime
import argparse
import requests
from anthropic import Anthropic

from company_history_service import history_tracker, CompanyHistoryTracker
from company_intelligence_service import get_competitor_list, CompanyIntelligence

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CompanyResearchAgent:
    """AI-powered research agent for company intelligence."""

    def __init__(self):
        self.client = Anthropic()
        self.db = None
        self._init_db()

        # Initialize findings service
        from company_research_findings_service import research_findings_service
        research_findings_service.set_db(self.db)

    def _init_db(self):
        """Initialize Supabase connection."""
        try:
            from supabase import create_client
            self.db = create_client(
                os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
                os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
            )
            history_tracker.set_db(self.db)
        except Exception as e:
            logger.error(f"Failed to init DB: {e}")

    def research_company(self, company_name: str) -> dict:
        """
        Research a company using Claude and save findings for admin review.
        Hybrid workflow: auto-gather + human approval.

        Returns: {
            'company': company_name,
            'status': 'success' | 'partial' | 'failed',
            'findings_saved': bool
        }
        """
        logger.info(f"🔍 Starting research for {company_name}...")

        # Fetch basic company info
        intel = CompanyIntelligence(company_name)
        basics = intel.fetch_all()

        if not basics or 'error' in basics:
            logger.error(f"Failed to fetch basics for {company_name}")
            return {'company': company_name, 'status': 'failed', 'error': 'Could not fetch company basics'}

        logger.info(f"✓ Fetched basics for {company_name}")

        result = {
            'company': company_name,
            'status': 'partial',
            'findings_saved': False
        }

        # Gather auto data
        auto_data = {
            'description': basics.get('description'),
            'market_position': basics.get('sector'),
            'risks': [],
            'opportunities': [],
            'brands': basics.get('brands', []),
            'financials': {
                'stock_price': basics.get('stock', {}).get('price'),
                'market_cap': basics.get('stock', {}).get('market_cap'),
                'employees': basics.get('stock', {}).get('employees'),
            }
        }

        # Step 1: Research financials
        logger.info(f"  📊 Researching financials...")
        financials = self._research_financials(company_name, basics)
        if financials and len(financials) > 0:
            auto_data['financials'].update(financials[0])

        # Step 2: Research deals
        logger.info(f"  🤝 Researching deals...")
        deals = self._research_deals(company_name, basics)

        # Step 3: Research market trends
        logger.info(f"  📈 Researching market trends...")
        trends = self._research_market_trends(company_name, basics)

        # Save auto-gathered findings for admin review
        logger.info(f"  💾 Saving findings for admin review...")
        self._save_findings(company_name, auto_data)
        result['findings_saved'] = True
        result['status'] = 'success'

        logger.info(f"✅ Research complete for {company_name}: findings ready for admin review")
        return result

    def _research_financials(self, company_name: str, basics: dict) -> list:
        """Use Claude to research company financials from news/sources."""
        prompt = f"""Research recent financial data for {company_name}.

Company sector: {basics.get('sector', 'Unknown')}
Company description: {basics.get('description', 'Unknown')}

Find and extract:
1. Latest annual revenue (in millions)
2. Gross margin percentage
3. Operating margin percentage
4. Employee count
5. Year-over-year revenue growth %
6. Latest fiscal year (2025, 2024, etc.)

Format as JSON:
{{
  "period": "2025",
  "revenue_millions": 15600,
  "gross_margin_pct": 48.2,
  "operating_margin_pct": 21.8,
  "employees": 16200,
  "revenue_growth_pct": 6.5,
  "source": "research"
}}

If you can't find exact data, provide best estimates based on public sources.
Return ONLY valid JSON, no explanations."""

        try:
            response = self.client.messages.create(
                model="claude-opus-5",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text if response.content else ""

            # Try to extract JSON
            try:
                data = json.loads(text)
                return [data] if data else []
            except:
                logger.warning(f"Could not parse financials JSON for {company_name}")
                return []

        except Exception as e:
            logger.error(f"Financial research failed: {e}")
            return []

    def _research_deals(self, company_name: str, basics: dict) -> list:
        """Use Claude to research company M&A activity."""
        prompt = f"""Research recent M&A activity and investments for {company_name}.

Find:
1. Recent acquisitions (past 3 years)
2. Companies acquired by {company_name}
3. Investment rounds / funding
4. Divestitures
5. Strategic partnerships

For each deal, find:
- Target company name
- Deal type (acquisition, investment, divestiture)
- Amount in millions
- Announcement date (YYYY-MM-DD)
- Brief description

Format as JSON array:
[
  {{
    "deal_type": "acquisition",
    "target_company": "TargetName",
    "amount_millions": 500,
    "announcement_date": "2025-01-15",
    "description": "Strategic acquisition..."
  }}
]

Return ONLY valid JSON array, no explanations."""

        try:
            response = self.client.messages.create(
                model="claude-opus-5",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text if response.content else "[]"

            try:
                deals = json.loads(text)
                return deals if isinstance(deals, list) else []
            except:
                logger.warning(f"Could not parse deals JSON for {company_name}")
                return []

        except Exception as e:
            logger.error(f"Deal research failed: {e}")
            return []

    def _research_market_trends(self, company_name: str, basics: dict) -> list:
        """Use Claude to research market share and growth trends."""
        prompt = f"""Research market position and trends for {company_name}.

Sector: {basics.get('sector', 'Unknown')}

Find:
1. Market share percentages (in main categories/markets)
2. Category growth rates (market CAGR)
3. Regional/geographic growth rates
4. Competitive position vs main rivals
5. Product/brand specific metrics if CPG or consumer brand

Format as JSON array of trends:
[
  {{
    "category": "market_share",
    "metric_name": "disinfectant_market_share",
    "value_pct": 42,
    "period": "2025"
  }},
  {{
    "category": "category_growth",
    "metric_name": "disinfectant_market_growth",
    "value_pct": 4.2,
    "period": "2025"
  }}
]

Return ONLY valid JSON array, no explanations."""

        try:
            response = self.client.messages.create(
                model="claude-opus-5",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text if response.content else "[]"

            try:
                trends = json.loads(text)
                return trends if isinstance(trends, list) else []
            except:
                logger.warning(f"Could not parse trends JSON for {company_name}")
                return []

        except Exception as e:
            logger.error(f"Trend research failed: {e}")
            return []

    def _save_financials(self, company_name: str, financials: list):
        """Save financial records to database."""
        for record in financials:
            try:
                history_tracker.add_financials(company_name, record.get('period'), record)
            except Exception as e:
                logger.error(f"Failed to save financial data: {e}")

    def _save_deals(self, company_name: str, deals: list):
        """Save deals to database."""
        for deal in deals:
            try:
                history_tracker.add_deal(company_name, deal)
            except Exception as e:
                logger.error(f"Failed to save deal: {e}")

    def _save_market_trends(self, company_name: str, trends: list):
        """Save market trends to database."""
        for trend in trends:
            try:
                category = trend.pop('category', 'other')
                period = trend.pop('period', datetime.now().strftime('%Y'))
                history_tracker.add_market_trend(company_name, category, {**trend, 'period': period})
            except Exception as e:
                logger.error(f"Failed to save trend: {e}")

    def _save_findings(self, company_name: str, auto_data: dict):
        """Save auto-gathered findings for admin review (hybrid workflow)."""
        try:
            from company_research_findings_service import research_findings_service
            research_findings_service.set_db(self.db)
            research_findings_service.save_agent_findings(company_name, auto_data)
            logger.info(f"[findings] Saved auto-gathered data for {company_name}")
        except Exception as e:
            logger.error(f"[findings] Failed to save: {e}")


def main():
    """CLI interface for research agent."""
    parser = argparse.ArgumentParser(description='Research company intelligence data')
    parser.add_argument('--companies', type=str, help='Comma-separated company names (e.g. "Reckitt,Henkel,Netflix")')
    parser.add_argument('--company', type=str, help='Single company name')

    args = parser.parse_args()

    agent = CompanyResearchAgent()

    # Determine which companies to research
    if args.company:
        companies = [args.company]
    elif args.companies:
        companies = [c.strip() for c in args.companies.split(',')]
    else:
        print("Usage: python3 company_research_agent.py --companies 'Reckitt,Henkel,Netflix'")
        sys.exit(1)

    # Research each company
    results = []
    for company in companies:
        result = agent.research_company(company)
        results.append(result)

    # Print summary
    print("\n" + "="*60)
    print("RESEARCH SUMMARY")
    print("="*60)
    for result in results:
        print(f"\n{result['company']}: {result['status'].upper()}")
        print(f"  Financial records: {len(result.get('financials', []))}")
        print(f"  Deals: {len(result.get('deals', []))}")
        print(f"  Market trends: {len(result.get('market_trends', []))}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
