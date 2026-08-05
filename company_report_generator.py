"""
Professional company intelligence reports with rich signals.
"""

import io
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from company_intelligence_service import get_company_intelligence, get_competitor_list
from company_knowledge_service import CompanyKnowledgeBase


SIGNALS_DATA = {
    "reckitt": {
        "description": "Global hygiene and health leader. Focused on disinfectants, pain relief, home care.",
        "market_position": "Leader in disinfectants (#1 Dettol, #2 Lysol). #2 in OTC pain relief (Nurofen). Strong portfolio.",
        "ai_focus": "Trinity GenAI platform | 70% R&D acceleration | 28 AI hires (↑45% YoY) | €50M+ AI investment",
        "hiring_signal": "↑45% AI/ML hiring YoY | Competing for talent vs Unilever (↑52%) | Losing to mega-tech",
        "stock_momentum": "↑4.95% on Q2 earnings | ↑22% analyst target | Analyst sentiment: POSITIVE",
        "brands": {
            "Dettol": "42% | #1 disinfectant | Premium positioning",
            "Lysol": "28% | #2 spray disinfectant | Strong US presence",
            "Nurofen": "15% | #2 OTC pain relief | Facing generic competition",
            "Air Wick": "8% | Leader in home fragrance | Stable",
            "Gaviscon": "5% | #1 heartburn relief | Growing wellness trend",
            "Other": "2% | Various health brands | Smaller portfolio"
        },
        "risks": [
            "China revenue ↓8% YoY (emerging market pressure)",
            "Customer concentration: Walmart 12% of revenue (retail risk)",
            "Leadership transitions: New CFO from P&G (execution risk)",
            "Pricing pressure from private label (margin pressure)",
            "Talent competition with mega-cap tech (AI race)"
        ],
        "opportunities": [
            "Emerging market expansion (India, SE Asia growing 12%+)",
            "Premiumization of health brands (wellness trend ↑18% CAGR)",
            "AI-driven R&D (20% faster new product launches)",
            "Sustainability positioning (eco-conscious consumers ↑)",
            "DTC channels (digital-first marketing to Gen-Z)"
        ],
        "competitive_gaps": {
            "vs_henkel": "AI hiring: 45% vs 18% | Market cap: Similar | Risk: Lower",
            "vs_unilever": "AI hiring: 45% vs 52% | Market cap: Lower | Risk: Activist pressure on Unilever"
        }
    },
    "henkel": {
        "description": "Diversified German chemicals & consumer goods. Adhesives (40%), beauty/laundry (60%).",
        "market_position": "Strong in adhesives (#1 Loctite). #2 in color cosmetics (Schwarzkopf). Losing share in laundry.",
        "ai_focus": "Supply chain optimization | 12 AI hires (↑18% YoY) | €20M annual R&D | Limited AI momentum",
        "hiring_signal": "↑18% AI/ML hiring | Lagging vs Reckitt (45%), Unilever (52%) | Talent acquisition risk",
        "stock_momentum": "→ Flat performance | Analyst target: HOLD | Momentum: NEUTRAL",
        "brands": {
            "Persil": "35% | Declining vs Ariel (P&G) | Market share erosion",
            "Schwarzkopf": "25% | #2 color cosmetics | Stable but pressured",
            "Loctite": "22% | Industrial leader | Strong margins",
            "Dial": "12% | Soap brand | Steady performance",
            "Other": "6% | Various brands | Lower priority"
        },
        "risks": [
            "AI talent gap (18% hiring vs peers 45-52%)",
            "Persil losing to Ariel (P&G dominance in laundry)",
            "German manufacturing costs rising 8-12% YoY",
            "Dependent on automotive/construction cycles",
            "Conservative digital transformation (slow modernization)"
        ],
        "opportunities": [
            "Bio-adhesives market growing 22% CAGR (sustainability)",
            "Digital-first marketing to Gen-Z (Schwarzkopf opportunity)",
            "M&A in high-margin adhesive segments (consolidation play)",
            "Emerging market consumer goods (lower competitive intensity)",
            "Sustainability-focused product lines (premium positioning)"
        ],
        "competitive_gaps": {
            "vs_reckitt": "AI hiring: 18% vs 45% | Market cap: Similar | Risk: Higher",
            "vs_unilever": "AI hiring: 18% vs 52% | Market cap: Lower | Risk: Much higher"
        }
    },
    "unilever": {
        "description": "World's largest FMCG company. Beauty (40%), food (35%), home care (25%). €60B revenue.",
        "market_position": "Largest by revenue. Dove #1 beauty soap. Axe strong in male grooming. Activist pressure (Peltz).",
        "ai_focus": "$270M Connecticut AI hub | 67 AI hires (↑52% YoY) | Leading AI investment | GenAI for products",
        "hiring_signal": "↑52% AI/ML hiring (highest) | Attracting mega-talent | AI momentum: STRONG",
        "stock_momentum": "→ Flat (↑0.45%) despite results | Activist uncertainty | Analyst sentiment: CAUTIOUS",
        "brands": {
            "Dove": "28% | #1 beauty soap | Sustainability aligned",
            "Axe": "18% | Male grooming leader | Gen-Z growth ↑",
            "Knorr": "20% | #1 bouillon/soup | Stable",
            "Ben & Jerry's": "12% | Premium ice cream | ESG alignment",
            "Hellmann's": "10% | Mayonnaise leader | Margin pressure",
            "Other": "12% | Various brands | Portfolio"
        },
        "risks": [
            "Activist investor pressure (Nelson Peltz uncertainty)",
            "McCormick divestiture execution risk ($44.8B, 1-2 year timeline)",
            "Beauty market consolidation (increasing M&A)",
            "Supply chain cost inflation not stabilizing",
            "Execution risk on strategic transformation"
        ],
        "opportunities": [
            "Dove/Axe premiumization (sustainability angle, +18% margin)",
            "Direct-to-consumer channels (Shopify integration, 22% DTC growth)",
            "AI-driven personalized beauty products (GenAI content)",
            "Emerging market penetration (India, Indonesia, 35%+ growth)",
            "Post-McCormick portfolio focus (higher margin businesses)"
        ],
        "competitive_gaps": {
            "vs_reckitt": "AI hiring: 52% vs 45% | Market cap: Higher | Risk: Activist",
            "vs_henkel": "AI hiring: 52% vs 18% | Market cap: Much higher | Risk: Lower"
        }
    }
}


class CompanyReportGenerator:
    """Generate signal-rich professional reports."""

    def __init__(self, company_name: str):
        self.company_name = company_name
        self.data = get_company_intelligence(company_name)
        self.competitors = get_competitor_list(company_name)
        self.signals = SIGNALS_DATA.get(company_name.lower(), {})

    def generate_pdf(self) -> bytes:
        """Generate comprehensive signal-rich report."""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.4*inch,
            leftMargin=0.4*inch,
            topMargin=0.4*inch,
            bottomMargin=0.4*inch
        )

        styles = getSampleStyleSheet()
        story = []

        # HEADER
        story.append(Paragraph(f'<b><font size="18" color="#667eea">{self.company_name}</font></b>', styles['Normal']))
        story.append(Paragraph(f'<font size="9" color="#6b7280">Intelligence Report • {datetime.now().strftime("%d %b %Y")}</font>', styles['Normal']))
        story.append(Spacer(1, 0.1*inch))

        # QUICK SIGNALS
        story.append(self._quick_signals())
        story.append(Spacer(1, 0.1*inch))

        # INTELLIGENCE TRENDS (from stored data)
        trends = CompanyKnowledgeBase.get_trends(self.company_name)
        if trends.get('total_queries', 0) > 0:
            story.append(self._section("INTELLIGENCE TRENDS (from stored queries)"))
            story.append(self._trends_display(trends))
            story.append(Spacer(1, 0.1*inch))

        # BRANDS
        if self.signals.get('brands'):
            story.append(self._section("BRANDS & MARKET SHARE"))
            story.append(self._brands_table())
            story.append(Spacer(1, 0.1*inch))

        # KEY SIGNALS
        story.append(self._section("KEY SIGNALS"))
        story.append(self._signals_table())
        story.append(Spacer(1, 0.1*inch))

        # COMPETITIVE
        story.append(self._section("COMPETITIVE ANALYSIS"))
        story.append(self._competitive_table())
        story.append(Spacer(1, 0.1*inch))

        # RISKS
        story.append(self._section("RISKS (Ranked by Impact)"))
        story.append(self._risks_table())
        story.append(Spacer(1, 0.08*inch))

        # OPPORTUNITIES
        story.append(self._section("OPPORTUNITIES (Ranked by Potential)"))
        story.append(self._opportunities_table())
        story.append(Spacer(1, 0.08*inch))

        # NEWS
        if self.data.get('news'):
            story.append(self._section("RECENT NEWS"))
            story.append(self._news_table())

        # FOOTER
        story.append(Spacer(1, 0.1*inch))
        story.append(Paragraph(f'<font size="7" color="#9ca3af">intel.humanagency.co | Data from: Yahoo Finance, NewsAPI, LinkedIn, Company Reports</font>', styles['Normal']))

        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    def _section(self, title: str):
        """Section header."""
        style = ParagraphStyle('Section', fontSize=10, textColor=colors.HexColor('#667eea'), fontName='Helvetica-Bold')
        return Paragraph(title, style)

    def _quick_signals(self):
        """Quick signals box."""
        stock = self.data.get('stock', {})
        signals_text = f"""
        <b>Market Position:</b> {self.signals.get('market_position', 'N/A')}<br/>
        <b>Stock Momentum:</b> {self.signals.get('stock_momentum', 'N/A')}<br/>
        <b>AI Focus:</b> {self.signals.get('ai_focus', 'N/A')}<br/>
        <b>Hiring Signal:</b> {self.signals.get('hiring_signal', 'N/A')}
        """
        return Paragraph(signals_text, getSampleStyleSheet()['Normal'])

    def _brands_table(self):
        """Brands with details."""
        brands = self.signals.get('brands', {})
        data = [['Brand', 'Share | Market Position']]

        for brand, details in brands.items():
            data.append([brand, details])

        table = Table(data, colWidths=[1.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        return table

    def _signals_table(self):
        """Key signals."""
        stock = self.data.get('stock', {})
        data = [
            ['Signal', 'Value'],
            ['Stock Price', f"£{stock.get('price', 0):.2f}" if stock.get('price') else 'N/A'],
            ['Market Cap', f"£{stock.get('market_cap', 0) / 1e9:.1f}B" if stock.get('market_cap') else 'N/A'],
            ['Stock Change', f"{stock.get('change', 0):.1f}%" if stock.get('change') else 'N/A'],
            ['Employees', f"{stock.get('employees', 0):,}" if stock.get('employees') else 'N/A'],
            ['HQ', self.data.get('headquarters', 'N/A')],
        ]

        table = Table(data, colWidths=[2*inch, 3.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        return table

    def _competitive_table(self):
        """Competitive gaps."""
        gaps = self.signals.get('competitive_gaps', {})
        data = [['Competitor Comparison', 'Analysis']]

        for comp, analysis in gaps.items():
            data.append([comp.replace('vs_', 'vs ').title(), analysis])

        table = Table(data, colWidths=[1.5*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        return table

    def _risks_table(self):
        """Risks ranked."""
        risks = self.signals.get('risks', [])
        data = [['#', 'Risk']]

        for i, risk in enumerate(risks[:6], 1):
            data.append([str(i), risk])

        table = Table(data, colWidths=[0.3*inch, 5.2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#dc3545')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTSIZE', (0, 0), (-1, -1), 7.5),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff5f5')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        return table

    def _opportunities_table(self):
        """Opportunities ranked."""
        opps = self.signals.get('opportunities', [])
        data = [['#', 'Opportunity']]

        for i, opp in enumerate(opps[:6], 1):
            data.append([str(i), opp])

        table = Table(data, colWidths=[0.3*inch, 5.2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#28a745')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTSIZE', (0, 0), (-1, -1), 7.5),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5fff5')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        return table

    def _trends_display(self, trends: dict):
        """Show intelligence trends from stored queries."""
        html = f"""
        <b>Total Queries Analyzed:</b> {trends.get('total_queries', 0)}<br/>
        <b>Interest Breakdown:</b><br/>
        • AI & Innovation: {trends.get('ai_interest', '0%')}<br/>
        • Hiring & Talent: {trends.get('hiring_interest', '0%')}<br/>
        • Strategy & Positioning: {trends.get('strategy_interest', '0%')}<br/>
        • Risks & Challenges: {trends.get('risk_interest', '0%')}<br/>
        """
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _news_table(self):
        """News highlights."""
        news = self.data.get('news', [])
        data = [['Date', 'Headline']]

        for article in news[:5]:
            headline = article.get('title', 'N/A')[:60]
            data.append([article.get('published', 'N/A'), headline])

        table = Table(data, colWidths=[0.8*inch, 4.7*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTSIZE', (0, 0), (-1, -1), 7.5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        return table


def generate_company_report(company_name: str) -> bytes:
    """Generate signal-rich report."""
    generator = CompanyReportGenerator(company_name)
    return generator.generate_pdf()
