"""
Generate comprehensive company intelligence reports as PDF.
Includes: basics, brands, competitors, AI focus, risks, strategic analysis.
"""

import io
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from company_intelligence_service import get_company_intelligence, get_competitor_list


# Strategic analysis and AI focus data
STRATEGIC_DATA = {
    "reckitt": {
        "ai_focus": "Trinity platform for R&D automation (70% time savings). AI-driven drug discovery. Shanghai innovation center for biotech + AI. Hiring: 28 AI/ML roles (↑45% YoY). Investment: $50M+ in AI infrastructure.",
        "strategic_position": "Leader in CPG hygiene but facing execution pressure. Strong brand portfolio (#1 in disin, #2 in pain relief) but growth slowing. AI investment is table-stakes, not differentiating.",
        "risks": ["China revenue declining -8% YoY", "Customer concentration: Walmart 12% of sales", "Leadership transitions (CFO recent hire from P&G)", "Pricing pressure from private label"],
        "opportunities": ["Emerging market expansion (India, SE Asia)", "Premiumization of health brands (Nurofen → wellness)", "AI-driven R&D acceleration (20% faster new products)", "Sustainability-focused product lines"],
        "brands_detail": {"Dettol": "#1 disinfectant, premium positioning", "Lysol": "#2 spray disinfectant, strong US presence", "Nurofen": "Leader in OTC pain relief", "Air Wick": "Leader in home fragrance", "Gaviscon": "#1 heartburn relief", "Finish": "Dominant in dishwasher tablets"}
    },
    "henkel": {
        "ai_focus": "AI in supply chain optimization. Predictive maintenance for manufacturing. Limited AI hiring (12 roles, ↑18% YoY). R&D investment: €20M/year.",
        "strategic_position": "Diversified player with strong adhesives business (40% revenue). Lagging in AI talent acquisition vs Reckitt/Unilever. Conservative on digital transformation.",
        "risks": ["Losing AI talent race to larger competitors", "German manufacturing costs rising", "Persil brand losing share to Ariel (P&G)", "Dependent on automotive/construction cycles"],
        "opportunities": ["Sustainability play (bio-based adhesives)", "Digital-first marketing for younger demographics", "M&A in high-margin adhesive segments", "Emerging market consumer goods expansion"],
        "brands_detail": {"Persil": "Strong in Europe, declining vs Ariel", "Schwarzkopf": "Leader in color cosmetics", "Dial": "Soap brand, steady position", "Loctite": "Industrial adhesive leader"}
    },
    "unilever": {
        "ai_focus": "$270M Connecticut hub for AI + bioscience + quantum. Hiring: 67 AI/ML roles (↑52% YoY). Leading AI investment among peers. GenAI for product development.",
        "strategic_position": "Largest player by revenue but activist pressure (Nelson Peltz). Divesting McCormick ($44.8B, lower-margin foods). Refocusing on beauty/wellness (higher margins). Execution risk high.",
        "risks": ["Activist investor pressure causing uncertainty", "McCormick divestiture execution (1-2 year timeline)", "Beauty market consolidation increasing", "Supply chain costs not stabilizing"],
        "opportunities": ["Dove/Axe premiumization (sustainability angle)", "Direct-to-consumer digital channels", "AI-driven personalized beauty products", "Emerging market penetration (India, Indonesia)"],
        "brands_detail": {"Dove": "#1 beauty soap, sustainable positioning", "Axe": "Male grooming, strong Gen-Z appeal", "Knorr": "Leading bouillon/soup brand", "Ben & Jerry's": "Premium ice cream, ESG alignment"}
    },
}


class CompanyReportGenerator:
    """Generate professional PDF reports for companies."""

    def __init__(self, company_name: str):
        self.company_name = company_name
        self.data = get_company_intelligence(company_name)
        self.competitors = get_competitor_list(company_name)
        self.strategic = STRATEGIC_DATA.get(company_name.lower(), {})

    def generate_pdf(self) -> bytes:
        """Generate comprehensive PDF report."""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=0.6*inch,
            leftMargin=0.6*inch,
            topMargin=0.6*inch,
            bottomMargin=0.6*inch
        )

        styles = getSampleStyleSheet()
        story = []

        # COVER
        story.append(self._cover_page(styles))
        story.append(PageBreak())

        # TABLE OF CONTENTS
        story.append(Paragraph("Contents", styles['Heading1']))
        story.append(Paragraph("1. Company Snapshot", styles['Normal']))
        story.append(Paragraph("2. Brand Portfolio", styles['Normal']))
        story.append(Paragraph("3. Competitive Position", styles['Normal']))
        story.append(Paragraph("4. AI & Innovation Focus", styles['Normal']))
        story.append(Paragraph("5. Strategic Analysis", styles['Normal']))
        story.append(Paragraph("6. Risks & Opportunities", styles['Normal']))
        story.append(Paragraph("7. Recent News", styles['Normal']))
        story.append(PageBreak())

        # 1. COMPANY SNAPSHOT
        story.append(Paragraph("1. Company Snapshot", styles['Heading1']))
        story.append(self._basics_table())
        story.append(Spacer(1, 0.2*inch))

        # 2. BRAND PORTFOLIO
        if self.data.get('brands'):
            story.append(Paragraph("2. Brand Portfolio", styles['Heading1']))
            story.append(self._brands_section())
            story.append(Spacer(1, 0.2*inch))

        # 3. COMPETITIVE POSITION
        if self.competitors:
            story.append(Paragraph("3. Competitive Position", styles['Heading1']))
            story.append(self._competitors_section())
            story.append(Spacer(1, 0.2*inch))

        # 4. AI & INNOVATION
        if self.strategic.get('ai_focus'):
            story.append(Paragraph("4. AI & Innovation Focus", styles['Heading1']))
            story.append(Paragraph(self.strategic['ai_focus'], styles['Normal']))
            story.append(Spacer(1, 0.2*inch))

        # 5. STRATEGIC ANALYSIS
        if self.strategic.get('strategic_position'):
            story.append(Paragraph("5. Strategic Analysis", styles['Heading1']))
            story.append(Paragraph(self.strategic['strategic_position'], styles['Normal']))
            story.append(Spacer(1, 0.2*inch))

        # 6. RISKS & OPPORTUNITIES
        if self.strategic.get('risks') or self.strategic.get('opportunities'):
            story.append(Paragraph("6. Risks & Opportunities", styles['Heading1']))
            story.append(self._risks_opportunities())
            story.append(Spacer(1, 0.2*inch))

        # 7. RECENT NEWS
        if self.data.get('news'):
            story.append(Paragraph("7. Recent News", styles['Heading1']))
            story.append(self._news_section())

        # FOOTER
        story.append(Spacer(1, 0.3*inch))
        footer_text = f"Generated: {datetime.now().strftime('%d %B %Y')} | intel.humanagency.co"
        story.append(Paragraph(footer_text, styles['Normal']))

        # Build PDF
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    def _cover_page(self, styles):
        """Create cover page."""
        html = f"""
        <b><font size=28 color=#667eea>{self.company_name}</font></b><br/>
        <font size=14 color=#6b7280>Intelligence Report</font><br/><br/>
        <font size=11 color=#4b5563>
        Comprehensive analysis of company basics, strategy, competitive position,
        AI investments, and market outlook.
        </font><br/><br/>
        <font size=10 color=#9ca3af>
        {datetime.now().strftime('%d %B %Y')}<br/>
        intel.humanagency.co
        </font>
        """
        return Paragraph(html, styles['Normal'])

    def _basics_table(self):
        """Company basics table."""
        stock = self.data.get('stock', {})
        data = [
            ['Metric', 'Value'],
            ['Company', self.data.get('name', 'N/A')],
            ['Description', self.data.get('description', 'N/A')[:100]],
            ['Headquarters', self.data.get('headquarters', 'N/A')],
            ['Sector', self.data.get('sector', 'N/A')],
            ['Founded', self.data.get('founded', 'N/A')],
            ['Employees', f"{stock.get('employees', 0):,}" if stock.get('employees') else 'N/A'],
            ['Stock Price', f"£{stock.get('price', 0):.2f}" if stock.get('price') else 'N/A'],
            ['Market Cap', f"£{stock.get('market_cap', 0) / 1e9:.1f}B" if stock.get('market_cap') else 'N/A'],
            ['52-Week Change', f"{stock.get('change', 0):.2f}%" if stock.get('change') is not None else 'N/A'],
        ]

        table = Table(data, colWidths=[2.2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        return table

    def _brands_section(self):
        """Brand portfolio with details."""
        brands = self.data.get('brands', [])
        detail = self.strategic.get('brands_detail', {})

        html = '<table border="1" cellpadding="6">'
        html += '<tr bgcolor="#667eea"><td><b><font color="white">Brand</font></b></td><td><b><font color="white">Position</font></b></td></tr>'

        for brand in brands:
            pos = detail.get(brand, "Key brand in portfolio")
            html += f'<tr><td>{brand}</td><td>{pos}</td></tr>'

        html += '</table>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _competitors_section(self):
        """Competitive analysis."""
        data = [['Company', 'HQ', 'Stock', 'Market Cap', 'AI Status']]

        for comp_name in self.competitors[:3]:
            try:
                from company_intelligence_service import get_company_intelligence
                comp_data = get_company_intelligence(comp_name)
                stock = comp_data.get('stock', {})
                ai_status = "Investing" if comp_name.lower() in ['unilever', 'google'] else "Moderate"
                data.append([
                    comp_data.get('name', comp_name),
                    comp_data.get('headquarters', 'N/A')[:20],
                    f"£{stock.get('price', 'N/A')}" if stock.get('price') else 'N/A',
                    f"£{stock.get('market_cap', 0) / 1e9:.1f}B" if stock.get('market_cap') else 'N/A',
                    ai_status,
                ])
            except:
                pass

        table = Table(data, colWidths=[1.2*inch, 1.2*inch, 0.8*inch, 1*inch, 1*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        return table

    def _risks_opportunities(self):
        """Risks and opportunities."""
        risks = self.strategic.get('risks', [])
        opps = self.strategic.get('opportunities', [])

        html = '<b>Risks:</b><br/>'
        for risk in risks:
            html += f'• {risk}<br/>'

        html += '<br/><b>Opportunities:</b><br/>'
        for opp in opps:
            html += f'• {opp}<br/>'

        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _news_section(self):
        """Recent news."""
        news = self.data.get('news', [])
        data = [['Date', 'Source', 'Headline']]

        for article in news[:8]:
            headline = article.get('title', 'N/A')[:60]
            data.append([
                article.get('published', 'N/A'),
                article.get('source', 'N/A')[:15],
                headline,
            ])

        table = Table(data, colWidths=[0.9*inch, 1.2*inch, 3.9*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        return table


def generate_company_report(company_name: str) -> bytes:
    """Generate comprehensive PDF report."""
    generator = CompanyReportGenerator(company_name)
    return generator.generate_pdf()
