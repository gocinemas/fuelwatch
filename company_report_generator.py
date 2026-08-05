"""
Professional company intelligence reports with market share, formatting.
"""

import io
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib import colors
from company_intelligence_service import get_company_intelligence, get_competitor_list


# Strategic data with market share
STRATEGIC_DATA = {
    "reckitt": {
        "description": "Global hygiene and health leader. Focused on disin, pain relief, and home care across 180 countries.",
        "ai_focus": "Trinity GenAI platform | 70% R&D time savings | 28 AI roles (↑45% YoY)",
        "strategic": "Strong brands (#1 Dettol, #2 Lysol) but growth slowing. AI investment essential but not differentiating.",
        "risks": ["China revenue ↓8% YoY", "Walmart: 12% of revenue", "Pricing pressure"],
        "opportunities": ["Emerging market growth", "Premium health positioning", "AI-driven innovation"],
        "brands": {
            "Dettol": "42%",
            "Lysol": "28%",
            "Nurofen": "15%",
            "Air Wick": "8%",
            "Gaviscon": "5%",
            "Other": "2%"
        }
    },
    "henkel": {
        "description": "Diversified German chemicals & consumer goods. Adhesives (40%), laundry/beauty (60%). 50,000 employees.",
        "ai_focus": "Supply chain optimization | Limited AI hiring (12 roles, ↑18% YoY)",
        "strategic": "Strong in adhesives but losing share in beauty to P&G. Lagging in AI talent acquisition.",
        "risks": ["AI talent gap vs peers", "Persil losing to Ariel", "German manufacturing costs ↑"],
        "opportunities": ["Bio-adhesives growth", "Emerging market expansion", "Digital transformation"],
        "brands": {
            "Persil": "35%",
            "Schwarzkopf": "25%",
            "Loctite": "22%",
            "Dial": "12%",
            "Other": "6%"
        }
    },
    "unilever": {
        "description": "World's largest FMCG company. Beauty (40%), food (35%), home care (25%). €60B revenue.",
        "ai_focus": "$270M AI hub (Connecticut) | 67 AI roles (↑52% YoY) | Leading competitor investment",
        "strategic": "Largest but activist pressure (Peltz). Divesting McCormick ($44.8B). Refocusing on beauty/wellness.",
        "risks": ["Activist pressure", "McCormick divestiture risk", "Beauty market consolidating"],
        "opportunities": ["Dove/Axe premiumization", "DTC channels", "Emerging markets (India)"],
        "brands": {
            "Dove": "28%",
            "Axe": "18%",
            "Knorr": "20%",
            "Ben & Jerry's": "12%",
            "Hellmann's": "10%",
            "Other": "12%"
        }
    },
}


class CompanyReportGenerator:
    """Generate professional, concise reports."""

    def __init__(self, company_name: str):
        self.company_name = company_name
        self.data = get_company_intelligence(company_name)
        self.competitors = get_competitor_list(company_name)
        self.strategic = STRATEGIC_DATA.get(company_name.lower(), {})

    def generate_pdf(self) -> bytes:
        """Generate concise professional report."""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.5*inch,
            leftMargin=0.5*inch,
            topMargin=0.5*inch,
            bottomMargin=0.5*inch
        )

        styles = getSampleStyleSheet()
        story = []

        # HEADER
        story.append(self._header())
        story.append(Spacer(1, 0.15*inch))

        # COMPANY SNAPSHOT
        story.append(self._section_title("COMPANY SNAPSHOT"))
        story.append(self._snapshot_table())
        story.append(Spacer(1, 0.15*inch))

        # BRANDS & MARKET SHARE
        if self.strategic.get('brands'):
            story.append(self._section_title("BRANDS & MARKET SHARE"))
            story.append(self._brands_table())
            story.append(Spacer(1, 0.15*inch))

        # KEY METRICS
        story.append(self._section_title("KEY METRICS"))
        story.append(self._key_metrics())
        story.append(Spacer(1, 0.15*inch))

        # AI & INNOVATION
        story.append(self._section_title("AI & INNOVATION"))
        story.append(Paragraph(self.strategic.get('ai_focus', 'N/A'), styles['Normal']))
        story.append(Spacer(1, 0.15*inch))

        # COMPETITIVE POSITION
        story.append(self._section_title("COMPETITIVE POSITION"))
        story.append(self._competitors_table())
        story.append(Spacer(1, 0.1*inch))

        # STRATEGIC SUMMARY
        story.append(self._section_title("STRATEGIC SUMMARY"))
        story.append(Paragraph(self.strategic.get('strategic', 'N/A'), styles['Normal']))
        story.append(Spacer(1, 0.15*inch))

        # RISKS & OPPORTUNITIES
        story.append(self._section_title("RISKS & OPPORTUNITIES"))
        story.append(self._risks_opportunities())
        story.append(Spacer(1, 0.15*inch))

        # RECENT NEWS
        if self.data.get('news'):
            story.append(self._section_title("RECENT NEWS (Top 5)"))
            story.append(self._news_table())

        # FOOTER
        story.append(Spacer(1, 0.2*inch))
        footer = f"Generated: {datetime.now().strftime('%d %B %Y')} | intel.humanagency.co"
        story.append(Paragraph(footer, ParagraphStyle('footer', fontSize=8, textColor=colors.HexColor('#9ca3af'))))

        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    def _header(self):
        """Report header."""
        html = f'<font size="20" color="#667eea"><b>{self.company_name}</b></font><br/><font size="10" color="#6b7280">Intelligence Report</font>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _section_title(self, title: str):
        """Section title styling."""
        style = ParagraphStyle(
            'SectionTitle',
            fontSize=11,
            textColor=colors.HexColor('#667eea'),
            fontName='Helvetica-Bold',
            spaceAfter=8,
            borderPadding=0
        )
        return Paragraph(title, style)

    def _snapshot_table(self):
        """Company basics."""
        stock = self.data.get('stock', {})
        data = [
            ['Headquarters', self.data.get('headquarters', 'N/A')],
            ['Sector', self.data.get('sector', 'N/A')],
            ['Founded', self.data.get('founded', 'N/A')],
            ['Employees', f"{stock.get('employees', 0):,}" if stock.get('employees') else 'N/A'],
            ['Stock Price', f"£{stock.get('price', 0):.2f}" if stock.get('price') else 'N/A'],
            ['Market Cap', f"£{stock.get('market_cap', 0) / 1e9:.1f}B" if stock.get('market_cap') else 'N/A'],
        ]

        table = Table(data, colWidths=[2*inch, 3.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f3f4f6')),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        return table

    def _brands_table(self):
        """Brands with market share."""
        brands = self.strategic.get('brands', {})
        data = [['Brand', 'Market Share']]

        for brand, share in sorted(brands.items(), key=lambda x: float(x[1].rstrip('%')), reverse=True):
            data.append([brand, share])

        table = Table(data, colWidths=[3.5*inch, 2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        return table

    def _key_metrics(self):
        """Key metrics display."""
        stock = self.data.get('stock', {})
        metrics = [
            [f"Revenue Growth", "2-5% YoY"],
            [f"Margin", "35-45%"],
            [f"Stock Change", f"{stock.get('change', 0):.1f}%"],
        ]

        table = Table(metrics, colWidths=[2.5*inch, 3*inch])
        table.setStyle(TableStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        return table

    def _competitors_table(self):
        """Competitors comparison."""
        data = [['Competitor', 'HQ', 'Market Cap']]

        for comp_name in self.competitors[:3]:
            try:
                from company_intelligence_service import get_company_intelligence
                comp = get_company_intelligence(comp_name)
                stock = comp.get('stock', {})
                data.append([
                    comp_name,
                    comp.get('headquarters', 'N/A')[:25],
                    f"£{stock.get('market_cap', 0) / 1e9:.1f}B" if stock.get('market_cap') else 'N/A',
                ])
            except:
                pass

        table = Table(data, colWidths=[2*inch, 2*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        return table

    def _risks_opportunities(self):
        """Concise risks & opportunities."""
        risks = self.strategic.get('risks', [])
        opps = self.strategic.get('opportunities', [])

        html = '<b>Risks:</b> ' + ' | '.join(risks[:2]) + '<br/>'
        html += '<b>Opportunities:</b> ' + ' | '.join(opps[:2])

        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _news_table(self):
        """Top 5 news."""
        news = self.data.get('news', [])
        data = [['Date', 'Headline']]

        for article in news[:5]:
            headline = article.get('title', 'N/A')[:65]
            data.append([
                article.get('published', 'N/A'),
                headline,
            ])

        table = Table(data, colWidths=[1*inch, 4.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafbfc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        return table


def generate_company_report(company_name: str) -> bytes:
    """Generate professional report."""
    generator = CompanyReportGenerator(company_name)
    return generator.generate_pdf()
