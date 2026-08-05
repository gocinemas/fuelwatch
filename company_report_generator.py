"""
Generate comprehensive company intelligence reports as PDF.
"""

import io
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from company_intelligence_service import get_company_intelligence, get_competitor_list


class CompanyReportGenerator:
    """Generate professional PDF reports for companies."""

    def __init__(self, company_name: str):
        self.company_name = company_name
        self.data = get_company_intelligence(company_name)
        self.competitors = get_competitor_list(company_name)

    def generate_pdf(self) -> bytes:
        """Generate PDF report and return as bytes."""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch
        )

        styles = getSampleStyleSheet()
        story = []

        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=28,
            textColor=colors.HexColor('#667eea'),
            spaceAfter=6,
            fontName='Helvetica-Bold'
        )
        story.append(Paragraph(f"{self.company_name} Intelligence Report", title_style))

        # Date
        date_style = ParagraphStyle(
            'DateStyle',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#6b7280'),
            spaceAfter=20
        )
        story.append(Paragraph(f"Generated: {datetime.now().strftime('%d %B %Y')}", date_style))
        story.append(Spacer(1, 0.2*inch))

        # Section 1: Basics
        story.append(self._section_heading("📋 Company Basics"))
        story.append(self._basics_table())
        story.append(Spacer(1, 0.3*inch))

        # Section 2: Brands
        if self.data.get('brands'):
            story.append(self._section_heading("🏷️ Brand Portfolio"))
            story.append(self._brands_table())
            story.append(Spacer(1, 0.3*inch))

        # Section 3: Competitors
        if self.competitors:
            story.append(self._section_heading("🆚 Competitive Position"))
            story.append(self._competitors_table())
            story.append(Spacer(1, 0.3*inch))

        # Section 4: Key Information
        story.append(self._section_heading("📊 Key Information"))
        story.append(self._key_info_table())
        story.append(Spacer(1, 0.3*inch))

        # Section 5: News
        if self.data.get('news'):
            story.append(self._section_heading("📰 Recent News"))
            story.append(self._news_table())
            story.append(Spacer(1, 0.3*inch))

        # Build PDF
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    def _section_heading(self, text: str) -> Paragraph:
        """Create a section heading."""
        style = ParagraphStyle(
            'SectionHeading',
            fontSize=14,
            textColor=colors.HexColor('#1c1917'),
            fontName='Helvetica-Bold',
            spaceAfter=12,
            borderPadding=10,
            borderColor=colors.HexColor('#667eea'),
            borderWidth=2,
            borderRadius=4
        )
        return Paragraph(text, style)

    def _basics_table(self) -> Table:
        """Create basics information table."""
        stock = self.data.get('stock', {})
        data = [
            ['Company Name', self.data.get('name', 'N/A')],
            ['Description', self.data.get('description', 'N/A')],
            ['Headquarters', self.data.get('headquarters', 'N/A')],
            ['Sector', self.data.get('sector', 'N/A')],
            ['Founded', self.data.get('founded', 'N/A')],
            ['Employees', f"{stock.get('employees', 'N/A'):,}" if stock.get('employees') else 'N/A'],
            ['Stock Price', f"£{stock.get('price', 'N/A')}" if stock.get('price') else 'N/A'],
            ['Market Cap', f"£{stock.get('market_cap', 'N/A') / 1e9:.1f}B" if stock.get('market_cap') else 'N/A'],
            ['Stock Change', f"{stock.get('change', 0):.2f}%" if stock.get('change') else 'N/A'],
        ]

        table = Table(data, colWidths=[2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f3f4f6')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), TA_LEFT),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e5e7eb')),
        ]))
        return table

    def _brands_table(self) -> Table:
        """Create brands table."""
        brands = self.data.get('brands', [])
        data = [['Brand']]
        for brand in brands:
            data.append([brand])

        table = Table(data, colWidths=[6*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), TA_LEFT),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e5e7eb')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
        ]))
        return table

    def _competitors_table(self) -> Table:
        """Create competitors comparison table."""
        data = [['Company', 'HQ', 'Stock Price', 'Market Cap', 'Employees']]

        for comp_name in self.competitors:
            try:
                from company_intelligence_service import get_company_intelligence
                comp_data = get_company_intelligence(comp_name)
                stock = comp_data.get('stock', {})
                data.append([
                    comp_data.get('name', comp_name),
                    comp_data.get('headquarters', 'N/A'),
                    f"£{stock.get('price', 'N/A')}" if stock.get('price') else 'N/A',
                    f"£{stock.get('market_cap', 'N/A') / 1e9:.1f}B" if stock.get('market_cap') else 'N/A',
                    f"{stock.get('employees', 'N/A'):,}" if stock.get('employees') else 'N/A',
                ])
            except:
                pass

        table = Table(data, colWidths=[1.5*inch, 1.5*inch, 1.2*inch, 1.2*inch, 1.2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), TA_LEFT),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e5e7eb')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
        ]))
        return table

    def _key_info_table(self) -> Table:
        """Create key information table."""
        data = [
            ['Metric', 'Details'],
            ['Website', self.data.get('website', 'N/A')],
            ['AI Focus', 'See Company Q&A for detailed insights'],
            ['Data Source', 'Company Database + Wikipedia + NewsAPI + Yahoo Finance'],
        ]

        table = Table(data, colWidths=[2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), TA_LEFT),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e5e7eb')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
        ]))
        return table

    def _news_table(self) -> Table:
        """Create news table."""
        news = self.data.get('news', [])
        data = [['Date', 'Source', 'Headline']]

        for article in news[:10]:  # Limit to 10 latest
            data.append([
                article.get('published', 'N/A'),
                article.get('source', 'N/A'),
                article.get('title', 'N/A')[:60] + '...' if len(article.get('title', '')) > 60 else article.get('title', 'N/A'),
            ])

        table = Table(data, colWidths=[1*inch, 1.5*inch, 3.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), TA_LEFT),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e5e7eb')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
        ]))
        return table


def generate_company_report(company_name: str) -> bytes:
    """Generate PDF report for a company."""
    generator = CompanyReportGenerator(company_name)
    return generator.generate_pdf()
