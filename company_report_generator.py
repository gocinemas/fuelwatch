"""
Generate comprehensive company intelligence reports as PDF.
"""

import io
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
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
            'Title',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#667eea'),
            spaceAfter=6
        )
        story.append(Paragraph(f"{self.company_name} Intelligence Report", title_style))

        # Date
        date_text = f"Generated: {datetime.now().strftime('%d %B %Y')}"
        story.append(Paragraph(date_text, styles['Normal']))
        story.append(Spacer(1, 0.3*inch))

        # Section 1: Basics
        story.append(Paragraph("Company Basics", styles['Heading2']))
        story.append(self._basics_table())
        story.append(Spacer(1, 0.2*inch))

        # Section 2: Brands
        if self.data.get('brands'):
            story.append(Paragraph("Brand Portfolio", styles['Heading2']))
            story.append(self._brands_table())
            story.append(Spacer(1, 0.2*inch))

        # Section 3: Competitors
        if self.competitors:
            story.append(Paragraph("Competitors", styles['Heading2']))
            story.append(self._competitors_table())
            story.append(Spacer(1, 0.2*inch))

        # Section 4: News
        if self.data.get('news'):
            story.append(Paragraph("Recent News", styles['Heading2']))
            story.append(self._news_table())
            story.append(Spacer(1, 0.2*inch))

        # Build PDF
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

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
        ]

        table = Table(data, colWidths=[2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
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
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ]))
        return table

    def _competitors_table(self) -> Table:
        """Create competitors comparison table."""
        data = [['Company', 'HQ', 'Stock Price', 'Market Cap']]

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
                ])
            except:
                pass

        table = Table(data, colWidths=[1.5*inch, 2*inch, 1.5*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ]))
        return table

    def _news_table(self) -> Table:
        """Create news table."""
        news = self.data.get('news', [])
        data = [['Date', 'Source', 'Headline']]

        for article in news[:10]:
            headline = article.get('title', 'N/A')
            if len(headline) > 50:
                headline = headline[:50] + '...'
            data.append([
                article.get('published', 'N/A'),
                article.get('source', 'N/A'),
                headline,
            ])

        table = Table(data, colWidths=[1*inch, 1.5*inch, 3.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ]))
        return table


def generate_company_report(company_name: str) -> bytes:
    """Generate PDF report for a company."""
    generator = CompanyReportGenerator(company_name)
    return generator.generate_pdf()
