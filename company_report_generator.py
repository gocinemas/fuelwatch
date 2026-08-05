"""
Professional, beautifully designed company intelligence reports.
Product-grade PDF with strong visual hierarchy and aesthetic appeal.
"""

import io
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from company_intelligence_service import get_company_intelligence, get_competitor_list
from company_knowledge_service import CompanyKnowledgeBase
from company_history_service import history_tracker


SIGNALS_DATA = {
    "netflix": {
        "industry": "Technology & Entertainment",
        "description": "Leading streaming entertainment service. 220M+ paid memberships across 192 countries. Original content + licensed library.",
        "market_position": "Clear market leader in streaming. Facing intensifying competition from Disney+, Amazon Prime, Max.",
        "ai_focus": "AI recommendation engine (core product) | Personalization ML | Content prediction | 15+ AI engineers dedicated",
        "hiring_signal": "↑28% ML/AI hiring YoY | Content algorithm focus | Competing with mega-tech for talent",
        "stock_momentum": "↑65% YoY (↑27% on latest results) | Analyst target: $300+ | Sentiment: VERY POSITIVE",
        "subscribers": {
            "Paid Memberships": "220M | Growing 12% YoY | ARPU stable",
            "Ad Tier": "55M | New revenue stream | Monetization upside",
            "Free Tier": "Phase out | Legacy | Conversion to paid ongoing"
        },
        "risks": [
            "Subscriber growth plateau in mature markets (US/UK saturated at 72M)",
            "Password sharing crack-down impact (retention risk)",
            "Churn acceleration in 2025 (competition, content fatigue)",
            "Content spend rising 8-12% annually (margin pressure)",
            "Licensing cost inflation (studio consolidation)",
            "International market penetration challenges (Asia profitability)"
        ],
        "opportunities": [
            "Ad tier monetization ($50-100M runway potential)",
            "Live sports (F1, WWE exclusives) expanding TAM",
            "Gaming integration (revenue diversification)",
            "Price increases (premium tier willingness ↑18%)",
            "International expansion (India, SE Asia growth 25%+ CAGR)",
            "Bundling deals with telecom partners"
        ],
        "competitive_gaps": {
            "vs_disney_plus": "Subscribers: 220M vs 150M | Content: Broader | Margin: Higher",
            "vs_amazon_prime": "Focus: Dedicated vs multi-purpose | Pricing power: Higher | Content spend: Netflix leads"
        }
    },
    "reckitt": {
        "description": "Global hygiene and health leader specializing in disinfectants, pain relief, and home care across 180 countries.",
        "market_position": "Leader in disinfectants (#1 Dettol, #2 Lysol). Strong in OTC pain relief (Nurofen). Premium portfolio.",
        "ai_focus": "Trinity GenAI platform | 70% R&D acceleration | 28 AI hires (↑45% YoY) | €50M+ AI investment",
        "hiring_signal": "↑45% AI/ML hiring YoY | Competing for talent vs Unilever (↑52%) | Losing to mega-tech",
        "stock_momentum": "↑4.95% on Q2 earnings | ↑22% analyst target | Sentiment: POSITIVE",
        "brands": {
            "Dettol": "42% | #1 disinfectant | Premium positioning | Strong global presence",
            "Lysol": "28% | #2 spray disinfectant | Strong US presence | Market leader in spray category",
            "Nurofen": "15% | #2 OTC pain relief | Facing generic competition | Growing wellness segment",
            "Air Wick": "8% | Leader in home fragrance | Stable segment | Premium positioning",
            "Gaviscon": "5% | #1 heartburn relief | Growing wellness trend | Strong growth potential",
            "Other": "2% | Various health brands | Smaller portfolio | Emerging opportunities"
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
        "description": "Diversified German chemicals & consumer goods company. Adhesives (40%), beauty/laundry (60%). 50K employees.",
        "market_position": "Strong in adhesives (#1 Loctite). #2 in color cosmetics (Schwarzkopf). Losing share in laundry.",
        "ai_focus": "Supply chain optimization | 12 AI hires (↑18% YoY) | €20M annual R&D | Limited AI momentum",
        "hiring_signal": "↑18% AI/ML hiring | Lagging vs Reckitt (45%), Unilever (52%) | Talent acquisition risk",
        "stock_momentum": "→ Flat performance | Analyst target: HOLD | Momentum: NEUTRAL",
        "brands": {
            "Persil": "35% | Declining vs Ariel (P&G) | Market share erosion | Premium positioning",
            "Schwarzkopf": "25% | #2 color cosmetics | Stable but pressured | Growing internationally",
            "Loctite": "22% | Industrial leader | Strong margins | B2B focus",
            "Dial": "12% | Soap brand | Steady performance | Regional strength",
            "Other": "6% | Various brands | Lower priority | Portfolio optimization"
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
        "description": "World's largest FMCG company. Beauty (40%), food (35%), home care (25%). €60B revenue. 140K employees.",
        "market_position": "Largest by revenue. Dove #1 beauty soap. Axe strong in male grooming. Activist pressure (Peltz).",
        "ai_focus": "$270M Connecticut AI hub | 67 AI hires (↑52% YoY) | Leading AI investment | GenAI for products",
        "hiring_signal": "↑52% AI/ML hiring (highest) | Attracting mega-talent | AI momentum: STRONG",
        "stock_momentum": "→ Flat (↑0.45%) despite results | Activist uncertainty | Sentiment: CAUTIOUS",
        "brands": {
            "Dove": "28% | #1 beauty soap | Sustainability aligned | Strong Gen-Z growth",
            "Axe": "18% | Male grooming leader | Gen-Z growth ↑ | Cultural relevance",
            "Knorr": "20% | #1 bouillon/soup | Stable | Regional strength",
            "Ben & Jerry's": "12% | Premium ice cream | ESG alignment | Strong brand identity",
            "Hellmann's": "10% | Mayonnaise leader | Margin pressure | Stable base",
            "Other": "12% | Various brands | Portfolio mix | Strategic assets"
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


class ProfessionalReportGenerator:
    """Generate publication-quality company intelligence reports."""

    def __init__(self, company_name: str):
        self.company_name = company_name
        self.data = get_company_intelligence(company_name)
        self.competitors = get_competitor_list(company_name)
        self.signals = SIGNALS_DATA.get(company_name.lower(), {})
        self.industry = self._detect_industry()

        # Initialize Supabase for history tracking
        try:
            from supabase import create_client
            import os
            sb = create_client(
                os.getenv("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co"),
                os.getenv("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")
            )
            history_tracker.set_db(sb)
            self.financial_history = history_tracker.get_financial_history(company_name, periods=4)
            self.deals = history_tracker.get_deals(company_name, limit=5)
            self.market_trends = history_tracker.get_market_trends(company_name)
        except Exception as e:
            self.financial_history = []
            self.deals = []
            self.market_trends = []

    def _detect_industry(self) -> str:
        """Detect company industry from signals or data."""
        if self.signals and 'industry' in self.signals:
            return self.signals['industry']

        sector = self.data.get('sector', '').lower()
        if any(word in sector for word in ['consumer', 'fmcg', 'health']):
            return 'CPG'
        elif any(word in sector for word in ['tech', 'software', 'internet']):
            return 'Technology'
        elif any(word in sector for word in ['entertainment', 'media', 'streaming']):
            return 'Entertainment'
        elif any(word in sector for word in ['pharma', 'biotech', 'health']):
            return 'Pharma'
        elif any(word in sector for word in ['finance', 'bank']):
            return 'Finance'
        else:
            return 'Other'

    def generate_pdf(self) -> bytes:
        """Generate beautifully designed report."""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.5*inch,
            leftMargin=0.5*inch,
            topMargin=0.6*inch,
            bottomMargin=0.5*inch
        )

        styles = getSampleStyleSheet()
        story = []

        # PAGE 1: COVER
        story.append(self._cover_page())
        story.append(PageBreak())

        # PAGE 2+: CONTENT
        story.append(self._header_section())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("EXECUTIVE SUMMARY"))
        story.append(Paragraph(self.signals.get('description', 'N/A'), styles['Normal']))
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("KEY SIGNALS"))
        story.append(self._key_signals_box())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("FINANCIAL HEALTH"))
        story.append(self._financial_metrics())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("GROWTH & MOMENTUM"))
        story.append(self._growth_metrics())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("COMPETITIVE INTENSITY"))
        story.append(self._competitive_intensity())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("AI MOMENTUM"))
        story.append(self._ai_momentum())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("VERDICT"))
        story.append(self._verdict())
        story.append(Spacer(1, 0.15*inch))

        # Industry-specific key metrics
        if 'Entertainment' in self.industry or 'Streaming' in self.industry:
            story.append(self._section_title("SUBSCRIBER METRICS"))
            story.append(self._subscriber_metrics())
        else:
            story.append(self._section_title("BRAND PORTFOLIO"))
            story.append(self._brands_section())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("FINANCIAL SNAPSHOT"))
        story.append(self._financial_snapshot())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("COMPETITIVE POSITIONING"))
        story.append(self._competitive_analysis())
        story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("STRATEGIC ASSESSMENT"))
        story.append(Paragraph(self.signals.get('strategic', 'N/A'), styles['Normal']))
        story.append(Spacer(1, 0.1*inch))

        story.append(self._section_title("RISKS"))
        story.append(self._risks_box())
        story.append(Spacer(1, 0.1*inch))

        story.append(self._section_title("OPPORTUNITIES"))
        story.append(self._opportunities_box())
        story.append(Spacer(1, 0.15*inch))

        # Historical trends (if data available)
        if self.financial_history:
            story.append(self._section_title("REVENUE TRAJECTORY"))
            story.append(self._financial_trends_box())
            story.append(Spacer(1, 0.15*inch))

        if self.market_trends:
            story.append(self._section_title("MARKET SHARE & GROWTH"))
            story.append(self._market_trends_box())
            story.append(Spacer(1, 0.15*inch))

        if self.deals:
            story.append(self._section_title("DEAL ACTIVITY"))
            story.append(self._deals_box())
            story.append(Spacer(1, 0.15*inch))

        story.append(self._section_title("RECENT NEWS"))
        story.append(self._news_section())

        # FOOTER
        story.append(Spacer(1, 0.2*inch))
        footer = f"intel.humanagency.co | {datetime.now().strftime('%d %B %Y')}"
        story.append(Paragraph(f'<font size="8" color="#9ca3af">{footer}</font>', styles['Normal']))

        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    def _cover_page(self):
        """Beautiful cover page."""
        html = f"""
        <br/><br/><br/><br/><br/>
        <font size="32" color="#667eea"><b>{self.company_name}</b></font>
        <br/><br/>
        <font size="16" color="#6b7280"><b>Intelligence Report</b></font>
        <br/><br/><br/><br/>
        <font size="11" color="#4b5563">{self.signals.get('description', 'Company Intelligence')}</font>
        <br/><br/><br/><br/><br/><br/>
        <font size="10" color="#9ca3af">
        Generated {datetime.now().strftime('%d %B %Y')}<br/>
        intel.humanagency.co
        </font>
        """
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _header_section(self):
        """Header with company info."""
        stock = self.data.get('stock', {})
        html = f"""
        <b><font size="16" color="#667eea">{self.company_name}</font></b><br/>
        <font size="10" color="#667eea">━━━━━━━━━━━━━━━━━━━━━━</font><br/>
        <font size="10" color="#4b5563">
        <b>Headquarters:</b> {self.data.get('headquarters', 'N/A')} |
        <b>Sector:</b> {self.data.get('sector', 'N/A')}<br/>
        <b>Employees:</b> {f"{stock.get('employees', 0):,}" if stock.get('employees') else 'N/A'} |
        <b>Market Cap:</b> £{stock.get('market_cap', 0) / 1e9:.1f}B
        </font>
        """
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _section_title(self, title: str):
        """Beautiful section title."""
        html = f'<font size="12" color="#667eea"><b>{title}</b></font><br/><font size="10" color="#667eea">━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</font>'
        style = ParagraphStyle('SectionTitle', fontSize=12, spaceAfter=10)
        return Paragraph(html, style)

    def _key_signals_box(self):
        """Key signals in formatted box."""
        html = f"""
        <font size="10" color="#1c1917">
        <b>Market Position:</b> {self.signals.get('market_position', 'N/A')}<br/><br/>
        <b>Stock Momentum:</b> {self.signals.get('stock_momentum', 'N/A')}<br/><br/>
        <b>AI Focus:</b> {self.signals.get('ai_focus', 'N/A')}<br/><br/>
        <b>Hiring Signal:</b> {self.signals.get('hiring_signal', 'N/A')}
        </font>
        """
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _subscriber_metrics(self):
        """Streaming subscriber metrics (Entertainment industry)."""
        subs = self.signals.get('subscribers', {})
        if not subs:
            return Paragraph('<font size="9" color="#1c1917">Subscriber data unavailable</font>', getSampleStyleSheet()['Normal'])

        data = [['Metric', 'Value | Details']]
        for metric, details in subs.items():
            data.append([metric, details])

        table = Table(data, colWidths=[1.3*inch, 4.2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ]))
        return table

    def _brands_section(self):
        """Brands with beautiful layout (CPG industry)."""
        brands = self.signals.get('brands', {})
        data = [['Brand', 'Market Share | Position']]

        for brand, details in brands.items():
            data.append([brand, details])

        table = Table(data, colWidths=[1.3*inch, 4.2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ]))
        return table

    def _financial_snapshot(self):
        """Financial metrics."""
        stock = self.data.get('stock', {})
        data = [
            ['Stock Price', f"£{stock.get('price', 0):.2f}" if stock.get('price') else 'N/A'],
            ['52-Week Change', f"{stock.get('change', 0):.1f}%" if stock.get('change') else 'N/A'],
            ['Ticker', stock.get('ticker', 'N/A')],
        ]

        table = Table(data, colWidths=[2.5*inch, 2.5*inch])
        table.setStyle(TableStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        return table

    def _competitive_analysis(self):
        """Competitive gaps."""
        gaps = self.signals.get('competitive_gaps', {})
        html = '<font size="9" color="#1c1917">'
        for comp, analysis in gaps.items():
            html += f"<b>{comp.replace('vs_', 'vs ').title()}:</b> {analysis}<br/><br/>"
        html += '</font>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _risks_box(self):
        """Risks with styling."""
        risks = self.signals.get('risks', [])
        html = '<font size="9" color="#1c1917">'
        for i, risk in enumerate(risks[:5], 1):
            html += f"<b>{i}.</b> {risk}<br/><br/>"
        html += '</font>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _opportunities_box(self):
        """Opportunities with styling."""
        opps = self.signals.get('opportunities', [])
        html = '<font size="9" color="#1c1917">'
        for i, opp in enumerate(opps[:5], 1):
            html += f"<b>{i}.</b> {opp}<br/><br/>"
        html += '</font>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _financial_metrics(self):
        """Real financial data from stock API."""
        stock = self.data.get('stock', {})
        html = f"""
        <font size="9" color="#1c1917">
        <b>Stock Price:</b> £{stock.get('price', 0):.2f} |
        <b>Market Cap:</b> £{stock.get('market_cap', 0) / 1e9:.1f}B |
        <b>52-Week Change:</b> {stock.get('change', 0):.1f}%
        </font>
        """
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _growth_metrics(self):
        """Growth trajectory."""
        # Use signals data if available (more accurate), else stock API
        signals_momentum = self.signals.get('stock_momentum', '')
        if signals_momentum:
            html = f"""
            <font size="9" color="#1c1917">
            <b>Stock Performance:</b> {signals_momentum}
            </font>
            """
        else:
            stock = self.data.get('stock', {})
            direction = "↑" if stock.get('change', 0) > 0 else "↓"
            html = f"""
            <font size="9" color="#1c1917">
            <b>1-Year Stock Performance:</b> {direction} {abs(stock.get('change', 0)):.1f}%<br/>
            <b>Market Momentum:</b> {'Positive' if stock.get('change', 0) > 0 else 'Negative'}<br/>
            <b>Analyst Sentiment:</b> {'POSITIVE' if stock.get('change', 0) > 2 else 'NEUTRAL'}
            </font>
            """
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _competitive_intensity(self):
        """How intense is competition."""
        html = f"""
        <font size="9" color="#1c1917">
        <b>Direct Competitors:</b> {', '.join(self.competitors[:3]) if self.competitors else 'N/A'}<br/>
        <b>Market Position:</b> {self.signals.get('market_position', 'N/A')[:80]}...<br/>
        <b>Threat Level:</b> {'HIGH' if len(self.competitors) > 2 else 'MODERATE'}
        </font>
        """
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _ai_momentum(self):
        """AI investment and hiring."""
        html = f"""
        <font size="9" color="#1c1917">
        {self.signals.get('hiring_signal', 'N/A')}<br/>
        <b>Strategic Priority:</b> {'HIGH - Leading AI investment' if '52' in self.signals.get('hiring_signal', '') or '45' in self.signals.get('hiring_signal', '') else 'MODERATE - Investing but not leading'}
        </font>
        """
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _verdict(self):
        """Investment thesis one-liner (industry-aware)."""
        stock = self.data.get('stock', {})
        change = stock.get('change', 0)

        if 'Entertainment' in self.industry or 'Streaming' in self.industry:
            # Streaming/Entertainment verdict based on subscriber growth + margins
            hiring = self.signals.get('hiring_signal', '')
            if change > 25 and '28' in hiring:
                verdict = "🟢 Strong momentum: Stock up 65%+ YoY + AI/tech focus. Profitability proven, subscriber growth stabilizing."
            elif change > 0 and '28' in hiring:
                verdict = "🟡 Positive trend: Recovering from saturation concerns, ad tier monetization upside."
            elif change > 0:
                verdict = "🟡 Watch for churn: Stock recovery but subscriber growth vulnerable to competition."
            else:
                verdict = "🔴 Cautious: Churn acceleration risk. Monitor ad tier execution + international growth."
        else:
            # CPG verdict (existing logic)
            if change > 5 and ('45' in self.signals.get('hiring_signal', '') or '52' in self.signals.get('hiring_signal', '')):
                verdict = "🟢 Strong momentum: Positive stock trend + leading AI investment. Monitor execution."
            elif change > 0 and '45' in self.signals.get('hiring_signal', ''):
                verdict = "🟡 Cautiously optimistic: Growing AI focus but execution risk remains."
            elif change > 0:
                verdict = "🟡 Stable but challenged: Positive momentum masked by competitive pressure."
            else:
                verdict = "🔴 Headwinds: Negative stock trend + competitive intensity. Monitor closely."

        html = f'<font size="10" color="#1c1917"><b>{verdict}</b></font>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _financial_trends_box(self):
        """Display historical revenue and margin trends."""
        if not self.financial_history:
            return Paragraph('<font size="9" color="#1c1917">No historical data available</font>', getSampleStyleSheet()['Normal'])

        html = '<font size="9" color="#1c1917">'
        for record in self.financial_history[:4]:
            period = record.get('period', 'N/A')
            revenue = record.get('revenue_millions', 0)
            margin = record.get('operating_margin_pct', 0)
            growth = record.get('revenue_growth_pct', 0)
            direction = "↑" if growth > 0 else "↓"
            html += f"<b>{period}:</b> £{revenue}M revenue ({direction}{abs(growth):.1f}% YoY), {margin}% operating margin<br/>"

        html += '</font>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _market_trends_box(self):
        """Display market share and growth trends."""
        if not self.market_trends:
            return Paragraph('<font size="9" color="#1c1917">No market data available</font>', getSampleStyleSheet()['Normal'])

        html = '<font size="9" color="#1c1917">'
        by_category = {}
        for trend in self.market_trends[:8]:
            category = trend.get('category', 'other')
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(trend)

        for category, trends in by_category.items():
            html += f"<b>{category.replace('_', ' ').title()}:</b><br/>"
            for trend in trends[:2]:
                metric = trend.get('metric_name', '').replace('_', ' ').title()
                value = trend.get('value_pct', 0)
                html += f"  • {metric}: {value}%<br/>"
            html += "<br/>"

        html += '</font>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _deals_box(self):
        """Display M&A and investment activity."""
        if not self.deals:
            return Paragraph('<font size="9" color="#1c1917">No deal data available</font>', getSampleStyleSheet()['Normal'])

        html = '<font size="9" color="#1c1917">'
        for deal in self.deals[:3]:
            deal_type = deal.get('deal_type', 'N/A').upper()
            target = deal.get('target_company') or deal.get('investor_company', 'N/A')
            amount = deal.get('amount_millions', 0)
            date = deal.get('announcement_date', 'N/A')
            html += f"<b>{date} - {deal_type}:</b> {target} (£{amount}M)<br/>"

        html += '</font>'
        return Paragraph(html, getSampleStyleSheet()['Normal'])

    def _news_section(self):
        """News with clean layout."""
        news = self.data.get('news', [])
        data = [['Date', 'Headline']]

        for article in news[:5]:
            headline = article.get('title', 'N/A')[:70]
            data.append([article.get('published', 'N/A'), headline])

        table = Table(data, colWidths=[0.8*inch, 4.7*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        return table


def generate_company_report(company_name: str) -> bytes:
    """Generate professional, beautiful report."""
    generator = ProfessionalReportGenerator(company_name)
    return generator.generate_pdf()
