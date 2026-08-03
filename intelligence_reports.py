"""
Scheduled Intelligence Reports
Generates weekly email briefs for companies and competitors.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from intelligence_5signals import get_5_signals, BriefGenerator

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates email-friendly intelligence reports."""

    @staticmethod
    def generate_weekly_report(
        primary_company: str,
        competitor_companies: Optional[List[str]] = None,
        week_start: Optional[datetime] = None
    ) -> str:
        """
        Generate weekly intelligence brief for email.

        Returns HTML-formatted email-friendly report.
        """
        if not week_start:
            # Default to this week
            today = datetime.now()
            week_start = today - timedelta(days=today.weekday())

        week_end = week_start + timedelta(days=6)

        # Fetch 5 signals for primary company
        primary_signals = get_5_signals(primary_company)

        # Fetch signals for competitors
        competitor_signals = {}
        if competitor_companies:
            for company in competitor_companies[:2]:  # Max 2 competitors for brevity
                competitor_signals[company] = get_5_signals(company)

        # Generate HTML report
        html = ReportGenerator._build_email_html(
            primary_company,
            primary_signals,
            competitor_signals,
            week_start,
            week_end
        )

        return html

    @staticmethod
    def _build_email_html(
        primary: str,
        primary_signals: Dict,
        competitors: Dict,
        week_start: datetime,
        week_end: datetime
    ) -> str:
        """Build email-friendly HTML report."""

        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{primary} Intelligence Brief</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            border-bottom: 3px solid #667eea;
            padding-bottom: 15px;
            margin-bottom: 25px;
        }}
        .title {{
            font-size: 1.6em;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .week {{
            font-size: 0.9em;
            opacity: 0.7;
        }}
        .section {{
            margin-bottom: 25px;
            background: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
        }}
        .section-title {{
            font-weight: bold;
            font-size: 1.1em;
            margin-bottom: 12px;
            border-bottom: 2px solid #667eea;
            padding-bottom: 8px;
        }}
        .signal-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #e0e0e0;
        }}
        .signal-row:last-child {{
            border-bottom: none;
        }}
        .signal-label {{
            font-weight: 600;
        }}
        .signal-value {{
            text-align: right;
        }}
        .company-section {{
            background: white;
            border-left: 4px solid #667eea;
            margin-bottom: 15px;
            padding: 12px;
        }}
        .brief {{
            background: #f0f4ff;
            padding: 15px;
            border-radius: 6px;
            margin-top: 20px;
            font-style: italic;
        }}
        .footer {{
            text-align: center;
            font-size: 0.85em;
            opacity: 0.6;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #e0e0e0;
        }}
        .trend-up {{ color: #28a745; }}
        .trend-down {{ color: #dc3545; }}
        .trend-flat {{ color: #ffc107; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="title">{primary} Intelligence Brief</div>
        <div class="week">Week of {week_start.strftime("%b %d, %Y")} – {week_end.strftime("%b %d, %Y")}</div>
    </div>

    <!-- 5-SIGNAL SNAPSHOT -->
    <div class="section">
        <div class="section-title">5-Signal Snapshot: {primary}</div>

        <div class="signal-row">
            <span class="signal-label">📈 Stock</span>
            <span class="signal-value">{ReportGenerator._format_signal(primary_signals.get('stock', {}))}
            </span>
        </div>

        <div class="signal-row">
            <span class="signal-label">💬 Sentiment</span>
            <span class="signal-value">{primary_signals.get('sentiment', {}).get('score', '—')}/100</span>
        </div>

        <div class="signal-row">
            <span class="signal-label">📊 Trends</span>
            <span class="signal-value">{ReportGenerator._format_signal(primary_signals.get('trends', {}))}
            </span>
        </div>

        <div class="signal-row">
            <span class="signal-label">👥 Hiring</span>
            <span class="signal-value">{ReportGenerator._format_hiring(primary_signals.get('hiring', {}))}
            </span>
        </div>

        <div class="signal-row">
            <span class="signal-label">📰 News</span>
            <span class="signal-value">{primary_signals.get('news', {}).get('count', '0')} articles</span>
        </div>
    </div>

    <!-- COMPETITIVE POSITION -->
    {ReportGenerator._build_competitive_section(primary, primary_signals, competitors)}

    <!-- INTERPRETATION -->
    <div class="section">
        <div class="section-title">Signal Interpretation</div>
        <div class="brief">
            {BriefGenerator.generate_brief(primary, primary_signals)}
        </div>
    </div>

    <!-- ACTION ITEMS -->
    <div class="section">
        <div class="section-title">Key Watch Items</div>
        <ul style="margin-top: 10px;">
            {ReportGenerator._build_action_items(primary, primary_signals)}
        </ul>
    </div>

    <div class="footer">
        <p>Real-time intelligence for deal-makers • Humanagency Intel</p>
        <p>View full dashboard: <a href="https://intel.humanagency.co/intelligence/5signals/{primary}">intel.humanagency.co</a></p>
    </div>
</body>
</html>
"""
        return html

    @staticmethod
    def _format_signal(signal: Dict) -> str:
        """Format a signal for display."""
        if not signal:
            return "—"

        direction = signal.get("direction", "flat")
        value = signal.get("value") or signal.get("change")

        if direction == "up":
            return f'<span class="trend-up">↑ {value}</span>'
        elif direction == "down":
            return f'<span class="trend-down">↓ {value}</span>'
        else:
            return f'<span class="trend-flat">→ {value}</span>'

    @staticmethod
    def _format_hiring(hiring: Dict) -> str:
        """Format hiring signal."""
        if not hiring:
            return "—"

        direction = hiring.get("direction", "flat")
        count = hiring.get("count", 0)

        if direction == "up":
            return f'<span class="trend-up">↑↑ {count} roles</span>'
        elif direction == "down":
            return f'<span class="trend-down">↓↓ {count} roles</span>'
        else:
            return f'<span class="trend-flat">→ {count} roles</span>'

    @staticmethod
    def _build_competitive_section(primary: str, primary_signals: Dict, competitors: Dict) -> str:
        """Build competitive position section."""
        if not competitors:
            return ""

        html = '<div class="section"><div class="section-title">Competitive Position</div>'

        for company, signals in competitors.items():
            html += f"""
            <div class="company-section">
                <strong>{company}</strong>
                <div class="signal-row" style="font-size: 0.9em; margin-top: 8px;">
                    <span>Sentiment: {signals.get('sentiment', {}).get('score', '—')}/100</span>
                    <span>Hiring: {ReportGenerator._format_hiring(signals.get('hiring', {}))}</span>
                </div>
            </div>
            """

        html += "</div>"
        return html

    @staticmethod
    def _build_action_items(primary: str, signals: Dict) -> str:
        """Generate action items based on signals."""
        items = []

        hiring = signals.get("hiring", {})
        sentiment = signals.get("sentiment", {})
        news = signals.get("news", {})

        # Hiring action item
        if hiring.get("direction") == "up":
            items.append(f"<li>Monitor hiring execution: Can {primary} hit {hiring.get('count')} role target?</li>")
        elif hiring.get("direction") == "down":
            items.append(f"<li>Investigate hiring slowdown: Any signals of upcoming changes?</li>")

        # Sentiment action item
        if sentiment.get("score", 50) < 40:
            items.append(f"<li>Declining sentiment: Review customer feedback and competitive threats</li>")

        # News action item
        if news.get("count", 0) > 5:
            items.append(f"<li>High news volume: Track major announcements and earnings releases</li>")

        # Default items
        if not items:
            items.append(f"<li>Monitor {primary}'s quarterly earnings release</li>")
            items.append("<li>Track competitive moves in core markets</li>")

        return "\n".join(items)


class ReportSubscription:
    """Manages report subscription preferences (stored in Supabase)."""

    @staticmethod
    def create_subscription(
        email: str,
        primary_company: str,
        competitor_companies: Optional[List[str]] = None,
        frequency: str = "weekly"
    ) -> Dict:
        """Create or update a report subscription."""
        try:
            import library as lib

            subscription = {
                "email": email,
                "primary_company": primary_company,
                "competitors": competitor_companies or [],
                "frequency": frequency,
                "created_at": datetime.utcnow().isoformat(),
                "next_send": (datetime.utcnow() + timedelta(days=7)).isoformat(),
                "active": True
            }

            # Store in Supabase
            sb = lib._sb()
            result = sb.table("intelligence_subscriptions").insert([subscription]).execute()

            return {"success": True, "subscription": result.data[0] if result.data else subscription}

        except Exception as e:
            logger.error(f"[subscription] Error: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    def get_subscriptions(email: str = None) -> List[Dict]:
        """Retrieve subscriptions for an email."""
        try:
            import library as lib

            sb = lib._sb()

            if email:
                result = sb.table("intelligence_subscriptions").select("*").eq("email", email).execute()
            else:
                result = sb.table("intelligence_subscriptions").select("*").eq("active", True).execute()

            return result.data or []

        except Exception as e:
            logger.error(f"[subscriptions] Error: {e}")
            return []

    @staticmethod
    def delete_subscription(email: str, primary_company: str) -> bool:
        """Delete a subscription."""
        try:
            import library as lib

            sb = lib._sb()
            sb.table("intelligence_subscriptions").delete().eq("email", email).eq(
                "primary_company", primary_company
            ).execute()

            return True

        except Exception as e:
            logger.error(f"[delete_subscription] Error: {e}")
            return False
