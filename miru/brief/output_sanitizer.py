"""OutputSanitizer — final safety check on generated narrative."""

import logging
import re

logger = logging.getLogger(__name__)


class OutputSanitizer:
    """Sanitize brief narrative before display.

    Checks:
    - Length limit (max 200 chars)
    - HTML escaping (safe for web display)
    - No dangerous Unicode
    - No URLs unless whitelisted
    """

    MAX_LENGTH = 200

    @staticmethod
    def sanitize(text: str) -> str:
        """Sanitize brief text.

        Args:
            text: Raw brief narrative from Groq

        Returns:
            Safe, sanitized text ready for display
        """
        if not text:
            return ""

        text = text.strip()

        # Hard length limit
        if len(text) > OutputSanitizer.MAX_LENGTH:
            text = text[:OutputSanitizer.MAX_LENGTH - 3] + "..."

        # Remove dangerous Unicode
        text = OutputSanitizer._clean_unicode(text)

        # HTML escape for web safety
        text = OutputSanitizer._html_escape(text)

        # Remove URLs unless whitelisted
        text = OutputSanitizer._sanitize_urls(text)

        logger.debug(f"Sanitized output: {text[:100]}...")
        return text

    @staticmethod
    def _clean_unicode(text: str) -> str:
        """Remove invalid/dangerous Unicode characters."""
        try:
            # Encode to UTF-8 (safe encoding)
            # Decode with 'ignore' to drop problematic chars
            return text.encode("utf-8", errors="ignore").decode("utf-8")
        except Exception:
            return text

    @staticmethod
    def _html_escape(text: str) -> str:
        """Escape HTML special characters."""
        escapes = {
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
        }
        for char, escape in escapes.items():
            text = text.replace(char, escape)
        return text

    @staticmethod
    def _sanitize_urls(text: str) -> str:
        """Remove URLs unless whitelisted."""
        # Whitelist: only humanagency.co domains
        WHITELIST = r"https?://(www\.)?humanagency\.co"

        # Find all URLs
        url_pattern = r"https?://\S+"
        urls = re.findall(url_pattern, text)

        for url in urls:
            if not re.match(WHITELIST, url):
                # Remove non-whitelisted URL
                text = text.replace(url, "[link]")
                logger.debug(f"Removed non-whitelisted URL: {url}")

        return text
