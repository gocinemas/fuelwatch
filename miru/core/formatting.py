"""Global formatting standards — single source of truth."""

from datetime import datetime, date
import re


class AmbiguityError(Exception):
    """Raised when date format is ambiguous and cannot be safely parsed."""
    pass


class DateFormatter:
    """Single source of truth for all date formatting.

    Storage: ISO YYYY-MM-DD in database
    Display: DD/MM/YY in UI (British format)
    """

    @staticmethod
    def to_display(iso_date: str) -> str:
        """Convert ISO YYYY-MM-DD to DD/MM/YY display format.

        Args:
            iso_date: ISO format date (2026-06-09)

        Returns:
            British format (09/06/26)

        Raises:
            ValueError: If date is invalid
        """
        if not iso_date:
            return ""

        try:
            d = datetime.strptime(iso_date[:10], "%Y-%m-%d")
            return d.strftime("%d/%m/%y")
        except (ValueError, TypeError):
            return iso_date or ""

    @staticmethod
    def to_storage(any_format: str) -> str:
        """Normalize any date format to ISO YYYY-MM-DD for storage.

        Args:
            any_format: Date in any format

        Returns:
            ISO format (2026-06-09)

        Raises:
            AmbiguityError: If format cannot be safely determined
        """
        if not any_format:
            return ""

        any_format = any_format.strip()

        # Already ISO? Return as-is
        if re.match(r"^\d{4}-\d{2}-\d{2}$", any_format):
            return any_format

        # DD/MM/YY or DD/MM/YYYY?
        if re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", any_format):
            parts = any_format.split("/")
            day, month = int(parts[0]), int(parts[1])
            year_str = parts[2]

            # If year is 2 digits, assume 20XX
            if len(year_str) == 2:
                year = 2000 + int(year_str)
            else:
                year = int(year_str)

            # Validate: day > 12 means definitely DD/MM
            if day > 12:
                return f"{year:04d}-{month:02d}-{day:02d}"
            # month > 12 means definitely MM/DD (reverse)
            elif month > 12:
                return f"{year:04d}-{day:02d}-{month:02d}"
            # Both <= 12: ambiguous
            else:
                raise AmbiguityError(
                    f"Date {any_format} is ambiguous (could be DD/MM or MM/DD). "
                    "Please use ISO YYYY-MM-DD or ensure day > 12."
                )

        # Try parsing common UK formats
        for fmt in ["%d %b %Y", "%d %b %y", "%d-%m-%Y", "%d-%m-%y"]:
            try:
                d = datetime.strptime(any_format, fmt)
                return d.strftime("%Y-%m-%d")
            except ValueError:
                continue

        raise ValueError(f"Cannot parse date: {any_format}")

    @staticmethod
    def to_relative(iso_date: str) -> str:
        """Convert ISO date to relative (today, tomorrow, next Monday, etc.).

        Args:
            iso_date: ISO format date

        Returns:
            Relative description
        """
        if not iso_date:
            return ""

        try:
            target = datetime.strptime(iso_date[:10], "%Y-%m-%d").date()
            today = date.today()
            delta = (target - today).days

            if delta == 0:
                return "today"
            elif delta == 1:
                return "tomorrow"
            elif delta == -1:
                return "yesterday"
            elif 2 <= delta <= 6:
                return target.strftime("next %A")
            elif -6 <= delta <= -2:
                return target.strftime("last %A")
            else:
                return DateFormatter.to_display(iso_date)
        except (ValueError, TypeError):
            return iso_date or ""


class CurrencyFormatter:
    """Single source of truth for currency formatting.

    All amounts stored as pence (integer) in database.
    Display as £X.XX (pounds).
    """

    @staticmethod
    def format(amount_pence: int) -> str:
        """Format pence amount as £X.XX.

        Args:
            amount_pence: Amount in pence (e.g., 12345 = £123.45)

        Returns:
            Formatted string (£123.45)
        """
        if amount_pence is None:
            return "£0.00"

        pounds = amount_pence / 100
        return f"£{pounds:,.2f}"

    @staticmethod
    def format_simple(amount_pence: int) -> str:
        """Format pence without thousand separators.

        Args:
            amount_pence: Amount in pence

        Returns:
            Formatted string (£123.45)
        """
        if amount_pence is None:
            return "£0.00"

        pounds = amount_pence / 100
        return f"£{pounds:.2f}"

    @staticmethod
    def parse(text: str) -> float:
        """Parse text with £ symbol to float pounds.

        Args:
            text: Text like "£123.45" or "123.45"

        Returns:
            Float amount in pounds
        """
        if not text:
            return 0.0

        # Remove £ symbol and whitespace
        cleaned = text.replace("£", "").replace(",", "").strip()

        try:
            return float(cleaned)
        except ValueError:
            return 0.0


class TimeFormatter:
    """Single source of truth for time formatting."""

    @staticmethod
    def to_display(time_str: str) -> str:
        """Convert time to HH:MM format.

        Args:
            time_str: Time in any format (HH:MM:SS, HH:MM, etc.)

        Returns:
            HH:MM format (09:34)
        """
        if not time_str:
            return ""

        time_str = time_str.strip()

        # Already HH:MM? Return as-is
        if re.match(r"^\d{2}:\d{2}$", time_str):
            return time_str

        # HH:MM:SS? Take first 5 chars
        if re.match(r"^\d{2}:\d{2}:\d{2}$", time_str):
            return time_str[:5]

        # Try parsing as time
        for fmt in ["%H:%M:%S", "%H:%M", "%I:%M %p", "%I:%M%p"]:
            try:
                t = datetime.strptime(time_str, fmt).time()
                return t.strftime("%H:%M")
            except ValueError:
                continue

        return time_str

    @staticmethod
    def to_relative(iso_datetime: str) -> str:
        """Convert ISO datetime to relative (in X mins, departing now, etc.).

        Args:
            iso_datetime: ISO format datetime

        Returns:
            Relative description
        """
        if not iso_datetime:
            return ""

        try:
            target = datetime.fromisoformat(iso_datetime.replace("Z", "+00:00"))
            now = datetime.now(target.tzinfo) if target.tzinfo else datetime.now()
            delta = (target - now).total_seconds() / 60  # minutes

            if delta < 0:
                return "departed"
            elif delta < 1:
                return "now"
            elif delta < 60:
                mins = int(delta)
                return f"in {mins} min{'s' if mins != 1 else ''}"
            elif delta < 1440:
                hours = int(delta / 60)
                return f"in {hours}h"
            else:
                return TimeFormatter.to_display(iso_datetime[:5])
        except (ValueError, TypeError):
            return iso_datetime or ""
