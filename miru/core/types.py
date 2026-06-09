"""Typed data classes for all Miru features."""

from dataclasses import dataclass, field
from typing import Optional, List, Dict
from datetime import datetime


@dataclass
class Train:
    """A train departure."""
    from_code: str
    to_code: str
    departs_iso: str  # ISO format
    destination_name: str
    status: str = "On time"
    platform: Optional[str] = None

    def departs_display(self) -> str:
        """Display departure time."""
        if self.departs_iso:
            return self.departs_iso[:5]
        return ""


@dataclass
class Fuel:
    """A fuel station and price."""
    station_name: str
    price_pence: int  # e.g., 13450 = 134.50p
    fuel_type: str = "unleaded"
    distance_km: float = 0.0
    postcode: Optional[str] = None

    def price_display(self) -> str:
        """Display price."""
        return f"{self.price_pence / 10:.1f}p"


@dataclass
class SchoolEvent:
    """A school event (class, activity, deadline)."""
    child_name: str
    event_title: str
    event_date_iso: str  # ISO format
    event_type: str  # "activity", "reminder", "deadline"
    action_needed: Optional[str] = None
    deadline_iso: Optional[str] = None
    location: Optional[str] = None

    def date_display(self) -> str:
        """Display event date."""
        if self.event_date_iso:
            from .formatting import DateFormatter
            return DateFormatter.to_display(self.event_date_iso)
        return ""


@dataclass
class SpendItem:
    """A spending category total."""
    category: str
    amount_pence: int  # e.g., 12345 = £123.45
    transaction_count: int = 0

    def amount_display(self) -> str:
        """Display amount."""
        from .formatting import CurrencyFormatter
        return CurrencyFormatter.format(self.amount_pence)


@dataclass
class Spend:
    """User's monthly spending."""
    total_pence: int
    month: str  # "June 2026"
    breakdown: Dict[str, SpendItem] = field(default_factory=dict)
    last_merchant: Optional[str] = None
    last_date_iso: Optional[str] = None
    last_amount_pence: Optional[int] = None

    def total_display(self) -> str:
        """Display total."""
        from .formatting import CurrencyFormatter
        return CurrencyFormatter.format(self.total_pence)

    def last_amount_display(self) -> str:
        """Display last purchase amount."""
        from .formatting import CurrencyFormatter
        if self.last_amount_pence:
            return CurrencyFormatter.format(self.last_amount_pence)
        return ""


@dataclass
class Weather:
    """Weather conditions."""
    postcode: str
    temp_c: float
    description: str
    icon_code: Optional[str] = None

    def temp_display(self) -> str:
        """Display temperature."""
        return f"{self.temp_c:.0f}°C"


@dataclass
class BriefContext:
    """Full context for brief generation."""
    trains: Optional[List[Train]] = None
    fuel: Optional[Fuel] = None
    school_events: Optional[List[SchoolEvent]] = None
    spend: Optional[Spend] = None
    weather: Optional[Weather] = None
    recurring_activities: Optional[List[Dict]] = None
    saved_items: Optional[List[Dict]] = None

    def has_data(self) -> bool:
        """Check if context has any data."""
        return any([
            self.trains,
            self.fuel,
            self.school_events,
            self.spend,
            self.weather,
            self.recurring_activities,
            self.saved_items,
        ])
