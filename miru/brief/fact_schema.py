"""Fact schema with source traceability for data-driven brief generation."""

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class FactWithSource:
    """A fact with its source traced back to real data.

    Every suggestion Groq makes must reference a FactWithSource so we can
    validate it against actual data and prevent hallucinations.
    """
    text: str                    # "Fuel: Tesco 148.5p" or "School pickup 3:15pm"
    source_type: str            # "fuel" | "school" | "weather" | "spend" | "trains" | "transit"
    source_data: Dict[str, Any] # Original API response or DB row (merchant, station, etc.)
    is_inferred: bool           # False = direct from API, True = computed from data
    confidence: float           # 0.0-1.0 (1.0 = direct fact, 0.8 = computed)

    def to_dict(self) -> Dict:
        """Serialize for caching (source_data must be JSON-serializable)."""
        return {
            "text": self.text,
            "source_type": self.source_type,
            "source_data": self.source_data,
            "is_inferred": self.is_inferred,
            "confidence": self.confidence,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "FactWithSource":
        """Deserialize from cached dict."""
        return cls(**d)
