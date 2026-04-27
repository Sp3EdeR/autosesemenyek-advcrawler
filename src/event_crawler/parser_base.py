from __future__ import annotations

import inspect
import re
import unicodedata
from datetime import date
from typing import Annotated, Any, ClassVar, TypeVar

HUNGARIAN_MONTHS = {
    "januar": 1,
    "februar": 2,
    "marcius": 3,
    "aprilis": 4,
    "majus": 5,
    "junius": 6,
    "julius": 7,
    "augusztus": 8,
    "szeptember": 9,
    "oktober": 10,
    "november": 11,
    "december": 12,
}

T = TypeVar("T")

class ParserBase:
    """Abstract base class for calendar event parsers."""

    id: Annotated[ClassVar[str],
        "Unique identifier for the parser. Override in each subclass."]

    Row = dict[str, Any]
    Result = list[Row]

    _registry: Annotated[ClassVar[dict[str, type[ParserBase]]],
        "Registry of all available parser implementations, keyed by their 'id' attribute."]
    _required_registry_attrs: ClassVar[tuple[str, ...]] = ("id", "url")

    @classmethod
    def get_registry(cls) -> dict[str, type[ParserBase]]:
        """Return the registry of all available parser implementations."""
        return cls._registry

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Automatically register subclasses in the registry of the nearest ParserBase owner."""
        super().__init_subclass__(**kwargs)

        if inspect.isabstract(cls):
            return

        for attr_name in cls._required_registry_attrs:
            if not hasattr(cls, attr_name):
                raise TypeError(f"{cls.__name__} must define {attr_name}")
        if not hasattr(cls, "_registry"):
            raise TypeError(f"{cls.__name__} must inherit from CrawlerBase or DownloaderBase.")

        if cls.id in cls._registry:
            raise ValueError(f"Duplicate 'id' registered: {cls.id}")
        cls._registry[cls.id] = cls

    @staticmethod
    def _extract_iso_date_from_caption(caption: str, day: int) -> str | None:
        """Extract an ISO date from a month caption and day number."""
        caption_norm = ParserBase._normalize_text_for_match(caption)
        month = None
        for month_name, month_number in HUNGARIAN_MONTHS.items():
            if month_name in caption_norm:
                month = month_number
                break

        if month is None:
            return None

        year_match = re.search(r"\b(\d{4})\b", caption_norm)
        if not year_match:
            return None

        year = int(year_match.group(1))
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    @staticmethod
    def _normalize_text_for_match(text: str) -> str:
        """Normalize text for more reliable matching: casefold and remove diacritics."""
        lowered = text.casefold()
        normalized = unicodedata.normalize("NFD", lowered)
        return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")

    @staticmethod
    def _collapse_whitespace(text: str) -> str:
        """Collapse all sequences of whitespace characters to a single space and trim."""
        return " ".join((text or "").replace("\xa0", " ").split())

    @staticmethod
    def _dedupe(values: list[T]) -> list[T]:
        """Return values in original order with duplicates removed."""
        seen: list[T] = []
        return [value for value in values if value not in seen and not seen.append(value)]
