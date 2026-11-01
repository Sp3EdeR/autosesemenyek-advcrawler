from __future__ import annotations

import importlib
import json
import re
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Any
from urllib import request
from zoneinfo import ZoneInfo

from utilities import get_first_town_norm, normalize_text_for_match

Record = dict[str, Any]
WrappedEvent = dict[str, dict[str, Any]]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEDUPE_DIR = PROJECT_ROOT / "dedupe"
SETTINGS_PATH = DEDUPE_DIR / "settings.dedupe"
SUMMARY_NORM_MAX_LEN = 256
DESCRIPTION_NORM_MAX_LEN = 512
LOCATION_NORM_MAX_LEN = 128
MERGE_JOINERS = {
    "summary": "\n",
    "location": "\n",
    "description": "\n\n",
}

HTML_TAG_RE = re.compile(r"</?[a-zA-Z]+[^>]*>")
NON_ALPHANUMERIC_RE = re.compile(r"[\W_]+")

DATE_TIMEZONE = ZoneInfo("Europe/Budapest")


def load_dedupe_modules() -> tuple[Any, Any]:
    """Import dedupe lazily so the CLI can still render help in broken environments."""
    try:
        dedupe_module = importlib.import_module("dedupe")
        variables_module = importlib.import_module("dedupe.variables")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Unable to import the installed dedupe package. "
            f"Missing module: {exc.name}."
        ) from exc

    return dedupe_module, variables_module


def load_event_list(path: str) -> list[WrappedEvent]:
    """Load a wrapped event list from JSON."""
    if re.match(r"^https?://", path):
        with request.urlopen(path) as response:
            return json.load(response)
    with open(path, encoding="utf-8") as file:
        return json.load(file)


def load_event_lists(paths: Sequence[str]) -> list[WrappedEvent]:
    """Load and concatenate multiple wrapped event lists."""
    events: list[WrappedEvent] = []
    for path in paths:
        events.extend(load_event_list(path))
    return events


def save_event_list(path: Path, events: Sequence[WrappedEvent]) -> None:
    """Write a wrapped event list to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(list(events), file, ensure_ascii=False, sort_keys=True)


def preprocess_events(events: Iterable[WrappedEvent]) -> dict[int, Record]:
    """Flatten wrapped events into indexed records and add normalized helper fields."""
    processed_records: dict[int, Record] = {}

    for index, wrapped_event in enumerate(events):
        for src, event in wrapped_event.items():
            processed = dict(event)
            processed["src"] = src

            processed["summary_norm"] = _normalized_dedupe_text(
                processed["summary"], SUMMARY_NORM_MAX_LEN
            )
            # Only use the date portion for deduplication, time is inconsistent.
            processed["dtstart_norm"] = _normalized_date(processed["dtstart"])
            processed["dtend_norm"] = _normalized_date(processed.get("dtend"), is_end_date=True)
            desc = processed.get("description")
            processed["description_norm"] = _normalized_dedupe_text(
                desc, DESCRIPTION_NORM_MAX_LEN
            )
            loc = processed.get("location")
            processed["location_norm"] = _normalized_dedupe_text(loc, LOCATION_NORM_MAX_LEN)
            town_sources = " ".join(filter(None, [loc, processed["summary"], desc]))
            processed["town_norm"] = get_first_town_norm(town_sources)
            # autosnaptar produces towns from reverse geocoding, which is sometimes incorrect
            processed["town_quality"] = "geocoded" if src == "autosnaptar_event" else "text"
            processed_records[index] = processed

    return processed_records


def merge_partitioned_records(
    records: dict[int, Record],
    clusters: Sequence[tuple[Sequence[int], Sequence[float]]],
) -> list[WrappedEvent]:
    """Merge partitioned records back into the wrapped event list format."""
    merged_events: list[WrappedEvent] = []

    for record_ids, _scores in sorted(clusters, key=lambda cluster: min(cluster[0])):
        cluster_records = [records[record_id] for record_id in sorted(record_ids)]
        merged_events.append(_merge_cluster_records(cluster_records))

    return merged_events


def _make_link(url: str) -> str:
    """Return a HTML link for the given URL."""
    return f'<a href="{url}">{url}</a>'


def _merge_cluster_records(records: Sequence[Record]) -> WrappedEvent:
    merged: dict[str, Any] = {}
    for field in {field for rec in records for field in rec}:
        if field in {"src", "town_quality"} or field.endswith("_norm"):
            continue

        values = list({rec[field] for rec in records if field in rec and rec[field] is not None})
        if not values:
            continue

        values.sort()
        if field == "summary":
            if len(values) == 1:
                merged[field] = values[0]
            else:
                merged[field] = max((str(value) for value in values), key=len)
                merged["summaries"] = "\n".join(values)
        elif field in {"dtstart", "dtend"}:
            merged[field] = max((str(value) for value in values), key=len)
        elif field == "description":
            merged[field] = "\n\n".join(values)
        elif field == "location":
            merged[field] = ", ".join(values)
        elif field == "url":
            if len(values) == 1:
                merged[field] = values[0]
            else:
                merged["urls"] = "\n".join(_make_link(url) for url in values)
        else:
            merged[field] = "\n".join(str(value) for value in values)

    src_key = ", ".join(sorted({rec["src"] for rec in records if "src" in rec}))
    return { src_key: merged }


def _normalized_date(value: str | None, *, is_end_date: bool = False) -> str | None:
    """Use Budapest dates and exclusive midnight ends; naive times are local, dates unchanged."""
    if not value:
        return None
    if len(value) == 10:
        return value
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(DATE_TIMEZONE)
    if is_end_date and not any((
        timestamp.hour, timestamp.minute, timestamp.second, timestamp.microsecond,
    )):
        timestamp -= timedelta(days=1)
    return timestamp.date().isoformat()


def _normalized_dedupe_text(value: str | None, max_length: int) -> str | None:
    """Normalize text for deduplication, removing HTML tags and non-alphanumeric characters."""
    if not value:
        return None

    normalized = normalize_text_for_match(HTML_TAG_RE.sub(" ", unescape(value)))
    normalized = NON_ALPHANUMERIC_RE.sub(" ", normalized).strip()
    if not normalized:
        return None
    if len(normalized) <= max_length:
        return normalized

    trimmed = normalized[:max_length].rstrip()
    head, _sep, _tail = trimmed.rpartition(" ")
    return head or trimmed


def probability(value: str) -> float:
    """Argparse helper to read a probability value between 0 and 1."""
    import argparse

    parsed = float(value)
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("expected a number between 0 and 1")
    return parsed
