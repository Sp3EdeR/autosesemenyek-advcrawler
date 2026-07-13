from __future__ import annotations

import importlib
import json
import re
from urllib import request
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from utilities import get_first_town_norm, normalize_text_for_match

Record = dict[str, Any]
WrappedEvent = dict[str, dict[str, Any]]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEDUPE_DIR = PROJECT_ROOT / "dedupe"
TRAINING_EVENTS_PATH = DEDUPE_DIR / "training_events.json"
TRAINING_DATA_PATH = DEDUPE_DIR / "training_data.json"
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


def build_dedupe_fields(variables_module: Any) -> list[Any]:
    """Return the field configuration used for event deduplication."""
    import datetimetype

    return [
        variables_module.Text("summary_norm"),
        datetimetype.DateTime("dtstart_norm"),
        datetimetype.DateTime("dtend_norm", has_missing=True),
        variables_module.Text("description_norm", has_missing=True),
        variables_module.String("location_norm", has_missing=True),
        variables_module.ShortString("town_norm", has_missing=True),
    ]


def load_event_list(path: str) -> list[WrappedEvent]:
    """Load a wrapped event list from JSON."""
    if re.match(r"^https?://", path):
        with request.urlopen(path) as response:
            return json.load(response)
    with open(path, "r", encoding="utf-8") as file:
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
            processed["dtstart_norm"] = processed["dtstart"][:10]
            processed["dtend_norm"] = processed["dtend"][:10] if "dtend" in processed else None
            desc = processed.get("description")
            processed["description_norm"] = _normalized_dedupe_text(
                desc, DESCRIPTION_NORM_MAX_LEN
            )
            loc = processed.get("location")
            processed["location_norm"] = _normalized_dedupe_text(loc, LOCATION_NORM_MAX_LEN)
            town_sources = " ".join(filter(None, [loc, processed["summary"], desc]))
            processed["town_norm"] = get_first_town_norm(town_sources)
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
        if field == "src" or field.endswith("_norm"):
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


def _normalized_dedupe_text(value: str | None, max_length: int) -> str | None:
    if not value:
        return None

    normalized = HTML_TAG_RE.sub("", value)

    normalized = normalize_text_for_match(normalized)
    if len(normalized) <= max_length:
        return normalized

    trimmed = normalized[:max_length].rstrip()
    head, _sep, _tail = trimmed.rpartition(" ")
    return head or trimmed
