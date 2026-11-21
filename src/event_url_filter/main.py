"""Remove Google events whose Facebook event IDs occur in the current event feed."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlsplit


def facebook_event_id(url: str) -> str | None:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    if re.search(r"(?:^|\.)(facebook|fb)\.com$", host) is None:
        return None
    m = re.search(r"^/events/(?:[^/]+/)*?(\d{5,})/?", parsed.path)
    return m.group(1) if m else None


def filter_events(
    events: list[dict[str, dict[str, str]]], current_feed: str
) -> list[dict[str, dict[str, str]]]:
    event_ids = {
        event_id
        for row in events
        for event in row.values()
        if (event_id := facebook_event_id(event["url"])) is not None
    }
    matched_ids: set[str] = set()
    if event_ids:
        ids = "|".join(sorted(event_ids))
        pattern = re.compile(
            r"https?://(?:\w+\.)*(facebook|fb)\.com/(?:[^/]+/)*?"
            rf'(?P<event_id>{ids})\b',
            re.IGNORECASE,
        )
        # Standard JSON leaves slashes intact; also accept optional JSON slash escaping.
        matched_ids = {
            match["event_id"] for match in pattern.finditer(current_feed.replace(r"\/", "/"))
        }
    return [
        row for row in events
        if all(facebook_event_id(event["url"]) not in matched_ids for event in row.values())
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_json")
    parser.add_argument("-f", "--filter", dest="filter_json", required=True)
    parser.add_argument("-o", "--output", dest="output_json", required=True)
    args = parser.parse_args()

    events = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    current_feed = Path(args.filter_json).read_text(encoding="utf-8")
    filtered_events = filter_events(events, current_feed)
    print(f"Filtered {len(events)} events into {len(filtered_events)} output events.")

    output = Path(args.output_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(filtered_events, ensure_ascii=False, sort_keys=True), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
