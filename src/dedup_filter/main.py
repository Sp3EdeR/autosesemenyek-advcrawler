"""Filter JSON events by keys not containing the `autosesemenyek` marker."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json")
    parser.add_argument("output_json")
    args = parser.parse_args()

    input_path = Path(args.input_json)
    output_path = Path(args.output_json)

    with input_path.open("r", encoding="utf-8") as input_file:
        events = json.load(input_file)

    filtered_events = [
        event for event in events if all("autosesemenyek" not in key for key in event.keys())
    ]

    print(f"Filtered {len(events)} events into {len(filtered_events)} output events.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(filtered_events, output_file, ensure_ascii=False, sort_keys=True)


if __name__ == "__main__":
    main()
