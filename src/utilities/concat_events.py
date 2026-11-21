"""Concatenate JSON event arrays in input order without deduplication."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_json", nargs="+")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()

    events: list[dict] = []
    for filename in args.input_json:
        rows = json.loads(Path(filename).read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError(f"Expected a JSON event array in {filename}")
        events.extend(rows)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(events, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(f"Concatenated {len(args.input_json)} files into {len(events)} output events.")


if __name__ == "__main__":
    main()
