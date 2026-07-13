from __future__ import annotations

import argparse
from pathlib import Path

from .common import (
    SETTINGS_PATH,
    load_dedupe_modules,
    load_event_lists,
    merge_partitioned_records,
    preprocess_events,
    save_event_list,
)


def configure_parser(parser: argparse.ArgumentParser) -> None:
    """Define arguments for the process sub-command."""
    parser.add_argument("input_files", nargs="+", type=str, help="one or more input event JSON files")
    parser.add_argument("-o", "--output", default=Path("deduped.json"), type=Path, help="output JSON file path")
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Deduplicate input files and write the merged wrapped event list."""
    dedupe_module, _variables_module = load_dedupe_modules()
    records = preprocess_events(load_event_lists(args.input_files))

    with SETTINGS_PATH.open("rb") as file:
        deduper = dedupe_module.StaticDedupe(file, num_cores=0, in_memory=True)

    merged_events = merge_partitioned_records(records, deduper.partition(records))
    save_event_list(args.output, merged_events)

    print(f"Processed {len(records)} records into {len(merged_events)} output events.")
    print(f"Saved merged events to {args.output}.")
    return 0