from __future__ import annotations

import argparse
import io
import json
import random
from pathlib import Path

from .common import (
    SETTINGS_PATH,
    DEDUPE_DIR,
    Record,
    load_dedupe_modules,
    load_event_lists,
    preprocess_events,
)

LabeledPairs = dict[str, list[tuple[Record, Record]]]

TRAINING_EVENTS_PATH = DEDUPE_DIR / "training_events.json"
TRAINING_DATA_PATH = DEDUPE_DIR / "training_data.json"


def configure_parser(parser: argparse.ArgumentParser) -> None:
    """Attach the train handler to its parser."""
    parser.add_argument("input_files", nargs="*", default=[str(TRAINING_EVENTS_PATH)])
    parser.add_argument("--no-auto-pairing", action="store_true", help="skip the automatic pairing process")
    parser.add_argument("--no-manual-pairing", action="store_true", help="skip the manual pairing process")
    parser.add_argument("--training-data", type=Path, default=TRAINING_DATA_PATH)
    parser.add_argument("--training-output", type=Path, help="where to save combined labels (defaults to --training-data)")
    parser.add_argument("--settings", type=Path, default=SETTINGS_PATH)
    parser.add_argument("--seed", type=int, default=0, help="random seed for reproducible sampling")
    parser.set_defaults(handler=run)


def load_training_data(path: Path) -> LabeledPairs:
    """Read full-record dedupe labels and refresh normalized helper fields."""
    dedupe_module, _variables_module = load_dedupe_modules()
    with path.open("r", encoding="utf-8") as file:
        labels = dedupe_module.read_training(file)
    normalized: LabeledPairs = {"match": [], "distinct": []}
    for label in normalized:
        for pair in labels[label]:
            if len(pair) != 2:
                raise ValueError(f"Expected a pair in {path}: {pair!r}")
            refreshed = [preprocess_events([{record["src"]: record}])[0] for record in pair]
            normalized[label].append((refreshed[0], refreshed[1]))
    return normalized


def build_dedupe_fields(variables_module: Any) -> list[Any]:
    """Return the field configuration used for event deduplication."""
    import datetimetype

    return [
        variables_module.Text("summary_norm"),
        variables_module.String("summary_norm", name="summary_characters"),
        datetimetype.DateTime("dtstart_norm"),
        variables_module.Exact("dtstart_norm"),
        datetimetype.DateTime("dtend_norm", has_missing=True),
        variables_module.Text("description_norm", has_missing=True),
        variables_module.String("location_norm", has_missing=True),
        variables_module.Exact("town_norm", name="town", has_missing=True),
        # When town is geocoded, its quality is worse, so categorize it differently to weigh it
        variables_module.Categorical("town_quality", categories=["geocoded", "text"]),
        variables_module.Interaction("town", "(town_quality: Categorical)"),
    ]


def record_key(record: Record) -> str:
    """Identify a complete normalized record independently of dictionary ordering."""
    return json.dumps(record, sort_keys=True, ensure_ascii=True)


def select_pairs(records: dict[int, Record], labels: LabeledPairs) -> None:
    """Add or correct labels using positions in the normalized input event list."""
    print("Commands: list [title text], match ID ID, distinct ID ID, done")
    while True:
        parts = input("Pair selection> ").split()
        if not parts:
            continue
        action, *values = parts

        if action == "done" and not values:
            return

        if action == "list":
            query = " ".join(values).casefold()
            matches = [
                (record_id, record) for record_id, record in records.items()
                if query in record["summary"].casefold()
            ]
            matches.sort(key=lambda item: item[1].get("dtstart_norm") or "")
            for record_id, record in matches:
                line = f"{record_id}\t{record['dtstart_norm']}\t{record['summary']}"
                if location := record.get("location"):
                    line += f" | {location}"
                print(line)
            continue

        if (action not in labels or len(values) != 2
            or not all(value.isdecimal() for value in values)
        ):
            print("Use match ID ID or distinct ID ID; list [title text] shows event IDs.")
            continue

        # Add or update the label for the selected pair
        left, right = map(int, values)
        if left == right or left not in records or right not in records:
            print("Choose two different event IDs from the input list.")
            continue
        pair = (records[left], records[right])
        key = sorted(map(record_key, pair))
        for label in labels:
            labels[label] = [
                saved for saved in labels[label] if sorted(map(record_key, saved)) != key
            ]
        labels[action].append(pair)
        print(f"Recorded {action}: {left}, {right}.")


def run(args: argparse.Namespace) -> int:
    """Train the dedupe model from the repository training dataset."""
    import numpy as np
    random.seed(args.seed)
    np.random.seed(args.seed)

    dedupe_module, variables_module = load_dedupe_modules()
    training_records = preprocess_events(load_event_lists(args.input_files))
    deduper = dedupe_module.Dedupe(
        build_dedupe_fields(variables_module), num_cores=0, in_memory=True,
    )
    deduper.classifier.n_jobs = 1

    # Initialize existing training pre-existing training
    labels = { "match": [], "distinct": [] }
    if args.training_data.exists():
        labels = load_training_data(args.training_data)

    # Manually select matching or distinct event pairs if requested
    if not args.no_manual_pairing:
        select_pairs(training_records, labels)
    if args.no_auto_pairing and not all(labels.values()):
        raise ValueError("Provide matching and distinct examples when auto pairing is disabled.")

    known_records = {record_key(record) for record in training_records.values()}
    for pairs in labels.values():
        for pair in pairs:
            for record in pair:
                key = record_key(record)
                if key not in known_records:
                    training_records[len(training_records)] = record
                    known_records.add(key)

    if any(labels.values()):
        training_file = io.StringIO()
        dedupe_module.write_training(labels, training_file)
        training_file.seek(0)
        deduper.prepare_training(training_records, training_file=training_file)
    else:
        deduper.prepare_training(training_records)

    print(f"Loaded {len(training_records)} training records.")
    print(f"Loaded {len(labels['match'])} match and {len(labels['distinct'])} distinct pairs.")

    # Give the user an automatic pair training GUI if requested
    if not args.no_auto_pairing:
        dedupe_module.console_label(deduper)

    # Run model training
    deduper.train()

    training_output = args.training_output or args.training_data
    training_output.parent.mkdir(parents=True, exist_ok=True)
    with training_output.open("w", encoding="utf-8") as file:
        deduper.write_training(file)

    args.settings.parent.mkdir(parents=True, exist_ok=True)
    with args.settings.open("wb") as file:
        deduper.write_settings(file)

    print(f"Saved training data to {training_output}.")
    print(f"Saved dedupe settings to {args.settings}.")
    return 0