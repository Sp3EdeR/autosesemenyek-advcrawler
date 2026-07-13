from __future__ import annotations

import argparse

from .common import (
    SETTINGS_PATH,
    TRAINING_DATA_PATH,
    TRAINING_EVENTS_PATH,
    build_dedupe_fields,
    load_dedupe_modules,
    load_event_list,
    preprocess_events,
)


def configure_parser(parser: argparse.ArgumentParser) -> None:
    """Attach the train handler to its parser."""
    parser.set_defaults(handler=run)


def run(_args: argparse.Namespace) -> int:
    """Train the dedupe model from the repository training dataset."""
    dedupe_module, variables_module = load_dedupe_modules()
    training_records = preprocess_events(load_event_list(str(TRAINING_EVENTS_PATH)))
    deduper = dedupe_module.Dedupe(build_dedupe_fields(variables_module), num_cores=0, in_memory=True)
    deduper.classifier.n_jobs = 1

    if TRAINING_DATA_PATH.exists():
        with TRAINING_DATA_PATH.open("r", encoding="utf-8") as file:
            deduper.prepare_training(training_records, training_file=file)
    else:
        deduper.prepare_training(training_records)

    print(f"Loaded {len(training_records)} training records from {TRAINING_EVENTS_PATH}.")
    dedupe_module.console_label(deduper)
    deduper.train()

    TRAINING_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TRAINING_DATA_PATH.open("w", encoding="utf-8") as file:
        deduper.write_training(file)

    with SETTINGS_PATH.open("wb") as file:
        deduper.write_settings(file)

    print(f"Saved training data to {TRAINING_DATA_PATH}.")
    print(f"Saved dedupe settings to {SETTINGS_PATH}.")
    return 0