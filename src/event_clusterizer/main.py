from __future__ import annotations

import argparse
from collections.abc import Sequence

from . import process_command, train_command


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for event deduplication tasks."""
    parser = argparse.ArgumentParser(
        description="Train and run the event deduplication model.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    train_parser = subparsers.add_parser(
        "train",
        help="train the dedupe model from dedupe/training_events.json",
    )
    train_command.configure_parser(train_parser)

    process_parser = subparsers.add_parser(
        "process",
        help="deduplicate one or more event JSON files",
    )
    process_command.configure_parser(process_parser)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the event clusterizer CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
