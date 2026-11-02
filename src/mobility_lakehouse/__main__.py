"""Command line interface; errors go to stderr with a nonzero exit status."""
import argparse
import json
import logging
from pathlib import Path
import sys

from .pipeline import fetch, ingest, report


def main(argv=None):
    parser = argparse.ArgumentParser(description="TLC immutable-source monthly lakehouse (local execution)")
    commands = parser.add_subparsers(dest="command", required=True)
    ingest_parser = commands.add_parser("ingest", help="Retain and load one reviewed monthly Parquet file")
    ingest_parser.add_argument("--file", type=Path, required=True)
    ingest_parser.add_argument("--month", required=True)
    ingest_parser.add_argument("--source-url")
    ingest_parser.add_argument("--allow-revert", action="store_true", help="Explicitly reactivate a historical source SHA")
    fetch_parser = commands.add_parser("fetch", help="Fetch one bounded historical TLC source file")
    fetch_parser.add_argument("--month", choices=["2024-01", "2024-02", "2024-03"], default="2024-01")
    report_parser = commands.add_parser("report", help="Recover/publish aggregate outputs from committed data")
    for command in (ingest_parser, fetch_parser, report_parser):
        command.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.command == "ingest":
            result = ingest(args.file, args.month, args.data_dir, args.source_url, allow_revert=args.allow_revert)
        elif args.command == "fetch":
            result = fetch(args.month, args.data_dir)
        else:
            result = report(args.data_dir)
        print(json.dumps(result, indent=2, allow_nan=False))
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
