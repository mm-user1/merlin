"""Pattern Lab data command line.

The CLI calls exactly the Python API that later researcher scripts use.  JSON
goes to stdout, diagnostics go to stderr.  Exit status: 0 success, 1 verification
problems found, 2 an invalid request, invalid data or a missing dependency.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Sequence

from . import PatternLabDependencyError, PatternLabError
from . import data as pack_data
from . import manifest as pack_manifest
from .import_npz import import_npz_pack, load_source_metadata

EXIT_OK = 0
EXIT_VERIFICATION_PROBLEMS = 1
EXIT_ERROR = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.pattern_lab",
        description=(
            "Pattern Lab data foundation: import the historical NPZ pack, inspect a Parquet "
            "pack and read a fixed UTC interval. Hypothesis, bracket and report commands are "
            "future work and are not implemented here."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="command")

    importer = commands.add_parser(
        "import-npz",
        help="Convert the prototype NPZ pack into a new Parquet pack.",
        description=(
            "One-way archival conversion. The output root must be new and must not overlap the "
            "source root. A source-metadata sidecar declaring quote-unit evidence is required."
        ),
    )
    importer.add_argument("--source-root", type=Path, required=True)
    importer.add_argument("--output-root", type=Path, required=True)
    importer.add_argument("--source-metadata", type=Path, required=True, metavar="JSON")
    importer.add_argument("--note", default=None, help="Optional note stored in the update history.")

    inspector = commands.add_parser(
        "inspect",
        help="Print read-only pack metadata, coverage and verification limitations.",
        description=(
            "--verify additionally checks file hashes, schema and actual coverage against the "
            "manifest. It is an integrity check and never promotes unknown evidence."
        ),
    )
    inspector.add_argument("--data-root", type=Path, required=True)
    inspector.add_argument("--verify", action="store_true")

    reader = commands.add_parser(
        "slice",
        help="Print the metadata, coverage and input fingerprint of a fixed interval.",
        description=(
            "Times must be timezone-aware ISO-8601 values. The interval is half-open "
            "[start, end); the consumed interval is [warmup-start, end). No rows are written or "
            "printed."
        ),
    )
    reader.add_argument("--data-root", type=Path, required=True)
    reader.add_argument("--instrument", required=True, metavar="ID")
    reader.add_argument("--start", required=True, metavar="UTC")
    reader.add_argument("--end", required=True, metavar="UTC")
    reader.add_argument("--warmup-start", default=None, metavar="UTC")
    reader.add_argument(
        "--timeframe-minutes", type=int, default=pack_manifest.BASE_TIMEFRAME_MINUTES, metavar="INT"
    )
    return parser


def _emit(payload) -> None:
    sys.stdout.write(pack_manifest.dumps_json(payload) + "\n")


def _run(args: argparse.Namespace) -> int:
    # Every data command needs the pinned reader; report the dependency once, up front.
    pack_data.require_pyarrow()
    if args.command == "import-npz":
        metadata = load_source_metadata(args.source_metadata)
        _emit(import_npz_pack(args.source_root, args.output_root, source_metadata=metadata, note=args.note))
        return EXIT_OK
    if args.command == "inspect":
        report = pack_data.inspect_pack(args.data_root, verify=args.verify)
        _emit(report)
        if args.verify and report["verification_check"]["problems"]:
            for problem in report["verification_check"]["problems"]:
                print(f"pattern-lab: verification problem: {problem}", file=sys.stderr)
            return EXIT_VERIFICATION_PROBLEMS
        return EXIT_OK
    if args.command == "slice":
        loaded = pack_data.load_slice(
            args.data_root,
            args.instrument,
            start=args.start,
            end=args.end,
            warmup_start=args.warmup_start,
            timeframe_minutes=args.timeframe_minutes,
        )
        _emit(pack_data.slice_metadata(loaded))
        return EXIT_OK
    raise AssertionError(f"unhandled command {args.command!r}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return _run(args)
    except PatternLabDependencyError as exc:
        print(f"pattern-lab: missing dependency: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except PatternLabError as exc:
        print(f"pattern-lab: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
