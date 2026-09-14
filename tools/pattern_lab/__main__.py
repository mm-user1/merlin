"""Pattern Lab data command line.

The CLI calls exactly the Python API that later researcher scripts use.  JSON
goes to stdout, diagnostics and progress go to stderr.  Exit status: 0 success,
1 verification problems or a reported tail shortfall, 2 an invalid request,
invalid data, a source failure or a missing dependency, 3 a busy pack, 4 a valid
pending operation that must be recovered or aborted first.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Sequence

from . import (
    PatternLabBusyError,
    PatternLabDependencyError,
    PatternLabError,
    PatternLabPendingError,
)
from . import collect as pack_collect
from . import data as pack_data
from . import exchange_data
from . import manifest as pack_manifest
from .import_npz import import_npz_pack, load_source_metadata

EXIT_OK = 0
EXIT_VERIFICATION_PROBLEMS = 1
EXIT_ERROR = 2
EXIT_BUSY = 3
EXIT_PENDING = 4

# Collector operations report a failure as structured JSON on stdout as well as
# an actionable stderr explanation; the older archival commands keep their
# stderr-only error behavior.
COLLECTOR_COMMANDS = ("collect", "update", "recover", "abort-update")


def _add_http_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--okx-rps",
        type=float,
        default=exchange_data.DEFAULT_REQUESTS_PER_SECOND,
        metavar="RATE",
        help=(
            "OKX pacing in requests per second, retries and metadata included "
            f"(default {exchange_data.DEFAULT_REQUESTS_PER_SECOND}, ceiling "
            f"{exchange_data.MAX_REQUESTS_PER_SECOND})."
        ),
    )
    parser.add_argument(
        "--bybit-rps",
        type=float,
        default=exchange_data.DEFAULT_REQUESTS_PER_SECOND,
        metavar="RATE",
        help=(
            "Bybit pacing in requests per second, retries and metadata included "
            f"(default {exchange_data.DEFAULT_REQUESTS_PER_SECOND}, ceiling "
            f"{exchange_data.MAX_REQUESTS_PER_SECOND})."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=exchange_data.DEFAULT_TIMEOUT_SECONDS,
        metavar="SECONDS",
        help=f"Per-request timeout (default {exchange_data.DEFAULT_TIMEOUT_SECONDS}).",
    )
    parser.add_argument("--note", default=None, help="Optional note stored in the update history.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.pattern_lab",
        description=(
            "Pattern Lab data foundation: collect and update a Parquet market-data pack from public "
            "exchange APIs, recover an interrupted operation, import the historical NPZ pack, inspect "
            "a pack and read a fixed UTC interval. Hypothesis, bracket and report commands are future "
            "work and are not implemented here."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="command")

    collector = commands.add_parser(
        "collect",
        help="Collect a NEW pack for an explicit roster from the public exchange APIs.",
        description=(
            "The destination must be absent, empty, or contain nothing except the persistent "
            ".pack-lock file; its parent must already exist. Times are timezone-aware UTC values on "
            "the 5m grid; the interval is half-open [start, end). 'latest-closed' resolves once to a "
            "concrete safe closed end and never enters stored metadata."
        ),
    )
    collector.add_argument(
        "--universe",
        type=Path,
        required=True,
        metavar="JSON",
        help=f"Roster configuration; the tracked default is {pack_collect.DEFAULT_ROSTER_PATH}.",
    )
    collector.add_argument("--data-root", type=Path, required=True, metavar="NEW_PATH")
    collector.add_argument("--start", required=True, metavar="UTC")
    collector.add_argument("--end", required=True, metavar=f"UTC_OR_{pack_collect.LATEST_CLOSED}")
    _add_http_options(collector)

    updater = commands.add_parser(
        "update",
        help="Append newly closed bars, and optionally an earlier prefix, to a managed pack.",
        description=(
            "The roster comes from the pack's own collector metadata: an update never adds, drops or "
            "relabels an instrument, and never adopts an archival NPZ-imported pack. An optional "
            "earlier --start adds missing prefix history; a later start never trims stored data and an "
            "older end never truncates the tail."
        ),
    )
    updater.add_argument("--data-root", type=Path, required=True, metavar="PATH")
    updater.add_argument("--end", required=True, metavar=f"UTC_OR_{pack_collect.LATEST_CLOSED}")
    updater.add_argument("--start", default=None, metavar="EARLIER_UTC")
    _add_http_options(updater)

    recoverer = commands.add_parser(
        "recover",
        help="Finish an interrupted collect or update from its recorded journal.",
        description=(
            "Completed staged instruments are reused by digest; only an unfinished instrument is "
            "downloaded again. Once the applying phase has been recorded, recovery finishes entirely "
            "from the staged artifacts and never re-downloads."
        ),
    )
    recoverer.add_argument("--data-root", type=Path, required=True, metavar="PATH")

    aborter = commands.add_parser(
        "abort-update",
        help="Discard a staging operation, leaving the existing pack byte-for-byte unchanged.",
        description=(
            "Allowed only before the applying phase. The data root and its persistent .pack-lock are "
            "always retained, so an aborted initial collect leaves a reusable lock-only root."
        ),
    )
    aborter.add_argument("--data-root", type=Path, required=True, metavar="PATH")

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
            "manifest. It is an integrity check and never promotes unknown evidence. A pending "
            "operation is reported before a manifest is required, and never certified."
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


def _http_options(args: argparse.Namespace) -> dict:
    return {
        "okx_rps": args.okx_rps,
        "bybit_rps": args.bybit_rps,
        "timeout_seconds": args.timeout,
    }


def _collector_status(result) -> int:
    """A published pack does not mean the requested range is complete."""
    shortfall = result.get("tail_shortfall_bars", 0)
    if shortfall:
        print(
            f"pattern-lab: {shortfall} requested 5m bar(s) are missing from the tail of "
            f"{len(result['instruments_with_tail_shortfall'])} instrument(s); the pack is published "
            "but the requested range is not complete.",
            file=sys.stderr,
        )
        return EXIT_VERIFICATION_PROBLEMS
    return EXIT_OK


def _run(args: argparse.Namespace) -> int:
    # Every data command needs the pinned reader; report the dependency once, up front.
    pack_data.require_pyarrow()
    progress = pack_collect.stderr_progress
    if args.command == "collect":
        result = pack_collect.collect_pack(
            args.data_root,
            start=args.start,
            end=args.end,
            roster_path=args.universe,
            options=_http_options(args),
            progress=progress,
            note=args.note,
        )
        _emit(result)
        return _collector_status(result)
    if args.command == "update":
        result = pack_collect.update_pack(
            args.data_root,
            end=args.end,
            start=args.start,
            options=_http_options(args),
            progress=progress,
            note=args.note,
        )
        _emit(result)
        return _collector_status(result)
    if args.command == "recover":
        result = pack_collect.recover_pack(args.data_root, progress=progress)
        _emit(result)
        if result["status"] == "nothing_to_recover":
            return EXIT_OK
        return _collector_status(result)
    if args.command == "abort-update":
        _emit(pack_collect.abort_update(args.data_root, progress=progress))
        return EXIT_OK
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


def _report_failure(command: str, exc: PatternLabError, label: str) -> None:
    """Explain a failure on stderr, and add structured JSON for collector commands."""
    if command in COLLECTOR_COMMANDS:
        _emit({"status": "failed", "command": command, "error_code": exc.error_code, "error": str(exc)})
    print(f"pattern-lab: {label}{exc}", file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = getattr(args, "command", "")
    try:
        return _run(args)
    except PatternLabDependencyError as exc:
        _report_failure(command, exc, "missing dependency: ")
        return EXIT_ERROR
    except PatternLabBusyError as exc:
        _report_failure(command, exc, "")
        return EXIT_BUSY
    except PatternLabPendingError as exc:
        _report_failure(command, exc, "")
        return EXIT_PENDING
    except PatternLabError as exc:
        _report_failure(command, exc, "")
        return EXIT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
