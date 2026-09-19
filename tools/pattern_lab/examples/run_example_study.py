"""Run a small Pattern Lab study through the public Python API.

This is the agent-facing path the CLI also uses.  It demonstrates a declared
trusted extension end to end: a causal SMA feature, a ``close_above_sma``
hypothesis, an axis-free custom model and a custom summary metric, alongside the
built-in condition and the built-in fixed-horizon/path model.

Run it from the repository root with your own verified pack:

    python -m tools.pattern_lab.examples.run_example_study \\
        --data-root docs/_work/pattern-lab-data \\
        --output-root /tmp/pattern-lab-example-run \\
        --instrument OKX_LINK-USDT-SWAP \\
        --start 2025-07-01T00:00:00Z --end 2025-08-01T00:00:00Z \\
        --warmup-start 2025-06-01T00:00:00Z \\
        --workers 1

Nothing is downloaded, no ignored local artifact is assumed to exist, and the
output root must be a new directory.

``--workers`` above 1 starts an explicit spawn pool, so this file must stay an
importable module whose work is behind the ``if __name__ == "__main__"`` guard
below: each spawned child re-imports it, and unguarded top-level work would run
again in every child.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from tools.pattern_lab import PatternLabError
from tools.pattern_lab import study as pack_study

EXAMPLES_DIR = Path(__file__).resolve().parent
CONFIGS_DIR = EXAMPLES_DIR.parent / "configs"
PROTOCOL_PATH = CONFIGS_DIR / "protocol_development_v1.json"


def build_request(
    *, instruments: list[str], timeframe: int, start: str, end: str, warmup_start: str
) -> dict:
    """Build the study request this example runs, with explicit absolute paths."""
    return {
        "schema_version": 1,
        "study_name": "Example: built-in and custom extensions side by side",
        "notes": "Tracked example; it works in a fresh clone once the caller supplies a valid pack.",
        "protocol": str(PROTOCOL_PATH),
        "study": {"start_utc": start, "end_utc": end, "warmup_start_utc": warmup_start},
        "instruments": {"ids": instruments},
        "timeframes_minutes": [timeframe],
        "hypotheses": [
            {
                "id": "two_green_every_bar",
                "hypothesis": "two_green_rising_quote_volume",
                "parameters": {},
                "occurrence": "every_qualifying_bar",
            },
            {
                "id": "close_above_sma_state_entry",
                "hypothesis": "example_close_above_sma",
                "parameters": {"period": 20},
                "occurrence": "state_entry",
            },
        ],
        "models": [
            {
                "id": "fixed_horizon",
                "model": "fixed_horizon_path",
                "settings": {
                    "directions": ["long", "short"],
                    "commission_pct_per_side": 0.05,
                    "by_timeframe": {
                        str(timeframe): {
                            "horizons_minutes": [timeframe * 2, timeframe * 4],
                            "primary_horizon_minutes": timeframe * 4,
                        }
                    },
                },
            },
            {"id": "open_gap", "model": "example_next_open_gap", "settings": {}},
        ],
        "metrics": [{"id": "positive_net_share", "metric": "example_positive_net_share"}],
        "extensions": [
            {"module": "custom_extension", "source_root": str(EXAMPLES_DIR), "helpers": []}
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--instrument", action="append", required=True, dest="instruments")
    parser.add_argument("--timeframe-minutes", type=int, default=30)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--warmup-start", required=True)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "Positive instrument worker count (default 1). Above 1 the jobs run in an explicit "
            "spawn pool bounded by the number of selected instruments."
        ),
    )
    args = parser.parse_args(argv)

    if not Path(args.data_root).is_dir():
        print(
            f"pattern-lab: the requested market-data pack {args.data_root} does not exist. Supply an "
            "existing verified pack with --data-root; this example never downloads data and never "
            "falls back to synthetic prices.",
            file=sys.stderr,
        )
        return 2
    request = build_request(
        instruments=list(args.instruments),
        timeframe=args.timeframe_minutes,
        start=args.start,
        end=args.end,
        warmup_start=args.warmup_start,
    )
    try:
        result = pack_study.run_study(
            request=request,
            data_root=args.data_root,
            output_root=args.output_root,
            workers=args.workers,
        )
    except PatternLabError as exc:
        print(f"pattern-lab: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))

    results = pack_study.load_results(result["run_root"])
    summary = pack_study.summarize_results(results)
    print(
        f"pattern-lab: {len(summary['groups'])} declared outcome group(s); "
        f"report at {result['report']}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
