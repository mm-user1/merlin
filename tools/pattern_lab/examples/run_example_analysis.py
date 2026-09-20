"""Run a study, then analyze it, then read the saved analysis tables.

This is the agent-facing M3a path the CLI also uses: one completed event study,
one offline analysis of it, and the documented access to the saved comparison,
stratum and daily tables.  Nothing here executes a hypothesis module, a saved
Python snapshot or a metric plugin.

Run it from the repository root with your own verified pack:

    python -m tools.pattern_lab.examples.run_example_analysis \\
        --data-root docs/_work/pattern-lab-data \\
        --study-root /tmp/pattern-lab-two-green-pair \\
        --analysis-root /tmp/pattern-lab-two-green-analysis \\
        --instrument OKX_LINK-USDT-SWAP \\
        --start 2025-07-01T00:00:00Z --end 2026-07-01T00:00:00Z \\
        --warmup-start 2025-06-01T00:00:00Z

Nothing is downloaded, no ignored local artifact is assumed to exist, and both
roots must be new directories.  A short study publishes complete descriptive
results with explicit unsupported-inference reasons: the near-year admission
window is deliberate, not a failure.

The study step accepts ``--workers`` above 1, which starts an explicit spawn
pool, so this file stays an importable module whose work is behind the
``if __name__ == "__main__"`` guard below.  The analysis itself is a
coordinator-side vectorized calculation and takes no worker count.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from tools.pattern_lab import PatternLabError
from tools.pattern_lab import analysis as pack_analysis
from tools.pattern_lab import study as pack_study

EXAMPLES_DIR = Path(__file__).resolve().parent
CONFIGS_DIR = EXAMPLES_DIR.parent / "configs"
PROTOCOL_PATH = CONFIGS_DIR / "protocol_development_v1.json"
ANALYSIS_REQUEST_PATH = CONFIGS_DIR / "example_analysis_two_green_pair.json"


def build_study_request(
    *, instruments: list[str], timeframe: int, start: str, end: str, warmup_start: str
) -> dict:
    """The source study: both two-green variants, both directions, four horizons."""
    return {
        "schema_version": 1,
        "study_name": "Two green candles, with and without the rising-volume filter",
        "notes": "Tracked M3a example; it works in a fresh clone once the caller supplies a pack.",
        "protocol": str(PROTOCOL_PATH),
        "study": {"start_utc": start, "end_utc": end, "warmup_start_utc": warmup_start},
        "instruments": {"ids": instruments},
        "timeframes_minutes": [timeframe],
        "hypotheses": [
            {
                "id": "two_green_volume",
                "hypothesis": "two_green_rising_quote_volume",
                "parameters": {},
                "occurrence": "every_qualifying_bar",
            },
            {
                "id": "two_green_plain",
                "hypothesis": "two_green",
                "parameters": {},
                "occurrence": "every_qualifying_bar",
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
                            "horizons_minutes": [
                                timeframe * 2, timeframe * 4, timeframe * 8, timeframe * 16
                            ],
                            "primary_horizon_minutes": timeframe * 8,
                        }
                    },
                },
            }
        ],
        "metrics": [],
        "extensions": [],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--study-root", type=Path, required=True, metavar="NEW_RUN")
    parser.add_argument("--analysis-root", type=Path, required=True, metavar="NEW_ANALYSIS")
    parser.add_argument("--instrument", action="append", required=True, dest="instruments")
    parser.add_argument("--timeframe-minutes", type=int, default=30)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--warmup-start", required=True)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args(argv)

    if not Path(args.data_root).is_dir():
        print(
            f"pattern-lab: the requested market-data pack {args.data_root} does not exist. Supply "
            "an existing verified pack with --data-root; this example never downloads data and "
            "never falls back to synthetic prices.",
            file=sys.stderr,
        )
        return 2
    try:
        study = pack_study.run_study(
            request=build_study_request(
                instruments=list(args.instruments),
                timeframe=args.timeframe_minutes,
                start=args.start,
                end=args.end,
                warmup_start=args.warmup_start,
            ),
            data_root=args.data_root,
            output_root=args.study_root,
            workers=args.workers,
        )
        analysis = pack_analysis.run_analysis(
            request=ANALYSIS_REQUEST_PATH,
            run_root=study["run_root"],
            output_root=args.analysis_root,
        )
    except PatternLabError as exc:
        print(f"pattern-lab: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(analysis, indent=2, sort_keys=True))

    # Reading the saved tables needs only the sealed analysis artifact.
    saved = pack_analysis.load_analysis(analysis["analysis_root"])
    comparisons = saved.comparisons()
    strata = saved.strata()
    daily = saved.daily()
    print(
        f"pattern-lab: {len(comparisons)} family member(s), "
        f"{int(comparisons['inference_available'].sum())} with inference; "
        f"{len(strata)} stratum row(s), {int(strata['retained'].sum())} retained; "
        f"{len(daily)} daily row(s). Report at {analysis['report']}.",
        file=sys.stderr,
    )
    print(
        "pattern-lab: inference is an approximate development screen. A Holm rejection here is "
        "nominal, not a validated edge.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
