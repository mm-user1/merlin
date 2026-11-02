"""Validate, run or report a continuous Fixed research series."""
import argparse
import json
from pathlib import Path
import sys

from .config import load


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate")
    validate.add_argument("request", type=Path)
    execute = sub.add_parser("run")
    execute.add_argument("request", type=Path)
    execute.add_argument("--data-root", type=Path, required=True)
    execute.add_argument("--output-dir", type=Path, required=True)
    execute.add_argument("--resume", action="store_true")
    execute.add_argument("--preflight-only", action="store_true")
    view = sub.add_parser("report")
    view.add_argument("--dataset", type=Path, required=True)
    view.add_argument("--output-dir", type=Path, required=True)
    view.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command in ("validate", "run"):
            raw, _, resolved = load(args.request)
            g = raw["generation"]
            print(json.dumps({"expected_run_count":g["expected_run_count"], "resolved":resolved,
                "periods":g["periods"], "timeframes_minutes":g["timeframes_minutes"], "outer_workers":1,
                "date_policy":"inclusive UTC bar-open End; date-only includes the entire day; full target-bar warmup 100..5000",
                "source_admission":"requires_preflight"}, allow_nan=False))
        if args.command == "run":
            from .run import run
            print(json.dumps(run(args.request, args.data_root, args.output_dir, resume=args.resume, preflight_only=args.preflight_only)))
        elif args.command == "report":
            from .report import report
            print(json.dumps(report(args.dataset, args.output_dir, allow_partial=args.allow_partial)))
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        print(f"Fixed {args.command} failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
