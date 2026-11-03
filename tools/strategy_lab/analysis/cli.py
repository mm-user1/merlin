"""Command-line entry point for safe Strategy Lab analysis."""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import json
import sys
from pathlib import Path
from typing import Sequence

from .dataset import AnalysisError, open_dataset
from .allocation import DatasetInput, evaluate_allocation
from .evaluate import evaluate_scope
from .output import write_allocation, write_analysis
from .research_inputs import load_universe
from .candidate_policy import load_candidate_policy


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m tools.strategy_lab.analysis.cli")
    commands = parser.add_subparsers(dest="command", required=True)
    analyze = commands.add_parser("analyze", help="evaluate a frozen Strategy Lab scope")
    analyze.add_argument("--dataset", type=Path, required=True)
    analyze.add_argument("--scope", default="development")
    analyze.add_argument("--output", type=Path, required=True)
    analyze.add_argument("--allow-partial-scope", action="store_true")
    analyze.add_argument("--allow-incomplete-dataset", action="store_true")
    analyze.add_argument("--unlock-scope", action="store_true")
    analyze.add_argument("--policy", type=Path)
    analyze.add_argument("--ticker-list", type=Path)
    analyze.add_argument("--candidate-policy", type=Path)
    allocate = commands.add_parser(
        "allocate", help="evaluate fixed-capacity allocation over aligned datasets"
    )
    allocate.add_argument("--dataset", action="append", required=True)
    allocate.add_argument("--scope", default="development")
    selection = allocate.add_mutually_exclusive_group(required=True)
    selection.add_argument("--rule")
    selection.add_argument("--candidate-policy", type=Path)
    allocate.add_argument("--ticker-list", type=Path)
    allocate.add_argument("--ticker-list-mode", choices=("strict", "intersection"))
    allocate.add_argument("--primary-k", type=int, default=6)
    allocate.add_argument("--sensitivity-k", type=int, default=8)
    allocate.add_argument("--output", type=Path, required=True)
    allocate.add_argument("--allow-partial-scope", action="store_true")
    allocate.add_argument("--allow-incomplete-dataset", action="store_true")
    allocate.add_argument("--unlock-scope", action="store_true")
    allocate.add_argument("--policy", type=Path)
    audit = commands.add_parser("field-audit", parents=[], help="audit the saved candidate field")
    audit.add_argument("--dataset", type=Path, required=True)
    audit.add_argument("--scope", required=True)
    audit.add_argument("--output", type=Path, required=True)
    audit.add_argument("--allow-partial-scope", action="store_true")
    audit.add_argument("--allow-incomplete-dataset", action="store_true")
    audit.add_argument("--unlock-scope", action="store_true")
    audit.add_argument("--policy", type=Path)
    audit.add_argument("--ticker-list", type=Path)
    audit.add_argument("--candidate-policy", type=Path)
    screen = commands.add_parser("screen", help="compare saved IS/OOS allocation results")
    screen.add_argument("--dataset", action="append", required=True)
    screen.add_argument("--scope", required=True)
    screen.add_argument("--scope-for", action="append", default=[])
    screen.add_argument("--unlock-policy", action="append", default=[])
    screen.add_argument("--unlock-scope", action="store_true")
    screen.add_argument("--allow-partial-scope", action="store_true")
    screen.add_argument("--allow-incomplete-dataset", action="store_true")
    screen.add_argument("--ticker-list", type=Path)
    screen.add_argument("--ticker-list-mode", choices=("strict", "intersection"))
    select = screen.add_mutually_exclusive_group(required=True)
    select.add_argument("--rule")
    select.add_argument("--candidate-policy", type=Path)
    base = screen.add_mutually_exclusive_group()
    base.add_argument("--baseline")
    base.add_argument("--filter-baseline")
    screen.add_argument("--primary-k", type=int, default=6)
    screen.add_argument("--sensitivity-k", type=int, default=8)
    screen.add_argument("--output", type=Path, required=True)
    return parser


def _dataset_argument(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise AnalysisError("--dataset must use the explicit label=path form.")
    label, raw_path = value.split("=", 1)
    if not label or not raw_path:
        raise AnalysisError("--dataset requires a non-empty label and path.")
    return label, Path(raw_path)


def _assignments(values, labels):
    result = {}
    for value in values:
        if "=" not in value:
            raise AnalysisError("label assignment requires LABEL=VALUE.")
        label, content = value.split("=", 1)
        if label not in labels or label in result or not content:
            raise AnalysisError("unknown, duplicate or empty label assignment.")
        result[label] = content
    return result


def _run(args):
    from .field_audit import evaluate_field_audit, write_field_audit
    from .screen import evaluate_screen, write_screen
    from ..report_support import labels
    try:
        if args.command in ("analyze", "field-audit"):
            dataset = open_dataset(
                args.dataset, allow_incomplete=args.allow_incomplete_dataset
            )
            scope = dataset.resolve_scope(
                args.scope,
                allow_partial=args.allow_partial_scope,
                unlock=args.unlock_scope,
                policy_path=args.policy,
            )
            evaluate = evaluate_scope if args.command == "analyze" else evaluate_field_audit
            result = evaluate(dataset, scope,
                universe=load_universe(args.ticker_list) if args.ticker_list else None,
                candidate_policy=load_candidate_policy(args.candidate_policy) if args.candidate_policy else None)
            publication = write_analysis(result, args.output, dataset_root=dataset.root) if args.command == "analyze" else write_field_audit(result, args.output)
        else:
            labelled = tuple(_dataset_argument(value) for value in args.dataset)
            if len({label for label, _ in labelled}) != len(labelled):
                raise AnalysisError("dataset labels must be unique.")
            if args.command == "screen":
                labels(labelled)
            scope_for = _assignments(args.scope_for, dict(labelled)) if args.command == "screen" else {}
            unlock_for = _assignments(args.unlock_policy, dict(labelled)) if args.command == "screen" else {}
            inputs = []
            for label, path in labelled:
                dataset = open_dataset(
                    path, allow_incomplete=args.allow_incomplete_dataset
                )
                scope = dataset.resolve_scope(
                    scope_for.get(label, args.scope),
                    allow_partial=args.allow_partial_scope,
                    unlock=args.unlock_scope,
                    policy_path=Path(unlock_for[label]) if label in unlock_for else getattr(args, "policy", None),
                )
                inputs.append(DatasetInput(label, dataset, scope))
            evaluate = evaluate_screen if args.command == "screen" else evaluate_allocation
            result = evaluate(
                inputs,
                candidate_rule=args.rule,
                candidate_policy=load_candidate_policy(args.candidate_policy) if args.candidate_policy else None,
                universe=load_universe(args.ticker_list) if args.ticker_list else None,
                ticker_list_mode=args.ticker_list_mode,
                primary_k=args.primary_k,
                sensitivity_k=args.sensitivity_k,
                **(dict(baseline=args.baseline, filter_baseline=args.filter_baseline) if args.command == "screen" else {}),
            )
            publication = write_screen(result, args.output) if args.command == "screen" else write_allocation(
                result,
                args.output,
                dataset_roots=tuple(item.dataset.root for item in inputs),
            )
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(f"Strategy Lab analysis error: {exc}", file=sys.stderr)
        return None
    return publication


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    with redirect_stdout(sys.stderr):
        publication = _run(args)
    if publication is None:
        return 2
    print(json.dumps(publication, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
