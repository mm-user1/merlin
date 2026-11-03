"""Command-line entry point for safe Strategy Lab analysis."""

from __future__ import annotations

import argparse
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
    return parser


def _dataset_argument(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise AnalysisError("--dataset must use the explicit label=path form.")
    label, raw_path = value.split("=", 1)
    if not label or not raw_path:
        raise AnalysisError("--dataset requires a non-empty label and path.")
    return label, Path(raw_path)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "analyze":
            dataset = open_dataset(
                args.dataset, allow_incomplete=args.allow_incomplete_dataset
            )
            scope = dataset.resolve_scope(
                args.scope,
                allow_partial=args.allow_partial_scope,
                unlock=args.unlock_scope,
                policy_path=args.policy,
            )
            result = evaluate_scope(dataset, scope,
                universe=load_universe(args.ticker_list) if args.ticker_list else None,
                candidate_policy=load_candidate_policy(args.candidate_policy) if args.candidate_policy else None)
            publication = write_analysis(result, args.output, dataset_root=dataset.root)
        else:
            labelled = tuple(_dataset_argument(value) for value in args.dataset)
            if len({label for label, _ in labelled}) != len(labelled):
                raise AnalysisError("dataset labels must be unique.")
            inputs = []
            for label, path in labelled:
                dataset = open_dataset(
                    path, allow_incomplete=args.allow_incomplete_dataset
                )
                scope = dataset.resolve_scope(
                    args.scope,
                    allow_partial=args.allow_partial_scope,
                    unlock=args.unlock_scope,
                    policy_path=args.policy,
                )
                inputs.append(DatasetInput(label, dataset, scope))
            result = evaluate_allocation(
                inputs,
                candidate_rule=args.rule,
                candidate_policy=load_candidate_policy(args.candidate_policy) if args.candidate_policy else None,
                universe=load_universe(args.ticker_list) if args.ticker_list else None,
                ticker_list_mode=args.ticker_list_mode,
                primary_k=args.primary_k,
                sensitivity_k=args.sensitivity_k,
            )
            publication = write_allocation(
                result,
                args.output,
                dataset_roots=tuple(item.dataset.root for item in inputs),
            )
    except AnalysisError as exc:
        print(f"Strategy Lab analysis error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(publication, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
