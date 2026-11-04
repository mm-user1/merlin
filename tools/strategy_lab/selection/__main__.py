"""Explicit file-based X preparation, Fixed transform and saved-outcome evaluation."""
import argparse
from contextlib import redirect_stdout
import json
import sys

from . import prepare_ranking, write_fixed_request, evaluate_selection, write_selection
from .ranking import require
from ..analysis import open_dataset, DatasetInput, load_candidate_policy


def labelled(values):
    result = {}
    for value in values or []:
        require("=" in value, "expected LABEL=VALUE.")
        name, path = value.split("=", 1)
        require(name and path and name not in result, "duplicate or empty label/value.")
        result[name] = path
    return result


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("request")
    for name in ("ticker-list", "data-root", "output-dir"):
        prepare.add_argument("--"+name, required=True)
    transform = sub.add_parser("fixed-request")
    for name in ("ranking", "template", "output-dir"):
        transform.add_argument("--"+name, required=True)
    transform.add_argument("--warmup-bars", type=int, required=True)
    evaluate = sub.add_parser("evaluate")
    evaluate.add_argument("--ranking", required=True)
    evaluate.add_argument("--output-dir", required=True)
    family = evaluate.add_mutually_exclusive_group(required=True)
    family.add_argument("--fixed-dataset", action="append")
    family.add_argument("--dataset", action="append")
    evaluate.add_argument("--parameter-set", action="append")
    evaluate.add_argument("--allow-partial", action="store_true")
    evaluate.add_argument("--baseline")
    evaluate.add_argument("--scope")
    evaluate.add_argument("--scope-for", action="append")
    selection = evaluate.add_mutually_exclusive_group()
    selection.add_argument("--rule")
    selection.add_argument("--candidate-policy")
    evaluate.add_argument("--unlock-scope", action="store_true")
    evaluate.add_argument("--unlock-policy", action="append")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        with redirect_stdout(sys.stderr):
            if args.command == "prepare":
                result = prepare_ranking(args.request, args.output_dir, ticker_list=args.ticker_list, data_root=args.data_root)
            elif args.command == "fixed-request":
                result = write_fixed_request(args.ranking, args.template, args.output_dir, warmup_bars=args.warmup_bars)
            elif args.fixed_dataset:
                require(not any((args.scope, args.scope_for, args.rule, args.candidate_policy, args.unlock_scope, args.unlock_policy)),
                    "candidate/scope options are irrelevant to Fixed inputs.")
                result = write_selection(evaluate_selection(args.ranking, fixed_inputs=labelled(args.fixed_dataset),
                    parameter_sets=labelled(args.parameter_set), allow_partial=args.allow_partial, baseline=args.baseline), args.output_dir)
            else:
                require(not args.parameter_set and not args.allow_partial, "parameter-set/allow-partial are Fixed-only.")
                paths, scopes, unlocks = labelled(args.dataset), labelled(args.scope_for), labelled(args.unlock_policy)
                require(not (set(scopes)|set(unlocks))-set(paths), "unknown scope/unlock label.")
                require(not unlocks or args.unlock_scope, "unlock-policy requires unlock-scope.")
                inputs = []
                for label, path in paths.items():
                    scope_name = scopes.get(label, args.scope)
                    require(scope_name is not None, "each candidate input requires an explicit scope.")
                    dataset = open_dataset(path)
                    scope = dataset.resolve_scope(scope_name, unlock=args.unlock_scope, policy_path=unlocks.get(label))
                    inputs.append(DatasetInput(label, dataset, scope))
                result = write_selection(evaluate_selection(args.ranking, inputs=inputs, candidate_rule=args.rule,
                    candidate_policy=load_candidate_policy(args.candidate_policy) if args.candidate_policy else None,
                    baseline=args.baseline), args.output_dir)
    except (ValueError, OSError) as exc:
        print(f"Strategy Lab selection: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
