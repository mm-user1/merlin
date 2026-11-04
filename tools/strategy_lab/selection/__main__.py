"""Explicit file-based X preparation, Fixed transform and saved-outcome evaluation."""
import argparse
from contextlib import redirect_stdout
import json
import sys

from . import prepare_ranking, write_fixed_request, evaluate_selection, write_selection
from . import prepare_history_plan, write_history_fixed_request, evaluate_history
from .history_plan import checked_input
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
    hp = sub.add_parser("history-plan")
    hp.add_argument("request")
    hp.add_argument("--ticker-list", required=True)
    hp.add_argument("--output-dir", required=True)
    ht = sub.add_parser("history-fixed-request")
    for name in ("plan", "template", "output-dir"):
        ht.add_argument("--"+name, required=True)
    ht.add_argument("--warmup-bars", type=int, required=True)
    add_evaluation_arguments(sub.add_parser("evaluate"), history=False)
    add_evaluation_arguments(sub.add_parser("history-evaluate"), history=True)
    return p


def add_evaluation_arguments(evaluate, *, history):
    evaluate.add_argument("--plan" if history else "--ranking", required=True)
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
    if history:
        evaluate.add_argument("--history-scope")
        evaluate.add_argument("--history-scope-for", action="append")
        evaluate.add_argument("--history-unlock-scope", action="store_true")
        evaluate.add_argument("--history-unlock-policy", action="append")


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        with redirect_stdout(sys.stderr):
            if args.command == "prepare":
                result = prepare_ranking(args.request, args.output_dir, ticker_list=args.ticker_list, data_root=args.data_root)
            elif args.command == "fixed-request":
                result = write_fixed_request(args.ranking, args.template, args.output_dir, warmup_bars=args.warmup_bars)
            elif args.command == "history-plan":
                result = prepare_history_plan(args.request, args.output_dir, ticker_list=args.ticker_list)
            elif args.command == "history-fixed-request":
                result = write_history_fixed_request(args.plan, args.template, args.output_dir, warmup_bars=args.warmup_bars)
            elif args.fixed_dataset:
                require(not any((args.scope, args.scope_for, args.rule, args.candidate_policy, args.unlock_scope, args.unlock_policy)),
                    "candidate/scope options are irrelevant to Fixed inputs.")
                options = dict(fixed_inputs=labelled(args.fixed_dataset), parameter_sets=labelled(args.parameter_set),
                    allow_partial=args.allow_partial, baseline=args.baseline)
                if args.command == "history-evaluate":
                    require(not any((args.history_scope, args.history_scope_for, args.history_unlock_scope, args.history_unlock_policy)),
                        "history scope options are irrelevant to Fixed.")
                    result = evaluate_history(args.plan, args.output_dir, **options)
                else:
                    result = write_selection(evaluate_selection(args.ranking, **options), args.output_dir)
            else:
                require(not args.parameter_set and not args.allow_partial, "parameter-set/allow-partial are Fixed-only.")
                paths, scopes, unlocks = labelled(args.dataset), labelled(args.scope_for), labelled(args.unlock_policy)
                require(not (set(scopes)|set(unlocks))-set(paths), "unknown scope/unlock label.")
                require(not unlocks or args.unlock_scope, "unlock-policy requires unlock-scope.")
                inputs = []
                history = args.command == "history-evaluate"
                history_scopes = {}
                if history:
                    hs, hu = labelled(args.history_scope_for), labelled(args.history_unlock_policy)
                    require(not (set(hs)|set(hu))-set(paths), "unknown history scope/unlock label.")
                    require(not hu or args.history_unlock_scope, "history-unlock-policy requires history-unlock-scope.")
                    for path in [*paths.values(), *unlocks.values(), *hu.values(), *([args.candidate_policy] if args.candidate_policy else [])]:
                        checked_input(path)
                for label, path in paths.items():
                    scope_name = scopes.get(label, args.scope)
                    require(scope_name is not None, "each candidate input requires an explicit scope.")
                    dataset = open_dataset(path)
                    scope = dataset.resolve_scope(scope_name, unlock=args.unlock_scope, policy_path=unlocks.get(label))
                    inputs.append(DatasetInput(label, dataset, scope))
                    if history:
                        history_name = hs.get(label, args.history_scope)
                        require(history_name is not None, "each candidate input requires an explicit history scope.")
                        history_scopes[label] = dataset.resolve_scope(history_name, unlock=args.history_unlock_scope, policy_path=hu.get(label))
                options = dict(inputs=inputs, candidate_rule=args.rule,
                    candidate_policy=load_candidate_policy(args.candidate_policy) if args.candidate_policy else None, baseline=args.baseline)
                result = evaluate_history(args.plan, args.output_dir, history_scopes=history_scopes, **options) if history else write_selection(
                    evaluate_selection(args.ranking, **options), args.output_dir)
    except (ValueError, OSError) as exc:
        print(f"Strategy Lab selection: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
