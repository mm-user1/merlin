"""Comparative views of one allocation evaluation; no independent selection loop."""
from copy import deepcopy
from dataclasses import dataclass
import json

from .allocation import DatasetInput, evaluate_allocation
from .dataset import AnalysisError, git_facts
from .output import render_allocation_files
from .research_inputs import restrict_universe
from ..report_support import (finite, number, labels, concentration, saved_roots, source_facts,
    new_output, write_rows, write_bytes, finish_report)


def _equal(left, right, label, field):
    if left != right:
        raise AnalysisError(f"strict filter pair {label!r} differs at {field}.")


def _strict_pair(inputs, baseline):
    if len(inputs) != 2 or any(i.dataset.schema_version != "strategy_lab_dataset_v4" or
            i.dataset.status != "complete" for i in inputs):
        raise AnalysisError("filter-baseline requires exactly two complete saved v4 datasets.")
    off = next(i for i in inputs if i.label == baseline)
    on = next(i for i in inputs if i.label != baseline)
    if off.dataset.filter_metadata["effective"] or not on.dataset.filter_metadata["effective"]:
        raise AnalysisError("filter-baseline must be effective Off with one active input.")
    _equal(sorted(off.scope.tickers), sorted(on.scope.tickers), on.label, "effective ticker set")
    _equal(sorted((w.is_start, w.is_end, *w.block_key) for w in off.scope.windows),
           sorted((w.is_start, w.is_end, *w.block_key) for w in on.scope.windows), on.label, "effective IS/OOS windows")
    generations = [deepcopy(i.dataset.run_spec["generation"]) for i in (off, on)]
    for g in generations:
        del g["execution"]["entryFilters"]
    _equal(*generations, on.label, "generation minus execution.entryFilters")
    for name in ("analysis_contract", "rule_registry", "seed", "random_control_draws"):
        _equal(off.dataset.run_spec["preregistration"][name], on.dataset.run_spec["preregistration"][name],
               on.label, "preregistration."+name)
    _equal(*(json.loads((i.dataset.root/"candidates.json").read_text(encoding="utf-8")) for i in (off, on)),
           on.label, "candidates.json")
    for field in ("dataset_schema", "roster_sha256", "included_tickers", "windows", "geometry_sha256",
        "semantic_key_digest", "versions", "resources", "metric_axis", "segment_axis", "candidate_count",
        "candidate_axis", "dtype", "expected_group_shape", "metric_requests"):
        _equal(off.dataset.manifest["identity"][field], on.dataset.manifest["identity"][field], on.label, "identity."+field)
    bindings = [{b["instrument_id"]: b for b in json.loads((i.dataset.root/"input_bindings.json").read_text(encoding="utf-8"))["instruments"]}
        for i in (off, on)]
    from core.engine_v2.admission import read_admission_envelope
    for ticker in off.scope.tickers:
        a, b = (binding[ticker] for binding in bindings)
        for field in ("instrument_id", "cell", "input_pin", "semantic_key_digest", "segments"):
            _equal(a[field], b[field], on.label, f"binding.{ticker}.{field}")
        for field in ("version", "mode", "maxEntryLeverage"):
            _equal(a["entry_admission"][field], b["entry_admission"][field], on.label, f"admission.{field}")
        _equal(*(read_admission_envelope(x["entry_admission"], x["entry_admission"]["maxEntryLeverage"]).semantic()
                 for x in (a, b)), on.label, f"admission.{ticker}.rules")
    return off, on


def _comparisons(allocation, baseline):
    rows = []
    if baseline is None:
        return rows
    datasets = allocation.summary["datasets"]
    pairs = allocation.pair_decisions
    for label, summary in datasets.items():
        if label == baseline:
            continue
        blocks = summary["blocks"]
        variants = [*summary["variants"], "all_available"]
        for variant in variants:
            observed = []
            paired_values = []
            for left, right in zip(datasets[baseline]["blocks"], blocks):
                block = right["block_key"]
                if variant == "all_available":
                    sets = [sorted(p["ticker"] for p in pairs if p["dataset_label"] == name and
                            [p["oos_start"], p["oos_end"]] == block and finite(p["ticker_score"]) and finite(p["oos_return_pct"]))
                            for name in (baseline, label)]
                    values = [b["all_available_mean_pct"] for b in (left, right)]
                    reason = "no_finite_tickers" if not any(sets) else "different_finite_ticker_sets" if sets[0] != sets[1] else None
                else:
                    sets = [b["variants"][variant]["selected_tickers"] for b in (left, right)]
                    values = [b["variants"][variant]["capacity_return_pct"] for b in (left, right)]
                    reason = None if all(finite(v) for v in values) else "unavailable_capacity_block"
                delta = values[1]-values[0] if reason is None and all(finite(v) for v in values) else None
                observed.append(delta)
                paired_values.append(values)
                rows.append(dict(level="block", baseline=baseline, dataset_label=label, variant=variant,
                    block_key=block, baseline_value=values[0], input_value=values[1], delta=delta,
                    unit="percentage_points", reason=reason, baseline_selected_tickers=sets[0], input_selected_tickers=sets[1],
                    baseline_unit="percent", input_unit="percent", delta_unit="percentage_points",
                    baseline_selected_count=len(sets[0]), input_selected_count=len(sets[1]),
                    expected_blocks=len(blocks), finite_paired_blocks=None))
            rows.append(dict(level="headline", baseline=baseline, dataset_label=label, variant=variant,
                block_key=None,
                baseline_value=sum(v[0] for v in paired_values)/len(blocks) if all(finite(x) for x in observed) else None,
                input_value=sum(v[1] for v in paired_values)/len(blocks) if all(finite(x) for x in observed) else None,
                delta=sum(observed)/len(observed) if all(finite(x) for x in observed) else None,
                unit="percentage_points", reason=None if all(finite(x) for x in observed) else "incomplete_paired_blocks",
                baseline_selected_tickers=None, input_selected_tickers=None,
                baseline_unit="percent", input_unit="percent", delta_unit="percentage_points",
                baseline_selected_count=None, input_selected_count=None,
                expected_blocks=len(blocks), finite_paired_blocks=sum(finite(x) for x in observed)))
    return rows


def _turnover(allocation):
    blocks = allocation.run_metadata["alignment"]["common_blocks"]
    rows = []
    for label in allocation.run_metadata["dataset_labels"]:
        pairs = {(p["ticker"], p["oos_start"], p["oos_end"]): p["semantic_key"] for p in allocation.pair_decisions if p["dataset_label"] == label}
        for ticker in allocation.run_metadata["alignment"]["common_tickers"]:
            changed = unchanged = unavailable = 0
            for a, b in zip(blocks, blocks[1:]):
                before, after = pairs[ticker, *a], pairs[ticker, *b]
                if before is None or after is None:
                    unavailable += 1
                elif before == after:
                    unchanged += 1
                else:
                    changed += 1
            rows.append(dict(dataset_label=label, ticker=ticker, changed=changed, unchanged=unchanged,
                unavailable=unavailable, valid_transitions=changed+unchanged,
                changed_share=changed/(changed+unchanged) if changed+unchanged else None))
    return rows


def _concentration(allocation):
    rows = []
    n = allocation.run_metadata["alignment"]["common_block_count"]
    for label, summary in allocation.summary["datasets"].items():
        for variant in summary["variants"]:
            complete = all(finite(b["variants"][variant]["capacity_return_pct"]) for b in summary["blocks"])
            for dimension in ("ticker", "time"):
                contributions = []
                for block in summary["blocks"]:
                    valid = finite(block["variants"][variant]["capacity_return_pct"])
                    members = [p for p in allocation.ticker_allocations if p["dataset_label"] == label and
                        p["variant"] == variant and p["allocation_kind"] == "top_k" and
                        [p["oos_start"], p["oos_end"]] == block["block_key"]]
                    for p in members:
                        contributions.append((p["ticker"] if dimension == "ticker" else block["block_key"][0],
                            p["capacity_contribution_pct"]/n if valid and finite(p["capacity_contribution_pct"]) else None))
                    if not members:
                        contributions.append(("cash" if dimension == "ticker" else block["block_key"][0], 0 if valid else None))
                rows.extend(dict(dataset_label=label, variant=variant, dimension=dimension,
                    unit="percentage_points_of_mean_capacity_return", expected_blocks=n, **r)
                    for r in concentration(contributions, complete=complete, partial_support=not complete))
    return rows


@dataclass
class ScreenResult:
    allocation: object
    metadata: dict
    tables: dict
    protected_roots: tuple
    filter_pair: tuple | None


def evaluate_screen(inputs, *, candidate_rule=None, candidate_policy=None, universe=None,
                    ticker_list_mode=None, primary_k=6, sensitivity_k=8, baseline=None, filter_baseline=None):
    inputs = list(inputs)
    labels([(i.label, i.dataset.root) for i in inputs])
    if baseline is not None and filter_baseline is not None:
        raise AnalysisError("baseline and filter-baseline are mutually exclusive.")
    chosen = filter_baseline if filter_baseline is not None else baseline
    if chosen is not None and chosen not in [i.label for i in inputs]:
        raise AnalysisError("baseline label is not an input.")
    scopes, _ = restrict_universe(universe, [(i.label, i.dataset, i.scope) for i in inputs], ticker_list_mode)
    effective = [DatasetInput(i.label, i.dataset, deepcopy(s)) for i, s in zip(inputs, scopes)]
    pair = _strict_pair(effective, filter_baseline) if filter_baseline is not None else None
    allocation = evaluate_allocation(inputs, candidate_rule=candidate_rule, candidate_policy=candidate_policy,
        universe=universe, ticker_list_mode=ticker_list_mode, primary_k=primary_k, sensitivity_k=sensitivity_k)
    metadata = dict(schema_version="strategy_lab_screen_v1", git=git_facts(),
        classification="strict_filter_pair" if pair else "descriptive_aligned",
        sources={i.label: source_facts(i.dataset, i.scope) for i in effective},
        research_inputs=deepcopy(allocation.run_metadata.get("research_inputs")),
        alignment=deepcopy(allocation.run_metadata["alignment"]), baseline=chosen,
        variants=deepcopy(allocation.summary["datasets"]),
        confounders="Compare saved strategy/version, economics, timeframe, source pins and IS geometry; alignment alone is not identical samples.",
        interpretation="policy and ticker allocation comparison on common IS-eligible pool",
        turnover="adjacent ordered common blocks including gaps; nonnull frozen IS keys independent of OOS/common-pool status",
        inference="not_requested", metric_units={"return": "percent", "delta": "percentage_points"})
    if pair:
        metadata["filter_pair_labels"] = dict(off=pair[0].label, on=pair[1].label)
        metadata["estimands"] = ["same configuration", "policy reselection with filters",
            "policy and ticker allocation comparison on common IS-eligible pool"]
    return ScreenResult(allocation, metadata, dict(comparisons=_comparisons(allocation, chosen),
        concentration=_concentration(allocation), candidate_turnover=_turnover(allocation)),
        tuple(root for i in inputs for root in saved_roots(i.dataset)), pair)


def _filter_rows(result, selection_rows):
    off, on = result.filter_pair
    pair_lookup = {(p["dataset_label"], p["ticker"], p["oos_start"], p["oos_end"]): p for p in result.allocation.pair_decisions}
    keys = [dict(zip(i.dataset.geometry.semantic_keys, range(i.dataset.geometry.count))) for i in (off, on)]
    if len(keys[0]) != off.dataset.geometry.count or set(keys[0]) != set(keys[1]):
        raise AnalysisError("strict filter semantic join is not one-to-one.")
    for key in keys[0]:
        _equal(dict(off.dataset.geometry.params[keys[0][key]]), dict(on.dataset.geometry.params[keys[1][key]]), on.label, "candidate scalars "+key)
    starts = [len(i.dataset.access_log) for i in (off, on)]
    windows = [{w.block_key: w for w in i.scope.windows} for i in (off, on)]
    metrics = ("net_profit_pct", "max_drawdown_pct", "max_drawdown_mtm_pct", "total_trades")
    for block in sorted(windows[0]):
        for segment in ("is", "oos"):
            arrays = [i.dataset._load_segment(i.scope, mapping[block], segment) for i, mapping in zip((off, on), windows)]
            for ticker in off.scope.tickers:
                for key in keys[0]:
                    row = dict(ticker=ticker, block_key=block, segment=segment, semantic_key=key,
                        params=dict(off.dataset.geometry.params[keys[0][key]]), interpretation="same configuration")
                    for label, item, values, mapping in zip(("off", "on"), (off, on), arrays, keys):
                        row[label+"_manifest_sha256"] = item.dataset.manifest_sha256
                        row[label+"_candidate_id"] = int(item.dataset.geometry.candidate_ids[mapping[key]])
                        for metric in metrics:
                            row[label+"_"+metric] = number(values[ticker][mapping[key], item.dataset.metric_index[metric]])
                    for metric in metrics:
                        a, b = row["off_"+metric], row["on_"+metric]
                        row["delta_"+metric] = b-a if a is not None and b is not None else None
                        row["reason_"+metric] = None if a is not None and b is not None else "nonfinite_pair"
                    yield row
                if segment == "oos":
                    row = dict(ticker=ticker, block_key=block, interpretation="policy reselection with filters")
                    for label, item, values, mapping in zip(("off", "on"), (off, on), arrays, keys):
                        pair = pair_lookup[item.label, ticker, *block]
                        key = pair["semantic_key"]
                        row[label+"_semantic_key"] = key
                        row[label+"_manifest_sha256"] = item.dataset.manifest_sha256
                        row[label+"_is_net_profit_pct"] = pair["is_net_profit_pct"]
                        row[label+"_selection_available"] = key is not None
                        row[label+"_ticker_score_available"] = finite(pair["is_net_profit_pct"])
                        row[label+"_in_common_pool"] = finite(pair["ticker_score"])
                        for metric in metrics:
                            row[label+"_"+metric] = number(values[ticker][mapping[key], item.dataset.metric_index[metric]]) if key is not None else None
                    for metric in metrics:
                        a, b = row["off_"+metric], row["on_"+metric]
                        row["delta_"+metric] = b-a if a is not None and b is not None else None
                        row["reason_"+metric] = None if a is not None and b is not None else "missing_selection_or_nonfinite_oos"
                    selection_rows.append(row)
            del arrays
    result.metadata["diagnostic_access"] = {i.label: dict(start=start, entries=[list(e) for e in i.dataset.access_log[start:]])
        for i, start in zip((off, on), starts)}


def write_screen(result, output_dir):
    output = new_output(output_dir, result.protected_roots)
    files = []
    for name, content in render_allocation_files(result.allocation).items():
        name = "allocation/"+name
        write_bytes(output, name, content)
        files.append(name)
    for name, rows in result.tables.items():
        name += ".csv"
        write_rows(output, name, rows)
        files.append(name)
    if result.filter_pair:
        selection = []
        write_rows(output, "filter_candidate_pairs.csv", _filter_rows(result, selection))
        write_rows(output, "filter_selection_pairs.csv", selection)
        files += ["filter_candidate_pairs.csv", "filter_selection_pairs.csv"]
    summaries = [dict(input=label, variant=name, headline=v["headline_mean_capacity_return_pct"],
        control_headlines=v["control_headlines"], ticker_turnover=v["turnover"],
        omitted_tickers=facts["omitted_tickers"], omitted_blocks=facts["omitted_blocks"])
        for label, facts in result.allocation.summary["datasets"].items() for name, v in facts["variants"].items()]
    return finish_report(output, "screen.json", result.metadata, files, "Standard IS/OOS screen",
        dict(classification=result.metadata["classification"], inputs=list(result.metadata["sources"]),
             common_tickers=result.metadata["alignment"]["common_ticker_count"],
             common_blocks=result.metadata["alignment"]["common_block_count"], interpretation=result.metadata["interpretation"]),
        [("Allocation and existing controls", summaries)])
