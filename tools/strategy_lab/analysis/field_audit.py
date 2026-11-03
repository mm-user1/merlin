"""Window-major population diagnostics after the existing IS decisions are frozen."""
from dataclasses import dataclass
from collections import defaultdict

import numpy as np

from .dataset import AnalysisError, git_facts
from .evaluate import evaluate_scope, _spearman
from .output import render_files
from .research_inputs import restrict_universe
from .rules import star_neighbours
from ..report_support import (statistics, number, neighbourhood, concentration, saved_roots,
    source_facts, new_output, write_rows, write_bytes, finish_report)

METRICS = ("net_profit_pct", "max_drawdown_pct", "max_drawdown_mtm_pct", "total_trades")


class _Reduction:
    """Only C*(T+W) sums/counts/extrema, never a candidate/ticker/window cube."""
    def __init__(self, candidates, tickers, windows):
        self.block = np.full((candidates, windows), np.nan)
        self.block_count = np.zeros((candidates, windows), dtype=int)
        self.block_positive = np.zeros((candidates, windows), dtype=int)
        self.block_min = np.full((candidates, windows), np.inf)
        self.block_max = np.full((candidates, windows), -np.inf)
        self.ticker_sum = np.zeros((candidates, tickers))
        self.ticker_count = np.zeros((candidates, tickers), dtype=int)
        self.ticker_positive = np.zeros((candidates, tickers), dtype=int)
        self.ticker_min = np.full((candidates, tickers), np.inf)
        self.ticker_max = np.full((candidates, tickers), -np.inf)
        self.weighted = np.zeros((candidates, tickers))
        self.minimum = np.full(candidates, np.inf)
        self.maximum = np.full(candidates, -np.inf)
        self.positive = np.zeros(candidates, dtype=int)

    def add(self, window, values):
        # values is the current C*T window, released before the next window.
        valid = np.isfinite(values)
        count = valid.sum(axis=1)
        summed = np.where(valid, values, 0).sum(axis=1)
        np.divide(summed, count, out=self.block[:, window], where=count > 0)
        self.ticker_sum += np.where(valid, values, 0)
        self.ticker_count += valid
        self.block_count[:, window] = count
        self.block_positive[:, window] = (valid & (values > 0)).sum(axis=1)
        self.block_min[:, window] = np.where(valid, values, np.inf).min(axis=1)
        self.block_max[:, window] = np.where(valid, values, -np.inf).max(axis=1)
        self.ticker_positive += valid & (values > 0)
        self.ticker_min = np.minimum(self.ticker_min, np.where(valid, values, np.inf))
        self.ticker_max = np.maximum(self.ticker_max, np.where(valid, values, -np.inf))
        self.weighted += np.divide(np.where(valid, values, 0), count[:, None],
            out=np.zeros_like(values), where=count[:, None] > 0)
        self.minimum = np.minimum(self.minimum, np.where(valid, values, np.inf).min(axis=1))
        self.maximum = np.maximum(self.maximum, np.where(valid, values, -np.inf).max(axis=1))
        self.positive += (valid & (values > 0)).sum(axis=1)

    def facts(self, row):
        block = statistics(self.block[row])
        count = int(self.ticker_count[row].sum())
        expected = self.ticker_count.shape[1]*self.block.shape[1]
        return dict(mean=block["mean"], minimum=number(self.minimum[row]), maximum=number(self.maximum[row]),
            expected_cells=expected, finite_cells=count, missing_cells=expected-count,
            finite_tickers=int((self.ticker_count[row] > 0).sum()), finite_blocks=block["finite"],
            positive_cells=int(self.positive[row]), positive_share_expected=float(self.positive[row])/expected,
            positive_share_finite=float(self.positive[row])/count if count else None,
            reason=None if count else "no_finite_values")


@dataclass
class FieldAuditResult:
    analysis: object
    metadata: dict
    tables: dict
    protected_roots: tuple


def evaluate_field_audit(dataset, scope, *, universe=None, candidate_policy=None):
    if dataset.access_log:
        raise AnalysisError("field audit requires a freshly opened dataset with an empty access log.")
    analysis = evaluate_scope(dataset, scope, universe=universe, candidate_policy=candidate_policy)
    (scope,), _ = restrict_universe(universe, [("analysis", dataset, scope)])
    start = len(dataset.access_log)
    geometry = dataset.geometry
    c, t, w = geometry.count, len(scope.tickers), len(scope.windows)
    metrics = [m for m in METRICS if m in dataset.metric_index]
    reductions = {(segment, m): _Reduction(c, t, w) for segment in ("is", "oos") for m in metrics}
    neighbour_reductions = {(segment, m): _Reduction(c, t, w)
        for segment in ("is", "oos") for m in ("mean", "positive_share", "worst", "center_minus_mean")}
    support = defaultdict(lambda: np.zeros(c, dtype=int))
    neighbours = star_neighbours(geometry)
    selected_rule = candidate_policy.key if candidate_policy else dataset.contract.rule_registry["baseline_rule"]
    selected = {(p["ticker"], p["window_id"]): p for p in analysis.pair_decisions if p["rule"] == selected_rule}
    key_rows = {key: i for i, key in enumerate(geometry.semantic_keys)}
    axes = defaultdict(list)
    for i in range(c):
        for j, name in enumerate(geometry.global_axis_names):
            if geometry.active_masks[i, j]:
                axes[(geometry.block_keys[i], tuple(bool(v) for v in geometry.active_masks[i]), name,
                      int(geometry.axis_codes[i, j]))].append(i)
    axis_groups = list(axes.items())
    marginals = {(segment, m): _Reduction(len(axes), t, w)
        for segment in ("is", "oos") for m in metrics}
    marginal_finite = {key: np.zeros(len(axes), dtype=int) for key in marginals}
    cells, detailed = [], []
    for wi, window in enumerate(scope.windows):
        is_views = dataset.load_is_window(scope, window)
        oos = dataset.load_oos_window(scope, window)
        net_is = np.column_stack([is_views[ticker].metrics["net_profit_pct"] for ticker in scope.tickers])
        net_oos = np.column_stack([oos[ticker][:, dataset.metric_index["net_profit_pct"]] for ticker in scope.tickers])
        for segment in ("is", "oos"):
            for metric in metrics:
                values = np.column_stack([is_views[ticker].metrics[metric] if segment == "is" else
                    oos[ticker][:, dataset.metric_index[metric]] for ticker in scope.tickers])
                reductions[segment, metric].add(wi, values)
                marginal = np.full((len(axes), t), np.nan)
                for ai, (_, indices) in enumerate(axis_groups):
                    valid = np.isfinite(values[indices])
                    marginal_finite[segment, metric][ai] += int(valid.sum())
                    np.divide(np.where(valid, values[indices], 0).sum(axis=0), valid.sum(axis=0),
                        out=marginal[ai], where=valid.sum(axis=0) > 0)
                marginals[segment, metric].add(wi, marginal)
                for ti, ticker in enumerate(scope.tickers):
                    paired = np.isfinite(net_is[:, ti]) & np.isfinite(net_oos[:, ti])
                    corr = _spearman(net_is[:, ti], net_oos[:, ti])
                    decision = selected[ticker, window.window_id]
                    stats = statistics(values[:, ti])
                    feasibility = dict(status="not_requested")
                    if candidate_policy is not None:
                        from .candidate_policy import OPERATORS
                        view = is_views[ticker]
                        result = candidate_policy.evaluate(view, geometry)
                        feasibility = dict(status="requested", finite_objective_count=int(np.isfinite(
                            view.metrics[candidate_policy.document["objective"]["metric"]]).sum()),
                            constraint_pass_counts=[int((np.isfinite(view.metrics[x["metric"]]) &
                                OPERATORS[x["operator"]](view.metrics[x["metric"]], x["value"])).sum())
                                for x in candidate_policy.document["constraints"]],
                            eligible_count=int(result.eligible.sum()), no_eligible=not bool(result.eligible.any()))
                    population = statistics(net_oos[:, ti])
                    chosen = decision["top1_oos_net_profit_pct"]
                    cells.append(dict(ticker=ticker, window_id=window.window_id, segment=segment, metric=metric,
                        **stats, zero_trade_count=int((is_views[ticker].metrics["total_trades"] == 0).sum())
                            if segment == "is" and "total_trades" in dataset.metric_index else
                            int((oos[ticker][:, dataset.metric_index["total_trades"]] == 0).sum())
                            if "total_trades" in dataset.metric_index else None,
                        spearman=corr, paired_finite=int(paired.sum()),
                        spearman_reason=None if corr is not None else "fewer_than_eight_pairs" if paired.sum() < 8 else "constant_vector",
                        feasibility=feasibility, selected_rule=selected_rule, selected_semantic_keys=decision["selected_semantic_keys"][:1],
                        selected_oos=chosen, selected_minus_population_mean=chosen-population["mean"]
                            if chosen is not None and population["mean"] is not None else None,
                        selected_minus_population_median=chosen-population["median"]
                            if chosen is not None and population["median"] is not None else None))
            values = net_is if segment == "is" else net_oos
            neighbour_values = {m: np.full((c, t), np.nan) for m in ("mean", "positive_share", "worst", "center_minus_mean")}
            for ti, ticker in enumerate(scope.tickers):
                decision = selected[ticker, window.window_id]
                chosen = key_rows.get(next(iter(decision["selected_semantic_keys"]), None))
                for ci, indices in enumerate(neighbours):
                    facts = neighbourhood(values[:, ti], indices, ci)
                    support[segment, facts["status"]][ci] += 1
                    support[segment, "incomplete"][ci] += facts["partial_support"]
                    for m in neighbour_values:
                        if facts[m] is not None:
                            neighbour_values[m][ci, ti] = facts[m]
                    if ci == chosen:
                        detailed.append(dict(level="selected_cell", semantic_key=geometry.semantic_keys[ci],
                            candidate_id=int(geometry.candidate_ids[ci]), segment=segment, ticker=ticker,
                            window_id=window.window_id, **facts, expected_cells=None, eligible_cells=None,
                            insufficient_neighbors=None, no_finite_distinct_neighbor=None, incomplete_cells=None))
            for m, values in neighbour_values.items():
                neighbour_reductions[segment, m].add(wi, values)
        del is_views, oos, net_is, net_oos
    candidates, axis_rows, neighbour_rows, contributions = [], [], [], []
    window_groups = {f"window:{window.window_id}": dict(window_id=window.window_id,
        is_interval=[window.is_start, window.is_end], oos_interval=[window.oos_start, window.oos_end])
        for window in scope.windows}
    time_order = {key: (facts["oos_interval"][0], facts["window_id"]) for key, facts in window_groups.items()}
    for segment in ("is", "oos"):
        for ci, key in enumerate(geometry.semantic_keys):
            base = dict(semantic_key=key, candidate_id=int(geometry.candidate_ids[ci]), params=dict(geometry.params[ci]), segment=segment)
            for metric in metrics:
                reduction = reductions[segment, metric]
                candidates.append(dict(**base, metric=metric, level="candidate", ticker=None, window_id=None, **reduction.facts(ci)))
                if metric != "net_profit_pct":
                    continue
                for ti, ticker in enumerate(scope.tickers):
                    count = int(reduction.ticker_count[ci, ti])
                    candidates.append({**base, "metric":metric, "level":"ticker", "ticker":ticker,
                        "mean":float(reduction.ticker_sum[ci, ti])/count if count else None,
                        "expected_cells":w, "finite_cells":count, "missing_cells":w-count,
                        "minimum":number(reduction.ticker_min[ci, ti]), "maximum":number(reduction.ticker_max[ci, ti]),
                        "positive_cells":int(reduction.ticker_positive[ci, ti]), "reason":None if count else "no_finite_values"})
                for wi, window in enumerate(scope.windows):
                    candidates.append({**base, "metric":metric, "level":"block", "window_id":window.window_id,
                        "mean":number(reduction.block[ci, wi]),
                        "expected_cells":t, "finite_cells":int(reduction.block_count[ci, wi]),
                        "missing_cells":int(t-reduction.block_count[ci, wi]), "positive_cells":int(reduction.block_positive[ci, wi]),
                        "minimum":number(reduction.block_min[ci, wi]), "maximum":number(reduction.block_max[ci, wi]),
                        "reason":None if reduction.block_count[ci, wi] else "no_finite_values"})
            reduction = reductions[segment, "net_profit_pct"]
            facts = reduction.facts(ci)
            blocks = facts["finite_blocks"]
            for dimension, values in (("ticker", [(ticker, float(reduction.weighted[ci, ti])/blocks if blocks and reduction.ticker_count[ci, ti] else None)
                    for ti, ticker in enumerate(scope.tickers)]),
                    ("time", [(f"window:{window.window_id}", number(reduction.block[ci, wi]/blocks) if blocks else None)
                    for wi, window in enumerate(scope.windows)])):
                for row in concentration(values, partial_support=facts["missing_cells"] > 0,
                        order_key=time_order.__getitem__ if dimension == "time" else None):
                    contributions.append(dict(**base, dimension=dimension, unit="percentage_points_of_hierarchical_mean", **row))
            mean = neighbour_reductions[segment, "mean"].facts(ci)
            neighbour_rows.append(dict(level="candidate", semantic_key=key, candidate_id=int(geometry.candidate_ids[ci]),
                segment=segment, ticker=None, window_id=None, expected_members=len(neighbours[ci]), finite_members=None,
                finite_distinct_neighbor_count=None, status="available" if mean["finite_cells"] else "unavailable",
                partial_support=bool(support[segment, "incomplete"][ci]), center=facts["mean"], mean=mean["mean"],
                worst=number(neighbour_reductions[segment, "worst"].minimum[ci]),
                positive_share=neighbour_reductions[segment, "positive_share"].facts(ci)["mean"],
                center_minus_mean=neighbour_reductions[segment, "center_minus_mean"].facts(ci)["mean"],
                expected_cells=t*w, eligible_cells=mean["finite_cells"],
                insufficient_neighbors=int(support[segment, "insufficient_neighbors"][ci]),
                no_finite_distinct_neighbor=int(support[segment, "no_finite_distinct_neighbor"][ci]),
                incomplete_cells=int(support[segment, "incomplete"][ci])))
        for ai, ((block, mask, axis, code), indices) in enumerate(axis_groups):
            for metric in metrics:
                axis_rows.append(dict(segment=segment, metric=metric, geometry_block=block, active_mask=mask,
                    axis=axis, code=code, value=geometry.params[indices[0]][axis], candidate_count=len(indices),
                    expected_candidate_cells=len(indices)*t*w, finite_candidate_cells=int(marginal_finite[segment, metric][ai]),
                    **marginals[segment, metric].facts(ai)))
    metadata = dict(schema_version="strategy_lab_field_audit_v2", git=git_facts(),
        window_groups=window_groups,
        time_grouping="window/block; segment selects is_interval or oos_interval; overlapping IS blocks are not disjoint calendar returns",
        time_tie_break="saved OOS-start string ascending as in v1; window_id breaks identical-start ties",
        source=source_facts(dataset, scope), research_inputs=analysis.run_metadata.get("research_inputs"),
        selected_rule=selected_rule, rule_states=analysis.run_metadata.get("rule_states", analysis.summary.get("rule_states")),
        classification="descriptive_candidate_population", metric_units={m: "count" if m == "total_trades" else "percent" for m in metrics},
        unsupported_metrics={m: "absent_from_saved_metric_axis" for m in METRICS if m not in metrics},
        reduction="finite candidates per cell; equal finite tickers per block; equal available blocks",
        neighbours="core generated-code adjacency within block/mask; center included; no trade gate; categorical adjacency is not numeric distance",
        diagnostic_access_start=start, diagnostic_access=[list(x) for x in dataset.access_log[start:]],
        candidate_count=c, ticker_count=t, block_count=w)
    return FieldAuditResult(analysis, metadata, dict(cell_summary=cells, candidate_summary=candidates,
        axis_marginals=axis_rows, neighbour_stability=neighbour_rows+detailed, concentration=contributions), tuple(saved_roots(dataset)))


def write_field_audit(result, output_dir):
    output = new_output(output_dir, result.protected_roots)
    files = []
    for name, content in render_files(result.analysis).items():
        name = "analysis/"+name
        write_bytes(output, name, content)
        files.append(name)
    for name, rows in result.tables.items():
        name += ".csv"
        write_rows(output, name, rows)
        files.append(name)
    population, stability = [], []
    for segment in ("is", "oos"):
        cells = [r for r in result.tables["cell_summary"] if r["segment"] == segment and r["metric"] == "net_profit_pct"]
        block_means = [statistics(r["mean"] for r in cells if r["window_id"] == wid)["mean"] for wid in sorted({r["window_id"] for r in cells})]
        population.append(dict(segment=segment, expected=sum(r["expected"] for r in cells), finite=sum(r["finite"] for r in cells),
            positive=sum(r["positive"] for r in cells), hierarchical_mean=statistics(block_means)["mean"],
            no_eligible_cells=sum(r["feasibility"].get("no_eligible", False) for r in cells)
                if any(r["feasibility"]["status"] == "requested" for r in cells) else "not_requested"))
        rows = [r for r in result.tables["neighbour_stability"] if r["level"] == "candidate" and r["segment"] == segment]
        stability.append(dict(segment=segment, candidate_count=len(rows), **{k: sum(r[k] for r in rows)
            for k in ("expected_cells", "eligible_cells", "insufficient_neighbors", "no_finite_distinct_neighbor", "incomplete_cells")}))
    return finish_report(output, "audit.json", result.metadata, files, "Candidate-field audit",
        {k: result.metadata[k] for k in ("candidate_count", "ticker_count", "block_count", "selected_rule", "reduction", "neighbours")},
        [("Population and feasibility", population), ("Aggregate neighborhood support", stability)])
