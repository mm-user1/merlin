"""Leakage-safe rule evaluation, aggregation, evidence, and diagnostics."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from .dataset import (
    EVALUABLE_EVIDENCE_BLOCKS,
    AnalysisDataset,
    AnalysisError,
    ResolvedScope,
    git_facts,
)
from .rules import (
    FORMULA_DIAGNOSTICS,
    OOS_DIAGNOSTICS,
    RULE_REQUIREMENTS,
    RuleResult,
    Selection,
    average_ranks,
    evaluate_custom_rule,
    evaluate_rule,
    percentile_rank,
    select_candidates,
    star_neighbours,
)


@dataclass(frozen=True)
class CustomRule:
    name: str
    version: str
    function: Callable[..., Any]


@dataclass(frozen=True)
class AnalysisResult:
    run_metadata: Mapping[str, Any]
    summary: Mapping[str, Any]
    pair_decisions: tuple[Mapping[str, Any], ...]
    monthly_results: tuple[Mapping[str, Any], ...]


def _finite_mean(values: np.ndarray) -> float | None:
    finite = np.asarray(values, dtype=np.float64)
    finite = finite[np.isfinite(finite)]
    return float(np.mean(finite)) if finite.size else None


def _finite_median(values: np.ndarray) -> float | None:
    finite = np.asarray(values, dtype=np.float64)
    finite = finite[np.isfinite(finite)]
    return float(np.median(finite)) if finite.size else None


def _finite_quantile(values: np.ndarray, q: float) -> float | None:
    finite = np.asarray(values, dtype=np.float64)
    finite = finite[np.isfinite(finite)]
    return float(np.quantile(finite, q)) if finite.size else None


def monthly_headline(values: np.ndarray) -> tuple[np.ndarray, float | None]:
    """Equal-weight valid tickers per block, then equal-weight finite blocks."""
    matrix = np.asarray(values, dtype=np.float64)
    if matrix.ndim != 2:
        raise AnalysisError("monthly_headline requires [ticker, block] values.")
    monthly = np.asarray([_finite_mean(matrix[:, index]) for index in range(matrix.shape[1])], dtype=object)
    numeric = np.asarray(
        [float(value) if value is not None else np.nan for value in monthly],
        dtype=np.float64,
    )
    return numeric, _finite_mean(numeric)


def profitable_share(values: np.ndarray, *, denominator: int | None = None) -> float | None:
    matrix = np.asarray(values, dtype=np.float64)
    divisor = matrix.size if denominator is None else denominator
    if divisor <= 0:
        return None
    return float(np.sum(np.isfinite(matrix) & (matrix > 0.0)) / divisor)


def descriptive_bootstrap(
    monthly_values: Sequence[float], uncertainty: Mapping[str, Any]
) -> Mapping[str, Any]:
    required = {
        "type": "descriptive_month_block_bootstrap",
        "sampling": "with_replacement",
        "pass_fail_criterion": False,
    }
    for key, expected in required.items():
        if uncertainty.get(key) != expected:
            raise AnalysisError(f"unsupported uncertainty contract {key}={uncertainty.get(key)!r}.")
    blocks = uncertainty.get("blocks")
    resamples = uncertainty.get("resamples")
    sample_size = uncertainty.get("sample_size_per_resample")
    seed = uncertainty.get("seed")
    percentiles = uncertainty.get("percentiles")
    if (
        any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in (blocks, resamples, sample_size))
        or isinstance(seed, bool)
        or not isinstance(seed, int)
        or not isinstance(percentiles, list)
        or len(percentiles) != 2
    ):
        raise AnalysisError("malformed uncertainty sampling contract.")
    finite = np.asarray(monthly_values, dtype=np.float64)
    finite = finite[np.isfinite(finite)]
    if finite.size < blocks:
        return {
            "status": "unavailable",
            "reason": f"requires {blocks} finite blocks; observed {finite.size}",
            "block_count": int(finite.size),
            "lower": None,
            "upper": None,
            "width": None,
        }
    rng = np.random.default_rng(seed)
    draws = rng.choice(finite, size=(resamples, sample_size), replace=True).mean(axis=1)
    lower, upper = np.percentile(draws, percentiles, method="linear")
    return {
        "status": "available",
        "reason": None,
        "block_count": int(finite.size),
        "lower": float(lower),
        "upper": float(upper),
        "width": float(upper - lower),
    }


def outlier_robustness(
    paired_lift: np.ndarray,
    selected: np.ndarray,
    tickers: Sequence[str],
    procedure: Mapping[str, Any],
    *,
    ticker_cell_count: int | None = None,
) -> Mapping[str, Any]:
    if (
        procedure.get("contribution")
        != "ticker_mean_top1_paired_lift_over_six_windows"
        or procedure.get("removal_candidates") != "strictly_positive_contributors_only"
        or procedure.get("quota") != "ceil(0.10*total_ticker_count_in_cell)"
        or procedure.get("remove_count") != "min(quota,positive_contributor_count)"
        or procedure.get("order")
        != ["mean_contribution_desc", "canonical_symbol_asc"]
        or procedure.get("recompute")
        != "monthly_means_then_equal_weight_six_month_headline"
        or procedure.get("criterion_scope")
        != "recomputed_headline_only_monthly_signs_descriptive"
    ):
        raise AnalysisError("unsupported outlier procedure contract.")
    lift = np.asarray(paired_lift, dtype=np.float64)
    absolute = np.asarray(selected, dtype=np.float64)
    if lift.shape != absolute.shape or lift.ndim != 2 or lift.shape[0] != len(tickers):
        raise AnalysisError("outlier inputs must align by ticker and block.")
    contributions: list[tuple[str, int, float]] = []
    for index, ticker in enumerate(tickers):
        contribution = _finite_mean(lift[index])
        if contribution is not None and contribution > 0.0:
            contributions.append((str(ticker), index, contribution))
    quota_authority = len(tickers) if ticker_cell_count is None else ticker_cell_count
    if (
        isinstance(quota_authority, bool)
        or not isinstance(quota_authority, int)
        or quota_authority < len(tickers)
        or quota_authority < 1
    ):
        raise AnalysisError("outlier ticker-cell count is invalid for the analyzed subset.")
    quota = math.ceil(0.10 * quota_authority)
    contributions.sort(key=lambda item: (-item[2], item[0]))
    removed = contributions[: min(quota, len(contributions))]
    removed_indices = {item[1] for item in removed}
    keep = [index for index in range(len(tickers)) if index not in removed_indices]
    original_monthly, original = monthly_headline(lift)
    robust_monthly, robust = monthly_headline(lift[keep])
    _, robust_absolute = monthly_headline(absolute[keep])
    return {
        "removed_tickers": [item[0] for item in removed],
        "positive_contributor_count": len(contributions),
        "quota": quota,
        "ticker_cell_count": quota_authority,
        "original_lift_headline": original,
        "robust_lift_headline": robust,
        "robust_absolute_selected_headline": robust_absolute,
        "original_monthly_lift": original_monthly.tolist(),
        "robust_monthly_lift_descriptive": robust_monthly.tolist(),
        "reference_divergence": (
            "persisted symbol-ascending tie-break applied; preserved prototype omitted it"
        ),
    }


def _criterion_status(operator: str, observed: float, threshold: float) -> bool:
    if operator == ">":
        return observed > threshold
    if operator == ">=":
        return observed >= threshold
    raise AnalysisError(f"unknown evidence operator: {operator!r}.")


def evaluate_evidence(
    evidence: Mapping[str, Any],
    population: Mapping[str, Any],
    rule_summary: Mapping[str, Any],
    *,
    actual_block_count: int,
    scope_complete: bool = True,
    rule_supported: bool = True,
    unsupported_reason: str | None = None,
) -> tuple[Mapping[str, Any], ...]:
    observed_map = {
        ("broad_population_edge", "median_candidate_oos_net_profit_pct"): population.get("median"),
        ("broad_population_edge", "profitable_observation_share"): population.get("profitable_share"),
        ("broad_population_edge", "positive_monthly_population_medians"): population.get("positive_monthly_medians"),
        ("selected_strategy_viability", "six_month_headline_mean_oos_net_profit_pct"): rule_summary.get("top1_headline_mean"),
        ("selected_strategy_viability", "median_selected_oos_net_profit_pct"): rule_summary.get("top1_pooled_median"),
        ("selected_strategy_viability", "profitable_selected_observation_share"): rule_summary.get("profitable_selected_observation_share"),
        ("selected_strategy_viability", "positive_monthly_selected_means"): rule_summary.get("positive_monthly_selected_means"),
        ("selection_lift", "six_month_equal_weight_monthly_mean"): rule_summary.get("top1_lift_headline"),
        ("selection_lift", "positive_monthly_top1_paired_means"): rule_summary.get("positive_monthly_top1_lift"),
        ("selection_lift", "top5_equal_weight_monthly_mean"): rule_summary.get("top5_lift_headline"),
        ("selection_lift", "robust_top1_headline_mean"): rule_summary.get("robustness", {}).get("robust_lift_headline"),
    }
    rows: list[Mapping[str, Any]] = []
    for block_name in EVALUABLE_EVIDENCE_BLOCKS:
        block = evidence[block_name]
        for leaf_name, leaf in block.items():
            if block_name == "selection_lift" and leaf_name == "comparison":
                continue
            operator = leaf["operator"]
            threshold = leaf["value"]
            declared_of = leaf.get("of")
            observed = observed_map[(block_name, leaf_name)]
            reason = None
            if not rule_supported:
                status = "unavailable"
                reason = unsupported_reason or "official rule is unsupported for this dataset"
            elif not scope_complete:
                status = "unavailable"
                reason = "actual analysis is a partial intersection of the frozen scope"
            elif declared_of is not None and declared_of != actual_block_count:
                status = "unavailable"
                reason = (
                    f"declared denominator is {declared_of} blocks; actual scope has "
                    f"{actual_block_count}"
                )
            elif observed is None or not np.isfinite(float(observed)):
                status = "unavailable"
                reason = "computed quantity is unavailable"
            else:
                status = "pass" if _criterion_status(operator, float(observed), threshold) else "fail"
            rows.append(
                {
                    "criterion": f"{block_name}.{leaf_name}",
                    "operator": operator,
                    "threshold": threshold,
                    "of": declared_of,
                    "observed": observed,
                    "status": status,
                    "reason": reason,
                }
            )
    if len(rows) != 11:
        raise AnalysisError(f"strategy_lab_evidence_v1 must evaluate 11 leaves, got {len(rows)}.")
    return tuple(rows)


def _spearman(x: np.ndarray, y: np.ndarray) -> float | None:
    x = np.asarray(x, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    valid = np.isfinite(x) & np.isfinite(y)
    if valid.sum() < 8:
        return None
    rx, ry = average_ranks(x[valid]), average_ranks(y[valid])
    sx, sy = rx.std(), ry.std()
    if sx == 0.0 or sy == 0.0:
        return None
    return float(np.mean((rx - rx.mean()) * (ry - ry.mean())) / (sx * sy))


def _outcome(values: np.ndarray, rows: Sequence[int], limit: int) -> float | None:
    selected = np.asarray(rows[:limit], dtype=np.int64)
    if selected.size == 0:
        return None
    return _finite_mean(values[selected])


def _decision_row(
    *,
    rule: str,
    kind: str,
    ticker: str,
    window: Any,
    status: str,
    reason: str | None,
    selection: Selection | None,
    required_metrics: Sequence[str],
    is_view: Any,
    oos_matrix: np.ndarray,
    metric_index: Mapping[str, int],
    label: str,
) -> Mapping[str, Any]:
    if selection is None:
        rows: tuple[int, ...] = ()
        ids: tuple[int, ...] = ()
        keys: tuple[str, ...] = ()
        scores: tuple[float, ...] = ()
        is_net: tuple[float | None, ...] = ()
        tie_depth, tie_level = 0, "unavailable"
    else:
        rows = selection.row_indices
        ids = selection.candidate_ids
        keys = selection.semantic_keys
        scores = selection.scores
        is_net = selection.is_net_profit_pct
        tie_depth = selection.tie_depth_at_rank1
        tie_level = selection.tie_break_level_used
    oos_net = oos_matrix[:, metric_index["net_profit_pct"]]
    required_is = {
        metric: [
            (
                float(is_view.metrics[metric][row])
                if np.isfinite(is_view.metrics[metric][row])
                else None
            )
            for row in rows
        ]
        for metric in required_metrics
        if metric in is_view.metrics
    }
    matching_metrics = tuple(dict.fromkeys((*required_metrics, "net_profit_pct")))
    matching_oos = {
        metric: [
            (
                float(oos_matrix[row, metric_index[metric]])
                if metric in metric_index and np.isfinite(oos_matrix[row, metric_index[metric]])
                else None
            )
            for row in rows
        ]
        for metric in matching_metrics
    }
    top1 = _outcome(oos_net, rows, 1)
    top5 = _outcome(oos_net, rows, 5)
    top10 = _outcome(oos_net, rows, 10)
    ranks = [percentile_rank(oos_net, row) for row in rows]
    return {
        "rule": rule,
        "rule_kind": kind,
        "result_label": label,
        "ticker": ticker,
        "window_id": window.window_id,
        "is_start": window.is_start,
        "is_end": window.is_end,
        "oos_start": window.oos_start,
        "oos_end": window.oos_end,
        "status": status,
        "reason": reason,
        "selected_candidate_ids": list(ids),
        "selected_semantic_keys": list(keys),
        "ordered_scores": list(scores),
        "ordered_is_net_profit_pct": list(is_net),
        "required_is_metrics": required_is,
        "matching_oos_metrics": matching_oos,
        "tie_depth_at_rank1": tie_depth,
        "tie_break_level_used": tie_level,
        "top1_oos_net_profit_pct": top1,
        "top5_oos_net_profit_pct": top5,
        "top10_oos_net_profit_pct": top10,
        "top1_oos_percentile": ranks[0] if ranks else None,
        "top5_mean_individual_oos_percentile": _finite_mean(np.asarray(ranks[:5])),
    }


def _validate_registry(dataset: AnalysisDataset) -> tuple[tuple[str, ...], tuple[str, ...]]:
    registry = dataset.contract.rule_registry
    selectable = tuple(registry.get("selectable_rules", ()))
    diagnostics = tuple(registry.get("non_nominatable_diagnostics", ()))
    unknown_selectable = [name for name in selectable if name not in RULE_REQUIREMENTS]
    unknown_diagnostics = [
        name for name in diagnostics if name not in FORMULA_DIAGNOSTICS + OOS_DIAGNOSTICS
    ]
    if unknown_selectable or unknown_diagnostics:
        raise AnalysisError(
            "pre-registered rule lacks a formula: "
            f"{unknown_selectable + unknown_diagnostics}."
        )
    if registry.get("baseline_rule") not in selectable:
        raise AnalysisError("baseline rule is not selectable.")
    return selectable, diagnostics


def evaluate_scope(
    dataset: AnalysisDataset,
    scope: ResolvedScope,
    *,
    custom_rules: Sequence[CustomRule] = (),
) -> AnalysisResult:
    if dataset.contract.descriptive and custom_rules:
        raise AnalysisError("descriptive v3 currently supports the frozen built-in rules only.")
    selectable, declared_diagnostics = _validate_registry(dataset)
    formula_rules = selectable + tuple(
        name for name in declared_diagnostics if name in FORMULA_DIAGNOSTICS
    )
    missing_by_rule = {
        name: tuple(metric for metric in RULE_REQUIREMENTS[name] if metric not in dataset.metric_index)
        for name in formula_rules
    }
    rule_states: dict[str, Mapping[str, Any]] = {
        name: {
            "status": "unsupported_for_dataset" if missing else "supported",
            "missing_metrics": list(missing),
            "kind": "selectable" if name in selectable else "diagnostic",
        }
        for name, missing in missing_by_rule.items()
    }
    for name in declared_diagnostics:
        if name in OOS_DIAGNOSTICS:
            missing = () if "net_profit_pct" in dataset.metric_index else ("net_profit_pct",)
            rule_states[name] = {
                "status": "unsupported_for_dataset" if missing else "supported",
                "missing_metrics": list(missing),
                "kind": "diagnostic",
            }
    for custom in custom_rules:
        if custom.name in rule_states:
            raise AnalysisError(f"custom rule name collides with a registered rule: {custom.name!r}.")
        rule_states[custom.name] = {
            "status": "supported",
            "missing_metrics": [],
            "kind": "custom",
            "version": custom.version,
            "result_label": "exploratory",
        }
    if "net_profit_pct" not in dataset.metric_index:
        raise AnalysisError("population analysis requires net_profit_pct.")
    active_formula_rules = tuple(
        name for name in formula_rules if not missing_by_rule[name]
    )
    active_selectable_rules = tuple(
        name for name in selectable if not missing_by_rule[name]
    )
    active_oos_diagnostics = tuple(
        name
        for name in declared_diagnostics
        if name in OOS_DIAGNOSTICS and rule_states[name]["status"] == "supported"
    )
    if not active_selectable_rules and not custom_rules:
        raise AnalysisError(
            "analysis has no supported official rule and no valid custom rule to evaluate."
        )
    neighbours = star_neighbours(dataset.geometry)
    ticker_position = {ticker: index for index, ticker in enumerate(scope.tickers)}
    block_position = {window.block_key: index for index, window in enumerate(scope.windows)}
    ticker_count, window_count, candidate_count = (
        len(scope.tickers),
        len(scope.windows),
        dataset.geometry.count,
    )
    all_rule_names = (
        active_formula_rules
        + active_oos_diagnostics
        + tuple(custom.name for custom in custom_rules)
    )
    pair_values = {
        name: np.full((ticker_count, window_count, 3), np.nan, dtype=np.float64)
        for name in all_rule_names
    }
    pair_ranks = {
        name: np.full((ticker_count, window_count, 2), np.nan, dtype=np.float64)
        for name in all_rule_names
    }
    is_net_grid = np.full((ticker_count, window_count, candidate_count), np.nan)
    oos_net_grid = np.full_like(is_net_grid, np.nan)
    metric_finite = {
        segment: {metric: 0 for metric in dataset.metric_axis}
        for segment in ("is", "oos")
    }
    descriptive_metrics = tuple(
        metric
        for metric in (
            "total_trades",
            "max_drawdown_pct",
            "max_drawdown_mtm_pct",
            "profit_factor",
            "win_rate_pct",
            "sharpe_daily",
            "sqn",
            "rejected_fill_count",
            "zero_size_entry_count",
            "invalid_stop_distance_count",
            "max_required_leverage",
            "margin_reject_count",
        )
        if metric in dataset.metric_index
    )
    metric_values: dict[str, dict[str, list[np.ndarray]]] = {
        segment: {metric: [] for metric in descriptive_metrics}
        for segment in ("is", "oos")
    }
    pair_decisions: list[Mapping[str, Any]] = []
    gate = float(dataset.contract.rule_registry["minimum_completed_trades"])
    flag_values: list[np.ndarray] = []
    guardrail_faults = {"zero_size_entry_count": 0, "invalid_stop_distance_count": 0}
    gross_profit_sum = 0.0
    gross_loss_sum = 0.0
    win_rates: list[np.ndarray] = []
    gate_eligible_count = 0
    for window in scope.windows:
        pending: dict[tuple[str, str], tuple[Selection | None, RuleResult | None]] = {}
        window_views = dataset.load_is_window(scope, window)
        for ticker in scope.tickers:
            view = window_views[ticker]
            ti, wi = ticker_position[ticker], block_position[window.block_key]
            is_net = np.asarray(view.metrics["net_profit_pct"], dtype=np.float64)
            is_net_grid[ti, wi] = is_net
            trade_values = view.metrics.get("total_trades")
            if trade_values is not None:
                gate_eligible_count += int(
                    np.sum(np.isfinite(trade_values) & (trade_values >= gate))
                )
            for metric, values in view.metrics.items():
                metric_finite["is"][metric] += int(np.isfinite(values).sum())
                if metric in metric_values["is"]:
                    metric_values["is"][metric].append(
                        np.asarray(values, dtype=np.float64).copy()
                    )
            for name in active_formula_rules:
                result = evaluate_rule(
                    name, view, dataset.geometry, gate, neighbours=neighbours
                )
                selection = select_candidates(result, is_net, dataset.geometry)
                pending[(ticker, name)] = (selection, result)
            for custom in custom_rules:
                result = evaluate_custom_rule(
                    custom.name,
                    custom.version,
                    custom.function,
                    view,
                    dataset.geometry,
                    {"minimum_completed_trades": gate, "rule_version": "exploratory"},
                )
                selection = select_candidates(result, is_net, dataset.geometry)
                pending[(ticker, custom.name)] = (selection, result)
        # OOS is not loaded until every selection for this window is frozen above.
        oos_matrices = dataset.load_oos_window(scope, window)
        for ticker in scope.tickers:
            matrix = oos_matrices[ticker]
            ti, wi = ticker_position[ticker], block_position[window.block_key]
            oos_net = matrix[:, dataset.metric_index["net_profit_pct"]]
            oos_net_grid[ti, wi] = oos_net
            for metric, index in dataset.metric_index.items():
                metric_finite["oos"][metric] += int(np.isfinite(matrix[:, index]).sum())
                if metric in metric_values["oos"]:
                    metric_values["oos"][metric].append(matrix[:, index].copy())
            if "flags" in dataset.metric_index:
                flag_values.append(matrix[:, dataset.metric_index["flags"]].copy())
            for metric in guardrail_faults:
                if metric in dataset.metric_index:
                    values = matrix[:, dataset.metric_index[metric]]
                    guardrail_faults[metric] += int(np.sum(np.isfinite(values) & (values != 0)))
            if "gross_profit" in dataset.metric_index:
                values = matrix[:, dataset.metric_index["gross_profit"]]
                gross_profit_sum += float(np.nansum(values))
            if "gross_loss" in dataset.metric_index:
                values = matrix[:, dataset.metric_index["gross_loss"]]
                gross_loss_sum += float(np.nansum(values))
            if "win_rate_pct" in dataset.metric_index:
                values = matrix[:, dataset.metric_index["win_rate_pct"]]
                if "total_trades" in dataset.metric_index:
                    trades = matrix[:, dataset.metric_index["total_trades"]]
                    values = values[np.isfinite(trades) & (trades > 0.0)]
                win_rates.append(values.copy())
            for name in active_formula_rules + tuple(custom.name for custom in custom_rules):
                selection, result = pending[(ticker, name)]
                view = window_views[ticker]
                if selection is None or not selection.row_indices:
                    status = "unavailable_pair"
                    reason = "no eligible finite candidate"
                else:
                    status = "selected"
                    reason = None
                row = _decision_row(
                    rule=name,
                    kind=rule_states[name]["kind"],
                    ticker=ticker,
                    window=window,
                    status=status,
                    reason=reason,
                    selection=selection,
                    required_metrics=() if result is None else result.required_metrics,
                    is_view=view,
                    oos_matrix=matrix,
                    metric_index=dataset.metric_index,
                    label="exploratory" if name in {item.name for item in custom_rules} else "pre_registered",
                )
                pair_decisions.append(row)
                for column, key in enumerate(("top1_oos_net_profit_pct", "top5_oos_net_profit_pct", "top10_oos_net_profit_pct")):
                    value = row[key]
                    if value is not None:
                        pair_values[name][ti, wi, column] = value
                for column, key in enumerate(("top1_oos_percentile", "top5_mean_individual_oos_percentile")):
                    value = row[key]
                    if value is not None:
                        pair_ranks[name][ti, wi, column] = value
            if "population_no_skill" in declared_diagnostics:
                mean = _finite_mean(oos_net)
                row = {
                    "rule": "population_no_skill",
                    "rule_kind": "diagnostic",
                    "result_label": "non_deployable_population_baseline",
                    "ticker": ticker,
                    "window_id": window.window_id,
                    "is_start": window.is_start,
                    "is_end": window.is_end,
                    "oos_start": window.oos_start,
                    "oos_end": window.oos_end,
                    "status": "available" if mean is not None else "unavailable_pair",
                    "reason": None if mean is not None else "no finite OOS Net Profit",
                    "selected_candidate_ids": [],
                    "selected_semantic_keys": [],
                    "ordered_scores": [],
                    "ordered_is_net_profit_pct": [],
                    "required_is_metrics": {},
                    "matching_oos_metrics": {},
                    "tie_depth_at_rank1": 0,
                    "tie_break_level_used": "not_applicable",
                    "top1_oos_net_profit_pct": mean,
                    "top5_oos_net_profit_pct": mean,
                    "top10_oos_net_profit_pct": mean,
                    "top1_oos_percentile": None,
                    "top5_mean_individual_oos_percentile": None,
                }
                pair_decisions.append(row)
                if mean is not None:
                    pair_values["population_no_skill"][ti, wi, :] = mean
            for name, maximize in (("oos_oracle", True), ("oos_anti_oracle", False)):
                if name not in declared_diagnostics:
                    continue
                finite_rows = np.flatnonzero(np.isfinite(oos_net)).tolist()
                finite_rows.sort(
                    key=lambda index: (
                        -oos_net[index] if maximize else oos_net[index],
                        dataset.geometry.semantic_keys[index],
                        int(dataset.geometry.candidate_ids[index]),
                    )
                )
                if finite_rows:
                    row_index = finite_rows[0]
                    selection = Selection(
                        (row_index,),
                        (int(dataset.geometry.candidate_ids[row_index]),),
                        (dataset.geometry.semantic_keys[row_index],),
                        (float(oos_net[row_index]),),
                        (float(is_net_grid[ti, wi, row_index]) if np.isfinite(is_net_grid[ti, wi, row_index]) else None,),
                        int(np.sum(oos_net == oos_net[row_index])),
                        "hindsight_oos_net_profit_pct",
                    )
                else:
                    selection = None
                view = window_views[ticker]
                row = _decision_row(
                    rule=name,
                    kind="diagnostic",
                    ticker=ticker,
                    window=window,
                    status="hindsight" if selection else "unavailable_pair",
                    reason=None if selection else "no finite OOS Net Profit",
                    selection=selection,
                    required_metrics=(),
                    is_view=view,
                    oos_matrix=matrix,
                    metric_index=dataset.metric_index,
                    label="diagnostic_hindsight_non_deployable",
                )
                pair_decisions.append(row)
                if selection:
                    value = row["top1_oos_net_profit_pct"]
                    pair_values[name][ti, wi, :] = value
        pending.clear()
        window_views.clear()
        oos_matrices.clear()
        selection = result = view = matrix = oos_net = None
        del pending, window_views, oos_matrices
    baseline_name = str(dataset.contract.rule_registry["baseline_rule"])
    baseline = pair_values.get(
        baseline_name,
        np.full((ticker_count, window_count, 3), np.nan, dtype=np.float64),
    )
    monthly_rows: list[Mapping[str, Any]] = []
    rule_summaries: dict[str, Mapping[str, Any]] = {}
    from .descriptive import not_requested, interval_not_requested
    descriptive = dataset.contract.descriptive
    uncertainty = not_requested() if descriptive else dataset.contract.evidence["uncertainty"]
    procedure = not_requested() if descriptive else dataset.contract.evidence["outlier_procedure"]
    population_flat = oos_net_grid.ravel()
    population_monthly_mean: list[float | None] = []
    population_monthly_median: list[float | None] = []
    population_monthly_profitable: list[float | None] = []
    for wi, window in enumerate(scope.windows):
        values = oos_net_grid[:, wi, :].ravel()
        population_monthly_mean.append(
            _finite_mean(np.asarray([_finite_mean(row) if np.isfinite(row).any() else np.nan
                                    for row in oos_net_grid[:, wi, :]], dtype=float))
            if descriptive else _finite_mean(values)
        )
        population_monthly_median.append(_finite_median(values))
        population_monthly_profitable.append(profitable_share(values))
        monthly_rows.append(
            {
                "row_kind": "population",
                "rule": "population",
                "window_id": window.window_id,
                "oos_start": window.oos_start,
                "oos_end": window.oos_end,
                "top1_mean": None,
                "top1_median": None,
                "top1_profitable_share": None,
                "top1_lift_mean": None,
                "top5_lift_mean": None,
                "population_mean": population_monthly_mean[-1],
                "population_median": population_monthly_median[-1],
                "population_profitable_share": population_monthly_profitable[-1],
            }
        )
    population = {
        "observation_count": int(population_flat.size),
        "finite_count": int(np.isfinite(population_flat).sum()),
        "unavailable_count": int((~np.isfinite(population_flat)).sum()),
        "mean": (_finite_mean(np.asarray([value if value is not None else np.nan for value in population_monthly_mean]))
                 if descriptive else _finite_mean(population_flat)),
        "median": _finite_median(population_flat),
        "quartile_25": _finite_quantile(population_flat, 0.25),
        "quartile_75": _finite_quantile(population_flat, 0.75),
        "worst_decile": _finite_quantile(population_flat, 0.10),
        "profitable_share": profitable_share(population_flat),
        "zero_share": float(np.mean(np.isfinite(population_flat) & (population_flat == 0.0))),
        "monthly_means": population_monthly_mean,
        "monthly_medians": population_monthly_median,
        "monthly_profitable_shares": population_monthly_profitable,
        "positive_monthly_medians": int(
            sum(value is not None and value > 0.0 for value in population_monthly_median)
        ),
    }
    for name in all_rule_names:
        values = pair_values[name]
        top1, top5, top10 = values[:, :, 0], values[:, :, 1], values[:, :, 2]
        top1_monthly, top1_headline = monthly_headline(top1)
        top5_monthly, top5_headline = monthly_headline(top5)
        top10_monthly, top10_headline = monthly_headline(top10)
        lift1 = top1 - baseline[:, :, 0]
        lift5 = top5 - baseline[:, :, 1]
        lift1_monthly, lift1_headline = monthly_headline(lift1)
        lift5_monthly, lift5_headline = monthly_headline(lift5)
        selected_pairs = int(np.isfinite(top1).sum())
        if descriptive:
            robustness = {**not_requested(), "robust_lift_headline": None}
            bootstraps = {name: interval_not_requested() for name in ("top1_absolute", "top1_lift", "top5_lift", "robust_top1_lift")}
        else:
            robustness = outlier_robustness(
                lift1,
                top1,
                scope.tickers,
                procedure,
                ticker_cell_count=scope.ticker_cell_count,
            )
            bootstraps = {
                "top1_absolute": {
                    "effect": top1_headline,
                    **descriptive_bootstrap(top1_monthly, uncertainty),
                },
                "top1_lift": {
                    "effect": lift1_headline,
                    **descriptive_bootstrap(lift1_monthly, uncertainty),
                },
                "top5_lift": {
                    "effect": lift5_headline,
                    **descriptive_bootstrap(lift5_monthly, uncertainty),
                },
                "robust_top1_lift": {
                    "effect": robustness["robust_lift_headline"],
                    **descriptive_bootstrap(
                        robustness["robust_monthly_lift_descriptive"], uncertainty
                    ),
                },
            }
        summary = {
            "rule_kind": rule_states[name]["kind"],
            "rule_status": rule_states[name]["status"],
            "selected_pairs": selected_pairs,
            "total_pairs": scope.total_pairs,
            "availability_share": float(selected_pairs / scope.total_pairs) if scope.total_pairs else None,
            "unavailable_pair_count": scope.total_pairs - selected_pairs,
            "unavailable_pair_reason": (
                "no eligible finite candidate"
                if selected_pairs < scope.total_pairs
                else None
            ),
            "top1_headline_mean": top1_headline,
            "top1_pooled_median": _finite_median(top1),
            "profitable_selected_observation_share": profitable_share(
                top1, denominator=scope.total_pairs
            ),
            "top1_monthly_means": top1_monthly.tolist(),
            "positive_monthly_selected_means": int(np.sum(top1_monthly > 0.0)),
            "top5_headline_mean": top5_headline,
            "top10_headline_mean": top10_headline,
            "top1_lift_headline": lift1_headline,
            "top1_lift_monthly_means": lift1_monthly.tolist(),
            "positive_monthly_top1_lift": int(np.sum(lift1_monthly > 0.0)),
            "top5_lift_headline": lift5_headline,
            "top5_lift_monthly_means": lift5_monthly.tolist(),
            "top1_oos_percentile_mean": _finite_mean(pair_ranks[name][:, :, 0]),
            "top5_oos_percentile_mean": _finite_mean(pair_ranks[name][:, :, 1]),
            "robustness": robustness,
            "bootstraps": bootstraps,
            "bootstrap_top1_lift": bootstraps["top1_lift"],
        }
        rule_summaries[name] = summary
        for wi, window in enumerate(scope.windows):
            monthly_rows.append(
                {
                    "row_kind": "rule",
                    "rule": name,
                    "window_id": window.window_id,
                    "oos_start": window.oos_start,
                    "oos_end": window.oos_end,
                    "top1_mean": float(top1_monthly[wi]) if np.isfinite(top1_monthly[wi]) else None,
                    "top1_median": _finite_median(top1[:, wi]),
                    "top1_profitable_share": profitable_share(
                        top1[:, wi], denominator=ticker_count
                    ),
                    "top1_lift_mean": float(lift1_monthly[wi]) if np.isfinite(lift1_monthly[wi]) else None,
                    "top5_lift_mean": float(lift5_monthly[wi]) if np.isfinite(lift5_monthly[wi]) else None,
                    "population_mean": None,
                    "population_median": None,
                    "population_profitable_share": None,
                }
            )
    evidence_by_rule = {}
    if descriptive:
        evidence_by_rule = {name: not_requested() for name in selectable}
    else:
        for name in selectable:
            supported = name in rule_summaries
            missing = rule_states[name]["missing_metrics"]
            evidence_by_rule[name] = evaluate_evidence(
                dataset.contract.evidence,
                population,
                rule_summaries.get(name, {}),
                actual_block_count=len(scope.block_keys),
                scope_complete=not scope.is_partial,
                rule_supported=supported,
                unsupported_reason=(
                    f"official rule is unsupported; missing metrics: {', '.join(missing)}"
                    if not supported
                    else None
                ),
            )
    flags = np.concatenate(flag_values) if flag_values else np.empty(0)
    finite_flags = flags[np.isfinite(flags)].astype(np.int64)
    known_mask = 2 | 4 | 8
    unknown_bits = int(np.bitwise_or.reduce(finite_flags & ~known_mask, initial=0))
    known_flag_bits = {
        "rejected_fill": 2,
        "invalid_stop_distance": 4,
        "zero_size_entry": 8,
    }
    known_flag_bit_counts = {
        name: int(np.sum((finite_flags & bit) != 0))
        for name, bit in known_flag_bits.items()
    }
    total_observations = ticker_count * window_count * candidate_count
    metric_summaries: dict[str, dict[str, Mapping[str, Any]]] = {"is": {}, "oos": {}}
    for segment in ("is", "oos"):
        for metric in descriptive_metrics:
            values = np.concatenate(metric_values[segment][metric])
            finite_values = values[np.isfinite(values)]
            metric_summaries[segment][metric] = {
                "finite_count": int(finite_values.size),
                "unavailable_count": int(values.size - finite_values.size),
                "mean": _finite_mean(finite_values),
                "median": _finite_median(finite_values),
                "quartile_25": _finite_quantile(finite_values, 0.25),
                "quartile_75": _finite_quantile(finite_values, 0.75),
                "minimum": float(np.min(finite_values)) if finite_values.size else None,
                "maximum": float(np.max(finite_values)) if finite_values.size else None,
            }
    trades_index = dataset.metric_index.get("total_trades")
    if trades_index is None:
        gate_share = None
    else:
        gate_share = float(gate_eligible_count / total_observations)
    candidate_mean = np.nanmean(oos_net_grid.reshape(-1, candidate_count), axis=0)
    candidate_monthly = np.nanmean(oos_net_grid, axis=0)
    fixed_headlines = np.nanmean(candidate_monthly, axis=0)
    # The preserved diagnostic defines halves over canonical-symbol order, not
    # the inventory's execution order.
    canonical_order = [ticker_position[ticker] for ticker in sorted(scope.tickers)]
    ordered_ticker_grid = oos_net_grid[canonical_order]
    ticker_half_a = np.nanmean(ordered_ticker_grid[0::2].reshape(-1, candidate_count), axis=0)
    ticker_half_b = (np.nanmean(ordered_ticker_grid[1::2].reshape(-1, candidate_count), axis=0)
                     if ticker_count > 1 else np.full(candidate_count, np.nan))
    split = max(1, window_count // 2)
    time_half_a = np.nanmean(oos_net_grid[:, :split].reshape(-1, candidate_count), axis=0)
    time_half_b = (np.nanmean(oos_net_grid[:, split:].reshape(-1, candidate_count), axis=0)
                  if window_count > split else np.full(candidate_count, np.nan))
    diagnostics = {
        "metric_finite_counts": metric_finite,
        "metric_summaries": metric_summaries,
        "trade_counts": {
            segment: {
                **metric_summaries[segment]["total_trades"],
                "zero_share": float(
                    np.mean(
                        np.concatenate(metric_values[segment]["total_trades"]) == 0.0
                    )
                ),
                "at_least_gate_share": float(
                    np.mean(
                        np.concatenate(metric_values[segment]["total_trades"]) >= gate
                    )
                ),
                "at_least_30_share": float(
                    np.mean(
                        np.concatenate(metric_values[segment]["total_trades"]) >= 30.0
                    )
                ),
            }
            for segment in ("is", "oos")
            if "total_trades" in metric_values[segment]
        },
        "gate_retained_candidate_share": gate_share,
        "gate_pairs_with_no_candidate": int(
            sum(
                row["status"] == "unavailable_pair"
                for row in pair_decisions
                if row["rule"] == "trade_gate15_profit"
            )
        ),
        "flags_nonzero_share": (
            float(np.mean(finite_flags != 0)) if finite_flags.size else None
        ),
        "known_flag_bits": known_flag_bits,
        "known_flag_bit_counts": known_flag_bit_counts,
        "unknown_flag_bits": unknown_bits,
        "guardrail_fault_observation_counts": guardrail_faults,
        "guardrails": {
            metric: {
                "nonzero_observation_count": int(
                    np.sum(
                        np.isfinite(np.concatenate(metric_values["oos"][metric]))
                        & (np.concatenate(metric_values["oos"][metric]) != 0.0)
                    )
                ),
                "nonzero_share": float(
                    np.mean(np.concatenate(metric_values["oos"][metric]) != 0.0)
                ),
                "maximum": metric_summaries["oos"][metric]["maximum"],
            }
            for metric in (
                "rejected_fill_count",
                "zero_size_entry_count",
                "invalid_stop_distance_count",
                "max_required_leverage",
                "margin_reject_count",
            )
            if metric in metric_values["oos"]
        },
        "oos_aggregate_profit_factor": (
            float(gross_profit_sum / gross_loss_sum) if gross_loss_sum > 0 else None
        ),
        "oos_mean_win_rate_pct": (
            _finite_mean(np.concatenate(win_rates)) if win_rates else None
        ),
        "candidate_rank_split_half_tickers_spearman": _spearman(ticker_half_a, ticker_half_b),
        "candidate_rank_split_half_time_spearman": _spearman(time_half_a, time_half_b),
        "pooled_is_to_oos_spearman": _spearman(is_net_grid.ravel(), oos_net_grid.ravel()),
        "best_fixed_candidate_headline": _finite_mean(
            np.asarray([np.nanmax(fixed_headlines)])
        ),
        "candidate_mean_oos_best": float(np.nanmax(candidate_mean)),
        "candidate_mean_oos_worst": float(np.nanmin(candidate_mean)),
    }
    per_ticker = []
    for ticker, ti in ticker_position.items():
        values = oos_net_grid[ti].ravel()
        per_ticker.append(
            {
                "ticker": ticker,
                "mean": _finite_mean(values),
                "median": _finite_median(values),
                "profitable_share": profitable_share(values),
            }
        )
    metadata = {
        "dataset": {
            "path": str(dataset.root),
            "manifest_sha256": dataset.manifest_sha256,
            "schema_version": dataset.schema_version,
            "scope": dataset.scope_label,
            "status": dataset.status,
        },
        "analysis_scope": {
            "name": scope.name,
            "ticker_cell": scope.ticker_cell,
            "tickers": list(scope.tickers),
            "ticker_count": ticker_count,
            "ticker_cell_count": scope.ticker_cell_count,
            "actual_calendar_block_count": len(scope.block_keys),
            "declared_window_ids": list(scope.declared_window_ids),
            "actual_window_ids": [window.window_id for window in scope.windows],
            "missing_window_ids": list(scope.missing_window_ids),
            "is_partial": scope.is_partial,
            "utc_blocks": [
                {
                    "window_id": window.window_id,
                    "block_key": list(window.block_key),
                    "is_start": window.is_start,
                    "is_end": window.is_end,
                    "oos_start": window.oos_start,
                    "oos_end": window.oos_end,
                }
                for window in scope.windows
            ],
            "requires_unlock": scope.requires_unlock,
            "unlock_evidence": scope.unlock_evidence,
        },
        "contracts": {
            "rule_registry_version": dataset.contract.rule_registry["version"],
            "evidence_criteria_version": dataset.contract.evidence_version,
            "observation_contract": dataset.contract.observation,
            "rule_registry": dataset.contract.rule_registry,
            "split": dataset.contract.split,
            "uncertainty": uncertainty,
            "outlier_procedure": procedure,
            "maximum_nominated_rules": dataset.contract.maximum_nominated_rules,
            "primary_comparison": dataset.contract.primary_comparison,
        },
        "git": git_facts(),
        "rule_states": rule_states,
        "omissions": [
            "ticker allocation and policy comparison are deferred to Phase 3L-B",
            "no nomination or strategy-quality interpretation is performed",
        ],
        "load_audit": {
            "group_segment_load_count": len(dataset.access_log),
            "ticker_count": len({entry[0] for entry in dataset.access_log}),
            "window_ids": sorted({entry[1] for entry in dataset.access_log}),
            "segments": sorted({entry[2] for entry in dataset.access_log}),
            "outside_scope_loads": [
                list(entry)
                for entry in dataset.access_log
                if entry[0] not in scope.tickers
                or entry[1] not in {window.window_id for window in scope.windows}
            ],
        },
    }
    summary = {
        "dimensions": {
            "tickers": ticker_count,
            "windows": window_count,
            "candidates": candidate_count,
            "segments": len(dataset.segment_axis),
            "metrics": len(dataset.metric_axis),
            "pairs": scope.total_pairs,
        },
        "population": population,
        "per_ticker_population": per_ticker,
        "rules": rule_summaries,
        "evidence_by_selectable_rule": evidence_by_rule,
        "diagnostics": diagnostics,
    }
    if descriptive:
        units = {"schema_version": "strategy_lab_analysis_v2", "period_unit": "oos_block",
                 "oos_period_months": dataset.run_spec["generation"]["windows"]["oos_period_months"]}
        capabilities = {name: not_requested() for name in ("evidence", "nominations", "bootstrap", "outlier_removal")}
        metadata.update(**units, capabilities=capabilities)
        summary.update(**units, capabilities=capabilities)
    pair_decisions.sort(key=lambda row: (row["window_id"], row["ticker"], row["rule"]))
    monthly_rows.sort(key=lambda row: (row["window_id"], row["row_kind"], row["rule"]))
    return AnalysisResult(metadata, summary, tuple(pair_decisions), tuple(monthly_rows))
