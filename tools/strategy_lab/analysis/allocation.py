"""Leakage-safe fixed-capacity ticker allocation for Strategy Lab datasets."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from .dataset import AnalysisDataset, AnalysisError, ResolvedScope, Window, git_facts
from .evaluate import descriptive_bootstrap
from .descriptive import random_settings, not_requested, interval_not_requested
from .json_utils import canonical_json_bytes
from .rules import RULE_REQUIREMENTS, evaluate_rule, select_candidates, star_neighbours


@dataclass(frozen=True)
class DatasetInput:
    label: str
    dataset: AnalysisDataset
    scope: ResolvedScope


@dataclass(frozen=True)
class SelectedISTickerView:
    dataset_label: str
    ticker: str
    window_id: int
    is_start_utc: str
    is_end_utc: str
    oos_start_utc: str
    oos_end_utc: str
    candidate_rule: str
    candidate_id: int
    semantic_key: str
    candidate_rule_score: float
    is_net_profit_pct: float
    is_total_trades: float | None


@dataclass(frozen=True)
class TickerScorer:
    name: str
    version: str
    configuration: Mapping[str, Any]
    function: Callable[[SelectedISTickerView, Mapping[str, Any]], Any]
    exploratory: bool = True


@dataclass(frozen=True)
class AllocationResult:
    run_metadata: Mapping[str, Any]
    summary: Mapping[str, Any]
    pair_decisions: tuple[Mapping[str, Any], ...]
    monthly_results: tuple[Mapping[str, Any], ...]
    ticker_allocations: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class _Decision:
    view: SelectedISTickerView
    row_index: int
    ticker_score: float


def _official_score(
    view: SelectedISTickerView, context: Mapping[str, Any]
) -> float | None:
    del context
    return view.is_net_profit_pct if math.isfinite(view.is_net_profit_pct) else None


OFFICIAL_TICKER_SCORER = TickerScorer(
    name="selected_is_net_profit",
    version="strategy_lab_ticker_score_v1",
    configuration={},
    function=_official_score,
    exploratory=False,
)


def _exact_positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise AnalysisError(f"{label} must be a positive integer; booleans are invalid.")
    return value


def _scorer_facts(scorer: TickerScorer) -> Mapping[str, Any]:
    if not isinstance(scorer.name, str) or not scorer.name:
        raise AnalysisError("ticker scorer name must be a non-empty string.")
    if not isinstance(scorer.version, str) or not scorer.version:
        raise AnalysisError("ticker scorer version must be a non-empty string.")
    if not isinstance(scorer.exploratory, bool) or not callable(scorer.function):
        raise AnalysisError("ticker scorer metadata/function is invalid.")
    if scorer is not OFFICIAL_TICKER_SCORER and not scorer.exploratory:
        raise AnalysisError("custom ticker scorers must declare exploratory=true.")
    if not isinstance(scorer.configuration, Mapping):
        raise AnalysisError("ticker scorer configuration must be a JSON object.")
    try:
        configuration = json.loads(canonical_json_bytes(dict(scorer.configuration)))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise AnalysisError(f"ticker scorer configuration is not strict JSON: {exc}") from None
    identity = {
        "name": scorer.name,
        "version": scorer.version,
        "configuration": configuration,
        "exploratory": scorer.exploratory,
    }
    return {
        **identity,
        "identity_sha256": hashlib.sha256(canonical_json_bytes(identity)).hexdigest(),
    }


def _ticker_score(
    scorer: TickerScorer,
    view: SelectedISTickerView,
    context: Mapping[str, Any],
) -> float | None:
    try:
        raw = scorer.function(view, context)
    except Exception as exc:
        raise AnalysisError(
            f"ticker scorer {scorer.name!r} failed for {view.ticker} block "
            f"{(view.oos_start_utc, view.oos_end_utc)!r}: {exc}"
        ) from exc
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float, np.integer, np.floating)):
        raise AnalysisError(
            f"ticker scorer {scorer.name!r} returned a non-scalar value for {view.ticker}."
        )
    value = float(raw)
    if not math.isfinite(value):
        raise AnalysisError(
            f"ticker scorer {scorer.name!r} returned a non-finite value for {view.ticker}."
        )
    return value


def _rank(decisions: Sequence[_Decision], *, bottom: bool = False) -> tuple[_Decision, ...]:
    def key(item: _Decision) -> tuple[Any, ...]:
        trades = item.view.is_total_trades
        return (
            item.ticker_score if bottom else -item.ticker_score,
            1 if trades is None or not math.isfinite(trades) else 0,
            0.0 if trades is None or not math.isfinite(trades) else -trades,
            item.view.ticker,
        )

    return tuple(sorted(decisions, key=key))


def _boundary_facts(ranked: Sequence[_Decision], k: int) -> Mapping[str, Any]:
    selected_count = min(k, len(ranked))
    if selected_count == 0:
        return {"boundary_tie_depth": 0, "boundary_tie_break_level": "unavailable"}
    if selected_count == len(ranked):
        return {"boundary_tie_depth": 1, "boundary_tie_break_level": "not_applicable"}
    boundary = ranked[selected_count - 1]
    excluded = ranked[selected_count]
    depth = sum(item.ticker_score == boundary.ticker_score for item in ranked)
    if boundary.ticker_score != excluded.ticker_score:
        level = "ticker_score"
    else:
        left, right = boundary.view.is_total_trades, excluded.view.is_total_trades
        left_key = (left is None or not math.isfinite(left), -(left or 0.0))
        right_key = (right is None or not math.isfinite(right), -(right or 0.0))
        level = "is_total_trades" if left_key != right_key else "canonical_ticker"
    return {"boundary_tie_depth": depth, "boundary_tie_break_level": level}


def random_percentile_fraction(random_returns: Sequence[float], observed: float) -> float:
    values = np.asarray(random_returns, dtype=np.float64)
    if values.ndim != 1 or values.size == 0 or not np.all(np.isfinite(values)):
        raise AnalysisError("random percentile requires one non-empty finite vector.")
    if isinstance(observed, bool) or not math.isfinite(float(observed)):
        raise AnalysisError("random percentile observed return must be finite.")
    return float((np.sum(values < observed) + 0.5 * np.sum(values == observed)) / values.size)


def _random_summary(
    *,
    dataset_input: DatasetInput,
    candidate_rule: str,
    scorer_facts: Mapping[str, Any],
    k: int,
    window: Window,
    returns: Sequence[float],
    observed: float | None,
) -> Mapping[str, Any]:
    uncertainty = random_settings(dataset_input.dataset.contract)
    base_seed = uncertainty.get("seed")
    draws = uncertainty.get("resamples")
    if (
        isinstance(base_seed, bool)
        or not isinstance(base_seed, int)
        or isinstance(draws, bool)
        or not isinstance(draws, int)
        or draws < 1
    ):
        raise AnalysisError("allocation random control requires persisted integer seed/resamples.")
    payload = {
        "schema": "strategy_lab_random_k_v1",
        "base_seed": base_seed,
        "dataset_manifest_sha256": dataset_input.dataset.manifest_sha256.lower(),
        "dataset_label": dataset_input.label,
        "candidate_rule": candidate_rule,
        "ticker_scorer": {
            "name": scorer_facts["name"],
            "version": scorer_facts["version"],
            "configuration": scorer_facts["configuration"],
        },
        "allocation_kind": "random_k",
        "k": k,
        "oos_start_utc": window.oos_start,
        "oos_end_utc": window.oos_end,
    }
    payload_bytes = canonical_json_bytes(payload)
    digest = hashlib.sha256(payload_bytes).digest()
    payload_sha256 = digest.hex()
    derived_seed = int.from_bytes(digest[:8], byteorder="big", signed=False)
    values = np.asarray(returns, dtype=np.float64)
    if values.size == 0:
        return {
            "draw_count": draws,
            "base_seed": base_seed,
            "derived_seed": derived_seed,
            "seed_payload_sha256": payload_sha256,
            "random_mean_pct": None,
            "random_median_pct": None,
            "top_k_random_percentile_fraction": None,
            "status": "unavailable",
            "reason": "zero available tickers",
        }
    if not np.all(np.isfinite(values)) or observed is None or not math.isfinite(observed):
        return {
            "draw_count": draws,
            "base_seed": base_seed,
            "derived_seed": derived_seed,
            "seed_payload_sha256": payload_sha256,
            "random_mean_pct": None,
            "random_median_pct": None,
            "top_k_random_percentile_fraction": None,
            "status": "unavailable",
            "reason": "non-finite available or observed OOS return",
        }
    rng = np.random.default_rng(derived_seed)
    selected_count = min(k, values.size)
    random_returns = np.empty(draws, dtype=np.float64)
    for index in range(draws):
        chosen = rng.choice(values.size, size=selected_count, replace=False)
        random_returns[index] = float(np.sum(values[chosen]) / k)
    return {
        "draw_count": draws,
        "base_seed": base_seed,
        "derived_seed": derived_seed,
        "seed_payload_sha256": payload_sha256,
        "random_mean_pct": float(np.mean(random_returns)),
        "random_median_pct": float(np.median(random_returns)),
        "top_k_random_percentile_fraction": random_percentile_fraction(
            random_returns, observed
        ),
        "status": "available",
        "reason": None,
    }


def _set_outcome(
    symbols: Sequence[str], returns: Mapping[str, float | None], k: int
) -> Mapping[str, Any]:
    selected = tuple(symbols[: min(k, len(symbols))])
    values = [returns.get(symbol) for symbol in selected]
    available = len(returns)
    selected_count = len(selected)
    fractions = {
        "requested_capacity_fraction": float(k / available) if available else None,
        "realized_selected_fraction": float(selected_count / available) if available else None,
        "selectivity": float(selected_count / available) if available else None,
    }
    if available == 0:
        return {
            "status": "unavailable",
            "reason": "zero available tickers",
            "selected_tickers": list(selected),
            "selected_count": selected_count,
            "cash_slots": k - selected_count,
            "capacity_return_pct": None,
            "conditional_selected_mean_pct": None,
            **fractions,
        }
    if any(value is None or not math.isfinite(value) for value in values):
        status, reason = "unavailable", "selected OOS return is unavailable"
        capacity = conditional = None
    else:
        status, reason = "available", None
        capacity = float(sum(values) / k)
        conditional = float(np.mean(values)) if values else None
    return {
        "status": status,
        "reason": reason,
        "selected_tickers": list(selected),
        "selected_count": selected_count,
        "cash_slots": k - selected_count,
        "capacity_return_pct": capacity,
        "conditional_selected_mean_pct": conditional,
        **fractions,
    }


def _portfolio(monthly: Sequence[float | None]) -> Mapping[str, Any]:
    if any(value is None or not math.isfinite(value) for value in monthly):
        return {
            "status": "unavailable",
            "reason": "one or more required calendar blocks are unavailable",
            "compounded_return_pct": None,
            "monthly_series_max_drawdown_pct": None,
        }
    equity = 1.0
    curve = [equity]
    for value in monthly:
        factor = 1.0 + float(value) / 100.0
        if not math.isfinite(factor) or factor <= 0.0:
            return {
                "status": "unavailable",
                "reason": "monthly gross factor is non-finite or non-positive",
                "compounded_return_pct": None,
                "monthly_series_max_drawdown_pct": None,
            }
        equity *= factor
        curve.append(equity)
    values = np.asarray(curve, dtype=np.float64)
    peaks = np.maximum.accumulate(values)
    drawdown = 1.0 - values / peaks
    return {
        "status": "available",
        "reason": None,
        "compounded_return_pct": float((equity - 1.0) * 100.0),
        "monthly_series_max_drawdown_pct": float(np.max(drawdown) * 100.0),
    }


def _turnover(
    sets: Sequence[Sequence[str]], ks: Sequence[int | None], *, varying: bool
) -> Mapping[str, Any]:
    rows: list[Mapping[str, Any]] = []
    for index in range(len(sets)):
        if index == 0:
            rows.append({"status": "unavailable", "reason": "first_block", "value": None})
            continue
        if ks[index] is None or ks[index - 1] is None:
            rows.append({"status": "unavailable", "reason": "zero_available_pool", "value": None})
            continue
        if varying and ks[index] != ks[index - 1]:
            rows.append({"status": "unavailable", "reason": "varying_capacity", "value": None})
            continue
        k = int(ks[index])
        previous, current = set(sets[index - 1]), set(sets[index])
        retained = len(previous & current) + min(k - len(previous), k - len(current))
        rows.append({"status": "available", "reason": None, "value": float(1.0 - retained / k)})
    finite = [row["value"] for row in rows if row["value"] is not None]
    return {
        "transitions": rows,
        "finite_transition_count": len(finite),
        "mean_turnover": float(np.mean(finite)) if finite else None,
    }


def _member_row(
    *,
    dataset_label: str,
    window: Window,
    variant: str,
    allocation_kind: str,
    k: int,
    decision: _Decision,
    rank: int,
    outcome: Mapping[str, Any],
    oos_return: float | None,
    deployable: bool,
    hindsight: bool,
    scorer_name: str,
    available_tickers: int,
) -> Mapping[str, Any]:
    return {
        "dataset_label": dataset_label,
        "candidate_rule": decision.view.candidate_rule,
        "ticker_scorer": scorer_name,
        "window_id": decision.view.window_id,
        "oos_start": window.oos_start,
        "oos_end": window.oos_end,
        "row_kind": "member",
        "variant": variant,
        "allocation_kind": allocation_kind,
        "k": k,
        "available_tickers": available_tickers,
        "selected_count": outcome["selected_count"],
        "cash_slots": outcome["cash_slots"],
        "requested_capacity_fraction": outcome["requested_capacity_fraction"],
        "realized_selected_fraction": outcome["realized_selected_fraction"],
        "selectivity": outcome["selectivity"],
        "ticker": decision.view.ticker,
        "rank": rank,
        "candidate_id": decision.view.candidate_id,
        "semantic_key": decision.view.semantic_key,
        "candidate_rule_score": decision.view.candidate_rule_score,
        "ticker_score": decision.ticker_score,
        "is_net_profit_pct": decision.view.is_net_profit_pct,
        "is_total_trades": decision.view.is_total_trades,
        "oos_return_pct": oos_return,
        "slot_weight": float(1.0 / k),
        "capacity_contribution_pct": (
            float(oos_return / k) if oos_return is not None and math.isfinite(oos_return) else None
        ),
        "draw_count": None,
        "base_seed": None,
        "derived_seed": None,
        "seed_payload_sha256": None,
        "random_mean_pct": None,
        "random_median_pct": None,
        "top_k_random_percentile_fraction": None,
        "deployable": deployable,
        "diagnostic": hindsight or not deployable,
        "hindsight": hindsight,
        "non_deployable": not deployable,
        "status": outcome["status"],
        "reason": outcome["reason"],
    }


def evaluate_allocation(
    inputs: Sequence[DatasetInput],
    *,
    candidate_rule: str,
    primary_k: int = 6,
    sensitivity_k: int = 8,
    ticker_scorer: TickerScorer = OFFICIAL_TICKER_SCORER,
) -> AllocationResult:
    """Evaluate N>=1 datasets through one exact-calendar, two-level freeze path."""
    if not inputs:
        raise AnalysisError("allocation requires at least one explicitly labelled dataset.")
    if len({item.dataset.schema_version for item in inputs}) != 1:
        raise AnalysisError("mixed v2/v3 allocation identity contracts are unsupported.")
    descriptive = inputs[0].dataset.contract.descriptive
    primary_k = _exact_positive_integer(primary_k, "primary_k")
    sensitivity_k = _exact_positive_integer(sensitivity_k, "sensitivity_k")
    labels = [item.label for item in inputs]
    if any(not isinstance(label, str) or not label or "=" in label for label in labels):
        raise AnalysisError("dataset labels must be unique non-empty strings without '='.")
    if len(set(labels)) != len(labels):
        raise AnalysisError("dataset labels must be unique.")
    manifest_hashes = [item.dataset.manifest_sha256 for item in inputs]
    if len(set(manifest_hashes)) != len(manifest_hashes):
        raise AnalysisError("dataset labels resolve to conflicting duplicate dataset identities.")
    scorer_facts = _scorer_facts(ticker_scorer)
    if descriptive and scorer_facts != _scorer_facts(OFFICIAL_TICKER_SCORER):
        raise AnalysisError("descriptive v3 currently supports the existing IS-profit ticker scorer only.")
    for item in inputs:
        registry = item.dataset.contract.rule_registry
        if candidate_rule not in RULE_REQUIREMENTS:
            raise AnalysisError(f"unknown candidate rule {candidate_rule!r}.")
        if candidate_rule not in tuple(registry.get("selectable_rules", ())):
            raise AnalysisError(
                f"dataset {item.label!r} does not declare selectable rule {candidate_rule!r}."
            )
        missing = [name for name in RULE_REQUIREMENTS[candidate_rule] if name not in item.dataset.metric_index]
        if missing:
            raise AnalysisError(
                f"dataset {item.label!r} does not support {candidate_rule!r}; missing: {missing}."
            )
        if not descriptive:
            declared = item.dataset.contract.split.get("development_ticker_count")
            _exact_positive_integer(declared, f"dataset {item.label!r} declared development pool")

    block_maps = [{window.block_key: window for window in item.scope.windows} for item in inputs]
    common_blocks = set(block_maps[0])
    for mapping in block_maps[1:]:
        common_blocks &= set(mapping)
    ordered_blocks = tuple(sorted(common_blocks))
    if not ordered_blocks:
        raise AnalysisError("datasets have no exact common OOS UTC calendar block.")
    common_tickers = set(inputs[0].scope.tickers)
    for item in inputs[1:]:
        common_tickers &= set(item.scope.tickers)
    ordered_tickers = tuple(sorted(common_tickers))
    if not ordered_tickers:
        raise AnalysisError("datasets have no common requested-scope canonical ticker.")

    pair_rows: list[Mapping[str, Any]] = []
    allocation_rows: list[Mapping[str, Any]] = []
    monthly_rows: list[Mapping[str, Any]] = []
    dataset_blocks: dict[str, list[Mapping[str, Any]]] = {label: [] for label in labels}
    access_starts = {item.label: len(item.dataset.access_log) for item in inputs}
    neighbours = {item.label: star_neighbours(item.dataset.geometry) for item in inputs}

    for block_key in ordered_blocks:
        frozen: dict[str, Mapping[str, Any]] = {}
        # Freeze both candidate and ticker decisions for every dataset before any OOS reveal.
        for item, mapping in zip(inputs, block_maps):
            window = mapping[block_key]
            scope = item.dataset.subset_scope(
                item.scope, tickers=ordered_tickers, window_ids=(window.window_id,)
            )
            is_views = item.dataset.load_is_window(scope, window)
            decisions: list[_Decision] = []
            unavailable: dict[str, str] = {}
            selected_views: dict[str, SelectedISTickerView] = {}
            for ticker in ordered_tickers:
                view = is_views[ticker]
                result = evaluate_rule(
                    candidate_rule,
                    view,
                    item.dataset.geometry,
                    float(item.dataset.contract.rule_registry["minimum_completed_trades"]),
                    neighbours=neighbours[item.label],
                )
                selection = select_candidates(
                    result, view.metrics["net_profit_pct"], item.dataset.geometry, maximum=1
                )
                if not selection.row_indices:
                    unavailable[ticker] = "no eligible finite candidate"
                    continue
                row = selection.row_indices[0]
                net = float(view.metrics["net_profit_pct"][row])
                trades_value = view.metrics.get("total_trades")
                trades = (
                    float(trades_value[row])
                    if trades_value is not None and np.isfinite(trades_value[row])
                    else None
                )
                scalar_view = SelectedISTickerView(
                    dataset_label=item.label,
                    ticker=ticker,
                    window_id=window.window_id,
                    is_start_utc=window.is_start,
                    is_end_utc=window.is_end,
                    oos_start_utc=window.oos_start,
                    oos_end_utc=window.oos_end,
                    candidate_rule=candidate_rule,
                    candidate_id=selection.candidate_ids[0],
                    semantic_key=selection.semantic_keys[0],
                    candidate_rule_score=selection.scores[0],
                    is_net_profit_pct=net,
                    is_total_trades=trades,
                )
                selected_views[ticker] = scalar_view
                score = _ticker_score(
                    ticker_scorer,
                    scalar_view,
                    {
                        "dataset_label": item.label,
                        "candidate_rule": candidate_rule,
                        "ticker_scorer": scorer_facts,
                    },
                )
                if score is None:
                    unavailable[ticker] = "ticker score is unavailable"
                    continue
                decisions.append(_Decision(scalar_view, row, score))
            frozen[item.label] = {
                "item": item,
                "window": window,
                "scope": scope,
                "decisions": tuple(decisions),
                "unavailable": unavailable,
                "selected_views": selected_views,
            }
            is_views.clear()

        common_available = set(ordered_tickers)
        for state in frozen.values():
            common_available &= {
                decision.view.ticker for decision in state["decisions"]
            }
        for item in inputs:
            state = frozen[item.label]
            local_decisions = state["decisions"]
            decisions = tuple(
                decision
                for decision in local_decisions
                if decision.view.ticker in common_available
            )
            for decision in local_decisions:
                if decision.view.ticker not in common_available:
                    state["unavailable"][decision.view.ticker] = (
                        "ticker is not candidate/score-available in every aligned dataset"
                    )
            ranked = _rank(decisions)
            bottom_ranked = _rank(decisions, bottom=True)
            available = len(ranked)
            variants = {
                "primary": {"k": primary_k, "label": "operational_primary"},
                "sensitivity": {"k": sensitivity_k, "label": "sensitivity"},
            }
            if not descriptive:
                declared_pool = int(item.dataset.contract.split["development_ticker_count"])
                matched_k = max(1, min(available, math.ceil(primary_k / declared_pool * available))) if available else None
                variants["matched_fraction"] = {"k": matched_k, "label": "diagnostic"}
            for variant in variants.values():
                k = variant["k"]
                variant["top"] = (
                    tuple(
                        decision.view.ticker
                        for decision in ranked[: min(k, available)]
                    )
                    if k is not None
                    else ()
                )
                variant["bottom"] = (
                    tuple(
                        decision.view.ticker
                        for decision in bottom_ranked[: min(k, available)]
                    )
                    if k is not None
                    else ()
                )
                variant.update(
                    _boundary_facts(ranked, k)
                    if k is not None
                    else {
                        "boundary_tie_depth": 0,
                        "boundary_tie_break_level": "unavailable",
                    }
                )
            state.update(
                decisions=decisions,
                locally_available_tickers=len(local_decisions),
                ranked=ranked,
                bottom_ranked=bottom_ranked,
                variants=variants,
            )

        for item in inputs:
            state = frozen[item.label]
            window = state["window"]
            oos = item.dataset.load_oos_window(state["scope"], window)
            decisions = state["decisions"]
            decision_by_ticker = {decision.view.ticker: decision for decision in decisions}
            returns: dict[str, float | None] = {}
            for ticker in ordered_tickers:
                decision = decision_by_ticker.get(ticker)
                if decision is None:
                    selected_view = state["selected_views"].get(ticker)
                    pair_rows.append(
                        {
                            "dataset_label": item.label,
                            "candidate_rule": candidate_rule,
                            "ticker_scorer": scorer_facts["name"],
                            "ticker": ticker,
                            "window_id": window.window_id,
                            "oos_start": window.oos_start,
                            "oos_end": window.oos_end,
                            "status": "unavailable",
                            "reason": state["unavailable"].get(ticker),
                            "candidate_id": (
                                selected_view.candidate_id if selected_view is not None else None
                            ),
                            "semantic_key": (
                                selected_view.semantic_key if selected_view is not None else None
                            ),
                            "candidate_rule_score": (
                                selected_view.candidate_rule_score
                                if selected_view is not None
                                else None
                            ),
                            "ticker_score": None,
                            "is_net_profit_pct": (
                                selected_view.is_net_profit_pct
                                if selected_view is not None
                                else None
                            ),
                            "is_total_trades": (
                                selected_view.is_total_trades
                                if selected_view is not None
                                else None
                            ),
                            "oos_return_pct": None,
                        }
                    )
                    continue
                raw = oos[ticker][decision.row_index, item.dataset.metric_index["net_profit_pct"]]
                value = float(raw) if np.isfinite(raw) else None
                returns[ticker] = value
                pair_rows.append(
                    {
                        "dataset_label": item.label,
                        "candidate_rule": candidate_rule,
                        "ticker_scorer": scorer_facts["name"],
                        "ticker": ticker,
                        "window_id": window.window_id,
                        "oos_start": window.oos_start,
                        "oos_end": window.oos_end,
                        "status": "selected" if value is not None else "unavailable",
                        "reason": None if value is not None else "matching OOS return is unavailable",
                        "candidate_id": decision.view.candidate_id,
                        "semantic_key": decision.view.semantic_key,
                        "candidate_rule_score": decision.view.candidate_rule_score,
                        "ticker_score": decision.ticker_score,
                        "is_net_profit_pct": decision.view.is_net_profit_pct,
                        "is_total_trades": decision.view.is_total_trades,
                        "oos_return_pct": value,
                    }
                )
            finite_returns = [value for value in returns.values() if value is not None and math.isfinite(value)]
            all_available = float(np.mean(finite_returns)) if finite_returns else None
            block_result: dict[str, Any] = {
                "block_key": list(block_key),
                "window_id": window.window_id,
                "requested_tickers": len(item.scope.tickers),
                "common_tickers": len(ordered_tickers),
                "available_tickers": len(decisions),
                "locally_available_tickers": state["locally_available_tickers"],
                "unavailable_tickers": len(ordered_tickers) - len(decisions),
                "omitted_tickers": sorted(set(item.scope.tickers) - set(ordered_tickers)),
                "all_available_mean_pct": all_available,
                "variants": {},
            }
            ranked = state["ranked"]
            for variant_name, variant in state["variants"].items():
                if variant["k"] is None:
                    unavailable_outcome = {
                        "status": "unavailable",
                        "reason": "zero available tickers",
                        "selected_tickers": [],
                        "selected_count": 0,
                        "cash_slots": None,
                        "capacity_return_pct": None,
                        "conditional_selected_mean_pct": None,
                        "requested_capacity_fraction": None,
                        "realized_selected_fraction": None,
                        "selectivity": None,
                    }
                    random = {
                        "label": "diagnostic",
                        "hindsight": False,
                        "non_deployable": True,
                        "draw_count": random_settings(item.dataset.contract)[
                            "resamples"
                        ],
                        "base_seed": random_settings(item.dataset.contract)[
                            "seed"
                        ],
                        "derived_seed": None,
                        "seed_payload_sha256": None,
                        "random_mean_pct": None,
                        "random_median_pct": None,
                        "top_k_random_percentile_fraction": None,
                        "status": "unavailable",
                        "reason": "zero available tickers; matched K is unavailable",
                    }
                    block_result["variants"][variant_name] = {
                        "label": variant["label"],
                        "k": None,
                        "available_tickers": 0,
                        **unavailable_outcome,
                        "boundary_tie_depth": 0,
                        "boundary_tie_break_level": "unavailable",
                        "all_available_mean_pct": None,
                        "bottom_k": {
                            **unavailable_outcome,
                            "label": "diagnostic",
                            "hindsight": False,
                            "non_deployable": True,
                        },
                        "random_k": random,
                        "oracle_k": {
                            **unavailable_outcome,
                            "label": "diagnostic",
                            "hindsight": True,
                            "non_deployable": True,
                        },
                        "anti_oracle_k": {
                            **unavailable_outcome,
                            "label": "diagnostic",
                            "hindsight": True,
                            "non_deployable": True,
                        },
                        "spreads": {
                            "top_k_minus_all_available_pct": None,
                            "top_k_minus_random_mean_pct": None,
                            "top_k_minus_bottom_k_pct": None,
                            "oracle_k_minus_top_k_pct": None,
                            "top_k_minus_anti_oracle_k_pct": None,
                        },
                    }
                    monthly_rows.append(
                        {
                            "dataset_label": item.label,
                            "candidate_rule": candidate_rule,
                            "ticker_scorer": scorer_facts["name"],
                            "variant": variant_name,
                            "k": None,
                            "window_id": window.window_id,
                            "oos_start": window.oos_start,
                            "oos_end": window.oos_end,
                            "capacity_return_pct": None,
                            "conditional_selected_mean_pct": None,
                            "all_available_mean_pct": None,
                            "bottom_k_return_pct": None,
                            "random_mean_pct": None,
                            "oracle_k_return_pct": None,
                            "anti_oracle_k_return_pct": None,
                            "top_k_random_percentile_fraction": None,
                            "status": "unavailable",
                            "reason": "zero available tickers; matched K is unavailable",
                        }
                    )
                    continue
                k = int(variant["k"])
                top = _set_outcome(variant["top"], returns, k)
                bottom = _set_outcome(variant["bottom"], returns, k)
                finite_decisions = [
                    decision for decision in decisions if returns.get(decision.view.ticker) is not None
                ]
                oracle_ranked = tuple(
                    sorted(finite_decisions, key=lambda decision: (-returns[decision.view.ticker], decision.view.ticker))
                )
                anti_ranked = tuple(
                    sorted(finite_decisions, key=lambda decision: (returns[decision.view.ticker], decision.view.ticker))
                )
                oracle_symbols = tuple(decision.view.ticker for decision in oracle_ranked)
                anti_symbols = tuple(decision.view.ticker for decision in anti_ranked)
                oracle = _set_outcome(oracle_symbols, returns, k)
                anti = _set_outcome(anti_symbols, returns, k)
                bottom = {
                    **bottom,
                    "label": "diagnostic",
                    "hindsight": False,
                    "non_deployable": True,
                }
                oracle = {
                    **oracle,
                    "label": "diagnostic",
                    "hindsight": True,
                    "non_deployable": True,
                }
                anti = {
                    **anti,
                    "label": "diagnostic",
                    "hindsight": True,
                    "non_deployable": True,
                }
                random = {
                    **_random_summary(
                        dataset_input=item,
                        candidate_rule=candidate_rule,
                        scorer_facts=scorer_facts,
                        k=k,
                        window=window,
                        returns=[returns[decision.view.ticker] for decision in ranked],
                        observed=top["capacity_return_pct"],
                    ),
                    "label": "diagnostic",
                    "hindsight": False,
                    "non_deployable": True,
                }
                def spread(left: float | None, right: float | None) -> float | None:
                    return float(left - right) if left is not None and right is not None else None

                spreads = {
                    "top_k_minus_all_available_pct": spread(top["capacity_return_pct"], all_available),
                    "top_k_minus_random_mean_pct": spread(top["capacity_return_pct"], random["random_mean_pct"]),
                    "top_k_minus_bottom_k_pct": spread(top["capacity_return_pct"], bottom["capacity_return_pct"]),
                    "oracle_k_minus_top_k_pct": spread(oracle["capacity_return_pct"], top["capacity_return_pct"]),
                    "top_k_minus_anti_oracle_k_pct": spread(top["capacity_return_pct"], anti["capacity_return_pct"]),
                }
                result = {
                    "label": variant["label"],
                    "k": k,
                    "available_tickers": len(decisions),
                    **top,
                    **variant,
                    "all_available_mean_pct": all_available,
                    "bottom_k": bottom,
                    "random_k": random,
                    "oracle_k": oracle,
                    "anti_oracle_k": anti,
                    "spreads": spreads,
                }
                result.pop("top", None)
                result.pop("bottom", None)
                block_result["variants"][variant_name] = result
                monthly_rows.append(
                    {
                        "dataset_label": item.label,
                        "candidate_rule": candidate_rule,
                        "ticker_scorer": scorer_facts["name"],
                        "variant": variant_name,
                        "k": k,
                        "window_id": window.window_id,
                        "oos_start": window.oos_start,
                        "oos_end": window.oos_end,
                        "capacity_return_pct": top["capacity_return_pct"],
                        "conditional_selected_mean_pct": top["conditional_selected_mean_pct"],
                        "all_available_mean_pct": all_available,
                        "bottom_k_return_pct": bottom["capacity_return_pct"],
                        "random_mean_pct": random["random_mean_pct"],
                        "oracle_k_return_pct": oracle["capacity_return_pct"],
                        "anti_oracle_k_return_pct": anti["capacity_return_pct"],
                        "top_k_random_percentile_fraction": random["top_k_random_percentile_fraction"],
                        "status": top["status"],
                        "reason": top["reason"],
                    }
                )
                member_specs = (
                    (
                        "top_k",
                        tuple(
                            decision
                            for decision in ranked
                            if decision.view.ticker in set(variant["top"])
                        ),
                        top,
                        variant_name in {"primary", "sensitivity"},
                        False,
                    ),
                    ("bottom_k", tuple(decision for decision in state["bottom_ranked"] if decision.view.ticker in set(variant["bottom"])), bottom, False, False),
                    ("oracle_k", oracle_ranked[: min(k, len(oracle_ranked))], oracle, False, True),
                    ("anti_oracle_k", anti_ranked[: min(k, len(anti_ranked))], anti, False, True),
                )
                for kind, members, member_outcome, deployable, hindsight in member_specs:
                    for rank_index, decision in enumerate(members, start=1):
                        allocation_rows.append(
                            _member_row(
                                dataset_label=item.label,
                                window=window,
                                variant=variant_name,
                                allocation_kind=kind,
                                k=k,
                                decision=decision,
                                rank=rank_index,
                                outcome=member_outcome,
                                oos_return=returns.get(decision.view.ticker),
                                deployable=deployable,
                                hindsight=hindsight,
                                scorer_name=scorer_facts["name"],
                                available_tickers=len(decisions),
                            )
                        )
                allocation_rows.append(
                    {
                        "dataset_label": item.label,
                        "candidate_rule": candidate_rule,
                        "ticker_scorer": scorer_facts["name"],
                        "window_id": window.window_id,
                        "oos_start": window.oos_start,
                        "oos_end": window.oos_end,
                        "row_kind": "random_summary",
                        "variant": variant_name,
                        "allocation_kind": "random_k",
                        "k": k,
                        "available_tickers": len(decisions),
                        "selected_count": min(k, len(decisions)),
                        "cash_slots": max(0, k - len(decisions)),
                        "requested_capacity_fraction": top["requested_capacity_fraction"],
                        "realized_selected_fraction": top["realized_selected_fraction"],
                        "selectivity": top["selectivity"],
                        "ticker": None,
                        "rank": None,
                        "candidate_id": None,
                        "semantic_key": None,
                        "candidate_rule_score": None,
                        "ticker_score": None,
                        "is_net_profit_pct": None,
                        "is_total_trades": None,
                        "oos_return_pct": None,
                        "slot_weight": None,
                        "capacity_contribution_pct": None,
                        **random,
                        "deployable": False,
                        "diagnostic": True,
                        "hindsight": False,
                        "non_deployable": True,
                    }
                )
            dataset_blocks[item.label].append(block_result)
            oos.clear()
        frozen.clear()

    dataset_summaries: dict[str, Mapping[str, Any]] = {}
    for item in inputs:
        variants: dict[str, Mapping[str, Any]] = {}
        for variant_name in (("primary", "sensitivity") if descriptive else ("primary", "sensitivity", "matched_fraction")):
            blocks = dataset_blocks[item.label]
            monthly = [block["variants"][variant_name]["capacity_return_pct"] for block in blocks]
            sets = [block["variants"][variant_name]["selected_tickers"] for block in blocks]
            ks = [block["variants"][variant_name]["k"] for block in blocks]
            effects = {
                key: [block["variants"][variant_name]["spreads"][key] for block in blocks]
                for key in blocks[0]["variants"][variant_name]["spreads"]
            }
            controls = {
                "all_available_mean_pct": [
                    block["variants"][variant_name]["all_available_mean_pct"]
                    for block in blocks
                ],
                "bottom_k_capacity_return_pct": [
                    block["variants"][variant_name]["bottom_k"]["capacity_return_pct"]
                    for block in blocks
                ],
                "random_k_mean_pct": [
                    block["variants"][variant_name]["random_k"]["random_mean_pct"]
                    for block in blocks
                ],
                "oracle_k_capacity_return_pct": [
                    block["variants"][variant_name]["oracle_k"]["capacity_return_pct"]
                    for block in blocks
                ],
                "anti_oracle_k_capacity_return_pct": [
                    block["variants"][variant_name]["anti_oracle_k"]["capacity_return_pct"]
                    for block in blocks
                ],
                "top_k_random_percentile_fraction": [
                    block["variants"][variant_name]["random_k"][
                        "top_k_random_percentile_fraction"
                    ]
                    for block in blocks
                ],
            }
            headline = float(np.mean(monthly)) if all(value is not None for value in monthly) else None
            variants[variant_name] = {
                "label": blocks[0]["variants"][variant_name]["label"],
                "monthly_capacity_returns_pct": monthly,
                "headline_mean_capacity_return_pct": headline,
                "portfolio": _portfolio(monthly),
                "turnover": _turnover(sets, ks, varying=variant_name == "matched_fraction"),
                "bootstrap": {
                    "effect": headline,
                    **(interval_not_requested() if descriptive else descriptive_bootstrap(monthly, item.dataset.contract.evidence["uncertainty"])),
                },
                "mean_spreads": {
                    key: (float(np.mean(values)) if all(value is not None for value in values) else None)
                    for key, values in effects.items()
                },
                "control_headlines": {
                    key: (
                        float(np.mean(values))
                        if all(value is not None for value in values)
                        else None
                    )
                    for key, values in controls.items()
                },
                "ks": ks,
            }
        dataset_summaries[item.label] = {
            "manifest_sha256": item.dataset.manifest_sha256,
            "requested_scope": item.scope.name,
            "requested_ticker_count": len(item.scope.tickers),
            "common_ticker_count": len(ordered_tickers),
            "omitted_tickers": sorted(set(item.scope.tickers) - set(ordered_tickers)),
            "omitted_blocks": [list(key) for key in sorted(set(block_maps[labels.index(item.label)]) - set(ordered_blocks))],
            "blocks": dataset_blocks[item.label],
            "variants": variants,
        }

    allocation_rows.sort(
        key=lambda row: (
            row["oos_start"], row["oos_end"], row["dataset_label"], row["variant"],
            row["allocation_kind"], row["rank"] or 0, row["ticker"] or "",
        )
    )
    pair_rows.sort(key=lambda row: (row["oos_start"], row["oos_end"], row["dataset_label"], row["ticker"]))
    monthly_rows.sort(key=lambda row: (row["oos_start"], row["oos_end"], row["dataset_label"], row["variant"]))
    metadata = {
        "mode": "fixed_capacity_allocation",
        "schema_version": "strategy_lab_allocation_v1",
        "git": git_facts(),
        "candidate_rule": candidate_rule,
        "ticker_scorer": scorer_facts,
        "primary_k": primary_k,
        "sensitivity_k": sensitivity_k,
        "dataset_labels": labels,
        "datasets": {
            item.label: {
                "path": str(item.dataset.root),
                "manifest_sha256": item.dataset.manifest_sha256,
                "schema_version": item.dataset.schema_version,
                "scope": item.dataset.scope_label,
                "status": item.dataset.status,
                "requested_scope": item.scope.name,
                "declared_ticker_count": len(item.dataset.tickers),
                "requested_ticker_count": len(item.scope.tickers),
                "actual_window_ids": [window.window_id for window in item.scope.windows],
            }
            for item in inputs
        },
        "alignment": {
            "key": ["canonical_ticker", "oos_start_utc", "oos_end_utc"],
            "common_tickers": list(ordered_tickers),
            "common_ticker_count": len(ordered_tickers),
            "common_blocks": [list(key) for key in ordered_blocks],
            "common_block_count": len(ordered_blocks),
        },
        "load_audit": {
            item.label: {
                "entries": [list(entry) for entry in item.dataset.access_log[access_starts[item.label]:]],
                "outside_scope_loads": [
                    list(entry)
                    for entry in item.dataset.access_log[access_starts[item.label]:]
                    if entry[0] not in ordered_tickers
                    or entry[1] not in {block_maps[labels.index(item.label)][key].window_id for key in ordered_blocks}
                ],
            }
            for item in inputs
        },
        "limitations": [
            "capacity returns are calendar-block summaries, not bar-level portfolio equity",
            "oracle and anti-oracle controls are hindsight and non-deployable",
            "no policy nomination or strategy-quality conclusion is performed",
        ],
    }
    summary = {
        "alignment": metadata["alignment"],
        "datasets": dataset_summaries,
        "aligned_primary_results": [
            {
                "block_key": list(key),
                "capacity_return_pct_by_dataset": {
                    label: dataset_blocks[label][index]["variants"]["primary"]["capacity_return_pct"]
                    for label in labels
                },
            }
            for index, key in enumerate(ordered_blocks)
        ],
    }
    if descriptive:
        units = {"schema_version": "strategy_lab_allocation_v2", "period_unit": "oos_block",
                 "oos_period_months": {item.label: item.dataset.run_spec["generation"]["windows"]["oos_period_months"] for item in inputs}}
        capabilities = {name: not_requested() for name in ("matched_fraction", "evidence", "nominations", "bootstrap", "outlier_removal")}
        metadata.update(**units, capabilities=capabilities)
        summary.update(**units, capabilities=capabilities)
    return AllocationResult(
        metadata,
        summary,
        tuple(pair_rows),
        tuple(monthly_rows),
        tuple(allocation_rows),
    )
