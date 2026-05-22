"""Deterministic Grid optimizer orchestration.

Grid mode is intentionally separate from Optuna.  The core engine owns generic
budget parsing, mode allocation, ranking, validation orchestration, and storage
handoff.  Strategy-specific parameter semantics and fast numeric evaluation live
in per-strategy backends.
"""
from __future__ import annotations

import json
import logging
import math
import re
import time
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .backtest_engine import align_date_bounds, load_data, prepare_dataset_with_warmup
from .optuna_engine import (
    CONSTRAINT_OPERATORS,
    OBJECTIVE_DIRECTIONS,
    ConstraintSpec,
    MultiObjectiveConfig,
    OptimizationConfig,
    OptimizationResult,
    _build_constraint_specs,
    _calculate_total_violation,
    _is_non_finite,
    _run_single_combination,
    calculate_score,
    evaluate_constraints,
)
from .post_process import DSRResult, calculate_dsr, calculate_expected_max_sharpe, calculate_luck_share

logger = logging.getLogger(__name__)

GRID_MODE = "grid"
GRID_STRATEGY_ID = "s03_reversal_v10"
GRID_SUPPORTED_OBJECTIVES = {
    "net_profit_pct",
    "max_drawdown_pct",
    "romad",
    "profit_factor",
    "win_rate",
}
GRID_SUPPORTED_CONSTRAINTS = {
    "total_trades",
    "net_profit_pct",
    "max_drawdown_pct",
    "romad",
    "profit_factor",
    "win_rate",
    "max_consecutive_losses",
}
MODE_ORDER = ("cc_only", "tbands_only", "both")


@dataclass
class GridSettings:
    requested_budget: int = 200_000
    seed: int = 42
    top_candidates: int = 10
    allocation_method: str = "auto_sqrt_space"
    min_quota: float = 0.10
    manual_percents: Dict[str, float] = field(default_factory=dict)
    diversity_enabled: bool = True
    diversity_max_per_group: int = 2
    strict_validation: bool = True
    validation_tolerances: Dict[str, float] = field(
        default_factory=lambda: {
            "net_profit_pct_abs": 0.001,
            "max_drawdown_pct_abs": 0.001,
            "romad_abs": 0.005,
            "win_rate_abs": 0.001,
            "total_trades_abs": 0.0,
            "winning_trades_abs": 0.0,
            "losing_trades_abs": 0.0,
            "max_consecutive_losses_abs": 0.0,
        }
    )


@dataclass
class GridAllocation:
    requested_budget: int
    actual_budget: int
    unused_budget: int
    mode_space_sizes: Dict[str, int]
    mode_budgets: Dict[str, int]
    mode_coverage_pct: Dict[str, float]
    target_mode_quotas: Dict[str, float]
    allocation_method: str
    allocation_params: Dict[str, Any]


def supports_fast_grid(strategy_id: str) -> bool:
    return str(strategy_id or "").strip().lower() == GRID_STRATEGY_ID


def parse_grid_budget(value: Any) -> int:
    """Parse integer or compact budget strings such as 200k, 1.5m, 2b."""
    if isinstance(value, bool):
        raise ValueError("Grid candidates must be a positive integer.")
    if isinstance(value, int):
        budget = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError("Grid candidates must be a positive integer.")
        budget = int(value)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise ValueError("Grid candidates is required.")
        match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([kKmMbB]?)", raw)
        if not match:
            raise ValueError(f"Invalid Grid candidates value: {value!r}.")
        number = float(match.group(1))
        suffix = match.group(2).lower()
        multiplier = {"": 1, "k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[suffix]
        budget_float = number * multiplier
        if not math.isfinite(budget_float):
            raise ValueError("Grid candidates must be finite.")
        budget = int(round(budget_float))
    else:
        raise ValueError("Grid candidates must be a positive integer.")

    if budget <= 0:
        raise ValueError("Grid candidates must be greater than zero.")
    return budget


def format_compact_count(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    abs_value = abs(number)
    for suffix, scale in (("B", 1_000_000_000), ("M", 1_000_000), ("k", 1_000)):
        if abs_value >= scale:
            scaled = number / scale
            if abs(scaled) >= 100:
                return f"{scaled:.0f}{suffix}"
            if abs(scaled) >= 10:
                return f"{scaled:.1f}".rstrip("0").rstrip(".") + suffix
            return f"{scaled:.2f}".rstrip("0").rstrip(".") + suffix
    return f"{int(round(number))}"


def format_coverage_pct(value: Any) -> str:
    try:
        pct = float(value)
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(pct):
        return "-"
    if abs(pct - 100.0) < 1e-9:
        return "100%"
    if pct >= 10:
        return f"{pct:.1f}%"
    if pct >= 1:
        return f"{pct:.1f}%"
    return f"{pct:.2f}%"


def _normalize_allocation_method(value: Any) -> str:
    method = str(value or "auto_sqrt_space").strip().lower()
    aliases = {
        "auto": "auto_sqrt_space",
        "sqrt": "auto_sqrt_space",
        "auto_sqrt": "auto_sqrt_space",
        "auto-sqrt-space": "auto_sqrt_space",
        "proportional": "proportional_space",
        "proportional-space": "proportional_space",
        "space": "proportional_space",
        "manual": "manual",
        "manual_percent": "manual",
        "manual_pct": "manual",
    }
    return aliases.get(method, method)


def _mode_order_index(mode: str) -> int:
    try:
        return MODE_ORDER.index(mode)
    except ValueError:
        return len(MODE_ORDER)


def _quota_weights(
    mode_space_sizes: Dict[str, int],
    method: str,
    min_quota: float,
    manual_percents: Dict[str, float],
) -> Dict[str, float]:
    enabled_modes = [mode for mode in MODE_ORDER if int(mode_space_sizes.get(mode, 0)) > 0]
    if not enabled_modes:
        raise ValueError("Grid parameter space is empty.")

    if method == "manual":
        total_pct = 0.0
        quotas: Dict[str, float] = {}
        for mode in MODE_ORDER:
            pct = float(manual_percents.get(mode, 0.0) or 0.0)
            if pct < 0:
                raise ValueError("Manual Grid allocation percentages must be non-negative.")
            if int(mode_space_sizes.get(mode, 0)) <= 0 and pct > 0:
                raise ValueError(f"Manual Grid allocation assigns budget to disabled mode '{mode}'.")
            quotas[mode] = pct / 100.0
            if mode in enabled_modes:
                total_pct += pct
        if abs(total_pct - 100.0) > 1e-6:
            raise ValueError("Manual Grid allocation must sum to 100% across enabled modes.")
        return quotas

    if method == "auto_sqrt_space":
        weights = {mode: math.sqrt(float(mode_space_sizes[mode])) for mode in enabled_modes}
        min_q = max(0.0, float(min_quota))
        if min_q * len(enabled_modes) >= 1.0 and len(enabled_modes) > 1:
            raise ValueError("Grid min quota is too high for the number of enabled modes.")
        base = min_q if len(enabled_modes) > 1 else 0.0
        remaining = max(0.0, 1.0 - base * len(enabled_modes))
    elif method == "proportional_space":
        weights = {mode: float(mode_space_sizes[mode]) for mode in enabled_modes}
        base = 0.0
        remaining = 1.0
    else:
        raise ValueError(f"Unsupported Grid allocation method: {method}")

    total_weight = sum(weights.values())
    if total_weight <= 0:
        raise ValueError("Grid allocation weights are empty.")
    quotas = {mode: 0.0 for mode in MODE_ORDER}
    for mode in enabled_modes:
        quotas[mode] = base + remaining * (weights[mode] / total_weight)
    return quotas


def allocate_mode_budgets(
    mode_space_sizes: Dict[str, int],
    requested_budget: int,
    *,
    method: str = "auto_sqrt_space",
    min_quota: float = 0.10,
    manual_percents: Optional[Dict[str, float]] = None,
) -> GridAllocation:
    requested = parse_grid_budget(requested_budget)
    sizes = {mode: max(0, int(mode_space_sizes.get(mode, 0) or 0)) for mode in MODE_ORDER}
    total_space = sum(sizes.values())
    if total_space <= 0:
        raise ValueError("Grid parameter space is empty.")

    actual_budget = min(requested, total_space)
    method = _normalize_allocation_method(method)
    manual = dict(manual_percents or {})
    quotas = _quota_weights(sizes, method, min_quota, manual)

    if actual_budget >= total_space:
        budgets = dict(sizes)
    else:
        budgets = {mode: 0 for mode in MODE_ORDER}
        remaining = actual_budget
        available = {mode for mode in MODE_ORDER if sizes[mode] > 0}

        while remaining > 0 and available:
            quota_sum = sum(max(0.0, quotas.get(mode, 0.0)) for mode in available)
            if quota_sum <= 0:
                quota_sum = float(len(available))
                local_quota = {mode: 1.0 / quota_sum for mode in available}
            else:
                local_quota = {mode: max(0.0, quotas.get(mode, 0.0)) / quota_sum for mode in available}

            desired = {mode: remaining * local_quota[mode] for mode in available}
            additions = {
                mode: min(sizes[mode] - budgets[mode], int(math.floor(desired[mode])))
                for mode in available
            }
            added = sum(additions.values())
            for mode, count in additions.items():
                budgets[mode] += count
            remaining -= added

            if remaining <= 0:
                break

            candidates = [mode for mode in available if budgets[mode] < sizes[mode]]
            if not candidates:
                break

            candidates.sort(
                key=lambda mode: (
                    -(desired.get(mode, 0.0) - math.floor(desired.get(mode, 0.0))),
                    -quotas.get(mode, 0.0),
                    _mode_order_index(mode),
                )
            )
            progressed = False
            for mode in candidates:
                if remaining <= 0:
                    break
                if budgets[mode] >= sizes[mode]:
                    continue
                budgets[mode] += 1
                remaining -= 1
                progressed = True

            available = {mode for mode in available if budgets[mode] < sizes[mode]}
            if not progressed and remaining > 0 and available:
                mode = sorted(available, key=_mode_order_index)[0]
                take = min(remaining, sizes[mode] - budgets[mode])
                budgets[mode] += take
                remaining -= take

    actual = sum(budgets.values())
    coverage = {
        mode: (budgets[mode] / sizes[mode] * 100.0) if sizes[mode] > 0 else 0.0
        for mode in MODE_ORDER
    }
    return GridAllocation(
        requested_budget=requested,
        actual_budget=actual,
        unused_budget=max(0, requested - actual),
        mode_space_sizes=sizes,
        mode_budgets=budgets,
        mode_coverage_pct=coverage,
        target_mode_quotas={mode: float(quotas.get(mode, 0.0)) for mode in MODE_ORDER},
        allocation_method=method,
        allocation_params={
            "min_quota": float(min_quota),
            "manual_percents": manual if method == "manual" else {},
        },
    )


def _settings_from_config(config: OptimizationConfig) -> GridSettings:
    raw_budget = getattr(config, "grid_budget", 200_000)
    return GridSettings(
        requested_budget=parse_grid_budget(raw_budget),
        seed=int(getattr(config, "grid_seed", 42) or 42),
        top_candidates=max(1, int(getattr(config, "grid_top_candidates", 10) or 10)),
        allocation_method=_normalize_allocation_method(
            getattr(config, "grid_allocation_method", "auto_sqrt_space")
        ),
        min_quota=max(0.0, float(getattr(config, "grid_min_quota", 0.10) or 0.0)),
        manual_percents=dict(getattr(config, "grid_manual_percents", {}) or {}),
        diversity_enabled=bool(getattr(config, "grid_diversity_enabled", True)),
        diversity_max_per_group=max(1, int(getattr(config, "grid_diversity_max_per_group", 2) or 2)),
        strict_validation=bool(getattr(config, "grid_strict_validation", True)),
    )


def _load_backend(strategy_id: str):
    if not supports_fast_grid(strategy_id):
        raise ValueError("Grid mode is supported only for S03 Reversal v10.")
    from strategies.s03_reversal_v10 import fast_grid

    return fast_grid


def validate_grid_config(config: OptimizationConfig) -> None:
    if not supports_fast_grid(config.strategy_id):
        raise ValueError("Grid mode is supported only for S03 Reversal v10.")

    backend = _load_backend(config.strategy_id)
    if not getattr(backend, "NUMBA_AVAILABLE", False):
        reason = getattr(backend, "NUMBA_IMPORT_ERROR", None) or "Numba import failed"
        raise ValueError(f"Grid mode requires Numba: {reason}")

    objectives = list(getattr(config, "objectives", []) or ["net_profit_pct"])
    if not objectives:
        raise ValueError("At least 1 objective is required.")
    unsupported_objectives = sorted(set(objectives) - GRID_SUPPORTED_OBJECTIVES)
    if unsupported_objectives:
        if "composite_score" in unsupported_objectives:
            raise ValueError("Composite Score is not supported in Grid v1.")
        raise ValueError(
            "Grid v1 objective is not available for fast screening: "
            + ", ".join(unsupported_objectives)
        )

    primary = getattr(config, "primary_objective", None)
    if len(objectives) > 1 and primary not in objectives:
        raise ValueError("Primary objective must be one of the selected Grid objectives.")

    constraint_specs = _build_constraint_specs(getattr(config, "constraints", []) or [])
    unsupported_constraints = sorted(
        {spec.metric for spec in constraint_specs if spec.enabled and spec.metric not in GRID_SUPPORTED_CONSTRAINTS}
    )
    if unsupported_constraints:
        raise ValueError(
            "Grid v1 constraint metric is not available for fast screening: "
            + ", ".join(unsupported_constraints)
        )


def preview_grid_parameter_space(config: OptimizationConfig) -> Dict[str, Any]:
    validate_grid_config(config)
    backend = _load_backend(config.strategy_id)
    settings = _settings_from_config(config)
    space = backend.build_parameter_space(config)
    allocation = allocate_mode_budgets(
        space.mode_space_sizes,
        settings.requested_budget,
        method=settings.allocation_method,
        min_quota=settings.min_quota,
        manual_percents=settings.manual_percents,
    )
    return backend.build_preview(space, allocation)


def _metric_value(result: OptimizationResult, metric: str) -> Any:
    if metric == "composite_score":
        return result.score
    return getattr(result, metric, None)


def _objective_values_for_result(
    result: OptimizationResult,
    objectives: Sequence[str],
) -> Optional[List[float]]:
    values: List[float] = []
    for objective in objectives:
        value = _metric_value(result, objective)
        if _is_non_finite(value):
            return None
        values.append(float(value))
    return values


def _dominates_values(candidate: Sequence[float], other: Sequence[float], directions: Sequence[str]) -> bool:
    better_or_equal = True
    strictly_better = False
    for value, other_value, direction in zip(candidate, other, directions):
        if direction == "maximize":
            if value < other_value:
                better_or_equal = False
                break
            if value > other_value:
                strictly_better = True
        else:
            if value > other_value:
                better_or_equal = False
                break
            if value < other_value:
                strictly_better = True
    return better_or_equal and strictly_better


def _mark_grid_pareto(results: List[OptimizationResult], mo_config: MultiObjectiveConfig) -> None:
    if not mo_config.is_multi_objective():
        for result in results:
            result.is_pareto_optimal = None
        return
    directions = mo_config.get_directions()
    feasible = [result for result in results if result.constraints_satisfied is not False]
    pareto_numbers = set()
    for idx, candidate in enumerate(feasible):
        if not candidate.objective_values:
            continue
        dominated = False
        for jdx, other in enumerate(feasible):
            if idx == jdx or not other.objective_values:
                continue
            if _dominates_values(other.objective_values, candidate.objective_values, directions):
                dominated = True
                break
        if not dominated:
            pareto_numbers.add(candidate.optuna_trial_number)
    for result in results:
        result.is_pareto_optimal = bool(result.optuna_trial_number in pareto_numbers)


def rank_grid_results(
    results: List[OptimizationResult],
    *,
    objectives: Sequence[str],
    primary_objective: Optional[str],
    constraints: Sequence[ConstraintSpec],
) -> List[OptimizationResult]:
    if not results:
        return []

    mo_config = MultiObjectiveConfig(list(objectives), primary_objective)
    constraints_enabled = any(spec.enabled for spec in constraints)

    valid: List[OptimizationResult] = []
    for result in results:
        objective_values = _objective_values_for_result(result, objectives)
        if objective_values is None:
            continue
        result.objective_values = objective_values
        all_metrics = {metric: _metric_value(result, metric) for metric in GRID_SUPPORTED_CONSTRAINTS | set(objectives)}
        constraint_values = evaluate_constraints(all_metrics, list(constraints))
        result.constraint_values = constraint_values
        result.constraints_satisfied = all(value <= 0.0 for value in constraint_values) if constraint_values else True
        valid.append(result)

    if not valid:
        raise ValueError("Grid screening produced no candidates with usable objective values.")

    _mark_grid_pareto(valid, mo_config)

    primary = primary_objective or objectives[0]
    primary_idx = list(objectives).index(primary)
    primary_direction = OBJECTIVE_DIRECTIONS[primary]

    def group_rank(item: OptimizationResult) -> int:
        if constraints_enabled and item.constraints_satisfied is False:
            return 2
        if mo_config.is_multi_objective():
            return 0 if item.is_pareto_optimal else 1
        return 0

    def primary_key(item: OptimizationResult) -> float:
        value = item.objective_values[primary_idx] if item.objective_values else 0.0
        return -float(value) if primary_direction == "maximize" else float(value)

    def objective_tie_keys(item: OptimizationResult) -> Tuple[float, ...]:
        values = item.objective_values or []
        tie_values: List[float] = []
        for idx, direction in enumerate(mo_config.get_directions()):
            value = float(values[idx]) if idx < len(values) else 0.0
            tie_values.append(-value if direction == "maximize" else value)
        return tuple(tie_values)

    def semantic_key(item: OptimizationResult) -> str:
        return str(getattr(item, "semantic_key", "") or "")

    ranked = sorted(
        valid,
        key=lambda item: (
            group_rank(item),
            _calculate_total_violation(item.constraint_values, item.constraints_satisfied),
            primary_key(item),
            objective_tie_keys(item),
            semantic_key(item),
            int(getattr(item, "optuna_trial_number", 0) or 0),
        ),
    )
    for rank, item in enumerate(ranked, 1):
        setattr(item, "grid_rank", rank)
    return ranked


def calculate_grid_display_scores(
    results: List[OptimizationResult],
    score_config: Optional[Dict[str, Any]],
) -> List[OptimizationResult]:
    """Calculate optional display scores without filtering Grid candidates."""
    display_score_config = deepcopy(score_config or {})
    display_score_config["filter_enabled"] = False
    return calculate_score(results, display_score_config)


def apply_diversity_cap(
    ranked: Sequence[OptimizationResult],
    *,
    top_n: int,
    enabled: bool,
    max_per_group: int,
) -> Tuple[List[OptimizationResult], Dict[str, Any]]:
    top_n = max(1, int(top_n))
    if not ranked:
        return [], {
            "diversity_enabled": bool(enabled),
            "diversity_group_fields": ["mode", "maType3", "maLength3"],
            "diversity_max_per_group": int(max_per_group),
            "diversity_relaxed_count": 0,
            "warnings": ["No valid Grid candidates were available."],
        }

    if top_n >= len(ranked):
        return list(ranked), {
            "diversity_enabled": bool(enabled),
            "diversity_group_fields": ["mode", "maType3", "maLength3"],
            "diversity_max_per_group": int(max_per_group),
            "diversity_relaxed_count": 0,
            "warnings": ["Top candidates exceeds valid candidates; selected all valid candidates."],
        }

    if not enabled:
        return list(ranked[:top_n]), {
            "diversity_enabled": False,
            "diversity_group_fields": ["mode", "maType3", "maLength3"],
            "diversity_max_per_group": int(max_per_group),
            "diversity_relaxed_count": 0,
            "warnings": [],
        }

    group_counts: Dict[str, int] = {}
    selected: List[OptimizationResult] = []
    for item in ranked:
        group = str(getattr(item, "diversity_group", "") or "")
        count = group_counts.get(group, 0)
        if count >= max_per_group:
            continue
        selected.append(item)
        group_counts[group] = count + 1
        if len(selected) >= top_n:
            break

    relaxed_count = 0
    warnings: List[str] = []
    if len(selected) < top_n:
        selected_ids = {id(item) for item in selected}
        for item in ranked:
            if id(item) in selected_ids:
                continue
            selected.append(item)
            relaxed_count += 1
            if len(selected) >= top_n:
                break
        warnings.append("Diversity cap was relaxed to fill the requested top candidates.")

    return selected, {
        "diversity_enabled": True,
        "diversity_group_fields": ["mode", "maType3", "maLength3"],
        "diversity_max_per_group": int(max_per_group),
        "diversity_relaxed_count": relaxed_count,
        "warnings": warnings,
    }


def _finite_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _selection_sources(result: OptimizationResult) -> List[str]:
    raw = getattr(result, "selection_sources", None)
    if isinstance(raw, (list, tuple)):
        return [str(item) for item in raw]
    if isinstance(raw, str) and raw:
        return [raw]
    return []


def _add_selection_source(result: OptimizationResult, source: str) -> None:
    sources = _selection_sources(result)
    if source not in sources:
        sources.append(source)
    source_order = {"objective": 0, "dsr": 1}
    sources.sort(key=lambda item: (source_order.get(item, 99), item))
    setattr(result, "selection_sources", sources)
    setattr(result, f"is_{source}_selected", True)


def apply_fast_grid_dsr(
    ranked_fast: Sequence[OptimizationResult],
    *,
    top_k: int,
) -> Tuple[List[OptimizationResult], Dict[str, Any]]:
    """Compute fast DSR fields and select DSR top-K from the full valid Grid pool."""
    top_k = max(1, int(top_k or 1))
    finite_sharpes = [
        value
        for value in (_finite_float(getattr(result, "sharpe_ratio", None)) for result in ranked_fast)
        if value is not None
    ]
    mean_sharpe = sum(finite_sharpes) / len(finite_sharpes) if finite_sharpes else None
    var_sharpe = None
    if finite_sharpes:
        var_sharpe = sum((value - mean_sharpe) ** 2 for value in finite_sharpes) / len(finite_sharpes)

    sr0 = None
    if var_sharpe is not None:
        sr0 = calculate_expected_max_sharpe(0.0, var_sharpe, len(finite_sharpes))

    candidates: List[OptimizationResult] = []
    for result in ranked_fast:
        sr_value = _finite_float(getattr(result, "sharpe_ratio", None))
        skewness = _finite_float(getattr(result, "dsr_skewness", None))
        kurtosis = _finite_float(getattr(result, "dsr_kurtosis", None))
        try:
            track_length = int(getattr(result, "dsr_track_length", 0) or 0)
        except (TypeError, ValueError):
            track_length = 0

        dsr_probability = None
        luck_share = None
        if sr_value is not None and sr0 is not None and skewness is not None and kurtosis is not None:
            dsr_probability = calculate_dsr(sr_value, sr0, skewness, kurtosis, track_length)
            luck_share = calculate_luck_share(sr_value, sr0)

        setattr(result, "dsr_probability", dsr_probability)
        setattr(result, "dsr_skewness", skewness)
        setattr(result, "dsr_kurtosis", kurtosis)
        setattr(result, "dsr_track_length", track_length if track_length > 0 else None)
        setattr(result, "dsr_luck_share_pct", luck_share)
        if sr_value is not None:
            candidates.append(result)

    def dsr_sort_key(item: OptimizationResult) -> Tuple[Any, ...]:
        probability = getattr(item, "dsr_probability", None)
        probability_key = float(probability) if probability is not None else float("-inf")
        return (
            probability is None,
            -probability_key,
            int(getattr(item, "grid_rank", 0) or 0),
            str(getattr(item, "semantic_key", "") or ""),
            int(getattr(item, "candidate_id", getattr(item, "optuna_trial_number", 0)) or 0),
        )

    ranked_dsr = sorted(candidates, key=dsr_sort_key)
    selected = ranked_dsr[: min(top_k, len(ranked_dsr))]
    for rank, result in enumerate(selected, 1):
        setattr(result, "dsr_rank", rank)
        _add_selection_source(result, "dsr")

    return selected, {
        "enabled": True,
        "top_k": top_k,
        "selected_count": len(selected),
        "dsr_n_trials": len(finite_sharpes),
        "dsr_mean_sharpe": mean_sharpe,
        "dsr_var_sharpe": var_sharpe,
        "dsr_sr0": sr0,
    }


def _union_selected_candidates(
    objective_selected: Sequence[OptimizationResult],
    dsr_selected: Sequence[OptimizationResult],
) -> List[OptimizationResult]:
    selected: List[OptimizationResult] = []
    seen: set[str] = set()
    for result in objective_selected:
        _add_selection_source(result, "objective")
        key = str(getattr(result, "semantic_key", "") or getattr(result, "candidate_id", id(result)))
        if key in seen:
            continue
        seen.add(key)
        selected.append(result)

    for result in sorted(
        dsr_selected,
        key=lambda item: (
            int(getattr(item, "dsr_rank", 0) or 0),
            int(getattr(item, "grid_rank", 0) or 0),
            str(getattr(item, "semantic_key", "") or ""),
        ),
    ):
        key = str(getattr(result, "semantic_key", "") or getattr(result, "candidate_id", id(result)))
        if key in seen:
            continue
        seen.add(key)
        selected.append(result)
    return selected


def build_grid_dsr_results(
    results: Sequence[OptimizationResult],
    *,
    limit: Optional[int] = None,
) -> List[DSRResult]:
    """Build Post Process DSR payloads from precomputed Grid DSR metadata."""
    selected = [
        result
        for result in (results or [])
        if bool(getattr(result, "is_dsr_selected", False)) or getattr(result, "dsr_rank", None) is not None
    ]
    selected.sort(
        key=lambda item: (
            int(getattr(item, "dsr_rank", 10**9) or 10**9),
            int(getattr(item, "grid_rank", 10**9) or 10**9),
            str(getattr(item, "semantic_key", "") or ""),
            int(getattr(item, "candidate_id", getattr(item, "optuna_trial_number", 0)) or 0),
        )
    )
    if limit is not None:
        selected = selected[: max(0, int(limit))]

    payloads: List[DSRResult] = []
    for idx, result in enumerate(selected, 1):
        trial_number = getattr(result, "optuna_trial_number", None)
        if trial_number is None:
            trial_number = getattr(result, "candidate_id", idx)
        payloads.append(
            DSRResult(
                trial_number=int(trial_number),
                optuna_rank=int(getattr(result, "grid_rank", idx) or idx),
                params=dict(getattr(result, "params", {}) or {}),
                original_result=result,
                dsr_probability=getattr(result, "dsr_probability", None),
                dsr_rank=getattr(result, "dsr_rank", None),
                dsr_skewness=getattr(result, "dsr_skewness", None),
                dsr_kurtosis=getattr(result, "dsr_kurtosis", None),
                dsr_track_length=getattr(result, "dsr_track_length", None),
                dsr_luck_share_pct=getattr(result, "dsr_luck_share_pct", None),
            )
        )
    return payloads


def _refresh_selected_metrics(
    results: Sequence[OptimizationResult],
    *,
    objectives: Sequence[str],
    constraints: Sequence[ConstraintSpec],
) -> List[OptimizationResult]:
    refreshed: List[OptimizationResult] = []
    for result in results:
        objective_values = _objective_values_for_result(result, objectives)
        if objective_values is not None:
            result.objective_values = objective_values
        all_metrics = {metric: _metric_value(result, metric) for metric in GRID_SUPPORTED_CONSTRAINTS | set(objectives)}
        constraint_values = evaluate_constraints(all_metrics, list(constraints))
        result.constraint_values = constraint_values
        result.constraints_satisfied = all(value <= 0.0 for value in constraint_values) if constraint_values else True
        refreshed.append(result)
    return refreshed


def _prepare_grid_dataframe(config: OptimizationConfig) -> Tuple[pd.DataFrame, int, Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    df = load_data(config.csv_file)
    fixed = config.fixed_params or {}
    use_date_filter = bool(fixed.get("dateFilter", False))
    start_ts, end_ts = align_date_bounds(df.index, fixed.get("start"), fixed.get("end"))
    trade_start_idx = 0
    if use_date_filter and (start_ts is not None or end_ts is not None):
        df, trade_start_idx = prepare_dataset_with_warmup(
            df,
            start_ts,
            end_ts,
            int(getattr(config, "warmup_bars", 1000) or 1000),
        )
    return df, trade_start_idx, start_ts, end_ts


def _resolve_csv_path_for_storage(csv_file: Any) -> str:
    if isinstance(csv_file, (str, Path)):
        try:
            return str(Path(csv_file).resolve())
        except Exception:
            return str(csv_file)
    name = getattr(csv_file, "name", "")
    if name:
        try:
            return str(Path(name).resolve())
        except Exception:
            return str(name)
    return ""


def run_grid_optimization(
    config: OptimizationConfig,
    *,
    save_study: bool = True,
) -> Tuple[List[OptimizationResult], Optional[str]]:
    validate_grid_config(config)
    backend = _load_backend(config.strategy_id)
    settings = _settings_from_config(config)
    constraints = _build_constraint_specs(getattr(config, "constraints", []) or [])
    objectives = list(getattr(config, "objectives", []) or ["net_profit_pct"])
    primary_objective = getattr(config, "primary_objective", None)
    needs_dsr = bool(getattr(config, "grid_needs_dsr", False))

    started = time.time()
    timings: Dict[str, float] = {}

    space_started = time.time()
    space = backend.build_parameter_space(config)
    allocation = allocate_mode_budgets(
        space.mode_space_sizes,
        settings.requested_budget,
        method=settings.allocation_method,
        min_quota=settings.min_quota,
        manual_percents=settings.manual_percents,
    )
    preview = backend.build_preview(space, allocation)
    timings["parameter_space_seconds"] = time.time() - space_started

    generation_started = time.time()
    candidate_set = backend.generate_candidates(config, space, allocation, seed=settings.seed)
    timings["candidate_generation_seconds"] = time.time() - generation_started
    if not candidate_set.candidates:
        raise ValueError("Grid generated no candidates.")

    data_started = time.time()
    df, trade_start_idx, start_ts, end_ts = _prepare_grid_dataframe(config)
    fast_data = backend.prepare_fast_data(df, trade_start_idx, candidate_set.candidates)
    timings["ma_cache_build_seconds"] = getattr(fast_data, "ma_cache_build_seconds", 0.0)
    timings["data_prepare_seconds"] = time.time() - data_started

    eval_started = time.time()
    all_fast_results = backend.evaluate_candidates(
        fast_data,
        candidate_set.candidates,
        n_workers=int(getattr(config, "worker_processes", 1) or 1),
        needs_dsr=needs_dsr,
    )
    timings["fast_evaluation_seconds"] = time.time() - eval_started

    ranked_fast = rank_grid_results(
        all_fast_results,
        objectives=objectives,
        primary_objective=primary_objective,
        constraints=constraints,
    )
    selected_fast, diversity_metadata = apply_diversity_cap(
        ranked_fast,
        top_n=settings.top_candidates,
        enabled=settings.diversity_enabled,
        max_per_group=settings.diversity_max_per_group,
    )
    objective_selected_count = len(selected_fast)

    if needs_dsr:
        dsr_top_k = max(
            1,
            int(getattr(config, "grid_dsr_top_k", settings.top_candidates) or settings.top_candidates),
        )
        dsr_selected_fast, dsr_metadata = apply_fast_grid_dsr(ranked_fast, top_k=dsr_top_k)
    else:
        dsr_selected_fast = []
        dsr_metadata = {
            "enabled": False,
            "top_k": None,
            "selected_count": 0,
            "dsr_n_trials": None,
            "dsr_mean_sharpe": None,
            "dsr_var_sharpe": None,
            "dsr_sr0": None,
        }
    union_fast = _union_selected_candidates(selected_fast, dsr_selected_fast)

    validation_started = time.time()
    selected_results = backend.validate_selected_candidates(
        df,
        trade_start_idx,
        union_fast,
        tolerances=settings.validation_tolerances,
        fail_on_error=settings.strict_validation,
    )
    timings["slow_validation_seconds"] = time.time() - validation_started

    ranked_selected = _refresh_selected_metrics(
        selected_results,
        objectives=objectives,
        constraints=constraints,
    )
    ranked_selected = calculate_grid_display_scores(ranked_selected, getattr(config, "score_config", None))
    for result in ranked_selected:
        setattr(result, "optimizer_mode", GRID_MODE)
        result.optuna_trial_number = int(
            getattr(result, "candidate_id", result.optuna_trial_number or getattr(result, "grid_rank", 0) or 0)
        )

    total_seconds = time.time() - started
    timings["total_seconds"] = total_seconds
    actual_candidates = len(candidate_set.candidates)
    candidates_per_second = actual_candidates / timings["fast_evaluation_seconds"] if timings["fast_evaluation_seconds"] > 0 else None
    union_selected_count = len(ranked_selected)
    dsr_selected_count = int(dsr_metadata.get("selected_count") or 0)

    summary = {
        "method": "Grid",
        "optimizer_mode": GRID_MODE,
        "objectives": objectives,
        "primary_objective": primary_objective,
        "requested_budget": allocation.requested_budget,
        "actual_budget": allocation.actual_budget,
        "unused_budget": allocation.unused_budget,
        "total_trials": allocation.actual_budget,
        "completed_trials": len(ranked_fast),
        "pruned_trials": 0,
        "candidate_count": actual_candidates,
        "valid_candidate_count": len(ranked_fast),
        "selected_candidate_count": union_selected_count,
        "full_candidate_count": actual_candidates,
        "objective_selected_count": objective_selected_count,
        "dsr_selected_count": dsr_selected_count,
        "union_selected_count": union_selected_count,
        "dsr_n_trials": dsr_metadata.get("dsr_n_trials"),
        "dsr_mean_sharpe": dsr_metadata.get("dsr_mean_sharpe"),
        "dsr_var_sharpe": dsr_metadata.get("dsr_var_sharpe"),
        "best_trial_number": getattr(ranked_selected[0], "candidate_id", None) if ranked_selected else None,
        "best_value": ranked_selected[0].objective_values[0] if ranked_selected and ranked_selected[0].objective_values else None,
        "best_values": dict(zip(objectives, ranked_selected[0].objective_values)) if ranked_selected and len(objectives) > 1 else None,
        "pareto_front_size": sum(1 for result in ranked_fast if getattr(result, "is_pareto_optimal", False))
        if len(objectives) > 1
        else None,
        "optimization_time_seconds": total_seconds,
        "grid": {
            "preview": preview,
            "allocation": allocation.__dict__,
            "candidate_generation": candidate_set.diagnostics,
            "diversity": diversity_metadata,
            "ma_cache_entries": getattr(fast_data, "ma_cache_entries", None),
            "ma_cache_estimated_mb": getattr(fast_data, "ma_cache_estimated_mb", None),
            "dsr_metric_computation_enabled": needs_dsr,
            "dsr": dsr_metadata,
            "full_candidate_count": actual_candidates,
            "objective_selected_count": objective_selected_count,
            "dsr_selected_count": dsr_selected_count,
            "union_selected_count": union_selected_count,
            "timings": timings,
            "candidates_per_second": candidates_per_second,
            "start": start_ts.isoformat() if isinstance(start_ts, pd.Timestamp) else None,
            "end": end_ts.isoformat() if isinstance(end_ts, pd.Timestamp) else None,
        },
    }
    setattr(config, "optuna_summary", summary)
    setattr(config, "grid_summary", summary)
    setattr(config, "optuna_all_results", all_fast_results)

    study_id = None
    if save_study:
        from .storage import save_grid_study_to_db

        study_id = save_grid_study_to_db(
            config=config,
            grid_settings=settings,
            grid_summary=summary,
            trial_results=ranked_selected,
            csv_file_path=_resolve_csv_path_for_storage(getattr(config, "csv_file", "")),
            start_time=started,
            score_config=getattr(config, "score_config", None),
        )
    return ranked_selected, study_id
