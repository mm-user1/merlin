"""Optuna-based Bayesian optimization engine for S_01 TrailingMA."""
from __future__ import annotations

import bisect
import io
import itertools
import json
import logging
import math
import multiprocessing as mp
import queue
import time
import traceback
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import optuna
from optuna.pruners import MedianPruner, PercentilePruner, PatientPruner
from optuna.samplers import NSGAIIISampler, NSGAIISampler, RandomSampler, TPESampler
from optuna.storages.journal import BaseJournalBackend
from optuna.trial import TrialState
import pandas as pd

from . import metrics
from .backtest_engine import load_data
from .storage import save_optuna_study_to_db

logger = logging.getLogger(__name__)
OPTUNA_LOGGER = optuna.logging.get_logger("optuna")

_DUPLICATE_SKIPPED_ATTR = "merlin.duplicate_skipped"
_DUPLICATE_SKIP_REASON_ATTR = "merlin.duplicate_skip_reason"
_UNSET = object()


class InMemoryJournalBackend(BaseJournalBackend):
    """Process-shared in-memory journal backend for multiprocess Optuna."""

    def __init__(self, shared_logs: Any) -> None:
        self._logs = shared_logs

    def read_logs(self, log_number_from: int):
        return list(self._logs[log_number_from:])

    def append_logs(self, logs: List[Dict[str, Any]]) -> None:
        if logs:
            self._logs.extend(logs)


# ============================================================================
# Data structures
# ============================================================================


@dataclass
class OptimizationConfig:
    """Generic optimization configuration for any strategy."""

    # Required fields
    csv_file: Any
    strategy_id: str
    enabled_params: Dict[str, bool]
    param_ranges: Dict[str, Tuple[float, float, float]]
    param_types: Dict[str, str]
    fixed_params: Dict[str, Any]

    # Execution settings
    worker_processes: int = 1
    warmup_bars: int = 1000
    csv_original_name: Optional[str] = None

    # Strategy-specific execution defaults
    contract_size: float = 1.0
    commission_rate: float = 0.0005
    risk_per_trade_pct: float = 1.0

    # Optimization control
    filter_min_profit: bool = False
    min_profit_threshold: float = 0.0
    score_config: Optional[Dict[str, Any]] = None
    detailed_log: bool = False
    optimization_mode: str = "optuna"
    objectives: List[str] = field(default_factory=list)
    primary_objective: Optional[str] = None
    constraints: List[Dict[str, Any]] = field(default_factory=list)
    sanitize_enabled: bool = True
    sanitize_trades_threshold: int = 0
    sampler_type: str = "tpe"
    population_size: int = 50
    crossover_prob: float = 0.9
    mutation_prob: Optional[float] = None
    swapping_prob: float = 0.5
    n_startup_trials: int = 20
    coverage_mode: bool = False


@dataclass
class OptimizationResult:
    """Generic optimization result for any strategy."""

    params: Dict[str, Any]
    net_profit_pct: float
    max_drawdown_pct: float
    total_trades: int
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    max_consecutive_losses: int = 0
    romad: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    profit_factor: Optional[float] = None
    ulcer_index: Optional[float] = None
    sqn: Optional[float] = None
    consistency_score: Optional[float] = None
    score: float = 0.0
    optuna_trial_number: Optional[int] = None
    objective_values: List[float] = field(default_factory=list)
    constraint_values: List[float] = field(default_factory=list)
    constraints_satisfied: Optional[bool] = None
    is_pareto_optimal: Optional[bool] = None
    dominance_rank: Optional[int] = None


@dataclass
class MultiObjectiveConfig:
    """Configuration for optimization objectives."""

    objectives: List[str]
    primary_objective: Optional[str] = None

    def is_multi_objective(self) -> bool:
        return len(self.objectives) > 1

    def get_directions(self) -> List[str]:
        return [OBJECTIVE_DIRECTIONS[obj] for obj in self.objectives]

    def get_single_direction(self) -> str:
        assert len(self.objectives) == 1
        return OBJECTIVE_DIRECTIONS[self.objectives[0]]

    def get_metric_names(self) -> List[str]:
        return [OBJECTIVE_DISPLAY_NAMES[obj] for obj in self.objectives]


@dataclass
class ConstraintSpec:
    """Specification for a single constraint."""

    metric: str
    threshold: float
    enabled: bool = False

    @property
    def operator(self) -> str:
        return CONSTRAINT_OPERATORS[self.metric]


@dataclass
class SamplerConfig:
    """Configuration for Optuna sampler."""

    sampler_type: str = "tpe"
    population_size: int = 50
    crossover_prob: float = 0.9
    mutation_prob: Optional[float] = None
    swapping_prob: float = 0.5
    n_startup_trials: int = 20


# ============================================================================
# Constants
# ============================================================================

SCORE_METRIC_ATTRS: Dict[str, str] = {
    "romad": "romad",
    "sharpe": "sharpe_ratio",
    "pf": "profit_factor",
    "ulcer": "ulcer_index",
    "sqn": "sqn",
    "consistency": "consistency_score",
}

DEFAULT_METRIC_BOUNDS: Dict[str, Dict[str, float]] = {
    "romad": {"min": 0.0, "max": 10.0},
    "sharpe": {"min": -1.0, "max": 3.0},
    "pf": {"min": 0.0, "max": 5.0},
    "ulcer": {"min": 0.0, "max": 20.0},
    "sqn": {"min": -2.0, "max": 7.0},
    "consistency": {"min": -1.0, "max": 1.0},
}

DEFAULT_SCORE_CONFIG: Dict[str, Any] = {
    "weights": {},
    "enabled_metrics": {},
    "invert_metrics": {},
    "normalization_method": "minmax",
    "filter_enabled": False,
    "min_score_threshold": 0.0,
    "metric_bounds": DEFAULT_METRIC_BOUNDS,
}

OBJECTIVE_DIRECTIONS: Dict[str, str] = {
    "net_profit_pct": "maximize",
    "max_drawdown_pct": "minimize",
    "sharpe_ratio": "maximize",
    "sortino_ratio": "maximize",
    "romad": "maximize",
    "profit_factor": "maximize",
    "win_rate": "maximize",
    "sqn": "maximize",
    "ulcer_index": "minimize",
    "consistency_score": "maximize",
    "composite_score": "maximize",
}

OBJECTIVE_DISPLAY_NAMES: Dict[str, str] = {
    "net_profit_pct": "Net Profit %",
    "max_drawdown_pct": "Min Drawdown %",
    "sharpe_ratio": "Sharpe Ratio",
    "sortino_ratio": "Sortino Ratio",
    "romad": "RoMaD",
    "profit_factor": "Profit Factor",
    "win_rate": "Win Rate %",
    "sqn": "SQN",
    "ulcer_index": "Ulcer Index",
    "consistency_score": "Consistency",
    "composite_score": "Composite Score",
}

SANITIZE_METRICS = {"sharpe_ratio", "sortino_ratio", "sqn", "profit_factor"}

CONSTRAINT_OPERATORS: Dict[str, str] = {
    "total_trades": "gte",
    "net_profit_pct": "gte",
    "max_drawdown_pct": "lte",
    "sharpe_ratio": "gte",
    "sortino_ratio": "gte",
    "romad": "gte",
    "profit_factor": "gte",
    "win_rate": "gte",
    "max_consecutive_losses": "lte",
    "sqn": "gte",
    "ulcer_index": "lte",
    "consistency_score": "gte",
}

_LEGACY_CONSISTENCY_BOUNDS = {"min": 0.0, "max": 100.0}


def _migrate_legacy_consistency_bounds(metric_bounds: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    """
    Upgrade stale score configs that still treat consistency as 0..100 percent.

    The metric semantics changed to signed R² in [-1, 1]. Old saved queue/study
    configs may still carry the retired 0..100 bounds, which would distort score
    normalization if used verbatim.
    """
    bounds = {
        key: {"min": float(value.get("min", 0.0)), "max": float(value.get("max", 100.0))}
        for key, value in metric_bounds.items()
    }

    consistency = bounds.get("consistency")
    if consistency is None:
        return bounds

    if (
        math.isclose(consistency.get("min", 0.0), _LEGACY_CONSISTENCY_BOUNDS["min"])
        and math.isclose(consistency.get("max", 0.0), _LEGACY_CONSISTENCY_BOUNDS["max"])
    ):
        bounds["consistency"] = dict(DEFAULT_METRIC_BOUNDS["consistency"])

    return bounds


# ============================================================================
# Utilities
# ============================================================================


def _is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


def _is_non_finite(value: Any) -> bool:
    if value is None:
        return True
    try:
        return not math.isfinite(float(value))
    except (TypeError, ValueError):
        return True


def _is_inf(value: Any) -> bool:
    if value is None:
        return False
    try:
        return math.isinf(float(value))
    except (TypeError, ValueError):
        return False


def _format_objective_value(value: Any) -> str:
    if _is_inf(value):
        return "Inf" if float(value) > 0 else "-Inf"
    if _is_non_finite(value):
        return "NaN"


def _build_params_key(params: Dict[str, Any]) -> str:
    """Create a deterministic key for exact duplicate suppression."""
    return json.dumps(params, sort_keys=True, separators=(",", ":"), default=str)


def _estimate_search_space_size(search_space: Dict[str, Dict[str, Any]]) -> Optional[int]:
    """Return the exact number of unique combinations for finite discrete spaces."""
    total = 1
    for spec in (search_space or {}).values():
        p_type = str(spec.get("type", "")).lower()
        if p_type == "categorical":
            choices = list(spec.get("choices") or [])
            if not choices:
                return 0
            total *= len(choices)
            continue

        if p_type == "int":
            low = int(spec.get("low", 0))
            high = int(spec.get("high", 0))
            step = max(1, int(spec.get("step", 1) or 1))
            total *= max(0, ((high - low) // step) + 1)
            continue

        if p_type == "float":
            step = spec.get("step")
            if step in (None, 0, 0.0) or spec.get("log"):
                return None
            try:
                low = float(spec.get("low", 0.0))
                high = float(spec.get("high", 0.0))
                step_val = float(step)
            except (TypeError, ValueError):
                return None
            if step_val <= 0:
                return None
            levels = int(math.floor(((high - low) / step_val) + 1e-9) + 1)
            total *= max(0, levels)
            continue

        return None

    return total


def _resolve_strategy_param_type(
    base_config: OptimizationConfig,
    param_name: str,
    param_spec: Dict[str, Any],
) -> str:
    return str(base_config.param_types.get(param_name, param_spec.get("type", "float"))).lower()


def _is_duplicate_skipped_trial(trial: Any) -> bool:
    attrs = getattr(trial, "user_attrs", {}) or {}
    if not isinstance(attrs, dict):
        return False
    return bool(attrs.get(_DUPLICATE_SKIPPED_ATTR, False))
    try:
        return str(float(value))
    except (TypeError, ValueError):
        return "NaN"


def _coerce_bool_value(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    return None


def _normalize_bool_choices(raw_values: Any) -> List[bool]:
    if raw_values is None:
        return []
    if isinstance(raw_values, (list, tuple)):
        source_values = raw_values
    else:
        source_values = [raw_values]

    normalized: List[bool] = []
    for raw in source_values:
        parsed = _coerce_bool_value(raw)
        if parsed is None or parsed in normalized:
            continue
        normalized.append(parsed)
    return normalized


def _normalize_param_dependencies(raw_value: Any) -> Tuple[str, ...]:
    if raw_value is None:
        return ()
    if isinstance(raw_value, str):
        source_values = [raw_value]
    elif isinstance(raw_value, (list, tuple)):
        source_values = list(raw_value)
    else:
        raise ValueError("Parameter 'depends_on' must be a string or list of strings.")

    normalized: List[str] = []
    seen: Set[str] = set()
    for item in source_values:
        name = str(item or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        normalized.append(name)
    return tuple(normalized)


def _extract_bool_group_rules(strategy_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(strategy_config, dict):
        return []

    optimization_rules = strategy_config.get("optimization_rules")
    if not isinstance(optimization_rules, dict):
        return []

    raw_groups = optimization_rules.get("bool_groups")
    if not isinstance(raw_groups, list):
        return []

    rules: List[Dict[str, Any]] = []
    for raw_group in raw_groups:
        if not isinstance(raw_group, dict):
            continue

        raw_params = (
            raw_group.get("params")
            or raw_group.get("parameters")
            or raw_group.get("members")
        )
        if not isinstance(raw_params, list):
            continue

        deduped_params: List[str] = []
        seen: Set[str] = set()
        for raw_name in raw_params:
            name = str(raw_name or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            deduped_params.append(name)
        if len(deduped_params) < 2:
            continue

        rules.append(
            {
                "params": deduped_params,
                "mode": str(raw_group.get("mode", "at_least_one_true")).strip().lower(),
            }
        )

    return rules


_COVERAGE_FLOAT_TOL = 1e-12
_COVERAGE_RECOMMENDED_MULTIPLIER = 2


def _extract_coverage_axes(
    search_space: Dict[str, Dict[str, Any]],
) -> Tuple[List[Tuple[str, List[Any]]], Dict[str, Dict[str, Any]]]:
    categorical_axes: List[Tuple[str, List[Any]]] = []
    numeric_specs: Dict[str, Dict[str, Any]] = {}

    for name, spec in (search_space or {}).items():
        p_type = str(spec.get("type", "")).lower()
        if p_type == "categorical":
            choices = list(spec.get("choices") or [])
            if choices:
                categorical_axes.append((name, choices))
            continue
        if p_type in {"int", "float"}:
            numeric_specs[name] = spec

    return categorical_axes, numeric_specs


def _infer_primary_numeric_param(
    main_axis_name: Optional[str],
    numeric_specs: Dict[str, Dict[str, Any]],
) -> Optional[str]:
    numeric_names = list(numeric_specs.keys())
    if not numeric_names:
        return None
    unconditional_names = [
        name for name, spec in numeric_specs.items() if not tuple(spec.get("depends_on") or ())
    ]
    candidate_names = unconditional_names or numeric_names
    if not main_axis_name:
        return candidate_names[0]

    axis_name = str(main_axis_name)
    axis_lower = axis_name.lower()
    candidates: List[str] = []

    # Common pairings like maType -> maLength and maType3 -> maLength3.
    candidates.extend(
        [
            axis_name.replace("Type", "Length"),
            axis_name.replace("type", "length"),
            axis_name.replace("_type", "_length"),
            axis_name.replace("_Type", "_Length"),
            axis_name.replace("Type", "Period"),
            axis_name.replace("type", "period"),
            axis_name.replace("_type", "_period"),
            axis_name.replace("_Type", "_Period"),
        ]
    )

    if axis_lower.endswith("type"):
        root = axis_name[:-4]
        candidates.extend([f"{root}Length", f"{root}length", f"{root}Period", f"{root}period"])
    if axis_lower.endswith("_type"):
        root = axis_name[:-5]
        candidates.extend([f"{root}_length", f"{root}_Length", f"{root}_period", f"{root}_Period"])

    seen = set()
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            if candidate in candidate_names:
                return candidate

    digits = "".join(ch for ch in axis_name if ch.isdigit())
    if digits:
        for name in numeric_names:
            if name in candidate_names and (
                name.endswith(digits) and ("length" in name.lower() or "period" in name.lower())
            ):
                return name

    for name in candidate_names:
        if "length" in name.lower() or "period" in name.lower():
            return name

    return candidate_names[0]


def _fraction_to_level_index(fraction: float, levels: int) -> int:
    if levels <= 1:
        return 0
    bounded = max(0.0, min(1.0, float(fraction)))
    target = bounded * float(levels - 1)
    lower = int(math.floor(target))
    upper = int(math.ceil(target))
    if upper == lower:
        return lower
    dist_lower = target - float(lower)
    dist_upper = float(upper) - target
    if abs(dist_lower - dist_upper) <= _COVERAGE_FLOAT_TOL:
        return lower
    return lower if dist_lower < dist_upper else upper


def _quantize_numeric_fraction(norm_value: float, spec: Dict[str, Any]) -> Union[int, float]:
    bounded = max(0.0, min(1.0, float(norm_value)))
    p_type = str(spec.get("type", "")).lower()

    low = float(spec.get("low"))
    high = float(spec.get("high"))
    if high < low:
        low, high = high, low

    if p_type == "int":
        low_i = int(round(low))
        high_i = int(round(high))
        step_i = max(1, int(round(float(spec.get("step", 1) or 1))))
        levels = int(((high_i - low_i) // step_i) + 1)
        levels = max(1, levels)
        idx = _fraction_to_level_index(bounded, levels)
        value = low_i + idx * step_i
        return int(max(low_i, min(high_i, value)))

    step = spec.get("step")
    if step not in (None, 0, 0.0):
        step_f = float(step)
        if math.isfinite(step_f) and step_f > 0:
            levels = int(math.floor(((high - low) / step_f) + 1e-9) + 1)
            levels = max(1, levels)
            idx = _fraction_to_level_index(bounded, levels)
            quantized = low + idx * step_f
            quantized = max(low, min(high, quantized))
            return float(round(quantized, 12))

    value = low + bounded * (high - low)
    return float(round(max(low, min(high, value)), 12))


def _build_anchor_fractions(full_blocks: int) -> List[float]:
    block_count = max(0, int(full_blocks or 0))
    if block_count <= 0:
        return []
    if block_count == 1:
        return [0.5]
    if block_count == 2:
        return [1.0 / 3.0, 2.0 / 3.0]
    denominator = float(block_count - 1)
    return [idx / denominator for idx in range(block_count)]


def _next_partial_anchor_fraction(full_blocks: int, used_fractions: List[float]) -> float:
    if full_blocks <= 0:
        return 0.5
    candidates = _build_anchor_fractions(int(full_blocks) + 1)
    unused = [
        candidate
        for candidate in candidates
        if all(abs(candidate - used) > _COVERAGE_FLOAT_TOL for used in used_fractions)
    ]
    if not unused:
        return 0.5
    return min(unused, key=lambda value: (abs(value - 0.5), value))


def _build_categorical_combinations(
    categorical_axes: List[Tuple[str, List[Any]]]
) -> List[Dict[str, Any]]:
    if not categorical_axes:
        return [{}]
    axis_names = [name for name, _ in categorical_axes]
    axis_choices = [choices for _, choices in categorical_axes]
    combinations: List[Dict[str, Any]] = []
    for values in itertools.product(*axis_choices):
        combinations.append(dict(zip(axis_names, values)))
    return combinations


def _analyze_coverage_requirements(
    search_space: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    categorical_axes, numeric_specs = _extract_coverage_axes(search_space)

    coverage_block_size = 1
    for _, choices in categorical_axes:
        coverage_block_size *= max(1, len(choices))

    main_axis_name: Optional[str] = None
    main_axis_options = 1
    if categorical_axes:
        main_axis_name, main_axis_choices = max(categorical_axes, key=lambda item: len(item[1]))
        main_axis_options = max(1, len(main_axis_choices))

    primary_numeric_name = _infer_primary_numeric_param(main_axis_name, numeric_specs)
    n_min = int(max(1, coverage_block_size))
    n_rec = int(max(n_min, n_min * _COVERAGE_RECOMMENDED_MULTIPLIER))

    return {
        "n_min": n_min,
        "n_rec": n_rec,
        "coverage_block_size": n_min,
        "coverage_combinations": n_min,
        "categorical_axes_count": int(len(categorical_axes)),
        "numeric_axes_count": int(len(numeric_specs)),
        "main_axis_name": main_axis_name,
        "main_axis_options": int(main_axis_options),
        "primary_numeric_name": primary_numeric_name,
    }


def _generate_coverage_trials(
    search_space: Dict[str, Dict[str, Any]],
    n_trials: int,
) -> List[Dict[str, Any]]:
    target_count = max(0, int(n_trials or 0))
    if target_count <= 0:
        return []
    coverage_report = _analyze_coverage_requirements(search_space)
    categorical_axes, numeric_specs = _extract_coverage_axes(search_space)
    categorical_combinations = _build_categorical_combinations(categorical_axes)
    combination_count = max(1, len(categorical_combinations))

    full_blocks = target_count // combination_count
    remainder = target_count % combination_count
    primary_numeric_name = coverage_report.get("primary_numeric_name")
    midpoint_values: Dict[str, Union[int, float]] = {
        name: _quantize_numeric_fraction(0.5, spec) for name, spec in numeric_specs.items()
    }

    results: List[Dict[str, Any]] = []
    anchor_fractions = _build_anchor_fractions(full_blocks)

    for fraction in anchor_fractions:
        primary_override = None
        if primary_numeric_name and primary_numeric_name in numeric_specs:
            primary_override = _quantize_numeric_fraction(
                fraction, numeric_specs[primary_numeric_name]
            )
        for combo in categorical_combinations:
            params: Dict[str, Any] = dict(combo)
            params.update(midpoint_values)
            if primary_numeric_name and primary_override is not None:
                params[primary_numeric_name] = primary_override
            results.append(params)

    if remainder > 0:
        partial_fraction = _next_partial_anchor_fraction(full_blocks, anchor_fractions)
        primary_override = None
        if primary_numeric_name and primary_numeric_name in numeric_specs:
            primary_override = _quantize_numeric_fraction(
                partial_fraction, numeric_specs[primary_numeric_name]
            )
        start_offset = (full_blocks * 37 + remainder * 11) % combination_count
        for idx in range(remainder):
            combo = categorical_combinations[(start_offset + idx) % combination_count]
            params = dict(combo)
            params.update(midpoint_values)
            if primary_numeric_name and primary_override is not None:
                params[primary_numeric_name] = primary_override
            results.append(params)

    return results[:target_count]


def evaluate_constraints(
    all_metrics: Dict[str, Any],
    constraints: List[ConstraintSpec],
) -> List[float]:
    """
    Evaluate soft constraints.

    Returns list where:
        > 0: constraint violated
        <= 0: constraint satisfied

    Missing/NaN values are treated as VIOLATED.
    """
    enabled_constraints = [c for c in constraints if c.enabled]
    if not enabled_constraints:
        return []

    violations: List[float] = []
    for spec in enabled_constraints:
        value = all_metrics.get(spec.metric)
        if _is_non_finite(value):
            violations.append(1.0)
            continue

        value = float(value)
        if spec.operator == "gte":
            violation = spec.threshold - value
        else:
            violation = value - spec.threshold
        violations.append(violation)

    return violations


def create_constraints_func(constraints: List[ConstraintSpec]):
    """
    Create constraints function for Optuna sampler.

    Returns function that retrieves stored constraint values,
    with fallback to violated vector of correct shape.
    """
    enabled_constraints = [c for c in constraints if c.enabled]
    n_constraints = len(enabled_constraints)
    if n_constraints == 0:
        return None

    def constraints_func(trial: optuna.Trial) -> List[float]:
        values = trial.user_attrs.get("merlin.constraint_values")
        if values is None:
            return [1.0] * n_constraints
        if len(values) != n_constraints:
            return [1.0] * n_constraints
        return list(values)

    return constraints_func


def create_sampler(
    config: SamplerConfig,
    constraints_func=None,
) -> optuna.samplers.BaseSampler:
    """Create Optuna sampler based on configuration."""
    if config.sampler_type == "tpe":
        return TPESampler(
            n_startup_trials=config.n_startup_trials,
            multivariate=True,
            group=True,
            constant_liar=True,
            constraints_func=constraints_func,
        )
    if config.sampler_type == "nsga2":
        return NSGAIISampler(
            population_size=config.population_size,
            crossover_prob=config.crossover_prob,
            mutation_prob=config.mutation_prob,
            swapping_prob=config.swapping_prob,
            constraints_func=constraints_func,
        )
    if config.sampler_type == "nsga3":
        return NSGAIIISampler(
            population_size=config.population_size,
            crossover_prob=config.crossover_prob,
            mutation_prob=config.mutation_prob,
            swapping_prob=config.swapping_prob,
            constraints_func=constraints_func,
        )
    if config.sampler_type == "random":
        return RandomSampler()
    raise ValueError(f"Unknown sampler type: {config.sampler_type}")


def create_optimization_study(
    mo_config: MultiObjectiveConfig,
    sampler: optuna.samplers.BaseSampler,
    study_name: Optional[str] = None,
    storage=None,
    pruner: Optional[optuna.pruners.BasePruner] = None,
    load_if_exists: bool = False,
) -> optuna.Study:
    """
    Create Optuna study with proper single/multi-objective handling.

    - 1 objective: uses direction=
    - 2+ objectives: uses directions=
    """
    if mo_config.is_multi_objective():
        study = optuna.create_study(
            study_name=study_name,
            directions=mo_config.get_directions(),
            sampler=sampler,
            storage=storage,
            pruner=pruner,
            load_if_exists=load_if_exists,
        )
        study.set_metric_names(mo_config.get_metric_names())
    else:
        study = optuna.create_study(
            study_name=study_name,
            direction=mo_config.get_single_direction(),
            sampler=sampler,
            storage=storage,
            pruner=pruner,
            load_if_exists=load_if_exists,
        )
    return study


def _dominates(
    candidate: List[float],
    other: List[float],
    directions: List[str],
) -> bool:
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


def _compute_pareto_front(
    results: List[OptimizationResult],
    mo_config: MultiObjectiveConfig,
) -> set:
    pareto_numbers = set()
    if not results:
        return pareto_numbers
    directions = mo_config.get_directions()
    for idx, candidate in enumerate(results):
        candidate_values = candidate.objective_values
        if not candidate_values:
            continue
        dominated = False
        for jdx, other in enumerate(results):
            if idx == jdx:
                continue
            other_values = other.objective_values
            if not other_values:
                continue
            if _dominates(other_values, candidate_values, directions):
                dominated = True
                break
        if not dominated:
            pareto_numbers.add(candidate.optuna_trial_number)
    return pareto_numbers


def _calculate_total_violation(
    constraint_values: Optional[List[float]],
    constraints_satisfied: Optional[bool],
) -> float:
    """
    Calculate total constraint violation magnitude.

    Optuna treats constraint values > 0 as violated and <= 0 as satisfied.
    Lower totals are closer to feasibility.
    """
    if not constraint_values:
        if constraints_satisfied is False:
            return float("inf")
        return 0.0
    return sum(max(0.0, float(v)) for v in constraint_values)


def sort_optimization_results(
    results: List[OptimizationResult],
    study: Optional[optuna.Study],
    mo_config: MultiObjectiveConfig,
    constraints_enabled: bool,
) -> List[OptimizationResult]:
    """Sort results based on optimization mode and constraints."""
    if not results:
        return results

    if not mo_config.is_multi_objective():
        direction = mo_config.get_single_direction()
        reverse = direction == "maximize"
        return sorted(
            results,
            key=lambda r: r.objective_values[0] if r.objective_values else 0.0,
            reverse=reverse,
        )

    primary_obj = mo_config.primary_objective or mo_config.objectives[0]
    primary_idx = mo_config.objectives.index(primary_obj)
    primary_direction = OBJECTIVE_DIRECTIONS[primary_obj]

    feasible_results = results
    pareto_numbers: set = set()

    if constraints_enabled:
        feasible_results = [r for r in results if r.constraints_satisfied]
        pareto_numbers = _compute_pareto_front(feasible_results, mo_config)
    else:
        pareto_numbers = _compute_pareto_front(results, mo_config)

    for result in results:
        result.is_pareto_optimal = bool(result.optuna_trial_number in pareto_numbers)

    def group_rank(item: OptimizationResult) -> int:
        if constraints_enabled:
            if not item.constraints_satisfied:
                return 2
            return 0 if item.is_pareto_optimal else 1
        return 0 if item.is_pareto_optimal else 1

    def primary_sort_value(item: OptimizationResult) -> float:
        value = 0.0
        if item.objective_values and len(item.objective_values) > primary_idx:
            value = float(item.objective_values[primary_idx])
        if primary_direction == "maximize":
            return -value
        return value

    def tie_breaker(item: OptimizationResult) -> int:
        return int(item.optuna_trial_number or 0)

    return sorted(
        results,
        key=lambda item: (
            group_rank(item),
            _calculate_total_violation(item.constraint_values, item.constraints_satisfied),
            primary_sort_value(item),
            tie_breaker(item),
        ),
    )


def _build_constraint_specs(constraints_payload: Any) -> List[ConstraintSpec]:
    specs: List[ConstraintSpec] = []
    if not isinstance(constraints_payload, list):
        return specs
    for item in constraints_payload:
        if not isinstance(item, dict):
            continue
        metric = item.get("metric")
        if metric not in CONSTRAINT_OPERATORS:
            continue
        try:
            threshold = float(item.get("threshold"))
        except (TypeError, ValueError):
            threshold = 0.0
        enabled = bool(item.get("enabled", False))
        specs.append(ConstraintSpec(metric=metric, threshold=threshold, enabled=enabled))
    return specs


def _build_sampler_config(config: Any) -> SamplerConfig:
    sampler_type = getattr(config, "sampler_type", None)
    if sampler_type is None:
        sampler_type = getattr(config, "optuna_sampler", None)
    sampler_type = str(sampler_type or "tpe").strip().lower()

    population_size = getattr(config, "population_size", None)
    crossover_prob = getattr(config, "crossover_prob", None)
    mutation_prob = getattr(config, "mutation_prob", None)
    swapping_prob = getattr(config, "swapping_prob", None)
    n_startup_trials = getattr(config, "n_startup_trials", None)
    if n_startup_trials is None:
        n_startup_trials = getattr(config, "optuna_warmup_trials", None)

    return SamplerConfig(
        sampler_type=sampler_type,
        population_size=int(population_size) if population_size is not None else 50,
        crossover_prob=float(crossover_prob) if crossover_prob is not None else 0.9,
        mutation_prob=float(mutation_prob) if mutation_prob is not None else None,
        swapping_prob=float(swapping_prob) if swapping_prob is not None else 0.5,
        n_startup_trials=int(n_startup_trials) if n_startup_trials is not None else 20,
    )


def _run_single_combination(
    args: Tuple[Dict[str, Any], pd.DataFrame, int, Any]
) -> OptimizationResult:
    """
    Worker function to run a single parameter combination using strategy.run().

    Args:
        args: Tuple of (params_dict, df, trade_start_idx, strategy_class)

    Returns:
        OptimizationResult with metrics for this combination
    """

    params_dict, df, trade_start_idx, strategy_class = args

    def _base_result(params: Dict[str, Any]) -> OptimizationResult:
        return OptimizationResult(
            params=params.copy(),
            net_profit_pct=0.0,
            max_drawdown_pct=0.0,
            total_trades=0,
            win_rate=0.0,
            avg_win=0.0,
            avg_loss=0.0,
            gross_profit=0.0,
            gross_loss=0.0,
            max_consecutive_losses=0,
            sharpe_ratio=None,
            sortino_ratio=None,
            profit_factor=None,
            romad=None,
            ulcer_index=None,
            sqn=None,
            consistency_score=None,
        )

    try:
        result = strategy_class.run(df, params_dict, trade_start_idx)

        basic_metrics = metrics.calculate_basic(result)
        advanced_metrics = metrics.calculate_advanced(result)

        return OptimizationResult(
            params=params_dict.copy(),
            net_profit_pct=basic_metrics.net_profit_pct,
            max_drawdown_pct=basic_metrics.max_drawdown_pct,
            total_trades=basic_metrics.total_trades,
            win_rate=basic_metrics.win_rate,
            avg_win=basic_metrics.avg_win,
            avg_loss=basic_metrics.avg_loss,
            gross_profit=basic_metrics.gross_profit,
            gross_loss=basic_metrics.gross_loss,
            max_consecutive_losses=basic_metrics.max_consecutive_losses,
            romad=advanced_metrics.romad,
            sharpe_ratio=advanced_metrics.sharpe_ratio,
            sortino_ratio=advanced_metrics.sortino_ratio,
            profit_factor=advanced_metrics.profit_factor,
            ulcer_index=advanced_metrics.ulcer_index,
            sqn=advanced_metrics.sqn,
            consistency_score=advanced_metrics.consistency_score,
        )
    except Exception:
        return _base_result(params_dict)


# ---------------------------------------------------------------------------
# Multi-process helpers (module-level for pickling)
# ---------------------------------------------------------------------------


def _trial_set_result_attrs(
    trial: optuna.Trial,
    result: OptimizationResult,
    objective_values: List[float],
    all_metrics: Dict[str, Any],
    constraint_values: List[float],
    constraints_satisfied: bool,
) -> None:
    """
    Persist key metrics into trial.user_attrs for cross-process aggregation.
    """
    trial.set_user_attr("merlin.params", dict(result.params))
    trial.set_user_attr("merlin.objective_values", list(objective_values))
    trial.set_user_attr("merlin.constraint_values", list(constraint_values))
    trial.set_user_attr("merlin.constraints_satisfied", bool(constraints_satisfied))
    trial.set_user_attr("merlin.all_metrics", dict(all_metrics))


def _result_from_trial(trial: optuna.trial.FrozenTrial) -> OptimizationResult:
    """
    Rebuild OptimizationResult from persisted user_attrs.
    """
    attrs = trial.user_attrs
    all_metrics = attrs.get("merlin.all_metrics") or {}
    objective_values = list(attrs.get("merlin.objective_values") or [])
    constraint_values = list(attrs.get("merlin.constraint_values") or [])
    constraints_satisfied = attrs.get("merlin.constraints_satisfied")
    if constraints_satisfied is not None:
        constraints_satisfied = bool(constraints_satisfied)
    result = OptimizationResult(
        params=dict(attrs.get("merlin.params") or trial.params),
        net_profit_pct=float(all_metrics.get("net_profit_pct", 0.0)),
        max_drawdown_pct=float(all_metrics.get("max_drawdown_pct", 0.0)),
        total_trades=int(all_metrics.get("total_trades", 0)),
        win_rate=float(all_metrics.get("win_rate", 0.0) or 0.0),
        avg_win=float(all_metrics.get("avg_win", 0.0) or 0.0),
        avg_loss=float(all_metrics.get("avg_loss", 0.0) or 0.0),
        gross_profit=float(all_metrics.get("gross_profit", 0.0) or 0.0),
        gross_loss=float(all_metrics.get("gross_loss", 0.0) or 0.0),
        max_consecutive_losses=int(all_metrics.get("max_consecutive_losses", 0) or 0),
        romad=all_metrics.get("romad"),
        sharpe_ratio=all_metrics.get("sharpe_ratio"),
        sortino_ratio=all_metrics.get("sortino_ratio"),
        profit_factor=all_metrics.get("profit_factor"),
        ulcer_index=all_metrics.get("ulcer_index"),
        sqn=all_metrics.get("sqn"),
        consistency_score=all_metrics.get("consistency_score"),
        score=0.0,
        optuna_trial_number=trial.number,
        objective_values=objective_values,
        constraint_values=constraint_values,
        constraints_satisfied=constraints_satisfied,
    )
    if trial.values is not None:
        setattr(result, "optuna_values", list(trial.values))
    elif trial.value is not None:
        setattr(result, "optuna_values", [float(trial.value)])
    return result


def _serialize_csv_source_for_worker(csv_source: Any) -> Tuple[str, Union[str, bytes]]:
    """Convert CSV source into a picklable worker payload."""
    if isinstance(csv_source, (str, Path)):
        return "path", str(csv_source)

    if hasattr(csv_source, "name") and csv_source.name:
        possible_path = Path(csv_source.name)
        if possible_path.exists() and possible_path.is_file():
            return "path", str(possible_path)

    if hasattr(csv_source, "read"):
        reset_after_read = False
        if hasattr(csv_source, "seek"):
            try:
                csv_source.seek(0)
                reset_after_read = True
            except Exception:
                pass

        content = csv_source.read()
        if reset_after_read and hasattr(csv_source, "seek"):
            try:
                csv_source.seek(0)
            except Exception:
                pass

        if isinstance(content, (bytes, bytearray)):
            return "bytes", bytes(content)
        return "text", str(content)

    return "path", str(csv_source)


def _restore_csv_source_from_worker(
    mode: str,
    payload: Union[str, bytes],
) -> Union[str, io.StringIO, io.BytesIO]:
    """Rebuild a CSV source object inside a worker process."""
    if mode == "path":
        return str(payload)
    if mode == "bytes":
        raw = payload if isinstance(payload, (bytes, bytearray)) else str(payload).encode("utf-8")
        return io.BytesIO(bytes(raw))
    text = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else str(payload)
    return io.StringIO(text)


def _terminate_processes(processes: List[mp.Process], join_timeout: float = 5.0) -> None:
    """Best-effort worker shutdown used by the multiprocess optimizer."""
    for proc in processes:
        if proc.is_alive():
            proc.terminate()
    for proc in processes:
        proc.join(timeout=join_timeout)



def _drain_worker_errors(error_queue: Any) -> List[Dict[str, Any]]:
    """Collect worker error payloads without blocking."""
    errors: List[Dict[str, Any]] = []
    while True:
        try:
            errors.append(error_queue.get_nowait())
        except queue.Empty:
            break
    return errors


def _resolve_csv_path_for_study(csv_source: Any) -> str:
    if isinstance(csv_source, (str, Path)):
        return str(csv_source)
    if hasattr(csv_source, "name") and csv_source.name:
        return str(csv_source.name)
    return ""


def _worker_process_entry(
    study_name: str,
    shared_logs: Any,
    csv_source_mode: str,
    csv_source_payload: Union[str, bytes],
    base_config_dict: Dict[str, Any],
    optuna_config_dict: Dict[str, Any],
    n_trials: Optional[int],
    timeout: Optional[int],
    worker_id: int,
    error_queue: Any,
) -> None:
    """
    Entry point for multi-process Optuna workers.
    """
    from optuna.storages import JournalStorage
    from optuna.study import MaxTrialsCallback

    worker_logger = logging.getLogger(__name__)
    worker_logger.info("Worker %s starting (pid=%s)", worker_id, mp.current_process().pid)

    try:
        base_config = OptimizationConfig(**base_config_dict)
        base_config.csv_file = _restore_csv_source_from_worker(csv_source_mode, csv_source_payload)
        optuna_config = OptunaConfig(**optuna_config_dict)

        optimizer = OptunaOptimizer(base_config, optuna_config)
        optimizer._prepare_data_and_strategy()
        optimizer.pruner = optimizer._create_pruner()
        worker_sampler = optimizer._create_sampler()

        storage = JournalStorage(InMemoryJournalBackend(shared_logs))
        study = optuna.load_study(
            study_name=study_name,
            storage=storage,
            sampler=worker_sampler,
            pruner=optimizer.pruner,
        )
        search_space = optimizer._build_search_space()

        def worker_objective(trial: optuna.Trial) -> float:
            return optimizer._objective_for_worker(trial, search_space)

        callbacks = []
        if n_trials is not None:
            callbacks.append(MaxTrialsCallback(n_trials, states=None))

        worker_logger.info(
            "Worker %s running optimise (n_trials=%s, timeout=%s)", worker_id, n_trials, timeout
        )
        study.optimize(
            worker_objective,
            n_trials=None,
            timeout=timeout,
            callbacks=callbacks or None,
            show_progress_bar=False,
            n_jobs=1,
        )
        worker_logger.info("Worker %s finished", worker_id)
    except Exception as exc:  # pragma: no cover - defensive
        try:
            error_queue.put_nowait(
                {
                    "worker_id": worker_id,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
        except Exception:
            worker_logger.debug("Failed to publish worker error for worker %s", worker_id, exc_info=True)
        worker_logger.error("Worker %s failed: %s", worker_id, exc, exc_info=True)
        raise


def _evaluator_worker_entry(
    task_queue: mp.Queue,
    result_queue: mp.Queue,
    csv_source_mode: str,
    csv_source_payload: Union[str, bytes],
    base_config_dict: Dict[str, Any],
    optuna_config_dict: Dict[str, Any],
    worker_id: int,
    error_queue: Any,
) -> None:
    """Worker process used by centralized NSGA ask/tell orchestration."""
    worker_logger = logging.getLogger(__name__)
    worker_logger.info("Evaluator worker %s starting (pid=%s)", worker_id, mp.current_process().pid)

    try:
        base_config = OptimizationConfig(**base_config_dict)
        base_config.csv_file = _restore_csv_source_from_worker(csv_source_mode, csv_source_payload)
        optuna_config = OptunaConfig(**optuna_config_dict)

        optimizer = OptunaOptimizer(base_config, optuna_config)
        optimizer._prepare_data_and_strategy()

        while True:
            task = task_queue.get()
            if task is None:
                worker_logger.info("Evaluator worker %s received stop signal", worker_id)
                break

            trial_number = int(task["trial_number"])
            params = dict(task["params"])
            payload = optimizer._evaluate_trial_payload(params)
            result_queue.put(
                {
                    "trial_number": trial_number,
                    "params_key": str(task["params_key"]),
                    "payload": payload,
                }
            )

        worker_logger.info("Evaluator worker %s finished", worker_id)
    except Exception as exc:  # pragma: no cover - defensive
        try:
            error_queue.put_nowait(
                {
                    "worker_id": worker_id,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
        except Exception:
            worker_logger.debug(
                "Failed to publish evaluator error for worker %s", worker_id, exc_info=True
            )
        worker_logger.error("Evaluator worker %s failed: %s", worker_id, exc, exc_info=True)
        raise


def _normalize_minmax(
    results: List[OptimizationResult],
    metrics_to_normalize: List[str],
    invert_metrics: Dict[str, Any],
    metric_bounds: Dict[str, Dict[str, float]],
) -> Dict[str, Dict[int, float]]:
    """
    Normalize metrics using min-max scaling with fixed bounds.

    Each value is scaled to 0-100 based on predefined min/max bounds.
    Values outside bounds are clamped.
    """
    normalized_values: Dict[str, Dict[int, float]] = {}

    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "y", "on"}
        return False

    for metric_name in metrics_to_normalize:
        attr_name = SCORE_METRIC_ATTRS[metric_name]
        bounds = metric_bounds.get(metric_name, {"min": 0.0, "max": 100.0})
        min_bound = float(bounds.get("min", 0.0))
        max_bound = float(bounds.get("max", 100.0))
        invert = _as_bool(invert_metrics.get(metric_name, False))

        normalized_values[metric_name] = {}

        range_val = max_bound - min_bound
        if range_val <= 0:
            range_val = 1.0

        for item in results:
            value = getattr(item, attr_name)
            if value is None:
                normalized = 50.0
            else:
                clamped = max(min_bound, min(max_bound, float(value)))
                normalized = ((clamped - min_bound) / range_val) * 100.0
                if invert:
                    normalized = 100.0 - normalized
            normalized_values[metric_name][id(item)] = normalized

    return normalized_values


def _normalize_percentile(
    results: List[OptimizationResult],
    metrics_to_normalize: List[str],
    invert_metrics: Dict[str, Any],
) -> Dict[str, Dict[int, float]]:
    """
    Normalize metrics using percentile ranking across all results.

    WARNING: Requires all results to be available; not suitable for multi-process mode.
    """
    normalized_values: Dict[str, Dict[int, float]] = {}

    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "y", "on"}
        return False

    for metric_name in metrics_to_normalize:
        attr_name = SCORE_METRIC_ATTRS[metric_name]
        metric_values = [
            getattr(item, attr_name)
            for item in results
            if getattr(item, attr_name) is not None
        ]
        if not metric_values:
            normalized_values[metric_name] = {id(item): 50.0 for item in results}
            continue

        sorted_vals = sorted(float(value) for value in metric_values)
        total = len(sorted_vals)
        normalized_values[metric_name] = {}
        invert = _as_bool(invert_metrics.get(metric_name, False))

        for item in results:
            value = getattr(item, attr_name)
            if value is None:
                rank = 50.0
            else:
                idx = bisect.bisect_left(sorted_vals, float(value))
                rank = (idx / total) * 100.0
                if invert:
                    rank = 100.0 - rank
            normalized_values[metric_name][id(item)] = rank

    return normalized_values


def calculate_score(
    results: List[OptimizationResult],
    config: Optional[Dict[str, Any]],
) -> List[OptimizationResult]:
    """Calculate composite score for optimization results.

    Supports "minmax" (deterministic, multi-process safe) and "percentile".
    """

    if not results:
        return results

    if config is None:
        config = {}

    normalized_config = DEFAULT_SCORE_CONFIG.copy()
    normalized_config.update({k: v for k, v in (config or {}).items() if v is not None})

    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y", "on"}:
                return True
            if lowered in {"false", "0", "no", "n", "off"}:
                return False
        return False

    weights = normalized_config.get("weights") or {}
    enabled_metrics = normalized_config.get("enabled_metrics") or {}
    invert_metrics = normalized_config.get("invert_metrics") or {}
    filter_enabled = _as_bool(normalized_config.get("filter_enabled", False))
    try:
        min_score_threshold = float(normalized_config.get("min_score_threshold", 0.0))
    except (TypeError, ValueError):
        min_score_threshold = 0.0
    min_score_threshold = max(0.0, min(100.0, min_score_threshold))

    normalization_method_raw = normalized_config.get("normalization_method", "minmax")
    normalization_method = (
        str(normalization_method_raw).strip().lower() if normalization_method_raw is not None else "minmax"
    )
    if normalization_method not in {"minmax", "percentile"}:
        normalization_method = "minmax"

    metric_bounds: Dict[str, Dict[str, float]] = {
        key: {"min": float(value.get("min", 0.0)), "max": float(value.get("max", 100.0))}
        for key, value in DEFAULT_METRIC_BOUNDS.items()
    }
    metric_bounds_raw = normalized_config.get("metric_bounds")
    if isinstance(metric_bounds_raw, dict):
        for key, bounds in metric_bounds_raw.items():
            if not isinstance(bounds, dict):
                continue
            current = metric_bounds.get(key, {"min": 0.0, "max": 100.0})
            try:
                min_val = float(bounds.get("min", current.get("min", 0.0)))
                max_val = float(bounds.get("max", current.get("max", 100.0)))
            except (TypeError, ValueError):
                min_val = current.get("min", 0.0)
                max_val = current.get("max", 100.0)
            metric_bounds[key] = {"min": min_val, "max": max_val}
    metric_bounds = _migrate_legacy_consistency_bounds(metric_bounds)

    metrics_to_normalize: List[str] = []
    for metric in SCORE_METRIC_ATTRS:
        if _as_bool(enabled_metrics.get(metric, False)):
            metrics_to_normalize.append(metric)

    if normalization_method == "minmax":
        normalized_values = _normalize_minmax(
            results, metrics_to_normalize, invert_metrics, metric_bounds
        )
    else:
        normalized_values = _normalize_percentile(
            results, metrics_to_normalize, invert_metrics
        )

    for item in results:
        item.score = 0.0
        score_total = 0.0
        weight_total = 0.0
        for metric_name in metrics_to_normalize:
            weight_raw = weights.get(metric_name, 0.0)
            try:
                weight = float(weight_raw)
            except (TypeError, ValueError):
                weight = 0.0
            weight = max(0.0, min(1.0, weight))
            if weight <= 0:
                continue
            score_total += normalized_values[metric_name][id(item)] * weight
            weight_total += weight
        if weight_total > 0:
            item.score = score_total / weight_total

    if filter_enabled:
        results = [item for item in results if item.score >= min_score_threshold]

    return results


@dataclass
class OptunaConfig:
    """Configuration parameters that control Optuna optimisation."""

    objectives: List[str] = field(default_factory=lambda: ["net_profit_pct"])
    primary_objective: Optional[str] = None
    constraints: List[ConstraintSpec] = field(default_factory=list)
    sanitize_enabled: bool = True
    sanitize_trades_threshold: int = 0
    sampler_config: SamplerConfig = field(default_factory=SamplerConfig)
    budget_mode: str = "trials"  # "trials", "time", or "convergence"
    n_trials: int = 500
    time_limit: int = 3600  # seconds
    convergence_patience: int = 50
    enable_pruning: bool = True
    pruner: str = "median"  # "median", "percentile", "patient", "none"
    warmup_trials: int = 20
    coverage_mode: bool = False
    save_study: bool = False
    study_name: Optional[str] = None


class OptunaOptimizer:
    """Optuna-based optimizer for Bayesian hyperparameter search using multiprocess evaluation."""

    def __init__(self, base_config, optuna_config: OptunaConfig) -> None:
        self.base_config = base_config
        self.optuna_config = optuna_config
        if bool(getattr(self.optuna_config, "save_study", False)):
            logger.warning("Raw Optuna study persistence is disabled; ignoring save_study=True")
            self.optuna_config.save_study = False
        objectives = list(optuna_config.objectives or [])
        if not objectives:
            objectives = ["net_profit_pct"]
        primary_objective = optuna_config.primary_objective
        if len(objectives) > 1 and primary_objective not in objectives:
            primary_objective = objectives[0]
        self.mo_config = MultiObjectiveConfig(
            objectives=objectives,
            primary_objective=primary_objective,
        )
        raw_constraints = list(optuna_config.constraints or [])
        if raw_constraints and isinstance(raw_constraints[0], dict):
            self.constraints = _build_constraint_specs(raw_constraints)
        else:
            self.constraints = raw_constraints

        raw_sampler_config = optuna_config.sampler_config
        if isinstance(raw_sampler_config, dict):
            self.sampler_config = SamplerConfig(**raw_sampler_config)
        else:
            self.sampler_config = raw_sampler_config or SamplerConfig()
        if bool(getattr(self.optuna_config, "coverage_mode", False)) and str(
            getattr(self.sampler_config, "sampler_type", "")
        ).lower() == "tpe":
            # Coverage trials replace random startup when coverage mode is enabled.
            self.sampler_config.n_startup_trials = 0
        self.df: Optional[pd.DataFrame] = None
        self.trade_start_idx: int = 0
        self.strategy_class: Optional[Any] = None
        self.trial_results: List[OptimizationResult] = []
        self.best_value: float = float("-inf")
        self.trials_without_improvement: int = 0
        self.start_time: Optional[float] = None
        self.pruned_trials: int = 0
        self.study: Optional[optuna.Study] = None
        self.pruner: Optional[optuna.pruners.BasePruner] = None
        self.param_type_map: Dict[str, str] = {}
        self._param_defaults: Dict[str, Any] = {}
        self._param_dependencies: Dict[str, Tuple[str, ...]] = {}
        self._optimizable_param_names: Set[str] = set()
        self._multiprocess_mode: bool = False
        self._coverage_report: Optional[Dict[str, Any]] = None
        self._bool_group_choice_map: Dict[str, Dict[str, Dict[str, bool]]] = {}
        self._duplicate_skipped_count: int = 0

    # ------------------------------------------------------------------
    # Search space handling
    # ------------------------------------------------------------------
    def _build_search_space(self) -> Dict[str, Dict[str, Any]]:
        """Construct the Optuna search space from strategy config metadata."""

        from strategies import get_strategy_config

        try:
            strategy_config = get_strategy_config(self.base_config.strategy_id)
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Failed to load strategy config for {self.base_config.strategy_id}: {exc}")

        parameters = strategy_config.get("parameters", {}) if isinstance(strategy_config, dict) else {}
        if not isinstance(parameters, dict):
            raise ValueError(f"Invalid parameters section in strategy config for {self.base_config.strategy_id}")

        space: Dict[str, Dict[str, Any]] = {}
        self.param_type_map = {}
        self._param_defaults = {}
        self._param_dependencies = {}
        self._optimizable_param_names = set()
        self._bool_group_choice_map = {}
        resolved_param_types = {
            param_name: _resolve_strategy_param_type(self.base_config, param_name, param_spec)
            for param_name, param_spec in parameters.items()
            if isinstance(param_spec, dict)
        }

        for param_name, param_spec in parameters.items():
            if not isinstance(param_spec, dict):
                continue

            self._param_defaults[param_name] = param_spec.get("default")
            dependencies = _normalize_param_dependencies(param_spec.get("depends_on"))
            for dependency_name in dependencies:
                if dependency_name not in parameters:
                    raise ValueError(
                        "Parameter dependency references unknown parent "
                        f"'{dependency_name}' for '{param_name}' in strategy "
                        f"'{self.base_config.strategy_id}'"
                    )
                if resolved_param_types.get(dependency_name) != "bool":
                    raise ValueError(
                        "Parameter dependency requires a bool parent "
                        f"for '{param_name}' in strategy '{self.base_config.strategy_id}': "
                        f"'{dependency_name}' is typed as "
                        f"'{resolved_param_types.get(dependency_name, 'unknown')}'"
                    )
            if dependencies:
                self._param_dependencies[param_name] = dependencies

            param_type = resolved_param_types[param_name]
            self.param_type_map[param_name] = param_type

            if not self.base_config.enabled_params.get(param_name, False):
                continue
            self._optimizable_param_names.add(param_name)

            optimize_spec = param_spec.get("optimize", {}) if isinstance(param_spec.get("optimize", {}), dict) else {}
            override_range = self.base_config.param_ranges.get(param_name)

            if param_type == "int":
                min_val = optimize_spec.get("min", param_spec.get("min", 0))
                max_val = optimize_spec.get("max", param_spec.get("max", 0))
                step = optimize_spec.get("step", param_spec.get("step", 1))
                if override_range:
                    min_val, max_val, step = override_range
                space[param_name] = {
                    "type": "int",
                    "low": int(round(float(min_val))),
                    "high": int(round(float(max_val))),
                    "step": max(1, int(round(float(step)))) if step is not None else 1,
                }
            elif param_type == "float":
                min_val = optimize_spec.get("min", param_spec.get("min", 0.0))
                max_val = optimize_spec.get("max", param_spec.get("max", 0.0))
                step = optimize_spec.get("step", param_spec.get("step"))
                if override_range:
                    min_val, max_val, step = override_range
                spec: Dict[str, Any] = {
                    "type": "float",
                    "low": float(min_val),
                    "high": float(max_val),
                }
                if step not in (None, 0, 0.0):
                    spec["step"] = float(step)
                space[param_name] = spec
            elif param_type in {"select", "options"}:
                options = param_spec.get("options", [])

                range_override = self.base_config.param_ranges.get(param_name)
                if isinstance(range_override, dict):
                    override_options = range_override.get("values") or range_override.get("options")
                    if isinstance(override_options, (list, tuple)):
                        options = override_options

                fixed_override = self.base_config.fixed_params.get(f"{param_name}_options")
                if isinstance(fixed_override, (list, tuple)) and fixed_override:
                    options = fixed_override

                cleaned_options = [opt for opt in options if str(opt).strip()]
                if not cleaned_options:
                    continue

                space[param_name] = {
                    "type": "categorical",
                    "choices": list(cleaned_options),
                }

            elif param_type in {"bool", "boolean"}:
                bool_choices = [True, False]

                range_override = self.base_config.param_ranges.get(param_name)
                if isinstance(range_override, dict):
                    override_options = range_override.get("values") or range_override.get("options")
                    normalized_override = _normalize_bool_choices(override_options)
                    if normalized_override:
                        bool_choices = normalized_override

                fixed_override = self.base_config.fixed_params.get(f"{param_name}_options")
                normalized_fixed = _normalize_bool_choices(fixed_override)
                if normalized_fixed:
                    bool_choices = normalized_fixed

                space[param_name] = {
                    "type": "categorical",
                    "choices": list(bool_choices),
                }

            if dependencies and param_name in space:
                space[param_name]["depends_on"] = list(dependencies)

        return self._apply_bool_group_rules(space, strategy_config)

    def _build_bool_group_surrogate_name(self, member_names: List[str]) -> str:
        sanitized = [name for name in member_names if name]
        return "__bool_group__" + "__".join(sanitized)

    def _apply_bool_group_rules(
        self,
        search_space: Dict[str, Dict[str, Any]],
        strategy_config: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        rules = _extract_bool_group_rules(strategy_config)
        if not rules:
            return search_space

        updated_space: Dict[str, Dict[str, Any]] = dict(search_space)
        strategy_params = (
            strategy_config.get("parameters", {})
            if isinstance(strategy_config.get("parameters", {}), dict)
            else {}
        )

        for rule_index, rule in enumerate(rules):
            member_names = list(rule.get("params") or [])
            mode = str(rule.get("mode", "")).strip().lower()
            if len(member_names) < 2:
                continue
            if mode != "at_least_one_true":
                raise ValueError(
                    f"Unsupported bool group mode '{mode}' in strategy '{self.base_config.strategy_id}'"
                )

            optimizable_members: List[str] = []
            member_choices: Dict[str, List[bool]] = {}
            for name in member_names:
                spec = updated_space.get(name)
                if isinstance(spec, dict) and str(spec.get("type", "")).lower() == "categorical":
                    normalized_choices = _normalize_bool_choices(spec.get("choices"))
                    if not normalized_choices:
                        raise ValueError(
                            f"Invalid bool choices for '{name}' in strategy '{self.base_config.strategy_id}'"
                        )
                    member_choices[name] = normalized_choices
                    optimizable_members.append(name)
                    continue

                fixed_value = self.base_config.fixed_params.get(name)
                if fixed_value is None and isinstance(strategy_params, dict):
                    param_spec = strategy_params.get(name, {})
                    if isinstance(param_spec, dict):
                        fixed_value = param_spec.get("default")

                parsed_fixed = _coerce_bool_value(fixed_value)
                if parsed_fixed is None:
                    raise ValueError(
                        f"Unable to resolve bool value for '{name}' in strategy '{self.base_config.strategy_id}'"
                    )
                member_choices[name] = [parsed_fixed]

            if not optimizable_members:
                continue

            combinations: List[Dict[str, bool]] = []
            choice_lists = [member_choices[name] for name in member_names]
            for combo_values in itertools.product(*choice_lists):
                combo = dict(zip(member_names, combo_values))
                if mode == "at_least_one_true" and not any(combo.values()):
                    continue
                combinations.append(combo)

            if not combinations:
                raise ValueError(
                    "Boolean optimization rules produced no valid combinations for "
                    f"strategy '{self.base_config.strategy_id}'."
                )

            projected_combinations: List[Dict[str, bool]] = []
            for combo in combinations:
                projected = {name: combo[name] for name in optimizable_members}
                if projected not in projected_combinations:
                    projected_combinations.append(projected)

            if not projected_combinations:
                raise ValueError(
                    "Boolean optimization rules produced no projected combinations for "
                    f"strategy '{self.base_config.strategy_id}'."
                )

            if len(optimizable_members) == 1:
                target_name = optimizable_members[0]
                allowed_values: List[bool] = []
                for projected in projected_combinations:
                    value = projected[target_name]
                    if value not in allowed_values:
                        allowed_values.append(value)
                updated_space[target_name] = {
                    "type": "categorical",
                    "choices": allowed_values,
                }
                continue

            surrogate_name = self._build_bool_group_surrogate_name(optimizable_members)
            while surrogate_name in updated_space:
                surrogate_name = f"{surrogate_name}_{rule_index}"

            choice_map: Dict[str, Dict[str, bool]] = {}
            surrogate_choices: List[str] = []
            for idx, projected in enumerate(projected_combinations):
                token = f"g{rule_index}_{idx}"
                choice_map[token] = projected
                surrogate_choices.append(token)

            for name in optimizable_members:
                updated_space.pop(name, None)

            updated_space[surrogate_name] = {
                "type": "categorical",
                "choices": surrogate_choices,
            }
            self.param_type_map[surrogate_name] = "categorical"
            self._bool_group_choice_map[surrogate_name] = choice_map

        return updated_space

    # ------------------------------------------------------------------
    # Sampler / pruner factories
    # ------------------------------------------------------------------
    def _create_sampler(self) -> optuna.samplers.BaseSampler:
        constraints_func = create_constraints_func(self.constraints)
        return create_sampler(self.sampler_config, constraints_func=constraints_func)

    def _create_pruner(self) -> Optional[optuna.pruners.BasePruner]:
        if self.mo_config.is_multi_objective():
            return None
        if not self.optuna_config.enable_pruning or self.optuna_config.pruner == "none":
            return None
        if self.optuna_config.pruner == "percentile":
            return PercentilePruner(
                percentile=25.0,
                n_startup_trials=max(0, int(self.optuna_config.warmup_trials)),
            )
        if self.optuna_config.pruner == "patient":
            return PatientPruner(
                wrapped_pruner=MedianPruner(
                    n_startup_trials=max(0, int(self.optuna_config.warmup_trials))
                ),
                patience=3,
            )
        return MedianPruner(
            n_startup_trials=max(0, int(self.optuna_config.warmup_trials))
        )

    def _enqueue_coverage_trials(
        self,
        search_space: Dict[str, Dict[str, Any]],
        context_label: str = "",
    ) -> int:
        self._coverage_report = _analyze_coverage_requirements(
            search_space=search_space,
        )

        if not bool(getattr(self.optuna_config, "coverage_mode", False)):
            return 0
        if self.study is None:
            return 0

        n_initial = max(0, int(getattr(self.optuna_config, "warmup_trials", 0) or 0))
        if n_initial <= 0:
            return 0

        coverage_trials = [
            self._prune_inactive_trial_values(params)
            for params in _generate_coverage_trials(search_space, n_initial)
        ]
        for params in coverage_trials:
            self.study.enqueue_trial(params)

        suffix = f" {context_label}".strip()
        logger.info("Enqueued %d coverage trials%s", len(coverage_trials), f" {suffix}" if suffix else "")
        return len(coverage_trials)

    # ------------------------------------------------------------------
    # Data preparation (shared by single and multi process)
    # ------------------------------------------------------------------
    def _prepare_data_and_strategy(self) -> None:
        """Load strategy class and data, apply optional date filtering."""

        from strategies import get_strategy
        from .backtest_engine import align_date_bounds, prepare_dataset_with_warmup

        try:
            strategy_class = get_strategy(self.base_config.strategy_id)
        except ValueError as exc:
            raise ValueError(f"Failed to load strategy '{self.base_config.strategy_id}': {exc}")

        df = load_data(self.base_config.csv_file)

        use_date_filter = bool(self.base_config.fixed_params.get("dateFilter", False))
        start_raw = self.base_config.fixed_params.get("start")
        end_raw = self.base_config.fixed_params.get("end")
        start_ts, end_ts = align_date_bounds(df.index, start_raw, end_raw)

        trade_start_idx = 0
        if use_date_filter and (start_ts is not None or end_ts is not None):
            try:
                df, trade_start_idx = prepare_dataset_with_warmup(
                    df, start_ts, end_ts, self.base_config.warmup_bars
                )
            except Exception as exc:
                raise ValueError(f"Failed to prepare dataset with warmup: {exc}")

        self.df = df
        self.trade_start_idx = trade_start_idx
        self.strategy_class = strategy_class

    # ------------------------------------------------------------------
    # Objective evaluation
    # ------------------------------------------------------------------
    def _evaluate_parameters(self, params_dict: Dict[str, Any]) -> OptimizationResult:
        if self.df is None or self.strategy_class is None:
            raise RuntimeError("Data and strategy must be prepared before evaluation.")

        args = (params_dict, self.df, self.trade_start_idx, self.strategy_class)
        return _run_single_combination(args)

    def _evaluate_trial_payload(self, params_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate one parameter set and return a serializable Optuna payload."""
        result = self._evaluate_parameters(params_dict)

        score_config = self.base_config.score_config or DEFAULT_SCORE_CONFIG
        score_needed = score_config.get("filter_enabled") or (
            "composite_score" in self.mo_config.objectives
        )
        if score_needed:
            minmax_config = score_config.copy()
            minmax_config["normalization_method"] = "minmax"
            scored_results = calculate_score([result], minmax_config)
            if scored_results:
                result = scored_results[0]

        all_metrics = self._collect_metrics(result)
        objective_values, sanitized, objective_return, should_fail = self._prepare_objective_values(
            all_metrics
        )
        payload: Dict[str, Any] = {
            "result": result,
            "all_metrics": all_metrics,
            "objective_values": objective_values,
            "sanitized_metrics": list(sanitized),
            "objective_return": objective_return,
            "should_fail": bool(should_fail),
            "constraint_values": [],
            "constraints_satisfied": True,
        }
        if should_fail:
            return payload

        constraint_values = evaluate_constraints(all_metrics, self.constraints)
        constraints_satisfied = True
        if constraint_values:
            constraints_satisfied = all(value <= 0.0 for value in constraint_values)

        result.objective_values = objective_values
        result.constraint_values = constraint_values
        result.constraints_satisfied = constraints_satisfied

        payload["constraint_values"] = constraint_values
        payload["constraints_satisfied"] = constraints_satisfied
        return payload

    def _cast_param_value(self, name: str, value: Any) -> Any:
        param_type = self.param_type_map.get(name, "").lower()
        try:
            if param_type == "int":
                return int(float(value))
            if param_type == "float":
                return float(value)
            if param_type == "bool":
                parsed_bool = _coerce_bool_value(value)
                return bool(value) if parsed_bool is None else parsed_bool
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return value
        return value

    def _suggest_trial_parameter(
        self,
        trial: optuna.Trial,
        key: str,
        spec: Dict[str, Any],
    ) -> Any:
        p_type = spec["type"]
        if p_type == "int":
            return trial.suggest_int(
                key,
                int(spec["low"]),
                int(spec["high"]),
                step=int(spec.get("step", 1)),
            )
        if p_type == "float":
            if spec.get("log"):
                return trial.suggest_float(
                    key,
                    float(spec["low"]),
                    float(spec["high"]),
                    log=True,
                )
            step = spec.get("step")
            if step:
                return trial.suggest_float(
                    key,
                    float(spec["low"]),
                    float(spec["high"]),
                    step=float(step),
                )
            return trial.suggest_float(
                key,
                float(spec["low"]),
                float(spec["high"]),
            )
        if p_type == "categorical":
            return trial.suggest_categorical(key, list(spec["choices"]))
        raise ValueError(f"Unsupported search space type '{p_type}' for parameter '{key}'")

    def _decode_bool_group_params(
        self,
        params_dict: Dict[str, Any],
        *,
        remove_surrogates: bool,
    ) -> Dict[str, Any]:
        resolved = dict(params_dict)
        for surrogate_name, choice_map in self._bool_group_choice_map.items():
            token = resolved.get(surrogate_name)
            if token is None:
                continue
            decoded = choice_map.get(str(token))
            if not decoded:
                raise ValueError(
                    f"Invalid bool-group token '{token}' for '{surrogate_name}' "
                    f"in strategy '{self.base_config.strategy_id}'"
                )
            if remove_surrogates:
                resolved.pop(surrogate_name, None)
            resolved.update(decoded)
        return resolved

    def _is_dependency_ready(self, dependency_name: str, params_dict: Dict[str, Any]) -> bool:
        if dependency_name in params_dict:
            return True
        if dependency_name in self._optimizable_param_names:
            return False
        fixed_value = (self.base_config.fixed_params or {}).get(dependency_name, _UNSET)
        if fixed_value is not _UNSET:
            return True
        return dependency_name in self._param_defaults

    def _resolve_parameter_value(self, name: str, params_dict: Dict[str, Any]) -> Any:
        if name in params_dict:
            return params_dict[name]
        fixed_value = (self.base_config.fixed_params or {}).get(name, _UNSET)
        if fixed_value is not _UNSET:
            return self._cast_param_value(name, fixed_value)
        default_value = self._param_defaults.get(name, _UNSET)
        if default_value is not _UNSET:
            return self._cast_param_value(name, default_value)
        return None

    def _dependencies_ready(self, name: str, params_dict: Dict[str, Any]) -> bool:
        dependencies = self._param_dependencies.get(name, ())
        return all(self._is_dependency_ready(dep_name, params_dict) for dep_name in dependencies)

    def _dependency_is_active(self, name: str, params_dict: Dict[str, Any]) -> bool:
        dependencies = self._param_dependencies.get(name, ())
        if not dependencies:
            return True
        for dep_name in dependencies:
            dep_value = self._resolve_parameter_value(dep_name, params_dict)
            parsed = _coerce_bool_value(dep_value)
            dep_enabled = bool(dep_value) if parsed is None else parsed
            if not dep_enabled:
                return False
        return True

    def _prune_inactive_trial_values(self, params_dict: Dict[str, Any]) -> Dict[str, Any]:
        if not self._param_dependencies:
            return dict(params_dict)
        pruned = dict(params_dict)
        context = self._decode_bool_group_params(pruned, remove_surrogates=False)
        for name in list(pruned.keys()):
            if name in self._param_dependencies and not self._dependency_is_active(name, context):
                pruned.pop(name, None)
        return pruned

    def _prepare_trial_parameters(self, trial: optuna.Trial, search_space: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        params_dict: Dict[str, Any] = {}
        pending_specs = list(search_space.items())

        while pending_specs:
            next_pending: List[Tuple[str, Dict[str, Any]]] = []
            progressed = False

            for key, spec in pending_specs:
                if not self._dependencies_ready(key, params_dict):
                    next_pending.append((key, spec))
                    continue
                if not self._dependency_is_active(key, params_dict):
                    progressed = True
                    continue

                params_dict[key] = self._suggest_trial_parameter(trial, key, spec)
                if key in self._bool_group_choice_map:
                    params_dict = self._decode_bool_group_params(
                        params_dict,
                        remove_surrogates=True,
                    )
                progressed = True

            if not progressed and next_pending:
                unresolved_names = ", ".join(key for key, _ in next_pending)
                raise ValueError(
                    "Unable to resolve parameter dependencies for "
                    f"strategy '{self.base_config.strategy_id}': {unresolved_names}"
                )

            pending_specs = next_pending

        params_dict = self._decode_bool_group_params(params_dict, remove_surrogates=True)

        for key, value in (self.base_config.fixed_params or {}).items():
            if value is None or key in params_dict:
                continue
            if not self._dependencies_ready(key, params_dict):
                continue
            if not self._dependency_is_active(key, params_dict):
                continue
            params_dict[key] = self._cast_param_value(key, value)

        params_dict.setdefault("riskPerTrade", float(self.base_config.risk_per_trade_pct))
        params_dict.setdefault("contractSize", float(self.base_config.contract_size))
        params_dict.setdefault("commissionRate", float(self.base_config.commission_rate))

        return params_dict

    def _collect_metrics(self, result: OptimizationResult) -> Dict[str, Any]:
        return {
            "net_profit_pct": result.net_profit_pct,
            "max_drawdown_pct": result.max_drawdown_pct,
            "total_trades": result.total_trades,
            "win_rate": result.win_rate,
            "max_consecutive_losses": result.max_consecutive_losses,
            "avg_win": result.avg_win,
            "avg_loss": result.avg_loss,
            "gross_profit": result.gross_profit,
            "gross_loss": result.gross_loss,
            "sharpe_ratio": result.sharpe_ratio,
            "sortino_ratio": result.sortino_ratio,
            "romad": result.romad,
            "profit_factor": result.profit_factor,
            "ulcer_index": result.ulcer_index,
            "sqn": result.sqn,
            "consistency_score": result.consistency_score,
            "composite_score": result.score,
        }

    def _extract_objective_values(self, all_metrics: Dict[str, Any]) -> List[Optional[float]]:
        objective_values: List[Optional[float]] = []
        for obj in self.mo_config.objectives:
            value = all_metrics.get(obj)
            if value is None or _is_nan(value):
                objective_values.append(None)
                continue
            try:
                objective_values.append(float(value))
            except (TypeError, ValueError):
                objective_values.append(None)
        return objective_values

    def _sanitize_objective_values(
        self,
        objective_values: List[Optional[float]],
        all_metrics: Dict[str, Any],
    ) -> Tuple[List[Optional[float]], List[str], bool]:
        """
        Normalize objective edge cases without introducing penalty constants.
        """
        sanitized_metrics: List[str] = []
        total_trades = all_metrics.get("total_trades")

        for idx, metric in enumerate(self.mo_config.objectives):
            if metric != "profit_factor":
                continue
            value = objective_values[idx] if idx < len(objective_values) else None
            if _is_inf(value):
                return objective_values, sanitized_metrics, True

        sanitize_enabled = bool(getattr(self.optuna_config, "sanitize_enabled", True))
        sanitize_threshold = getattr(self.optuna_config, "sanitize_trades_threshold", 0)
        try:
            sanitize_threshold = int(sanitize_threshold)
        except (TypeError, ValueError):
            sanitize_threshold = 0

        gate = sanitize_enabled and not _is_non_finite(total_trades)
        if gate:
            try:
                gate = float(total_trades) <= sanitize_threshold
            except (TypeError, ValueError):
                gate = False

        if not gate:
            return objective_values, sanitized_metrics, False

        for idx, value in enumerate(objective_values):
            metric = self.mo_config.objectives[idx]
            if metric not in SANITIZE_METRICS:
                continue
            if metric == "profit_factor":
                if _is_non_finite(value) and not _is_inf(value):
                    objective_values[idx] = 0.0
                    sanitized_metrics.append(metric)
                continue
            if _is_non_finite(value):
                objective_values[idx] = 0.0
                sanitized_metrics.append(metric)

        return objective_values, sanitized_metrics, False

    def _prepare_objective_values(
        self,
        all_metrics: Dict[str, Any],
    ) -> Tuple[List[Optional[float]], List[str], Union[float, Tuple[float, ...]], bool]:
        objective_values = self._extract_objective_values(all_metrics)
        objective_values, sanitized, force_fail = self._sanitize_objective_values(
            objective_values, all_metrics
        )

        if force_fail or any(_is_non_finite(v) for v in objective_values):
            nan_marker = tuple([float("nan")] * len(self.mo_config.objectives))
            objective_return = nan_marker if self.mo_config.is_multi_objective() else float("nan")
            return objective_values, sanitized, objective_return, True

        objective_return = (
            tuple(objective_values) if self.mo_config.is_multi_objective() else objective_values[0]
        )
        return objective_values, sanitized, objective_return, False

    def _log_failed_objective(
        self,
        trial: optuna.Trial,
        objective_values: List[Optional[float]],
        all_metrics: Dict[str, Any],
    ) -> None:
        if not getattr(self.base_config, "detailed_log", False):
            return
        pairs = []
        for name, value in zip(self.mo_config.objectives, objective_values):
            label = OBJECTIVE_DISPLAY_NAMES.get(name, name)
            pairs.append(f"'{label}': {_format_objective_value(value)}")
        values_str = ", ".join(pairs)
        total_trades = all_metrics.get("total_trades")
        trades_label = "?"
        if not _is_non_finite(total_trades):
            try:
                trades_label = str(int(round(float(total_trades))))
            except (TypeError, ValueError):
                trades_label = "?"
        OPTUNA_LOGGER.warning(
            "Trial %s failed with value (%s). Trades: %s.",
            getattr(trial, "number", "?"),
            values_str,
            trades_label,
        )

    def _primary_objective_for_improvement(self, objective_values: List[float]) -> float:
        primary_obj = self.mo_config.primary_objective or self.mo_config.objectives[0]
        primary_idx = self.mo_config.objectives.index(primary_obj)
        value = float(objective_values[primary_idx])
        if OBJECTIVE_DIRECTIONS[primary_obj] == "minimize":
            return -value
        return value

    def _mark_coverage_generation_for_nsga(self, trial: optuna.Trial) -> None:
        """Ensure enqueued coverage trials are visible to NSGA generations."""
        if not bool(getattr(self.optuna_config, "coverage_mode", False)):
            return

        sampler_type = str(getattr(self.sampler_config, "sampler_type", "")).lower()
        if sampler_type == "nsga2":
            generation_key = NSGAIISampler._GENERATION_KEY
        elif sampler_type == "nsga3":
            generation_key = NSGAIIISampler._GENERATION_KEY
        else:
            return

        trial_id = getattr(trial, "_trial_id", None)
        storage = getattr(getattr(trial, "study", None), "_storage", None)
        if trial_id is None or storage is None:
            return

        try:
            frozen_trial = storage.get_trial(trial_id)
            system_attrs = dict(getattr(frozen_trial, "system_attrs", {}) or {})
            if "fixed_params" not in system_attrs:
                return
            if system_attrs.get(generation_key) is not None:
                return
            storage.set_trial_system_attr(trial_id, generation_key, 0)
        except Exception:  # pragma: no cover - defensive safety
            logger.debug(
                "Failed to mark NSGA generation for trial %s in coverage mode",
                getattr(trial, "number", "?"),
                exc_info=True,
            )

    def _objective(self, trial: optuna.Trial, search_space: Dict[str, Dict[str, Any]]):
        self._mark_coverage_generation_for_nsga(trial)
        params_dict = self._prepare_trial_parameters(trial, search_space)

        payload = self._evaluate_trial_payload(params_dict)
        result = payload["result"]
        all_metrics = payload["all_metrics"]
        objective_values = payload["objective_values"]
        objective_return = payload["objective_return"]
        sanitized = payload["sanitized_metrics"]
        if sanitized:
            trial.set_user_attr("merlin.sanitized_metrics", sanitized)

        if payload["should_fail"]:
            self._log_failed_objective(trial, objective_values, all_metrics)
            # Optuna treats NaN returns as FAILED without aborting the study.
            return objective_return

        _trial_set_result_attrs(
            trial=trial,
            result=result,
            objective_values=objective_values,
            all_metrics=all_metrics,
            constraint_values=payload["constraint_values"],
            constraints_satisfied=payload["constraints_satisfied"],
        )

        if self.pruner is not None:
            trial.report(objective_return, step=0)
            if trial.should_prune():
                self.pruned_trials += 1
                raise optuna.TrialPruned("Pruned by Optuna")

        self.trial_results.append(result)
        result.optuna_trial_number = trial.number

        improvement_value = self._primary_objective_for_improvement(objective_values)
        if improvement_value > self.best_value:
            self.best_value = improvement_value
            self.trials_without_improvement = 0
        else:
            self.trials_without_improvement += 1

        return objective_return

    def _objective_for_worker(
        self, trial: optuna.Trial, search_space: Dict[str, Dict[str, Any]]
    ):
        """
        Objective used inside worker processes (no shared state).
        """
        self._mark_coverage_generation_for_nsga(trial)
        params_dict = self._prepare_trial_parameters(trial, search_space)
        payload = self._evaluate_trial_payload(params_dict)
        result = payload["result"]
        all_metrics = payload["all_metrics"]
        objective_values = payload["objective_values"]
        objective_return = payload["objective_return"]
        sanitized = payload["sanitized_metrics"]
        if sanitized:
            trial.set_user_attr("merlin.sanitized_metrics", sanitized)

        if payload["should_fail"]:
            self._log_failed_objective(trial, objective_values, all_metrics)
            # Optuna treats NaN returns as FAILED without aborting the study.
            return objective_return

        _trial_set_result_attrs(
            trial=trial,
            result=result,
            objective_values=objective_values,
            all_metrics=all_metrics,
            constraint_values=payload["constraint_values"],
            constraints_satisfied=payload["constraints_satisfied"],
        )

        if self.pruner is not None:
            trial.report(objective_return, step=0)
            if trial.should_prune():
                raise optuna.TrialPruned("Pruned by Optuna")

        return objective_return

    # ------------------------------------------------------------------
    # Main execution entrypoint
    # ------------------------------------------------------------------
    def _get_sampler_type(self) -> str:
        return str(getattr(self.sampler_config, "sampler_type", "")).strip().lower()

    def _log_centralized_trial_completion(
        self,
        trial_number: int,
        objective_values: Sequence[float],
        params_dict: Dict[str, Any],
    ) -> None:
        objective_map = dict(zip(self.mo_config.get_metric_names(), objective_values))
        logger.info(
            "Trial %s finished with values: %s and parameters: %s.",
            trial_number,
            objective_map,
            params_dict,
        )

    def optimize(self) -> List[OptimizationResult]:
        workers = max(1, int(getattr(self.base_config, "worker_processes", 1) or 1))
        if workers <= 1:
            return self._optimize_single_process()
        sampler_type = self._get_sampler_type()
        if sampler_type in {"nsga2", "nsga3"}:
            return self._optimize_multiprocess_nsga(workers)
        if sampler_type == "tpe":
            return self._optimize_multiprocess_tpe(workers)
        return self._optimize_multiprocess(workers)

    def _optimize_single_process(self) -> List[OptimizationResult]:
        logger.info(
            "Starting single-process Optuna optimisation: objectives=%s, budget_mode=%s",
            ",".join(self.mo_config.objectives),
            self.optuna_config.budget_mode,
        )

        self._multiprocess_mode = False
        self.start_time = time.time()
        self.trial_results = []
        self.best_value = float("-inf")
        self.trials_without_improvement = 0
        self.pruned_trials = 0

        search_space = self._build_search_space()
        self._prepare_data_and_strategy()

        sampler = self._create_sampler()
        self.pruner = None

        study_name = self.optuna_config.study_name or f"strategy_opt_{time.time_ns()}"

        self.study = create_optimization_study(
            mo_config=self.mo_config,
            sampler=sampler,
            study_name=study_name,
            storage=None,
            pruner=self.pruner,
            load_if_exists=False,
        )
        self._enqueue_coverage_trials(search_space, context_label="single-process")

        timeout = None
        n_trials = None
        callbacks = []

        if self.optuna_config.budget_mode == "time":
            timeout = max(60, int(self.optuna_config.time_limit))
        elif self.optuna_config.budget_mode == "trials":
            n_trials = max(1, int(self.optuna_config.n_trials))
        elif self.optuna_config.budget_mode == "convergence":
            n_trials = 10000

            def convergence_callback(study: optuna.Study, _trial: optuna.Trial) -> None:
                if self.trials_without_improvement >= int(self.optuna_config.convergence_patience):
                    study.stop()
                    logger.info(
                        "Stopping optimisation due to convergence threshold (patience=%s)",
                        self.optuna_config.convergence_patience,
                    )

            callbacks.append(convergence_callback)

        try:
            self.study.optimize(
                lambda trial: self._objective(trial, search_space),
                n_trials=n_trials,
                timeout=timeout,
                callbacks=callbacks or None,
                show_progress_bar=False,
            )
        except KeyboardInterrupt:
            logger.info("Optuna optimisation interrupted by user")
        finally:
            self.pruner = None

        return self._finalize_results()

    def _optimize_multiprocess_tpe(self, n_workers: int) -> List[OptimizationResult]:
        return self._optimize_multiprocess_centralized(
            n_workers,
            dispatcher_label="TPE",
            worker_name_prefix="TPEEvaluator",
            coverage_context_label="multiprocess-tpe",
            mark_nsga_generation=False,
            allow_pruning=not self.mo_config.is_multi_objective(),
        )

    def _optimize_multiprocess_nsga(self, n_workers: int) -> List[OptimizationResult]:
        return self._optimize_multiprocess_centralized(
            n_workers,
            dispatcher_label="NSGA",
            worker_name_prefix="NSGAEvaluator",
            coverage_context_label="multiprocess-nsga",
            mark_nsga_generation=True,
            allow_pruning=False,
        )

    def _optimize_multiprocess_centralized(
        self,
        n_workers: int,
        *,
        dispatcher_label: str,
        worker_name_prefix: str,
        coverage_context_label: str,
        mark_nsga_generation: bool,
        allow_pruning: bool,
    ) -> List[OptimizationResult]:
        logger.info(
            "Starting multi-process %s optimisation via centralized ask/tell: "
            "objectives=%s, budget_mode=%s, workers=%s",
            dispatcher_label,
            ",".join(self.mo_config.objectives),
            self.optuna_config.budget_mode,
            n_workers,
        )

        self._multiprocess_mode = True
        self.start_time = time.time()
        self.trial_results = []
        self.pruned_trials = 0
        self.best_value = float("-inf")
        self.trials_without_improvement = 0
        self._duplicate_skipped_count = 0

        search_space = self._build_search_space()
        search_space_size = _estimate_search_space_size(search_space)

        csv_source_mode, csv_source_payload = _serialize_csv_source_for_worker(self.base_config.csv_file)
        base_config_dict = {
            f.name: (None if f.name == "csv_file" else getattr(self.base_config, f.name))
            for f in fields(self.base_config)
        }
        optuna_config_dict = asdict(self.optuna_config)

        task_queue: mp.Queue = mp.Queue()
        result_queue: mp.Queue = mp.Queue()
        error_queue: mp.Queue = mp.Queue()

        sampler = self._create_sampler()
        self.pruner = self._create_pruner() if allow_pruning else None
        study_name = self.optuna_config.study_name or f"strategy_opt_{time.time_ns()}"
        processes: List[mp.Process] = []
        sampler_type = self._get_sampler_type()

        if self.optuna_config.budget_mode == "time":
            timeout = max(60, int(self.optuna_config.time_limit))
            target_trials: Optional[int] = None
            logger.info("Time budget per study: %ss", timeout)
        elif self.optuna_config.budget_mode == "trials":
            timeout = None
            target_trials = max(1, int(self.optuna_config.n_trials))
            logger.info("Global trial budget: %s", target_trials)
        elif self.optuna_config.budget_mode == "convergence":
            timeout = None
            target_trials = 10000
            logger.warning(
                "Convergence budget is not fully supported in multi-process mode; "
                "using trial cap of 10000."
            )
        else:
            timeout = None
            target_trials = max(1, int(self.optuna_config.n_trials))

        duplicate_retry_limit = 1000
        pending_trials: Dict[int, Tuple[optuna.Trial, str]] = {}
        in_flight_keys: Set[str] = set()
        seen_keys: Set[str] = set()
        completed_evaluations = 0
        consecutive_duplicate_skips = 0
        dispatch_closed = False

        try:
            self.study = create_optimization_study(
                mo_config=self.mo_config,
                sampler=sampler,
                study_name=study_name,
                storage=None,
                pruner=self.pruner,
                load_if_exists=False,
            )
            self._enqueue_coverage_trials(search_space, context_label=coverage_context_label)

            for worker_id in range(n_workers):
                proc = mp.Process(
                    target=_evaluator_worker_entry,
                    args=(
                        task_queue,
                        result_queue,
                        csv_source_mode,
                        csv_source_payload,
                        base_config_dict,
                        optuna_config_dict,
                        worker_id,
                        error_queue,
                    ),
                    name=f"{worker_name_prefix}-{worker_id}",
                )
                proc.start()
                processes.append(proc)
                logger.info("Started %s evaluator worker %s (pid=%s)", dispatcher_label, worker_id, proc.pid)

            while True:
                error_details = _drain_worker_errors(error_queue)
                if error_details:
                    first_detail = error_details[0]
                    for error_detail in error_details:
                        logger.error(
                            "Evaluator worker %s failure detail: %s\n%s",
                            error_detail.get("worker_id"),
                            error_detail.get("message"),
                            error_detail.get("traceback"),
                        )
                    raise RuntimeError(
                        f"Multi-process {dispatcher_label} optimisation failed; first error from worker "
                        f"{first_detail.get('worker_id')}: {first_detail.get('message')}"
                    )

                for worker_id, proc in enumerate(processes):
                    if proc.exitcode not in (None, 0):
                        raise RuntimeError(
                            f"Multi-process {dispatcher_label} optimisation failed; worker {worker_id} "
                            f"exited with code {proc.exitcode}"
                        )

                if timeout is not None and (time.time() - (self.start_time or time.time())) >= timeout:
                    dispatch_closed = True

                if search_space_size is not None and (len(seen_keys) + len(in_flight_keys)) >= search_space_size:
                    if not dispatch_closed:
                        logger.info(
                            "%s search space exhausted after %s unique evaluations; stopping early.",
                            dispatcher_label,
                            len(seen_keys),
                        )
                    dispatch_closed = True

                while not dispatch_closed and len(pending_trials) < n_workers:
                    if target_trials is not None and (completed_evaluations + len(pending_trials)) >= target_trials:
                        dispatch_closed = True
                        break

                    trial = self.study.ask()
                    if mark_nsga_generation:
                        self._mark_coverage_generation_for_nsga(trial)
                    params_dict = self._prepare_trial_parameters(trial, search_space)
                    params_key = _build_params_key(params_dict)

                    if params_key in seen_keys or params_key in in_flight_keys:
                        self._duplicate_skipped_count += 1
                        consecutive_duplicate_skips += 1
                        trial.set_user_attr(_DUPLICATE_SKIPPED_ATTR, True)
                        trial.set_user_attr(_DUPLICATE_SKIP_REASON_ATTR, "exact_params")
                        self.study.tell(trial, state=TrialState.FAIL)
                        logger.info(
                            "Skipping duplicate %s proposal trial=%s sampler=%s",
                            dispatcher_label,
                            trial.number,
                            sampler_type,
                        )
                        if consecutive_duplicate_skips >= duplicate_retry_limit:
                            logger.warning(
                                "%s generated %s consecutive duplicate proposals; stopping dispatch early.",
                                dispatcher_label,
                                consecutive_duplicate_skips,
                            )
                            dispatch_closed = True
                            break
                        continue

                    consecutive_duplicate_skips = 0
                    pending_trials[trial.number] = (trial, params_key)
                    in_flight_keys.add(params_key)
                    task_queue.put(
                        {
                            "trial_number": trial.number,
                            "params": params_dict,
                            "params_key": params_key,
                        }
                    )

                if dispatch_closed and not pending_trials:
                    break

                try:
                    result_message = result_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                trial_number = int(result_message["trial_number"])
                pending_entry = pending_trials.pop(trial_number, None)
                if pending_entry is None:
                    logger.warning(
                        "Received %s worker result for unknown trial %s",
                        dispatcher_label,
                        trial_number,
                    )
                    continue

                trial, params_key = pending_entry
                in_flight_keys.discard(params_key)
                seen_keys.add(params_key)
                completed_evaluations += 1

                payload = dict(result_message["payload"])
                sanitized = list(payload.get("sanitized_metrics") or [])
                if sanitized:
                    trial.set_user_attr("merlin.sanitized_metrics", sanitized)

                objective_values = list(payload.get("objective_values") or [])
                all_metrics = dict(payload.get("all_metrics") or {})
                objective_return = payload.get("objective_return")

                if payload.get("should_fail"):
                    self._log_failed_objective(trial, objective_values, all_metrics)
                    self.study.tell(trial, state=TrialState.FAIL)
                    continue

                result = payload["result"]
                _trial_set_result_attrs(
                    trial=trial,
                    result=result,
                    objective_values=objective_values,
                    all_metrics=all_metrics,
                    constraint_values=list(payload.get("constraint_values") or []),
                    constraints_satisfied=bool(payload.get("constraints_satisfied", True)),
                )

                if self.pruner is not None:
                    trial.report(float(objective_return), step=0)
                    if trial.should_prune():
                        self.pruned_trials += 1
                        self.study.tell(trial, state=TrialState.PRUNED)
                        continue

                self.study.tell(trial, values=objective_return)
                self._log_centralized_trial_completion(trial.number, objective_values, result.params)

            self.trial_results = []
            for trial in self.study.trials:
                if _is_duplicate_skipped_trial(trial):
                    continue
                if trial.state == TrialState.COMPLETE:
                    try:
                        self.trial_results.append(_result_from_trial(trial))
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.warning("Failed to rebuild trial %s: %s", trial.number, exc)

            if self._duplicate_skipped_count:
                logger.info(
                    "%s dispatcher skipped %s duplicate proposals.",
                    dispatcher_label,
                    self._duplicate_skipped_count,
                )

            return self._finalize_results()
        finally:
            for _ in processes:
                try:
                    task_queue.put_nowait(None)
                except Exception:
                    break
            _terminate_processes(processes)
            for ipc_queue in (task_queue, result_queue, error_queue):
                try:
                    ipc_queue.close()
                    ipc_queue.join_thread()
                except Exception:  # pragma: no cover - defensive
                    logger.debug("Failed to shutdown multiprocessing queue", exc_info=True)
            self.pruner = None

    def _optimize_multiprocess(self, n_workers: int) -> List[OptimizationResult]:
        logger.info(
            "Starting multi-process Optuna optimisation: objectives=%s, budget_mode=%s, workers=%s",
            ",".join(self.mo_config.objectives),
            self.optuna_config.budget_mode,
            n_workers,
        )

        self._multiprocess_mode = True
        self.start_time = time.time()
        self.trial_results = []
        self.pruned_trials = 0
        self.best_value = float("-inf")
        self.trials_without_improvement = 0

        # Build search space early to validate config and prepare deterministic coverage.
        search_space = self._build_search_space()

        csv_source_mode, csv_source_payload = _serialize_csv_source_for_worker(self.base_config.csv_file)
        base_config_dict = {
            f.name: (None if f.name == "csv_file" else getattr(self.base_config, f.name))
            for f in fields(self.base_config)
        }
        optuna_config_dict = asdict(self.optuna_config)

        from optuna.storages import JournalStorage
        manager = mp.Manager()
        shared_logs = manager.list()
        error_queue: mp.Queue = mp.Queue()

        study_name = self.optuna_config.study_name or f"strategy_opt_{time.time_ns()}"
        storage = JournalStorage(InMemoryJournalBackend(shared_logs))

        sampler = self._create_sampler()
        self.pruner = self._create_pruner()
        processes: List[mp.Process] = []
        try:
            self.study = create_optimization_study(
                mo_config=self.mo_config,
                sampler=sampler,
                study_name=study_name,
                storage=storage,
                pruner=self.pruner,
                load_if_exists=False,
            )
            self._enqueue_coverage_trials(search_space, context_label="multiprocess")

            timeout: Optional[int] = None
            n_trials: Optional[int] = None

            if self.optuna_config.budget_mode == "time":
                timeout = max(60, int(self.optuna_config.time_limit))
                logger.info("Time budget per study: %ss", timeout)
            elif self.optuna_config.budget_mode == "trials":
                n_trials = max(1, int(self.optuna_config.n_trials))
                logger.info("Global trial budget: %s", n_trials)
            elif self.optuna_config.budget_mode == "convergence":
                logger.warning(
                    "Convergence budget is not fully supported in multi-process mode; "
                    "using trial cap of 10000."
                )
                n_trials = 10000

            worker_failures: List[Tuple[int, Optional[int]]] = []

            try:
                for worker_id in range(n_workers):
                    proc = mp.Process(
                        target=_worker_process_entry,
                        args=(
                            study_name,
                            shared_logs,
                            csv_source_mode,
                            csv_source_payload,
                            base_config_dict,
                            optuna_config_dict,
                            n_trials,
                            timeout,
                            worker_id,
                            error_queue,
                        ),
                        name=f"OptunaWorker-{worker_id}",
                    )
                    proc.start()
                    processes.append(proc)
                    logger.info("Started worker %s (pid=%s)", worker_id, proc.pid)

                logger.info("Waiting for %s workers to finish...", n_workers)
                remaining_workers = set(range(len(processes)))
                while remaining_workers:
                    completed_this_pass = False
                    for worker_id in list(remaining_workers):
                        proc = processes[worker_id]
                        proc.join(timeout=0.1)
                        if proc.exitcode is None:
                            continue

                        remaining_workers.remove(worker_id)
                        completed_this_pass = True

                        if proc.exitcode == 0:
                            logger.info("Worker %s completed successfully", worker_id)
                            continue

                        logger.error("Worker %s exited with code %s", worker_id, proc.exitcode)
                        worker_failures.append((worker_id, proc.exitcode))
                        _terminate_processes([processes[idx] for idx in remaining_workers])
                        remaining_workers.clear()
                        break

                    if not completed_this_pass:
                        time.sleep(0.05)

            except KeyboardInterrupt:
                logger.info("Optimisation interrupted; terminating workers...")
                raise RuntimeError("Multi-process Optuna optimisation interrupted.")

            if worker_failures:
                failed_workers = ", ".join(
                    f"{worker_id}:{exitcode}" for worker_id, exitcode in worker_failures
                )
                worker_error_details = _drain_worker_errors(error_queue)
                for error_detail in worker_error_details:
                    logger.error(
                        "Worker %s failure detail: %s\n%s",
                        error_detail.get("worker_id"),
                        error_detail.get("message"),
                        error_detail.get("traceback"),
                    )
                failure_message = f"Multi-process Optuna optimisation failed; worker exit codes: {failed_workers}"
                if worker_error_details:
                    first_detail = worker_error_details[0]
                    failure_message = (
                        f"{failure_message}; first error from worker {first_detail.get('worker_id')}: "
                        f"{first_detail.get('message')}"
                    )
                raise RuntimeError(failure_message)

            self.study = optuna.load_study(study_name=study_name, storage=storage)

            self.trial_results = []
            for trial in self.study.trials:
                if trial.state == TrialState.COMPLETE:
                    try:
                        self.trial_results.append(_result_from_trial(trial))
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.warning("Failed to rebuild trial %s: %s", trial.number, exc)

            return self._finalize_results()
        finally:
            _terminate_processes(processes)
            try:
                error_queue.close()
                error_queue.join_thread()
            except Exception:  # pragma: no cover - defensive
                logger.debug("Failed to shutdown worker error queue", exc_info=True)
            try:
                manager.shutdown()
            except Exception:  # pragma: no cover - defensive
                logger.debug("Failed to shutdown multiprocessing Manager", exc_info=True)
            self.pruner = None

    def _finalize_results(self) -> List[OptimizationResult]:
        end_time = time.time()
        optimisation_time = end_time - (self.start_time or end_time)
        effective_trials = []
        if self.study:
            effective_trials = [trial for trial in self.study.trials if not _is_duplicate_skipped_trial(trial)]

        logger.info(
            "Optuna optimisation completed: trials=%s, time=%.1fs",
            len(effective_trials) if self.study else len(self.trial_results),
            optimisation_time,
        )

        score_config = self.base_config.score_config or DEFAULT_SCORE_CONFIG
        self.all_trial_results = list(self.trial_results)
        self.trial_results = calculate_score(self.trial_results, score_config)

        if self.study:
            completed_trials = sum(1 for trial in effective_trials if trial.state == TrialState.COMPLETE)
            pruned_trials = sum(1 for trial in effective_trials if trial.state == TrialState.PRUNED)
            total_trials = len(effective_trials)
        else:
            completed_trials = len(self.trial_results)
            pruned_trials = self.pruned_trials
            total_trials = completed_trials + pruned_trials

        constraints_enabled = any(spec.enabled for spec in self.constraints)
        self.trial_results = sort_optimization_results(
            self.trial_results, self.study, self.mo_config, constraints_enabled
        )

        best_result = self.trial_results[0] if self.trial_results else None
        best_trial_number = best_result.optuna_trial_number if best_result else None
        best_value = None
        best_values = None
        if best_result and best_result.objective_values:
            if self.mo_config.is_multi_objective():
                best_values = dict(zip(self.mo_config.objectives, best_result.objective_values))
            else:
                best_value = float(best_result.objective_values[0])

        pareto_front_size = sum(1 for r in self.trial_results if r.is_pareto_optimal) if self.mo_config.is_multi_objective() else None

        initial_trials = max(0, int(getattr(self.optuna_config, "warmup_trials", 0) or 0))
        coverage_mode = bool(getattr(self.optuna_config, "coverage_mode", False))
        coverage_min = self._coverage_report.get("n_min") if self._coverage_report else None
        coverage_rec = self._coverage_report.get("n_rec") if self._coverage_report else None
        coverage_warning: Optional[str] = None
        if (
            coverage_mode
            and coverage_min is not None
            and coverage_rec is not None
            and initial_trials < int(coverage_min)
        ):
            coverage_warning = (
                f"Need more initial trials (min: {int(coverage_min)}, recommended: {int(coverage_rec)})"
            )

        summary = {
            "method": "Optuna",
            "objectives": list(self.mo_config.objectives),
            "primary_objective": self.mo_config.primary_objective,
            "budget_mode": self.optuna_config.budget_mode,
            "total_trials": total_trials,
            "completed_trials": completed_trials,
            "pruned_trials": pruned_trials,
            "best_trial_number": best_trial_number,
            "best_value": best_value,
            "best_values": best_values,
            "pareto_front_size": pareto_front_size,
            "optimization_time_seconds": optimisation_time,
            "multiprocess_mode": self._multiprocess_mode,
            "initial_search_mode": "coverage" if coverage_mode else "random",
            "initial_search_trials": initial_trials,
            "coverage_min_trials": coverage_min,
            "coverage_recommended_trials": coverage_rec,
            "coverage_block_size": self._coverage_report.get("coverage_block_size") if self._coverage_report else None,
            "coverage_main_axis": self._coverage_report.get("main_axis_name") if self._coverage_report else None,
            "coverage_main_axis_options": self._coverage_report.get("main_axis_options") if self._coverage_report else None,
            "coverage_primary_numeric": self._coverage_report.get("primary_numeric_name") if self._coverage_report else None,
            "coverage_warning": coverage_warning,
        }
        setattr(self.base_config, "optuna_summary", summary)

        return self.trial_results


def run_optuna_optimization(
    base_config, optuna_config: OptunaConfig
) -> Tuple[List[OptimizationResult], Optional[str]]:
    """Execute Optuna optimisation using the provided configuration."""

    optimizer = OptunaOptimizer(base_config, optuna_config)
    results = optimizer.optimize()
    setattr(base_config, "optuna_all_results", getattr(optimizer, "all_trial_results", list(results)))

    study_id: Optional[str] = None
    if getattr(base_config, "optimization_mode", "optuna") == "optuna":
        csv_path = _resolve_csv_path_for_study(getattr(base_config, "csv_file", ""))
        try:
            study_id = save_optuna_study_to_db(
                study=optimizer.study,
                config=base_config,
                optuna_config=optuna_config,
                trial_results=results,
                csv_file_path=csv_path,
                start_time=optimizer.start_time or time.time(),
                score_config=getattr(base_config, "score_config", None),
            )
        except Exception:
            logger.exception("Failed to save Optuna study to database")
            raise

    return results, study_id


def run_optimization(config: OptimizationConfig) -> Tuple[List[OptimizationResult], Optional[str]]:
    """Compat wrapper that executes Optuna optimization only."""

    if not getattr(config, "strategy_id", ""):
        raise ValueError("strategy_id must be specified in OptimizationConfig.")

    if getattr(config, "optimization_mode", "optuna") != "optuna":
        raise ValueError("Only Optuna optimization is supported in Phase 3.")

    objectives = getattr(config, "objectives", None) or getattr(config, "optuna_objectives", None) or []
    primary_objective = getattr(config, "primary_objective", None)
    constraints_payload = getattr(config, "constraints", None) or []
    n_startup_trials = getattr(config, "n_startup_trials", None)
    if n_startup_trials is None:
        n_startup_trials = getattr(config, "optuna_warmup_trials", 20)

    optuna_config = OptunaConfig(
        objectives=list(objectives),
        primary_objective=primary_objective,
        constraints=_build_constraint_specs(constraints_payload),
        sanitize_enabled=bool(getattr(config, "sanitize_enabled", True)),
        sanitize_trades_threshold=int(getattr(config, "sanitize_trades_threshold", 0) or 0),
        sampler_config=_build_sampler_config(config),
        budget_mode=getattr(config, "optuna_budget_mode", "trials"),
        n_trials=int(getattr(config, "optuna_n_trials", 500) or 500),
        time_limit=int(getattr(config, "optuna_time_limit", 3600) or 3600),
        convergence_patience=int(getattr(config, "optuna_convergence", 50) or 50),
        enable_pruning=bool(getattr(config, "optuna_enable_pruning", True)),
        pruner=getattr(config, "optuna_pruner", "median"),
        warmup_trials=int(n_startup_trials or 20),
        coverage_mode=bool(getattr(config, "coverage_mode", False)),
        save_study=False,
        study_name=getattr(config, "optuna_study_name", None),
    )

    return run_optuna_optimization(config, optuna_config)
