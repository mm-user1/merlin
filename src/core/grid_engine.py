"""Deterministic Grid optimizer orchestration.

Grid mode is intentionally separate from Optuna.  The core engine owns generic
budget parsing, mode allocation, ranking, validation orchestration, and storage
handoff.  Strategy-specific parameter semantics and fast numeric evaluation live
in per-strategy backends.
"""
from __future__ import annotations

import importlib
import json
import logging
import math
import re
import time
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

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
FAST_GRID_BACKENDS = {
    "s03_reversal_v10": "strategies.s03_reversal_v10.fast_grid",
    "s06_r_trend_v02": "strategies.s06_r_trend_v02.fast_grid",
}
GRID_SUPPORTED_FAST_OBJECTIVES = {
    "net_profit_pct",
    "max_drawdown_pct",
    "romad",
    "profit_factor",
    "win_rate",
}
GRID_SUPPORTED_OBJECTIVES = GRID_SUPPORTED_FAST_OBJECTIVES
GRID_SUPPORTED_SLOW_OBJECTIVES = GRID_SUPPORTED_FAST_OBJECTIVES | {
    "sharpe_ratio",
    "sortino_ratio",
    "sqn",
    "ulcer_index",
    "consistency_score",
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
GRID_V2_SUPPORTED_FAST_OBJECTIVES = GRID_SUPPORTED_FAST_OBJECTIVES | {
    "total_trades",
    "max_consecutive_losses",
}
GRID_V2_SUPPORTED_SLOW_OBJECTIVES = GRID_SUPPORTED_SLOW_OBJECTIVES | {
    "total_trades",
    "max_consecutive_losses",
}
MODE_ORDER = ("cc_only", "tbands_only", "both")

OBJECTIVE_DIRECTIONS.setdefault("total_trades", "maximize")
OBJECTIVE_DIRECTIONS.setdefault("max_consecutive_losses", "minimize")


@dataclass(frozen=True)
class GridSelectionConfig:
    fast_objectives: List[str]
    fast_primary_objective: Optional[str]
    slow_refinement_enabled: bool
    slow_objectives: List[str]
    slow_primary_objective: Optional[str]

    @property
    def final_objectives(self) -> List[str]:
        return self.slow_objectives if self.slow_refinement_enabled else self.fast_objectives

    @property
    def final_primary_objective(self) -> Optional[str]:
        return self.slow_primary_objective if self.slow_refinement_enabled else self.fast_primary_objective


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
    return str(strategy_id or "").strip().lower() in FAST_GRID_BACKENDS


def supports_grid_v2(strategy_id: str) -> bool:
    """Return whether a registered strategy opts into the generic V2 Grid path."""

    try:
        from strategies import get_strategy_config

        strategy_config = get_strategy_config(str(strategy_id or "").strip())
    except Exception:
        return False
    return str(strategy_config.get("engine", "v1")).strip().lower() == "v2"


def get_grid_v2_backend_metadata(strategy_id: str) -> Dict[str, Any]:
    """Return capability metadata for the generic V2 Grid backend."""

    from strategies import get_strategy_config

    from .engine_v2.compiled_kernel import compiled_batch_available, compiled_unavailable_reason
    from .engine_v2.profile import parse_execution_profile
    from .grid_v2 import COMPILED_BATCH_KIND, GRID_V2_ENGINE_VERSION, REFERENCE_BATCH_KIND

    strategy_config = get_strategy_config(str(strategy_id or "").strip())
    if str(strategy_config.get("engine", "v1")).strip().lower() != "v2":
        raise ValueError(f"Grid V2 is not supported for strategy '{strategy_id}'.")
    profile = parse_execution_profile(strategy_config)
    compiled_available = compiled_batch_available()
    return {
        "profile": "full_enumeration_v2",
        "engine": "v2",
        "engine_version": GRID_V2_ENGINE_VERSION,
        "backend_kind": COMPILED_BATCH_KIND if compiled_available else REFERENCE_BATCH_KIND,
        "compiled_batch_available": compiled_available,
        "compiled_unavailable_reason": compiled_unavailable_reason(),
        "numba_available": compiled_available,
        "numba_import_error": compiled_unavailable_reason(),
        "supports_partial_coverage": False,
        "supports_seed": False,
        "supports_mode_allocation": False,
        "retain_all_fast_results": True,
        "modes": [
            {"id": name, "label": name, "default_enabled": True}
            for name in profile.variants
        ],
        "diversity_group_fields": ["variant_name"],
    }


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


def _grid_v2_settings_from_config(config: OptimizationConfig):
    from strategies import get_strategy_config

    from .grid_v2 import GridV2Settings

    strategy_config = get_strategy_config(config.strategy_id)
    parameter_specs = strategy_config.get("parameters", {}) if isinstance(strategy_config, Mapping) else {}
    optimized_names = {
        str(name)
        for name, spec in parameter_specs.items()
        if isinstance(spec, Mapping)
        and isinstance(spec.get("optimize", {}), Mapping)
        and bool(spec.get("optimize", {}).get("enabled", False))
    }
    enabled_params = getattr(config, "enabled_params", {}) or {}
    enabled_axes = tuple(
        name for name, enabled in enabled_params.items()
        if bool(enabled) and str(name) in optimized_names
    )
    enabled_variants = tuple(getattr(config, "grid_enabled_modes", []) or ()) or None
    return GridV2Settings(
        top_n=max(0, int(getattr(config, "grid_top_candidates", 10) or 10)),
        worker_multiplier=max(1, int(getattr(config, "worker_processes", 1) or 1)),
        enabled_variants=enabled_variants,
        enabled_axes=enabled_axes or None,
        prefer_compiled=bool(getattr(config, "grid_v2_prefer_compiled", True)),
        primary_metric=(
            getattr(config, "grid_fast_primary_objective", None)
            or (getattr(config, "grid_fast_objectives", None) or getattr(config, "objectives", None) or ["net_profit_pct"])[0]
        ),
    )


def _load_backend(strategy_id: str):
    normalized = str(strategy_id or "").strip().lower()
    module_name = FAST_GRID_BACKENDS.get(normalized)
    if not module_name:
        raise ValueError(f"Grid mode is not supported for strategy '{strategy_id}'.")
    backend = importlib.import_module(module_name)
    required = (
        "build_parameter_space",
        "build_preview",
        "generate_candidates",
        "prepare_fast_data",
        "evaluate_candidates",
        "validate_selected_candidates",
    )
    missing = [name for name in required if not callable(getattr(backend, name, None))]
    if missing:
        raise ValueError(
            f"Malformed Grid backend '{module_name}': missing callable(s): "
            + ", ".join(missing)
        )
    return backend


def get_fast_grid_backend_metadata(strategy_id: str) -> Dict[str, Any]:
    """Return normalized capability metadata for a strategy fast backend."""
    backend = _load_backend(strategy_id)
    metadata_factory = getattr(backend, "get_backend_metadata", None)
    metadata = metadata_factory() if callable(metadata_factory) else {}
    if not isinstance(metadata, dict):
        raise ValueError("Malformed Grid backend metadata: expected a mapping.")
    normalized = {
        "profile": "sampled_by_mode",
        "modes": [],
        "supports_partial_coverage": True,
        "supports_seed": True,
        "supports_mode_allocation": True,
        "retain_all_fast_results": True,
        "diversity_group_fields": ["mode", "maType3", "maLength3"],
    }
    normalized.update(metadata)
    normalized["numba_available"] = bool(getattr(backend, "NUMBA_AVAILABLE", False))
    normalized["numba_import_error"] = getattr(backend, "NUMBA_IMPORT_ERROR", None)
    return normalized


def default_grid_enabled_modes(strategy_id: str) -> List[str]:
    """Return ordered backend modes whose metadata marks them default-enabled.

    Used to default a missing ``grid_enabled_modes`` field without a per-strategy
    hardcode.  Strategies whose backend declares no explicit modes (e.g. S03,
    which allocates by cc/tbands/both rather than enabled modes) yield an empty
    list, leaving their existing defaulting untouched.  Backend order is
    preserved and malformed metadata is rejected clearly.
    """
    if supports_grid_v2(strategy_id):
        metadata = get_grid_v2_backend_metadata(strategy_id)
        return [
            str(mode.get("id"))
            for mode in metadata.get("modes", [])
            if isinstance(mode, Mapping) and mode.get("default_enabled", True) is not False
        ]
    if not supports_fast_grid(strategy_id):
        return []
    metadata = get_fast_grid_backend_metadata(strategy_id)
    modes = metadata.get("modes")
    if not modes:
        return []
    if not isinstance(modes, (list, tuple)):
        raise ValueError("Malformed Grid backend metadata: 'modes' must be a list.")
    enabled: List[str] = []
    for mode in modes:
        if not isinstance(mode, Mapping):
            raise ValueError("Malformed Grid backend metadata: each mode must be a mapping.")
        mode_id = str(mode.get("id") or "").strip().lower()
        if not mode_id:
            raise ValueError("Malformed Grid backend metadata: mode is missing an 'id'.")
        if mode.get("default_enabled", True) is not False:
            enabled.append(mode_id)
    return list(dict.fromkeys(enabled))


def normalize_diversity_group_fields(value: Any) -> Any:
    """Return a JSON-safe defensive copy of backend diversity metadata.

    The generic contract preserves the backend-provided shape so that flat
    backends (e.g. S03) keep their ``list[str]`` field list while mode-specific
    backends (e.g. S06) keep their ``dict[str, list[str]]`` mapping.  Malformed
    metadata is rejected clearly rather than silently corrupted (the historical
    ``list(...)`` coercion reduced a mapping to its mode names).
    """
    if value is None:
        return []
    if isinstance(value, Mapping):
        normalized: Dict[str, List[str]] = {}
        for key, fields in value.items():
            if not isinstance(fields, (list, tuple)):
                raise ValueError(
                    "Malformed diversity_group_fields: mapping values must be lists of field names."
                )
            normalized[str(key)] = [str(field) for field in fields]
        return normalized
    if isinstance(value, (list, tuple)):
        return [str(field) for field in value]
    raise ValueError(
        "Malformed diversity_group_fields: expected a list or a mapping of mode -> field list."
    )


def _build_backend_allocation(
    backend: Any,
    config: OptimizationConfig,
    space: Any,
    settings: GridSettings,
) -> GridAllocation:
    builder = getattr(backend, "build_allocation", None)
    if callable(builder):
        allocation = builder(config, space, settings)
        if not isinstance(allocation, GridAllocation):
            raise ValueError("Malformed Grid backend allocation: expected GridAllocation.")
        return allocation
    return allocate_mode_budgets(
        space.mode_space_sizes,
        settings.requested_budget,
        method=settings.allocation_method,
        min_quota=settings.min_quota,
        manual_percents=settings.manual_percents,
    )


def _objective_list(value: Any, fallback: Sequence[str]) -> List[str]:
    if isinstance(value, (list, tuple)):
        objectives = [str(item).strip() for item in value if str(item).strip()]
    else:
        objectives = []
    if not objectives:
        objectives = [str(item).strip() for item in fallback if str(item).strip()]
    return objectives or ["net_profit_pct"]


def _primary_objective(value: Any, objectives: Sequence[str], fallback: Any = None) -> Optional[str]:
    primary = str(value).strip() if value not in (None, "") else ""
    if not primary and fallback not in (None, ""):
        primary = str(fallback).strip()
    if len(objectives) <= 1:
        return None
    return primary or None


def resolve_grid_selection_config(config: OptimizationConfig) -> GridSelectionConfig:
    """Resolve explicit Grid objective settings with old-study fallback."""
    legacy_objectives = _objective_list(getattr(config, "objectives", None), ["net_profit_pct"])
    legacy_primary = getattr(config, "primary_objective", None)

    fast_objectives = _objective_list(
        getattr(config, "grid_fast_objectives", None),
        legacy_objectives,
    )
    fast_primary = _primary_objective(
        getattr(config, "grid_fast_primary_objective", None),
        fast_objectives,
        legacy_primary,
    )

    slow_refinement_enabled = bool(getattr(config, "grid_slow_refinement_enabled", False))
    slow_objectives = _objective_list(
        getattr(config, "grid_slow_objectives", None),
        legacy_objectives,
    )
    slow_primary = _primary_objective(
        getattr(config, "grid_slow_primary_objective", None),
        slow_objectives,
        legacy_primary,
    )

    return GridSelectionConfig(
        fast_objectives=fast_objectives,
        fast_primary_objective=fast_primary,
        slow_refinement_enabled=slow_refinement_enabled,
        slow_objectives=slow_objectives,
        slow_primary_objective=slow_primary,
    )


def _validate_objective_set(
    *,
    stage: str,
    objectives: Sequence[str],
    primary_objective: Optional[str],
    supported: set[str],
) -> None:
    if not objectives:
        raise ValueError(f"At least 1 Grid {stage} objective is required.")
    unsupported = sorted(set(objectives) - supported)
    if unsupported:
        if "composite_score" in unsupported:
            raise ValueError("Composite Score is not supported in Grid v1.")
        raise ValueError(
            f"Grid {stage} objective is not available: " + ", ".join(unsupported)
        )
    if len(objectives) > 1 and primary_objective not in objectives:
        raise ValueError(f"Primary Grid {stage} objective must be one of the selected objectives.")


def validate_grid_config(config: OptimizationConfig) -> None:
    if not supports_fast_grid(config.strategy_id):
        raise ValueError(f"Grid mode is not supported for strategy '{config.strategy_id}'.")

    backend = _load_backend(config.strategy_id)
    if not getattr(backend, "NUMBA_AVAILABLE", False):
        reason = getattr(backend, "NUMBA_IMPORT_ERROR", None) or "Numba import failed"
        raise ValueError(f"Grid mode requires Numba: {reason}")

    selection = resolve_grid_selection_config(config)
    _validate_objective_set(
        stage="fast screening",
        objectives=selection.fast_objectives,
        primary_objective=selection.fast_primary_objective,
        supported=GRID_SUPPORTED_FAST_OBJECTIVES,
    )
    if selection.slow_refinement_enabled:
        _validate_objective_set(
            stage="slow refinement",
            objectives=selection.slow_objectives,
            primary_objective=selection.slow_primary_objective,
            supported=GRID_SUPPORTED_SLOW_OBJECTIVES,
        )

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
    if supports_grid_v2(config.strategy_id):
        return _preview_grid_v2_parameter_space(config)
    validate_grid_config(config)
    backend = _load_backend(config.strategy_id)
    settings = _settings_from_config(config)
    space = backend.build_parameter_space(config)
    allocation = _build_backend_allocation(backend, config, space, settings)
    return backend.build_preview(space, allocation)


def _preview_grid_v2_parameter_space(config: OptimizationConfig) -> Dict[str, Any]:
    from strategies import get_strategy_config

    from .grid_v2 import GridV2Settings, preview_grid_v2_counts

    strategy_config = get_strategy_config(config.strategy_id)
    settings = _grid_v2_settings_from_config(config)
    preview = preview_grid_v2_counts(
        strategy_config,
        settings=settings,
        base_params=getattr(config, "fixed_params", {}) or {},
    )
    total = int(preview.deduped_candidate_count or preview.enumerated_candidate_count)
    return {
        "engine": "v2",
        "profile": "full_enumeration_v2",
        "full_candidate_count": total,
        "candidate_count": total,
        "coverage_pct": 100.0,
        "mode_space_sizes": dict(preview.per_variant_counts),
        "mode_budgets": dict(preview.per_variant_counts),
        "mode_coverage_pct": {name: 100.0 for name in preview.per_variant_counts},
        "axis_names_by_variant": {
            name: list(values) for name, values in preview.axis_names_by_variant.items()
        },
    }


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
    stage_label: str = "Grid screening",
    rank_attr: str = "grid_rank",
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
        objective_list = ", ".join(objectives)
        raise ValueError(
            f"{stage_label} produced no candidates with usable objective values"
            + (f" for: {objective_list}" if objective_list else ".")
        )

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
        setattr(item, rank_attr, rank)
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


def compute_grid_dsr_benchmark(reference_results: Sequence[OptimizationResult]) -> Dict[str, Any]:
    """Compute the full-population Grid DSR benchmark used for candidate ranking."""
    references = list(reference_results or [])
    finite_sharpes = [
        value
        for value in (_finite_float(getattr(result, "sharpe_ratio", None)) for result in references)
        if value is not None
    ]
    mean_sharpe = sum(finite_sharpes) / len(finite_sharpes) if finite_sharpes else None
    var_sharpe = None
    if finite_sharpes:
        var_sharpe = sum((value - mean_sharpe) ** 2 for value in finite_sharpes) / len(finite_sharpes)

    sr0 = None
    if var_sharpe is not None:
        sr0 = calculate_expected_max_sharpe(0.0, var_sharpe, len(finite_sharpes))

    return {
        "enabled": True,
        "dsr_n_trials": len(finite_sharpes),
        "dsr_mean_sharpe": mean_sharpe,
        "dsr_var_sharpe": var_sharpe,
        "dsr_sr0": sr0,
    }


def _benchmark_sr0(dsr_benchmark: Mapping[str, Any]) -> Optional[float]:
    for key in ("dsr_sr0", "sr0", "grid_dsr_sr0"):
        value = _finite_float(dsr_benchmark.get(key))
        if value is not None:
            return value
    return None


def rank_grid_candidates_by_dsr(
    candidate_results: Sequence[OptimizationResult],
    *,
    dsr_benchmark: Mapping[str, Any],
    top_k: int,
) -> List[OptimizationResult]:
    """Rank candidates by DSR using a persisted full-population benchmark."""
    top_k = max(1, int(top_k or 1))
    eligible = list(candidate_results or [])[:top_k]
    sr0 = _benchmark_sr0(dsr_benchmark)

    candidates: List[OptimizationResult] = []
    for source_rank, result in enumerate(eligible, 1):
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
        setattr(result, "dsr_source_rank", source_rank)
        if sr_value is not None:
            candidates.append(result)

    def rank_value(item: OptimizationResult, attr: str) -> int:
        try:
            value = int(getattr(item, attr, 0) or 0)
        except (TypeError, ValueError):
            value = 0
        return value if value > 0 else 10**9

    def dsr_sort_key(item: OptimizationResult) -> Tuple[Any, ...]:
        probability = getattr(item, "dsr_probability", None)
        probability_key = float(probability) if probability is not None else float("-inf")
        return (
            probability is None,
            -probability_key,
            rank_value(item, "dsr_source_rank"),
            rank_value(item, "grid_rank"),
            str(getattr(item, "semantic_key", "") or ""),
            int(getattr(item, "candidate_id", getattr(item, "optuna_trial_number", 0)) or 0),
        )

    ranked_dsr = sorted(candidates, key=dsr_sort_key)
    selected = ranked_dsr[: min(top_k, len(ranked_dsr))]
    for rank, result in enumerate(selected, 1):
        setattr(result, "dsr_rank", rank)
        _add_selection_source(result, "dsr")

    return selected


def apply_fast_grid_dsr(
    candidate_results: Sequence[OptimizationResult],
    *,
    reference_results: Optional[Sequence[OptimizationResult]] = None,
    top_k: int,
) -> Tuple[List[OptimizationResult], Dict[str, Any]]:
    """Compute fast DSR fields and select DSR top-K from eligible Grid candidates."""
    top_k = max(1, int(top_k or 1))
    references = list(reference_results if reference_results is not None else candidate_results)
    dsr_metadata = compute_grid_dsr_benchmark(references)
    selected = rank_grid_candidates_by_dsr(
        candidate_results,
        dsr_benchmark=dsr_metadata,
        top_k=top_k,
    )
    dsr_metadata = dict(dsr_metadata)
    dsr_metadata.update(
        {
            "top_k": top_k,
            "selected_count": len(selected),
        }
    )
    return selected, {
        "enabled": dsr_metadata.get("enabled"),
        "top_k": dsr_metadata.get("top_k"),
        "selected_count": dsr_metadata.get("selected_count"),
        "dsr_n_trials": dsr_metadata.get("dsr_n_trials"),
        "dsr_mean_sharpe": dsr_metadata.get("dsr_mean_sharpe"),
        "dsr_var_sharpe": dsr_metadata.get("dsr_var_sharpe"),
        "dsr_sr0": dsr_metadata.get("dsr_sr0"),
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
                optuna_rank=int(getattr(result, "dsr_source_rank", idx) or idx),
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


def _preserve_fast_selection_metadata(
    selected_fast: Sequence[OptimizationResult],
    selected_results: Sequence[OptimizationResult],
) -> None:
    for fast_result, slow_result in zip(selected_fast, selected_results):
        for attr in ("is_pareto_optimal", "dominance_rank"):
            if hasattr(fast_result, attr):
                setattr(slow_result, attr, getattr(fast_result, attr, None))


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


def _grid_v2_result_from_row(row: Any, *, metric_tier: str) -> OptimizationResult:
    profit_factor = float(row.profit_factor)
    result = OptimizationResult(
        params=dict(row.params),
        net_profit_pct=float(row.net_profit_pct),
        max_drawdown_pct=float(row.max_drawdown_pct),
        total_trades=int(row.total_trades),
        winning_trades=int(row.winning_trades),
        losing_trades=int(row.losing_trades),
        win_rate=float(row.win_rate_pct),
        avg_win=(float(row.gross_profit) / int(row.winning_trades)) if int(row.winning_trades) else 0.0,
        avg_loss=(float(row.gross_loss) / int(row.losing_trades)) if int(row.losing_trades) else 0.0,
        gross_profit=float(row.gross_profit),
        gross_loss=float(row.gross_loss),
        max_consecutive_losses=int(row.max_consecutive_losses),
        romad=float(row.romad),
        profit_factor=None if math.isnan(profit_factor) else profit_factor,
        optuna_trial_number=int(row.candidate_id),
    )
    setattr(result, "engine", "v2")
    setattr(result, "candidate_id", int(row.candidate_id))
    setattr(result, "semantic_key", row.semantic_key)
    setattr(result, "param_key", row.semantic_key)
    setattr(result, "canonical_identity", row.canonical_identity)
    setattr(result, "variant_name", row.variant_name)
    setattr(result, "grid_mode_name", row.variant_name)
    setattr(result, "grid_generation_mode", "full_enumeration_v2")
    setattr(result, "grid_backend_kind", row.backend_kind)
    setattr(result, "grid_v2_engine_version", "grid_v2_phase2_5")
    setattr(result, "metric_tier", metric_tier)
    setattr(result, "guardrail_summary", dict(row.guardrail_summary or {}))
    setattr(
        result,
        "fast_metrics",
        {
            "net_profit_pct": row.net_profit_pct,
            "max_drawdown_pct": row.max_drawdown_pct,
            "romad": row.romad,
            "profit_factor": row.profit_factor,
            "win_rate": row.win_rate_pct,
            "total_trades": row.total_trades,
            "winning_trades": row.winning_trades,
            "losing_trades": row.losing_trades,
            "gross_profit": row.gross_profit,
            "gross_loss": row.gross_loss,
            "max_consecutive_losses": row.max_consecutive_losses,
        },
    )
    setattr(result, "validation_status", row.status)
    return result


def _grid_v2_slow_result(
    *,
    plan: Any,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: Any,
    row: Any,
) -> OptimizationResult:
    candidate = plan.candidates[int(row.candidate_id) - 1]
    params = hooks.normalize_params(dict(candidate.params)) if hooks.normalize_params else dict(candidate.params)
    data = hooks.build_execution_data(df, params)
    from .engine_v2.runner import run_v2_strategy

    run = run_v2_strategy(
        data=data,
        profile=plan.profile,
        params=params,
        trade_start_idx=trade_start_idx,
    )
    strategy_result = run.strategy_result
    profit_factor = getattr(strategy_result, "profit_factor", None)
    result = OptimizationResult(
        params=dict(candidate.params),
        net_profit_pct=float(getattr(strategy_result, "net_profit_pct", 0.0)),
        max_drawdown_pct=float(getattr(strategy_result, "max_drawdown_pct", 0.0)),
        total_trades=int(getattr(strategy_result, "total_trades", 0)),
        winning_trades=int(getattr(strategy_result, "winning_trades", 0)),
        losing_trades=int(getattr(strategy_result, "losing_trades", 0)),
        win_rate=(
            float(getattr(strategy_result, "winning_trades", 0))
            / float(getattr(strategy_result, "total_trades", 0))
            * 100.0
            if int(getattr(strategy_result, "total_trades", 0)) else 0.0
        ),
        gross_profit=float(getattr(strategy_result, "gross_profit", 0.0)),
        gross_loss=float(getattr(strategy_result, "gross_loss", 0.0)),
        max_consecutive_losses=_max_consecutive_losses_for_grid_v2(strategy_result.trades),
        romad=getattr(strategy_result, "romad", None),
        profit_factor=profit_factor,
        sharpe_ratio=getattr(strategy_result, "sharpe_ratio", None),
        sortino_ratio=getattr(strategy_result, "sortino_ratio", None),
        sqn=getattr(strategy_result, "sqn", None),
        ulcer_index=getattr(strategy_result, "ulcer_index", None),
        consistency_score=getattr(strategy_result, "consistency_score", None),
        optuna_trial_number=int(row.candidate_id),
    )
    result.avg_win = (
        result.gross_profit / result.winning_trades if result.winning_trades else 0.0
    )
    result.avg_loss = (
        result.gross_loss / result.losing_trades if result.losing_trades else 0.0
    )
    fast_result = _grid_v2_result_from_row(row, metric_tier="fast")
    for attr in (
        "engine",
        "candidate_id",
        "semantic_key",
        "param_key",
        "canonical_identity",
        "variant_name",
        "grid_mode_name",
        "grid_generation_mode",
        "grid_backend_kind",
        "grid_v2_engine_version",
        "guardrail_summary",
        "fast_metrics",
    ):
        if hasattr(fast_result, attr):
            setattr(result, attr, getattr(fast_result, attr))
    setattr(result, "metric_tier", "slow_public_v2")
    setattr(result, "validation_status", "passed")
    return result


def _max_consecutive_losses_for_grid_v2(trades: Sequence[Any]) -> int:
    max_consecutive = 0
    consecutive = 0
    for trade in trades:
        if isinstance(trade, Mapping):
            pnl = float(trade["net_pnl"])
        elif hasattr(trade, "net_pnl"):
            pnl = float(trade.net_pnl)
        else:
            pnl = float(trade)
        if pnl <= 0.0:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0
    return max_consecutive


def _run_grid_v2_optimization(
    config: OptimizationConfig,
    *,
    save_study: bool,
) -> Tuple[List[OptimizationResult], Optional[str]]:
    if bool(getattr(config, "grid_needs_dsr", False)):
        raise ValueError("V2 Grid DSR is unavailable in Phase 2.5; disable DSR for engine='v2'.")

    from strategies import get_strategy, get_strategy_config

    from .grid_v2 import GridV2StrategyHooks, build_grid_v2_plan, execute_grid_v2_candidates

    strategy_config = get_strategy_config(config.strategy_id)
    strategy_class = get_strategy(config.strategy_id)
    strategy_module = importlib.import_module(strategy_class.__module__)
    hooks = GridV2StrategyHooks.from_strategy(strategy_module)
    settings = _grid_v2_settings_from_config(config)
    constraints = _build_constraint_specs(getattr(config, "constraints", []) or [])
    selection_config = resolve_grid_selection_config(config)
    _validate_objective_set(
        stage="V2 fast screening",
        objectives=selection_config.fast_objectives,
        primary_objective=selection_config.fast_primary_objective,
        supported=GRID_V2_SUPPORTED_FAST_OBJECTIVES,
    )
    if selection_config.slow_refinement_enabled:
        _validate_objective_set(
            stage="V2 slow refinement",
            objectives=selection_config.slow_objectives,
            primary_objective=selection_config.slow_primary_objective,
            supported=GRID_V2_SUPPORTED_SLOW_OBJECTIVES,
        )
    unsupported_constraints = sorted(
        {spec.metric for spec in constraints if spec.enabled and spec.metric not in GRID_SUPPORTED_CONSTRAINTS}
    )
    if unsupported_constraints:
        raise ValueError(
            "Grid V2 constraint metric is not available: " + ", ".join(unsupported_constraints)
        )

    started = time.time()
    timings: Dict[str, float] = {}

    plan_started = time.time()
    plan = build_grid_v2_plan(
        strategy_config,
        settings=settings,
        base_params=getattr(config, "fixed_params", {}) or {},
    )
    timings["candidate_generation_seconds"] = time.time() - plan_started

    data_started = time.time()
    df, trade_start_idx, start_ts, end_ts = _prepare_grid_dataframe(config)
    timings["data_prepare_seconds"] = time.time() - data_started

    eval_started = time.time()
    run_result = execute_grid_v2_candidates(
        plan,
        df,
        trade_start_idx,
        hooks,
    )
    timings["fast_evaluation_seconds"] = time.time() - eval_started

    metric_tier = (
        "compiled_fast"
        if bool(run_result.metadata.get("compiled_batch_used"))
        else "reference_fast"
    )
    all_fast_results = [
        _grid_v2_result_from_row(row, metric_tier=metric_tier)
        for row in run_result.rows
    ]
    ranked_fast = rank_grid_results(
        all_fast_results,
        objectives=selection_config.fast_objectives,
        primary_objective=selection_config.fast_primary_objective,
        constraints=constraints,
        stage_label="Grid V2 fast screening",
    )
    selected_fast, diversity_metadata = apply_diversity_cap(
        ranked_fast,
        top_n=settings.top_n,
        enabled=bool(getattr(config, "grid_diversity_enabled", True)),
        max_per_group=max(1, int(getattr(config, "grid_diversity_max_per_group", 2) or 2)),
    )
    objective_selected_count = len(selected_fast)

    row_by_id = {int(row.candidate_id): row for row in run_result.rows}
    validation_started = time.time()
    selected_results = [
        _grid_v2_slow_result(
            plan=plan,
            df=df,
            trade_start_idx=trade_start_idx,
            hooks=hooks,
            row=row_by_id[int(getattr(result, "candidate_id"))],
        )
        for result in selected_fast
    ]
    _preserve_fast_selection_metadata(selected_fast, selected_results)
    timings["slow_validation_seconds"] = time.time() - validation_started

    slow_refinement_started = time.time()
    if selection_config.slow_refinement_enabled:
        ranked_selected = rank_grid_results(
            selected_results,
            objectives=selection_config.slow_objectives,
            primary_objective=selection_config.slow_primary_objective,
            constraints=constraints,
            stage_label="Grid V2 slow refinement",
            rank_attr="slow_refinement_rank",
        )
    else:
        ranked_selected = _refresh_selected_metrics(
            selected_results,
            objectives=selection_config.fast_objectives,
            constraints=constraints,
        )
    timings["slow_refinement_seconds"] = time.time() - slow_refinement_started

    for result in ranked_selected:
        _add_selection_source(result, "objective")
        setattr(result, "optimizer_mode", GRID_MODE)
        result.optuna_trial_number = int(getattr(result, "candidate_id", result.optuna_trial_number or 0) or 0)

    ranked_selected = calculate_grid_display_scores(ranked_selected, getattr(config, "score_config", None))

    total_seconds = time.time() - started
    timings["total_seconds"] = total_seconds
    candidates_per_second = (
        len(run_result.rows) / timings["fast_evaluation_seconds"]
        if timings["fast_evaluation_seconds"] > 0.0
        else None
    )
    requested_budget = parse_grid_budget(getattr(config, "grid_budget", plan.deduped_candidate_count))
    actual_budget = len(run_result.rows)
    dsr_metadata = {
        "enabled": False,
        "status": "unavailable_deferred",
        "reason": "V2 Grid DSR is deferred; full-population DSR is not computed in Phase 2.5.",
        "top_k": None,
        "selected_count": 0,
        "dsr_n_trials": None,
        "dsr_mean_sharpe": None,
        "dsr_var_sharpe": None,
        "dsr_sr0": None,
    }
    cache_estimate = asdict(run_result.cache_estimate)
    cache_stats = asdict(run_result.cache_stats)
    backend_metadata = get_grid_v2_backend_metadata(config.strategy_id)
    backend_metadata.update(
        {
            "backend_kind": run_result.metadata.get("backend_kind"),
            "compiled_batch_used": bool(run_result.metadata.get("compiled_batch_used")),
        }
    )
    summary = {
        "method": "Grid",
        "optimizer_mode": GRID_MODE,
        "engine": "v2",
        "grid_v2_engine_version": plan.metadata.get("engine_version"),
        "objectives": selection_config.final_objectives,
        "primary_objective": selection_config.final_primary_objective,
        "selection_objectives": selection_config.final_objectives,
        "selection_primary_objective": selection_config.final_primary_objective,
        "grid_fast_objectives": selection_config.fast_objectives,
        "grid_fast_primary_objective": selection_config.fast_primary_objective,
        "grid_slow_refinement_enabled": selection_config.slow_refinement_enabled,
        "grid_slow_objectives": selection_config.slow_objectives,
        "grid_slow_primary_objective": selection_config.slow_primary_objective,
        "requested_budget": requested_budget,
        "actual_budget": actual_budget,
        "unused_budget": max(0, requested_budget - actual_budget),
        "total_trials": actual_budget,
        "completed_trials": len(ranked_fast),
        "pruned_trials": 0,
        "candidate_count": plan.deduped_candidate_count,
        "valid_candidate_count": len(ranked_fast),
        "selected_candidate_count": len(ranked_selected),
        "full_candidate_count": plan.deduped_candidate_count,
        "objective_selected_count": objective_selected_count,
        "dsr_selected_count": 0,
        "union_selected_count": len(ranked_selected),
        "dsr_n_trials": None,
        "dsr_mean_sharpe": None,
        "dsr_var_sharpe": None,
        "best_trial_number": getattr(ranked_selected[0], "candidate_id", None) if ranked_selected else None,
        "best_value": ranked_selected[0].objective_values[0] if ranked_selected and ranked_selected[0].objective_values else None,
        "best_values": dict(zip(selection_config.final_objectives, ranked_selected[0].objective_values))
        if ranked_selected and len(selection_config.final_objectives) > 1
        else None,
        "pareto_front_size": sum(1 for result in ranked_selected if getattr(result, "is_pareto_optimal", False))
        if len(selection_config.final_objectives) > 1
        else None,
        "optimization_time_seconds": total_seconds,
        "grid": {
            "backend": backend_metadata,
            "backend_kind": run_result.metadata.get("backend_kind"),
            "compiled_batch_available": bool(run_result.metadata.get("compiled_batch_available")),
            "compiled_batch_used": bool(run_result.metadata.get("compiled_batch_used")),
            "candidate_count": plan.deduped_candidate_count,
            "valid_candidate_count": len(ranked_fast),
            "selected_candidate_count": len(ranked_selected),
            "per_variant_counts": dict(plan.per_variant_counts),
            "cache_estimate": cache_estimate,
            "cache_stats": cache_stats,
            "timings": timings,
            "candidates_per_second": candidates_per_second,
            "fast_objectives": selection_config.fast_objectives,
            "fast_primary_objective": selection_config.fast_primary_objective,
            "slow_refinement_enabled": selection_config.slow_refinement_enabled,
            "slow_objectives": selection_config.slow_objectives,
            "slow_primary_objective": selection_config.slow_primary_objective,
            "preview": {
                "full_candidate_count": plan.deduped_candidate_count,
                "coverage_pct": 100.0,
            },
            "allocation": {
                "requested_budget": requested_budget,
                "actual_budget": actual_budget,
                "unused_budget": max(0, requested_budget - actual_budget),
                "mode_space_sizes": dict(plan.per_variant_counts),
                "mode_budgets": dict(plan.per_variant_counts),
                "mode_coverage_pct": {name: 100.0 for name in plan.per_variant_counts},
                "allocation_method": "full_enumeration_v2",
            },
            "optional_axis_settings": {
                "enabled_axes": list(settings.enabled_axes) if settings.enabled_axes is not None else None,
                "enabled_variants": list(settings.enabled_variants) if settings.enabled_variants is not None else None,
            },
            "dsr_metric_computation_enabled": False,
            "dsr": dsr_metadata,
            "guardrail_aggregate_summary": _grid_v2_guardrail_aggregate(run_result.rows),
            "full_candidate_count": plan.deduped_candidate_count,
            "objective_selected_count": objective_selected_count,
            "dsr_selected_count": 0,
            "union_selected_count": len(ranked_selected),
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


def _grid_v2_guardrail_aggregate(rows: Sequence[Any]) -> Dict[str, Any]:
    aggregate: Dict[str, Any] = {
        "candidate_rows": len(rows),
        "rows_with_guardrail_flags": 0,
        "invalid_stop_distance_count": 0,
        "zero_size_entry_count": 0,
        "rejected_fill_count": 0,
        "margin_reject_count": 0,
        "liquidation_count": 0,
        "no_capital_halt_count": 0,
    }
    for row in rows:
        summary = dict(getattr(row, "guardrail_summary", {}) or {})
        flags = int(summary.get("flags", 0) or 0)
        if flags:
            aggregate["rows_with_guardrail_flags"] += 1
        for key in (
            "invalid_stop_distance_count",
            "zero_size_entry_count",
            "rejected_fill_count",
            "margin_reject_count",
            "liquidation_count",
        ):
            aggregate[key] += int(summary.get(key, 0) or 0)
        if bool(summary.get("no_capital_halt", False)):
            aggregate["no_capital_halt_count"] += 1
    return aggregate


def run_grid_optimization(
    config: OptimizationConfig,
    *,
    save_study: bool = True,
) -> Tuple[List[OptimizationResult], Optional[str]]:
    if supports_grid_v2(config.strategy_id):
        return _run_grid_v2_optimization(config, save_study=save_study)

    validate_grid_config(config)
    backend = _load_backend(config.strategy_id)
    backend_metadata = get_fast_grid_backend_metadata(config.strategy_id)
    settings = _settings_from_config(config)
    constraints = _build_constraint_specs(getattr(config, "constraints", []) or [])
    selection_config = resolve_grid_selection_config(config)
    fast_objectives = selection_config.fast_objectives
    fast_primary_objective = selection_config.fast_primary_objective
    final_objectives = selection_config.final_objectives
    final_primary_objective = selection_config.final_primary_objective
    needs_dsr = bool(getattr(config, "grid_needs_dsr", False))

    started = time.time()
    timings: Dict[str, float] = {}

    space_started = time.time()
    space = backend.build_parameter_space(config)
    allocation = _build_backend_allocation(backend, config, space, settings)
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
        objectives=fast_objectives,
        primary_objective=fast_primary_objective,
        constraints=constraints,
        stage_label="Grid fast screening",
    )
    selected_fast, diversity_metadata = apply_diversity_cap(
        ranked_fast,
        top_n=settings.top_candidates,
        enabled=settings.diversity_enabled,
        max_per_group=settings.diversity_max_per_group,
    )
    diversity_metadata["diversity_group_fields"] = normalize_diversity_group_fields(
        backend_metadata.get("diversity_group_fields")
    )
    objective_selected_count = len(selected_fast)

    validation_started = time.time()
    selected_results = backend.validate_selected_candidates(
        df,
        trade_start_idx,
        selected_fast,
        tolerances=settings.validation_tolerances,
        fail_on_error=settings.strict_validation,
    )
    _preserve_fast_selection_metadata(selected_fast, selected_results)
    timings["slow_validation_seconds"] = time.time() - validation_started

    slow_refinement_started = time.time()
    if selection_config.slow_refinement_enabled:
        ranked_selected = rank_grid_results(
            selected_results,
            objectives=selection_config.slow_objectives,
            primary_objective=selection_config.slow_primary_objective,
            constraints=constraints,
            stage_label="Grid slow refinement",
            rank_attr="slow_refinement_rank",
        )
    else:
        ranked_selected = _refresh_selected_metrics(
            selected_results,
            objectives=fast_objectives,
            constraints=constraints,
        )
    timings["slow_refinement_seconds"] = time.time() - slow_refinement_started

    for result in ranked_selected:
        _add_selection_source(result, "objective")

    if needs_dsr:
        dsr_top_k = max(
            1,
            int(getattr(config, "grid_dsr_top_k", settings.top_candidates) or settings.top_candidates),
        )
        dsr_selected, dsr_metadata = apply_fast_grid_dsr(
            ranked_selected[:dsr_top_k],
            reference_results=ranked_fast,
            top_k=dsr_top_k,
        )
    else:
        dsr_selected = []
        dsr_metadata = {
            "enabled": False,
            "top_k": None,
            "selected_count": 0,
            "dsr_n_trials": None,
            "dsr_mean_sharpe": None,
            "dsr_var_sharpe": None,
            "dsr_sr0": None,
        }

    ranked_selected = _union_selected_candidates(ranked_selected, dsr_selected)
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
        "objectives": final_objectives,
        "primary_objective": final_primary_objective,
        "selection_objectives": final_objectives,
        "selection_primary_objective": final_primary_objective,
        "grid_fast_objectives": fast_objectives,
        "grid_fast_primary_objective": fast_primary_objective,
        "grid_slow_refinement_enabled": selection_config.slow_refinement_enabled,
        "grid_slow_objectives": selection_config.slow_objectives,
        "grid_slow_primary_objective": selection_config.slow_primary_objective,
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
        "best_values": dict(zip(final_objectives, ranked_selected[0].objective_values)) if ranked_selected and len(final_objectives) > 1 else None,
        "pareto_front_size": sum(1 for result in ranked_selected if getattr(result, "is_pareto_optimal", False))
        if len(final_objectives) > 1
        else None,
        "optimization_time_seconds": total_seconds,
        "grid": {
            "backend": backend_metadata,
            "fast_objectives": fast_objectives,
            "fast_primary_objective": fast_primary_objective,
            "slow_refinement_enabled": selection_config.slow_refinement_enabled,
            "slow_objectives": selection_config.slow_objectives,
            "slow_primary_objective": selection_config.slow_primary_objective,
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
    setattr(
        config,
        "optuna_all_results",
        all_fast_results if backend_metadata.get("retain_all_fast_results", True) else [],
    )

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
