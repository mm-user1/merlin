"""Generic Backtester V2 Grid backend.

This module is backend-only. It plans deterministic V2 candidate spaces and
executes selected candidates through the shared V2 reference runner. It does
not integrate with the legacy Grid dispatcher.
"""

from __future__ import annotations

import hashlib
import inspect
import itertools
import json
import math
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import numpy as np
import pandas as pd

from core.engine_v2.contracts import ExecutionProfile, GuardrailSummary, VariantSpec
from core.engine_v2.kernel import ExecutionData
from core.engine_v2.metrics_kernel import CoreMetrics, compute_core_metrics_from_balance_and_trades
from core.engine_v2.profile import (
    active_parameter_names,
    canonical_selector_key,
    inactive_parameter_names,
    mode_binding_for,
    parse_execution_profile,
)
from core.engine_v2.runner import V2RunResult, run_v2_strategy


GRID_V2_ENGINE_VERSION = "grid_v2_phase2_reference"
REFERENCE_BATCH_KIND = "reference"
_BOOL_SIGNAL_ARRAYS = 2
_FLOAT_DATAPREP_ARRAYS = 5
_BYTES_PER_MB = 1024.0 * 1024.0


@dataclass(frozen=True)
class GridV2Settings:
    """Runtime settings for a backend-only Grid V2 run."""

    top_n: int = 10
    max_signal_cache_mb: float = 512.0
    worker_multiplier: int = 1
    enabled_variants: tuple[str, ...] | None = None
    enabled_axes: tuple[str, ...] | None = None
    price_rounding: str | None = None
    prefer_compiled: bool = True
    primary_metric: str = "net_profit_pct"
    include_inactive_axes_for_dedup: bool = False


@dataclass(frozen=True)
class GridV2StrategyHooks:
    """Strategy-specific data hooks consumed by the generic backend."""

    build_execution_data: Callable[[pd.DataFrame, Mapping[str, Any]], ExecutionData]
    normalize_params: Callable[[Mapping[str, Any] | None], Mapping[str, Any]] | None = None
    label: str = ""
    signal_param_names: tuple[str, ...] | None = None
    dataprep_param_names: tuple[str, ...] | None = None
    function_fingerprint: str | None = None

    @classmethod
    def from_strategy(cls, strategy: Any) -> "GridV2StrategyHooks":
        builder = getattr(strategy, "build_v2_execution_data", None)
        if builder is None:
            builder = getattr(strategy, "build_execution_data", None)
        if builder is None or not callable(builder):
            raise TypeError("Grid V2 strategy hooks require a callable build_v2_execution_data.")
        normalizer = getattr(strategy, "normalized_params", None)
        if normalizer is not None and not callable(normalizer):
            normalizer = None
        signal_names = _optional_name_tuple(getattr(strategy, "SIGNAL_CACHE_PARAM_NAMES", None))
        dataprep_names = _optional_name_tuple(getattr(strategy, "DATAPREP_CACHE_PARAM_NAMES", None))
        return cls(
            build_execution_data=builder,
            normalize_params=normalizer,
            label=str(getattr(strategy, "__name__", getattr(strategy, "__class__", type(strategy)).__name__)),
            signal_param_names=signal_names,
            dataprep_param_names=dataprep_names,
        )


@dataclass(frozen=True)
class GridV2ParameterDomain:
    name: str
    role: str | None
    values: tuple[Any, ...]
    default: Any
    is_axis: bool
    is_runtime: bool
    source: str


@dataclass(frozen=True)
class CandidateMappingRecord:
    candidate_id: int
    variant_name: str
    semantic_key: str
    canonical_identity: str
    active_param_values: Mapping[str, Any]
    axis_param_values: Mapping[str, Any]


@dataclass(frozen=True)
class GridV2Candidate:
    candidate_id: int
    variant_name: str
    modes: Mapping[str, str]
    params: Mapping[str, Any]
    active_param_names: tuple[str, ...]
    inactive_param_names: tuple[str, ...]
    axis_param_names: tuple[str, ...]
    semantic_key: str
    semantic_payload: Mapping[str, Any]
    canonical_identity: str


@dataclass(frozen=True)
class GridV2Plan:
    settings: GridV2Settings
    strategy_id: str
    strategy_version: str
    profile: ExecutionProfile
    parameter_domains: Mapping[str, GridV2ParameterDomain]
    candidates: tuple[GridV2Candidate, ...]
    mapping_records: tuple[CandidateMappingRecord, ...]
    raw_candidate_count: int
    enumerated_candidate_count: int
    deduped_candidate_count: int
    per_variant_counts: Mapping[str, int]
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GridV2CountPreview:
    raw_candidate_count: int
    enumerated_candidate_count: int
    deduped_candidate_count: int | None
    per_variant_counts: Mapping[str, int]
    axis_names_by_variant: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class GridV2CacheEstimate:
    n_bars: int
    signal_combo_count: int
    dataprep_combo_count: int
    worker_multiplier: int
    bytes_per_signal_combo: int
    bytes_per_dataprep_combo: int
    estimated_signal_mb: float
    estimated_dataprep_mb: float
    estimated_total_mb: float
    max_signal_cache_mb: float


@dataclass
class GridV2CacheStats:
    signal_hits: int = 0
    signal_misses: int = 0
    dataprep_hits: int = 0
    dataprep_misses: int = 0


@dataclass(frozen=True)
class GridV2ResultRow:
    candidate_id: int
    semantic_key: str
    variant_name: str
    modes: Mapping[str, str]
    params: Mapping[str, Any]
    net_profit_pct: float
    max_drawdown_pct: float
    romad: float
    profit_factor: float
    win_rate_pct: float
    total_trades: int
    final_balance: float
    guardrail_summary: Mapping[str, Any]
    status: str = "ok"
    error: str | None = None

    @property
    def win_rate(self) -> float:
        return self.win_rate_pct

    def metric_value(self, name: str) -> float:
        if name == "win_rate":
            name = "win_rate_pct"
        return float(getattr(self, name))


@dataclass(frozen=True)
class GridV2SelectedResult:
    row: GridV2ResultRow
    metrics: Mapping[str, Any]
    guardrail_summary: Mapping[str, Any]


@dataclass(frozen=True)
class GridV2RunResult:
    plan: GridV2Plan
    rows: tuple[GridV2ResultRow, ...]
    selected: tuple[GridV2SelectedResult, ...]
    cache_estimate: GridV2CacheEstimate
    cache_stats: GridV2CacheStats
    metadata: Mapping[str, Any] = field(default_factory=dict)


def build_grid_v2_plan(
    config: Mapping[str, Any],
    settings: GridV2Settings | None = None,
    base_params: Mapping[str, Any] | None = None,
) -> GridV2Plan:
    """Build a deterministic candidate plan from V2 config/profile metadata."""

    settings = settings or GridV2Settings()
    config_copy = _config_with_settings(config, settings)
    profile = parse_execution_profile(config_copy)
    params_spec = _parameters(config_copy)
    defaults = _parameter_defaults(params_spec)
    fixed_params = dict(defaults)
    fixed_params.update(dict(base_params or {}))
    domains = _build_parameter_domains(config_copy, settings, fixed_params, profile)
    selector_values = _selector_values_by_variant(config_copy, profile)
    selected_variants = _selected_variants(profile, settings)

    candidates: list[GridV2Candidate] = []
    records: list[CandidateMappingRecord] = []
    semantic_seen: set[str] = set()
    raw_count = 0
    enumerated_count = 0
    per_variant_counts: dict[str, int] = {name: 0 for name in selected_variants}

    for variant_name in selected_variants:
        variant = profile.variants[variant_name]
        seed_params = dict(fixed_params)
        if profile.variant_selector is not None:
            seed_params[profile.variant_selector.param] = selector_values[variant_name]
        active_names = _ordered_active_names(profile, seed_params)
        inactive_names = _ordered_inactive_names(profile, seed_params)
        axis_names = _variant_axis_names(
            profile=profile,
            domains=domains,
            active_names=active_names,
            settings=settings,
        )
        raw_count += _product_size(domains[name].values for name in axis_names)
        for values in itertools.product(*(domains[name].values for name in axis_names)):
            enumerated_count += 1
            params = dict(seed_params)
            params.update(zip(axis_names, values))
            semantic_payload = _semantic_payload(
                config=config_copy,
                profile=profile,
                variant=variant,
                params=params,
                active_names=active_names,
            )
            semantic_key = _stable_json(semantic_payload)
            if semantic_key in semantic_seen:
                continue
            semantic_seen.add(semantic_key)
            candidate_id = len(candidates) + 1
            canonical_identity = _canonical_identity(
                variant_name=variant_name,
                params=params,
                names=active_names,
            )
            candidate = GridV2Candidate(
                candidate_id=candidate_id,
                variant_name=variant_name,
                modes=dict(variant.modes),
                params=_jsonable_mapping(params),
                active_param_names=active_names,
                inactive_param_names=inactive_names,
                axis_param_names=tuple(axis_names),
                semantic_key=semantic_key,
                semantic_payload=semantic_payload,
                canonical_identity=canonical_identity,
            )
            candidates.append(candidate)
            per_variant_counts[variant_name] += 1
            records.append(
                CandidateMappingRecord(
                    candidate_id=candidate_id,
                    variant_name=variant_name,
                    semantic_key=semantic_key,
                    canonical_identity=canonical_identity,
                    active_param_values={name: _jsonable_value(params[name]) for name in active_names if name in params},
                    axis_param_values={name: _jsonable_value(params[name]) for name in axis_names if name in params},
                )
            )

    metadata = {
        "backend_kind": REFERENCE_BATCH_KIND,
        "engine_version": GRID_V2_ENGINE_VERSION,
        "compiled_batch_available": False,
        "default_enabled_axes": [
            name for name, domain in domains.items() if domain.is_axis
        ],
        "variant_order": list(selected_variants),
        "semantic_dedup_count": enumerated_count - len(candidates),
    }
    return GridV2Plan(
        settings=settings,
        strategy_id=profile.strategy_id,
        strategy_version=str(config_copy.get("version", "")),
        profile=profile,
        parameter_domains=domains,
        candidates=tuple(candidates),
        mapping_records=tuple(records),
        raw_candidate_count=raw_count,
        enumerated_candidate_count=enumerated_count,
        deduped_candidate_count=len(candidates),
        per_variant_counts=per_variant_counts,
        metadata=metadata,
    )


def preview_grid_v2_counts(
    config: Mapping[str, Any],
    settings: GridV2Settings | None = None,
    base_params: Mapping[str, Any] | None = None,
) -> GridV2CountPreview:
    """Compute deterministic Grid V2 breadth without materializing candidates."""

    settings = settings or GridV2Settings()
    config_copy = _config_with_settings(config, settings)
    profile = parse_execution_profile(config_copy)
    params_spec = _parameters(config_copy)
    defaults = _parameter_defaults(params_spec)
    fixed_params = dict(defaults)
    fixed_params.update(dict(base_params or {}))
    domains = _build_parameter_domains(config_copy, settings, fixed_params, profile)
    selector_values = _selector_values_by_variant(config_copy, profile)
    selected_variants = _selected_variants(profile, settings)
    per_variant: dict[str, int] = {}
    axis_names_by_variant: dict[str, tuple[str, ...]] = {}
    total = 0
    for variant_name in selected_variants:
        seed_params = dict(fixed_params)
        if profile.variant_selector is not None:
            seed_params[profile.variant_selector.param] = selector_values[variant_name]
        active_names = _ordered_active_names(profile, seed_params)
        axis_names = _variant_axis_names(
            profile=profile,
            domains=domains,
            active_names=active_names,
            settings=settings,
        )
        count = _product_size(domains[name].values for name in axis_names)
        per_variant[variant_name] = count
        axis_names_by_variant[variant_name] = axis_names
        total += count
    deduped = None if settings.include_inactive_axes_for_dedup else total
    return GridV2CountPreview(
        raw_candidate_count=total,
        enumerated_candidate_count=total,
        deduped_candidate_count=deduped,
        per_variant_counts=per_variant,
        axis_names_by_variant=axis_names_by_variant,
    )


def estimate_grid_v2_cache(
    plan: GridV2Plan,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks | Any,
    candidate_indices: Sequence[int] | None = None,
) -> GridV2CacheEstimate:
    """Estimate local cache memory for a planned run."""

    hooks = _coerce_hooks(hooks)
    selected = _selected_candidates(plan, candidate_indices)
    n_bars = int(len(df))
    signal_keys = {
        _signal_cache_key(plan, candidate, df, trade_start_idx, hooks)
        for candidate in selected
    }
    dataprep_keys = {
        _dataprep_cache_key(plan, candidate, df, trade_start_idx, hooks)
        for candidate in selected
    }
    signal_combo_count = len(signal_keys)
    dataprep_combo_count = len(dataprep_keys)
    worker_multiplier = max(1, int(plan.settings.worker_multiplier))
    bytes_per_signal_combo = int(n_bars * _BOOL_SIGNAL_ARRAYS * np.dtype(np.bool_).itemsize)
    bytes_per_dataprep_combo = int(n_bars * _FLOAT_DATAPREP_ARRAYS * np.dtype(np.float64).itemsize)
    estimated_signal_bytes = signal_combo_count * bytes_per_signal_combo
    estimated_dataprep_bytes = dataprep_combo_count * bytes_per_dataprep_combo
    estimated_total_bytes = (estimated_signal_bytes + estimated_dataprep_bytes) * worker_multiplier
    return GridV2CacheEstimate(
        n_bars=n_bars,
        signal_combo_count=signal_combo_count,
        dataprep_combo_count=dataprep_combo_count,
        worker_multiplier=worker_multiplier,
        bytes_per_signal_combo=bytes_per_signal_combo,
        bytes_per_dataprep_combo=bytes_per_dataprep_combo,
        estimated_signal_mb=estimated_signal_bytes / _BYTES_PER_MB,
        estimated_dataprep_mb=estimated_dataprep_bytes / _BYTES_PER_MB,
        estimated_total_mb=estimated_total_bytes / _BYTES_PER_MB,
        max_signal_cache_mb=float(plan.settings.max_signal_cache_mb),
    )


def execute_grid_v2_candidates(
    plan: GridV2Plan,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks | Any,
    candidate_indices: Sequence[int] | None = None,
) -> GridV2RunResult:
    """Execute planned candidates through the shared V2 reference runner."""

    hooks = _coerce_hooks(hooks)
    selected_candidates = _selected_candidates(plan, candidate_indices)
    estimate = estimate_grid_v2_cache(plan, df, trade_start_idx, hooks, candidate_indices)
    if estimate.estimated_total_mb > estimate.max_signal_cache_mb:
        raise MemoryError(
            "Grid V2 cache estimate exceeds max_signal_cache_mb "
            f"({estimate.estimated_total_mb:.3f} MB > {estimate.max_signal_cache_mb:.3f} MB)."
        )

    stats = GridV2CacheStats()
    signal_seen: set[str] = set()
    dataprep_cache: dict[str, ExecutionData] = {}
    rows: list[GridV2ResultRow] = []

    for candidate in selected_candidates:
        signal_key = _signal_cache_key(plan, candidate, df, trade_start_idx, hooks)
        if signal_key in signal_seen:
            stats.signal_hits += 1
        else:
            stats.signal_misses += 1
            signal_seen.add(signal_key)

        data_key = _dataprep_cache_key(plan, candidate, df, trade_start_idx, hooks)
        if data_key in dataprep_cache:
            stats.dataprep_hits += 1
            data = dataprep_cache[data_key]
        else:
            stats.dataprep_misses += 1
            try:
                data = hooks.build_execution_data(df, _normalized_candidate_params(hooks, candidate.params))
            except Exception as exc:
                rows.append(_error_row(candidate, exc))
                continue
            dataprep_cache[data_key] = data

        try:
            run = run_v2_strategy(
                data=data,
                profile=plan.profile,
                params=_normalized_candidate_params(hooks, candidate.params),
                trade_start_idx=trade_start_idx,
            )
        except Exception as exc:
            rows.append(_error_row(candidate, exc))
            continue
        rows.append(_row_from_run(candidate, run))

    selected = tuple(
        _slow_enrich_selected(plan, df, trade_start_idx, hooks, row, dataprep_cache)
        for row in _rank_rows(rows, plan.settings.primary_metric)[: max(0, int(plan.settings.top_n))]
    )
    return GridV2RunResult(
        plan=plan,
        rows=tuple(rows),
        selected=selected,
        cache_estimate=estimate,
        cache_stats=stats,
        metadata={
            "backend_kind": REFERENCE_BATCH_KIND,
            "compiled_batch_available": False,
            "metric_tier": "core_fast_rows_plus_selected_public_v2_enrichment",
            "executed_candidate_count": len(rows),
        },
    )


def run_grid_v2(
    config: Mapping[str, Any],
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks | Any,
    settings: GridV2Settings | None = None,
    base_params: Mapping[str, Any] | None = None,
    candidate_indices: Sequence[int] | None = None,
) -> GridV2RunResult:
    """Build and execute a Grid V2 plan."""

    plan = build_grid_v2_plan(config, settings=settings, base_params=base_params)
    return execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, candidate_indices)


def deterministic_candidate_subset_indices(
    total_count: int,
    limit: int,
    required_indices: Sequence[int] = (),
) -> tuple[int, ...]:
    """Return a stable zero-based subset covering edges and evenly spaced rows."""

    total = max(0, int(total_count))
    budget = max(0, int(limit))
    required = {int(index) for index in required_indices if 0 <= int(index) < total}
    if total == 0 or budget == 0:
        return ()
    if budget >= total:
        return tuple(range(total))
    required.update({0, total - 1})
    remaining = max(0, budget - len(required))
    if remaining > 0:
        for value in np.linspace(0, total - 1, remaining + 2, dtype=int)[1:-1]:
            required.add(int(value))
    if len(required) > budget:
        ordered_required = sorted(required)
        keep = {ordered_required[0], ordered_required[-1]}
        for value in ordered_required[1:-1]:
            if len(keep) >= budget:
                break
            keep.add(value)
        required = keep
    return tuple(sorted(required))


def _config_with_settings(config: Mapping[str, Any], settings: GridV2Settings) -> dict[str, Any]:
    config_copy = deepcopy(dict(config))
    if settings.price_rounding is not None:
        execution = dict(config_copy.get("execution") or {})
        execution["priceRounding"] = settings.price_rounding
        config_copy["execution"] = execution
    return config_copy


def _parameters(config: Mapping[str, Any]) -> Mapping[str, Any]:
    params = config.get("parameters", {})
    if not isinstance(params, Mapping):
        raise ValueError("Grid V2 config parameters must be a mapping.")
    return params


def _parameter_defaults(params: Mapping[str, Any]) -> dict[str, Any]:
    defaults: dict[str, Any] = {}
    for name, spec in params.items():
        if isinstance(spec, Mapping) and "default" in spec:
            defaults[str(name)] = spec["default"]
    return defaults


def _build_parameter_domains(
    config: Mapping[str, Any],
    settings: GridV2Settings,
    fixed_params: Mapping[str, Any],
    profile: ExecutionProfile,
) -> dict[str, GridV2ParameterDomain]:
    params_spec = _parameters(config)
    selector_name = profile.variant_selector.param if profile.variant_selector is not None else None
    enabled_axes = set(settings.enabled_axes) if settings.enabled_axes is not None else None
    if enabled_axes is not None:
        unknown = sorted(enabled_axes - {str(name) for name in params_spec})
        if unknown:
            raise ValueError(f"Grid V2 enabled_axes contains unknown parameter(s): {unknown}.")

    domains: dict[str, GridV2ParameterDomain] = {}
    for raw_name, raw_spec in params_spec.items():
        name = str(raw_name)
        spec = raw_spec if isinstance(raw_spec, Mapping) else {}
        role = profile.parameter_roles.get(name)
        param_type = str(spec.get("type", "float")).strip().lower()
        optimize = spec.get("optimize", {}) if isinstance(spec.get("optimize", {}), Mapping) else {}
        is_runtime = role == "runtime"
        is_selector = name == selector_name
        default = fixed_params.get(name, spec.get("default"))
        axis_available = bool(optimize.get("enabled", False)) and not is_runtime and not is_selector
        if name in (enabled_axes or set()) and not axis_available:
            raise ValueError(f"Grid V2 axis '{name}' is not an optimized non-runtime parameter.")
        is_axis = axis_available and _axis_enabled(name, optimize, settings)
        if is_axis:
            values, source = _axis_values(name, spec, param_type)
        else:
            values, source = (_coerce_value(default, param_type),), "fixed_default"
        if not values:
            raise ValueError(f"Grid V2 parameter '{name}' has an empty domain.")
        domains[name] = GridV2ParameterDomain(
            name=name,
            role=role,
            values=tuple(values),
            default=_jsonable_value(default),
            is_axis=is_axis,
            is_runtime=is_runtime,
            source=source,
        )
    return domains


def _axis_enabled(name: str, optimize: Mapping[str, Any], settings: GridV2Settings) -> bool:
    if settings.enabled_axes is not None:
        return name in set(settings.enabled_axes)
    return optimize.get("default_enabled", True) is not False


def _axis_values(name: str, spec: Mapping[str, Any], param_type: str) -> tuple[tuple[Any, ...], str]:
    optimize = spec.get("optimize", {}) if isinstance(spec.get("optimize", {}), Mapping) else {}
    for source, values in (
        ("gridValues", spec.get("gridValues")),
        ("optimize.gridValues", optimize.get("gridValues")),
        ("optimize.values", optimize.get("values")),
    ):
        if values is not None:
            return _explicit_values(name, values, param_type), source
    if param_type in {"select", "options"}:
        return _explicit_values(name, spec.get("options"), param_type), "options"
    if param_type == "bool":
        return (False, True), "bool"
    if param_type in {"int", "integer", "float", "number"}:
        return _numeric_range_values(name, spec, optimize, param_type), "optimize.range"
    raise ValueError(f"Grid V2 parameter '{name}' has unsupported type '{param_type}'.")


def _explicit_values(name: str, values: Any, param_type: str) -> tuple[Any, ...]:
    if not isinstance(values, (list, tuple)) or not values:
        raise ValueError(f"Grid V2 parameter '{name}' requires a non-empty value list.")
    normalized = tuple(dict.fromkeys(_coerce_value(value, param_type) for value in values))
    if not normalized:
        raise ValueError(f"Grid V2 parameter '{name}' has an empty value list.")
    return normalized


def _numeric_range_values(
    name: str,
    spec: Mapping[str, Any],
    optimize: Mapping[str, Any],
    param_type: str,
) -> tuple[Any, ...]:
    missing = [
        key for key in ("min", "max", "step")
        if key not in optimize and key not in spec
    ]
    if missing:
        raise ValueError(f"Grid V2 numeric parameter '{name}' is missing {missing}.")
    start = _finite_decimal(optimize.get("min", spec.get("min")), name)
    stop = _finite_decimal(optimize.get("max", spec.get("max")), name)
    step = _finite_decimal(optimize.get("step", spec.get("step")), name)
    if step <= 0:
        raise ValueError(f"Grid V2 numeric parameter '{name}' requires a positive step.")
    if stop < start:
        raise ValueError(f"Grid V2 numeric parameter '{name}' has max below min.")
    values: list[Any] = []
    current = start
    while current <= stop:
        values.append(_coerce_decimal(current, param_type))
        current += step
    if not values:
        raise ValueError(f"Grid V2 numeric parameter '{name}' has an empty range.")
    return tuple(values)


def _finite_decimal(value: Any, name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Grid V2 numeric parameter '{name}' has a non-numeric bound.") from exc
    if not parsed.is_finite():
        raise ValueError(f"Grid V2 numeric parameter '{name}' must be finite.")
    return parsed


def _coerce_decimal(value: Decimal, param_type: str) -> Any:
    if param_type in {"int", "integer"}:
        integral = value.to_integral_value()
        if value != integral:
            raise ValueError(f"Grid V2 int range produced non-integral value {value}.")
        return int(integral)
    as_float = float(value)
    if not math.isfinite(as_float):
        raise ValueError("Grid V2 float range produced a non-finite value.")
    return 0.0 if as_float == 0.0 else as_float


def _coerce_value(value: Any, param_type: str) -> Any:
    normalized_type = str(param_type).strip().lower()
    if normalized_type in {"int", "integer"}:
        return int(Decimal(str(value)).to_integral_value())
    if normalized_type in {"float", "number"}:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("Grid V2 float values must be finite.")
        return 0.0 if parsed == 0.0 else parsed
    if normalized_type == "bool":
        return _coerce_bool(value)
    return value


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    raise ValueError(f"Grid V2 cannot coerce {value!r} to bool.")


def _selector_values_by_variant(
    config: Mapping[str, Any],
    profile: ExecutionProfile,
) -> dict[str, Any]:
    selector = profile.variant_selector
    if selector is None:
        return {}
    params = _parameters(config)
    selector_spec = params.get(selector.param, {})
    param_type = str(selector_spec.get("type", "select")).strip().lower() if isinstance(selector_spec, Mapping) else "select"
    values: dict[str, Any] = {}
    for raw_key, variant_name in selector.mapping.items():
        if variant_name in values:
            continue
        values[variant_name] = _coerce_value(raw_key, param_type)
    missing = [name for name in profile.variants if name not in values]
    if missing:
        raise ValueError(f"Grid V2 selector mapping is missing variant(s): {missing}.")
    return values


def _selected_variants(profile: ExecutionProfile, settings: GridV2Settings) -> tuple[str, ...]:
    variant_order = tuple(profile.variants.keys())
    if settings.enabled_variants is None:
        return variant_order
    requested = tuple(settings.enabled_variants)
    unknown = sorted(set(requested) - set(variant_order))
    if unknown:
        raise ValueError(f"Grid V2 enabled_variants contains unknown variant(s): {unknown}.")
    return tuple(name for name in variant_order if name in set(requested))


def _ordered_active_names(profile: ExecutionProfile, params: Mapping[str, Any]) -> tuple[str, ...]:
    active = active_parameter_names(profile, params)
    return tuple(name for name in profile.parameter_names if name in active)


def _ordered_inactive_names(profile: ExecutionProfile, params: Mapping[str, Any]) -> tuple[str, ...]:
    inactive = inactive_parameter_names(profile, params)
    return tuple(name for name in profile.parameter_names if name in inactive)


def _variant_axis_names(
    *,
    profile: ExecutionProfile,
    domains: Mapping[str, GridV2ParameterDomain],
    active_names: Sequence[str],
    settings: GridV2Settings,
) -> tuple[str, ...]:
    active = set(active_names)
    names: list[str] = []
    for name in profile.parameter_names:
        domain = domains[name]
        if not domain.is_axis:
            continue
        if settings.include_inactive_axes_for_dedup or name in active:
            names.append(name)
    return tuple(names)


def _product_size(value_groups: Any) -> int:
    total = 1
    for values in value_groups:
        total *= len(values)
    return total


def _semantic_payload(
    *,
    config: Mapping[str, Any],
    profile: ExecutionProfile,
    variant: VariantSpec,
    params: Mapping[str, Any],
    active_names: Sequence[str],
) -> dict[str, Any]:
    active_params = {
        name: _jsonable_value(params[name])
        for name in active_names
        if name in params and profile.parameter_roles.get(name) != "runtime"
    }
    return {
        "engine": GRID_V2_ENGINE_VERSION,
        "strategy": {
            "id": str(config.get("id", profile.strategy_id)),
            "version": str(config.get("version", "")),
        },
        "variant": variant.name,
        "modes": {name: _jsonable_value(value) for name, value in sorted(variant.modes.items())},
        "params": active_params,
    }


def _canonical_identity(
    *,
    variant_name: str,
    params: Mapping[str, Any],
    names: Sequence[str],
) -> str:
    return _stable_json(
        {
            "variant": variant_name,
            "params": {name: _jsonable_value(params[name]) for name in names if name in params},
        }
    )


def _stable_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


def _jsonable_mapping(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _jsonable_value(value) for key, value in payload.items()}


def _jsonable_value(value: Any) -> Any:
    if isinstance(value, (bool, int, str)) or value is None:
        return value
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (float, np.floating)):
        parsed = float(value)
        if not math.isfinite(parsed):
            return str(parsed)
        return 0.0 if parsed == 0.0 else parsed
    if isinstance(value, pd.Timestamp):
        ts = value if value.tzinfo is not None else value.tz_localize("UTC")
        return ts.tz_convert("UTC").isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return _jsonable_value(float(value))
    if isinstance(value, (list, tuple)):
        return [_jsonable_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _jsonable_value(item) for key, item in value.items()}
    return value


def _optional_name_tuple(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    return tuple(str(item) for item in value)


def _coerce_hooks(hooks: GridV2StrategyHooks | Any) -> GridV2StrategyHooks:
    if isinstance(hooks, GridV2StrategyHooks):
        if hooks.function_fingerprint is not None:
            return hooks
        return GridV2StrategyHooks(
            build_execution_data=hooks.build_execution_data,
            normalize_params=hooks.normalize_params,
            label=hooks.label,
            signal_param_names=hooks.signal_param_names,
            dataprep_param_names=hooks.dataprep_param_names,
            function_fingerprint=_callable_fingerprint(hooks.build_execution_data),
        )
    return _coerce_hooks(GridV2StrategyHooks.from_strategy(hooks))


def _callable_fingerprint(func: Callable[..., Any]) -> str:
    module = getattr(func, "__module__", "")
    qualname = getattr(func, "__qualname__", "")
    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        source = ""
    return hashlib.blake2b(
        _stable_json({"module": module, "qualname": qualname, "source": source}).encode("utf-8"),
        digest_size=12,
    ).hexdigest()


def _data_fingerprint(df: pd.DataFrame) -> str:
    payload = {
        "shape": tuple(int(item) for item in df.shape),
        "columns": tuple(str(column) for column in df.columns),
        "dtypes": tuple(str(dtype) for dtype in df.dtypes),
        "first_index": _jsonable_value(df.index[0]) if len(df.index) else None,
        "last_index": _jsonable_value(df.index[-1]) if len(df.index) else None,
    }
    digest = hashlib.blake2b(_stable_json(payload).encode("utf-8"), digest_size=12)
    if not df.empty:
        hashed = pd.util.hash_pandas_object(df, index=True).to_numpy(dtype=np.uint64, copy=False)
        digest.update(np.ascontiguousarray(hashed).tobytes())
    return digest.hexdigest()


def _cache_param_names(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    hooks: GridV2StrategyHooks,
    *,
    signal_only: bool,
) -> tuple[str, ...]:
    if signal_only and hooks.signal_param_names is not None:
        return tuple(name for name in hooks.signal_param_names if name in candidate.params)
    if not signal_only and hooks.dataprep_param_names is not None:
        return tuple(name for name in hooks.dataprep_param_names if name in candidate.params)
    active = set(candidate.active_param_names)
    if signal_only:
        return tuple(
            name
            for name in plan.profile.parameter_names
            if name in active and plan.profile.parameter_roles.get(name) == "signal"
        )
    names: set[str] = set(_cache_param_names(plan, candidate, hooks, signal_only=True))
    for mode_field, mode_value in candidate.modes.items():
        binding = mode_binding_for(mode_field, mode_value)
        if binding is None or not binding.dataprep:
            continue
        names.update(name for name in binding.consumes_params if name in candidate.params)
    if not names:
        names.update(candidate.active_param_names)
    return tuple(name for name in plan.profile.parameter_names if name in names)


def _cache_key_payload(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks,
    *,
    signal_only: bool,
) -> dict[str, Any]:
    param_names = _cache_param_names(plan, candidate, hooks, signal_only=signal_only)
    return {
        "strategy_id": plan.strategy_id,
        "strategy_version": plan.strategy_version,
        "engine": GRID_V2_ENGINE_VERSION,
        "data": _data_fingerprint(df),
        "trade_start_idx": int(trade_start_idx),
        "function": hooks.function_fingerprint,
        "params": {
            name: _jsonable_value(candidate.params[name])
            for name in param_names
            if name in candidate.params
        },
    }


def _signal_cache_key(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks,
) -> str:
    return _stable_json(_cache_key_payload(plan, candidate, df, trade_start_idx, hooks, signal_only=True))


def _dataprep_cache_key(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks,
) -> str:
    payload = _cache_key_payload(plan, candidate, df, trade_start_idx, hooks, signal_only=False)
    payload["variant"] = candidate.variant_name
    payload["modes"] = dict(candidate.modes)
    return _stable_json(payload)


def _selected_candidates(
    plan: GridV2Plan,
    candidate_indices: Sequence[int] | None,
) -> tuple[GridV2Candidate, ...]:
    if candidate_indices is None:
        return plan.candidates
    selected: list[GridV2Candidate] = []
    for index in candidate_indices:
        idx = int(index)
        if idx < 0 or idx >= len(plan.candidates):
            raise IndexError(f"Grid V2 candidate index out of range: {idx}.")
        selected.append(plan.candidates[idx])
    return tuple(selected)


def _normalized_candidate_params(
    hooks: GridV2StrategyHooks,
    params: Mapping[str, Any],
) -> Mapping[str, Any]:
    if hooks.normalize_params is None:
        return params
    return hooks.normalize_params(dict(params))


def _row_from_run(candidate: GridV2Candidate, run: V2RunResult) -> GridV2ResultRow:
    result = run.strategy_result
    initial_balance = float(candidate.params.get("initialCapital", 100.0))
    core = compute_core_metrics_from_balance_and_trades(
        result.balance_curve,
        result.trades,
        initial_balance=initial_balance,
    )
    return GridV2ResultRow(
        candidate_id=candidate.candidate_id,
        semantic_key=candidate.semantic_key,
        variant_name=candidate.variant_name,
        modes=candidate.modes,
        params=candidate.params,
        net_profit_pct=core.net_profit_pct,
        max_drawdown_pct=core.max_drawdown_pct,
        romad=core.romad,
        profit_factor=core.profit_factor,
        win_rate_pct=core.win_rate_pct,
        total_trades=core.total_trades,
        final_balance=core.final_balance,
        guardrail_summary=_guardrail_mapping(run.guardrail_summary),
    )


def _error_row(candidate: GridV2Candidate, exc: Exception) -> GridV2ResultRow:
    return GridV2ResultRow(
        candidate_id=candidate.candidate_id,
        semantic_key=candidate.semantic_key,
        variant_name=candidate.variant_name,
        modes=candidate.modes,
        params=candidate.params,
        net_profit_pct=float("nan"),
        max_drawdown_pct=float("nan"),
        romad=float("nan"),
        profit_factor=float("nan"),
        win_rate_pct=float("nan"),
        total_trades=0,
        final_balance=float("nan"),
        guardrail_summary={},
        status="error",
        error=str(exc),
    )


def _guardrail_mapping(summary: GuardrailSummary) -> Mapping[str, Any]:
    if is_dataclass(summary):
        return _jsonable_mapping(asdict(summary))
    if isinstance(summary, Mapping):
        return _jsonable_mapping(summary)
    return {}


def _rank_rows(rows: Sequence[GridV2ResultRow], metric: str) -> list[GridV2ResultRow]:
    metric_name = "win_rate_pct" if metric == "win_rate" else metric
    minimize = metric_name in {"max_drawdown_pct"}

    def key(row: GridV2ResultRow) -> tuple[int, float, int]:
        if row.status != "ok":
            return (1, 0.0, row.candidate_id)
        value = float(getattr(row, metric_name, float("nan")))
        if not math.isfinite(value):
            return (1, 0.0, row.candidate_id)
        return (0, value if minimize else -value, row.candidate_id)

    return sorted(rows, key=key)


def _slow_enrich_selected(
    plan: GridV2Plan,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks,
    row: GridV2ResultRow,
    dataprep_cache: Mapping[str, ExecutionData],
) -> GridV2SelectedResult:
    if row.status != "ok":
        return GridV2SelectedResult(row=row, metrics={}, guardrail_summary={})
    candidate = plan.candidates[row.candidate_id - 1]
    data_key = _dataprep_cache_key(plan, candidate, df, trade_start_idx, hooks)
    data = dataprep_cache[data_key]
    run = run_v2_strategy(
        data=data,
        profile=plan.profile,
        params=_normalized_candidate_params(hooks, candidate.params),
        trade_start_idx=trade_start_idx,
    )
    strategy_result = run.strategy_result
    metrics = {
        "net_profit_pct": getattr(strategy_result, "net_profit_pct", None),
        "max_drawdown_pct": getattr(strategy_result, "max_drawdown_pct", None),
        "romad": getattr(strategy_result, "romad", None),
        "profit_factor": getattr(strategy_result, "profit_factor", None),
        "win_rate_pct": getattr(strategy_result, "win_rate", None),
        "total_trades": getattr(strategy_result, "total_trades", None),
        "sharpe_ratio": getattr(strategy_result, "sharpe_ratio", None),
        "sortino_ratio": getattr(strategy_result, "sortino_ratio", None),
        "sqn": getattr(strategy_result, "sqn", None),
        "ulcer_index": getattr(strategy_result, "ulcer_index", None),
        "consistency_score": getattr(strategy_result, "consistency_score", None),
    }
    return GridV2SelectedResult(
        row=row,
        metrics=_jsonable_mapping(metrics),
        guardrail_summary=_guardrail_mapping(run.guardrail_summary),
    )


__all__ = [
    "GRID_V2_ENGINE_VERSION",
    "REFERENCE_BATCH_KIND",
    "CandidateMappingRecord",
    "GridV2CacheEstimate",
    "GridV2CacheStats",
    "GridV2Candidate",
    "GridV2CountPreview",
    "GridV2ParameterDomain",
    "GridV2Plan",
    "GridV2ResultRow",
    "GridV2RunResult",
    "GridV2SelectedResult",
    "GridV2Settings",
    "GridV2StrategyHooks",
    "build_grid_v2_plan",
    "deterministic_candidate_subset_indices",
    "estimate_grid_v2_cache",
    "execute_grid_v2_candidates",
    "preview_grid_v2_counts",
    "run_grid_v2",
]
