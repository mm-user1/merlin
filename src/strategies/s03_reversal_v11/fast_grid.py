"""S03 Reversal v11 fast Grid backend.

The slow S03 strategy remains the source of truth.  This module provides a
Numba-backed scalar screening loop, deterministic S03 semantic candidate
generation, and exact slow-path validation for selected candidates.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import math
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import numba
except Exception as exc:  # pragma: no cover - exercised only without numba.
    numba = None
    NUMBA_IMPORT_ERROR = str(exc)
else:
    NUMBA_IMPORT_ERROR = None

try:
    from scipy.stats import qmc
except Exception:  # pragma: no cover - validation keeps evaluation from using it.
    qmc = None

from core.grid_engine import GridAllocation, format_compact_count, format_coverage_pct
from core.optuna_engine import OptimizationConfig, OptimizationResult, _run_single_combination
from indicators.ma import get_ma
from strategies import get_strategy_config
from strategies.s03_reversal_v11.strategy import S03ReversalV11

NUMBA_AVAILABLE = numba is not None

MODE_ORDER = ("cc_only", "tbands_only", "both")
MODE_LABELS = {
    "cc_only": "Close Count only",
    "tbands_only": "T Bands only",
    "both": "Both",
}
MODE_BOOLEANS = {
    "cc_only": {"useCloseCount": True, "useTBands": False},
    "tbands_only": {"useCloseCount": False, "useTBands": True},
    "both": {"useCloseCount": True, "useTBands": True},
}
MODE_AXES = {
    "cc_only": ("maType3", "maLength3", "maOffset3", "closeCountLong", "closeCountShort"),
    "tbands_only": ("maType3", "maLength3", "maOffset3", "tBandLongPct", "tBandShortPct"),
    "both": (
        "maType3",
        "maLength3",
        "maOffset3",
        "closeCountLong",
        "closeCountShort",
        "tBandLongPct",
        "tBandShortPct",
    ),
}
DEPENDENT_DEFAULTS = {
    "closeCountLong": 7,
    "closeCountShort": 5,
    "tBandLongPct": 1.0,
    "tBandShortPct": 1.3,
    "emergencySlPct": 20.0,
    "emergencySlUpdateBars": 16,
}
FLOAT_PARAMS = {
    "maOffset3",
    "tBandLongPct",
    "tBandShortPct",
    "contractSize",
    "initialCapital",
    "commissionPct",
    "emergencySlPct",
}
INT_PARAMS = {"maLength3", "closeCountLong", "closeCountShort", "emergencySlUpdateBars"}
BOOL_PARAMS = {"useCloseCount", "useTBands", "dateFilter", "useEmergencySL"}
MAX_AXIS_VALUES = 1_000_000


@dataclass
class GridParameterSpace:
    axes: Dict[str, List[Any]]
    fixed_values: Dict[str, Any]
    optimized_params: Dict[str, bool]
    mode_space_sizes: Dict[str, int]
    total_space_size: int
    allowed_modes: List[str]
    ma_types: List[str]
    ma_lengths: List[int]
    ma_offsets: List[float]


@dataclass
class GridCandidate:
    candidate_id: int
    mode: str
    params: Dict[str, Any]
    semantic_key: str
    generation_mode: str
    diversity_group: str


@dataclass
class CandidateSet:
    candidates: List[GridCandidate]
    diagnostics: Dict[str, Any]


@dataclass
class FastGridData:
    df: pd.DataFrame
    trade_start_idx: int
    close_values: np.ndarray
    open_values: np.ndarray
    high_values: np.ndarray
    low_values: np.ndarray
    month_ids: np.ndarray
    ma_cache: Dict[Tuple[str, int], np.ndarray]
    ma_cache_entries: int
    ma_cache_build_seconds: float
    ma_cache_estimated_mb: float


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
    return default


def _decimal_places(value: Any) -> int:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return 0
    exponent = dec.as_tuple().exponent
    return max(0, -int(exponent))


def _stable_round(value: Any, places: int = 12) -> float:
    numeric = float(value)
    rounded = round(numeric, places)
    if abs(rounded) < 10 ** (-(places - 1)):
        return 0.0
    return rounded


def _range_values(start: Any, stop: Any, step: Any, *, is_int: bool, name: str) -> List[Any]:
    try:
        from_value = float(start)
        to_value = float(stop)
        step_value = float(step)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid Grid range for '{name}'.")
    if not all(math.isfinite(v) for v in (from_value, to_value, step_value)):
        raise ValueError(f"Grid range for '{name}' must be finite.")
    if step_value <= 0:
        raise ValueError(f"Grid range step for '{name}' must be > 0.")
    if to_value < from_value:
        raise ValueError(f"Grid range end for '{name}' must be >= start.")

    n_float = (to_value - from_value) / step_value
    n = int(round(n_float)) + 1
    if n < 1:
        raise ValueError(f"Grid range for '{name}' is empty.")
    if n > MAX_AXIS_VALUES:
        raise ValueError(f"Grid range for '{name}' generates too many values ({n}).")

    places = max(_decimal_places(start), _decimal_places(stop), _decimal_places(step)) + 2
    values: List[Any] = []
    for idx in range(n):
        value = from_value + idx * step_value
        if abs(value - to_value) <= abs(step_value) * 1e-9:
            value = to_value
        if is_int:
            values.append(int(round(value)))
        else:
            values.append(_stable_round(value, places))
    return values


def _clean_ma_type(value: Any) -> str:
    return str(value or "").strip().upper()


def _param_default(strategy_params: Dict[str, Any], name: str, fallback: Any = None) -> Any:
    spec = strategy_params.get(name, {}) if isinstance(strategy_params, dict) else {}
    if isinstance(spec, dict) and "default" in spec:
        return spec.get("default")
    return fallback


def _fixed_value(config: OptimizationConfig, strategy_params: Dict[str, Any], name: str, fallback: Any = None) -> Any:
    fixed = dict(config.fixed_params or {})
    if name in fixed:
        return fixed[name]
    return _param_default(strategy_params, name, fallback)


def _select_options(config: OptimizationConfig, strategy_params: Dict[str, Any], name: str) -> List[Any]:
    fixed = dict(config.fixed_params or {})
    from_fixed = fixed.get(f"{name}_options")
    if isinstance(from_fixed, (list, tuple)) and from_fixed:
        return list(from_fixed)
    spec = strategy_params.get(name, {}) if isinstance(strategy_params, dict) else {}
    options = spec.get("options") if isinstance(spec, dict) else None
    if isinstance(options, (list, tuple)) and options:
        return list(options)
    return [_fixed_value(config, strategy_params, name)]


def _bool_domain(config: OptimizationConfig, strategy_params: Dict[str, Any], name: str) -> List[bool]:
    if not bool((config.enabled_params or {}).get(name, False)):
        return [_coerce_bool(_fixed_value(config, strategy_params, name), False)]
    raw = _select_options(config, strategy_params, name)
    values: List[bool] = []
    for item in raw:
        parsed = _coerce_bool(item, False)
        if parsed not in values:
            values.append(parsed)
    if not values:
        values = [True, False]
    return values


def _axis_values(config: OptimizationConfig, strategy_params: Dict[str, Any], name: str) -> List[Any]:
    enabled = bool((config.enabled_params or {}).get(name, False))
    param_type = str((config.param_types or {}).get(name, "")).strip().lower()
    if not enabled:
        value = _fixed_value(config, strategy_params, name, DEPENDENT_DEFAULTS.get(name))
        if name in INT_PARAMS:
            return [int(float(value))]
        if name in FLOAT_PARAMS:
            return [float(value)]
        if name in BOOL_PARAMS:
            return [_coerce_bool(value, False)]
        return [value]

    if param_type in {"select", "options"}:
        if name == "maType3":
            values = []
            for item in _select_options(config, strategy_params, name):
                cleaned = _clean_ma_type(item)
                if cleaned and cleaned not in values:
                    values.append(cleaned)
            return values
        values = []
        for item in _select_options(config, strategy_params, name):
            if item not in values:
                values.append(item)
        return values

    if param_type in {"bool", "boolean"}:
        return _bool_domain(config, strategy_params, name)

    range_spec = (config.param_ranges or {}).get(name)
    spec = strategy_params.get(name, {}) if isinstance(strategy_params, dict) else {}
    opt_spec = spec.get("optimize", {}) if isinstance(spec.get("optimize", {}), dict) else {}
    if range_spec:
        start, stop, step = range_spec
    else:
        start = opt_spec.get("min", spec.get("min", _fixed_value(config, strategy_params, name, 0)))
        stop = opt_spec.get("max", spec.get("max", start))
        step = opt_spec.get("step", spec.get("step", 1))
    return _range_values(start, stop, step, is_int=name in INT_PARAMS or param_type == "int", name=name)


def _stable_float(value: Any) -> float:
    return _stable_round(value, 10)


def _canonical_param_value(name: str, value: Any) -> Any:
    if name == "maType3":
        return _clean_ma_type(value)
    if name in INT_PARAMS:
        return int(float(value))
    if name in FLOAT_PARAMS:
        return _stable_float(value)
    if name in BOOL_PARAMS:
        return _coerce_bool(value, False)
    return value


def _semantic_payload(mode: str, params: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "mode": mode,
        "maType3": _canonical_param_value("maType3", params.get("maType3")),
        "maLength3": _canonical_param_value("maLength3", params.get("maLength3")),
        "maOffset3": _canonical_param_value("maOffset3", params.get("maOffset3", 0.0)),
    }
    if mode in {"cc_only", "both"}:
        payload["closeCountLong"] = _canonical_param_value("closeCountLong", params.get("closeCountLong"))
        payload["closeCountShort"] = _canonical_param_value("closeCountShort", params.get("closeCountShort"))
    if mode in {"tbands_only", "both"}:
        payload["tBandLongPct"] = _canonical_param_value("tBandLongPct", params.get("tBandLongPct"))
        payload["tBandShortPct"] = _canonical_param_value("tBandShortPct", params.get("tBandShortPct"))
    if _coerce_bool(params.get("useEmergencySL"), False):
        payload["useEmergencySL"] = True
        payload["emergencySlPct"] = _canonical_param_value("emergencySlPct", params.get("emergencySlPct", 20.0))
        payload["emergencySlUpdateBars"] = _canonical_param_value(
            "emergencySlUpdateBars",
            params.get("emergencySlUpdateBars", 16),
        )
    return payload


def candidate_semantic_key(mode: str, params: Dict[str, Any]) -> str:
    return json.dumps(_semantic_payload(mode, params), sort_keys=True, separators=(",", ":"))


def _mode_axis_names(space: GridParameterSpace, mode: str) -> List[str]:
    names = []
    for name in MODE_AXES[mode]:
        if name == "maOffset3" and not space.optimized_params.get("maOffset3", False):
            continue
        names.append(name)
    if _coerce_bool(space.fixed_values.get("useEmergencySL"), False):
        for name in ("emergencySlPct", "emergencySlUpdateBars"):
            if space.optimized_params.get(name, False):
                names.append(name)
    return names


def _mode_space_size(space: GridParameterSpace, mode: str) -> int:
    if mode not in space.allowed_modes:
        return 0
    size = 1
    for name in _mode_axis_names(space, mode):
        size *= len(space.axes.get(name, []))
    return int(size)


def build_parameter_space(config: OptimizationConfig) -> GridParameterSpace:
    strategy_config = get_strategy_config(config.strategy_id)
    strategy_params = strategy_config.get("parameters", {}) if isinstance(strategy_config, dict) else {}

    fixed_values: Dict[str, Any] = {
        name: _fixed_value(config, strategy_params, name, DEPENDENT_DEFAULTS.get(name))
        for name in strategy_params.keys()
    }
    fixed_values.update({k: v for k, v in (config.fixed_params or {}).items() if not str(k).endswith("_options")})

    axes: Dict[str, List[Any]] = {}
    for name in (
        "maType3",
        "maLength3",
        "maOffset3",
        "closeCountLong",
        "closeCountShort",
        "tBandLongPct",
        "tBandShortPct",
        "emergencySlPct",
        "emergencySlUpdateBars",
    ):
        axes[name] = [_canonical_param_value(name, value) for value in _axis_values(config, strategy_params, name)]
        axes[name] = list(dict.fromkeys(axes[name]))
        if not axes[name]:
            raise ValueError(f"Grid parameter axis '{name}' is empty.")

    ma_types = [_clean_ma_type(value) for value in axes["maType3"] if _clean_ma_type(value)]
    if not ma_types:
        raise ValueError("Grid requires at least one MA type.")
    if any(value == "VWAP" for value in ma_types):
        raise ValueError("VWAP is not supported in S03 Grid mode. Disable VWAP for Grid.")
    axes["maType3"] = ma_types

    close_domain = _bool_domain(config, strategy_params, "useCloseCount")
    tbands_domain = _bool_domain(config, strategy_params, "useTBands")
    allowed_modes: List[str] = []
    if True in close_domain and False in tbands_domain:
        allowed_modes.append("cc_only")
    if False in close_domain and True in tbands_domain:
        allowed_modes.append("tbands_only")
    if True in close_domain and True in tbands_domain:
        allowed_modes.append("both")
    if not allowed_modes:
        raise ValueError("Grid parameter space is empty: useCloseCount=false and useTBands=false is invalid.")

    optimized = {name: bool((config.enabled_params or {}).get(name, False)) for name in axes}
    # Fixed maOffset3 still participates in semantic keys, but it must not
    # multiply the space unless explicitly enabled for optimization.
    if not optimized.get("maOffset3", False):
        fixed_values["maOffset3"] = _canonical_param_value("maOffset3", axes["maOffset3"][0])
    fixed_values["useEmergencySL"] = _coerce_bool(fixed_values.get("useEmergencySL"), False)
    if not optimized.get("emergencySlPct", False):
        fixed_values["emergencySlPct"] = _canonical_param_value("emergencySlPct", axes["emergencySlPct"][0])
    if not optimized.get("emergencySlUpdateBars", False):
        fixed_values["emergencySlUpdateBars"] = _canonical_param_value(
            "emergencySlUpdateBars",
            axes["emergencySlUpdateBars"][0],
        )

    placeholder = GridParameterSpace(
        axes=axes,
        fixed_values=fixed_values,
        optimized_params=optimized,
        mode_space_sizes={mode: 0 for mode in MODE_ORDER},
        total_space_size=0,
        allowed_modes=allowed_modes,
        ma_types=ma_types,
        ma_lengths=[int(v) for v in axes["maLength3"]],
        ma_offsets=[float(v) for v in axes["maOffset3"]],
    )
    sizes = {mode: _mode_space_size(placeholder, mode) for mode in MODE_ORDER}
    placeholder.mode_space_sizes = sizes
    placeholder.total_space_size = sum(sizes.values())
    if placeholder.total_space_size <= 0:
        raise ValueError("Grid parameter space is empty.")
    return placeholder


def build_preview(space: GridParameterSpace, allocation: GridAllocation) -> Dict[str, Any]:
    total_space = int(space.total_space_size)
    coverage = (allocation.actual_budget / total_space * 100.0) if total_space else 0.0
    rows = []
    for mode in MODE_ORDER:
        mode_space = int(space.mode_space_sizes.get(mode, 0) or 0)
        budget = int(allocation.mode_budgets.get(mode, 0) or 0)
        mode_coverage = float(allocation.mode_coverage_pct.get(mode, 0.0) or 0.0)
        generation = "Disabled"
        if mode_space > 0:
            generation = "Full" if budget >= mode_space else ("Seeded subset" if budget / mode_space >= 0.5 else "LHS")
        rows.append(
            {
                "mode": mode,
                "label": MODE_LABELS[mode],
                "space_size": mode_space,
                "space_label": format_compact_count(mode_space),
                "budget": budget,
                "budget_label": format_compact_count(budget),
                "coverage_pct": mode_coverage,
                "coverage_label": format_coverage_pct(mode_coverage),
                "generation": generation,
            }
        )
    return {
        "total_space": total_space,
        "total_space_label": format_compact_count(total_space),
        "requested_budget": allocation.requested_budget,
        "requested_budget_label": format_compact_count(allocation.requested_budget),
        "actual_budget": allocation.actual_budget,
        "actual_budget_label": format_compact_count(allocation.actual_budget),
        "coverage_pct": coverage,
        "coverage_label": format_coverage_pct(coverage),
        "modes": rows,
        "allocation_method": allocation.allocation_method,
    }


def _hydrate_params(space: GridParameterSpace, mode: str, active_values: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(space.fixed_values)
    params.update(MODE_BOOLEANS[mode])
    params.setdefault("maOffset3", 0.0)
    for name, value in active_values.items():
        params[name] = _canonical_param_value(name, value)
    for name, default in DEPENDENT_DEFAULTS.items():
        params.setdefault(name, default)
    params.setdefault("contractSize", 0.01)
    params.setdefault("useEmergencySL", False)
    params.setdefault("emergencySlPct", 20.0)
    params.setdefault("emergencySlUpdateBars", 16)
    params.setdefault("initialCapital", 100.0)
    params.setdefault("commissionPct", 0.05)
    return params


def hydrate_params(candidate: GridCandidate) -> Dict[str, Any]:
    return dict(candidate.params)


def _candidate_from_active_values(
    space: GridParameterSpace,
    mode: str,
    active_values: Dict[str, Any],
    generation_mode: str,
) -> GridCandidate:
    params = _hydrate_params(space, mode, active_values)
    semantic_key = candidate_semantic_key(mode, params)
    group = f"{mode}|{_clean_ma_type(params.get('maType3'))}|{int(params.get('maLength3'))}"
    return GridCandidate(
        candidate_id=0,
        mode=mode,
        params=params,
        semantic_key=semantic_key,
        generation_mode=generation_mode,
        diversity_group=group,
    )


def _iter_mode_candidates(space: GridParameterSpace, mode: str, generation_mode: str) -> Iterable[GridCandidate]:
    axis_names = _mode_axis_names(space, mode)
    axis_values = [space.axes[name] for name in axis_names]
    for combo in itertools.product(*axis_values):
        yield _candidate_from_active_values(
            space,
            mode,
            dict(zip(axis_names, combo)),
            generation_mode,
        )


def _seeded_hash_order(seed: int, key: str) -> str:
    payload = f"{seed}:{key}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _seeded_subset(space: GridParameterSpace, mode: str, count: int, seed: int) -> List[GridCandidate]:
    all_candidates = list(_iter_mode_candidates(space, mode, "seeded_subset"))
    all_candidates.sort(key=lambda item: (_seeded_hash_order(seed, item.semantic_key), item.semantic_key))
    return all_candidates[:count]


def _lhs_candidates(space: GridParameterSpace, mode: str, count: int, seed: int) -> Tuple[List[GridCandidate], Dict[str, Any]]:
    if qmc is None:
        raise RuntimeError("scipy.stats.qmc is required for Grid LHS sampling.")

    axis_names = _mode_axis_names(space, mode)
    axis_values = [space.axes[name] for name in axis_names]
    dims = len(axis_names)
    selected: Dict[str, GridCandidate] = {}
    refill_batches = 0
    low_yield_streak = 0
    target = int(count)
    batch_size = max(target, 16)

    while len(selected) < target and refill_batches < 100:
        sampler = qmc.LatinHypercube(
            d=dims,
            scramble=True,
            seed=int(seed) + refill_batches,
        )
        sample_count = min(batch_size, max(target - len(selected), 1) * 2)
        sample = sampler.random(sample_count)
        before = len(selected)
        for row in sample:
            active: Dict[str, Any] = {}
            for idx, value in enumerate(row):
                values = axis_values[idx]
                level_idx = int(math.floor(float(value) * len(values)))
                if level_idx >= len(values):
                    level_idx = len(values) - 1
                active[axis_names[idx]] = values[level_idx]
            candidate = _candidate_from_active_values(space, mode, active, "lhs")
            selected.setdefault(candidate.semantic_key, candidate)
            if len(selected) >= target:
                break
        added = len(selected) - before
        remaining = max(1, target - before)
        if added < max(1, int(math.ceil(remaining * 0.01))):
            low_yield_streak += 1
        else:
            low_yield_streak = 0
        refill_batches += 1
        if low_yield_streak >= 5:
            break

    fallback_used = False
    if len(selected) < target:
        fallback_used = True
        for candidate in _iter_mode_candidates(space, mode, "fallback_enumeration"):
            if candidate.semantic_key in selected:
                continue
            selected[candidate.semantic_key] = candidate
            if len(selected) >= target:
                break

    ordered = list(selected.values())
    ordered.sort(key=lambda item: item.semantic_key)
    return ordered[:target], {
        "refill_batch_count": refill_batches,
        "fallback_enumeration": fallback_used,
        "dedupe_count": max(0, refill_batches * batch_size - len(selected)) if refill_batches else 0,
    }


def generate_candidates(
    config: OptimizationConfig,
    space: GridParameterSpace,
    allocation: GridAllocation,
    *,
    seed: int,
) -> CandidateSet:
    del config
    candidates: List[GridCandidate] = []
    diagnostics: Dict[str, Any] = {
        "target_candidate_count": allocation.actual_budget,
        "mode_space_sizes": dict(allocation.mode_space_sizes),
        "target_mode_counts": dict(allocation.mode_budgets),
        "target_mode_quotas": dict(allocation.target_mode_quotas),
        "actual_mode_counts": {mode: 0 for mode in MODE_ORDER},
        "dedupe_count": 0,
        "refill_batch_count": 0,
        "enumerated_full_modes": [],
        "lhs_modes": [],
        "seeded_subset_modes": [],
        "fallback_enumeration_modes": [],
        "sampling_seed": int(seed),
        "qmc_optimization": None,
    }

    seen: set[str] = set()
    for mode_idx, mode in enumerate(MODE_ORDER):
        budget = int(allocation.mode_budgets.get(mode, 0) or 0)
        mode_space = int(space.mode_space_sizes.get(mode, 0) or 0)
        if budget <= 0 or mode_space <= 0:
            continue

        mode_seed = int(seed) + mode_idx * 100_000
        if budget >= mode_space:
            mode_candidates = list(_iter_mode_candidates(space, mode, "full"))
            diagnostics["enumerated_full_modes"].append(mode)
        elif budget / mode_space >= 0.5:
            mode_candidates = _seeded_subset(space, mode, budget, mode_seed)
            diagnostics["seeded_subset_modes"].append(mode)
        else:
            mode_candidates, lhs_diag = _lhs_candidates(space, mode, budget, mode_seed)
            diagnostics["lhs_modes"].append(mode)
            diagnostics["refill_batch_count"] += int(lhs_diag.get("refill_batch_count", 0))
            diagnostics["dedupe_count"] += int(lhs_diag.get("dedupe_count", 0))
            if lhs_diag.get("fallback_enumeration"):
                diagnostics["fallback_enumeration_modes"].append(mode)

        for candidate in mode_candidates:
            if candidate.semantic_key in seen:
                diagnostics["dedupe_count"] += 1
                continue
            seen.add(candidate.semantic_key)
            candidates.append(candidate)
            diagnostics["actual_mode_counts"][mode] += 1

    candidates.sort(key=lambda item: (_mode_sort_key(item.mode), item.semantic_key))
    for idx, candidate in enumerate(candidates, 1):
        candidate.candidate_id = idx

    diagnostics["actual_candidate_count"] = len(candidates)
    diagnostics["actual_mode_quotas"] = {
        mode: (diagnostics["actual_mode_counts"][mode] / len(candidates)) if candidates else 0.0
        for mode in MODE_ORDER
    }
    return CandidateSet(candidates=candidates, diagnostics=diagnostics)


def _mode_sort_key(mode: str) -> int:
    try:
        return MODE_ORDER.index(mode)
    except ValueError:
        return len(MODE_ORDER)


def prepare_fast_data(
    df: pd.DataFrame,
    trade_start_idx: int,
    candidates: Sequence[GridCandidate],
) -> FastGridData:
    started = time.time()
    close = df["Close"]
    open_ = df["Open"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]
    required_ma = sorted(
        {
            (_clean_ma_type(candidate.params.get("maType3")), int(candidate.params.get("maLength3")))
            for candidate in candidates
        }
    )
    ma_cache: Dict[Tuple[str, int], np.ndarray] = {}
    for ma_type, ma_length in required_ma:
        series = get_ma(close, ma_type, ma_length, volume, high, low)
        ma_cache[(ma_type, ma_length)] = np.ascontiguousarray(series.to_numpy(copy=False), dtype=np.float64)
    estimated_mb = sum(array.nbytes for array in ma_cache.values()) / (1024.0 * 1024.0)
    return FastGridData(
        df=df,
        trade_start_idx=int(trade_start_idx),
        close_values=np.ascontiguousarray(close.to_numpy(copy=False), dtype=np.float64),
        open_values=np.ascontiguousarray(open_.to_numpy(copy=False), dtype=np.float64),
        high_values=np.ascontiguousarray(high.to_numpy(copy=False), dtype=np.float64),
        low_values=np.ascontiguousarray(low.to_numpy(copy=False), dtype=np.float64),
        month_ids=np.ascontiguousarray(
            np.asarray((df.index.year * 12) + df.index.month, dtype=np.int64)
        ),
        ma_cache=ma_cache,
        ma_cache_entries=len(ma_cache),
        ma_cache_build_seconds=time.time() - started,
        ma_cache_estimated_mb=estimated_mb,
    )


def legacy_recovered_max_drawdown_pct(balance_values: Sequence[float]) -> float:
    """Match Merlin's current slow drawdown boundary behavior."""
    values = [] if balance_values is None else balance_values
    running_peak = None
    period_max_drawdown_pct = 0.0
    max_drawdown_pct = 0.0
    last_boundary_index = -1

    for index, raw_value in enumerate(values):
        try:
            balance = float(raw_value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(balance):
            continue
        if running_peak is None:
            running_peak = balance
            continue
        if balance >= running_peak:
            if index > last_boundary_index + 1 and period_max_drawdown_pct > max_drawdown_pct:
                max_drawdown_pct = period_max_drawdown_pct
            running_peak = balance
            period_max_drawdown_pct = 0.0
            last_boundary_index = index
        elif running_peak > 0.0:
            drawdown_pct = (1.0 - balance / running_peak) * 100.0
            if drawdown_pct > period_max_drawdown_pct:
                period_max_drawdown_pct = drawdown_pct

    final_index = len(values) - 1
    if final_index > last_boundary_index + 1 and period_max_drawdown_pct > max_drawdown_pct:
        max_drawdown_pct = period_max_drawdown_pct
    return max_drawdown_pct


def _s03_fast_loop_impl(
    close_values: np.ndarray,
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    month_ids: np.ndarray,
    ma_values: np.ndarray,
    trade_start_idx: int,
    date_filter: bool,
    ma_offset: float,
    use_close_count: bool,
    use_tbands: bool,
    close_count_long: int,
    close_count_short: int,
    t_band_long_pct: float,
    t_band_short_pct: float,
    use_emergency_sl: bool,
    emergency_sl_pct: float,
    emergency_sl_update_bars: int,
    contract_size: float,
    initial_capital: float,
    commission_pct: float,
    compute_dsr: bool,
    risk_free_rate: float,
) -> Tuple[float, float, int, int, int, float, float, float, float, float, float, float, int, float, int, float, float]:
    n = close_values.shape[0]
    if n == 0:
        return (0.0, 0.0, 0, 0, 0, 0.0, 0.0, math.nan, 0.0, 0.0, 0.0, 0.0, 0, math.nan, 0, math.nan, math.nan)

    ma_multiplier = 1.0 + ma_offset / 100.0
    up_multiplier = 1.0 + t_band_long_pct / 100.0
    down_multiplier = 1.0 - t_band_short_pct / 100.0
    commission_rate = commission_pct / 100.0
    if emergency_sl_update_bars < 1:
        emergency_sl_update_bars = 1

    balance = initial_capital
    running_max_balance = balance
    max_drawdown_pct = 0.0
    current_drawdown_pct = 0.0
    last_drawdown_boundary_index = -1

    position = 0
    prev_position = 0
    position_size = 0.0
    entry_price = math.nan
    entry_commission = 0.0
    emergency_sl_price = math.nan
    emergency_sl_entry_index = -1
    emergency_sl_bars = 0

    t_band_state = 0
    count_close_long = 0
    count_close_short = 0
    trading_disabled = not (use_close_count or use_tbands)
    last_bar_index = n - 1

    total_trades = 0
    winning_trades = 0
    losing_trades = 0
    gross_profit = 0.0
    gross_loss = 0.0
    max_consecutive_losses = 0
    consecutive_losses = 0
    current_month = -1
    month_start_equity = 0.0
    last_equity = initial_capital
    monthly_count = 0
    monthly_sum = 0.0
    monthly_sumsq = 0.0
    monthly_sum3 = 0.0
    monthly_sum4 = 0.0

    for i in range(n):
        close_val = close_values[i]
        open_val = open_values[i]
        high_val = high_values[i]
        low_val = low_values[i]
        ma_raw = ma_values[i]

        ma_val = math.nan
        up_band = math.nan
        down_band = math.nan
        if not math.isnan(ma_raw):
            ma_val = ma_raw * ma_multiplier
            up_band = ma_val * up_multiplier
            down_band = ma_val * down_multiplier

        break_up = False
        break_down = False
        cross_fail = False
        if not math.isnan(up_band) and not math.isnan(down_band):
            break_up = (high_val > up_band) and (close_val > up_band)
            break_down = (low_val < down_band) and (close_val < down_band)
            cross_fail = (high_val >= up_band) and (low_val <= down_band)

        if cross_fail:
            if not math.isnan(ma_val):
                t_band_state = 1 if close_val > ma_val else -1
        else:
            if break_up:
                t_band_state = 1
            elif break_down:
                t_band_state = -1

        if not math.isnan(ma_val):
            if close_val > ma_val:
                count_close_long += 1
                count_close_short = 0
            elif close_val < ma_val:
                count_close_short += 1
                count_close_long = 0
            else:
                count_close_long = 0
                count_close_short = 0
        else:
            count_close_long = 0
            count_close_short = 0

        count_long = True if not use_close_count else count_close_long >= close_count_long
        count_short = True if not use_close_count else count_close_short >= close_count_short
        cross_tband_long = True if not use_tbands else t_band_state == 1
        cross_tband_short = True if not use_tbands else t_band_state == -1
        in_range = True
        if date_filter:
            in_range = i >= trade_start_idx

        long_conditions = (not trading_disabled) and in_range and count_long and cross_tband_long
        short_conditions = (not trading_disabled) and in_range and count_short and cross_tband_short

        emergency_exit = False
        if (
            use_emergency_sl
            and position != 0
            and emergency_sl_entry_index >= 0
            and i >= emergency_sl_entry_index + 2
            and not math.isnan(emergency_sl_price)
        ):
            emergency_exit_price = math.nan
            if position > 0 and low_val <= emergency_sl_price:
                emergency_exit_price = min(open_val, emergency_sl_price)
            elif position < 0 and high_val >= emergency_sl_price:
                emergency_exit_price = max(open_val, emergency_sl_price)
            if not math.isnan(emergency_exit_price):
                exit_commission = emergency_exit_price * position_size * commission_rate
                gross_pnl = (
                    (emergency_exit_price - entry_price) * position_size
                    if position > 0
                    else (entry_price - emergency_exit_price) * position_size
                )
                net_pnl = gross_pnl - exit_commission - entry_commission
                balance += net_pnl
                total_trades += 1
                if net_pnl > 0.0:
                    winning_trades += 1
                    gross_profit += net_pnl
                    consecutive_losses = 0
                else:
                    if net_pnl < 0.0:
                        losing_trades += 1
                        gross_loss += abs(net_pnl)
                    consecutive_losses += 1
                    if consecutive_losses > max_consecutive_losses:
                        max_consecutive_losses = consecutive_losses
                position = 0
                position_size = 0.0
                entry_price = math.nan
                entry_commission = 0.0
                emergency_sl_price = math.nan
                emergency_sl_entry_index = -1
                emergency_sl_bars = 0
                emergency_exit = True

        if not emergency_exit:
            if position > 0:
                if short_conditions or not in_range:
                    exit_commission = close_val * position_size * commission_rate
                    gross_pnl = (close_val - entry_price) * position_size
                    net_pnl = gross_pnl - exit_commission - entry_commission
                    balance += net_pnl
                    total_trades += 1
                    if net_pnl > 0.0:
                        winning_trades += 1
                        gross_profit += net_pnl
                        consecutive_losses = 0
                    else:
                        if net_pnl < 0.0:
                            losing_trades += 1
                            gross_loss += abs(net_pnl)
                        consecutive_losses += 1
                        if consecutive_losses > max_consecutive_losses:
                            max_consecutive_losses = consecutive_losses
                    position = 0
                    position_size = 0.0
                    entry_price = math.nan
                    entry_commission = 0.0
                    emergency_sl_price = math.nan
                    emergency_sl_entry_index = -1
                    emergency_sl_bars = 0
            elif position < 0:
                if long_conditions or not in_range:
                    exit_commission = close_val * position_size * commission_rate
                    gross_pnl = (entry_price - close_val) * position_size
                    net_pnl = gross_pnl - exit_commission - entry_commission
                    balance += net_pnl
                    total_trades += 1
                    if net_pnl > 0.0:
                        winning_trades += 1
                        gross_profit += net_pnl
                        consecutive_losses = 0
                    else:
                        if net_pnl < 0.0:
                            losing_trades += 1
                            gross_loss += abs(net_pnl)
                        consecutive_losses += 1
                        if consecutive_losses > max_consecutive_losses:
                            max_consecutive_losses = consecutive_losses
                    position = 0
                    position_size = 0.0
                    entry_price = math.nan
                    entry_commission = 0.0
                    emergency_sl_price = math.nan
                    emergency_sl_entry_index = -1
                    emergency_sl_bars = 0

        if (
            use_emergency_sl
            and position != 0
            and emergency_sl_entry_index >= 0
            and i >= emergency_sl_entry_index + 2
            and not math.isnan(emergency_sl_price)
        ):
            emergency_sl_bars += 1
            if emergency_sl_bars >= emergency_sl_update_bars:
                if position > 0:
                    candidate_sl = close_val * (1.0 - emergency_sl_pct / 100.0)
                    if candidate_sl > emergency_sl_price:
                        emergency_sl_price = candidate_sl
                else:
                    candidate_sl = close_val * (1.0 + emergency_sl_pct / 100.0)
                    if candidate_sl < emergency_sl_price:
                        emergency_sl_price = candidate_sl
                emergency_sl_bars = 0

        if (
            (not trading_disabled)
            and in_range
            and position == 0
            and (prev_position == 0 or emergency_exit)
            and close_val > 0.0
            and contract_size > 0.0
        ):
            size = math.floor((balance / close_val) / contract_size) * contract_size
            if size > 0.0:
                if long_conditions:
                    position = 1
                    position_size = size
                    entry_price = close_val
                    entry_commission = entry_price * position_size * commission_rate
                    if use_emergency_sl:
                        emergency_sl_price = entry_price * (1.0 - emergency_sl_pct / 100.0)
                        emergency_sl_entry_index = i
                        emergency_sl_bars = 0
                elif short_conditions:
                    position = -1
                    position_size = size
                    entry_price = close_val
                    entry_commission = entry_price * position_size * commission_rate
                    if use_emergency_sl:
                        emergency_sl_price = entry_price * (1.0 + emergency_sl_pct / 100.0)
                        emergency_sl_entry_index = i
                        emergency_sl_bars = 0

        if i == last_bar_index and position != 0:
            gross_pnl = (close_val - entry_price) * position_size if position > 0 else (entry_price - close_val) * position_size
            exit_commission = close_val * position_size * commission_rate
            net_pnl = gross_pnl - exit_commission - entry_commission
            balance += net_pnl
            total_trades += 1
            if net_pnl > 0.0:
                winning_trades += 1
                gross_profit += net_pnl
                consecutive_losses = 0
            else:
                if net_pnl < 0.0:
                    losing_trades += 1
                    gross_loss += abs(net_pnl)
                consecutive_losses += 1
                if consecutive_losses > max_consecutive_losses:
                    max_consecutive_losses = consecutive_losses
            position = 0
            position_size = 0.0
            entry_price = math.nan
            entry_commission = 0.0
            emergency_sl_price = math.nan
            emergency_sl_entry_index = -1
            emergency_sl_bars = 0

        unrealized = 0.0
        if position > 0 and not math.isnan(entry_price):
            unrealized = (close_val - entry_price) * position_size
        elif position < 0 and not math.isnan(entry_price):
            unrealized = (entry_price - close_val) * position_size
        equity_value = balance + unrealized
        last_equity = equity_value

        if compute_dsr:
            month_key = month_ids[i]
            if current_month < 0:
                current_month = month_key
                month_start_equity = equity_value
            elif month_key != current_month:
                if month_start_equity > 0.0:
                    monthly_return = ((equity_value / month_start_equity) - 1.0) * 100.0
                    monthly_count += 1
                    monthly_sum += monthly_return
                    monthly_sumsq += monthly_return * monthly_return
                    monthly_sum3 += monthly_return * monthly_return * monthly_return
                    monthly_sum4 += monthly_return * monthly_return * monthly_return * monthly_return
                current_month = month_key
                month_start_equity = equity_value

        if balance >= running_max_balance:
            if i > last_drawdown_boundary_index + 1 and current_drawdown_pct > max_drawdown_pct:
                max_drawdown_pct = current_drawdown_pct
            running_max_balance = balance
            current_drawdown_pct = 0.0
            last_drawdown_boundary_index = i
        elif running_max_balance > 0.0:
            drawdown_pct = (1.0 - balance / running_max_balance) * 100.0
            if drawdown_pct > current_drawdown_pct:
                current_drawdown_pct = drawdown_pct

        prev_position = position

    if last_bar_index > last_drawdown_boundary_index + 1 and current_drawdown_pct > max_drawdown_pct:
        max_drawdown_pct = current_drawdown_pct

    net_profit = balance - initial_capital
    net_profit_pct = (net_profit / initial_capital * 100.0) if initial_capital != 0.0 else 0.0
    win_rate = (winning_trades / total_trades * 100.0) if total_trades > 0 else 0.0
    avg_win = (gross_profit / winning_trades) if winning_trades > 0 else 0.0
    avg_loss = (gross_loss / losing_trades) if losing_trades > 0 else 0.0
    avg_trade = (net_profit / total_trades) if total_trades > 0 else 0.0
    if total_trades == 0:
        profit_factor = math.nan
    elif gross_loss > 0.0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0.0:
        profit_factor = math.inf
    else:
        profit_factor = 1.0

    if max_drawdown_pct >= 0.0:
        if abs(max_drawdown_pct) < 1e-9:
            romad = net_profit_pct * 100.0 if net_profit_pct >= 0.0 else 0.0
        elif max_drawdown_pct != 0.0:
            romad = net_profit_pct / abs(max_drawdown_pct)
        else:
            romad = 0.0
    else:
        romad = 0.0

    sharpe_ratio = math.nan
    dsr_skewness = math.nan
    dsr_kurtosis = math.nan
    if compute_dsr and total_trades > 0:
        if month_start_equity > 0.0:
            monthly_return = ((last_equity / month_start_equity) - 1.0) * 100.0
            monthly_count += 1
            monthly_sum += monthly_return
            monthly_sumsq += monthly_return * monthly_return
            monthly_sum3 += monthly_return * monthly_return * monthly_return
            monthly_sum4 += monthly_return * monthly_return * monthly_return * monthly_return
        if monthly_count >= 2:
            avg_return = monthly_sum / monthly_count
            variance = (monthly_sumsq / monthly_count) - (avg_return * avg_return)
            if variance < 0.0 and variance > -1e-12:
                variance = 0.0
            if variance > 0.0:
                sd_return = math.sqrt(variance)
                rfr_monthly = (risk_free_rate * 100.0) / 12.0
                sharpe_ratio = (avg_return - rfr_monthly) / sd_return
                if monthly_count >= 3:
                    raw_m2 = monthly_sumsq / monthly_count
                    raw_m3 = monthly_sum3 / monthly_count
                    raw_m4 = monthly_sum4 / monthly_count
                    central_m3 = raw_m3 - (3.0 * avg_return * raw_m2) + (2.0 * avg_return * avg_return * avg_return)
                    central_m4 = (
                        raw_m4
                        - (4.0 * avg_return * raw_m3)
                        + (6.0 * avg_return * avg_return * raw_m2)
                        - (3.0 * avg_return * avg_return * avg_return * avg_return)
                    )
                    dsr_skewness = central_m3 / (sd_return * sd_return * sd_return)
                    dsr_kurtosis = central_m4 / (variance * variance)

    return (
        net_profit_pct,
        max_drawdown_pct,
        total_trades,
        winning_trades,
        losing_trades,
        win_rate,
        gross_profit,
        gross_loss,
        profit_factor,
        romad,
        avg_win,
        avg_loss,
        max_consecutive_losses,
        sharpe_ratio,
        monthly_count,
        dsr_skewness,
        dsr_kurtosis,
    )


if NUMBA_AVAILABLE:
    _S03_FAST_LOOP = numba.njit(cache=True)(_s03_fast_loop_impl)
else:  # pragma: no cover
    _S03_FAST_LOOP = None


def _s03_fast_batch_loop_impl(
    close_values: np.ndarray,
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    month_ids: np.ndarray,
    ma_stack: np.ndarray,
    ma_indices: np.ndarray,
    trade_start_idx: int,
    date_filters: np.ndarray,
    ma_offsets: np.ndarray,
    use_close_counts: np.ndarray,
    use_tbands_values: np.ndarray,
    close_count_longs: np.ndarray,
    close_count_shorts: np.ndarray,
    t_band_long_pcts: np.ndarray,
    t_band_short_pcts: np.ndarray,
    use_emergency_sls: np.ndarray,
    emergency_sl_pcts: np.ndarray,
    emergency_sl_update_bars_values: np.ndarray,
    contract_sizes: np.ndarray,
    initial_capitals: np.ndarray,
    commission_pcts: np.ndarray,
    compute_dsr: bool,
    risk_free_rate: float,
    outputs: np.ndarray,
) -> None:
    for idx in numba.prange(ma_indices.shape[0]):
        (
            net_profit_pct,
            max_drawdown_pct,
            total_trades,
            winning_trades,
            losing_trades,
            win_rate,
            gross_profit,
            gross_loss,
            profit_factor,
            romad,
            avg_win,
            avg_loss,
            max_consecutive_losses,
            sharpe_ratio,
            monthly_count,
            dsr_skewness,
            dsr_kurtosis,
        ) = _S03_FAST_LOOP(
            close_values,
            open_values,
            high_values,
            low_values,
            month_ids,
            ma_stack[ma_indices[idx]],
            trade_start_idx,
            date_filters[idx],
            ma_offsets[idx],
            use_close_counts[idx],
            use_tbands_values[idx],
            close_count_longs[idx],
            close_count_shorts[idx],
            t_band_long_pcts[idx],
            t_band_short_pcts[idx],
            use_emergency_sls[idx],
            emergency_sl_pcts[idx],
            emergency_sl_update_bars_values[idx],
            contract_sizes[idx],
            initial_capitals[idx],
            commission_pcts[idx],
            compute_dsr,
            risk_free_rate,
        )
        outputs[idx, 0] = net_profit_pct
        outputs[idx, 1] = max_drawdown_pct
        outputs[idx, 2] = total_trades
        outputs[idx, 3] = winning_trades
        outputs[idx, 4] = losing_trades
        outputs[idx, 5] = win_rate
        outputs[idx, 6] = gross_profit
        outputs[idx, 7] = gross_loss
        outputs[idx, 8] = profit_factor
        outputs[idx, 9] = romad
        outputs[idx, 10] = avg_win
        outputs[idx, 11] = avg_loss
        outputs[idx, 12] = max_consecutive_losses
        outputs[idx, 13] = sharpe_ratio
        outputs[idx, 14] = monthly_count
        outputs[idx, 15] = dsr_skewness
        outputs[idx, 16] = dsr_kurtosis


if NUMBA_AVAILABLE:
    _S03_FAST_BATCH_LOOP = numba.njit(cache=True, parallel=True)(_s03_fast_batch_loop_impl)
else:  # pragma: no cover
    _S03_FAST_BATCH_LOOP = None


def _result_from_values(
    candidate: GridCandidate,
    values: Sequence[Any],
    *,
    needs_dsr: bool = False,
) -> OptimizationResult:
    (
        net_profit_pct,
        max_drawdown_pct,
        total_trades,
        winning_trades,
        losing_trades,
        win_rate,
        gross_profit,
        gross_loss,
        profit_factor,
        romad,
        avg_win,
        avg_loss,
        max_consecutive_losses,
        sharpe_ratio,
        dsr_track_length,
        dsr_skewness,
        dsr_kurtosis,
    ) = values
    result = OptimizationResult(
        params=dict(candidate.params),
        net_profit_pct=float(net_profit_pct),
        max_drawdown_pct=float(max_drawdown_pct),
        total_trades=int(total_trades),
        winning_trades=int(winning_trades),
        losing_trades=int(losing_trades),
        win_rate=float(win_rate),
        avg_win=float(avg_win),
        avg_loss=float(avg_loss),
        gross_profit=float(gross_profit),
        gross_loss=float(gross_loss),
        max_consecutive_losses=int(max_consecutive_losses),
        romad=float(romad),
        sharpe_ratio=None if math.isnan(float(sharpe_ratio)) else float(sharpe_ratio),
        profit_factor=None if math.isnan(float(profit_factor)) else float(profit_factor),
        optuna_trial_number=int(candidate.candidate_id),
    )
    setattr(result, "candidate_id", int(candidate.candidate_id))
    setattr(result, "semantic_key", candidate.semantic_key)
    setattr(result, "param_key", candidate.semantic_key)
    setattr(result, "grid_mode_name", candidate.mode)
    setattr(result, "grid_generation_mode", candidate.generation_mode)
    setattr(result, "diversity_group", candidate.diversity_group)
    if needs_dsr:
        setattr(result, "dsr_track_length", int(dsr_track_length))
        setattr(result, "dsr_skewness", None if math.isnan(float(dsr_skewness)) else float(dsr_skewness))
        setattr(result, "dsr_kurtosis", None if math.isnan(float(dsr_kurtosis)) else float(dsr_kurtosis))
    setattr(result, "fast_metrics", _result_metric_dict(result))
    return result


def _evaluate_one(data: FastGridData, candidate: GridCandidate, *, needs_dsr: bool = False) -> OptimizationResult:
    params = candidate.params
    ma_key = (_clean_ma_type(params.get("maType3")), int(params.get("maLength3")))
    ma_values = data.ma_cache[ma_key]
    if _S03_FAST_LOOP is None:
        raise RuntimeError(f"Numba is not available: {NUMBA_IMPORT_ERROR}")
    values = _S03_FAST_LOOP(
        data.close_values,
        data.open_values,
        data.high_values,
        data.low_values,
        data.month_ids,
        ma_values,
        int(data.trade_start_idx),
        _coerce_bool(params.get("dateFilter"), False),
        float(params.get("maOffset3", 0.0)),
        _coerce_bool(params.get("useCloseCount"), True),
        _coerce_bool(params.get("useTBands"), True),
        int(params.get("closeCountLong", 1)),
        int(params.get("closeCountShort", 1)),
        float(params.get("tBandLongPct", 0.0)),
        float(params.get("tBandShortPct", 0.0)),
        _coerce_bool(params.get("useEmergencySL"), False),
        float(params.get("emergencySlPct", 20.0)),
        int(params.get("emergencySlUpdateBars", 16)),
        float(params.get("contractSize", 0.01)),
        float(params.get("initialCapital", 100.0)),
        float(params.get("commissionPct", 0.05)),
        bool(needs_dsr),
        0.02,
    )
    return _result_from_values(candidate, values, needs_dsr=needs_dsr)


def evaluate_candidates(
    data: FastGridData,
    candidates: Sequence[GridCandidate],
    *,
    n_workers: int = 1,
    needs_dsr: bool = False,
) -> List[OptimizationResult]:
    if not candidates:
        return []
    if _S03_FAST_BATCH_LOOP is None:
        raise RuntimeError(f"Numba is not available: {NUMBA_IMPORT_ERROR}")

    ma_keys = sorted(data.ma_cache.keys())
    ma_index_by_key = {key: idx for idx, key in enumerate(ma_keys)}
    ma_stack = np.ascontiguousarray(
        np.vstack([data.ma_cache[key] for key in ma_keys]),
        dtype=np.float64,
    )

    count = len(candidates)
    ma_indices = np.empty(count, dtype=np.int64)
    date_filters = np.empty(count, dtype=np.bool_)
    ma_offsets = np.empty(count, dtype=np.float64)
    use_close_counts = np.empty(count, dtype=np.bool_)
    use_tbands_values = np.empty(count, dtype=np.bool_)
    close_count_longs = np.empty(count, dtype=np.int64)
    close_count_shorts = np.empty(count, dtype=np.int64)
    t_band_long_pcts = np.empty(count, dtype=np.float64)
    t_band_short_pcts = np.empty(count, dtype=np.float64)
    use_emergency_sls = np.empty(count, dtype=np.bool_)
    emergency_sl_pcts = np.empty(count, dtype=np.float64)
    emergency_sl_update_bars_values = np.empty(count, dtype=np.int64)
    contract_sizes = np.empty(count, dtype=np.float64)
    initial_capitals = np.empty(count, dtype=np.float64)
    commission_pcts = np.empty(count, dtype=np.float64)

    for idx, candidate in enumerate(candidates):
        params = candidate.params
        ma_key = (_clean_ma_type(params.get("maType3")), int(params.get("maLength3")))
        ma_indices[idx] = ma_index_by_key[ma_key]
        date_filters[idx] = _coerce_bool(params.get("dateFilter"), False)
        ma_offsets[idx] = float(params.get("maOffset3", 0.0))
        use_close_counts[idx] = _coerce_bool(params.get("useCloseCount"), True)
        use_tbands_values[idx] = _coerce_bool(params.get("useTBands"), True)
        close_count_longs[idx] = int(params.get("closeCountLong", 1))
        close_count_shorts[idx] = int(params.get("closeCountShort", 1))
        t_band_long_pcts[idx] = float(params.get("tBandLongPct", 0.0))
        t_band_short_pcts[idx] = float(params.get("tBandShortPct", 0.0))
        use_emergency_sls[idx] = _coerce_bool(params.get("useEmergencySL"), False)
        emergency_sl_pcts[idx] = float(params.get("emergencySlPct", 20.0))
        emergency_sl_update_bars_values[idx] = int(params.get("emergencySlUpdateBars", 16))
        contract_sizes[idx] = float(params.get("contractSize", 0.01))
        initial_capitals[idx] = float(params.get("initialCapital", 100.0))
        commission_pcts[idx] = float(params.get("commissionPct", 0.05))

    outputs = np.empty((count, 17), dtype=np.float64)
    requested_threads = max(1, int(n_workers or 1))
    previous_threads = numba.get_num_threads()
    target_threads = max(1, min(requested_threads, previous_threads))
    try:
        if target_threads != previous_threads:
            numba.set_num_threads(target_threads)
        _S03_FAST_BATCH_LOOP(
            data.close_values,
            data.open_values,
            data.high_values,
            data.low_values,
            data.month_ids,
            ma_stack,
            ma_indices,
            int(data.trade_start_idx),
            date_filters,
            ma_offsets,
            use_close_counts,
            use_tbands_values,
            close_count_longs,
            close_count_shorts,
            t_band_long_pcts,
            t_band_short_pcts,
            use_emergency_sls,
            emergency_sl_pcts,
            emergency_sl_update_bars_values,
            contract_sizes,
            initial_capitals,
            commission_pcts,
            bool(needs_dsr),
            0.02,
            outputs,
        )
    finally:
        if numba.get_num_threads() != previous_threads:
            numba.set_num_threads(previous_threads)

    return [
        _result_from_values(candidate, outputs[idx], needs_dsr=needs_dsr)
        for idx, candidate in enumerate(candidates)
    ]


def _result_metric_dict(result: OptimizationResult) -> Dict[str, Any]:
    payload = {
        "net_profit_pct": result.net_profit_pct,
        "max_drawdown_pct": result.max_drawdown_pct,
        "total_trades": result.total_trades,
        "winning_trades": getattr(result, "winning_trades", None),
        "win_rate": result.win_rate,
        "gross_profit": result.gross_profit,
        "gross_loss": result.gross_loss,
        "profit_factor": result.profit_factor,
        "romad": result.romad,
        "sharpe_ratio": result.sharpe_ratio,
        "max_consecutive_losses": result.max_consecutive_losses,
    }
    if hasattr(result, "dsr_track_length"):
        payload.update(
            {
                "dsr_skewness": getattr(result, "dsr_skewness", None),
                "dsr_kurtosis": getattr(result, "dsr_kurtosis", None),
                "dsr_track_length": getattr(result, "dsr_track_length", None),
            }
        )
    return payload


def _profit_factor_matches(fast_value: Any, slow_value: Any) -> bool:
    if fast_value is None and slow_value is None:
        return True
    try:
        fast_float = float(fast_value)
        slow_float = float(slow_value)
    except (TypeError, ValueError):
        return False
    if math.isinf(fast_float) or math.isinf(slow_float):
        return math.isinf(fast_float) and math.isinf(slow_float) and (fast_float > 0) == (slow_float > 0)
    return abs(fast_float - slow_float) <= max(1e-6, 1e-4 * max(abs(fast_float), abs(slow_float), 1.0))


def _validation_diffs(
    fast: OptimizationResult,
    slow: OptimizationResult,
    tolerances: Dict[str, float],
) -> Tuple[bool, Dict[str, Any]]:
    diffs: Dict[str, Any] = {}
    checks = (
        ("net_profit_pct", "net_profit_pct_abs"),
        ("max_drawdown_pct", "max_drawdown_pct_abs"),
        ("romad", "romad_abs"),
        ("win_rate", "win_rate_abs"),
        ("total_trades", "total_trades_abs"),
        ("max_consecutive_losses", "max_consecutive_losses_abs"),
    )
    ok = True
    for attr, tol_key in checks:
        fast_value = getattr(fast, attr, None)
        slow_value = getattr(slow, attr, None)
        try:
            diff = abs(float(fast_value or 0.0) - float(slow_value or 0.0))
        except (TypeError, ValueError):
            diff = math.inf
        tolerance = float(tolerances.get(tol_key, 0.0))
        passed = diff <= tolerance
        ok = ok and passed
        diffs[attr] = {
            "fast": fast_value,
            "slow": slow_value,
            "diff": diff,
            "tolerance": tolerance,
            "passed": passed,
        }

    for attr, tol_key in (
        ("winning_trades", "winning_trades_abs"),
        ("losing_trades", "losing_trades_abs"),
    ):
        fast_count = getattr(fast, attr, None)
        slow_count = getattr(slow, attr, None)
        count_diff = abs(int(fast_count or 0) - int(slow_count or 0))
        count_tol = int(tolerances.get(tol_key, 0.0))
        count_passed = count_diff <= count_tol
        ok = ok and count_passed
        diffs[attr] = {
            "fast": fast_count,
            "slow": slow_count,
            "diff": count_diff,
            "tolerance": count_tol,
            "passed": count_passed,
        }

    pf_passed = _profit_factor_matches(fast.profit_factor, slow.profit_factor)
    ok = ok and pf_passed
    diffs["profit_factor"] = {
        "fast": fast.profit_factor,
        "slow": slow.profit_factor,
        "passed": pf_passed,
    }
    return ok, diffs


def validate_selected_candidates(
    df: pd.DataFrame,
    trade_start_idx: int,
    selected_fast: Sequence[OptimizationResult],
    *,
    tolerances: Dict[str, float],
    fail_on_error: bool,
) -> List[OptimizationResult]:
    validated: List[OptimizationResult] = []
    for fast_result in selected_fast:
        slow_result = _run_single_combination(
            (dict(fast_result.params), df, int(trade_start_idx), S03ReversalV11)
        )
        candidate_id = int(getattr(fast_result, "candidate_id", fast_result.optuna_trial_number or 0))
        setattr(slow_result, "candidate_id", candidate_id)
        slow_result.optuna_trial_number = candidate_id
        for attr in (
            "semantic_key",
            "param_key",
            "grid_mode_name",
            "grid_generation_mode",
            "diversity_group",
            "grid_rank",
            "selection_sources",
            "is_objective_selected",
            "is_dsr_selected",
            "is_pareto_optimal",
            "dominance_rank",
            "dsr_probability",
            "dsr_rank",
            "dsr_skewness",
            "dsr_kurtosis",
            "dsr_track_length",
            "dsr_luck_share_pct",
            "dsr_source_rank",
            "slow_refinement_rank",
        ):
            if hasattr(fast_result, attr):
                setattr(slow_result, attr, getattr(fast_result, attr, None))
        setattr(slow_result, "fast_metrics", getattr(fast_result, "fast_metrics", None))

        ok, diffs = _validation_diffs(fast_result, slow_result, tolerances)
        setattr(slow_result, "validation_status", "passed" if ok else "failed")
        setattr(slow_result, "validation_diffs", diffs)
        if not ok and fail_on_error:
            payload = {
                "candidate_id": candidate_id,
                "semantic_key": getattr(fast_result, "semantic_key", None),
                "params": fast_result.params,
                "diffs": diffs,
            }
            raise ValueError("Grid fast-vs-slow validation failed: " + json.dumps(payload, default=str, sort_keys=True))
        validated.append(slow_result)
    return validated
