"""S06 R-Trend v02 deterministic full-enumeration Numba Grid backend."""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import numba
except Exception as exc:  # pragma: no cover - exercised only without Numba.
    numba = None
    NUMBA_IMPORT_ERROR = str(exc)
else:
    NUMBA_IMPORT_ERROR = None

from core.grid_engine import GridAllocation, GridSettings, format_compact_count
from core.optuna_engine import OptimizationConfig, OptimizationResult, _run_single_combination
from strategies import get_strategy_config
from strategies.s06_r_trend_v02.strategy import (
    S06RTrendV02,
    _pine_ema,
    _pine_rma,
    _signal_events,
    _trail_ma,
    _williams_r,
)

NUMBA_AVAILABLE = numba is not None

MODE_ORDER = ("bracket", "trail")
MODE_LABELS = {"bracket": "Bracket", "trail": "Trail"}
THRESHOLD_DOMAIN = (20, 30, 40)
MODE_AXES = {
    "bracket": (
        "thresholdOS",
        "thresholdOB",
        "stopX",
        "stopRR",
        "stopLP",
        "stopMaxPct",
        "stopMaxDays",
    ),
    "trail": (
        "thresholdOS",
        "thresholdOB",
        "stopX",
        "stopLP",
        "stopMaxPct",
        "stopMaxDays",
        "trailMAType",
        "trailMALength",
        "trailMAOffsetEx",
        "trailRR",
    ),
}
INT_PARAMS = {
    "fastLength",
    "fastSmoothing",
    "slowLength",
    "slowSmoothing",
    "thresholdOS",
    "thresholdOB",
    "stopLP",
    "stopMaxDays",
    "trailMALength",
}
FLOAT_PARAMS = {
    "stopX",
    "stopRR",
    "stopMaxPct",
    "riskPerTrade",
    "contractSize",
    "trailRR",
    "trailMAOffsetEx",
    "initialCapital",
    "commissionPct",
}
BOOL_PARAMS = {"enableLong", "enableShort", "dateFilter", "useTrailMA"}
TRAIL_MA_CODE = {"SMA": 0, "HMA": 1, "KAMA": 2, "T3": 3}
EMPTY_PARAMS: Dict[str, Any] = {}


def get_backend_metadata() -> Dict[str, Any]:
    return {
        "profile": "full_enumeration",
        "modes": [
            {"id": "bracket", "label": "Bracket", "default_enabled": True},
            {"id": "trail", "label": "Trail", "default_enabled": True},
        ],
        "supports_partial_coverage": False,
        "supports_seed": False,
        "supports_mode_allocation": False,
        "retain_all_fast_results": False,
        "diversity_group_fields": {
            "bracket": ["mode", "stopX", "stopRR"],
            "trail": ["mode", "trailMAType", "trailMALength"],
        },
    }


def _coerce_bool(value: Any, default: bool = False) -> bool:
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
    return default


def _decimal_places(value: Any) -> int:
    try:
        exponent = Decimal(str(value)).as_tuple().exponent
    except (InvalidOperation, ValueError):
        return 0
    return max(0, -int(exponent))


def _range_values(start: Any, stop: Any, step: Any, *, is_int: bool, name: str) -> List[Any]:
    try:
        first = float(start)
        last = float(stop)
        stride = float(step)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid Grid range for '{name}'.") from exc
    if not all(math.isfinite(value) for value in (first, last, stride)):
        raise ValueError(f"Grid range for '{name}' must be finite.")
    if stride <= 0.0 or last < first:
        raise ValueError(f"Invalid Grid bounds for '{name}'.")
    count = int(round((last - first) / stride)) + 1
    places = max(_decimal_places(start), _decimal_places(stop), _decimal_places(step)) + 2
    values: List[Any] = []
    for index in range(count):
        value = first + index * stride
        if abs(value - last) <= stride * 1e-9:
            value = last
        values.append(int(round(value)) if is_int else round(value, places))
    return values


def _canonical_value(name: str, value: Any) -> Any:
    if name in INT_PARAMS:
        return int(float(value))
    if name in FLOAT_PARAMS:
        rounded = round(float(value), 10)
        return 0.0 if abs(rounded) < 1e-12 else rounded
    if name in BOOL_PARAMS:
        return _coerce_bool(value)
    if name == "trailMAType":
        return str(value or "").strip().upper()
    return value


def _fixed_value(
    config: OptimizationConfig,
    strategy_params: Mapping[str, Any],
    name: str,
    fallback: Any = None,
) -> Any:
    fixed = config.fixed_params or {}
    if name in fixed:
        return fixed[name]
    spec = strategy_params.get(name, {})
    if isinstance(spec, Mapping) and "default" in spec:
        return spec["default"]
    return fallback


def _selected_options(
    config: OptimizationConfig,
    strategy_params: Mapping[str, Any],
    name: str,
) -> List[Any]:
    fixed = config.fixed_params or {}
    explicit = fixed.get(f"{name}_options")
    if isinstance(explicit, (list, tuple)) and explicit:
        return list(explicit)
    range_spec = (config.param_ranges or {}).get(name)
    if isinstance(range_spec, Mapping):
        values = range_spec.get("values")
        if isinstance(values, (list, tuple)) and values:
            return list(values)
    spec = strategy_params.get(name, {})
    options = spec.get("options") if isinstance(spec, Mapping) else None
    if isinstance(options, (list, tuple)) and options:
        return list(options)
    return [_fixed_value(config, strategy_params, name)]


def _axis_values(
    config: OptimizationConfig,
    strategy_params: Mapping[str, Any],
    name: str,
) -> List[Any]:
    enabled = bool((config.enabled_params or {}).get(name, False))
    if not enabled:
        return [_canonical_value(name, _fixed_value(config, strategy_params, name))]
    if name in {"thresholdOS", "thresholdOB"}:
        return list(THRESHOLD_DOMAIN)
    param_type = str((config.param_types or {}).get(name, "")).lower()
    if param_type in {"select", "options"}:
        return list(dict.fromkeys(_canonical_value(name, value) for value in _selected_options(
            config, strategy_params, name
        )))
    range_spec = (config.param_ranges or {}).get(name)
    spec = strategy_params.get(name, {})
    optimize = spec.get("optimize", {}) if isinstance(spec, Mapping) else {}
    if isinstance(range_spec, (list, tuple)) and len(range_spec) == 3:
        start, stop, step = range_spec
    else:
        start = optimize.get("min", spec.get("min", _fixed_value(config, strategy_params, name)))
        stop = optimize.get("max", spec.get("max", start))
        step = optimize.get("step", spec.get("step", 1))
    return [
        _canonical_value(name, value)
        for value in _range_values(
            start,
            stop,
            step,
            is_int=name in INT_PARAMS or param_type in {"int", "integer"},
            name=name,
        )
    ]


def _identity_token(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float):
        return format(value, ".10g")
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


@dataclass
class GridParameterSpace:
    axes: Dict[str, List[Any]]
    fixed_values: Dict[str, Any]
    optimized_params: Dict[str, bool]
    mode_space_sizes: Dict[str, int]
    total_space_size: int
    allowed_modes: List[str]
    mode_axis_names: Dict[str, Tuple[str, ...]]
    identity_prefix: str


@dataclass(slots=True)
class GridCandidate:
    candidate_id: int
    mode: str
    active_names: Tuple[str, ...]
    active_values: Tuple[Any, ...]
    fixed_values: Mapping[str, Any]
    identity_prefix: str

    @property
    def params(self) -> Dict[str, Any]:
        payload = dict(self.fixed_values)
        payload.update(zip(self.active_names, self.active_values))
        payload["useTrailMA"] = self.mode == "trail"
        return payload

    @property
    def semantic_key(self) -> str:
        mode_code = "b" if self.mode == "bracket" else "t"
        active = ",".join(_identity_token(value) for value in self.active_values)
        return f"{mode_code}:{self.identity_prefix}:{active}"

    @property
    def generation_mode(self) -> str:
        return "full"

    @property
    def diversity_group(self) -> str:
        params = self.params
        if self.mode == "bracket":
            return (
                f"bracket|{_identity_token(params['stopX'])}|"
                f"{_identity_token(params['stopRR'])}"
            )
        return f"trail|{params['trailMAType']}|{params['trailMALength']}"


class CandidateSequence(Sequence[GridCandidate]):
    """Lazy deterministic mixed-radix view over the complete S06 space."""

    def __init__(self, space: GridParameterSpace):
        self.space = space
        self._mode_offsets: List[Tuple[str, int, int]] = []
        offset = 0
        for mode in MODE_ORDER:
            size = int(space.mode_space_sizes.get(mode, 0))
            if size > 0:
                self._mode_offsets.append((mode, offset, offset + size))
                offset += size
        self._length = offset

    def __len__(self) -> int:
        return self._length

    def __getitem__(self, index: int | slice) -> GridCandidate | List[GridCandidate]:
        if isinstance(index, slice):
            return [self[item] for item in range(*index.indices(self._length))]
        normalized = int(index)
        if normalized < 0:
            normalized += self._length
        if normalized < 0 or normalized >= self._length:
            raise IndexError(index)
        for mode, start, end in self._mode_offsets:
            if normalized >= end:
                continue
            local = normalized - start
            names = self.space.mode_axis_names[mode]
            values: List[Any] = [None] * len(names)
            for axis_index in range(len(names) - 1, -1, -1):
                axis = self.space.axes[names[axis_index]]
                values[axis_index] = axis[local % len(axis)]
                local //= len(axis)
            return GridCandidate(
                candidate_id=normalized + 1,
                mode=mode,
                active_names=names,
                active_values=tuple(values),
                fixed_values=self.space.fixed_values,
                identity_prefix=self.space.identity_prefix,
            )
        raise IndexError(index)

    def __iter__(self) -> Iterator[GridCandidate]:
        for index in range(self._length):
            yield self[index]


@dataclass
class CandidateSet:
    candidates: CandidateSequence
    diagnostics: Dict[str, Any]


@dataclass
class FastGridData:
    df: pd.DataFrame
    trade_start_idx: int
    open_values: np.ndarray
    high_values: np.ndarray
    low_values: np.ndarray
    close_values: np.ndarray
    timestamp_ns: np.ndarray
    month_ids: np.ndarray
    atr_values: np.ndarray
    lowest_stack: np.ndarray
    highest_stack: np.ndarray
    stop_lp_values: Tuple[int, ...]
    signal_long_stack: np.ndarray
    signal_short_stack: np.ndarray
    signal_pairs: Tuple[Tuple[int, int], ...]
    trail_ma_stack: np.ndarray
    trail_ma_keys: Tuple[Tuple[str, int], ...]
    fixed_values: Dict[str, Any]
    candidate_source: CandidateSequence
    ma_cache_entries: int
    ma_cache_build_seconds: float
    ma_cache_estimated_mb: float
    signal_cache_entries: int
    signal_cache_build_seconds: float
    data_cache_estimated_mb: float


@dataclass(slots=True)
class FastGridResult:
    params: Dict[str, Any]
    net_profit_pct: float
    max_drawdown_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    gross_profit: float
    gross_loss: float
    max_consecutive_losses: int
    romad: Optional[float]
    sharpe_ratio: Optional[float]
    profit_factor: Optional[float]
    score: float
    optuna_trial_number: int
    candidate_id: int
    semantic_key: str
    param_key: str
    grid_mode_name: str
    grid_generation_mode: str
    diversity_group: str
    candidate_source: CandidateSequence
    objective_values: List[float] = field(default_factory=list)
    constraint_values: List[float] = field(default_factory=list)
    constraints_satisfied: Optional[bool] = None
    is_pareto_optimal: Optional[bool] = None
    dominance_rank: Optional[int] = None
    grid_rank: Optional[int] = None
    selection_sources: Optional[List[str]] = None
    is_objective_selected: bool = False
    is_dsr_selected: bool = False
    dsr_probability: Optional[float] = None
    dsr_rank: Optional[int] = None
    dsr_skewness: Optional[float] = None
    dsr_kurtosis: Optional[float] = None
    dsr_track_length: Optional[int] = None
    dsr_luck_share_pct: Optional[float] = None
    dsr_source_rank: Optional[int] = None


def build_parameter_space(config: OptimizationConfig) -> GridParameterSpace:
    strategy_config = get_strategy_config(config.strategy_id)
    strategy_params = strategy_config.get("parameters", {})
    fixed_values = {
        name: _canonical_value(name, _fixed_value(config, strategy_params, name))
        for name in strategy_params
    }
    fixed_values.update(
        {
            key: value
            for key, value in (config.fixed_params or {}).items()
            if not str(key).endswith("_options")
        }
    )

    raw_modes = list(getattr(config, "grid_enabled_modes", None) or [])
    allowed_modes = [mode for mode in MODE_ORDER if mode in raw_modes]
    if not allowed_modes:
        raise ValueError("S06 Grid requires at least one enabled mode: bracket or trail.")

    axis_names = sorted({name for mode in allowed_modes for name in MODE_AXES[mode]})
    axes = {name: list(dict.fromkeys(_axis_values(config, strategy_params, name))) for name in axis_names}
    for name, values in axes.items():
        if not values:
            raise ValueError(f"Grid parameter axis '{name}' is empty.")
    if "trailMAType" in axes:
        unsupported = sorted(set(axes["trailMAType"]) - set(TRAIL_MA_CODE))
        if unsupported:
            raise ValueError("Unsupported S06 Trail MA type(s): " + ", ".join(unsupported))

    mode_axis_names: Dict[str, Tuple[str, ...]] = {}
    mode_sizes = {mode: 0 for mode in MODE_ORDER}
    for mode in allowed_modes:
        names = tuple(MODE_AXES[mode])
        mode_axis_names[mode] = names
        size = 1
        for name in names:
            size *= len(axes[name])
        mode_sizes[mode] = int(size)

    identity_fields = (
        "entryMode",
        "enableLong",
        "enableShort",
        "fastLength",
        "fastSmoothing",
        "slowLength",
        "slowSmoothing",
        "riskPerTrade",
        "contractSize",
        "initialCapital",
        "commissionPct",
        "dateFilter",
        "start",
        "end",
    )
    identity_payload = {
        name: _identity_token(fixed_values.get(name))
        for name in identity_fields
    }
    identity_prefix = hashlib.blake2b(
        json.dumps(identity_payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        digest_size=6,
    ).hexdigest()
    total = sum(mode_sizes.values())
    return GridParameterSpace(
        axes=axes,
        fixed_values=fixed_values,
        optimized_params={
            name: bool((config.enabled_params or {}).get(name, False))
            for name in axes
        },
        mode_space_sizes=mode_sizes,
        total_space_size=total,
        allowed_modes=allowed_modes,
        mode_axis_names=mode_axis_names,
        identity_prefix=identity_prefix,
    )


def build_allocation(
    config: OptimizationConfig,
    space: GridParameterSpace,
    settings: GridSettings,
) -> GridAllocation:
    del config, settings
    sizes = dict(space.mode_space_sizes)
    total = int(space.total_space_size)
    coverage = {mode: (100.0 if sizes.get(mode, 0) else 0.0) for mode in MODE_ORDER}
    return GridAllocation(
        requested_budget=total,
        actual_budget=total,
        unused_budget=0,
        mode_space_sizes=sizes,
        mode_budgets=dict(sizes),
        mode_coverage_pct=coverage,
        target_mode_quotas={
            mode: (sizes.get(mode, 0) / total if total else 0.0)
            for mode in MODE_ORDER
        },
        allocation_method="full_enumeration",
        allocation_params={},
    )


def build_preview(space: GridParameterSpace, allocation: GridAllocation) -> Dict[str, Any]:
    rows = []
    for mode in MODE_ORDER:
        size = int(space.mode_space_sizes.get(mode, 0))
        rows.append(
            {
                "mode": mode,
                "label": MODE_LABELS[mode],
                "space_size": size,
                "space_label": format_compact_count(size),
                "budget": size,
                "budget_label": format_compact_count(size),
                "coverage_pct": 100.0 if size else 0.0,
                "coverage_label": "100%" if size else "0%",
                "generation": "Full enumeration" if size else "Disabled",
            }
        )
    return {
        "profile": "full_enumeration",
        "method": "Full enumeration",
        "total_space": space.total_space_size,
        "total_space_label": format_compact_count(space.total_space_size),
        "requested_budget": allocation.requested_budget,
        "requested_budget_label": format_compact_count(allocation.requested_budget),
        "actual_budget": allocation.actual_budget,
        "actual_budget_label": format_compact_count(allocation.actual_budget),
        "coverage_pct": 100.0,
        "coverage_label": "100%",
        "modes": rows,
        "allocation_method": "full_enumeration",
    }


def generate_candidates(
    config: OptimizationConfig,
    space: GridParameterSpace,
    allocation: GridAllocation,
    *,
    seed: int,
) -> CandidateSet:
    del config, seed
    candidates = CandidateSequence(space)
    if len(candidates) != allocation.actual_budget:
        raise ValueError("S06 Grid preview/run space mismatch.")
    return CandidateSet(
        candidates=candidates,
        diagnostics={
            "profile": "full_enumeration",
            "sampling_seed": None,
            "target_candidate_count": len(candidates),
            "actual_candidate_count": len(candidates),
            "actual_mode_counts": dict(space.mode_space_sizes),
            "dedupe_count": 0,
            "lazy_candidate_sequence": True,
        },
    )


def hydrate_params(candidate: GridCandidate) -> Dict[str, Any]:
    return candidate.params


def _candidate_sequence(candidates: Sequence[GridCandidate]) -> CandidateSequence:
    if not isinstance(candidates, CandidateSequence):
        raise TypeError("S06 fast Grid requires its deterministic CandidateSequence.")
    return candidates


def prepare_fast_data(
    df: pd.DataFrame,
    trade_start_idx: int,
    candidates: Sequence[GridCandidate],
) -> FastGridData:
    started = time.perf_counter()
    source = _candidate_sequence(candidates)
    space = source.space
    fixed = dict(space.fixed_values)
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    high_values = np.ascontiguousarray(high.to_numpy(copy=False), dtype=np.float64)
    low_values = np.ascontiguousarray(low.to_numpy(copy=False), dtype=np.float64)
    close_values = np.ascontiguousarray(close.to_numpy(copy=False), dtype=np.float64)

    previous_close = np.roll(close_values, 1)
    if len(previous_close):
        previous_close[0] = np.nan
    true_range = np.maximum.reduce(
        (
            np.abs(high_values - low_values),
            np.abs(high_values - previous_close),
            np.abs(low_values - previous_close),
        )
    )
    if len(true_range):
        true_range[0] = abs(high_values[0] - low_values[0])
    atr_values = np.ascontiguousarray(_pine_rma(true_range, 14), dtype=np.float64)

    stop_lp_values = tuple(sorted(int(value) for value in space.axes["stopLP"]))
    lowest_stack = np.ascontiguousarray(
        np.vstack(
            [
                low.rolling(length, min_periods=length).min().to_numpy(copy=False)
                for length in stop_lp_values
            ]
        ),
        dtype=np.float64,
    )
    highest_stack = np.ascontiguousarray(
        np.vstack(
            [
                high.rolling(length, min_periods=length).max().to_numpy(copy=False)
                for length in stop_lp_values
            ]
        ),
        dtype=np.float64,
    )

    fast_percent_r = _pine_ema(
        _williams_r(high, low, close, int(fixed["fastLength"])),
        int(fixed["fastSmoothing"]),
    )
    slow_percent_r = _pine_ema(
        _williams_r(high, low, close, int(fixed["slowLength"])),
        int(fixed["slowSmoothing"]),
    )
    signal_started = time.perf_counter()
    signal_pairs = tuple(
        (int(os_value), int(ob_value))
        for os_value in space.axes["thresholdOS"]
        for ob_value in space.axes["thresholdOB"]
    )
    signal_long: List[np.ndarray] = []
    signal_short: List[np.ndarray] = []
    for threshold_os, threshold_ob in signal_pairs:
        events = _signal_events(
            fast_percent_r,
            slow_percent_r,
            threshold_os,
            threshold_ob,
            str(fixed["entryMode"]),
        )
        signal_long.append(events.long_signal)
        signal_short.append(events.short_signal)
    signal_long_stack = np.ascontiguousarray(np.vstack(signal_long), dtype=np.bool_)
    signal_short_stack = np.ascontiguousarray(np.vstack(signal_short), dtype=np.bool_)
    signal_seconds = time.perf_counter() - signal_started

    ma_started = time.perf_counter()
    if "trail" in space.allowed_modes:
        trail_ma_keys = tuple(
            (str(ma_type), int(length))
            for ma_type in space.axes["trailMAType"]
            for length in space.axes["trailMALength"]
        )
        trail_ma_stack = np.ascontiguousarray(
            np.vstack(
                [
                    _trail_ma(close, ma_type, length).to_numpy(copy=False)
                    for ma_type, length in trail_ma_keys
                ]
            ),
            dtype=np.float64,
        )
    else:
        trail_ma_keys = ()
        trail_ma_stack = np.full((1, len(df)), np.nan, dtype=np.float64)
    ma_seconds = time.perf_counter() - ma_started

    cache_arrays = (
        atr_values,
        lowest_stack,
        highest_stack,
        signal_long_stack,
        signal_short_stack,
        trail_ma_stack,
    )
    estimated_mb = sum(array.nbytes for array in cache_arrays) / (1024.0 * 1024.0)
    return FastGridData(
        df=df,
        trade_start_idx=int(trade_start_idx),
        open_values=np.ascontiguousarray(df["Open"].to_numpy(copy=False), dtype=np.float64),
        high_values=high_values,
        low_values=low_values,
        close_values=close_values,
        timestamp_ns=np.ascontiguousarray(df.index.asi8, dtype=np.int64),
        month_ids=np.ascontiguousarray(
            np.asarray(df.index.year * 12 + df.index.month, dtype=np.int64)
        ),
        atr_values=atr_values,
        lowest_stack=lowest_stack,
        highest_stack=highest_stack,
        stop_lp_values=stop_lp_values,
        signal_long_stack=signal_long_stack,
        signal_short_stack=signal_short_stack,
        signal_pairs=signal_pairs,
        trail_ma_stack=trail_ma_stack,
        trail_ma_keys=trail_ma_keys,
        fixed_values=fixed,
        candidate_source=source,
        ma_cache_entries=len(trail_ma_keys),
        ma_cache_build_seconds=ma_seconds,
        ma_cache_estimated_mb=trail_ma_stack.nbytes / (1024.0 * 1024.0),
        signal_cache_entries=len(signal_pairs),
        signal_cache_build_seconds=signal_seconds,
        data_cache_estimated_mb=estimated_mb,
    )


def _s06_fast_loop_impl(
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    month_ids: np.ndarray,
    atr_values: np.ndarray,
    lowest_values: np.ndarray,
    highest_values: np.ndarray,
    long_signals: np.ndarray,
    short_signals: np.ndarray,
    trail_ma_values: np.ndarray,
    trade_start_idx: int,
    use_date_filter: bool,
    start_ns: int,
    end_ns: int,
    enable_long: bool,
    enable_short: bool,
    use_trail: bool,
    stop_x: float,
    stop_rr: float,
    stop_max_pct: float,
    stop_max_days: int,
    risk_per_trade: float,
    contract_size: float,
    trail_rr: float,
    trail_offset_ex: float,
    initial_capital: float,
    commission_pct: float,
    compute_dsr: bool,
    risk_free_rate: float,
    record_trades: bool,
    trace_direction: np.ndarray,
    trace_entry_index: np.ndarray,
    trace_exit_index: np.ndarray,
    trace_entry_price: np.ndarray,
    trace_exit_price: np.ndarray,
    trace_size: np.ndarray,
    trace_net_pnl: np.ndarray,
) -> Tuple[float, float, int, int, int, float, float, float, float, float, float, float, int, float, int, float, float]:
    n = close_values.shape[0]
    if n == 0:
        return (0.0, 0.0, 0, 0, 0, 0.0, 0.0, 0.0, math.nan, 0.0, 0.0, 0.0, 0, math.nan, 0, math.nan, math.nan)

    commission_rate = commission_pct / 100.0
    day_ns = 86_400_000_000_000
    balance = initial_capital
    running_peak = initial_capital
    current_drawdown = 0.0
    max_drawdown = 0.0
    last_drawdown_boundary = -1

    position = 0
    previous_close_position = 0
    size = 0.0
    entry_price = math.nan
    entry_index = -1
    entry_commission = 0.0
    anchor_price = math.nan
    initial_stop = math.nan
    target_price = math.nan
    initial_risk = math.nan
    trail_stop = math.nan
    trail_active = False

    pending_entry = False
    pending_direction = 0
    pending_anchor = math.nan
    pending_stop = math.nan
    pending_risk = math.nan
    pending_target = math.nan
    pending_size = 0.0
    pending_market_close = False

    total_trades = 0
    winning_trades = 0
    losing_trades = 0
    gross_profit = 0.0
    gross_loss = 0.0
    consecutive_losses = 0
    max_consecutive_losses = 0

    current_month = -1
    month_start_equity = 0.0
    last_equity = initial_capital
    monthly_count = 0
    monthly_sum = 0.0
    monthly_sumsq = 0.0
    monthly_sum3 = 0.0
    monthly_sum4 = 0.0
    last_bar_index = n - 1

    for i in range(n):
        open_price = open_values[i]
        high = high_values[i]
        low = low_values[i]
        close = close_values[i]
        entry_filled_this_bar = False
        had_position_this_bar = position != 0

        exit_price = math.nan
        if pending_market_close and position != 0:
            exit_price = open_price

        if not math.isnan(exit_price):
            gross_pnl = (
                (exit_price - entry_price) * size
                if position > 0
                else (entry_price - exit_price) * size
            )
            exit_commission = exit_price * size * commission_rate
            net_pnl = gross_pnl - entry_commission - exit_commission
            balance += gross_pnl - exit_commission
            if record_trades:
                trace_direction[total_trades] = position
                trace_entry_index[total_trades] = entry_index
                trace_exit_index[total_trades] = i
                trace_entry_price[total_trades] = entry_price
                trace_exit_price[total_trades] = exit_price
                trace_size[total_trades] = size
                trace_net_pnl[total_trades] = net_pnl
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
            size = 0.0
            entry_price = math.nan
            entry_index = -1
            entry_commission = 0.0
            anchor_price = math.nan
            initial_stop = math.nan
            target_price = math.nan
            initial_risk = math.nan
            trail_stop = math.nan
            trail_active = False
            pending_market_close = False

        if pending_entry and position == 0:
            position = pending_direction
            size = pending_size
            entry_price = open_price
            entry_index = i
            entry_commission = entry_price * size * commission_rate
            balance -= entry_commission
            anchor_price = pending_anchor
            initial_stop = pending_stop
            initial_risk = pending_risk
            target_price = pending_target
            trail_stop = initial_stop
            trail_active = False
            pending_entry = False
            entry_filled_this_bar = True
            had_position_this_bar = True
            if use_trail:
                activation = anchor_price + position * initial_risk * trail_rr
                activation_reached = high >= activation if position > 0 else low <= activation
                if activation_reached:
                    trail_active = True
                    ma_value = trail_ma_values[i]
                    if not math.isnan(ma_value):
                        offset_pct = 1.0 + trail_offset_ex
                        current_band = (
                            ma_value * (1.0 - offset_pct / 100.0)
                            if position > 0
                            else ma_value * (1.0 + offset_pct / 100.0)
                        )
                        trail_stop = (
                            max(trail_stop, current_band)
                            if position > 0
                            else min(trail_stop, current_band)
                        )

        stop_active = math.nan
        if position != 0:
            stop_active = trail_stop if use_trail and trail_active else initial_stop
            if position > 0:
                if open_price <= stop_active:
                    exit_price = open_price
                elif not use_trail and open_price >= target_price:
                    exit_price = open_price
            else:
                if open_price >= stop_active:
                    exit_price = open_price
                elif not use_trail and open_price <= target_price:
                    exit_price = open_price

        if position != 0 and math.isnan(exit_price):
            high_first = abs(open_price - high) < abs(open_price - low)
            current = open_price
            for segment in range(3):
                if high_first:
                    endpoint = high if segment == 0 else (low if segment == 1 else close)
                else:
                    endpoint = low if segment == 0 else (high if segment == 1 else close)
                rising = endpoint >= current
                if not use_trail:
                    if position > 0:
                        if rising and min(current, endpoint) <= target_price <= max(current, endpoint):
                            exit_price = target_price
                        elif (not rising) and min(current, endpoint) <= initial_stop <= max(current, endpoint):
                            exit_price = initial_stop
                    else:
                        if rising and min(current, endpoint) <= initial_stop <= max(current, endpoint):
                            exit_price = initial_stop
                        elif (not rising) and min(current, endpoint) <= target_price <= max(current, endpoint):
                            exit_price = target_price
                elif position > 0 and (not rising) and min(current, endpoint) <= stop_active <= max(current, endpoint):
                    exit_price = stop_active
                elif position < 0 and rising and min(current, endpoint) <= stop_active <= max(current, endpoint):
                    exit_price = stop_active
                if not math.isnan(exit_price):
                    break
                current = endpoint

        if position != 0 and not math.isnan(exit_price):
            gross_pnl = (
                (exit_price - entry_price) * size
                if position > 0
                else (entry_price - exit_price) * size
            )
            exit_commission = exit_price * size * commission_rate
            net_pnl = gross_pnl - entry_commission - exit_commission
            balance += gross_pnl - exit_commission
            if record_trades:
                trace_direction[total_trades] = position
                trace_entry_index[total_trades] = entry_index
                trace_exit_index[total_trades] = i
                trace_entry_price[total_trades] = entry_price
                trace_exit_price[total_trades] = exit_price
                trace_size[total_trades] = size
                trace_net_pnl[total_trades] = net_pnl
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
            size = 0.0
            entry_price = math.nan
            entry_index = -1
            entry_commission = 0.0
            anchor_price = math.nan
            initial_stop = math.nan
            target_price = math.nan
            initial_risk = math.nan
            trail_stop = math.nan
            trail_active = False
            pending_market_close = False

        if position != 0 and use_trail and not entry_filled_this_bar:
            activation = anchor_price + position * initial_risk * trail_rr
            activation_reached = high >= activation if position > 0 else low <= activation
            if trail_active or activation_reached:
                trail_active = True
                ma_value = trail_ma_values[i]
                if not math.isnan(ma_value):
                    offset_pct = 1.0 + trail_offset_ex
                    current_band = (
                        ma_value * (1.0 - offset_pct / 100.0)
                        if position > 0
                        else ma_value * (1.0 + offset_pct / 100.0)
                    )
                    trail_stop = (
                        max(initial_stop, trail_stop, current_band)
                        if position > 0
                        else min(initial_stop, trail_stop, current_band)
                    )

        if position != 0 and entry_index >= 0:
            days_in_trade = (timestamp_ns[i] - timestamp_ns[entry_index]) / day_ns
            if days_in_trade >= stop_max_days and i != last_bar_index:
                pending_market_close = True

        if i == last_bar_index:
            pending_entry = False
            pending_market_close = False
            if position != 0:
                exit_price = close
                gross_pnl = (
                    (exit_price - entry_price) * size
                    if position > 0
                    else (entry_price - exit_price) * size
                )
                exit_commission = exit_price * size * commission_rate
                net_pnl = gross_pnl - entry_commission - exit_commission
                balance += gross_pnl - exit_commission
                if record_trades:
                    trace_direction[total_trades] = position
                    trace_entry_index[total_trades] = entry_index
                    trace_exit_index[total_trades] = i
                    trace_entry_price[total_trades] = entry_price
                    trace_exit_price[total_trades] = exit_price
                    trace_size[total_trades] = size
                    trace_net_pnl[total_trades] = net_pnl
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
                size = 0.0
                entry_price = math.nan
                entry_index = -1
                entry_commission = 0.0
                anchor_price = math.nan
                initial_stop = math.nan
                target_price = math.nan
                initial_risk = math.nan
                trail_stop = math.nan
                trail_active = False

        in_date_range = i >= trade_start_idx
        if use_date_filter:
            in_date_range = (
                in_date_range
                and timestamp_ns[i] >= start_ns
                and timestamp_ns[i] <= end_ns
            )
        if (
            i != last_bar_index
            and in_date_range
            and position == 0
            and previous_close_position == 0
            and not had_position_this_bar
            and not pending_entry
        ):
            direction = 0
            if enable_long and long_signals[i]:
                direction = 1
            elif enable_short and short_signals[i]:
                direction = -1
            if direction != 0:
                atr_value = atr_values[i]
                anchor = close
                if direction > 0:
                    stop = lowest_values[i] - stop_x * atr_value
                    risk = anchor - stop
                    target = anchor + stop_rr * risk
                else:
                    stop = highest_values[i] + stop_x * atr_value
                    risk = stop - anchor
                    target = anchor - stop_rr * risk
                stop_pct = 100.0 * risk / anchor if anchor > 0.0 else math.inf
                risk_cash = balance * risk_per_trade / 100.0
                raw_size = risk_cash / risk if risk > 0.0 else 0.0
                order_size = math.floor(raw_size / contract_size) * contract_size
                if (
                    not math.isnan(stop)
                    and not math.isnan(risk)
                    and risk > 0.0
                    and stop_pct <= stop_max_pct
                    and not math.isnan(order_size)
                    and order_size > 0.0
                ):
                    pending_entry = True
                    pending_direction = direction
                    pending_anchor = anchor
                    pending_stop = stop
                    pending_risk = risk
                    pending_target = target
                    pending_size = order_size

        unrealized = 0.0
        if position > 0:
            unrealized = (close - entry_price) * size
        elif position < 0:
            unrealized = (entry_price - close) * size
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
                    monthly_sum3 += monthly_return ** 3
                    monthly_sum4 += monthly_return ** 4
                current_month = month_key
                month_start_equity = equity_value

        if balance >= running_peak:
            if i > last_drawdown_boundary + 1 and current_drawdown > max_drawdown:
                max_drawdown = current_drawdown
            running_peak = balance
            current_drawdown = 0.0
            last_drawdown_boundary = i
        elif running_peak > 0.0:
            drawdown = (1.0 - balance / running_peak) * 100.0
            if drawdown > current_drawdown:
                current_drawdown = drawdown

        previous_close_position = position

    if last_bar_index > last_drawdown_boundary + 1 and current_drawdown > max_drawdown:
        max_drawdown = current_drawdown

    net_profit_pct = (
        (balance - initial_capital) / initial_capital * 100.0
        if initial_capital != 0.0
        else 0.0
    )
    win_rate = winning_trades / total_trades * 100.0 if total_trades else 0.0
    avg_win = gross_profit / winning_trades if winning_trades else 0.0
    avg_loss = gross_loss / losing_trades if losing_trades else 0.0
    if total_trades == 0:
        profit_factor = math.nan
    elif gross_loss > 0.0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0.0:
        profit_factor = math.inf
    else:
        profit_factor = 1.0
    if abs(max_drawdown) < 1e-9:
        romad = net_profit_pct * 100.0 if net_profit_pct >= 0.0 else 0.0
    else:
        romad = net_profit_pct / abs(max_drawdown)

    sharpe_ratio = math.nan
    skewness = math.nan
    kurtosis = math.nan
    if compute_dsr and total_trades > 0:
        if month_start_equity > 0.0:
            monthly_return = ((last_equity / month_start_equity) - 1.0) * 100.0
            monthly_count += 1
            monthly_sum += monthly_return
            monthly_sumsq += monthly_return * monthly_return
            monthly_sum3 += monthly_return ** 3
            monthly_sum4 += monthly_return ** 4
        if monthly_count >= 2:
            mean_return = monthly_sum / monthly_count
            variance = monthly_sumsq / monthly_count - mean_return * mean_return
            if variance < 0.0 and variance > -1e-12:
                variance = 0.0
            if variance > 0.0:
                std_return = math.sqrt(variance)
                sharpe_ratio = (
                    mean_return - (risk_free_rate * 100.0) / 12.0
                ) / std_return
                if monthly_count >= 3:
                    raw_m2 = monthly_sumsq / monthly_count
                    raw_m3 = monthly_sum3 / monthly_count
                    raw_m4 = monthly_sum4 / monthly_count
                    central_m3 = raw_m3 - 3.0 * mean_return * raw_m2 + 2.0 * mean_return ** 3
                    central_m4 = (
                        raw_m4
                        - 4.0 * mean_return * raw_m3
                        + 6.0 * mean_return * mean_return * raw_m2
                        - 3.0 * mean_return ** 4
                    )
                    skewness = central_m3 / (std_return ** 3)
                    kurtosis = central_m4 / (variance * variance)

    return (
        net_profit_pct,
        max_drawdown,
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
        skewness,
        kurtosis,
    )


if NUMBA_AVAILABLE:
    _S06_FAST_LOOP = numba.njit(cache=True)(_s06_fast_loop_impl)
else:  # pragma: no cover
    _S06_FAST_LOOP = None


def _s06_fast_batch_loop_impl(
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    month_ids: np.ndarray,
    atr_values: np.ndarray,
    lowest_stack: np.ndarray,
    highest_stack: np.ndarray,
    signal_long_stack: np.ndarray,
    signal_short_stack: np.ndarray,
    trail_ma_stack: np.ndarray,
    trade_start_idx: int,
    use_date_filter: bool,
    start_ns: int,
    end_ns: int,
    enable_long: bool,
    enable_short: bool,
    use_trails: np.ndarray,
    signal_indices: np.ndarray,
    stop_lp_indices: np.ndarray,
    trail_ma_indices: np.ndarray,
    stop_x_values: np.ndarray,
    stop_rr_values: np.ndarray,
    stop_max_pct_values: np.ndarray,
    stop_max_days_values: np.ndarray,
    risk_per_trade: float,
    contract_size: float,
    trail_rr_values: np.ndarray,
    trail_offset_values: np.ndarray,
    initial_capital: float,
    commission_pct: float,
    compute_dsr: bool,
    risk_free_rate: float,
    outputs: np.ndarray,
) -> None:
    dummy_i = np.empty(1, dtype=np.int64)
    dummy_f = np.empty(1, dtype=np.float64)
    for index in numba.prange(use_trails.shape[0]):
        ma_index = trail_ma_indices[index]
        if ma_index < 0:
            ma_index = 0
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
            skewness,
            kurtosis,
        ) = _S06_FAST_LOOP(
            open_values,
            high_values,
            low_values,
            close_values,
            timestamp_ns,
            month_ids,
            atr_values,
            lowest_stack[stop_lp_indices[index]],
            highest_stack[stop_lp_indices[index]],
            signal_long_stack[signal_indices[index]],
            signal_short_stack[signal_indices[index]],
            trail_ma_stack[ma_index],
            trade_start_idx,
            use_date_filter,
            start_ns,
            end_ns,
            enable_long,
            enable_short,
            use_trails[index],
            stop_x_values[index],
            stop_rr_values[index],
            stop_max_pct_values[index],
            stop_max_days_values[index],
            risk_per_trade,
            contract_size,
            trail_rr_values[index],
            trail_offset_values[index],
            initial_capital,
            commission_pct,
            compute_dsr,
            risk_free_rate,
            False,
            dummy_i,
            dummy_i,
            dummy_i,
            dummy_f,
            dummy_f,
            dummy_f,
            dummy_f,
        )
        outputs[index, 0] = net_profit_pct
        outputs[index, 1] = max_drawdown_pct
        outputs[index, 2] = total_trades
        outputs[index, 3] = winning_trades
        outputs[index, 4] = losing_trades
        outputs[index, 5] = win_rate
        outputs[index, 6] = gross_profit
        outputs[index, 7] = gross_loss
        outputs[index, 8] = profit_factor
        outputs[index, 9] = romad
        outputs[index, 10] = avg_win
        outputs[index, 11] = avg_loss
        outputs[index, 12] = max_consecutive_losses
        outputs[index, 13] = sharpe_ratio
        outputs[index, 14] = monthly_count
        outputs[index, 15] = skewness
        outputs[index, 16] = kurtosis


if NUMBA_AVAILABLE:
    _S06_FAST_BATCH_LOOP = numba.njit(cache=True, parallel=True)(_s06_fast_batch_loop_impl)
else:  # pragma: no cover
    _S06_FAST_BATCH_LOOP = None


def _timestamp_ns(value: Any, default: int) -> int:
    if value in (None, ""):
        return default
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return int(timestamp.value)


def _pack_candidate_arrays(
    data: FastGridData,
    candidates: Sequence[GridCandidate],
) -> Tuple[np.ndarray, ...]:
    count = len(candidates)
    signal_index_by_key = {key: index for index, key in enumerate(data.signal_pairs)}
    stop_lp_index_by_value = {
        value: index for index, value in enumerate(data.stop_lp_values)
    }
    trail_ma_index_by_key = {
        key: index for index, key in enumerate(data.trail_ma_keys)
    }
    use_trails = np.empty(count, dtype=np.bool_)
    signal_indices = np.empty(count, dtype=np.int64)
    stop_lp_indices = np.empty(count, dtype=np.int64)
    trail_ma_indices = np.empty(count, dtype=np.int64)
    stop_x_values = np.empty(count, dtype=np.float64)
    stop_rr_values = np.empty(count, dtype=np.float64)
    stop_max_pct_values = np.empty(count, dtype=np.float64)
    stop_max_days_values = np.empty(count, dtype=np.int64)
    trail_rr_values = np.empty(count, dtype=np.float64)
    trail_offset_values = np.empty(count, dtype=np.float64)
    for index, candidate in enumerate(candidates):
        params = candidate.params
        use_trails[index] = candidate.mode == "trail"
        signal_indices[index] = signal_index_by_key[
            (int(params["thresholdOS"]), int(params["thresholdOB"]))
        ]
        stop_lp_indices[index] = stop_lp_index_by_value[int(params["stopLP"])]
        trail_ma_indices[index] = (
            trail_ma_index_by_key[(str(params["trailMAType"]), int(params["trailMALength"]))]
            if candidate.mode == "trail"
            else -1
        )
        stop_x_values[index] = float(params["stopX"])
        stop_rr_values[index] = float(params["stopRR"])
        stop_max_pct_values[index] = float(params["stopMaxPct"])
        stop_max_days_values[index] = int(params["stopMaxDays"])
        trail_rr_values[index] = float(params["trailRR"])
        trail_offset_values[index] = float(params["trailMAOffsetEx"])
    return (
        use_trails,
        signal_indices,
        stop_lp_indices,
        trail_ma_indices,
        stop_x_values,
        stop_rr_values,
        stop_max_pct_values,
        stop_max_days_values,
        trail_rr_values,
        trail_offset_values,
    )


def _result_from_values(
    data: FastGridData,
    candidate: GridCandidate,
    values: Sequence[Any],
    *,
    needs_dsr: bool,
) -> FastGridResult:
    profit_factor = float(values[8])
    sharpe_ratio = float(values[13])
    return FastGridResult(
        params=EMPTY_PARAMS,
        net_profit_pct=float(values[0]),
        max_drawdown_pct=float(values[1]),
        total_trades=int(values[2]),
        winning_trades=int(values[3]),
        losing_trades=int(values[4]),
        win_rate=float(values[5]),
        gross_profit=float(values[6]),
        gross_loss=float(values[7]),
        profit_factor=None if math.isnan(profit_factor) else profit_factor,
        romad=float(values[9]),
        avg_win=float(values[10]),
        avg_loss=float(values[11]),
        max_consecutive_losses=int(values[12]),
        sharpe_ratio=None if math.isnan(sharpe_ratio) else sharpe_ratio,
        score=0.0,
        optuna_trial_number=candidate.candidate_id,
        candidate_id=candidate.candidate_id,
        semantic_key=candidate.semantic_key,
        param_key=candidate.semantic_key,
        grid_mode_name=candidate.mode,
        grid_generation_mode="full",
        diversity_group=candidate.diversity_group,
        candidate_source=data.candidate_source,
        dsr_track_length=int(values[14]) if needs_dsr else None,
        dsr_skewness=(
            None if not needs_dsr or math.isnan(float(values[15])) else float(values[15])
        ),
        dsr_kurtosis=(
            None if not needs_dsr or math.isnan(float(values[16])) else float(values[16])
        ),
    )


def _compile_scalar_kernel(
    data: FastGridData,
    candidate: GridCandidate,
    *,
    needs_dsr: bool,
) -> None:
    """Compile/load the scalar kernel before Numba compiles the parallel wrapper."""
    packed = _pack_candidate_arrays(data, [candidate])
    fixed = data.fixed_values
    dummy_i = np.empty(1, dtype=np.int64)
    dummy_f = np.empty(1, dtype=np.float64)
    ma_index = max(0, int(packed[3][0]))
    _S06_FAST_LOOP(
        data.open_values,
        data.high_values,
        data.low_values,
        data.close_values,
        data.timestamp_ns,
        data.month_ids,
        data.atr_values,
        data.lowest_stack[int(packed[2][0])],
        data.highest_stack[int(packed[2][0])],
        data.signal_long_stack[int(packed[1][0])],
        data.signal_short_stack[int(packed[1][0])],
        data.trail_ma_stack[ma_index],
        data.trade_start_idx,
        _coerce_bool(fixed.get("dateFilter"), False),
        _timestamp_ns(fixed.get("start"), np.iinfo(np.int64).min),
        _timestamp_ns(fixed.get("end"), np.iinfo(np.int64).max),
        _coerce_bool(fixed.get("enableLong"), True),
        _coerce_bool(fixed.get("enableShort"), True),
        bool(packed[0][0]),
        float(packed[4][0]),
        float(packed[5][0]),
        float(packed[6][0]),
        int(packed[7][0]),
        float(fixed.get("riskPerTrade", 2.0)),
        float(fixed.get("contractSize", 0.01)),
        float(packed[8][0]),
        float(packed[9][0]),
        float(fixed.get("initialCapital", 100.0)),
        float(fixed.get("commissionPct", 0.05)),
        bool(needs_dsr),
        0.02,
        False,
        dummy_i,
        dummy_i,
        dummy_i,
        dummy_f,
        dummy_f,
        dummy_f,
        dummy_f,
    )


def evaluate_candidates(
    data: FastGridData,
    candidates: Sequence[GridCandidate],
    *,
    n_workers: int = 1,
    needs_dsr: bool = False,
) -> List[FastGridResult]:
    if not candidates:
        return []
    if _S06_FAST_BATCH_LOOP is None:
        raise RuntimeError(f"Numba is not available: {NUMBA_IMPORT_ERROR}")
    _compile_scalar_kernel(data, candidates[0], needs_dsr=needs_dsr)
    packed = _pack_candidate_arrays(data, candidates)
    outputs = np.empty((len(candidates), 17), dtype=np.float64)
    fixed = data.fixed_values
    previous_threads = numba.get_num_threads()
    target_threads = max(1, min(int(n_workers or 1), previous_threads))
    try:
        if target_threads != previous_threads:
            numba.set_num_threads(target_threads)
        _S06_FAST_BATCH_LOOP(
            data.open_values,
            data.high_values,
            data.low_values,
            data.close_values,
            data.timestamp_ns,
            data.month_ids,
            data.atr_values,
            data.lowest_stack,
            data.highest_stack,
            data.signal_long_stack,
            data.signal_short_stack,
            data.trail_ma_stack,
            data.trade_start_idx,
            _coerce_bool(fixed.get("dateFilter"), False),
            _timestamp_ns(fixed.get("start"), np.iinfo(np.int64).min),
            _timestamp_ns(fixed.get("end"), np.iinfo(np.int64).max),
            _coerce_bool(fixed.get("enableLong"), True),
            _coerce_bool(fixed.get("enableShort"), True),
            packed[0],
            packed[1],
            packed[2],
            packed[3],
            packed[4],
            packed[5],
            packed[6],
            packed[7],
            float(fixed.get("riskPerTrade", 2.0)),
            float(fixed.get("contractSize", 0.01)),
            packed[8],
            packed[9],
            float(fixed.get("initialCapital", 100.0)),
            float(fixed.get("commissionPct", 0.05)),
            bool(needs_dsr),
            0.02,
            outputs,
        )
    finally:
        if numba.get_num_threads() != previous_threads:
            numba.set_num_threads(previous_threads)
    return [
        _result_from_values(data, candidate, outputs[index], needs_dsr=needs_dsr)
        for index, candidate in enumerate(candidates)
    ]


def evaluate_candidate_trace(
    data: FastGridData,
    candidate: GridCandidate,
) -> List[Dict[str, Any]]:
    if _S06_FAST_LOOP is None:
        raise RuntimeError(f"Numba is not available: {NUMBA_IMPORT_ERROR}")
    packed = _pack_candidate_arrays(data, [candidate])
    fixed = data.fixed_values
    max_trades = max(1, len(data.df))
    direction = np.empty(max_trades, dtype=np.int64)
    entry_index = np.empty(max_trades, dtype=np.int64)
    exit_index = np.empty(max_trades, dtype=np.int64)
    entry_price = np.empty(max_trades, dtype=np.float64)
    exit_price = np.empty(max_trades, dtype=np.float64)
    size = np.empty(max_trades, dtype=np.float64)
    net_pnl = np.empty(max_trades, dtype=np.float64)
    ma_index = int(packed[3][0])
    values = _S06_FAST_LOOP(
        data.open_values,
        data.high_values,
        data.low_values,
        data.close_values,
        data.timestamp_ns,
        data.month_ids,
        data.atr_values,
        data.lowest_stack[int(packed[2][0])],
        data.highest_stack[int(packed[2][0])],
        data.signal_long_stack[int(packed[1][0])],
        data.signal_short_stack[int(packed[1][0])],
        data.trail_ma_stack[max(0, ma_index)],
        data.trade_start_idx,
        _coerce_bool(fixed.get("dateFilter"), False),
        _timestamp_ns(fixed.get("start"), np.iinfo(np.int64).min),
        _timestamp_ns(fixed.get("end"), np.iinfo(np.int64).max),
        _coerce_bool(fixed.get("enableLong"), True),
        _coerce_bool(fixed.get("enableShort"), True),
        bool(packed[0][0]),
        float(packed[4][0]),
        float(packed[5][0]),
        float(packed[6][0]),
        int(packed[7][0]),
        float(fixed.get("riskPerTrade", 2.0)),
        float(fixed.get("contractSize", 0.01)),
        float(packed[8][0]),
        float(packed[9][0]),
        float(fixed.get("initialCapital", 100.0)),
        float(fixed.get("commissionPct", 0.05)),
        False,
        0.02,
        True,
        direction,
        entry_index,
        exit_index,
        entry_price,
        exit_price,
        size,
        net_pnl,
    )
    return [
        {
            "direction": "long" if direction[index] > 0 else "short",
            "entry_index": int(entry_index[index]),
            "exit_index": int(exit_index[index]),
            "entry_time": data.df.index[int(entry_index[index])],
            "exit_time": data.df.index[int(exit_index[index])],
            "entry_price": float(entry_price[index]),
            "exit_price": float(exit_price[index]),
            "size": float(size[index]),
            "net_pnl": float(net_pnl[index]),
        }
        for index in range(int(values[2]))
    ]


def _result_metric_dict(result: Any) -> Dict[str, Any]:
    return {
        "net_profit_pct": result.net_profit_pct,
        "max_drawdown_pct": result.max_drawdown_pct,
        "total_trades": result.total_trades,
        "winning_trades": result.winning_trades,
        "losing_trades": result.losing_trades,
        "win_rate": result.win_rate,
        "gross_profit": result.gross_profit,
        "gross_loss": result.gross_loss,
        "profit_factor": result.profit_factor,
        "romad": result.romad,
        "max_consecutive_losses": result.max_consecutive_losses,
    }


def _profit_factor_matches(fast_value: Any, slow_value: Any) -> bool:
    if fast_value is None and slow_value is None:
        return True
    try:
        fast_float = float(fast_value)
        slow_float = float(slow_value)
    except (TypeError, ValueError):
        return False
    if math.isinf(fast_float) or math.isinf(slow_float):
        return (
            math.isinf(fast_float)
            and math.isinf(slow_float)
            and (fast_float > 0) == (slow_float > 0)
        )
    return abs(fast_float - slow_float) <= max(
        1e-6,
        1e-4 * max(abs(fast_float), abs(slow_float), 1.0),
    )


def _validation_diffs(
    fast: Any,
    slow: OptimizationResult,
    tolerances: Mapping[str, float],
) -> Tuple[bool, Dict[str, Any]]:
    checks = (
        ("net_profit_pct", "net_profit_pct_abs"),
        ("max_drawdown_pct", "max_drawdown_pct_abs"),
        ("romad", "romad_abs"),
        ("win_rate", "win_rate_abs"),
        ("total_trades", "total_trades_abs"),
        ("winning_trades", "winning_trades_abs"),
        ("losing_trades", "losing_trades_abs"),
        ("max_consecutive_losses", "max_consecutive_losses_abs"),
    )
    diffs: Dict[str, Any] = {}
    ok = True
    for attribute, tolerance_key in checks:
        fast_value = getattr(fast, attribute, None)
        slow_value = getattr(slow, attribute, None)
        difference = abs(float(fast_value or 0.0) - float(slow_value or 0.0))
        tolerance = float(tolerances.get(tolerance_key, 0.0))
        passed = difference <= tolerance
        ok = ok and passed
        diffs[attribute] = {
            "fast": fast_value,
            "slow": slow_value,
            "diff": difference,
            "tolerance": tolerance,
            "passed": passed,
        }
    for attribute in ("gross_profit", "gross_loss"):
        fast_value = float(getattr(fast, attribute, 0.0) or 0.0)
        slow_value = float(getattr(slow, attribute, 0.0) or 0.0)
        difference = abs(fast_value - slow_value)
        tolerance = max(1e-7, 1e-7 * max(abs(fast_value), abs(slow_value), 1.0))
        passed = difference <= tolerance
        ok = ok and passed
        diffs[attribute] = {
            "fast": fast_value,
            "slow": slow_value,
            "diff": difference,
            "tolerance": tolerance,
            "passed": passed,
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
    selected_fast: Sequence[FastGridResult],
    *,
    tolerances: Dict[str, float],
    fail_on_error: bool,
) -> List[OptimizationResult]:
    validated: List[OptimizationResult] = []
    for fast_result in selected_fast:
        candidate = fast_result.candidate_source[fast_result.candidate_id - 1]
        params = candidate.params
        slow_result = _run_single_combination(
            (params, df, int(trade_start_idx), S06RTrendV02)
        )
        candidate_id = fast_result.candidate_id
        slow_result.optuna_trial_number = candidate_id
        for attribute in (
            "candidate_id",
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
        ):
            setattr(slow_result, attribute, getattr(fast_result, attribute, None))
        setattr(slow_result, "fast_metrics", _result_metric_dict(fast_result))
        ok, diffs = _validation_diffs(fast_result, slow_result, tolerances)
        setattr(slow_result, "validation_status", "passed" if ok else "failed")
        setattr(slow_result, "validation_diffs", diffs)
        if not ok and fail_on_error:
            payload = {
                "candidate_id": candidate_id,
                "mode": candidate.mode,
                "semantic_key": candidate.semantic_key,
                "params": params,
                "fast_metrics": _result_metric_dict(fast_result),
                "slow_metrics": _result_metric_dict(slow_result),
                "diffs": diffs,
            }
            raise ValueError(
                "Grid fast-vs-slow validation failed: "
                + json.dumps(payload, default=str, sort_keys=True)
            )
        validated.append(slow_result)
    return validated
