"""Compiled batch evaluator for Backtester V2 execution profiles.

The public entry point packs generic V2 execution data and ``KernelConfig``
values into primitive NumPy arrays, then evaluates independent candidates in a
Numba-compiled loop. Strategy-specific signal/dataprep logic stays outside this
module.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .kernel import ExecutionData
from .price_rounding import PRICE_ROUNDING_NONE, PRICE_ROUNDING_TICK_OUTWARD, validate_tick_size
from .profile import active_mode_values


try:  # pragma: no cover - exercised indirectly when Numba is absent
    import numba

    NUMBA_IMPORT_ERROR: str | None = None
except Exception as exc:  # pragma: no cover
    numba = None
    NUMBA_IMPORT_ERROR = str(exc)


COMPILED_BATCH_KIND = "compiled_numba"
REFERENCE_UNAVAILABLE_REASON = "Numba is unavailable or disabled."

ROUNDING_NONE_CODE = 0
ROUNDING_TICK_OUTWARD_CODE = 1

OUTPUT_NET_PROFIT_PCT = 0
OUTPUT_MAX_DRAWDOWN_PCT = 1
OUTPUT_TOTAL_TRADES = 2
OUTPUT_WINNING_TRADES = 3
OUTPUT_LOSING_TRADES = 4
OUTPUT_WIN_RATE_PCT = 5
OUTPUT_GROSS_PROFIT = 6
OUTPUT_GROSS_LOSS = 7
OUTPUT_PROFIT_FACTOR = 8
OUTPUT_ROMAD = 9
OUTPUT_FINAL_BALANCE = 10
OUTPUT_MAX_CONSECUTIVE_LOSSES = 11
OUTPUT_INVALID_STOP_DISTANCE_COUNT = 12
OUTPUT_ZERO_SIZE_ENTRY_COUNT = 13
OUTPUT_REJECTED_FILL_COUNT = 14
OUTPUT_MARGIN_REJECT_COUNT = 15
OUTPUT_LIQUIDATION_COUNT = 16
OUTPUT_NO_CAPITAL_HALT = 17
OUTPUT_MAX_REQUIRED_LEVERAGE = 18
OUTPUT_MAX_NOTIONAL = 19
OUTPUT_FLAGS = 20
OUTPUT_COLUMN_COUNT = 21

GUARDRAIL_FLAG_REJECTED_FILL = 2
GUARDRAIL_FLAG_INVALID_STOP_DISTANCE = 4
GUARDRAIL_FLAG_ZERO_SIZE_ENTRY = 8


@dataclass(frozen=True)
class CompiledBatchOutput:
    """Fixed-width compiled metrics for one compiled batch."""

    outputs: np.ndarray
    backend_kind: str = COMPILED_BATCH_KIND
    execution_mode: str = "grouped"


@dataclass(frozen=True)
class StackedExecutionData:
    """Shared market arrays plus stacked signal/dataprep rows for compiled Grid V2."""

    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    timestamp_ns: np.ndarray
    long_entries: np.ndarray
    short_entries: np.ndarray
    atr: np.ndarray
    rolling_low: np.ndarray
    rolling_high: np.ndarray
    trail_long: np.ndarray
    trail_short: np.ndarray
    data_index: np.ndarray

    @property
    def row_count(self) -> int:
        return int(self.long_entries.shape[0])

    @property
    def candidate_count(self) -> int:
        return int(self.data_index.shape[0])

    @property
    def shared_market_nbytes(self) -> int:
        return int(
            self.open.nbytes
            + self.high.nbytes
            + self.low.nbytes
            + self.close.nbytes
            + self.timestamp_ns.nbytes
        )

    @property
    def signal_nbytes(self) -> int:
        return int(self.long_entries.nbytes + self.short_entries.nbytes)

    @property
    def dataprep_nbytes(self) -> int:
        return int(
            self.atr.nbytes
            + self.rolling_low.nbytes
            + self.rolling_high.nbytes
            + self.trail_long.nbytes
            + self.trail_short.nbytes
        )

    @property
    def nbytes(self) -> int:
        return int(self.shared_market_nbytes + self.signal_nbytes + self.dataprep_nbytes)


def compiled_batch_available() -> bool:
    """Return whether the compiled batch path can run in this process."""

    if numba is None:
        return False
    return str(os.environ.get("NUMBA_DISABLE_JIT", "")).strip() not in {"1", "true", "True"}


def compiled_unavailable_reason() -> str | None:
    if numba is None:
        return NUMBA_IMPORT_ERROR or REFERENCE_UNAVAILABLE_REASON
    if not compiled_batch_available():
        return "NUMBA_DISABLE_JIT is set."
    return None


def evaluate_compiled_batch(
    *,
    data: ExecutionData,
    profile: Any,
    params_batch: Sequence[Mapping[str, Any]],
    trade_start_idx: int,
    n_workers: int = 1,
) -> CompiledBatchOutput:
    """Evaluate one batch of candidates sharing the same ``ExecutionData``."""

    if not compiled_batch_available():
        raise RuntimeError(compiled_unavailable_reason() or REFERENCE_UNAVAILABLE_REASON)
    if not params_batch:
        return CompiledBatchOutput(outputs=np.empty((0, OUTPUT_COLUMN_COUNT), dtype=np.float64))

    packed = _pack_config_arrays(profile, params_batch, trade_start_idx)
    outputs = np.empty((len(params_batch), OUTPUT_COLUMN_COUNT), dtype=np.float64)
    worker_count = _validated_worker_count(n_workers)
    previous_threads = numba.get_num_threads()
    target_threads = max(1, min(worker_count, previous_threads))
    try:
        if target_threads != previous_threads:
            numba.set_num_threads(target_threads)
        _COMPILED_BATCH_LOOP(
            np.asarray(data.open, dtype=np.float64),
            np.asarray(data.high, dtype=np.float64),
            np.asarray(data.low, dtype=np.float64),
            np.asarray(data.close, dtype=np.float64),
            _timestamps_ns(data.timestamps),
            np.asarray(data.signals.long_entries, dtype=np.bool_),
            np.asarray(data.signals.short_entries, dtype=np.bool_),
            np.asarray(data.atr, dtype=np.float64),
            np.asarray(data.rolling_low, dtype=np.float64),
            np.asarray(data.rolling_high, dtype=np.float64),
            np.asarray(data.trail_long, dtype=np.float64),
            np.asarray(data.trail_short, dtype=np.float64),
            int(trade_start_idx),
            packed["initial_capital"],
            packed["commission_pct"],
            packed["stop_x"],
            packed["reward_risk"],
            packed["max_stop_pct"],
            packed["max_days"],
            packed["risk_per_trade_pct"],
            packed["contract_size"],
            packed["trail_activation_rr"],
            packed["tick_size"],
            packed["start_ns"],
            packed["end_ns"],
            packed["enable_long"],
            packed["enable_short"],
            packed["target_enabled"],
            packed["trail_enabled"],
            packed["max_days_enabled"],
            packed["use_date_filter"],
            packed["strict_boundary"],
            packed["boundary_none"],
            packed["report_margin"],
            packed["rounding_code"],
            outputs,
        )
    finally:
        if numba.get_num_threads() != previous_threads:
            numba.set_num_threads(previous_threads)
    return CompiledBatchOutput(outputs=outputs)


def build_stacked_execution_data(
    data_rows: Sequence[ExecutionData],
    data_index: Sequence[int],
) -> StackedExecutionData:
    """Build and validate a stacked compiled payload from execution-data rows."""

    rows = tuple(data_rows)
    if not rows:
        raise ValueError("Stacked compiled execution requires at least one ExecutionData row.")

    first = rows[0]
    open_values = _contiguous_1d(first.open, "open", np.float64)
    high_values = _contiguous_1d(first.high, "high", np.float64)
    low_values = _contiguous_1d(first.low, "low", np.float64)
    close_values = _contiguous_1d(first.close, "close", np.float64)
    timestamp_ns = _timestamps_ns(first.timestamps)

    long_rows: list[np.ndarray] = []
    short_rows: list[np.ndarray] = []
    atr_rows: list[np.ndarray] = []
    rolling_low_rows: list[np.ndarray] = []
    rolling_high_rows: list[np.ndarray] = []
    trail_long_rows: list[np.ndarray] = []
    trail_short_rows: list[np.ndarray] = []

    for row_number, data in enumerate(rows):
        row_timestamps = _timestamps_ns(data.timestamps)
        if row_timestamps.shape != timestamp_ns.shape or not np.array_equal(row_timestamps, timestamp_ns):
            raise ValueError(
                "Stacked compiled execution requires shared OHLC/timestamps across all ExecutionData rows; "
                f"timestamp mismatch at row {row_number}."
            )
        for name, expected in (
            ("open", open_values),
            ("high", high_values),
            ("low", low_values),
            ("close", close_values),
        ):
            actual = _contiguous_1d(getattr(data, name), name, np.float64)
            if actual.shape != expected.shape or not np.array_equal(actual, expected, equal_nan=True):
                raise ValueError(
                    "Stacked compiled execution requires shared OHLC/timestamps across all ExecutionData rows; "
                    f"{name} mismatch at row {row_number}."
                )
        long_rows.append(_contiguous_1d(data.signals.long_entries, "signals.long_entries", np.bool_))
        short_rows.append(_contiguous_1d(data.signals.short_entries, "signals.short_entries", np.bool_))
        atr_rows.append(_contiguous_1d(data.atr, "atr", np.float64))
        rolling_low_rows.append(_contiguous_1d(data.rolling_low, "rolling_low", np.float64))
        rolling_high_rows.append(_contiguous_1d(data.rolling_high, "rolling_high", np.float64))
        trail_long_rows.append(_contiguous_1d(data.trail_long, "trail_long", np.float64))
        trail_short_rows.append(_contiguous_1d(data.trail_short, "trail_short", np.float64))

    index_array = np.asarray(data_index, dtype=np.int64)
    if index_array.ndim != 1:
        raise ValueError("Stacked compiled execution data_index must be a 1D array.")
    if index_array.size:
        if int(index_array.min()) < 0 or int(index_array.max()) >= len(rows):
            raise ValueError("Stacked compiled execution data_index contains an out-of-range row index.")

    return StackedExecutionData(
        open=open_values,
        high=high_values,
        low=low_values,
        close=close_values,
        timestamp_ns=timestamp_ns,
        long_entries=np.ascontiguousarray(np.stack(long_rows, axis=0), dtype=np.bool_),
        short_entries=np.ascontiguousarray(np.stack(short_rows, axis=0), dtype=np.bool_),
        atr=np.ascontiguousarray(np.stack(atr_rows, axis=0), dtype=np.float64),
        rolling_low=np.ascontiguousarray(np.stack(rolling_low_rows, axis=0), dtype=np.float64),
        rolling_high=np.ascontiguousarray(np.stack(rolling_high_rows, axis=0), dtype=np.float64),
        trail_long=np.ascontiguousarray(np.stack(trail_long_rows, axis=0), dtype=np.float64),
        trail_short=np.ascontiguousarray(np.stack(trail_short_rows, axis=0), dtype=np.float64),
        data_index=np.ascontiguousarray(index_array, dtype=np.int64),
    )


def evaluate_compiled_stacked_batch(
    *,
    stacked_data: StackedExecutionData,
    profile: Any,
    params_batch: Sequence[Mapping[str, Any]],
    trade_start_idx: int,
    n_workers: int = 1,
) -> CompiledBatchOutput:
    """Evaluate one stacked compiled batch with per-candidate data row indices."""

    if not compiled_batch_available():
        raise RuntimeError(compiled_unavailable_reason() or REFERENCE_UNAVAILABLE_REASON)
    if not params_batch:
        return CompiledBatchOutput(
            outputs=np.empty((0, OUTPUT_COLUMN_COUNT), dtype=np.float64),
            execution_mode="stacked",
        )
    if len(params_batch) != stacked_data.candidate_count:
        raise ValueError("Stacked compiled params_batch length must match data_index length.")

    packed = _pack_config_arrays(profile, params_batch, trade_start_idx)
    outputs = np.empty((len(params_batch), OUTPUT_COLUMN_COUNT), dtype=np.float64)
    worker_count = _validated_worker_count(n_workers)
    previous_threads = numba.get_num_threads()
    target_threads = max(1, min(worker_count, previous_threads))
    try:
        if target_threads != previous_threads:
            numba.set_num_threads(target_threads)
        _COMPILED_STACKED_BATCH_LOOP(
            stacked_data.open,
            stacked_data.high,
            stacked_data.low,
            stacked_data.close,
            stacked_data.timestamp_ns,
            stacked_data.long_entries,
            stacked_data.short_entries,
            stacked_data.atr,
            stacked_data.rolling_low,
            stacked_data.rolling_high,
            stacked_data.trail_long,
            stacked_data.trail_short,
            stacked_data.data_index,
            int(trade_start_idx),
            packed["initial_capital"],
            packed["commission_pct"],
            packed["stop_x"],
            packed["reward_risk"],
            packed["max_stop_pct"],
            packed["max_days"],
            packed["risk_per_trade_pct"],
            packed["contract_size"],
            packed["trail_activation_rr"],
            packed["tick_size"],
            packed["start_ns"],
            packed["end_ns"],
            packed["enable_long"],
            packed["enable_short"],
            packed["target_enabled"],
            packed["trail_enabled"],
            packed["max_days_enabled"],
            packed["use_date_filter"],
            packed["strict_boundary"],
            packed["boundary_none"],
            packed["report_margin"],
            packed["rounding_code"],
            outputs,
        )
    finally:
        if numba.get_num_threads() != previous_threads:
            numba.set_num_threads(previous_threads)
    return CompiledBatchOutput(outputs=outputs, execution_mode="stacked")


def _contiguous_1d(values: Any, name: str, dtype: Any) -> np.ndarray:
    array = np.asarray(values, dtype=dtype)
    if array.ndim != 1:
        raise ValueError(f"{name} must be a 1D array for stacked compiled execution.")
    return np.ascontiguousarray(array, dtype=dtype)


def _validated_worker_count(value: Any) -> int:
    error = "Compiled Grid V2 n_workers must be a positive integer."
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(error)
    if isinstance(value, (int, np.integer)):
        workers = int(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped or not stripped.isdecimal():
            raise ValueError(error)
        workers = int(stripped)
    elif isinstance(value, (float, np.floating)):
        numeric = float(value)
        if not math.isfinite(numeric) or not numeric.is_integer():
            raise ValueError(error)
        workers = int(numeric)
    else:
        raise ValueError(error)
    if workers < 1:
        raise ValueError(error)
    return workers


def _pack_config_arrays(
    profile: Any,
    params_batch: Sequence[Mapping[str, Any]],
    trade_start_idx: int,
) -> dict[str, np.ndarray]:
    count = len(params_batch)
    arrays = {
        "initial_capital": np.empty(count, dtype=np.float64),
        "commission_pct": np.empty(count, dtype=np.float64),
        "stop_x": np.empty(count, dtype=np.float64),
        "reward_risk": np.empty(count, dtype=np.float64),
        "max_stop_pct": np.empty(count, dtype=np.float64),
        "max_days": np.empty(count, dtype=np.float64),
        "risk_per_trade_pct": np.empty(count, dtype=np.float64),
        "contract_size": np.empty(count, dtype=np.float64),
        "trail_activation_rr": np.empty(count, dtype=np.float64),
        "tick_size": np.empty(count, dtype=np.float64),
        "start_ns": np.empty(count, dtype=np.int64),
        "end_ns": np.empty(count, dtype=np.int64),
        "enable_long": np.empty(count, dtype=np.bool_),
        "enable_short": np.empty(count, dtype=np.bool_),
        "target_enabled": np.empty(count, dtype=np.bool_),
        "trail_enabled": np.empty(count, dtype=np.bool_),
        "max_days_enabled": np.empty(count, dtype=np.bool_),
        "use_date_filter": np.empty(count, dtype=np.bool_),
        "strict_boundary": np.empty(count, dtype=np.bool_),
        "boundary_none": np.empty(count, dtype=np.bool_),
        "report_margin": np.empty(count, dtype=np.bool_),
        "rounding_code": np.empty(count, dtype=np.int64),
    }
    mode_cache: dict[tuple[tuple[str, str], ...], tuple[bool, bool, bool, bool, bool, bool, int]] = {}
    start_ns_cache: dict[Any, int] = {}
    end_ns_cache: dict[Any, int] = {}
    for index, params in enumerate(params_batch):
        modes = active_mode_values(profile, params)
        mode_key = tuple(sorted((str(key), str(value)) for key, value in modes.items()))
        mode_state = mode_cache.get(mode_key)
        if mode_state is None:
            mode_state = _compiled_mode_state(modes)
            mode_cache[mode_key] = mode_state
        (
            target_enabled,
            trail_enabled,
            max_days_enabled,
            strict_boundary,
            boundary_none,
            report_margin,
            rounding_code,
        ) = mode_state

        arrays["initial_capital"][index] = float(params.get("initialCapital", 100.0))
        arrays["commission_pct"][index] = float(params.get("commissionPct", 0.0))
        arrays["stop_x"][index] = float(params.get("stopX", 2.0))
        arrays["reward_risk"][index] = float(params.get("stopRR", 2.0))
        arrays["max_stop_pct"][index] = float(params.get("stopMaxPct", math.inf))
        arrays["max_days"][index] = float(params.get("stopMaxDays", math.inf))
        arrays["risk_per_trade_pct"][index] = float(params.get("riskPerTrade", 2.0))
        arrays["contract_size"][index] = float(params.get("contractSize", 0.01))
        arrays["trail_activation_rr"][index] = float(params.get("trailRR", 1.0))
        if rounding_code == ROUNDING_TICK_OUTWARD_CODE:
            if "tickSize" not in params:
                raise ValueError("tickSize is required when priceRounding='tick_outward'.")
            arrays["tick_size"][index] = validate_tick_size(float(params["tickSize"]))
        else:
            arrays["tick_size"][index] = math.nan
        arrays["start_ns"][index] = _cached_timestamp_ns(
            params.get("start"),
            np.iinfo(np.int64).min,
            start_ns_cache,
        )
        arrays["end_ns"][index] = _cached_timestamp_ns(
            params.get("end"),
            np.iinfo(np.int64).max,
            end_ns_cache,
        )
        arrays["enable_long"][index] = _coerce_bool(params.get("enableLong"), True)
        arrays["enable_short"][index] = _coerce_bool(params.get("enableShort"), True)
        arrays["target_enabled"][index] = target_enabled
        arrays["trail_enabled"][index] = trail_enabled
        arrays["max_days_enabled"][index] = max_days_enabled
        arrays["use_date_filter"][index] = _coerce_bool(params.get("dateFilter"), True)
        arrays["strict_boundary"][index] = strict_boundary
        arrays["boundary_none"][index] = boundary_none
        arrays["report_margin"][index] = report_margin
        arrays["rounding_code"][index] = rounding_code
    return arrays


def _compiled_mode_state(
    modes: Mapping[str, str],
) -> tuple[bool, bool, bool, bool, bool, bool, int]:
    _require_mode(modes, "entryOrder", "market_next_open")
    _require_mode(modes, "stop", "atr_swing")
    _require_mode(modes, "sizing", "risk_per_trade")

    margin_mode = modes.get("margin", "off")
    if margin_mode not in {"off", "report_only"}:
        raise ValueError(f"Unsupported Phase-1 margin mode: {margin_mode!r}.")

    boundary_mode = modes.get("boundary", "strict_close")
    if boundary_mode not in {"strict_close", "none"}:
        raise ValueError(f"Unsupported Phase-1 boundary mode: {boundary_mode!r}.")

    target_mode = modes.get("target", "none")
    trail_mode = modes.get("trail", "none")
    if target_mode not in {"rr", "none"}:
        raise ValueError(f"Unsupported Phase-1 target mode: {target_mode!r}.")
    if trail_mode not in {"ma", "none"}:
        raise ValueError(f"Unsupported Phase-1 trail mode: {trail_mode!r}.")

    trail_activation_mode = modes.get("trailActivation", "none")
    if trail_activation_mode not in {"none", "rr"}:
        raise ValueError(f"Unsupported Phase-1 trailActivation mode: {trail_activation_mode!r}.")
    valid_target_exit = (
        target_mode == "rr"
        and trail_mode == "none"
        and trail_activation_mode == "none"
    )
    valid_ma_exit = (
        target_mode == "none"
        and trail_mode == "ma"
        and trail_activation_mode == "rr"
    )
    if not (valid_target_exit or valid_ma_exit):
        raise ValueError(
            "Phase 1 supports exactly one exit topology: target=rr with no trailing mode "
            "or target=none with moving-average trailing mode and trailActivation=rr."
        )

    max_days_mode = modes.get("maxDays", "false")
    if max_days_mode == "true":
        max_days_enabled = True
    elif max_days_mode == "false":
        max_days_enabled = False
    else:
        raise ValueError(
            f"Unsupported Phase-1 execution mode maxDays={max_days_mode!r}; "
            "expected 'true' or 'false'."
        )

    return (
        target_mode == "rr",
        trail_mode == "ma",
        max_days_enabled,
        boundary_mode == "strict_close",
        boundary_mode == "none",
        margin_mode == "report_only",
        _rounding_code(modes.get("priceRounding", PRICE_ROUNDING_NONE)),
    )


def _require_mode(modes: Mapping[str, str], name: str, expected: str) -> None:
    actual = modes.get(name)
    if actual != expected:
        raise ValueError(f"Unsupported Phase-1 execution mode {name}={actual!r}; expected {expected!r}.")


def _coerce_bool(value: Any, default: bool) -> bool:
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


def _cached_timestamp_ns(value: Any, default: int, cache: dict[Any, int]) -> int:
    key = _hashable_cache_key(value)
    if key not in cache:
        cache[key] = _timestamp_ns(value, default)
    return cache[key]


def _hashable_cache_key(value: Any) -> Any:
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def _rounding_code(mode: str) -> int:
    if mode == PRICE_ROUNDING_NONE:
        return ROUNDING_NONE_CODE
    if mode == PRICE_ROUNDING_TICK_OUTWARD:
        return ROUNDING_TICK_OUTWARD_CODE
    raise ValueError(f"Unsupported priceRounding mode for compiled V2 batch: {mode!r}.")


def _timestamp_ns(value: Any, default: int) -> int:
    if value in (None, ""):
        return int(default)
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return int(timestamp.value)


def _timestamps_ns(values: Sequence[Any]) -> np.ndarray:
    fast = _timestamps_ns_fast(values)
    if fast is not None:
        return fast
    output = np.empty(len(values), dtype=np.int64)
    for index, value in enumerate(values):
        output[index] = _timestamp_ns(value, 0)
    return output


def _timestamps_ns_fast(values: Sequence[Any]) -> np.ndarray | None:
    if isinstance(values, pd.DatetimeIndex):
        index = values.tz_convert("UTC") if values.tz is not None else values
        return np.asarray(index.asi8, dtype=np.int64).copy()

    array = np.asarray(values)
    if np.issubdtype(array.dtype, np.datetime64):
        return array.astype("datetime64[ns]", copy=False).astype(np.int64, copy=True)

    try:
        index = pd.DatetimeIndex(values)
    except (TypeError, ValueError):
        return None
    if len(index) != len(values) or index.hasnans:
        return None
    index = index.tz_convert("UTC") if index.tz is not None else index
    return np.asarray(index.asi8, dtype=np.int64).copy()


def _compiled_target(func):
    if numba is None:  # pragma: no cover
        return func
    return numba.njit(cache=True)(func)


@_compiled_target
def _scaled_price(price: float, tick_size: float) -> float:
    scaled = price / tick_size
    nearest = round(scaled)
    tolerance = 1e-9
    # Numba does not support math.ulp consistently here, so the compiled path
    # uses a relative float64 epsilon proxy. Certified SUIUSDT.P tick ranges are
    # covered by compiled/reference parity, and selected rows are slow-enriched.
    relative = abs(scaled) * 2.220446049250313e-16 * 8.0
    if relative > tolerance:
        tolerance = relative
    if abs(scaled - nearest) <= tolerance:
        return float(nearest)
    return scaled


@_compiled_target
def _round_floor(price: float, tick_size: float) -> float:
    return math.floor(_scaled_price(price, tick_size)) * tick_size


@_compiled_target
def _round_ceil(price: float, tick_size: float) -> float:
    return math.ceil(_scaled_price(price, tick_size)) * tick_size


@_compiled_target
def _round_stop(direction: int, price: float, tick_size: float) -> float:
    if direction > 0:
        return _round_floor(price, tick_size)
    return _round_ceil(price, tick_size)


@_compiled_target
def _round_target(direction: int, price: float, tick_size: float) -> float:
    if direction > 0:
        return _round_ceil(price, tick_size)
    return _round_floor(price, tick_size)


@_compiled_target
def _round_trail(direction: int, price: float, tick_size: float) -> float:
    if direction > 0:
        return _round_floor(price, tick_size)
    return _round_ceil(price, tick_size)


@_compiled_target
def _maybe_round_stop(direction: int, price: float, rounding_code: int, tick_size: float) -> float:
    if rounding_code == ROUNDING_TICK_OUTWARD_CODE:
        return _round_stop(direction, price, tick_size)
    return price


@_compiled_target
def _maybe_round_target(direction: int, price: float, rounding_code: int, tick_size: float) -> float:
    if rounding_code == ROUNDING_TICK_OUTWARD_CODE:
        return _round_target(direction, price, tick_size)
    return price


@_compiled_target
def _maybe_round_trail(direction: int, price: float, rounding_code: int, tick_size: float) -> float:
    if rounding_code == ROUNDING_TICK_OUTWARD_CODE:
        return _round_trail(direction, price, tick_size)
    return price


@_compiled_target
def _between(start: float, end: float, level: float) -> bool:
    return min(start, end) <= level <= max(start, end)


@_compiled_target
def _write_empty_result(outputs: np.ndarray, index: int, initial_capital: float) -> None:
    outputs[index, OUTPUT_NET_PROFIT_PCT] = 0.0
    outputs[index, OUTPUT_MAX_DRAWDOWN_PCT] = 0.0
    outputs[index, OUTPUT_TOTAL_TRADES] = 0.0
    outputs[index, OUTPUT_WINNING_TRADES] = 0.0
    outputs[index, OUTPUT_LOSING_TRADES] = 0.0
    outputs[index, OUTPUT_WIN_RATE_PCT] = 0.0
    outputs[index, OUTPUT_GROSS_PROFIT] = 0.0
    outputs[index, OUTPUT_GROSS_LOSS] = 0.0
    outputs[index, OUTPUT_PROFIT_FACTOR] = math.nan
    outputs[index, OUTPUT_ROMAD] = 0.0
    outputs[index, OUTPUT_FINAL_BALANCE] = initial_capital
    outputs[index, OUTPUT_MAX_CONSECUTIVE_LOSSES] = 0.0
    outputs[index, OUTPUT_INVALID_STOP_DISTANCE_COUNT] = 0.0
    outputs[index, OUTPUT_ZERO_SIZE_ENTRY_COUNT] = 0.0
    outputs[index, OUTPUT_REJECTED_FILL_COUNT] = 0.0
    outputs[index, OUTPUT_MARGIN_REJECT_COUNT] = 0.0
    outputs[index, OUTPUT_LIQUIDATION_COUNT] = 0.0
    outputs[index, OUTPUT_NO_CAPITAL_HALT] = 0.0
    outputs[index, OUTPUT_MAX_REQUIRED_LEVERAGE] = 0.0
    outputs[index, OUTPUT_MAX_NOTIONAL] = 0.0
    outputs[index, OUTPUT_FLAGS] = 0.0


@_compiled_target
def _compiled_loop_one(
    candidate_index: int,
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    long_signals: np.ndarray,
    short_signals: np.ndarray,
    atr_values: np.ndarray,
    rolling_low: np.ndarray,
    rolling_high: np.ndarray,
    trail_long: np.ndarray,
    trail_short: np.ndarray,
    trade_start_idx: int,
    initial_capital_values: np.ndarray,
    commission_pct_values: np.ndarray,
    stop_x_values: np.ndarray,
    reward_risk_values: np.ndarray,
    max_stop_pct_values: np.ndarray,
    max_days_values: np.ndarray,
    risk_per_trade_pct_values: np.ndarray,
    contract_size_values: np.ndarray,
    trail_activation_rr_values: np.ndarray,
    tick_size_values: np.ndarray,
    start_ns_values: np.ndarray,
    end_ns_values: np.ndarray,
    enable_long_values: np.ndarray,
    enable_short_values: np.ndarray,
    target_enabled_values: np.ndarray,
    trail_enabled_values: np.ndarray,
    max_days_enabled_values: np.ndarray,
    use_date_filter_values: np.ndarray,
    strict_boundary_values: np.ndarray,
    boundary_none_values: np.ndarray,
    report_margin_values: np.ndarray,
    rounding_code_values: np.ndarray,
    outputs: np.ndarray,
) -> None:
    n = close_values.shape[0]
    initial_capital = initial_capital_values[candidate_index]
    if n == 0:
        _write_empty_result(outputs, candidate_index, initial_capital)
        return

    commission_rate = commission_pct_values[candidate_index] / 100.0
    stop_x = stop_x_values[candidate_index]
    reward_risk = reward_risk_values[candidate_index]
    max_stop_pct = max_stop_pct_values[candidate_index]
    max_days = max_days_values[candidate_index]
    risk_per_trade_pct = risk_per_trade_pct_values[candidate_index]
    contract_size = contract_size_values[candidate_index]
    trail_activation_rr = trail_activation_rr_values[candidate_index]
    tick_size = tick_size_values[candidate_index]
    start_ns = start_ns_values[candidate_index]
    end_ns = end_ns_values[candidate_index]
    enable_long = enable_long_values[candidate_index]
    enable_short = enable_short_values[candidate_index]
    target_enabled = target_enabled_values[candidate_index]
    trail_enabled = trail_enabled_values[candidate_index]
    max_days_enabled = max_days_enabled_values[candidate_index]
    use_date_filter = use_date_filter_values[candidate_index]
    strict_boundary = strict_boundary_values[candidate_index]
    boundary_none = boundary_none_values[candidate_index]
    report_margin = report_margin_values[candidate_index]
    rounding_code = rounding_code_values[candidate_index]

    day_ns = 86_400_000_000_000.0
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

    invalid_stop_distance_count = 0
    zero_size_entry_count = 0
    rejected_fill_count = 0
    flags = 0
    max_notional = 0.0
    max_required_leverage = 0.0
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
            gross_pnl = (exit_price - entry_price) * size if position > 0 else (entry_price - exit_price) * size
            exit_commission = exit_price * size * commission_rate
            net_pnl = gross_pnl - entry_commission - exit_commission
            balance += gross_pnl - exit_commission
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

            notional = abs(open_price * size)
            if notional > max_notional:
                max_notional = notional
            if report_margin and balance > 0.0:
                leverage = notional / balance
                if leverage > max_required_leverage:
                    max_required_leverage = leverage

            if trail_enabled:
                activation = anchor_price + position * initial_risk * trail_activation_rr
                activation_reached = high >= activation if position > 0 else low <= activation
                if activation_reached:
                    trail_active = True
                    band = trail_long[i] if position > 0 else trail_short[i]
                    if math.isfinite(band):
                        band = _maybe_round_trail(position, band, rounding_code, tick_size)
                        if position > 0:
                            trail_stop = max(trail_stop, band)
                        else:
                            trail_stop = min(trail_stop, band)

        stop_active = math.nan
        if position != 0:
            stop_active = trail_stop if trail_enabled and trail_active else initial_stop
            if position > 0:
                if open_price <= stop_active:
                    exit_price = open_price
                elif target_enabled and open_price >= target_price:
                    exit_price = open_price
            else:
                if open_price >= stop_active:
                    exit_price = open_price
                elif target_enabled and open_price <= target_price:
                    exit_price = open_price

        if position != 0 and not math.isnan(exit_price):
            gross_pnl = (exit_price - entry_price) * size if position > 0 else (entry_price - exit_price) * size
            exit_commission = exit_price * size * commission_rate
            net_pnl = gross_pnl - entry_commission - exit_commission
            balance += gross_pnl - exit_commission
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

        if position != 0 and math.isnan(exit_price):
            high_first = abs(open_price - high) < abs(open_price - low)
            current = open_price
            for segment in range(3):
                if high_first:
                    endpoint = high if segment == 0 else (low if segment == 1 else close)
                else:
                    endpoint = low if segment == 0 else (high if segment == 1 else close)
                rising = endpoint >= current
                path_exit = math.nan
                if target_enabled:
                    if position > 0:
                        if rising and _between(current, endpoint, target_price):
                            path_exit = target_price
                        elif (not rising) and _between(current, endpoint, initial_stop):
                            path_exit = initial_stop
                    else:
                        if rising and _between(current, endpoint, initial_stop):
                            path_exit = initial_stop
                        elif (not rising) and _between(current, endpoint, target_price):
                            path_exit = target_price
                elif trail_enabled:
                    if position > 0 and (not rising) and _between(current, endpoint, stop_active):
                        path_exit = stop_active
                    elif position < 0 and rising and _between(current, endpoint, stop_active):
                        path_exit = stop_active
                if not math.isnan(path_exit):
                    gross_pnl = (path_exit - entry_price) * size if position > 0 else (entry_price - path_exit) * size
                    exit_commission = path_exit * size * commission_rate
                    net_pnl = gross_pnl - entry_commission - exit_commission
                    balance += gross_pnl - exit_commission
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
                    break
                current = endpoint

        if position != 0 and trail_enabled and not entry_filled_this_bar:
            activation = anchor_price + position * initial_risk * trail_activation_rr
            activation_reached = high >= activation if position > 0 else low <= activation
            if trail_active or activation_reached:
                trail_active = True
                band = trail_long[i] if position > 0 else trail_short[i]
                if math.isfinite(band):
                    band = _maybe_round_trail(position, band, rounding_code, tick_size)
                    if position > 0:
                        trail_stop = max(initial_stop, trail_stop, band)
                    else:
                        trail_stop = min(initial_stop, trail_stop, band)

        if position != 0 and entry_index >= 0 and max_days_enabled:
            days_in_trade = (timestamp_ns[i] - timestamp_ns[entry_index]) / day_ns
            if days_in_trade >= max_days and not (strict_boundary and i == last_bar_index):
                pending_market_close = True

        if i == last_bar_index and strict_boundary:
            pending_entry = False
            pending_market_close = False
            if position != 0:
                exit_price = close
                gross_pnl = (exit_price - entry_price) * size if position > 0 else (entry_price - exit_price) * size
                exit_commission = exit_price * size * commission_rate
                net_pnl = gross_pnl - entry_commission - exit_commission
                balance += gross_pnl - exit_commission
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
            in_date_range = in_date_range and timestamp_ns[i] >= start_ns and timestamp_ns[i] <= end_ns
        can_plan_entry = (i != last_bar_index) or boundary_none
        if (
            can_plan_entry
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
                    stop = rolling_low[i] - stop_x * atr_value
                    risk = anchor - stop
                    target = anchor + reward_risk * risk
                else:
                    stop = rolling_high[i] + stop_x * atr_value
                    risk = stop - anchor
                    target = anchor - reward_risk * risk
                stop_pct = 100.0 * risk / anchor if anchor > 0.0 else math.inf
                risk_cash = balance * risk_per_trade_pct / 100.0
                raw_size = risk_cash / risk if risk > 0.0 else 0.0
                order_size = math.floor(raw_size / contract_size) * contract_size
                if not (math.isfinite(stop) and math.isfinite(risk) and risk > 0.0):
                    invalid_stop_distance_count += 1
                    flags = flags | GUARDRAIL_FLAG_INVALID_STOP_DISTANCE
                elif stop_pct > max_stop_pct:
                    rejected_fill_count += 1
                    flags = flags | GUARDRAIL_FLAG_REJECTED_FILL
                elif not (math.isfinite(order_size) and order_size > 0.0):
                    zero_size_entry_count += 1
                    flags = flags | GUARDRAIL_FLAG_ZERO_SIZE_ENTRY
                else:
                    pending_entry = True
                    pending_direction = direction
                    pending_anchor = anchor
                    pending_stop = _maybe_round_stop(direction, stop, rounding_code, tick_size)
                    pending_risk = risk
                    if target_enabled:
                        pending_target = _maybe_round_target(direction, target, rounding_code, tick_size)
                    else:
                        pending_target = target
                    pending_size = order_size

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

    net_profit_pct = (balance - initial_capital) / initial_capital * 100.0 if initial_capital != 0.0 else 0.0
    win_rate = winning_trades / total_trades * 100.0 if total_trades else 0.0
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

    outputs[candidate_index, OUTPUT_NET_PROFIT_PCT] = net_profit_pct
    outputs[candidate_index, OUTPUT_MAX_DRAWDOWN_PCT] = max_drawdown
    outputs[candidate_index, OUTPUT_TOTAL_TRADES] = total_trades
    outputs[candidate_index, OUTPUT_WINNING_TRADES] = winning_trades
    outputs[candidate_index, OUTPUT_LOSING_TRADES] = losing_trades
    outputs[candidate_index, OUTPUT_WIN_RATE_PCT] = win_rate
    outputs[candidate_index, OUTPUT_GROSS_PROFIT] = gross_profit
    outputs[candidate_index, OUTPUT_GROSS_LOSS] = gross_loss
    outputs[candidate_index, OUTPUT_PROFIT_FACTOR] = profit_factor
    outputs[candidate_index, OUTPUT_ROMAD] = romad
    outputs[candidate_index, OUTPUT_FINAL_BALANCE] = balance
    outputs[candidate_index, OUTPUT_MAX_CONSECUTIVE_LOSSES] = max_consecutive_losses
    outputs[candidate_index, OUTPUT_INVALID_STOP_DISTANCE_COUNT] = invalid_stop_distance_count
    outputs[candidate_index, OUTPUT_ZERO_SIZE_ENTRY_COUNT] = zero_size_entry_count
    outputs[candidate_index, OUTPUT_REJECTED_FILL_COUNT] = rejected_fill_count
    outputs[candidate_index, OUTPUT_MARGIN_REJECT_COUNT] = 0.0
    outputs[candidate_index, OUTPUT_LIQUIDATION_COUNT] = 0.0
    outputs[candidate_index, OUTPUT_NO_CAPITAL_HALT] = 0.0
    outputs[candidate_index, OUTPUT_MAX_REQUIRED_LEVERAGE] = max_required_leverage
    outputs[candidate_index, OUTPUT_MAX_NOTIONAL] = max_notional
    outputs[candidate_index, OUTPUT_FLAGS] = flags


def _batch_loop_impl(
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    long_signals: np.ndarray,
    short_signals: np.ndarray,
    atr_values: np.ndarray,
    rolling_low: np.ndarray,
    rolling_high: np.ndarray,
    trail_long: np.ndarray,
    trail_short: np.ndarray,
    trade_start_idx: int,
    initial_capital_values: np.ndarray,
    commission_pct_values: np.ndarray,
    stop_x_values: np.ndarray,
    reward_risk_values: np.ndarray,
    max_stop_pct_values: np.ndarray,
    max_days_values: np.ndarray,
    risk_per_trade_pct_values: np.ndarray,
    contract_size_values: np.ndarray,
    trail_activation_rr_values: np.ndarray,
    tick_size_values: np.ndarray,
    start_ns_values: np.ndarray,
    end_ns_values: np.ndarray,
    enable_long_values: np.ndarray,
    enable_short_values: np.ndarray,
    target_enabled_values: np.ndarray,
    trail_enabled_values: np.ndarray,
    max_days_enabled_values: np.ndarray,
    use_date_filter_values: np.ndarray,
    strict_boundary_values: np.ndarray,
    boundary_none_values: np.ndarray,
    report_margin_values: np.ndarray,
    rounding_code_values: np.ndarray,
    outputs: np.ndarray,
) -> None:
    for index in numba.prange(outputs.shape[0]):
        _compiled_loop_one(
            index,
            open_values,
            high_values,
            low_values,
            close_values,
            timestamp_ns,
            long_signals,
            short_signals,
            atr_values,
            rolling_low,
            rolling_high,
            trail_long,
            trail_short,
            trade_start_idx,
            initial_capital_values,
            commission_pct_values,
            stop_x_values,
            reward_risk_values,
            max_stop_pct_values,
            max_days_values,
            risk_per_trade_pct_values,
            contract_size_values,
            trail_activation_rr_values,
            tick_size_values,
            start_ns_values,
            end_ns_values,
            enable_long_values,
            enable_short_values,
            target_enabled_values,
            trail_enabled_values,
            max_days_enabled_values,
            use_date_filter_values,
            strict_boundary_values,
            boundary_none_values,
            report_margin_values,
            rounding_code_values,
            outputs,
        )


def _stacked_batch_loop_impl(
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    long_entries: np.ndarray,
    short_entries: np.ndarray,
    atr_values: np.ndarray,
    rolling_low: np.ndarray,
    rolling_high: np.ndarray,
    trail_long: np.ndarray,
    trail_short: np.ndarray,
    data_index: np.ndarray,
    trade_start_idx: int,
    initial_capital_values: np.ndarray,
    commission_pct_values: np.ndarray,
    stop_x_values: np.ndarray,
    reward_risk_values: np.ndarray,
    max_stop_pct_values: np.ndarray,
    max_days_values: np.ndarray,
    risk_per_trade_pct_values: np.ndarray,
    contract_size_values: np.ndarray,
    trail_activation_rr_values: np.ndarray,
    tick_size_values: np.ndarray,
    start_ns_values: np.ndarray,
    end_ns_values: np.ndarray,
    enable_long_values: np.ndarray,
    enable_short_values: np.ndarray,
    target_enabled_values: np.ndarray,
    trail_enabled_values: np.ndarray,
    max_days_enabled_values: np.ndarray,
    use_date_filter_values: np.ndarray,
    strict_boundary_values: np.ndarray,
    boundary_none_values: np.ndarray,
    report_margin_values: np.ndarray,
    rounding_code_values: np.ndarray,
    outputs: np.ndarray,
) -> None:
    for index in numba.prange(outputs.shape[0]):
        row = data_index[index]
        _compiled_loop_one(
            index,
            open_values,
            high_values,
            low_values,
            close_values,
            timestamp_ns,
            long_entries[row],
            short_entries[row],
            atr_values[row],
            rolling_low[row],
            rolling_high[row],
            trail_long[row],
            trail_short[row],
            trade_start_idx,
            initial_capital_values,
            commission_pct_values,
            stop_x_values,
            reward_risk_values,
            max_stop_pct_values,
            max_days_values,
            risk_per_trade_pct_values,
            contract_size_values,
            trail_activation_rr_values,
            tick_size_values,
            start_ns_values,
            end_ns_values,
            enable_long_values,
            enable_short_values,
            target_enabled_values,
            trail_enabled_values,
            max_days_enabled_values,
            use_date_filter_values,
            strict_boundary_values,
            boundary_none_values,
            report_margin_values,
            rounding_code_values,
            outputs,
        )


if numba is not None:
    _COMPILED_BATCH_LOOP = numba.njit(cache=True, parallel=True)(_batch_loop_impl)
    _COMPILED_STACKED_BATCH_LOOP = numba.njit(cache=True, parallel=True)(_stacked_batch_loop_impl)
else:  # pragma: no cover
    _COMPILED_BATCH_LOOP = _batch_loop_impl
    _COMPILED_STACKED_BATCH_LOOP = _stacked_batch_loop_impl


__all__ = [
    "COMPILED_BATCH_KIND",
    "CompiledBatchOutput",
    "OUTPUT_COLUMN_COUNT",
    "OUTPUT_FINAL_BALANCE",
    "OUTPUT_FLAGS",
    "OUTPUT_GROSS_LOSS",
    "OUTPUT_GROSS_PROFIT",
    "OUTPUT_INVALID_STOP_DISTANCE_COUNT",
    "OUTPUT_LOSING_TRADES",
    "OUTPUT_MARGIN_REJECT_COUNT",
    "OUTPUT_MAX_CONSECUTIVE_LOSSES",
    "OUTPUT_MAX_DRAWDOWN_PCT",
    "OUTPUT_MAX_NOTIONAL",
    "OUTPUT_MAX_REQUIRED_LEVERAGE",
    "OUTPUT_NET_PROFIT_PCT",
    "OUTPUT_NO_CAPITAL_HALT",
    "OUTPUT_PROFIT_FACTOR",
    "OUTPUT_REJECTED_FILL_COUNT",
    "OUTPUT_ROMAD",
    "OUTPUT_TOTAL_TRADES",
    "OUTPUT_WINNING_TRADES",
    "OUTPUT_WIN_RATE_PCT",
    "OUTPUT_ZERO_SIZE_ENTRY_COUNT",
    "StackedExecutionData",
    "build_stacked_execution_data",
    "compiled_batch_available",
    "compiled_unavailable_reason",
    "evaluate_compiled_batch",
    "evaluate_compiled_stacked_batch",
]
