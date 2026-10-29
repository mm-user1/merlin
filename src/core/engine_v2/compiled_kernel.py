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
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from core.metrics import build_utc_day_ids
from .kernel import ExecutionData
from .price_rounding import PRICE_ROUNDING_NONE, PRICE_ROUNDING_TICK_OUTWARD, validate_tick_size
from .execution_modes import (
    TRAIL_MODE_CHANDELIER,
    TRAIL_MODE_FIXED_AF_SAR,
    TRAIL_MODE_MA,
    TRAIL_MODE_NONE,
    TRAIL_MODE_R_DISTANCE,
    resolve_position_mode_state,
    stateful_trail_params,
    trail_mode_code,
)
from .profile import active_mode_values
from .admission import ADMISSION_MODE, admission_cap, execution_admission
from .quantity import execution_quantity
from .contracts import GUARDRAIL_FLAG_EXECUTION_ERROR, GUARDRAIL_FLAG_MARGIN_REJECT


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
OUTPUT_SHARPE_RATIO = 21
OUTPUT_SQN = 22
OUTPUT_SHARPE_DAILY = 23
OUTPUT_SHARPE_DAILY_OBSERVATIONS = 24
OUTPUT_SHARPE_DAILY_ACTIVE_DAYS = 25
OUTPUT_COLUMN_COUNT = 26

GUARDRAIL_FLAG_REJECTED_FILL = 2
GUARDRAIL_FLAG_INVALID_STOP_DISTANCE = 4
GUARDRAIL_FLAG_ZERO_SIZE_ENTRY = 8


@dataclass(frozen=True)
class CompiledBatchOutput:
    """Fixed-width compiled metrics for one compiled batch."""

    outputs: np.ndarray
    max_drawdown_mtm_pct: np.ndarray | None = None
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
    month_ids: np.ndarray
    day_ids: np.ndarray
    long_entries: np.ndarray
    short_entries: np.ndarray
    atr: np.ndarray
    rolling_low: np.ndarray
    rolling_high: np.ndarray
    trail_long: np.ndarray
    trail_short: np.ndarray
    data_index: np.ndarray
    chandelier_atr: np.ndarray
    chandelier_data_index: np.ndarray

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
            + self.month_ids.nbytes
            + self.day_ids.nbytes
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
            + self.chandelier_atr.nbytes
            + self.chandelier_data_index.nbytes
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
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_sqn: bool = False,
    compute_max_drawdown_mtm: bool = False,
    month_ids: np.ndarray | None = None,
    day_ids: np.ndarray | None = None,
    admission_rules=None,
    quantity_rules=None,
) -> CompiledBatchOutput:
    """Evaluate one batch of candidates sharing the same ``ExecutionData``."""

    if not compiled_batch_available():
        raise RuntimeError(compiled_unavailable_reason() or REFERENCE_UNAVAILABLE_REASON)
    metric_month_ids = _validated_month_ids(
        build_calendar_month_ids(data.timestamps) if compute_sharpe and month_ids is None else month_ids,
        len(data.timestamps),
    )
    _validate_sharpe_month_ids(metric_month_ids, len(data.timestamps), compute_sharpe)
    metric_day_ids = _validated_day_ids(
        build_utc_day_ids(data.timestamps) if compute_sharpe_daily and day_ids is None else day_ids,
        len(data.timestamps),
    )
    _validate_sharpe_daily_day_ids(metric_day_ids, len(data.timestamps), compute_sharpe_daily)
    execution_quantity(profile, quantity_rules, admission_rules)
    if not params_batch:
        execution_admission(profile, profile.parameter_defaults, admission_rules)
        return CompiledBatchOutput(
            outputs=np.empty((0, OUTPUT_COLUMN_COUNT), dtype=np.float64),
            max_drawdown_mtm_pct=(
                np.empty(0, dtype=np.float64) if compute_max_drawdown_mtm else None
            ),
        )

    packed = _pack_config_arrays(profile, params_batch, trade_start_idx, admission_rules, quantity_rules)
    _validate_admission_batch(profile, packed, admission_rules, (data.open, data.high, data.low, data.close), quantity_rules)
    _validate_packed_stateful_trails(packed)
    chandelier_atr = _grouped_chandelier_values(data, packed)
    outputs = np.empty((len(params_batch), OUTPUT_COLUMN_COUNT), dtype=np.float64)
    mtm_sidecar = (
        np.full(len(params_batch), np.nan, dtype=np.float64)
        if compute_max_drawdown_mtm
        else np.empty(0, dtype=np.float64)
    )
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
            metric_month_ids,
            metric_day_ids,
            np.asarray(data.signals.long_entries, dtype=np.bool_),
            np.asarray(data.signals.short_entries, dtype=np.bool_),
            np.asarray(data.atr, dtype=np.float64),
            np.asarray(data.rolling_low, dtype=np.float64),
            np.asarray(data.rolling_high, dtype=np.float64),
            np.asarray(data.trail_long, dtype=np.float64),
            np.asarray(data.trail_short, dtype=np.float64),
            chandelier_atr,
            int(trade_start_idx),
            packed["initial_capital"],
            packed["commission_pct"],
            packed["stop_x"],
            packed["reward_risk"],
            packed["max_stop_pct"],
            packed["max_days"],
            packed["risk_per_trade_pct"],
            packed["contract_size"],
            packed["admission_cap"],
            packed["admission_min_lots"],
            packed["admission_min_notional"],
            packed["trail_activation_rr"],
            packed["trail_distance_r"],
            packed["chandelier_atr_mult"],
            packed["sar_speed"],
            packed["tick_size"],
            packed["start_ns"],
            packed["end_ns"],
            packed["enable_long"],
            packed["enable_short"],
            packed["target_enabled"],
            packed["trail_mode_code"],
            packed["max_days_enabled"],
            packed["use_date_filter"],
            packed["strict_boundary"],
            packed["boundary_none"],
            packed["report_margin"],
            packed["rounding_code"],
            bool(compute_sharpe),
            bool(compute_sharpe_daily),
            bool(compute_sqn),
            bool(compute_max_drawdown_mtm),
            outputs,
            mtm_sidecar,
        )
    finally:
        if numba.get_num_threads() != previous_threads:
            numba.set_num_threads(previous_threads)
    return CompiledBatchOutput(
        outputs=outputs,
        max_drawdown_mtm_pct=mtm_sidecar if compute_max_drawdown_mtm else None,
    )


def build_stacked_execution_data(
    data_rows: Sequence[ExecutionData],
    data_index: Sequence[int],
    *,
    month_ids: np.ndarray | None = None,
    day_ids: np.ndarray | None = None,
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
    metric_month_ids = _validated_month_ids(month_ids, len(timestamp_ns))
    metric_day_ids = _validated_day_ids(day_ids, len(timestamp_ns))

    long_rows: list[np.ndarray] = []
    short_rows: list[np.ndarray] = []
    atr_rows: list[np.ndarray] = []
    rolling_low_rows: list[np.ndarray] = []
    rolling_high_rows: list[np.ndarray] = []
    trail_long_rows: list[np.ndarray] = []
    trail_short_rows: list[np.ndarray] = []
    chandelier_rows: list[np.ndarray] = []
    chandelier_row_index: list[int] = []

    for row_number, data in enumerate(rows):
        if data.timestamps is not first.timestamps:
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
        if data.chandelier_atr is None:
            chandelier_row_index.append(-1)
        else:
            chandelier_row_index.append(len(chandelier_rows))
            chandelier_rows.append(
                _contiguous_1d(data.chandelier_atr, "chandelier_atr", np.float64)
            )

    index_array = np.asarray(data_index, dtype=np.int64)
    if index_array.ndim != 1:
        raise ValueError("Stacked compiled execution data_index must be a 1D array.")
    if index_array.size:
        if int(index_array.min()) < 0 or int(index_array.max()) >= len(rows):
            raise ValueError("Stacked compiled execution data_index contains an out-of-range row index.")

    has_chandelier = bool(chandelier_rows)
    return StackedExecutionData(
        open=open_values,
        high=high_values,
        low=low_values,
        close=close_values,
        timestamp_ns=timestamp_ns,
        month_ids=metric_month_ids,
        day_ids=metric_day_ids,
        long_entries=np.ascontiguousarray(np.stack(long_rows, axis=0), dtype=np.bool_),
        short_entries=np.ascontiguousarray(np.stack(short_rows, axis=0), dtype=np.bool_),
        atr=np.ascontiguousarray(np.stack(atr_rows, axis=0), dtype=np.float64),
        rolling_low=np.ascontiguousarray(np.stack(rolling_low_rows, axis=0), dtype=np.float64),
        rolling_high=np.ascontiguousarray(np.stack(rolling_high_rows, axis=0), dtype=np.float64),
        trail_long=np.ascontiguousarray(np.stack(trail_long_rows, axis=0), dtype=np.float64),
        trail_short=np.ascontiguousarray(np.stack(trail_short_rows, axis=0), dtype=np.float64),
        data_index=np.ascontiguousarray(index_array, dtype=np.int64),
        chandelier_atr=(
            np.ascontiguousarray(np.stack(chandelier_rows, axis=0), dtype=np.float64)
            if has_chandelier
            else np.empty((0, 0), dtype=np.float64)
        ),
        chandelier_data_index=(
            np.ascontiguousarray(chandelier_row_index, dtype=np.int32)
            if has_chandelier
            else np.empty(0, dtype=np.int32)
        ),
    )


def evaluate_compiled_stacked_batch(
    *,
    stacked_data: StackedExecutionData,
    profile: Any,
    params_batch: Sequence[Mapping[str, Any]] | None = None,
    trade_start_idx: int,
    n_workers: int = 1,
    packed_config_arrays: Mapping[str, np.ndarray] | None = None,
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_sqn: bool = False,
    compute_max_drawdown_mtm: bool = False,
    admission_rules=None,
    quantity_rules=None,
) -> CompiledBatchOutput:
    """Evaluate one stacked compiled batch with per-candidate data row indices."""

    if not compiled_batch_available():
        raise RuntimeError(compiled_unavailable_reason() or REFERENCE_UNAVAILABLE_REASON)
    metric_month_ids = _validated_month_ids(
        stacked_data.month_ids,
        len(stacked_data.timestamp_ns),
    )
    _validate_sharpe_month_ids(
        metric_month_ids,
        len(stacked_data.timestamp_ns),
        compute_sharpe,
    )
    metric_day_ids = _validated_day_ids(stacked_data.day_ids, len(stacked_data.timestamp_ns))
    _validate_sharpe_daily_day_ids(
        metric_day_ids,
        len(stacked_data.timestamp_ns),
        compute_sharpe_daily,
    )
    execution_quantity(profile, quantity_rules, admission_rules)
    if packed_config_arrays is None:
        if not params_batch:
            execution_admission(profile, profile.parameter_defaults, admission_rules)
            return CompiledBatchOutput(
                outputs=np.empty((0, OUTPUT_COLUMN_COUNT), dtype=np.float64),
                max_drawdown_mtm_pct=(
                    np.empty(0, dtype=np.float64) if compute_max_drawdown_mtm else None
                ),
                execution_mode="stacked",
            )
        if len(params_batch) != stacked_data.candidate_count:
            raise ValueError("Stacked compiled params_batch length must match data_index length.")
        packed = _pack_config_arrays(profile, params_batch, trade_start_idx, admission_rules, quantity_rules)
        candidate_count = len(params_batch)
    else:
        packed = _validated_config_arrays(packed_config_arrays)
        candidate_count = _packed_config_count(packed)
        if candidate_count != stacked_data.candidate_count:
            raise ValueError("Stacked compiled packed_config_arrays length must match data_index length.")
        if params_batch is not None and len(params_batch) != candidate_count:
            raise ValueError("Stacked compiled params_batch length must match packed_config_arrays length.")

    _validate_admission_batch(profile, packed, admission_rules,
        (stacked_data.open, stacked_data.high, stacked_data.low, stacked_data.close), quantity_rules)
    _validate_packed_stateful_trails(packed)
    _validate_stacked_chandelier(stacked_data, packed)

    if candidate_count == 0:
        return CompiledBatchOutput(
            outputs=np.empty((0, OUTPUT_COLUMN_COUNT), dtype=np.float64),
            max_drawdown_mtm_pct=(
                np.empty(0, dtype=np.float64) if compute_max_drawdown_mtm else None
            ),
            execution_mode="stacked",
        )

    outputs = np.empty((candidate_count, OUTPUT_COLUMN_COUNT), dtype=np.float64)
    mtm_sidecar = (
        np.full(candidate_count, np.nan, dtype=np.float64)
        if compute_max_drawdown_mtm
        else np.empty(0, dtype=np.float64)
    )
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
            metric_month_ids,
            metric_day_ids,
            stacked_data.long_entries,
            stacked_data.short_entries,
            stacked_data.atr,
            stacked_data.rolling_low,
            stacked_data.rolling_high,
            stacked_data.trail_long,
            stacked_data.trail_short,
            stacked_data.chandelier_atr,
            stacked_data.chandelier_data_index,
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
            packed["admission_cap"],
            packed["admission_min_lots"],
            packed["admission_min_notional"],
            packed["trail_activation_rr"],
            packed["trail_distance_r"],
            packed["chandelier_atr_mult"],
            packed["sar_speed"],
            packed["tick_size"],
            packed["start_ns"],
            packed["end_ns"],
            packed["enable_long"],
            packed["enable_short"],
            packed["target_enabled"],
            packed["trail_mode_code"],
            packed["max_days_enabled"],
            packed["use_date_filter"],
            packed["strict_boundary"],
            packed["boundary_none"],
            packed["report_margin"],
            packed["rounding_code"],
            bool(compute_sharpe),
            bool(compute_sharpe_daily),
            bool(compute_sqn),
            bool(compute_max_drawdown_mtm),
            outputs,
            mtm_sidecar,
        )
    finally:
        if numba.get_num_threads() != previous_threads:
            numba.set_num_threads(previous_threads)
    return CompiledBatchOutput(
        outputs=outputs,
        max_drawdown_mtm_pct=mtm_sidecar if compute_max_drawdown_mtm else None,
        execution_mode="stacked",
    )


def _contiguous_1d(values: Any, name: str, dtype: Any) -> np.ndarray:
    array = np.asarray(values, dtype=dtype)
    if array.ndim != 1:
        raise ValueError(f"{name} must be a 1D array for stacked compiled execution.")
    return np.ascontiguousarray(array, dtype=dtype)


def _grouped_chandelier_values(
    data: ExecutionData,
    packed: Mapping[str, np.ndarray],
) -> np.ndarray:
    active = np.any(packed["trail_mode_code"] == TRAIL_MODE_CHANDELIER)
    if active and data.chandelier_atr is None:
        raise ValueError("chandelier_atr is required for active compiled Chandelier candidates.")
    if not active:
        return np.empty(0, dtype=np.float64)
    return _contiguous_1d(data.chandelier_atr, "chandelier_atr", np.float64)


def _validate_packed_stateful_trails(packed: Mapping[str, np.ndarray]) -> None:
    codes = packed["trail_mode_code"]
    if np.any((codes < TRAIL_MODE_NONE) | (codes > TRAIL_MODE_FIXED_AF_SAR)):
        raise ValueError("Packed compiled trail mode code is unsupported.")
    stateful = codes >= TRAIL_MODE_R_DISTANCE
    if np.any(stateful & packed["target_enabled"]):
        raise ValueError("Active stateful trails require target_mode='none'.")
    activation = packed["trail_activation_rr"]
    if np.any(stateful & (~np.isfinite(activation) | (activation <= 0.0))):
        raise ValueError("trailRR must be finite and positive for the active stateful trail mode.")
    r_distance = codes == TRAIL_MODE_R_DISTANCE
    distance = packed["trail_distance_r"]
    if np.any(r_distance & (~np.isfinite(distance) | (distance <= 0.0))):
        raise ValueError("trailDistanceR must be finite and positive for r_distance.")
    chandelier = codes == TRAIL_MODE_CHANDELIER
    multiplier = packed["chandelier_atr_mult"]
    if np.any(chandelier & (~np.isfinite(multiplier) | (multiplier <= 0.0))):
        raise ValueError("chandelierATRMult must be finite and positive for chandelier.")
    sar = codes == TRAIL_MODE_FIXED_AF_SAR
    speed = packed["sar_speed"]
    if np.any(sar & (~np.isfinite(speed) | (speed <= 0.0) | (speed > 1.0))):
        raise ValueError("sarSpeed must be greater than 0 and no greater than 1.")


def _validate_stacked_chandelier(
    data: StackedExecutionData,
    packed: Mapping[str, np.ndarray],
) -> None:
    active_positions = np.flatnonzero(
        packed["trail_mode_code"] == TRAIL_MODE_CHANDELIER
    )
    if active_positions.size == 0:
        return
    if data.chandelier_data_index.size != data.row_count or data.chandelier_atr.shape[0] == 0:
        raise ValueError("chandelier_atr is required for active stacked Chandelier candidates.")
    data_rows = data.data_index[active_positions]
    if np.any(data.chandelier_data_index[data_rows] < 0):
        raise ValueError("chandelier_atr is missing for an active stacked Chandelier candidate.")


def build_calendar_month_ids(values: Sequence[Any]) -> np.ndarray:
    """Return contiguous UTC calendar-month ids for one execution dataset."""

    index = pd.DatetimeIndex(values)
    if index.hasnans:
        raise ValueError("Compiled Grid V2 timestamps cannot contain NaT values.")
    if index.tz is not None:
        index = index.tz_convert("UTC")
    return np.ascontiguousarray(
        np.asarray(index.year * 12 + index.month, dtype=np.int32),
        dtype=np.int32,
    )


def _validated_month_ids(values: Any, expected_length: int) -> np.ndarray:
    if values is None:
        return np.empty(0, dtype=np.int32)
    array = _contiguous_1d(values, "month_ids", np.int32)
    if array.size not in {0, int(expected_length)}:
        raise ValueError(
            "Compiled Grid V2 month_ids length must be zero or match the execution bars."
        )
    return array


def _validated_day_ids(values: Any, expected_length: int) -> np.ndarray:
    if values is None:
        return np.empty(0, dtype=np.int32)
    array = _contiguous_1d(values, "day_ids", np.int32)
    if array.size not in {0, int(expected_length)}:
        raise ValueError("Compiled Grid V2 day_ids length must be zero or match the execution bars.")
    return array


def _validate_sharpe_month_ids(
    month_ids: np.ndarray,
    expected_length: int,
    compute_sharpe: bool,
) -> None:
    if compute_sharpe and month_ids.size != int(expected_length):
        raise ValueError(
            "Compiled Grid V2 requires month_ids matching the execution bars when Sharpe is requested."
        )


def _validate_sharpe_daily_day_ids(
    day_ids: np.ndarray,
    expected_length: int,
    compute_sharpe_daily: bool,
) -> None:
    if compute_sharpe_daily and day_ids.size != int(expected_length):
        raise ValueError(
            "Compiled Grid V2 requires day_ids matching the execution bars when Daily Sharpe is requested."
        )


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


_CONFIG_ARRAY_DTYPES: Mapping[str, Any] = {
    "admission_cap": np.float64,
    "admission_min_lots": np.int64,
    "admission_min_notional": np.float64,
    "initial_capital": np.float64,
    "commission_pct": np.float64,
    "stop_x": np.float64,
    "reward_risk": np.float64,
    "max_stop_pct": np.float64,
    "max_days": np.float64,
    "risk_per_trade_pct": np.float64,
    "contract_size": np.float64,
    "trail_activation_rr": np.float64,
    "trail_distance_r": np.float64,
    "chandelier_atr_mult": np.float64,
    "sar_speed": np.float64,
    "tick_size": np.float64,
    "start_ns": np.int64,
    "end_ns": np.int64,
    "enable_long": np.bool_,
    "enable_short": np.bool_,
    "target_enabled": np.bool_,
    "trail_mode_code": np.int64,
    "max_days_enabled": np.bool_,
    "use_date_filter": np.bool_,
    "strict_boundary": np.bool_,
    "boundary_none": np.bool_,
    "report_margin": np.bool_,
    "rounding_code": np.int64,
}


def _empty_config_arrays(count: int) -> dict[str, np.ndarray]:
    return {
        name: (np.zeros(int(count), dtype=dtype) if name.startswith("admission_") else np.empty(int(count), dtype=dtype))
        for name, dtype in _CONFIG_ARRAY_DTYPES.items()
    }


def _pack_admission_row(arrays, index, admission):
    cap, rules = admission
    arrays["contract_size"][index] = float(rules.base_step)
    arrays["admission_cap"][index] = cap
    arrays["admission_min_lots"][index] = rules.minimum_lots
    arrays["admission_min_notional"][index] = math.nan if rules.minimum_notional is None else float(rules.minimum_notional)


def _validate_admission_batch(profile, arrays, rules, prices, quantity_rules=None):
    quantity = execution_quantity(profile, quantity_rules, rules)
    if profile.modes.get("entryAdmission") != ADMISSION_MODE:
        if np.any(arrays["admission_cap"] != 0):
            raise ValueError("Admission arrays cannot activate a legacy profile.")
        if quantity is not None:
            expected = math.nan if quantity.minimum_notional is None else float(quantity.minimum_notional)
            if (not np.all(arrays["contract_size"] == float(quantity.base_step))
                    or not np.all(arrays["admission_min_lots"] == quantity.minimum_lots)
                    or not np.all(np.isnan(arrays["admission_min_notional"]) if math.isnan(expected) else arrays["admission_min_notional"] == expected)
                    or not np.all(arrays["strict_boundary"]) or np.any(arrays["boundary_none"]) or np.any(arrays["rounding_code"] != 0)):
                raise ValueError("Packed arrays disagree with quantity execution context.")
        elif np.any(arrays["admission_min_lots"] != 0) or np.any(arrays["admission_min_notional"] != 0):
            raise ValueError("Minimum arrays require explicit quantity execution context.")
        return
    _, rules = execution_admission(profile, profile.parameter_defaults, rules)
    cap = arrays["admission_cap"]
    if not np.all(np.isfinite(cap) & (cap > 0)):
        raise ValueError("Admission arrays require finite positive caps.")
    if (not np.all(arrays["target_enabled"]) or np.any(arrays["trail_mode_code"] != TRAIL_MODE_NONE)
            or not np.all(arrays["strict_boundary"]) or np.any(arrays["boundary_none"])
            or np.any(arrays["rounding_code"] != 0)):
        raise ValueError("Admission arrays require a strict unrounded RR bracket without trailing.")
    if not np.all(arrays["contract_size"] == float(rules.base_step)) or not np.all(arrays["admission_min_lots"] == rules.minimum_lots):
        raise ValueError("Admission arrays disagree with instrument quantity rules.")
    expected = math.nan if rules.minimum_notional is None else float(rules.minimum_notional)
    if not np.all(np.isnan(arrays["admission_min_notional"]) if math.isnan(expected) else arrays["admission_min_notional"] == expected):
        raise ValueError("Admission arrays disagree with instrument minimum notional.")
    for values in prices:
        if not np.all(np.isfinite(values) & (values > 0)):
            raise ValueError("Admission execution requires finite positive OHLC prices.")


def _validated_config_arrays(values: Mapping[str, Any]) -> dict[str, np.ndarray]:
    arrays: dict[str, np.ndarray] = {}
    count: int | None = None
    for name, dtype in _CONFIG_ARRAY_DTYPES.items():
        if name not in values:
            raise ValueError(f"Packed compiled config arrays missing required field {name!r}.")
        array = np.asarray(values[name], dtype=dtype)
        if array.ndim != 1:
            raise ValueError(f"Packed compiled config array {name!r} must be 1D.")
        if count is None:
            count = int(array.shape[0])
        elif int(array.shape[0]) != count:
            raise ValueError("Packed compiled config arrays must all have the same length.")
        arrays[name] = np.ascontiguousarray(array, dtype=dtype)
    return arrays


def _packed_config_count(arrays: Mapping[str, np.ndarray]) -> int:
    first = next(iter(_CONFIG_ARRAY_DTYPES))
    return int(np.asarray(arrays[first]).shape[0])


def _pack_config_arrays(
    profile: Any,
    params_batch: Sequence[Mapping[str, Any]],
    trade_start_idx: int,
    admission_rules=None,
    quantity_rules=None,
) -> dict[str, np.ndarray]:
    count = len(params_batch)
    validated_admission = execution_admission(profile, profile.parameter_defaults, admission_rules)
    quantity = execution_quantity(profile, quantity_rules, admission_rules)
    arrays = _empty_config_arrays(count)
    mode_cache: dict[tuple[tuple[str, str], ...], tuple[bool, int, bool, bool, bool, bool, int]] = {}
    start_ns_cache: dict[Any, int] = {}
    end_ns_cache: dict[Any, int] = {}
    for index, params in enumerate(params_batch):
        admission = ((admission_cap(profile, params), validated_admission[1])
                     if validated_admission is not None else None)
        modes = active_mode_values(profile, params)
        mode_key = tuple(sorted((str(key), str(value)) for key, value in modes.items()))
        mode_state = mode_cache.get(mode_key)
        if mode_state is None:
            mode_state = _compiled_mode_state(modes)
            mode_cache[mode_key] = mode_state
        (
            target_enabled,
            packed_trail_mode_code,
            max_days_enabled,
            strict_boundary,
            boundary_none,
            report_margin,
            rounding_code,
        ) = mode_state
        trail_params = stateful_trail_params(modes.get("trail", "none"), params)

        arrays["initial_capital"][index] = float(params.get("initialCapital", 100.0))
        arrays["commission_pct"][index] = float(params.get("commissionPct", 0.0))
        arrays["stop_x"][index] = float(params.get("stopX", 2.0))
        arrays["reward_risk"][index] = float(params.get("stopRR", 2.0))
        arrays["max_stop_pct"][index] = float(params.get("stopMaxPct", math.inf))
        arrays["max_days"][index] = float(params.get("stopMaxDays", math.inf))
        arrays["risk_per_trade_pct"][index] = float(params.get("riskPerTrade", 2.0))
        arrays["contract_size"][index] = float(params.get("contractSize", 0.01))
        if admission is not None:
            _pack_admission_row(arrays, index, admission)
        elif quantity is not None:
            _pack_admission_row(arrays, index, (0.0, quantity))
        arrays["trail_activation_rr"][index] = trail_params.trail_activation_rr
        arrays["trail_distance_r"][index] = trail_params.trail_distance_r
        arrays["chandelier_atr_mult"][index] = trail_params.chandelier_atr_mult
        arrays["sar_speed"][index] = trail_params.sar_speed
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
        arrays["trail_mode_code"][index] = packed_trail_mode_code
        arrays["max_days_enabled"][index] = max_days_enabled
        arrays["use_date_filter"][index] = _coerce_bool(params.get("dateFilter"), True)
        arrays["strict_boundary"][index] = strict_boundary
        arrays["boundary_none"][index] = boundary_none
        arrays["report_margin"][index] = report_margin
        arrays["rounding_code"][index] = rounding_code
    return arrays


def pack_compiled_config_arrays_from_rows(
    *,
    row_count: int,
    get_value: Callable[[int, str, Any], Any],
    get_modes: Callable[[int], Mapping[str, str]],
    trade_start_idx: int,
    admission_rules=None,
    profile=None,
    quantity_rules=None,
) -> dict[str, np.ndarray]:
    """Pack compiled config arrays from typed row accessors.

    This is the table-aware counterpart to ``_pack_config_arrays``. It keeps
    the execution-mode semantics in this module while allowing Grid V2 to avoid
    building a full params mapping solely for kernel config packing.
    """

    count = int(row_count)
    if count < 0:
        raise ValueError("row_count must be non-negative for compiled config packing.")
    validated_admission = (execution_admission(profile, profile.parameter_defaults, admission_rules)
                           if profile is not None else None)
    quantity = execution_quantity(profile, quantity_rules, admission_rules)
    arrays = _empty_config_arrays(count)
    mode_cache: dict[tuple[tuple[str, str], ...], tuple[bool, int, bool, bool, bool, bool, int]] = {}
    start_ns_cache: dict[Any, int] = {}
    end_ns_cache: dict[Any, int] = {}
    for index in range(count):
        modes = get_modes(index)
        if quantity is not None and (modes.get("boundary") != "strict_close" or modes.get("priceRounding", "none") != "none"):
            raise ValueError("Packed quantity rows require strict-close without price rounding.")
        if profile is not None and modes.get("entryAdmission") != profile.modes.get("entryAdmission"):
            raise ValueError("Admission mode must match the profile across all packed rows.")
        if modes.get("entryAdmission") == ADMISSION_MODE:
            if profile is None:
                raise ValueError("Admission packing requires the validated profile and instrument rules.")
            sentinel = object()
            supplied = {name: value for name in ("maxEntryLeverage", "contractSize", "stopMaxPct")
                        if (value := get_value(index, name, sentinel)) is not sentinel}
            admission = (admission_cap(profile, supplied), validated_admission[1])
        else:
            admission = None
        mode_key = tuple(sorted((str(key), str(value)) for key, value in modes.items()))
        mode_state = mode_cache.get(mode_key)
        if mode_state is None:
            mode_state = _compiled_mode_state(modes)
            mode_cache[mode_key] = mode_state
        (
            target_enabled,
            packed_trail_mode_code,
            max_days_enabled,
            strict_boundary,
            boundary_none,
            report_margin,
            rounding_code,
        ) = mode_state
        raw_params = {
            "trailRR": get_value(index, "trailRR", 1.0),
        }
        if packed_trail_mode_code == TRAIL_MODE_R_DISTANCE:
            raw_params["trailDistanceR"] = get_value(index, "trailDistanceR", None)
        elif packed_trail_mode_code == TRAIL_MODE_CHANDELIER:
            raw_params["chandelierATRLength"] = get_value(index, "chandelierATRLength", None)
            raw_params["chandelierATRMult"] = get_value(index, "chandelierATRMult", None)
        elif packed_trail_mode_code == TRAIL_MODE_FIXED_AF_SAR:
            raw_params["sarSpeed"] = get_value(index, "sarSpeed", None)
        trail_params = stateful_trail_params(modes.get("trail", "none"), raw_params)

        arrays["initial_capital"][index] = float(get_value(index, "initialCapital", 100.0))
        arrays["commission_pct"][index] = float(get_value(index, "commissionPct", 0.0))
        arrays["stop_x"][index] = float(get_value(index, "stopX", 2.0))
        arrays["reward_risk"][index] = float(get_value(index, "stopRR", 2.0))
        arrays["max_stop_pct"][index] = float(get_value(index, "stopMaxPct", math.inf))
        arrays["max_days"][index] = float(get_value(index, "stopMaxDays", math.inf))
        arrays["risk_per_trade_pct"][index] = float(get_value(index, "riskPerTrade", 2.0))
        arrays["contract_size"][index] = float(get_value(index, "contractSize", 0.01))
        if admission is not None:
            _pack_admission_row(arrays, index, admission)
        elif quantity is not None:
            _pack_admission_row(arrays, index, (0.0, quantity))
        arrays["trail_activation_rr"][index] = trail_params.trail_activation_rr
        arrays["trail_distance_r"][index] = trail_params.trail_distance_r
        arrays["chandelier_atr_mult"][index] = trail_params.chandelier_atr_mult
        arrays["sar_speed"][index] = trail_params.sar_speed
        if rounding_code == ROUNDING_TICK_OUTWARD_CODE:
            tick_size = get_value(index, "tickSize", None)
            if tick_size is None:
                raise ValueError("tickSize is required when priceRounding='tick_outward'.")
            arrays["tick_size"][index] = validate_tick_size(float(tick_size))
        else:
            arrays["tick_size"][index] = math.nan
        arrays["start_ns"][index] = _cached_timestamp_ns(
            get_value(index, "start", None),
            np.iinfo(np.int64).min,
            start_ns_cache,
        )
        arrays["end_ns"][index] = _cached_timestamp_ns(
            get_value(index, "end", None),
            np.iinfo(np.int64).max,
            end_ns_cache,
        )
        arrays["enable_long"][index] = _coerce_bool(get_value(index, "enableLong", None), True)
        arrays["enable_short"][index] = _coerce_bool(get_value(index, "enableShort", None), True)
        arrays["target_enabled"][index] = target_enabled
        arrays["trail_mode_code"][index] = packed_trail_mode_code
        arrays["max_days_enabled"][index] = max_days_enabled
        arrays["use_date_filter"][index] = _coerce_bool(get_value(index, "dateFilter", None), True)
        arrays["strict_boundary"][index] = strict_boundary
        arrays["boundary_none"][index] = boundary_none
        arrays["report_margin"][index] = report_margin
        arrays["rounding_code"][index] = rounding_code
    return arrays


def _compiled_mode_state(
    modes: Mapping[str, str],
) -> tuple[bool, int, bool, bool, bool, bool, int]:
    state = resolve_position_mode_state(modes)

    return (
        state.target_mode == "rr",
        trail_mode_code(state.trail_mode),
        state.max_days_enabled,
        state.boundary_mode == "strict_close",
        state.boundary_mode == "none",
        state.margin_mode == "report_only",
        _rounding_code(state.price_rounding_mode),
    )


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
    outputs[index, OUTPUT_SHARPE_RATIO] = math.nan
    outputs[index, OUTPUT_SQN] = math.nan
    outputs[index, OUTPUT_SHARPE_DAILY] = math.nan
    outputs[index, OUTPUT_SHARPE_DAILY_OBSERVATIONS] = math.nan
    outputs[index, OUTPUT_SHARPE_DAILY_ACTIVE_DAYS] = math.nan


@_compiled_target
def _compiled_loop_one(
    candidate_index: int,
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    month_ids: np.ndarray,
    day_ids: np.ndarray,
    long_signals: np.ndarray,
    short_signals: np.ndarray,
    atr_values: np.ndarray,
    rolling_low: np.ndarray,
    rolling_high: np.ndarray,
    trail_long: np.ndarray,
    trail_short: np.ndarray,
    chandelier_atr: np.ndarray,
    trade_start_idx: int,
    initial_capital_values: np.ndarray,
    commission_pct_values: np.ndarray,
    stop_x_values: np.ndarray,
    reward_risk_values: np.ndarray,
    max_stop_pct_values: np.ndarray,
    max_days_values: np.ndarray,
    risk_per_trade_pct_values: np.ndarray,
    contract_size_values: np.ndarray,
    admission_cap_values: np.ndarray,
    admission_min_lots_values: np.ndarray,
    admission_min_notional_values: np.ndarray,
    trail_activation_rr_values: np.ndarray,
    trail_distance_r_values: np.ndarray,
    chandelier_atr_mult_values: np.ndarray,
    sar_speed_values: np.ndarray,
    tick_size_values: np.ndarray,
    start_ns_values: np.ndarray,
    end_ns_values: np.ndarray,
    enable_long_values: np.ndarray,
    enable_short_values: np.ndarray,
    target_enabled_values: np.ndarray,
    trail_mode_code_values: np.ndarray,
    max_days_enabled_values: np.ndarray,
    use_date_filter_values: np.ndarray,
    strict_boundary_values: np.ndarray,
    boundary_none_values: np.ndarray,
    report_margin_values: np.ndarray,
    rounding_code_values: np.ndarray,
    compute_sharpe: bool,
    compute_sharpe_daily: bool,
    compute_sqn: bool,
    compute_max_drawdown_mtm: bool,
    outputs: np.ndarray,
    mtm_sidecar: np.ndarray,
) -> None:
    n = close_values.shape[0]
    initial_capital = initial_capital_values[candidate_index]
    admission_cap = admission_cap_values[candidate_index]
    admission_active = admission_cap > 0
    minimum_lots = admission_min_lots_values[candidate_index]
    minimum_notional = admission_min_notional_values[candidate_index]
    quantity_active = not admission_active and minimum_lots > 0
    if (admission_active or quantity_active) and not math.isfinite(initial_capital):
        _write_empty_result(outputs, candidate_index, 0.0)
        outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
        return
    if n == 0:
        _write_empty_result(outputs, candidate_index, initial_capital)
        if compute_max_drawdown_mtm:
            mtm_sidecar[candidate_index] = math.nan
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
    trail_code = trail_mode_code_values[candidate_index]
    trail_enabled = trail_code != TRAIL_MODE_NONE
    ma_trail_enabled = trail_code == TRAIL_MODE_MA
    stateful_trail_enabled = trail_code >= TRAIL_MODE_R_DISTANCE
    trail_distance_r = 0.0
    chandelier_atr_mult = 0.0
    sar_speed = 0.0
    if trail_code == TRAIL_MODE_R_DISTANCE:
        trail_distance_r = trail_distance_r_values[candidate_index]
    elif trail_code == TRAIL_MODE_CHANDELIER:
        chandelier_atr_mult = chandelier_atr_mult_values[candidate_index]
    elif trail_code == TRAIL_MODE_FIXED_AF_SAR:
        sar_speed = sar_speed_values[candidate_index]
    max_days_enabled = max_days_enabled_values[candidate_index]
    use_date_filter = use_date_filter_values[candidate_index]
    strict_boundary = strict_boundary_values[candidate_index]
    boundary_none = boundary_none_values[candidate_index]
    report_margin = report_margin_values[candidate_index]
    rounding_code = rounding_code_values[candidate_index]

    day_ns = 86_400_000_000_000.0
    balance = initial_capital
    running_peak = math.nan
    last_drawdown_balance = math.nan
    max_drawdown = 0.0

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
    trail_armed = False
    trail_method_active = False
    best_extreme = math.nan
    sar_value = math.nan
    sar_ep = math.nan
    sar_activation_index = -1

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
    previous_month_equity = initial_capital
    last_equity = initial_capital
    monthly_count = 0
    monthly_mean = 0.0
    monthly_m2 = 0.0
    current_day = 0
    daily_started = False
    daily_opening_equity = initial_capital
    daily_last_equity = initial_capital
    daily_count = 0
    daily_active_days = 0
    daily_mean = 0.0
    daily_m2 = 0.0
    daily_valid = True
    sqn_count = 0
    sqn_mean = 0.0
    sqn_m2 = 0.0
    mtm_peak = initial_capital
    max_drawdown_mtm = 0.0

    invalid_stop_distance_count = 0
    zero_size_entry_count = 0
    rejected_fill_count = 0
    flags = 0
    cap_skips = 0
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
            if compute_sqn:
                sqn_count += 1
                sqn_delta = net_pnl - sqn_mean
                sqn_mean += sqn_delta / sqn_count
                sqn_m2 += sqn_delta * (net_pnl - sqn_mean)
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
            trail_armed = False
            trail_method_active = False
            best_extreme = math.nan
            sar_value = math.nan
            sar_ep = math.nan
            sar_activation_index = -1
            pending_market_close = False

        if pending_entry and quantity_active:
            notional = abs(open_price * pending_size)
            fee = notional * commission_rate
            if not math.isfinite(notional) or not math.isfinite(fee) or not math.isfinite(balance - fee):
                _write_empty_result(outputs, candidate_index, 0.0)
                outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
                return
            if not math.isnan(minimum_notional) and notional < minimum_notional:
                pending_entry = False

        if pending_entry and admission_active:
            notional = abs(open_price * pending_size)
            fee = notional * commission_rate
            denominator = balance - fee
            leverage = notional / denominator if denominator > 0 else math.nan
            finite = math.isfinite(notional) and math.isfinite(fee) and math.isfinite(denominator)
            finite = finite and (denominator <= 0 or math.isfinite(leverage))
            if not finite:
                pending_entry = False
            elif not math.isnan(minimum_notional) and notional < minimum_notional:
                pending_entry = False
            elif not math.isfinite(leverage):
                pending_entry = False
            elif leverage > admission_cap:
                pending_entry = False
                cap_skips += 1
                flags = flags | GUARDRAIL_FLAG_MARGIN_REJECT

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
            trail_armed = False
            trail_method_active = False
            best_extreme = math.nan
            sar_value = math.nan
            sar_ep = math.nan
            sar_activation_index = -1
            pending_entry = False
            entry_filled_this_bar = True
            had_position_this_bar = True

            notional = abs(open_price * size)
            if notional > max_notional:
                max_notional = notional
            if (report_margin or admission_active) and balance > 0.0:
                leverage = notional / balance
                if leverage > max_required_leverage:
                    max_required_leverage = leverage

            if ma_trail_enabled:
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
            if compute_sqn:
                sqn_count += 1
                sqn_delta = net_pnl - sqn_mean
                sqn_mean += sqn_delta / sqn_count
                sqn_m2 += sqn_delta * (net_pnl - sqn_mean)
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
            trail_armed = False
            trail_method_active = False
            best_extreme = math.nan
            sar_value = math.nan
            sar_ep = math.nan
            sar_activation_index = -1
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
                    if compute_sqn:
                        sqn_count += 1
                        sqn_delta = net_pnl - sqn_mean
                        sqn_mean += sqn_delta / sqn_count
                        sqn_m2 += sqn_delta * (net_pnl - sqn_mean)
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
                    trail_armed = False
                    trail_method_active = False
                    best_extreme = math.nan
                    sar_value = math.nan
                    sar_ep = math.nan
                    sar_activation_index = -1
                    pending_market_close = False
                    break
                current = endpoint

        if position != 0 and ma_trail_enabled and not entry_filled_this_bar:
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

        if position != 0 and stateful_trail_enabled:
            if not trail_armed:
                activation = anchor_price + position * initial_risk * trail_activation_rr
                activation_reached = high >= activation if position > 0 else low <= activation
                if activation_reached:
                    trail_armed = True
                    trail_active = True
                    best_extreme = high if position > 0 else low
                    if position > 0:
                        trail_stop = max(trail_stop, entry_price)
                    else:
                        trail_stop = min(trail_stop, entry_price)
                    if trail_code == TRAIL_MODE_FIXED_AF_SAR:
                        sar_value = entry_price
                        sar_ep = best_extreme
                        sar_activation_index = i

            if trail_armed:
                if position > 0:
                    best_extreme = max(best_extreme, high)
                else:
                    best_extreme = min(best_extreme, low)

                candidate = math.nan
                if trail_code == TRAIL_MODE_R_DISTANCE:
                    if position > 0:
                        candidate = best_extreme - initial_risk * trail_distance_r
                    else:
                        candidate = best_extreme + initial_risk * trail_distance_r
                elif trail_code == TRAIL_MODE_CHANDELIER:
                    chandelier_value = chandelier_atr[i]
                    if math.isfinite(chandelier_value):
                        if position > 0:
                            candidate = best_extreme - chandelier_value * chandelier_atr_mult
                        else:
                            candidate = best_extreme + chandelier_value * chandelier_atr_mult
                else:
                    if i > sar_activation_index:
                        if position > 0:
                            sar_ep = max(sar_ep, high)
                            raw_next = sar_value + sar_speed * (sar_ep - sar_value)
                            range_cap = low if i == 0 else min(low, low_values[i - 1])
                            sar_value = max(sar_value, min(raw_next, range_cap))
                        else:
                            sar_ep = min(sar_ep, low)
                            raw_next = sar_value + sar_speed * (sar_ep - sar_value)
                            range_cap = high if i == 0 else max(high, high_values[i - 1])
                            sar_value = min(sar_value, max(raw_next, range_cap))
                    candidate = sar_value

                if not trail_method_active:
                    candidate_valid = math.isfinite(candidate) and (
                        candidate < close if position > 0 else candidate > close
                    )
                    if candidate_valid:
                        trail_method_active = True
                if trail_method_active and math.isfinite(candidate):
                    candidate = _maybe_round_trail(position, candidate, rounding_code, tick_size)
                    if position > 0:
                        trail_stop = max(trail_stop, candidate)
                    else:
                        trail_stop = min(trail_stop, candidate)

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
                if compute_sqn:
                    sqn_count += 1
                    sqn_delta = net_pnl - sqn_mean
                    sqn_mean += sqn_delta / sqn_count
                    sqn_m2 += sqn_delta * (net_pnl - sqn_mean)
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
                trail_armed = False
                trail_method_active = False
                best_extreme = math.nan
                sar_value = math.nan
                sar_ep = math.nan
                sar_activation_index = -1

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
                allowed = True
                if admission_active:
                    if not math.isfinite(balance):
                        _write_empty_result(outputs, candidate_index, 0.0)
                        outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
                        return
                    swing = rolling_low[i] if direction > 0 else rolling_high[i]
                    allowed = (math.isfinite(atr_value) and math.isfinite(swing)
                        and math.isfinite(stop) and stop > 0 and math.isfinite(risk) and risk > 0
                        and balance > 0 and math.isfinite(target) and target > 0 and stop_pct <= max_stop_pct)
                order_size = 0.0
                if quantity_active:
                    if not math.isfinite(balance):
                        _write_empty_result(outputs, candidate_index, 0.0)
                        outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
                        return
                    if (math.isfinite(stop) and math.isfinite(risk) and risk > 0
                            and stop_pct <= max_stop_pct and balance > 0
                            and math.isfinite(risk_per_trade_pct) and risk_per_trade_pct > 0):
                        risk_cash = balance * risk_per_trade_pct / 100.0
                        raw_size = risk_cash / risk
                        ratio = raw_size / contract_size
                        if not math.isfinite(ratio) or ratio > 2**53:
                            _write_empty_result(outputs, candidate_index, 0.0)
                            outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
                            return
                        order_size = math.floor(ratio) * contract_size
                        recovered = order_size / contract_size
                        if not math.isfinite(order_size) or not math.isfinite(recovered) or recovered > 2**53:
                            _write_empty_result(outputs, candidate_index, 0.0)
                            outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
                            return
                        allowed = int(round(recovered)) >= minimum_lots
                elif allowed:
                    risk_cash = balance * risk_per_trade_pct / 100.0
                    raw_size = risk_cash / risk if risk > 0.0 else 0.0
                    ratio = raw_size / contract_size
                    if admission_active and (not math.isfinite(ratio) or ratio < 0 or ratio > 2**53):
                        _write_empty_result(outputs, candidate_index, 0.0)
                        outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
                        return
                    lots = math.floor(ratio)
                    order_size = lots * contract_size
                    if admission_active:
                        recovered = order_size / contract_size
                        if (not math.isfinite(order_size) or not math.isfinite(recovered)
                                or recovered < 0 or recovered > 2**53):
                            _write_empty_result(outputs, candidate_index, 0.0)
                            outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
                            return
                        allowed = int(round(recovered)) >= minimum_lots and order_size > 0
                if admission_active and not allowed:
                    pass
                elif not (math.isfinite(stop) and math.isfinite(risk) and risk > 0.0):
                    invalid_stop_distance_count += 1
                    flags = flags | GUARDRAIL_FLAG_INVALID_STOP_DISTANCE
                elif stop_pct > max_stop_pct:
                    rejected_fill_count += 1
                    flags = flags | GUARDRAIL_FLAG_REJECTED_FILL
                elif not (math.isfinite(order_size) and order_size > 0.0):
                    zero_size_entry_count += 1
                    flags = flags | GUARDRAIL_FLAG_ZERO_SIZE_ENTRY
                elif quantity_active and not allowed:
                    pass
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

        if admission_active or quantity_active:
            observed_equity = balance
            if position > 0:
                observed_equity += (close - entry_price) * size
            elif position < 0:
                observed_equity += (entry_price - close) * size
            if not math.isfinite(balance) or not math.isfinite(observed_equity):
                _write_empty_result(outputs, candidate_index, 0.0)
                outputs[candidate_index, OUTPUT_FLAGS] = GUARDRAIL_FLAG_EXECUTION_ERROR
                return

        if (
            compute_sharpe
            or compute_sharpe_daily
            or compute_max_drawdown_mtm
        ) and i >= trade_start_idx:
            unrealized = 0.0
            if position > 0:
                unrealized = (close - entry_price) * size
            elif position < 0:
                unrealized = (entry_price - close) * size
            equity_value = balance + unrealized
            last_equity = equity_value
            if compute_max_drawdown_mtm:
                if not math.isfinite(equity_value):
                    max_drawdown_mtm = math.nan
                elif not math.isnan(max_drawdown_mtm):
                    if equity_value > mtm_peak:
                        mtm_peak = equity_value
                    elif mtm_peak > 0.0 and equity_value < mtm_peak:
                        mtm_drawdown = (mtm_peak - equity_value) / mtm_peak * 100.0
                        if mtm_drawdown > max_drawdown_mtm:
                            max_drawdown_mtm = mtm_drawdown
            if compute_sharpe:
                month_key = month_ids[i]
                if current_month < 0:
                    current_month = month_key
                    month_start_equity = initial_capital
                elif month_key != current_month:
                    if month_start_equity > 0.0:
                        monthly_return = ((previous_month_equity / month_start_equity) - 1.0) * 100.0
                        monthly_count += 1
                        monthly_delta = monthly_return - monthly_mean
                        monthly_mean += monthly_delta / monthly_count
                        monthly_m2 += monthly_delta * (monthly_return - monthly_mean)
                    current_month = month_key
                    month_start_equity = previous_month_equity
                previous_month_equity = equity_value
            if compute_sharpe_daily:
                if not math.isfinite(equity_value):
                    daily_valid = False
                elif daily_valid:
                    day_key = day_ids[i]
                    if not daily_started:
                        daily_started = True
                        current_day = day_key
                        daily_opening_equity = initial_capital
                        daily_last_equity = equity_value
                    elif day_key != current_day:
                        if not math.isfinite(daily_opening_equity) or daily_opening_equity <= 0.0:
                            daily_valid = False
                        else:
                            daily_return = daily_last_equity / daily_opening_equity - 1.0
                            if not math.isfinite(daily_return):
                                daily_valid = False
                            else:
                                daily_count += 1
                                daily_delta = daily_return - daily_mean
                                daily_mean += daily_delta / daily_count
                                daily_m2 += daily_delta * (daily_return - daily_mean)
                                if abs(daily_return) > 1e-12:
                                    daily_active_days += 1
                        current_day = day_key
                        daily_opening_equity = daily_last_equity
                        daily_last_equity = equity_value
                    else:
                        daily_last_equity = equity_value

        if balance != last_drawdown_balance:
            last_drawdown_balance = balance
            if math.isfinite(balance):
                if not math.isfinite(running_peak) or balance > running_peak:
                    running_peak = balance
                elif running_peak > 0.0 and balance < running_peak:
                    drawdown = (running_peak - balance) / running_peak * 100.0
                    if drawdown > max_drawdown:
                        max_drawdown = drawdown

        previous_close_position = position

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

    sharpe_ratio = math.nan
    if compute_sharpe and total_trades > 0:
        if month_start_equity > 0.0:
            monthly_return = ((previous_month_equity / month_start_equity) - 1.0) * 100.0
            monthly_count += 1
            monthly_delta = monthly_return - monthly_mean
            monthly_mean += monthly_delta / monthly_count
            monthly_m2 += monthly_delta * (monthly_return - monthly_mean)
        if monthly_count >= 2:
            monthly_variance = monthly_m2 / monthly_count
            if monthly_variance > 0.0 and math.isfinite(monthly_variance):
                sharpe_ratio = (monthly_mean - (0.02 * 100.0) / 12.0) / math.sqrt(
                    monthly_variance
                )
                if not math.isfinite(sharpe_ratio):
                    sharpe_ratio = math.nan

    sqn = math.nan
    if compute_sqn and sqn_count >= 30:
        sqn_variance = sqn_m2 / (sqn_count - 1)
        if sqn_variance > 0.0 and math.isfinite(sqn_variance):
            sqn_std = math.sqrt(sqn_variance)
            if sqn_std >= 1e-10:
                sqn = math.sqrt(sqn_count) * sqn_mean / sqn_std
                if not math.isfinite(sqn):
                    sqn = math.nan

    sharpe_daily = math.nan
    sharpe_daily_observations = math.nan
    sharpe_daily_active = math.nan
    if compute_sharpe_daily:
        if daily_valid and daily_started:
            if not math.isfinite(daily_opening_equity) or daily_opening_equity <= 0.0:
                daily_valid = False
            else:
                daily_return = daily_last_equity / daily_opening_equity - 1.0
                if not math.isfinite(daily_return):
                    daily_valid = False
                else:
                    daily_count += 1
                    daily_delta = daily_return - daily_mean
                    daily_mean += daily_delta / daily_count
                    daily_m2 += daily_delta * (daily_return - daily_mean)
                    if abs(daily_return) > 1e-12:
                        daily_active_days += 1
        if daily_valid and daily_started:
            sharpe_daily_observations = float(daily_count)
            sharpe_daily_active = float(daily_active_days)
            if total_trades > 0 and daily_count >= 2:
                daily_variance = daily_m2 / daily_count
                if math.isfinite(daily_variance) and daily_variance > 0.0:
                    sharpe_daily = math.sqrt(365.0) * (daily_mean - 0.02 / 365.0) / math.sqrt(daily_variance)
                    if not math.isfinite(sharpe_daily):
                        sharpe_daily = math.nan

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
    outputs[candidate_index, OUTPUT_MARGIN_REJECT_COUNT] = cap_skips
    outputs[candidate_index, OUTPUT_LIQUIDATION_COUNT] = 0.0
    outputs[candidate_index, OUTPUT_NO_CAPITAL_HALT] = 0.0
    outputs[candidate_index, OUTPUT_MAX_REQUIRED_LEVERAGE] = max_required_leverage
    outputs[candidate_index, OUTPUT_MAX_NOTIONAL] = max_notional
    outputs[candidate_index, OUTPUT_FLAGS] = flags
    outputs[candidate_index, OUTPUT_SHARPE_RATIO] = sharpe_ratio
    outputs[candidate_index, OUTPUT_SQN] = sqn
    outputs[candidate_index, OUTPUT_SHARPE_DAILY] = sharpe_daily
    outputs[candidate_index, OUTPUT_SHARPE_DAILY_OBSERVATIONS] = sharpe_daily_observations
    outputs[candidate_index, OUTPUT_SHARPE_DAILY_ACTIVE_DAYS] = sharpe_daily_active
    if compute_max_drawdown_mtm:
        if trade_start_idx >= n:
            mtm_sidecar[candidate_index] = math.nan
        else:
            mtm_sidecar[candidate_index] = max_drawdown_mtm


def _batch_loop_impl(
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    month_ids: np.ndarray,
    day_ids: np.ndarray,
    long_signals: np.ndarray,
    short_signals: np.ndarray,
    atr_values: np.ndarray,
    rolling_low: np.ndarray,
    rolling_high: np.ndarray,
    trail_long: np.ndarray,
    trail_short: np.ndarray,
    chandelier_atr: np.ndarray,
    trade_start_idx: int,
    initial_capital_values: np.ndarray,
    commission_pct_values: np.ndarray,
    stop_x_values: np.ndarray,
    reward_risk_values: np.ndarray,
    max_stop_pct_values: np.ndarray,
    max_days_values: np.ndarray,
    risk_per_trade_pct_values: np.ndarray,
    contract_size_values: np.ndarray,
    admission_cap_values: np.ndarray,
    admission_min_lots_values: np.ndarray,
    admission_min_notional_values: np.ndarray,
    trail_activation_rr_values: np.ndarray,
    trail_distance_r_values: np.ndarray,
    chandelier_atr_mult_values: np.ndarray,
    sar_speed_values: np.ndarray,
    tick_size_values: np.ndarray,
    start_ns_values: np.ndarray,
    end_ns_values: np.ndarray,
    enable_long_values: np.ndarray,
    enable_short_values: np.ndarray,
    target_enabled_values: np.ndarray,
    trail_mode_code_values: np.ndarray,
    max_days_enabled_values: np.ndarray,
    use_date_filter_values: np.ndarray,
    strict_boundary_values: np.ndarray,
    boundary_none_values: np.ndarray,
    report_margin_values: np.ndarray,
    rounding_code_values: np.ndarray,
    compute_sharpe: bool,
    compute_sharpe_daily: bool,
    compute_sqn: bool,
    compute_max_drawdown_mtm: bool,
    outputs: np.ndarray,
    mtm_sidecar: np.ndarray,
) -> None:
    for index in numba.prange(outputs.shape[0]):
        _compiled_loop_one(
            index,
            open_values,
            high_values,
            low_values,
            close_values,
            timestamp_ns,
            month_ids,
            day_ids,
            long_signals,
            short_signals,
            atr_values,
            rolling_low,
            rolling_high,
            trail_long,
            trail_short,
            chandelier_atr,
            trade_start_idx,
            initial_capital_values,
            commission_pct_values,
            stop_x_values,
            reward_risk_values,
            max_stop_pct_values,
            max_days_values,
            risk_per_trade_pct_values,
            contract_size_values,
            admission_cap_values,
            admission_min_lots_values,
            admission_min_notional_values,
            trail_activation_rr_values,
            trail_distance_r_values,
            chandelier_atr_mult_values,
            sar_speed_values,
            tick_size_values,
            start_ns_values,
            end_ns_values,
            enable_long_values,
            enable_short_values,
            target_enabled_values,
            trail_mode_code_values,
            max_days_enabled_values,
            use_date_filter_values,
            strict_boundary_values,
            boundary_none_values,
            report_margin_values,
            rounding_code_values,
            compute_sharpe,
            compute_sharpe_daily,
            compute_sqn,
            compute_max_drawdown_mtm,
            outputs,
            mtm_sidecar,
        )


def _stacked_batch_loop_impl(
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    month_ids: np.ndarray,
    day_ids: np.ndarray,
    long_entries: np.ndarray,
    short_entries: np.ndarray,
    atr_values: np.ndarray,
    rolling_low: np.ndarray,
    rolling_high: np.ndarray,
    trail_long: np.ndarray,
    trail_short: np.ndarray,
    chandelier_atr: np.ndarray,
    chandelier_data_index: np.ndarray,
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
    admission_cap_values: np.ndarray,
    admission_min_lots_values: np.ndarray,
    admission_min_notional_values: np.ndarray,
    trail_activation_rr_values: np.ndarray,
    trail_distance_r_values: np.ndarray,
    chandelier_atr_mult_values: np.ndarray,
    sar_speed_values: np.ndarray,
    tick_size_values: np.ndarray,
    start_ns_values: np.ndarray,
    end_ns_values: np.ndarray,
    enable_long_values: np.ndarray,
    enable_short_values: np.ndarray,
    target_enabled_values: np.ndarray,
    trail_mode_code_values: np.ndarray,
    max_days_enabled_values: np.ndarray,
    use_date_filter_values: np.ndarray,
    strict_boundary_values: np.ndarray,
    boundary_none_values: np.ndarray,
    report_margin_values: np.ndarray,
    rounding_code_values: np.ndarray,
    compute_sharpe: bool,
    compute_sharpe_daily: bool,
    compute_sqn: bool,
    compute_max_drawdown_mtm: bool,
    outputs: np.ndarray,
    mtm_sidecar: np.ndarray,
) -> None:
    for index in numba.prange(outputs.shape[0]):
        row = data_index[index]
        chandelier_row = -1
        if chandelier_data_index.shape[0] != 0:
            chandelier_row = chandelier_data_index[row]
        candidate_chandelier_atr = close_values[:0]
        if chandelier_row >= 0:
            candidate_chandelier_atr = chandelier_atr[chandelier_row]
        _compiled_loop_one(
            index,
            open_values,
            high_values,
            low_values,
            close_values,
            timestamp_ns,
            month_ids,
            day_ids,
            long_entries[row],
            short_entries[row],
            atr_values[row],
            rolling_low[row],
            rolling_high[row],
            trail_long[row],
            trail_short[row],
            candidate_chandelier_atr,
            trade_start_idx,
            initial_capital_values,
            commission_pct_values,
            stop_x_values,
            reward_risk_values,
            max_stop_pct_values,
            max_days_values,
            risk_per_trade_pct_values,
            contract_size_values,
            admission_cap_values,
            admission_min_lots_values,
            admission_min_notional_values,
            trail_activation_rr_values,
            trail_distance_r_values,
            chandelier_atr_mult_values,
            sar_speed_values,
            tick_size_values,
            start_ns_values,
            end_ns_values,
            enable_long_values,
            enable_short_values,
            target_enabled_values,
            trail_mode_code_values,
            max_days_enabled_values,
            use_date_filter_values,
            strict_boundary_values,
            boundary_none_values,
            report_margin_values,
            rounding_code_values,
            compute_sharpe,
            compute_sharpe_daily,
            compute_sqn,
            compute_max_drawdown_mtm,
            outputs,
            mtm_sidecar,
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
    "OUTPUT_SHARPE_RATIO",
    "OUTPUT_SHARPE_DAILY",
    "OUTPUT_SHARPE_DAILY_OBSERVATIONS",
    "OUTPUT_SHARPE_DAILY_ACTIVE_DAYS",
    "OUTPUT_SQN",
    "OUTPUT_TOTAL_TRADES",
    "OUTPUT_WINNING_TRADES",
    "OUTPUT_WIN_RATE_PCT",
    "OUTPUT_ZERO_SIZE_ENTRY_COUNT",
    "StackedExecutionData",
    "build_calendar_month_ids",
    "build_stacked_execution_data",
    "compiled_batch_available",
    "compiled_unavailable_reason",
    "evaluate_compiled_batch",
    "evaluate_compiled_stacked_batch",
    "pack_compiled_config_arrays_from_rows",
]
