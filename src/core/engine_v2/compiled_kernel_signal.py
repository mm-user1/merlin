"""Compiled stacked evaluator for signal-reversal Backtester V2 profiles."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from . import compiled_kernel as _base
from .compiled_kernel import (
    COMPILED_BATCH_KIND,
    OUTPUT_COLUMN_COUNT,
    OUTPUT_FINAL_BALANCE,
    OUTPUT_FLAGS,
    OUTPUT_GROSS_LOSS,
    OUTPUT_GROSS_PROFIT,
    OUTPUT_INVALID_STOP_DISTANCE_COUNT,
    OUTPUT_LIQUIDATION_COUNT,
    OUTPUT_LOSING_TRADES,
    OUTPUT_MARGIN_REJECT_COUNT,
    OUTPUT_MAX_CONSECUTIVE_LOSSES,
    OUTPUT_MAX_DRAWDOWN_PCT,
    OUTPUT_MAX_NOTIONAL,
    OUTPUT_MAX_REQUIRED_LEVERAGE,
    OUTPUT_NET_PROFIT_PCT,
    OUTPUT_NO_CAPITAL_HALT,
    OUTPUT_PROFIT_FACTOR,
    OUTPUT_REJECTED_FILL_COUNT,
    OUTPUT_ROMAD,
    OUTPUT_TOTAL_TRADES,
    OUTPUT_WINNING_TRADES,
    OUTPUT_WIN_RATE_PCT,
    OUTPUT_ZERO_SIZE_ENTRY_COUNT,
    CompiledBatchOutput,
)
from .contracts import GUARDRAIL_FLAG_ZERO_SIZE_ENTRY
from .kernel import ExecutionData
from .price_rounding import PRICE_ROUNDING_NONE
from .profile import active_mode_values


REFERENCE_UNAVAILABLE_REASON = _base.REFERENCE_UNAVAILABLE_REASON
_write_empty_result = _base._write_empty_result
numba = _base.numba


@dataclass(frozen=True)
class SignalStackedExecutionData:
    """Shared market arrays plus stacked signal rows for signal-reversal profiles."""

    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    timestamp_ns: np.ndarray
    long_entries: np.ndarray
    short_entries: np.ndarray
    long_exits: np.ndarray
    short_exits: np.ndarray
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
        return int(
            self.long_entries.nbytes
            + self.short_entries.nbytes
            + self.long_exits.nbytes
            + self.short_exits.nbytes
        )

    @property
    def dataprep_nbytes(self) -> int:
        return 0

    @property
    def nbytes(self) -> int:
        return int(self.shared_market_nbytes + self.signal_nbytes)


_SIGNAL_CONFIG_ARRAY_DTYPES: Mapping[str, Any] = {
    "initial_capital": np.float64,
    "commission_pct": np.float64,
    "position_pct": np.float64,
    "contract_size": np.float64,
    "emergency_sl_pct": np.float64,
    "emergency_sl_update_bars": np.int64,
    "start_ns": np.int64,
    "end_ns": np.int64,
    "enable_long": np.bool_,
    "enable_short": np.bool_,
    "emergency_stop_enabled": np.bool_,
    "use_date_filter": np.bool_,
    "strict_boundary": np.bool_,
    "boundary_none": np.bool_,
}


def build_signal_stacked_execution_data(
    data_rows: Sequence[ExecutionData],
    data_index: Sequence[int],
) -> SignalStackedExecutionData:
    """Build and validate a stacked compiled payload for signal-only profiles."""

    rows = tuple(data_rows)
    if not rows:
        raise ValueError("Signal stacked compiled execution requires at least one ExecutionData row.")

    first = rows[0]
    open_values = _base._contiguous_1d(first.open, "open", np.float64)
    high_values = _base._contiguous_1d(first.high, "high", np.float64)
    low_values = _base._contiguous_1d(first.low, "low", np.float64)
    close_values = _base._contiguous_1d(first.close, "close", np.float64)
    timestamp_ns = _base._timestamps_ns(first.timestamps)

    long_entry_rows: list[np.ndarray] = []
    short_entry_rows: list[np.ndarray] = []
    long_exit_rows: list[np.ndarray] = []
    short_exit_rows: list[np.ndarray] = []

    for row_number, data in enumerate(rows):
        if data.timestamps is not first.timestamps:
            row_timestamps = _base._timestamps_ns(data.timestamps)
            if row_timestamps.shape != timestamp_ns.shape or not np.array_equal(row_timestamps, timestamp_ns):
                raise ValueError(
                    "Signal stacked compiled execution requires shared OHLC/timestamps across all ExecutionData rows; "
                    f"timestamp mismatch at row {row_number}."
                )
        for name, expected in (
            ("open", open_values),
            ("high", high_values),
            ("low", low_values),
            ("close", close_values),
        ):
            actual = _base._contiguous_1d(getattr(data, name), name, np.float64)
            if actual.shape != expected.shape or not np.array_equal(actual, expected, equal_nan=True):
                raise ValueError(
                    "Signal stacked compiled execution requires shared OHLC/timestamps across all ExecutionData rows; "
                    f"{name} mismatch at row {row_number}."
                )

        length = len(data.timestamps)
        long_entry_rows.append(_base._contiguous_1d(data.signals.long_entries, "signals.long_entries", np.bool_))
        short_entry_rows.append(_base._contiguous_1d(data.signals.short_entries, "signals.short_entries", np.bool_))
        long_exits = data.signals.long_exits
        short_exits = data.signals.short_exits
        long_exit_rows.append(
            _base._contiguous_1d(
                np.zeros(length, dtype=np.bool_) if long_exits is None else long_exits,
                "signals.long_exits",
                np.bool_,
            )
        )
        short_exit_rows.append(
            _base._contiguous_1d(
                np.zeros(length, dtype=np.bool_) if short_exits is None else short_exits,
                "signals.short_exits",
                np.bool_,
            )
        )

    index_array = np.asarray(data_index, dtype=np.int64)
    if index_array.ndim != 1:
        raise ValueError("Signal stacked compiled execution data_index must be a 1D array.")
    if index_array.size:
        if int(index_array.min()) < 0 or int(index_array.max()) >= len(rows):
            raise ValueError("Signal stacked compiled execution data_index contains an out-of-range row index.")

    return SignalStackedExecutionData(
        open=open_values,
        high=high_values,
        low=low_values,
        close=close_values,
        timestamp_ns=timestamp_ns,
        long_entries=np.ascontiguousarray(np.stack(long_entry_rows, axis=0), dtype=np.bool_),
        short_entries=np.ascontiguousarray(np.stack(short_entry_rows, axis=0), dtype=np.bool_),
        long_exits=np.ascontiguousarray(np.stack(long_exit_rows, axis=0), dtype=np.bool_),
        short_exits=np.ascontiguousarray(np.stack(short_exit_rows, axis=0), dtype=np.bool_),
        data_index=np.ascontiguousarray(index_array, dtype=np.int64),
    )


def evaluate_compiled_signal_stacked_batch(
    *,
    stacked_data: SignalStackedExecutionData,
    profile: Any,
    params_batch: Sequence[Mapping[str, Any]] | None = None,
    trade_start_idx: int,
    n_workers: int = 1,
    packed_config_arrays: Mapping[str, np.ndarray] | None = None,
) -> CompiledBatchOutput:
    """Evaluate one stacked signal-reversal compiled batch."""

    if not _base.compiled_batch_available():
        raise RuntimeError(_base.compiled_unavailable_reason() or REFERENCE_UNAVAILABLE_REASON)
    if packed_config_arrays is None:
        if not params_batch:
            return CompiledBatchOutput(
                outputs=np.empty((0, OUTPUT_COLUMN_COUNT), dtype=np.float64),
                execution_mode="stacked",
            )
        if len(params_batch) != stacked_data.candidate_count:
            raise ValueError("Signal stacked compiled params_batch length must match data_index length.")
        packed = _pack_signal_config_arrays(profile, params_batch)
        candidate_count = len(params_batch)
    else:
        packed = _validated_signal_config_arrays(packed_config_arrays)
        candidate_count = _packed_signal_config_count(packed)
        if candidate_count != stacked_data.candidate_count:
            raise ValueError("Signal stacked compiled packed_config_arrays length must match data_index length.")
        if params_batch is not None and len(params_batch) != candidate_count:
            raise ValueError("Signal stacked compiled params_batch length must match packed_config_arrays length.")

    if candidate_count == 0:
        return CompiledBatchOutput(
            outputs=np.empty((0, OUTPUT_COLUMN_COUNT), dtype=np.float64),
            execution_mode="stacked",
        )

    outputs = np.empty((candidate_count, OUTPUT_COLUMN_COUNT), dtype=np.float64)
    worker_count = _base._validated_worker_count(n_workers)
    previous_threads = numba.get_num_threads()
    target_threads = max(1, min(worker_count, previous_threads))
    try:
        if target_threads != previous_threads:
            numba.set_num_threads(target_threads)
        _COMPILED_SIGNAL_STACKED_BATCH_LOOP(
            stacked_data.open,
            stacked_data.high,
            stacked_data.low,
            stacked_data.close,
            stacked_data.timestamp_ns,
            stacked_data.long_entries,
            stacked_data.short_entries,
            stacked_data.long_exits,
            stacked_data.short_exits,
            stacked_data.data_index,
            int(trade_start_idx),
            packed["initial_capital"],
            packed["commission_pct"],
            packed["position_pct"],
            packed["contract_size"],
            packed["emergency_sl_pct"],
            packed["emergency_sl_update_bars"],
            packed["start_ns"],
            packed["end_ns"],
            packed["enable_long"],
            packed["enable_short"],
            packed["emergency_stop_enabled"],
            packed["use_date_filter"],
            packed["strict_boundary"],
            packed["boundary_none"],
            outputs,
        )
    finally:
        if numba.get_num_threads() != previous_threads:
            numba.set_num_threads(previous_threads)
    return CompiledBatchOutput(outputs=outputs, backend_kind=COMPILED_BATCH_KIND, execution_mode="stacked")


def _empty_signal_config_arrays(count: int) -> dict[str, np.ndarray]:
    return {
        name: np.empty(int(count), dtype=dtype)
        for name, dtype in _SIGNAL_CONFIG_ARRAY_DTYPES.items()
    }


def _validated_signal_config_arrays(values: Mapping[str, Any]) -> dict[str, np.ndarray]:
    arrays: dict[str, np.ndarray] = {}
    count: int | None = None
    for name, dtype in _SIGNAL_CONFIG_ARRAY_DTYPES.items():
        if name not in values:
            raise ValueError(f"Packed compiled signal config arrays missing required field {name!r}.")
        array = np.asarray(values[name], dtype=dtype)
        if array.ndim != 1:
            raise ValueError(f"Packed compiled signal config array {name!r} must be 1D.")
        if count is None:
            count = int(array.shape[0])
        elif int(array.shape[0]) != count:
            raise ValueError("Packed compiled signal config arrays must all have the same length.")
        arrays[name] = np.ascontiguousarray(array, dtype=dtype)
    return arrays


def _packed_signal_config_count(arrays: Mapping[str, np.ndarray]) -> int:
    first = next(iter(_SIGNAL_CONFIG_ARRAY_DTYPES))
    return int(np.asarray(arrays[first]).shape[0])


def _pack_signal_config_arrays(
    profile: Any,
    params_batch: Sequence[Mapping[str, Any]],
) -> dict[str, np.ndarray]:
    count = len(params_batch)
    arrays = _empty_signal_config_arrays(count)
    mode_cache: dict[tuple[tuple[str, str], ...], tuple[bool, bool, bool]] = {}
    start_ns_cache: dict[Any, int] = {}
    end_ns_cache: dict[Any, int] = {}
    for index, params in enumerate(params_batch):
        modes = active_mode_values(profile, params)
        mode_key = tuple(sorted((str(key), str(value)) for key, value in modes.items()))
        mode_state = mode_cache.get(mode_key)
        if mode_state is None:
            mode_state = _signal_mode_state(modes)
            mode_cache[mode_key] = mode_state
        emergency_stop_enabled, strict_boundary, boundary_none = mode_state

        emergency_sl_pct = float(params.get("emergencySlPct", 20.0))
        emergency_sl_update_bars = int(params.get("emergencySlUpdateBars", 16))
        if emergency_stop_enabled:
            if emergency_sl_pct <= 0.0:
                raise ValueError("emergencySlPct must be > 0 when useEmergencySL=true.")
            if emergency_sl_update_bars < 1:
                raise ValueError("emergencySlUpdateBars must be >= 1 when useEmergencySL=true.")

        arrays["initial_capital"][index] = float(params.get("initialCapital", 100.0))
        arrays["commission_pct"][index] = float(params.get("commissionPct", 0.0))
        arrays["position_pct"][index] = float(params.get("positionPct", 100.0))
        arrays["contract_size"][index] = float(params.get("contractSize", 0.01))
        arrays["emergency_sl_pct"][index] = emergency_sl_pct
        arrays["emergency_sl_update_bars"][index] = emergency_sl_update_bars
        arrays["start_ns"][index] = _base._cached_timestamp_ns(
            params.get("start"),
            np.iinfo(np.int64).min,
            start_ns_cache,
        )
        arrays["end_ns"][index] = _base._cached_timestamp_ns(
            params.get("end"),
            np.iinfo(np.int64).max,
            end_ns_cache,
        )
        arrays["enable_long"][index] = _base._coerce_bool(params.get("enableLong"), True)
        arrays["enable_short"][index] = _base._coerce_bool(params.get("enableShort"), True)
        arrays["emergency_stop_enabled"][index] = emergency_stop_enabled
        arrays["use_date_filter"][index] = _base._coerce_bool(params.get("dateFilter"), True)
        arrays["strict_boundary"][index] = strict_boundary
        arrays["boundary_none"][index] = boundary_none
    return arrays


def _signal_mode_state(modes: Mapping[str, str]) -> tuple[bool, bool, bool]:
    _require_signal_mode(modes, "topology", "signal_reversal")
    _require_signal_mode(modes, "entryOrder", "market_next_open")
    _require_signal_mode(modes, "sizing", "fixed_pct_equity")
    _require_signal_mode(modes, "exitOnSignal", "true")
    _require_signal_mode(modes, "priceRounding", PRICE_ROUNDING_NONE)
    _require_signal_absent_or(modes, "target", {"none"})
    _require_signal_absent_or(modes, "trail", {"none"})
    _require_signal_absent_or(modes, "trailActivation", {"none"})
    _require_signal_absent_or(modes, "maxDays", {"false"})
    _require_signal_absent_or(modes, "margin", {"off"})

    stop_mode = modes.get("stop", "none")
    if stop_mode not in {"none", "emergency_pct"}:
        raise ValueError(
            f"Unsupported signal_reversal execution mode stop={stop_mode!r}; "
            "expected 'none' or 'emergency_pct'."
        )
    boundary_mode = modes.get("boundary", "strict_close")
    if boundary_mode not in {"strict_close", "none"}:
        raise ValueError(f"Unsupported signal_reversal boundary mode: {boundary_mode!r}.")
    return stop_mode == "emergency_pct", boundary_mode == "strict_close", boundary_mode == "none"


def _require_signal_mode(modes: Mapping[str, str], name: str, expected: str) -> None:
    actual = modes.get(name)
    if actual != expected:
        raise ValueError(f"Unsupported signal_reversal execution mode {name}={actual!r}; expected {expected!r}.")


def _require_signal_absent_or(modes: Mapping[str, str], name: str, allowed: set[str]) -> str:
    actual = modes.get(name)
    if actual is None:
        return ""
    if actual not in allowed:
        expected = ", ".join(repr(value) for value in sorted(allowed))
        raise ValueError(f"Unsupported signal_reversal execution mode {name}={actual!r}; expected one of {expected}.")
    return actual


def _signal_stacked_batch_loop_impl(
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    long_entries: np.ndarray,
    short_entries: np.ndarray,
    long_exits: np.ndarray,
    short_exits: np.ndarray,
    data_index: np.ndarray,
    trade_start_idx: int,
    initial_capital_values: np.ndarray,
    commission_pct_values: np.ndarray,
    position_pct_values: np.ndarray,
    contract_size_values: np.ndarray,
    emergency_sl_pct_values: np.ndarray,
    emergency_sl_update_bars_values: np.ndarray,
    start_ns_values: np.ndarray,
    end_ns_values: np.ndarray,
    enable_long_values: np.ndarray,
    enable_short_values: np.ndarray,
    emergency_stop_enabled_values: np.ndarray,
    use_date_filter_values: np.ndarray,
    strict_boundary_values: np.ndarray,
    boundary_none_values: np.ndarray,
    outputs: np.ndarray,
) -> None:
    for index in numba.prange(outputs.shape[0]):
        row = data_index[index]
        _compiled_signal_loop_one(
            index,
            open_values,
            high_values,
            low_values,
            close_values,
            timestamp_ns,
            long_entries[row],
            short_entries[row],
            long_exits[row],
            short_exits[row],
            trade_start_idx,
            initial_capital_values,
            commission_pct_values,
            position_pct_values,
            contract_size_values,
            emergency_sl_pct_values,
            emergency_sl_update_bars_values,
            start_ns_values,
            end_ns_values,
            enable_long_values,
            enable_short_values,
            emergency_stop_enabled_values,
            use_date_filter_values,
            strict_boundary_values,
            boundary_none_values,
            outputs,
        )


def _compiled_signal_loop_one(
    candidate_index: int,
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    timestamp_ns: np.ndarray,
    long_entries: np.ndarray,
    short_entries: np.ndarray,
    long_exits: np.ndarray,
    short_exits: np.ndarray,
    trade_start_idx: int,
    initial_capital_values: np.ndarray,
    commission_pct_values: np.ndarray,
    position_pct_values: np.ndarray,
    contract_size_values: np.ndarray,
    emergency_sl_pct_values: np.ndarray,
    emergency_sl_update_bars_values: np.ndarray,
    start_ns_values: np.ndarray,
    end_ns_values: np.ndarray,
    enable_long_values: np.ndarray,
    enable_short_values: np.ndarray,
    emergency_stop_enabled_values: np.ndarray,
    use_date_filter_values: np.ndarray,
    strict_boundary_values: np.ndarray,
    boundary_none_values: np.ndarray,
    outputs: np.ndarray,
) -> None:
    n = close_values.shape[0]
    initial_capital = initial_capital_values[candidate_index]
    if n == 0:
        _write_empty_result(outputs, candidate_index, initial_capital)
        return

    commission_rate = commission_pct_values[candidate_index] / 100.0
    position_pct = position_pct_values[candidate_index]
    contract_size = contract_size_values[candidate_index]
    emergency_sl_pct = emergency_sl_pct_values[candidate_index]
    emergency_sl_update_bars = emergency_sl_update_bars_values[candidate_index]
    start_ns = start_ns_values[candidate_index]
    end_ns = end_ns_values[candidate_index]
    enable_long = enable_long_values[candidate_index]
    enable_short = enable_short_values[candidate_index]
    emergency_stop_enabled = emergency_stop_enabled_values[candidate_index]
    use_date_filter = use_date_filter_values[candidate_index]
    strict_boundary = strict_boundary_values[candidate_index]
    boundary_none = boundary_none_values[candidate_index]

    balance = initial_capital
    running_peak = initial_capital
    current_drawdown = 0.0
    max_drawdown = 0.0
    last_drawdown_boundary = -1

    position = 0
    size = 0.0
    entry_price = math.nan
    entry_commission = 0.0
    emergency_stop = math.nan
    emergency_fill_index = -1
    emergency_counter = 0

    pending_entry = False
    pending_direction = 0
    pending_size = 0.0
    pending_market_close = False

    total_trades = 0
    winning_trades = 0
    losing_trades = 0
    gross_profit = 0.0
    gross_loss = 0.0
    consecutive_losses = 0
    max_consecutive_losses = 0

    zero_size_entry_count = 0
    flags = 0
    max_notional = 0.0
    last_bar_index = n - 1

    for i in range(n):
        open_price = open_values[i]
        high = high_values[i]
        low = low_values[i]
        close = close_values[i]

        if pending_market_close:
            if position != 0:
                balance_delta = 0.0
                net_pnl = 0.0
                if position > 0:
                    gross_pnl = (open_price - entry_price) * size
                else:
                    gross_pnl = (entry_price - open_price) * size
                exit_commission = open_price * size * commission_rate
                net_pnl = gross_pnl - entry_commission - exit_commission
                balance_delta = gross_pnl - exit_commission
                balance += balance_delta
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
                entry_commission = 0.0
                emergency_stop = math.nan
                emergency_fill_index = -1
                emergency_counter = 0
            pending_market_close = False

        if pending_entry and position == 0:
            position = pending_direction
            size = pending_size
            entry_price = open_price
            entry_commission = open_price * size * commission_rate
            balance -= entry_commission
            pending_entry = False
            if emergency_stop_enabled:
                if position > 0:
                    emergency_stop = entry_price * (1.0 - emergency_sl_pct / 100.0)
                else:
                    emergency_stop = entry_price * (1.0 + emergency_sl_pct / 100.0)
                emergency_fill_index = i
                emergency_counter = 0
            notional = abs(open_price * size)
            if notional > max_notional:
                max_notional = notional

        if (
            emergency_stop_enabled
            and position != 0
            and emergency_fill_index >= 0
            and i >= emergency_fill_index + 1
            and math.isfinite(emergency_stop)
        ):
            emergency_exit_price = math.nan
            if position > 0 and low <= emergency_stop:
                emergency_exit_price = min(open_price, emergency_stop)
            elif position < 0 and high >= emergency_stop:
                emergency_exit_price = max(open_price, emergency_stop)
            if math.isfinite(emergency_exit_price):
                if position > 0:
                    gross_pnl = (emergency_exit_price - entry_price) * size
                else:
                    gross_pnl = (entry_price - emergency_exit_price) * size
                exit_commission = emergency_exit_price * size * commission_rate
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
                entry_commission = 0.0
                emergency_stop = math.nan
                emergency_fill_index = -1
                emergency_counter = 0
                pending_market_close = False

        if (
            emergency_stop_enabled
            and position != 0
            and emergency_fill_index >= 0
            and i >= emergency_fill_index + 1
            and math.isfinite(emergency_stop)
        ):
            emergency_counter += 1
            if emergency_counter >= emergency_sl_update_bars:
                if position > 0:
                    candidate_stop = close * (1.0 - emergency_sl_pct / 100.0)
                    if candidate_stop > emergency_stop:
                        emergency_stop = candidate_stop
                else:
                    candidate_stop = close * (1.0 + emergency_sl_pct / 100.0)
                    if candidate_stop < emergency_stop:
                        emergency_stop = candidate_stop
                emergency_counter = 0

        in_range = i >= trade_start_idx
        if use_date_filter:
            in_range = in_range and timestamp_ns[i] >= start_ns and timestamp_ns[i] <= end_ns
        can_plan_order = (i != last_bar_index) or boundary_none
        if can_plan_order:
            long_entry = long_entries[i]
            short_entry = short_entries[i]
            long_exit = long_exits[i]
            short_exit = short_exits[i]

            if position > 0:
                if short_entry or long_exit or not in_range:
                    pending_market_close = True
            elif position < 0:
                if long_entry or short_exit or not in_range:
                    pending_market_close = True

            if position == 0 and not pending_entry and in_range:
                direction = 0
                if enable_long and long_entry:
                    direction = 1
                elif enable_short and short_entry:
                    direction = -1
                if direction != 0:
                    raw_size = math.nan
                    if close > 0.0 and contract_size > 0.0:
                        raw_size = (balance * position_pct / 100.0 / close) / contract_size
                    order_size = math.floor(raw_size) * contract_size if math.isfinite(raw_size) else math.nan
                    if not (math.isfinite(order_size) and order_size > 0.0):
                        zero_size_entry_count += 1
                        flags = flags | GUARDRAIL_FLAG_ZERO_SIZE_ENTRY
                    else:
                        pending_entry = True
                        pending_direction = direction
                        pending_size = order_size

        if i == last_bar_index and strict_boundary:
            pending_entry = False
            pending_market_close = False
            if position != 0:
                if position > 0:
                    gross_pnl = (close - entry_price) * size
                else:
                    gross_pnl = (entry_price - close) * size
                exit_commission = close * size * commission_rate
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
                entry_commission = 0.0
                emergency_stop = math.nan
                emergency_fill_index = -1
                emergency_counter = 0

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
    outputs[candidate_index, OUTPUT_INVALID_STOP_DISTANCE_COUNT] = 0.0
    outputs[candidate_index, OUTPUT_ZERO_SIZE_ENTRY_COUNT] = zero_size_entry_count
    outputs[candidate_index, OUTPUT_REJECTED_FILL_COUNT] = 0.0
    outputs[candidate_index, OUTPUT_MARGIN_REJECT_COUNT] = 0.0
    outputs[candidate_index, OUTPUT_LIQUIDATION_COUNT] = 0.0
    outputs[candidate_index, OUTPUT_NO_CAPITAL_HALT] = 0.0
    outputs[candidate_index, OUTPUT_MAX_REQUIRED_LEVERAGE] = 0.0
    outputs[candidate_index, OUTPUT_MAX_NOTIONAL] = max_notional
    outputs[candidate_index, OUTPUT_FLAGS] = flags


if numba is not None:
    _compiled_signal_loop_one = numba.njit(cache=True)(_compiled_signal_loop_one)
    _COMPILED_SIGNAL_STACKED_BATCH_LOOP = numba.njit(cache=True, parallel=True)(_signal_stacked_batch_loop_impl)
else:  # pragma: no cover
    _COMPILED_SIGNAL_STACKED_BATCH_LOOP = _signal_stacked_batch_loop_impl


__all__ = [
    "SignalStackedExecutionData",
    "_pack_signal_config_arrays",
    "_signal_mode_state",
    "build_signal_stacked_execution_data",
    "evaluate_compiled_signal_stacked_batch",
]
