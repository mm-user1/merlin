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
from .price_rounding import PRICE_ROUNDING_NONE, PRICE_ROUNDING_TICK_OUTWARD
from .runner import build_kernel_config


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
    """Fixed-width compiled metrics for one homogeneous execution-data group."""

    outputs: np.ndarray
    backend_kind: str = COMPILED_BATCH_KIND


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
) -> CompiledBatchOutput:
    """Evaluate one batch of candidates sharing the same ``ExecutionData``."""

    if not compiled_batch_available():
        raise RuntimeError(compiled_unavailable_reason() or REFERENCE_UNAVAILABLE_REASON)
    if not params_batch:
        return CompiledBatchOutput(outputs=np.empty((0, OUTPUT_COLUMN_COUNT), dtype=np.float64))

    packed = _pack_config_arrays(profile, params_batch, trade_start_idx)
    outputs = np.empty((len(params_batch), OUTPUT_COLUMN_COUNT), dtype=np.float64)
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
    return CompiledBatchOutput(outputs=outputs)


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
    for index, params in enumerate(params_batch):
        config = build_kernel_config(profile=profile, params=params, trade_start_idx=trade_start_idx)
        arrays["initial_capital"][index] = config.initial_capital
        arrays["commission_pct"][index] = config.commission_pct
        arrays["stop_x"][index] = config.stop_x
        arrays["reward_risk"][index] = config.reward_risk
        arrays["max_stop_pct"][index] = config.max_stop_pct
        arrays["max_days"][index] = config.max_days
        arrays["risk_per_trade_pct"][index] = config.risk_per_trade_pct
        arrays["contract_size"][index] = config.contract_size
        arrays["trail_activation_rr"][index] = config.trail_activation_rr
        arrays["tick_size"][index] = config.tick_size
        arrays["start_ns"][index] = _timestamp_ns(config.start, np.iinfo(np.int64).min)
        arrays["end_ns"][index] = _timestamp_ns(config.end, np.iinfo(np.int64).max)
        arrays["enable_long"][index] = config.enable_long
        arrays["enable_short"][index] = config.enable_short
        arrays["target_enabled"][index] = config.target_mode == "rr"
        arrays["trail_enabled"][index] = config.trail_mode == "ma"
        arrays["max_days_enabled"][index] = config.max_days_enabled
        arrays["use_date_filter"][index] = config.use_date_filter
        arrays["strict_boundary"][index] = config.boundary_mode == "strict_close"
        arrays["boundary_none"][index] = config.boundary_mode == "none"
        arrays["report_margin"][index] = config.margin_mode == "report_only"
        arrays["rounding_code"][index] = _rounding_code(config.price_rounding_mode)
    return arrays


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
    output = np.empty(len(values), dtype=np.int64)
    for index, value in enumerate(values):
        output[index] = _timestamp_ns(value, 0)
    return output


def _compiled_target(func):
    if numba is None:  # pragma: no cover
        return func
    return numba.njit(cache=False)(func)


@_compiled_target
def _scaled_price(price: float, tick_size: float) -> float:
    scaled = price / tick_size
    nearest = round(scaled)
    tolerance = 1e-9
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
    for index in range(outputs.shape[0]):
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


if numba is not None:
    _COMPILED_BATCH_LOOP = numba.njit(cache=False)(_batch_loop_impl)
else:  # pragma: no cover
    _COMPILED_BATCH_LOOP = _batch_loop_impl


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
    "compiled_batch_available",
    "compiled_unavailable_reason",
    "evaluate_compiled_batch",
]
