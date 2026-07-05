"""Reference Backtester V2 execution kernel."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from core.backtest_engine import TradeRecord

from .contracts import (
    GUARDRAIL_FLAG_INVALID_STOP_DISTANCE,
    GUARDRAIL_FLAG_LIQUIDATION,
    GUARDRAIL_FLAG_MARGIN_REJECT,
    GUARDRAIL_FLAG_NO_CAPITAL_HALT,
    GUARDRAIL_FLAG_REJECTED_FILL,
    GUARDRAIL_FLAG_ZERO_SIZE_ENTRY,
    GuardrailSummary,
    Signals,
    StandingState,
)
from .price_rounding import (
    PRICE_ROUNDING_NONE,
    PRICE_ROUNDING_TICK_OUTWARD,
    round_stop_level,
    round_target_level,
    round_trail_level,
)
from .sizing import risk_position_size


@dataclass(frozen=True)
class ExecutionData:
    """Aligned market, signal, and execution dataprep arrays."""

    timestamps: Sequence[pd.Timestamp]
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    signals: Signals
    atr: np.ndarray
    rolling_low: np.ndarray
    rolling_high: np.ndarray
    trail_long: np.ndarray
    trail_short: np.ndarray

    def __post_init__(self) -> None:
        length = len(self.timestamps)
        for name in (
            "open",
            "high",
            "low",
            "close",
            "atr",
            "rolling_low",
            "rolling_high",
            "trail_long",
            "trail_short",
        ):
            array = np.asarray(getattr(self, name), dtype=float)
            if array.ndim != 1:
                raise ValueError(f"{name} must be a 1D array.")
            if len(array) != length:
                raise ValueError(f"{name} length must match timestamps length.")
            object.__setattr__(self, name, array)

        if len(self.signals.long_entries) != length:
            raise ValueError("signals length must match timestamps length.")


@dataclass(frozen=True)
class KernelConfig:
    """Primitive execution settings consumed by the reference kernel."""

    initial_capital: float = 100.0
    commission_pct: float = 0.0
    stop_x: float = 2.0
    reward_risk: float = 2.0
    max_stop_pct: float = math.inf
    max_days: float = math.inf
    risk_per_trade_pct: float = 2.0
    contract_size: float = 0.01
    enable_long: bool = True
    enable_short: bool = True
    target_mode: str = "rr"
    trail_mode: str = "none"
    trail_activation_mode: str = "none"
    trail_activation_rr: float = 1.0
    max_days_enabled: bool = True
    boundary_mode: str = "strict_close"
    margin_mode: str = "off"
    trade_start_idx: int = 0
    use_date_filter: bool = True
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    price_rounding_mode: str = PRICE_ROUNDING_NONE
    tick_size: float = math.nan


@dataclass(frozen=True)
class KernelResult:
    """Output of one reference-kernel run."""

    trades: list[TradeRecord]
    equity_curve: list[float]
    balance_curve: list[float]
    timestamps: list[pd.Timestamp]
    guardrail_summary: GuardrailSummary
    standing_state: StandingState


@dataclass
class _PendingEntry:
    direction: int
    anchor_price: float
    stop_price: float
    risk: float
    target_price: float
    size: float


@dataclass
class _Position:
    direction: int = 0
    size: float = 0.0
    entry_price: float = math.nan
    entry_time: Optional[pd.Timestamp] = None
    entry_commission: float = 0.0
    anchor_price: float = math.nan
    initial_stop: float = math.nan
    target_price: float = math.nan
    initial_risk: float = math.nan
    trail_stop: float = math.nan
    trail_active: bool = False


@dataclass
class _GuardrailAccumulator:
    rejected_fill_count: int = 0
    invalid_stop_distance_count: int = 0
    zero_size_entry_count: int = 0
    margin_reject_count: int = 0
    liquidation_count: int = 0
    no_capital_halt: bool = False
    max_required_leverage: float = 0.0
    max_notional: float = 0.0
    max_initial_margin_used_pct: float = 0.0
    min_margin_buffer_pct: float = math.inf
    first_guardrail_code: int = 0
    flags: int = 0

    def flag(self, code: int) -> None:
        self.flags |= code
        if self.first_guardrail_code == 0:
            self.first_guardrail_code = code

    def summary(self) -> GuardrailSummary:
        return GuardrailSummary(
            rejected_fill_count=self.rejected_fill_count,
            invalid_stop_distance_count=self.invalid_stop_distance_count,
            zero_size_entry_count=self.zero_size_entry_count,
            margin_reject_count=self.margin_reject_count,
            liquidation_count=self.liquidation_count,
            no_capital_halt=self.no_capital_halt,
            max_required_leverage=self.max_required_leverage,
            max_notional=self.max_notional,
            max_initial_margin_used_pct=self.max_initial_margin_used_pct,
            min_margin_buffer_pct=self.min_margin_buffer_pct,
            first_guardrail_code=self.first_guardrail_code,
            flags=self.flags,
        )


def intrabar_path(open_price: float, high: float, low: float, close: float) -> tuple[float, ...]:
    """Return the TradingView-style default OHLC traversal."""

    if abs(open_price - high) < abs(open_price - low):
        return open_price, high, low, close
    return open_price, low, high, close


def _between(start: float, end: float, level: float) -> bool:
    return min(start, end) <= level <= max(start, end)


def _trade_from_exit(
    *,
    position: _Position,
    exit_time: pd.Timestamp,
    exit_price: float,
    commission_rate: float,
) -> tuple[TradeRecord, float]:
    gross_pnl = (
        (exit_price - position.entry_price) * position.size
        if position.direction > 0
        else (position.entry_price - exit_price) * position.size
    )
    exit_commission = exit_price * position.size * commission_rate
    net_pnl = gross_pnl - position.entry_commission - exit_commission
    entry_value = position.entry_price * position.size
    return (
        TradeRecord(
            direction="long" if position.direction > 0 else "short",
            side="LONG" if position.direction > 0 else "SHORT",
            entry_time=position.entry_time,
            exit_time=exit_time,
            entry_price=position.entry_price,
            exit_price=float(exit_price),
            size=position.size,
            net_pnl=net_pnl,
            profit_pct=(net_pnl / entry_value * 100.0) if entry_value else None,
        ),
        gross_pnl - exit_commission,
    )


def _timestamp_ns(timestamp: Optional[pd.Timestamp]) -> int:
    if timestamp is None:
        return 0
    value = pd.Timestamp(timestamp)
    if value.tzinfo is None:
        value = value.tz_localize("UTC")
    else:
        value = value.tz_convert("UTC")
    return int(value.value)


def _date_allows_entry(timestamp: pd.Timestamp, config: KernelConfig, trade_start_idx: int, index: int) -> bool:
    if index < trade_start_idx:
        return False
    if not config.use_date_filter:
        return True
    if config.start is not None and timestamp < config.start:
        return False
    if config.end is not None and timestamp > config.end:
        return False
    return True


def _active_stop(position: _Position, trail_enabled: bool) -> float:
    if trail_enabled and position.trail_active:
        return position.trail_stop
    return position.initial_stop


def _rounding_enabled(config: KernelConfig) -> bool:
    if config.price_rounding_mode == PRICE_ROUNDING_NONE:
        return False
    if config.price_rounding_mode == PRICE_ROUNDING_TICK_OUTWARD:
        return True
    raise ValueError(f"Unsupported priceRounding mode: {config.price_rounding_mode!r}.")


def _rounded_stop(direction: int, price: float, config: KernelConfig) -> float:
    if not _rounding_enabled(config):
        return price
    return round_stop_level(direction, price, config.tick_size)


def _rounded_target(direction: int, price: float, config: KernelConfig) -> float:
    if not _rounding_enabled(config):
        return price
    return round_target_level(direction, price, config.tick_size)


def _rounded_trail(direction: int, price: float, config: KernelConfig) -> float:
    if not _rounding_enabled(config):
        return price
    return round_trail_level(direction, price, config.tick_size)


def _standing_state(
    *,
    position: _Position,
    pending_market_close: bool,
    pending_entry: Optional[_PendingEntry],
) -> StandingState:
    return StandingState(
        position_direction=position.direction,
        position_size=position.size,
        entry_price=position.entry_price,
        entry_time_ns=_timestamp_ns(position.entry_time),
        anchor_price=position.anchor_price,
        initial_stop=position.initial_stop,
        active_stop=_active_stop(position, True) if position.direction != 0 else math.nan,
        target_price=position.target_price,
        trail_active=position.trail_active,
        trail_stop=position.trail_stop,
        pending_market_close=pending_market_close,
        pending_entry_direction=pending_entry.direction if pending_entry is not None else 0,
        pending_entry_order_type="market_next_open" if pending_entry is not None else "",
        pending_entry_anchor_price=pending_entry.anchor_price if pending_entry is not None else math.nan,
        pending_entry_stop=pending_entry.stop_price if pending_entry is not None else math.nan,
        pending_entry_target=pending_entry.target_price if pending_entry is not None else math.nan,
        pending_entry_size=pending_entry.size if pending_entry is not None else 0.0,
        pending_entry_ttl_bars=1 if pending_entry is not None else 0,
    )


def run_reference_kernel(data: ExecutionData, config: KernelConfig) -> KernelResult:
    """Run the deterministic Phase-1 reference execution loop."""

    length = len(data.timestamps)
    guardrails = _GuardrailAccumulator()
    if length == 0:
        return KernelResult(
            trades=[],
            equity_curve=[],
            balance_curve=[],
            timestamps=[],
            guardrail_summary=guardrails.summary(),
            standing_state=StandingState(),
        )

    target_enabled = config.target_mode == "rr"
    trail_enabled = config.trail_mode == "ma"
    strict_boundary = config.boundary_mode == "strict_close"
    boundary_none = config.boundary_mode == "none"
    report_margin = config.margin_mode == "report_only"
    last_bar_index = length - 1
    trade_start_idx = max(0, min(int(config.trade_start_idx), length))
    commission_rate = config.commission_pct / 100.0

    balance = float(config.initial_capital)
    position = _Position()
    previous_close_position = 0
    pending_entry: Optional[_PendingEntry] = None
    pending_market_close = False

    trades: list[TradeRecord] = []
    equity_curve: list[float] = []
    balance_curve: list[float] = []
    timestamps: list[pd.Timestamp] = []

    def reset_position() -> None:
        nonlocal pending_market_close, position
        position = _Position()
        pending_market_close = False

    def close_position(exit_price: float, timestamp: pd.Timestamp) -> None:
        nonlocal balance
        if position.direction == 0 or position.entry_time is None:
            return
        trade, balance_delta = _trade_from_exit(
            position=position,
            exit_time=timestamp,
            exit_price=float(exit_price),
            commission_rate=commission_rate,
        )
        trades.append(trade)
        balance += balance_delta
        reset_position()

    for i, timestamp in enumerate(data.timestamps):
        timestamp = pd.Timestamp(timestamp)
        open_price = float(data.open[i])
        high = float(data.high[i])
        low = float(data.low[i])
        close = float(data.close[i])
        entry_filled_this_bar = False
        had_position_this_bar = position.direction != 0

        if pending_market_close and position.direction != 0:
            close_position(open_price, timestamp)

        if pending_entry is not None and position.direction == 0:
            order = pending_entry
            pending_entry = None
            position = _Position(
                direction=order.direction,
                size=order.size,
                entry_price=open_price,
                entry_time=timestamp,
                entry_commission=open_price * order.size * commission_rate,
                anchor_price=order.anchor_price,
                initial_stop=order.stop_price,
                target_price=order.target_price,
                initial_risk=order.risk,
                trail_stop=order.stop_price,
                trail_active=False,
            )
            balance -= position.entry_commission
            entry_filled_this_bar = True
            had_position_this_bar = True

            notional = abs(open_price * order.size)
            guardrails.max_notional = max(guardrails.max_notional, notional)
            if report_margin and balance > 0.0:
                guardrails.max_required_leverage = max(
                    guardrails.max_required_leverage,
                    notional / balance,
                )

            if trail_enabled and config.trail_activation_mode == "rr":
                activation = position.anchor_price + position.direction * position.initial_risk * config.trail_activation_rr
                activation_reached = high >= activation if position.direction > 0 else low <= activation
                if activation_reached:
                    position.trail_active = True
                    current_band = data.trail_long[i] if position.direction > 0 else data.trail_short[i]
                    if math.isfinite(float(current_band)):
                        current_band = _rounded_trail(position.direction, float(current_band), config)
                        if position.direction > 0:
                            position.trail_stop = max(position.trail_stop, current_band)
                        else:
                            position.trail_stop = min(position.trail_stop, current_band)

        if position.direction != 0:
            stop_active_for_this_bar = _active_stop(position, trail_enabled)
            gap_exit: Optional[float] = None
            if position.direction > 0:
                if open_price <= stop_active_for_this_bar:
                    gap_exit = open_price
                elif target_enabled and open_price >= position.target_price:
                    gap_exit = open_price
            else:
                if open_price >= stop_active_for_this_bar:
                    gap_exit = open_price
                elif target_enabled and open_price <= position.target_price:
                    gap_exit = open_price
            if gap_exit is not None:
                close_position(gap_exit, timestamp)

        if position.direction != 0:
            stop_active_for_this_bar = _active_stop(position, trail_enabled)
            path = intrabar_path(open_price, high, low, close)
            current = path[0]
            for endpoint in path[1:]:
                if position.direction == 0:
                    break
                rising = endpoint >= current
                exit_price: Optional[float] = None
                if target_enabled:
                    if position.direction > 0:
                        if rising and _between(current, endpoint, position.target_price):
                            exit_price = position.target_price
                        elif not rising and _between(current, endpoint, position.initial_stop):
                            exit_price = position.initial_stop
                    else:
                        if rising and _between(current, endpoint, position.initial_stop):
                            exit_price = position.initial_stop
                        elif not rising and _between(current, endpoint, position.target_price):
                            exit_price = position.target_price
                elif trail_enabled:
                    if position.direction > 0 and not rising and _between(current, endpoint, stop_active_for_this_bar):
                        exit_price = stop_active_for_this_bar
                    elif position.direction < 0 and rising and _between(current, endpoint, stop_active_for_this_bar):
                        exit_price = stop_active_for_this_bar

                if exit_price is not None:
                    close_position(exit_price, timestamp)
                    break
                current = endpoint

        if position.direction != 0 and trail_enabled and not entry_filled_this_bar:
            activation = position.anchor_price + position.direction * position.initial_risk * config.trail_activation_rr
            activation_reached = high >= activation if position.direction > 0 else low <= activation
            if position.trail_active or activation_reached:
                position.trail_active = True
                current_band = data.trail_long[i] if position.direction > 0 else data.trail_short[i]
                if math.isfinite(float(current_band)):
                    current_band = _rounded_trail(position.direction, float(current_band), config)
                    if position.direction > 0:
                        position.trail_stop = max(position.initial_stop, position.trail_stop, current_band)
                    else:
                        position.trail_stop = min(position.initial_stop, position.trail_stop, current_band)

        if position.direction != 0 and position.entry_time is not None and config.max_days_enabled:
            days_in_trade = (timestamp - position.entry_time).total_seconds() / 86400.0
            if days_in_trade >= config.max_days and not (strict_boundary and i == last_bar_index):
                pending_market_close = True

        if i == last_bar_index and strict_boundary:
            pending_entry = None
            pending_market_close = False
            if position.direction != 0 and position.entry_time is not None:
                close_position(close, timestamp)

        in_date_range = _date_allows_entry(timestamp, config, trade_start_idx, i)
        can_plan_entry = (i != last_bar_index) or boundary_none
        if (
            can_plan_entry
            and in_date_range
            and position.direction == 0
            and previous_close_position == 0
            and not had_position_this_bar
            and pending_entry is None
        ):
            direction = 0
            if config.enable_long and bool(data.signals.long_entries[i]):
                direction = 1
            elif config.enable_short and bool(data.signals.short_entries[i]):
                direction = -1
            if direction != 0:
                atr_value = float(data.atr[i])
                anchor = close
                if direction > 0:
                    stop = float(data.rolling_low[i]) - config.stop_x * atr_value
                    risk = anchor - stop
                    target = anchor + config.reward_risk * risk
                else:
                    stop = float(data.rolling_high[i]) + config.stop_x * atr_value
                    risk = stop - anchor
                    target = anchor - config.reward_risk * risk
                stop_pct = 100.0 * risk / anchor if anchor > 0.0 else math.inf
                order_size = risk_position_size(
                    balance=balance,
                    risk_distance=risk,
                    risk_per_trade_pct=config.risk_per_trade_pct,
                    contract_size=config.contract_size,
                )
                if not (math.isfinite(stop) and math.isfinite(risk) and risk > 0.0):
                    guardrails.invalid_stop_distance_count += 1
                    guardrails.flag(GUARDRAIL_FLAG_INVALID_STOP_DISTANCE)
                elif stop_pct > config.max_stop_pct:
                    guardrails.rejected_fill_count += 1
                    guardrails.flag(GUARDRAIL_FLAG_REJECTED_FILL)
                elif not (math.isfinite(order_size) and order_size > 0.0):
                    guardrails.zero_size_entry_count += 1
                    guardrails.flag(GUARDRAIL_FLAG_ZERO_SIZE_ENTRY)
                else:
                    order_stop = _rounded_stop(direction, stop, config)
                    order_target = _rounded_target(direction, target, config) if target_enabled else target
                    pending_entry = _PendingEntry(
                        direction=direction,
                        anchor_price=anchor,
                        stop_price=order_stop,
                        risk=risk,
                        target_price=order_target,
                        size=float(order_size),
                    )

        unrealized = 0.0
        if position.direction > 0:
            unrealized = (close - position.entry_price) * position.size
        elif position.direction < 0:
            unrealized = (position.entry_price - close) * position.size
        equity_curve.append(balance + unrealized)
        balance_curve.append(balance)
        timestamps.append(timestamp)
        previous_close_position = position.direction

    if guardrails.margin_reject_count:
        guardrails.flag(GUARDRAIL_FLAG_MARGIN_REJECT)
    if guardrails.liquidation_count:
        guardrails.flag(GUARDRAIL_FLAG_LIQUIDATION)
    if guardrails.no_capital_halt:
        guardrails.flag(GUARDRAIL_FLAG_NO_CAPITAL_HALT)

    return KernelResult(
        trades=trades,
        equity_curve=equity_curve,
        balance_curve=balance_curve,
        timestamps=timestamps,
        guardrail_summary=guardrails.summary(),
        standing_state=_standing_state(
            position=position,
            pending_market_close=pending_market_close,
            pending_entry=pending_entry,
        ),
    )


__all__ = [
    "ExecutionData",
    "KernelConfig",
    "KernelResult",
    "intrabar_path",
    "run_reference_kernel",
]
