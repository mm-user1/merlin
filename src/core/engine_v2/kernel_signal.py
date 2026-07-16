"""Reference kernel for signal-reversal Backtester V2 profiles."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from core.backtest_engine import TradeRecord

from .contracts import (
    GUARDRAIL_FLAG_ZERO_SIZE_ENTRY,
    GuardrailSummary,
    StandingState,
)
from .kernel import ExecutionData, KernelResult


EMERGENCY_SL_EXIT_REASON = "Emergency SL"


@dataclass(frozen=True)
class SignalKernelConfig:
    initial_capital: float = 100.0
    commission_pct: float = 0.0
    position_pct: float = 100.0
    contract_size: float = 0.01
    enable_long: bool = True
    enable_short: bool = True
    emergency_stop_enabled: bool = False
    emergency_sl_pct: float = 20.0
    emergency_sl_update_bars: int = 16
    boundary_mode: str = "strict_close"
    trade_start_idx: int = 0
    use_date_filter: bool = True
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None


@dataclass
class _PendingEntry:
    direction: int
    size: float


@dataclass
class _Position:
    direction: int = 0
    size: float = 0.0
    entry_price: float = math.nan
    entry_time: Optional[pd.Timestamp] = None
    entry_commission: float = 0.0
    emergency_stop: float = math.nan
    emergency_fill_index: int = -1
    emergency_counter: int = 0


@dataclass
class _GuardrailAccumulator:
    zero_size_entry_count: int = 0
    max_notional: float = 0.0
    first_guardrail_code: int = 0
    flags: int = 0

    def flag(self, code: int) -> None:
        self.flags |= code
        if self.first_guardrail_code == 0:
            self.first_guardrail_code = code

    def summary(self) -> GuardrailSummary:
        return GuardrailSummary(
            zero_size_entry_count=self.zero_size_entry_count,
            max_notional=self.max_notional,
            first_guardrail_code=self.first_guardrail_code,
            flags=self.flags,
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


def _date_allows_entry(timestamp: pd.Timestamp, config: SignalKernelConfig, trade_start_idx: int, index: int) -> bool:
    if index < trade_start_idx:
        return False
    if not config.use_date_filter:
        return True
    if config.start is not None and timestamp < config.start:
        return False
    if config.end is not None and timestamp > config.end:
        return False
    return True


def _validate_config(config: SignalKernelConfig) -> None:
    if config.boundary_mode not in {"strict_close", "none"}:
        raise ValueError(f"Unsupported signal_reversal boundary mode: {config.boundary_mode!r}.")
    if not config.emergency_stop_enabled:
        return
    if config.emergency_sl_pct <= 0:
        raise ValueError("emergencySlPct must be > 0 when useEmergencySL=true.")
    if config.emergency_sl_update_bars < 1:
        raise ValueError("emergencySlUpdateBars must be >= 1 when useEmergencySL=true.")


def _initial_emergency_stop(direction: int, entry_price: float, pct: float) -> float:
    if direction > 0:
        return entry_price * (1.0 - pct / 100.0)
    return entry_price * (1.0 + pct / 100.0)


def _candidate_emergency_stop(direction: int, close_price: float, pct: float) -> float:
    if direction > 0:
        return close_price * (1.0 - pct / 100.0)
    return close_price * (1.0 + pct / 100.0)


def _trade_from_exit(
    *,
    position: _Position,
    exit_time: pd.Timestamp,
    exit_price: float,
    commission_rate: float,
    exit_reason: Optional[str] = None,
) -> tuple[TradeRecord, float]:
    gross_pnl = (
        (exit_price - position.entry_price) * position.size
        if position.direction > 0
        else (position.entry_price - exit_price) * position.size
    )
    exit_commission = exit_price * position.size * commission_rate
    net_pnl = gross_pnl - position.entry_commission - exit_commission
    entry_value = position.entry_price * position.size
    trade = TradeRecord(
        direction="long" if position.direction > 0 else "short",
        side="LONG" if position.direction > 0 else "SHORT",
        entry_time=position.entry_time,
        exit_time=exit_time,
        entry_price=position.entry_price,
        exit_price=float(exit_price),
        size=position.size,
        net_pnl=net_pnl,
        profit_pct=(net_pnl / entry_value * 100.0) if entry_value else None,
        exit_reason=exit_reason,
    )
    return trade, gross_pnl - exit_commission


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
        initial_stop=position.emergency_stop,
        active_stop=position.emergency_stop if position.direction != 0 else math.nan,
        pending_market_close=pending_market_close,
        pending_entry_direction=pending_entry.direction if pending_entry is not None else 0,
        pending_entry_order_type="market_next_open" if pending_entry is not None else "",
        pending_entry_size=pending_entry.size if pending_entry is not None else 0.0,
        pending_entry_ttl_bars=1 if pending_entry is not None else 0,
    )


def run_signal_reversal_kernel(data: ExecutionData, config: SignalKernelConfig) -> KernelResult:
    """Run a next-open signal-reversal reference execution loop."""

    _validate_config(config)
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

    strict_boundary = config.boundary_mode == "strict_close"
    boundary_none = config.boundary_mode == "none"
    trade_start_idx = max(0, min(int(config.trade_start_idx), length))
    last_bar_index = length - 1
    commission_rate = config.commission_pct / 100.0

    balance = float(config.initial_capital)
    position = _Position()
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

    def close_position(exit_price: float, timestamp: pd.Timestamp, exit_reason: Optional[str] = None) -> None:
        nonlocal balance
        if position.direction == 0 or position.entry_time is None:
            return
        trade, balance_delta = _trade_from_exit(
            position=position,
            exit_time=timestamp,
            exit_price=float(exit_price),
            commission_rate=commission_rate,
            exit_reason=exit_reason,
        )
        trades.append(trade)
        balance += balance_delta
        reset_position()

    for i, raw_timestamp in enumerate(data.timestamps):
        timestamp = pd.Timestamp(raw_timestamp)
        open_price = float(data.open[i])
        high = float(data.high[i])
        low = float(data.low[i])
        close = float(data.close[i])

        if pending_market_close:
            if position.direction != 0:
                close_position(open_price, timestamp)
            else:
                pending_market_close = False

        if pending_entry is not None and position.direction == 0:
            order = pending_entry
            pending_entry = None
            position = _Position(
                direction=order.direction,
                size=order.size,
                entry_price=open_price,
                entry_time=timestamp,
                entry_commission=open_price * order.size * commission_rate,
            )
            balance -= position.entry_commission
            if config.emergency_stop_enabled:
                position.emergency_stop = _initial_emergency_stop(
                    position.direction,
                    position.entry_price,
                    config.emergency_sl_pct,
                )
                position.emergency_fill_index = i
                position.emergency_counter = 0
            guardrails.max_notional = max(guardrails.max_notional, abs(open_price * order.size))

        if (
            config.emergency_stop_enabled
            and position.direction != 0
            and position.emergency_fill_index >= 0
            and i >= position.emergency_fill_index + 1
            and math.isfinite(position.emergency_stop)
        ):
            emergency_exit_price = math.nan
            if position.direction > 0 and low <= position.emergency_stop:
                emergency_exit_price = min(open_price, position.emergency_stop)
            elif position.direction < 0 and high >= position.emergency_stop:
                emergency_exit_price = max(open_price, position.emergency_stop)
            if math.isfinite(emergency_exit_price):
                close_position(emergency_exit_price, timestamp, EMERGENCY_SL_EXIT_REASON)

        if (
            config.emergency_stop_enabled
            and position.direction != 0
            and position.emergency_fill_index >= 0
            and i >= position.emergency_fill_index + 1
            and math.isfinite(position.emergency_stop)
        ):
            position.emergency_counter += 1
            if position.emergency_counter >= config.emergency_sl_update_bars:
                candidate = _candidate_emergency_stop(position.direction, close, config.emergency_sl_pct)
                if position.direction > 0:
                    if candidate > position.emergency_stop:
                        position.emergency_stop = candidate
                elif candidate < position.emergency_stop:
                    position.emergency_stop = candidate
                position.emergency_counter = 0

        in_range = _date_allows_entry(timestamp, config, trade_start_idx, i)
        can_plan_order = (i != last_bar_index) or boundary_none
        if can_plan_order:
            long_entry = bool(data.signals.long_entries[i])
            short_entry = bool(data.signals.short_entries[i])
            long_exit = data.signals.long_exits is not None and bool(data.signals.long_exits[i])
            short_exit = data.signals.short_exits is not None and bool(data.signals.short_exits[i])

            if position.direction > 0:
                if short_entry or long_exit or not in_range:
                    pending_market_close = True
            elif position.direction < 0:
                if long_entry or short_exit or not in_range:
                    pending_market_close = True

            if position.direction == 0 and pending_entry is None and in_range:
                direction = 0
                if config.enable_long and long_entry:
                    direction = 1
                elif config.enable_short and short_entry:
                    direction = -1
                if direction != 0:
                    raw_size = math.nan
                    if close > 0.0 and config.contract_size > 0.0:
                        raw_size = (balance * config.position_pct / 100.0 / close) / config.contract_size
                    order_size = math.floor(raw_size) * config.contract_size if math.isfinite(raw_size) else math.nan
                    if not (math.isfinite(order_size) and order_size > 0.0):
                        guardrails.zero_size_entry_count += 1
                        guardrails.flag(GUARDRAIL_FLAG_ZERO_SIZE_ENTRY)
                    else:
                        pending_entry = _PendingEntry(direction=direction, size=float(order_size))

        if i == last_bar_index and strict_boundary:
            pending_entry = None
            pending_market_close = False
            if position.direction != 0 and position.entry_time is not None:
                close_position(close, timestamp)

        unrealized = 0.0
        if position.direction > 0:
            unrealized = (close - position.entry_price) * position.size
        elif position.direction < 0:
            unrealized = (position.entry_price - close) * position.size
        equity_curve.append(balance + unrealized)
        balance_curve.append(balance)
        timestamps.append(timestamp)

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
    "EMERGENCY_SL_EXIT_REASON",
    "SignalKernelConfig",
    "run_signal_reversal_kernel",
]
