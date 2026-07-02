"""S06 R-Trend v02 slow strategy."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core import metrics
from core.backtest_engine import StrategyResult, TradeRecord, build_forced_close_trade
from strategies.base import BaseStrategy


ENTRY_MODES = {"Reversal @ Triangle", "Trend @ Square"}
TRAIL_MA_TYPES = {"SMA", "HMA", "KAMA", "T3"}


@dataclass
class S06Params:
    use_date_filter: bool = True
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    entryMode: str = "Reversal @ Triangle"
    enableLong: bool = True
    enableShort: bool = True
    fastLength: int = 21
    fastSmoothing: int = 7
    slowLength: int = 112
    slowSmoothing: int = 3
    thresholdOS: int = 20
    thresholdOB: int = 20
    stopX: float = 2.0
    stopRR: float = 3.0
    stopLP: int = 2
    stopMaxPct: float = 4.0
    stopMaxDays: int = 4
    riskPerTrade: float = 2.0
    contractSize: float = 0.01
    useTrailMA: bool = True
    trailRR: float = 1.0
    trailMAType: str = "SMA"
    trailMALength: int = 150
    trailMAOffsetEx: float = 0.0
    initialCapital: float = 100.0
    commissionPct: float = 0.05

    @staticmethod
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

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[pd.Timestamp]:
        if value in (None, ""):
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "S06Params":
        d = payload or {}
        params = cls(
            use_date_filter=cls._coerce_bool(d.get("dateFilter"), cls.use_date_filter),
            start=cls._parse_timestamp(d.get("start")),
            end=cls._parse_timestamp(d.get("end")),
            entryMode=str(d.get("entryMode", cls.entryMode)),
            enableLong=cls._coerce_bool(d.get("enableLong"), cls.enableLong),
            enableShort=cls._coerce_bool(d.get("enableShort"), cls.enableShort),
            fastLength=int(d.get("fastLength", cls.fastLength)),
            fastSmoothing=int(d.get("fastSmoothing", cls.fastSmoothing)),
            slowLength=int(d.get("slowLength", cls.slowLength)),
            slowSmoothing=int(d.get("slowSmoothing", cls.slowSmoothing)),
            thresholdOS=int(d.get("thresholdOS", cls.thresholdOS)),
            thresholdOB=int(d.get("thresholdOB", cls.thresholdOB)),
            stopX=float(d.get("stopX", cls.stopX)),
            stopRR=float(d.get("stopRR", cls.stopRR)),
            stopLP=int(d.get("stopLP", cls.stopLP)),
            stopMaxPct=float(d.get("stopMaxPct", cls.stopMaxPct)),
            stopMaxDays=int(d.get("stopMaxDays", cls.stopMaxDays)),
            riskPerTrade=float(d.get("riskPerTrade", cls.riskPerTrade)),
            contractSize=float(d.get("contractSize", cls.contractSize)),
            useTrailMA=cls._coerce_bool(d.get("useTrailMA"), cls.useTrailMA),
            trailRR=float(d.get("trailRR", cls.trailRR)),
            trailMAType=str(d.get("trailMAType", cls.trailMAType)).upper(),
            trailMALength=int(d.get("trailMALength", cls.trailMALength)),
            trailMAOffsetEx=float(d.get("trailMAOffsetEx", cls.trailMAOffsetEx)),
            initialCapital=float(d.get("initialCapital", cls.initialCapital)),
            commissionPct=float(d.get("commissionPct", cls.commissionPct)),
        )
        params._validate()
        return params

    def _validate(self) -> None:
        if self.entryMode not in ENTRY_MODES:
            raise ValueError(
                f"Invalid entryMode '{self.entryMode}'. Expected one of {sorted(ENTRY_MODES)}."
            )
        if self.trailMAType not in TRAIL_MA_TYPES:
            raise ValueError(
                f"Invalid trailMAType '{self.trailMAType}'. "
                f"Expected one of {sorted(TRAIL_MA_TYPES)}."
            )
        for name in ("fastLength", "fastSmoothing", "slowLength", "slowSmoothing", "stopLP"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be greater than zero.")
        if self.trailMALength <= 0:
            raise ValueError("trailMALength must be greater than zero.")
        if not 1 <= self.thresholdOS <= 50 or not 1 <= self.thresholdOB <= 50:
            raise ValueError("thresholdOS and thresholdOB must be between 1 and 50.")
        for name in ("stopX", "stopRR", "stopMaxPct", "stopMaxDays", "riskPerTrade", "contractSize", "trailRR"):
            if not math.isfinite(float(getattr(self, name))) or float(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be a finite value greater than zero.")
        if self.trailMAOffsetEx < 0 or not math.isfinite(self.trailMAOffsetEx):
            raise ValueError("trailMAOffsetEx must be finite and non-negative.")
        if self.initialCapital <= 0 or not math.isfinite(self.initialCapital):
            raise ValueError("initialCapital must be a finite value greater than zero.")
        if self.commissionPct < 0 or not math.isfinite(self.commissionPct):
            raise ValueError("commissionPct must be finite and non-negative.")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("start must not be after end.")


@dataclass(frozen=True)
class _SignalEvents:
    overbought: np.ndarray
    oversold: np.ndarray
    ob_trend_start: np.ndarray
    os_trend_start: np.ndarray
    ob_reversal: np.ndarray
    os_reversal: np.ndarray
    long_signal: np.ndarray
    short_signal: np.ndarray


@dataclass(frozen=True)
class _StrategyArrays:
    atr: np.ndarray
    lowest: np.ndarray
    highest: np.ndarray
    trail_long: np.ndarray
    trail_short: np.ndarray
    long_signal: np.ndarray
    short_signal: np.ndarray


@dataclass(frozen=True)
class _PendingEntry:
    direction: int
    anchor_price: float
    stop_price: float
    risk: float
    target_price: float
    size: float


def _pine_ema(values: np.ndarray, length: int) -> np.ndarray:
    result = np.full(len(values), np.nan, dtype=float)
    alpha = 2.0 / (length + 1.0)
    previous = math.nan
    for i, raw in enumerate(values):
        value = float(raw)
        if not math.isfinite(value):
            if math.isfinite(previous):
                result[i] = previous
            continue
        previous = value if not math.isfinite(previous) else alpha * value + (1.0 - alpha) * previous
        result[i] = previous
    return result


def _pine_rma(values: np.ndarray, length: int) -> np.ndarray:
    result = np.full(len(values), np.nan, dtype=float)
    seed: List[float] = []
    previous = math.nan
    for i, raw in enumerate(values):
        value = float(raw)
        if not math.isfinite(value):
            continue
        if not math.isfinite(previous):
            seed.append(value)
            if len(seed) == length:
                previous = float(sum(seed) / length)
                result[i] = previous
            continue
        previous = (previous * (length - 1) + value) / length
        result[i] = previous
    return result


def _williams_r(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    length: int,
) -> np.ndarray:
    highest = high.rolling(length, min_periods=length).max().to_numpy(copy=False)
    lowest = low.rolling(length, min_periods=length).min().to_numpy(copy=False)
    close_values = close.to_numpy(copy=False)
    denominator = highest - lowest
    result = np.full(len(close), np.nan, dtype=float)
    valid = np.isfinite(denominator) & (denominator != 0.0) & np.isfinite(close_values)
    result[valid] = 100.0 * (close_values[valid] - highest[valid]) / denominator[valid]
    return result


def _pine_wma(series: pd.Series, length: int) -> pd.Series:
    weights = np.arange(1, length + 1, dtype=float)
    return series.rolling(length, min_periods=length).apply(
        lambda values: float(np.dot(values, weights) / weights.sum()),
        raw=True,
    )


def _pine_kama(series: pd.Series, length: int) -> pd.Series:
    values = series.to_numpy(copy=False)
    momentum = series.diff(length).abs().to_numpy(copy=False)
    volatility = series.diff().abs().rolling(length, min_periods=length).sum().to_numpy(copy=False)
    result = np.full(len(series), np.nan, dtype=float)
    fast_alpha = 2.0 / 3.0
    slow_alpha = 2.0 / 31.0
    previous = math.nan
    for i, price_raw in enumerate(values):
        price = float(price_raw)
        if not math.isfinite(price) or not math.isfinite(momentum[i]) or not math.isfinite(volatility[i]):
            continue
        efficiency_ratio = momentum[i] / volatility[i] if volatility[i] != 0.0 else 0.0
        alpha = (efficiency_ratio * (fast_alpha - slow_alpha) + slow_alpha) ** 2
        prior = previous if math.isfinite(previous) else price
        previous = alpha * price + (1.0 - alpha) * prior
        result[i] = previous
    return pd.Series(result, index=series.index)


def _trail_ma(series: pd.Series, ma_type: str, length: int) -> pd.Series:
    if ma_type == "SMA":
        return series.rolling(length, min_periods=length).mean()
    if ma_type == "HMA":
        half_length = max(1, length // 2)
        sqrt_length = max(1, int(round(math.sqrt(length))))
        return _pine_wma(2.0 * _pine_wma(series, half_length) - _pine_wma(series, length), sqrt_length)
    if ma_type == "KAMA":
        return _pine_kama(series, length)
    if ma_type == "T3":
        values = series.to_numpy(copy=False)

        def gd(source: np.ndarray) -> np.ndarray:
            first = _pine_ema(source, length)
            second = _pine_ema(first, length)
            return first * 1.7 - second * 0.7

        return pd.Series(gd(gd(gd(values))), index=series.index)
    raise ValueError(f"Unsupported trail MA type: {ma_type}")


def _signal_events(
    fast_percent_r: np.ndarray,
    slow_percent_r: np.ndarray,
    threshold_os: int,
    threshold_ob: int,
    entry_mode: str,
) -> _SignalEvents:
    valid = np.isfinite(fast_percent_r) & np.isfinite(slow_percent_r)
    overbought = valid & (fast_percent_r >= -threshold_ob) & (slow_percent_r >= -threshold_ob)
    oversold = valid & (fast_percent_r <= -100 + threshold_os) & (
        slow_percent_r <= -100 + threshold_os
    )
    previous_valid = np.zeros(len(valid), dtype=bool)
    previous_valid[1:] = valid[:-1]
    previous_ob = np.zeros(len(valid), dtype=bool)
    previous_os = np.zeros(len(valid), dtype=bool)
    previous_ob[1:] = overbought[:-1]
    previous_os[1:] = oversold[:-1]

    ob_trend_start = previous_valid & overbought & ~previous_ob
    os_trend_start = previous_valid & oversold & ~previous_os
    ob_reversal = previous_valid & ~overbought & previous_ob
    os_reversal = previous_valid & ~oversold & previous_os
    if entry_mode == "Reversal @ Triangle":
        long_signal = os_reversal
        short_signal = ob_reversal
    elif entry_mode == "Trend @ Square":
        long_signal = ob_trend_start
        short_signal = os_trend_start
    else:
        raise ValueError(f"Unsupported entry mode: {entry_mode}")
    return _SignalEvents(
        overbought=overbought,
        oversold=oversold,
        ob_trend_start=ob_trend_start,
        os_trend_start=os_trend_start,
        ob_reversal=ob_reversal,
        os_reversal=os_reversal,
        long_signal=long_signal,
        short_signal=short_signal,
    )


def _build_strategy_arrays(df: pd.DataFrame, params: S06Params) -> _StrategyArrays:
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    high_values = high.to_numpy(copy=False)
    low_values = low.to_numpy(copy=False)
    close_values = close.to_numpy(copy=False)

    fast_percent_r = _pine_ema(
        _williams_r(high, low, close, params.fastLength),
        params.fastSmoothing,
    )
    slow_percent_r = _pine_ema(
        _williams_r(high, low, close, params.slowLength),
        params.slowSmoothing,
    )
    signals = _signal_events(
        fast_percent_r,
        slow_percent_r,
        params.thresholdOS,
        params.thresholdOB,
        params.entryMode,
    )

    previous_close = np.roll(close_values, 1)
    previous_close[0] = np.nan
    true_range = np.maximum.reduce(
        [
            np.abs(high_values - low_values),
            np.abs(high_values - previous_close),
            np.abs(low_values - previous_close),
        ]
    )
    true_range[0] = abs(high_values[0] - low_values[0])
    atr_values = _pine_rma(true_range, 14)
    lowest = low.rolling(params.stopLP, min_periods=params.stopLP).min().to_numpy(copy=False)
    highest = high.rolling(params.stopLP, min_periods=params.stopLP).max().to_numpy(copy=False)

    trail_ma = _trail_ma(close, params.trailMAType, params.trailMALength).to_numpy(copy=False)
    offset_pct = 1.0 + params.trailMAOffsetEx
    trail_long = trail_ma * (1.0 - offset_pct / 100.0)
    trail_short = trail_ma * (1.0 + offset_pct / 100.0)
    return _StrategyArrays(
        atr=atr_values,
        lowest=lowest,
        highest=highest,
        trail_long=trail_long,
        trail_short=trail_short,
        long_signal=signals.long_signal,
        short_signal=signals.short_signal,
    )


def _intrabar_path(open_price: float, high: float, low: float, close: float) -> tuple[float, ...]:
    # TradingView uses high-first only when Open is strictly closer to High.
    if abs(open_price - high) < abs(open_price - low):
        return open_price, high, low, close
    return open_price, low, high, close


def _between(start: float, end: float, level: float) -> bool:
    return min(start, end) <= level <= max(start, end)


def _trade_from_exit(
    *,
    position: int,
    entry_time: pd.Timestamp,
    exit_time: pd.Timestamp,
    entry_price: float,
    exit_price: float,
    size: float,
    entry_commission: float,
    commission_rate: float,
) -> tuple[TradeRecord, float]:
    gross_pnl = (
        (exit_price - entry_price) * size
        if position > 0
        else (entry_price - exit_price) * size
    )
    exit_commission = exit_price * size * commission_rate
    net_pnl = gross_pnl - entry_commission - exit_commission
    entry_value = entry_price * size
    return (
        TradeRecord(
            direction="long" if position > 0 else "short",
            side="LONG" if position > 0 else "SHORT",
            entry_time=entry_time,
            exit_time=exit_time,
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
            net_pnl=net_pnl,
            profit_pct=(net_pnl / entry_value * 100.0) if entry_value else None,
        ),
        gross_pnl - exit_commission,
    )


class S06RTrendV02(BaseStrategy):
    STRATEGY_ID = "s06_r_trend_v02"
    STRATEGY_NAME = "S06 R-Trend"
    STRATEGY_VERSION = "v02"

    @staticmethod
    def run(
        df: pd.DataFrame,
        params: Dict[str, Any],
        trade_start_idx: int = 0,
    ) -> StrategyResult:
        p = S06Params.from_dict(params)
        if df.empty:
            result = StrategyResult(trades=[], equity_curve=[], balance_curve=[], timestamps=[])
            metrics.enrich_strategy_result(result, initial_balance=p.initialCapital)
            return result

        if p.use_date_filter and p.end is not None:
            eligible = np.flatnonzero(df.index <= p.end)
            if eligible.size == 0:
                result = StrategyResult(trades=[], equity_curve=[], balance_curve=[], timestamps=[])
                metrics.enrich_strategy_result(result, initial_balance=p.initialCapital)
                return result
            df = df.iloc[: int(eligible[-1]) + 1]

        arrays = _build_strategy_arrays(df, p)
        timestamps_index = list(df.index)
        open_values = df["Open"].to_numpy(copy=False)
        high_values = df["High"].to_numpy(copy=False)
        low_values = df["Low"].to_numpy(copy=False)
        close_values = df["Close"].to_numpy(copy=False)
        last_bar_index = len(df) - 1
        trade_start_idx = max(0, min(int(trade_start_idx), len(df)))
        commission_rate = p.commissionPct / 100.0

        balance = p.initialCapital
        position = 0
        previous_close_position = 0
        size = 0.0
        entry_price = math.nan
        entry_time: Optional[pd.Timestamp] = None
        entry_commission = 0.0
        anchor_price = math.nan
        initial_stop = math.nan
        target_price = math.nan
        initial_risk = math.nan
        trail_stop = math.nan
        trail_active = False
        pending_entry: Optional[_PendingEntry] = None
        pending_market_close = False

        trades: List[TradeRecord] = []
        equity_curve: List[float] = []
        balance_curve: List[float] = []
        timestamps: List[pd.Timestamp] = []

        def reset_position() -> None:
            nonlocal position, size, entry_price, entry_time, entry_commission
            nonlocal anchor_price, initial_stop, target_price, initial_risk
            nonlocal trail_stop, trail_active, pending_market_close
            position = 0
            size = 0.0
            entry_price = math.nan
            entry_time = None
            entry_commission = 0.0
            anchor_price = math.nan
            initial_stop = math.nan
            target_price = math.nan
            initial_risk = math.nan
            trail_stop = math.nan
            trail_active = False
            pending_market_close = False

        def close_position(exit_price: float, timestamp: pd.Timestamp) -> None:
            nonlocal balance
            if position == 0 or entry_time is None:
                return
            trade, balance_delta = _trade_from_exit(
                position=position,
                entry_time=entry_time,
                exit_time=timestamp,
                entry_price=entry_price,
                exit_price=float(exit_price),
                size=size,
                entry_commission=entry_commission,
                commission_rate=commission_rate,
            )
            trades.append(trade)
            balance += balance_delta
            reset_position()

        for i, timestamp in enumerate(timestamps_index):
            open_price = float(open_values[i])
            high = float(high_values[i])
            low = float(low_values[i])
            close = float(close_values[i])
            entry_filled_this_bar = False
            had_position_this_bar = position != 0

            if pending_market_close and position != 0:
                close_position(open_price, timestamp)

            if pending_entry is not None and position == 0:
                order = pending_entry
                pending_entry = None
                position = order.direction
                size = order.size
                entry_price = open_price
                entry_time = timestamp
                entry_commission = entry_price * size * commission_rate
                balance -= entry_commission
                anchor_price = order.anchor_price
                initial_stop = order.stop_price
                target_price = order.target_price
                initial_risk = order.risk
                trail_stop = initial_stop
                trail_active = False
                entry_filled_this_bar = True
                had_position_this_bar = True
                if p.useTrailMA:
                    # Pine exposes confirmed historical-bar OHLC during the
                    # calc_on_order_fills entry-fill recalculation.
                    activation = anchor_price + position * initial_risk * p.trailRR
                    activation_reached = high >= activation if position > 0 else low <= activation
                    if activation_reached:
                        trail_active = True
                        current_band = (
                            arrays.trail_long[i] if position > 0 else arrays.trail_short[i]
                        )
                        if math.isfinite(float(current_band)):
                            if position > 0:
                                trail_stop = max(trail_stop, float(current_band))
                            else:
                                trail_stop = min(trail_stop, float(current_band))

            if position != 0:
                stop_active_for_this_bar = (
                    trail_stop if p.useTrailMA and trail_active else initial_stop
                )
                gap_exit: Optional[float] = None
                if position > 0:
                    if open_price <= stop_active_for_this_bar:
                        gap_exit = open_price
                    elif not p.useTrailMA and open_price >= target_price:
                        gap_exit = open_price
                else:
                    if open_price >= stop_active_for_this_bar:
                        gap_exit = open_price
                    elif not p.useTrailMA and open_price <= target_price:
                        gap_exit = open_price
                if gap_exit is not None:
                    close_position(gap_exit, timestamp)

            if position != 0:
                path = _intrabar_path(open_price, high, low, close)
                current = path[0]
                for endpoint in path[1:]:
                    if position == 0:
                        break
                    rising = endpoint >= current
                    exit_price: Optional[float] = None
                    if not p.useTrailMA:
                        if position > 0:
                            if rising and _between(current, endpoint, target_price):
                                exit_price = target_price
                            elif not rising and _between(current, endpoint, initial_stop):
                                exit_price = initial_stop
                        else:
                            if rising and _between(current, endpoint, initial_stop):
                                exit_price = initial_stop
                            elif not rising and _between(current, endpoint, target_price):
                                exit_price = target_price
                    elif position > 0 and not rising and _between(
                        current, endpoint, stop_active_for_this_bar
                    ):
                        exit_price = stop_active_for_this_bar
                    elif position < 0 and rising and _between(
                        current, endpoint, stop_active_for_this_bar
                    ):
                        exit_price = stop_active_for_this_bar

                    if exit_price is not None:
                        close_position(exit_price, timestamp)
                        break
                    current = endpoint

            if position != 0 and p.useTrailMA and not entry_filled_this_bar:
                activation = anchor_price + position * initial_risk * p.trailRR
                activation_reached = high >= activation if position > 0 else low <= activation
                if trail_active or activation_reached:
                    trail_active = True
                    current_band = (
                        arrays.trail_long[i] if position > 0 else arrays.trail_short[i]
                    )
                    if math.isfinite(float(current_band)):
                        if position > 0:
                            trail_stop = max(initial_stop, trail_stop, float(current_band))
                        else:
                            trail_stop = min(initial_stop, trail_stop, float(current_band))
                # Normal historical-bar activation/ratchet is committed after
                # path processing and becomes executable on the next tick.

            if position != 0 and entry_time is not None:
                days_in_trade = (timestamp - entry_time).total_seconds() / 86400.0
                if days_in_trade >= p.stopMaxDays and i != last_bar_index:
                    pending_market_close = True

            if i == last_bar_index:
                pending_entry = None
                pending_market_close = False
                if position != 0 and entry_time is not None:
                    trade, gross_pnl, exit_commission, _ = build_forced_close_trade(
                        position=position,
                        entry_time=entry_time,
                        exit_time=timestamp,
                        entry_price=entry_price,
                        exit_price=close,
                        size=size,
                        entry_commission=entry_commission,
                        commission_rate=p.commissionPct,
                        commission_is_pct=True,
                    )
                    if trade is not None:
                        trades.append(trade)
                        balance += gross_pnl - exit_commission
                    reset_position()

            in_date_range = (
                i >= trade_start_idx
                and (not p.use_date_filter or p.start is None or timestamp >= p.start)
                and (not p.use_date_filter or p.end is None or timestamp <= p.end)
            )
            if (
                i != last_bar_index
                and in_date_range
                and position == 0
                and previous_close_position == 0
                and not had_position_this_bar
                and pending_entry is None
            ):
                direction = 0
                if p.enableLong and bool(arrays.long_signal[i]):
                    direction = 1
                elif p.enableShort and bool(arrays.short_signal[i]):
                    direction = -1
                if direction != 0:
                    atr_value = float(arrays.atr[i])
                    anchor = close
                    if direction > 0:
                        stop = float(arrays.lowest[i]) - p.stopX * atr_value
                        risk = anchor - stop
                        target = anchor + p.stopRR * risk
                    else:
                        stop = float(arrays.highest[i]) + p.stopX * atr_value
                        risk = stop - anchor
                        target = anchor - p.stopRR * risk
                    stop_pct = 100.0 * risk / anchor if anchor > 0 else math.inf
                    risk_cash = balance * p.riskPerTrade / 100.0
                    raw_size = risk_cash / risk if risk > 0 else 0.0
                    order_size = math.floor(raw_size / p.contractSize) * p.contractSize
                    if (
                        math.isfinite(stop)
                        and math.isfinite(risk)
                        and risk > 0
                        and stop_pct <= p.stopMaxPct
                        and math.isfinite(order_size)
                        and order_size > 0
                    ):
                        pending_entry = _PendingEntry(
                            direction=direction,
                            anchor_price=anchor,
                            stop_price=stop,
                            risk=risk,
                            target_price=target,
                            size=float(order_size),
                        )

            unrealized = 0.0
            if position > 0:
                unrealized = (close - entry_price) * size
            elif position < 0:
                unrealized = (entry_price - close) * size
            equity_curve.append(balance + unrealized)
            balance_curve.append(balance)
            timestamps.append(timestamp)
            previous_close_position = position

        result = StrategyResult(
            trades=trades,
            equity_curve=equity_curve,
            balance_curve=balance_curve,
            timestamps=timestamps,
        )
        metrics.enrich_strategy_result(result, initial_balance=p.initialCapital, risk_free_rate=0.02)
        return result
