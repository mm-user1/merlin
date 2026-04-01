"""
S01 Trailing MA Strategy - v26
Self-contained implementation that mirrors the legacy engine logic while
owning its parameters and execution flow.
"""

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core import metrics
from core.backtest_engine import StrategyResult, TradeRecord, build_forced_close_trade
from indicators.ma import get_ma
from indicators.volatility import atr
from strategies.base import BaseStrategy


@dataclass
class S01Params:
    use_date_filter: bool = True
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    maType: str = "EMA"
    maLength: int = 45
    closeCountLong: int = 7
    closeCountShort: int = 5
    stopLongX: float = 2.0
    stopLongRR: float = 3.0
    stopLongLP: int = 2
    stopShortX: float = 2.0
    stopShortRR: float = 3.0
    stopShortLP: int = 2
    stopLongMaxPct: float = 3.0
    stopShortMaxPct: float = 3.0
    stopLongMaxDays: int = 2
    stopShortMaxDays: int = 4
    trailRRLong: float = 1.0
    trailRRShort: float = 1.0
    trailMaType: str = "SMA"
    trailLongLength: int = 160
    trailLongOffset: float = -1.0
    trailShortLength: int = 160
    trailShortOffset: float = 1.0
    riskPerTrade: float = 2.0
    contractSize: float = 0.01
    commissionRate: float = 0.0005
    atrPeriod: int = 14

    # Note: The to_dict() method was removed in Phase 9-5-1.
    # Use dataclasses.asdict(params) to convert instances to dictionaries.

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "S01Params":
        """Parse S01 parameters - direct mapping, no conversion."""
        d = payload or {}

        # Date handling (convert string to Timestamp if needed)
        start = d.get("start")
        end = d.get("end")
        if isinstance(start, str):
            start = pd.Timestamp(start, tz="UTC")
        if isinstance(end, str):
            end = pd.Timestamp(end, tz="UTC")

        return cls(
            use_date_filter=bool(d.get("dateFilter", True)),
            start=start,
            end=end,
            maType=str(d.get("maType", cls.maType)),
            maLength=int(d.get("maLength", cls.maLength)),
            closeCountLong=int(d.get("closeCountLong", cls.closeCountLong)),
            closeCountShort=int(d.get("closeCountShort", cls.closeCountShort)),
            stopLongX=float(d.get("stopLongX", cls.stopLongX)),
            stopLongRR=float(d.get("stopLongRR", cls.stopLongRR)),
            stopLongLP=int(d.get("stopLongLP", cls.stopLongLP)),
            stopShortX=float(d.get("stopShortX", cls.stopShortX)),
            stopShortRR=float(d.get("stopShortRR", cls.stopShortRR)),
            stopShortLP=int(d.get("stopShortLP", cls.stopShortLP)),
            stopLongMaxPct=float(d.get("stopLongMaxPct", cls.stopLongMaxPct)),
            stopShortMaxPct=float(d.get("stopShortMaxPct", cls.stopShortMaxPct)),
            stopLongMaxDays=int(d.get("stopLongMaxDays", cls.stopLongMaxDays)),
            stopShortMaxDays=int(d.get("stopShortMaxDays", cls.stopShortMaxDays)),
            trailRRLong=float(d.get("trailRRLong", cls.trailRRLong)),
            trailRRShort=float(d.get("trailRRShort", cls.trailRRShort)),
            trailMaType=str(d.get("trailMaType", cls.trailMaType)),
            trailLongLength=int(d.get("trailLongLength", cls.trailLongLength)),
            trailLongOffset=float(d.get("trailLongOffset", cls.trailLongOffset)),
            trailShortLength=int(d.get("trailShortLength", cls.trailShortLength)),
            trailShortOffset=float(d.get("trailShortOffset", cls.trailShortOffset)),
            riskPerTrade=float(d.get("riskPerTrade", cls.riskPerTrade)),
            contractSize=float(d.get("contractSize", cls.contractSize)),
            commissionRate=float(d.get("commissionRate", cls.commissionRate)),
            atrPeriod=int(d.get("atrPeriod", cls.atrPeriod)),
        )


class S01TrailingMA(BaseStrategy):
    STRATEGY_ID = "s01_trailing_ma"
    STRATEGY_NAME = "S01 Trailing MA"
    STRATEGY_VERSION = "v26"

    @staticmethod
    def run(df: pd.DataFrame, params: Dict[str, Any], trade_start_idx: int = 0) -> StrategyResult:
        p = S01Params.from_dict(params)

        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]

        ma_series = get_ma(close, p.maType, p.maLength, volume, high, low)
        atr_series = atr(high, low, close, p.atrPeriod)
        lowest_long = low.rolling(p.stopLongLP, min_periods=1).min()
        highest_short = high.rolling(p.stopShortLP, min_periods=1).max()

        trail_ma_long = get_ma(close, p.trailMaType, p.trailLongLength, volume, high, low)
        trail_ma_short = get_ma(close, p.trailMaType, p.trailShortLength, volume, high, low)
        if p.trailLongLength > 0:
            trail_ma_long = trail_ma_long * (1 + p.trailLongOffset / 100.0)
        if p.trailShortLength > 0:
            trail_ma_short = trail_ma_short * (1 + p.trailShortOffset / 100.0)

        times = list(df.index)
        close_values = close.to_numpy(copy=False)
        high_values = high.to_numpy(copy=False)
        low_values = low.to_numpy(copy=False)
        ma_values = ma_series.to_numpy(copy=False)
        atr_values = atr_series.to_numpy(copy=False)
        lowest_long_values = lowest_long.to_numpy(copy=False)
        highest_short_values = highest_short.to_numpy(copy=False)
        trail_ma_long_values = trail_ma_long.to_numpy(copy=False)
        trail_ma_short_values = trail_ma_short.to_numpy(copy=False)
        if p.use_date_filter:
            time_in_range = np.zeros(len(times), dtype=bool)
            time_in_range[trade_start_idx:] = True
        else:
            time_in_range = np.ones(len(times), dtype=bool)

        equity = 100.0
        realized_equity = equity
        position = 0
        prev_position = 0
        position_size = 0.0
        entry_price = math.nan
        stop_price = math.nan
        target_price = math.nan
        trail_price_long = math.nan
        trail_price_short = math.nan
        trail_activated_long = False
        trail_activated_short = False
        entry_time_long: Optional[pd.Timestamp] = None
        entry_time_short: Optional[pd.Timestamp] = None
        entry_commission = 0.0

        counter_close_trend_long = 0
        counter_close_trend_short = 0
        counter_trade_long = 0
        counter_trade_short = 0

        trades: List[TradeRecord] = []
        realized_curve: List[float] = []
        mtm_curve: List[float] = []

        last_bar_index = len(times) - 1

        for i in range(len(times)):
            time = times[i]
            c = close_values[i]
            h = high_values[i]
            l = low_values[i]
            ma_value = ma_values[i]
            atr_value = atr_values[i]
            lowest_value = lowest_long_values[i]
            highest_value = highest_short_values[i]
            trail_long_value = trail_ma_long_values[i]
            trail_short_value = trail_ma_short_values[i]

            if not np.isnan(ma_value):
                if c > ma_value:
                    counter_close_trend_long += 1
                    counter_close_trend_short = 0
                elif c < ma_value:
                    counter_close_trend_short += 1
                    counter_close_trend_long = 0
                else:
                    counter_close_trend_long = 0
                    counter_close_trend_short = 0

            if position > 0:
                counter_trade_long = 1
                counter_trade_short = 0
            elif position < 0:
                counter_trade_long = 0
                counter_trade_short = 1

            exit_price: Optional[float] = None
            if position > 0:
                if not trail_activated_long and not math.isnan(entry_price) and not math.isnan(stop_price):
                    activation_price = entry_price + (entry_price - stop_price) * p.trailRRLong
                    if h >= activation_price:
                        trail_activated_long = True
                        if math.isnan(trail_price_long):
                            trail_price_long = stop_price
                if not math.isnan(trail_price_long) and not np.isnan(trail_long_value):
                    if np.isnan(trail_price_long) or trail_long_value > trail_price_long:
                        trail_price_long = trail_long_value
                if trail_activated_long:
                    if not math.isnan(trail_price_long) and l <= trail_price_long:
                        exit_price = h if trail_price_long > h else trail_price_long
                else:
                    if l <= stop_price:
                        exit_price = stop_price
                    elif h >= target_price:
                        exit_price = target_price
                if exit_price is None and entry_time_long is not None and p.stopLongMaxDays > 0:
                    days_in_trade = int(math.floor((time - entry_time_long).total_seconds() / 86400))
                    if days_in_trade >= p.stopLongMaxDays:
                        exit_price = c
                if exit_price is not None:
                    gross_pnl = (exit_price - entry_price) * position_size
                    exit_commission = exit_price * position_size * p.commissionRate
                    net_pnl = gross_pnl - exit_commission - entry_commission
                    realized_equity += gross_pnl - exit_commission
                    entry_value = entry_price * position_size
                    profit_pct = (net_pnl / entry_value * 100.0) if entry_value else None
                    trades.append(
                        TradeRecord(
                            direction="long",
                            side="LONG",
                            entry_time=entry_time_long,
                            exit_time=time,
                            entry_price=entry_price,
                            exit_price=exit_price,
                            size=position_size,
                            net_pnl=net_pnl,
                            profit_pct=profit_pct,
                        )
                    )
                    position = 0
                    position_size = 0.0
                    entry_price = math.nan
                    stop_price = math.nan
                    target_price = math.nan
                    trail_price_long = math.nan
                    trail_activated_long = False
                    entry_time_long = None
                    entry_commission = 0.0

            elif position < 0:
                if not trail_activated_short and not math.isnan(entry_price) and not math.isnan(stop_price):
                    activation_price = entry_price - (stop_price - entry_price) * p.trailRRShort
                    if l <= activation_price:
                        trail_activated_short = True
                        if math.isnan(trail_price_short):
                            trail_price_short = stop_price
                if not math.isnan(trail_price_short) and not np.isnan(trail_short_value):
                    if np.isnan(trail_price_short) or trail_short_value < trail_price_short:
                        trail_price_short = trail_short_value
                if trail_activated_short:
                    if not math.isnan(trail_price_short) and h >= trail_price_short:
                        exit_price = l if trail_price_short < l else trail_price_short
                else:
                    if h >= stop_price:
                        exit_price = stop_price
                    elif l <= target_price:
                        exit_price = target_price
                if exit_price is None and entry_time_short is not None and p.stopShortMaxDays > 0:
                    days_in_trade = int(math.floor((time - entry_time_short).total_seconds() / 86400))
                    if days_in_trade >= p.stopShortMaxDays:
                        exit_price = c
                if exit_price is not None:
                    gross_pnl = (entry_price - exit_price) * position_size
                    exit_commission = exit_price * position_size * p.commissionRate
                    net_pnl = gross_pnl - exit_commission - entry_commission
                    realized_equity += gross_pnl - exit_commission
                    entry_value = entry_price * position_size
                    profit_pct = (net_pnl / entry_value * 100.0) if entry_value else None
                    trades.append(
                        TradeRecord(
                            direction="short",
                            side="SHORT",
                            entry_time=entry_time_short,
                            exit_time=time,
                            entry_price=entry_price,
                            exit_price=exit_price,
                            size=position_size,
                            net_pnl=net_pnl,
                            profit_pct=profit_pct,
                        )
                    )
                    position = 0
                    position_size = 0.0
                    entry_price = math.nan
                    stop_price = math.nan
                    target_price = math.nan
                    trail_price_short = math.nan
                    trail_activated_short = False
                    entry_time_short = None
                    entry_commission = 0.0

            up_trend = counter_close_trend_long >= p.closeCountLong and counter_trade_long == 0
            down_trend = counter_close_trend_short >= p.closeCountShort and counter_trade_short == 0

            can_open_long = (
                up_trend
                and position == 0
                and prev_position == 0
                and time_in_range[i]
                and not np.isnan(atr_value)
                and not np.isnan(lowest_value)
            )
            can_open_short = (
                down_trend
                and position == 0
                and prev_position == 0
                and time_in_range[i]
                and not np.isnan(atr_value)
                and not np.isnan(highest_value)
            )

            if can_open_long:
                stop_size = atr_value * p.stopLongX
                long_stop_price = lowest_value - stop_size
                long_stop_distance = c - long_stop_price
                if long_stop_distance > 0:
                    long_stop_pct = (long_stop_distance / c) * 100
                    if long_stop_pct <= p.stopLongMaxPct or p.stopLongMaxPct <= 0:
                        risk_cash = realized_equity * (p.riskPerTrade / 100)
                        qty = risk_cash / long_stop_distance if long_stop_distance != 0 else 0
                        if p.contractSize > 0:
                            qty = math.floor((qty / p.contractSize)) * p.contractSize
                        if qty > 0:
                            position = 1
                            position_size = qty
                            entry_price = c
                            stop_price = long_stop_price
                            target_price = c + long_stop_distance * p.stopLongRR
                            trail_price_long = long_stop_price
                            trail_activated_long = False
                            entry_time_long = time
                            entry_commission = entry_price * position_size * p.commissionRate
                            realized_equity -= entry_commission

            if can_open_short and position == 0:
                stop_size = atr_value * p.stopShortX
                short_stop_price = highest_value + stop_size
                short_stop_distance = short_stop_price - c
                if short_stop_distance > 0:
                    short_stop_pct = (short_stop_distance / c) * 100
                    if short_stop_pct <= p.stopShortMaxPct or p.stopShortMaxPct <= 0:
                        risk_cash = realized_equity * (p.riskPerTrade / 100)
                        qty = risk_cash / short_stop_distance if short_stop_distance != 0 else 0
                        if p.contractSize > 0:
                            qty = math.floor((qty / p.contractSize)) * p.contractSize
                        if qty > 0:
                            position = -1
                            position_size = qty
                            entry_price = c
                            stop_price = short_stop_price
                            target_price = c - short_stop_distance * p.stopShortRR
                            trail_price_short = short_stop_price
                            trail_activated_short = False
                            entry_time_short = time
                            entry_commission = entry_price * position_size * p.commissionRate
                            realized_equity -= entry_commission

            if i == last_bar_index and position != 0:
                entry_time = entry_time_long if position > 0 else entry_time_short
                trade, gross_pnl, exit_commission, _ = build_forced_close_trade(
                    position=position,
                    entry_time=entry_time,
                    exit_time=time,
                    entry_price=entry_price,
                    exit_price=c,
                    size=position_size,
                    entry_commission=entry_commission,
                    commission_rate=p.commissionRate,
                    commission_is_pct=False,
                )
                if trade:
                    trades.append(trade)
                    realized_equity += gross_pnl - exit_commission
                position = 0
                position_size = 0.0
                entry_price = math.nan
                stop_price = math.nan
                target_price = math.nan
                trail_price_long = math.nan
                trail_price_short = math.nan
                trail_activated_long = False
                trail_activated_short = False
                entry_time_long = None
                entry_time_short = None
                entry_commission = 0.0

            mark_to_market = realized_equity
            if position > 0 and not math.isnan(entry_price):
                mark_to_market += (c - entry_price) * position_size
            elif position < 0 and not math.isnan(entry_price):
                mark_to_market += (entry_price - c) * position_size
            realized_curve.append(realized_equity)
            mtm_curve.append(mark_to_market)
            prev_position = position

        timestamps = list(df.index[: len(mtm_curve)])

        result = StrategyResult(
            trades=trades,
            equity_curve=mtm_curve,
            balance_curve=realized_curve,
            timestamps=timestamps,
        )

        metrics.enrich_strategy_result(result, initial_balance=equity, risk_free_rate=0.02)

        return result
