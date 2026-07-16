"""
S03 Reversal v11 Strategy
Reversal entries using close-count confirmation, T-Bands hysteresis, and
optional Emergency SL.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import math
import numpy as np
import pandas as pd

from core import metrics
from core.backtest_engine import StrategyResult, TradeRecord, build_forced_close_trade
from indicators.ma import get_ma
from strategies.base import BaseStrategy


EMERGENCY_SL_EXIT_REASON = "Emergency SL"


@dataclass
class S03Params:
    use_date_filter: bool = True
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    maType3: str = "SMA"
    maLength3: int = 75
    maOffset3: float = 0.2
    useCloseCount: bool = True
    closeCountLong: int = 7
    closeCountShort: int = 5
    useTBands: bool = True
    tBandLongPct: float = 1.0
    tBandShortPct: float = 1.3
    contractSize: float = 0.01
    useEmergencySL: bool = False
    emergencySlPct: float = 20.0
    emergencySlUpdateBars: int = 16
    initialCapital: float = 100.0
    commissionPct: float = 0.05

    @staticmethod
    def _coerce_bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y", "on"}:
                return True
            if lowered in {"false", "0", "no", "n", "off"}:
                return False
        return default

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "S03Params":
        d = payload or {}

        start = d.get("start")
        end = d.get("end")
        if isinstance(start, str):
            start = pd.Timestamp(start, tz="UTC")
        if isinstance(end, str):
            end = pd.Timestamp(end, tz="UTC")

        params = cls(
            use_date_filter=cls._coerce_bool(d.get("dateFilter"), cls.use_date_filter),
            start=start,
            end=end,
            maType3=str(d.get("maType3", cls.maType3)),
            maLength3=int(d.get("maLength3", cls.maLength3)),
            maOffset3=float(d.get("maOffset3", cls.maOffset3)),
            useCloseCount=cls._coerce_bool(d.get("useCloseCount"), cls.useCloseCount),
            closeCountLong=int(d.get("closeCountLong", cls.closeCountLong)),
            closeCountShort=int(d.get("closeCountShort", cls.closeCountShort)),
            useTBands=cls._coerce_bool(d.get("useTBands"), cls.useTBands),
            tBandLongPct=float(d.get("tBandLongPct", cls.tBandLongPct)),
            tBandShortPct=float(d.get("tBandShortPct", cls.tBandShortPct)),
            contractSize=float(d.get("contractSize", cls.contractSize)),
            useEmergencySL=cls._coerce_bool(d.get("useEmergencySL"), cls.useEmergencySL),
            emergencySlPct=float(d.get("emergencySlPct", cls.emergencySlPct)),
            emergencySlUpdateBars=int(d.get("emergencySlUpdateBars", cls.emergencySlUpdateBars)),
            initialCapital=float(d.get("initialCapital", cls.initialCapital)),
            commissionPct=float(d.get("commissionPct", cls.commissionPct)),
        )
        if params.useEmergencySL:
            if params.emergencySlPct <= 0:
                raise ValueError("emergencySlPct must be > 0 when useEmergencySL=true.")
            if params.emergencySlUpdateBars < 1:
                raise ValueError("emergencySlUpdateBars must be >= 1 when useEmergencySL=true.")
        return params


def _initial_emergency_sl_price(position: int, entry_price: float, pct: float) -> float:
    if position > 0:
        return entry_price * (1.0 - pct / 100.0)
    return entry_price * (1.0 + pct / 100.0)


def _candidate_emergency_sl_price(position: int, close_price: float, pct: float) -> float:
    if position > 0:
        return close_price * (1.0 - pct / 100.0)
    return close_price * (1.0 + pct / 100.0)


def _is_profitable_ratchet(position: int, current_price: float, candidate_price: float) -> bool:
    if math.isnan(current_price):
        return True
    if position > 0:
        return candidate_price > current_price
    return candidate_price < current_price


def _build_exit_trade(
    *,
    position: int,
    timestamp: pd.Timestamp,
    entry_time: Optional[pd.Timestamp],
    entry_price: float,
    exit_price: float,
    position_size: float,
    entry_commission: float,
    commission_pct: float,
    exit_reason: Optional[str] = None,
) -> tuple[TradeRecord, float]:
    exit_commission = exit_price * position_size * (commission_pct / 100.0)
    gross_pnl = (
        (exit_price - entry_price) * position_size
        if position > 0
        else (entry_price - exit_price) * position_size
    )
    net_pnl = gross_pnl - exit_commission - entry_commission
    entry_value = entry_price * position_size
    profit_pct = (net_pnl / entry_value * 100.0) if entry_value else None
    direction = "long" if position > 0 else "short"
    side = "LONG" if position > 0 else "SHORT"
    return (
        TradeRecord(
            direction=direction,
            side=side,
            entry_time=entry_time,
            exit_time=timestamp,
            entry_price=entry_price,
            exit_price=exit_price,
            size=position_size,
            net_pnl=net_pnl,
            profit_pct=profit_pct,
            exit_reason=exit_reason,
        ),
        net_pnl,
    )


class S03ReversalV11(BaseStrategy):
    STRATEGY_ID = "s03_reversal_v11"
    STRATEGY_NAME = "S03 Reversal"
    STRATEGY_VERSION = "v11"

    @staticmethod
    def run(
        df: pd.DataFrame,
        params: Dict[str, Any],
        trade_start_idx: int = 0,
        force_close_last_bar: bool = True,
    ) -> StrategyResult:
        p = S03Params.from_dict(params)

        if df.empty:
            return StrategyResult(trades=[], equity_curve=[], balance_curve=[], timestamps=[])

        close = df["Close"]
        open_ = df["Open"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]

        ma3 = get_ma(close, p.maType3, p.maLength3, volume, high, low)
        if p.maOffset3 != 0:
            ma3 = ma3 * (1 + p.maOffset3 / 100.0)

        ma3_up_band = ma3 * (1 + p.tBandLongPct / 100.0)
        ma3_down_band = ma3 * (1 - p.tBandShortPct / 100.0)
        timestamps_index = list(df.index)
        close_values = close.to_numpy(copy=False)
        open_values = open_.to_numpy(copy=False)
        high_values = high.to_numpy(copy=False)
        low_values = low.to_numpy(copy=False)
        ma3_values = ma3.to_numpy(copy=False)
        ma3_up_band_values = ma3_up_band.to_numpy(copy=False)
        ma3_down_band_values = ma3_down_band.to_numpy(copy=False)

        if p.use_date_filter:
            time_in_range = np.zeros(len(df), dtype=bool)
            time_in_range[trade_start_idx:] = True
        else:
            time_in_range = np.ones(len(df), dtype=bool)

        balance = p.initialCapital
        position = 0
        prev_position = 0
        position_size = 0.0
        entry_price = math.nan
        entry_commission = 0.0
        entry_time: Optional[pd.Timestamp] = None

        emergency_sl_price = math.nan
        emergency_sl_entry_index = -1
        emergency_sl_bars = 0

        t_band_state = 0
        count_close_long = 0
        count_close_short = 0

        trades: List[TradeRecord] = []
        equity_curve: List[float] = []
        balance_curve: List[float] = []
        timestamps: List[pd.Timestamp] = []

        trading_disabled = not (p.useCloseCount or p.useTBands)
        last_bar_index = len(timestamps_index) - 1

        for i in range(len(timestamps_index)):
            timestamp = timestamps_index[i]
            close_val = float(close_values[i])
            open_val = float(open_values[i])
            high_val = float(high_values[i])
            low_val = float(low_values[i])

            ma_val = ma3_values[i]
            up_band = ma3_up_band_values[i]
            down_band = ma3_down_band_values[i]

            break_up = False
            break_down = False
            cross_fail = False
            if not np.isnan(up_band) and not np.isnan(down_band):
                break_up = (high_val > up_band) and (close_val > up_band)
                break_down = (low_val < down_band) and (close_val < down_band)
                cross_fail = (high_val >= up_band) and (low_val <= down_band)

            if cross_fail:
                if not np.isnan(ma_val):
                    t_band_state = 1 if close_val > ma_val else -1
            else:
                if break_up:
                    t_band_state = 1
                elif break_down:
                    t_band_state = -1

            if not np.isnan(ma_val):
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

            count_long = True if not p.useCloseCount else count_close_long >= p.closeCountLong
            count_short = True if not p.useCloseCount else count_close_short >= p.closeCountShort

            cross_tband_long = True if not p.useTBands else t_band_state == 1
            cross_tband_short = True if not p.useTBands else t_band_state == -1

            in_range = time_in_range[i]

            long_conditions = (not trading_disabled) and in_range and count_long and cross_tband_long
            short_conditions = (not trading_disabled) and in_range and count_short and cross_tband_short

            emergency_exit = False
            if (
                p.useEmergencySL
                and position != 0
                and emergency_sl_entry_index >= 0
                and i >= emergency_sl_entry_index + 2
                and not math.isnan(emergency_sl_price)
            ):
                if position > 0 and low_val <= emergency_sl_price:
                    exit_price = min(open_val, emergency_sl_price)
                    trade, net_pnl = _build_exit_trade(
                        position=position,
                        timestamp=timestamp,
                        entry_time=entry_time,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        position_size=position_size,
                        entry_commission=entry_commission,
                        commission_pct=p.commissionPct,
                        exit_reason=EMERGENCY_SL_EXIT_REASON,
                    )
                    trades.append(trade)
                    balance += net_pnl
                    emergency_exit = True
                elif position < 0 and high_val >= emergency_sl_price:
                    exit_price = max(open_val, emergency_sl_price)
                    trade, net_pnl = _build_exit_trade(
                        position=position,
                        timestamp=timestamp,
                        entry_time=entry_time,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        position_size=position_size,
                        entry_commission=entry_commission,
                        commission_pct=p.commissionPct,
                        exit_reason=EMERGENCY_SL_EXIT_REASON,
                    )
                    trades.append(trade)
                    balance += net_pnl
                    emergency_exit = True

                if emergency_exit:
                    position = 0
                    position_size = 0.0
                    entry_price = math.nan
                    entry_commission = 0.0
                    entry_time = None
                    emergency_sl_price = math.nan
                    emergency_sl_entry_index = -1
                    emergency_sl_bars = 0

            if not emergency_exit:
                exit_price: Optional[float] = None
                if position > 0:
                    if short_conditions or not in_range:
                        exit_price = close_val
                    if exit_price is not None:
                        trade, net_pnl = _build_exit_trade(
                            position=position,
                            timestamp=timestamp,
                            entry_time=entry_time,
                            entry_price=entry_price,
                            exit_price=exit_price,
                            position_size=position_size,
                            entry_commission=entry_commission,
                            commission_pct=p.commissionPct,
                        )
                        trades.append(trade)
                        balance += net_pnl
                        position = 0
                        position_size = 0.0
                        entry_price = math.nan
                        entry_commission = 0.0
                        entry_time = None
                        emergency_sl_price = math.nan
                        emergency_sl_entry_index = -1
                        emergency_sl_bars = 0

                elif position < 0:
                    if long_conditions or not in_range:
                        exit_price = close_val
                    if exit_price is not None:
                        trade, net_pnl = _build_exit_trade(
                            position=position,
                            timestamp=timestamp,
                            entry_time=entry_time,
                            entry_price=entry_price,
                            exit_price=exit_price,
                            position_size=position_size,
                            entry_commission=entry_commission,
                            commission_pct=p.commissionPct,
                        )
                        trades.append(trade)
                        balance += net_pnl
                        position = 0
                        position_size = 0.0
                        entry_price = math.nan
                        entry_commission = 0.0
                        entry_time = None
                        emergency_sl_price = math.nan
                        emergency_sl_entry_index = -1
                        emergency_sl_bars = 0

            if (
                p.useEmergencySL
                and position != 0
                and emergency_sl_entry_index >= 0
                and i >= emergency_sl_entry_index + 2
                and not math.isnan(emergency_sl_price)
            ):
                emergency_sl_bars += 1
                if emergency_sl_bars >= p.emergencySlUpdateBars:
                    candidate_sl = _candidate_emergency_sl_price(position, close_val, p.emergencySlPct)
                    if _is_profitable_ratchet(position, emergency_sl_price, candidate_sl):
                        emergency_sl_price = candidate_sl
                    emergency_sl_bars = 0

            if (
                not trading_disabled
                and in_range
                and position == 0
                and (prev_position == 0 or emergency_exit)
                and close_val > 0
            ):
                contract_size = p.contractSize
                if contract_size > 0:
                    if long_conditions:
                        size = math.floor((balance / close_val) / contract_size) * contract_size
                        if size > 0:
                            position = 1
                            position_size = float(size)
                            entry_price = close_val
                            entry_commission = entry_price * position_size * (p.commissionPct / 100.0)
                            entry_time = timestamp
                            if p.useEmergencySL:
                                emergency_sl_price = _initial_emergency_sl_price(
                                    position,
                                    entry_price,
                                    p.emergencySlPct,
                                )
                                emergency_sl_entry_index = i
                                emergency_sl_bars = 0
                    elif short_conditions:
                        size = math.floor((balance / close_val) / contract_size) * contract_size
                        if size > 0:
                            position = -1
                            position_size = float(size)
                            entry_price = close_val
                            entry_commission = entry_price * position_size * (p.commissionPct / 100.0)
                            entry_time = timestamp
                            if p.useEmergencySL:
                                emergency_sl_price = _initial_emergency_sl_price(
                                    position,
                                    entry_price,
                                    p.emergencySlPct,
                                )
                                emergency_sl_entry_index = i
                                emergency_sl_bars = 0

            if force_close_last_bar and i == last_bar_index and position != 0:
                trade, gross_pnl, exit_commission, _ = build_forced_close_trade(
                    position=position,
                    entry_time=entry_time,
                    exit_time=timestamp,
                    entry_price=entry_price,
                    exit_price=close_val,
                    size=position_size,
                    entry_commission=entry_commission,
                    commission_rate=p.commissionPct,
                    commission_is_pct=True,
                )
                if trade:
                    trades.append(trade)
                    balance += gross_pnl - exit_commission - entry_commission
                position = 0
                position_size = 0.0
                entry_price = math.nan
                entry_commission = 0.0
                entry_time = None
                emergency_sl_price = math.nan
                emergency_sl_entry_index = -1
                emergency_sl_bars = 0

            unrealized = 0.0
            if position > 0 and not math.isnan(entry_price):
                unrealized = (close_val - entry_price) * position_size
            elif position < 0 and not math.isnan(entry_price):
                unrealized = (entry_price - close_val) * position_size

            equity_value = balance + unrealized
            equity_curve.append(equity_value)
            balance_curve.append(balance)
            timestamps.append(timestamp)

            prev_position = position

        result = StrategyResult(
            trades=trades,
            equity_curve=equity_curve,
            balance_curve=balance_curve,
            timestamps=timestamps,
        )

        if not force_close_last_bar:
            sl_price = None
            if p.useEmergencySL and position != 0 and not math.isnan(emergency_sl_price):
                sl_price = emergency_sl_price
            result.last_position = {
                "direction": "long" if position > 0 else ("short" if position < 0 else None),
                "entry_price": None if position == 0 or math.isnan(entry_price) else entry_price,
                "sl_price": sl_price,
                "trail_price": None,
                "entry_time": entry_time if position != 0 else None,
            }

        metrics.enrich_strategy_result(result, initial_balance=p.initialCapital, risk_free_rate=0.02)

        return result
