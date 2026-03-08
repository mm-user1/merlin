"""
S03 Reversal v10 Strategy
Reversal entries using close-count confirmation and T-Bands hysteresis.
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

        return cls(
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
            initialCapital=float(d.get("initialCapital", cls.initialCapital)),
            commissionPct=float(d.get("commissionPct", cls.commissionPct)),
        )


class S03ReversalV10(BaseStrategy):
    STRATEGY_ID = "s03_reversal_v10"
    STRATEGY_NAME = "S03 Reversal"
    STRATEGY_VERSION = "v10"

    @staticmethod
    def run(df: pd.DataFrame, params: Dict[str, Any], trade_start_idx: int = 0) -> StrategyResult:
        p = S03Params.from_dict(params)

        if df.empty:
            return StrategyResult(trades=[], equity_curve=[], balance_curve=[], timestamps=[])

        close = df["Close"]
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

            exit_price: Optional[float] = None
            if position > 0:
                if short_conditions or not in_range:
                    exit_price = close_val
                if exit_price is not None:
                    exit_commission = exit_price * position_size * (p.commissionPct / 100.0)
                    gross_pnl = (exit_price - entry_price) * position_size
                    balance += gross_pnl - exit_commission - entry_commission
                    net_pnl = gross_pnl - exit_commission - entry_commission
                    entry_value = entry_price * position_size
                    profit_pct = (net_pnl / entry_value * 100.0) if entry_value else None
                    trades.append(
                        TradeRecord(
                            direction="long",
                            side="LONG",
                            entry_time=entry_time,
                            exit_time=timestamp,
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
                    entry_commission = 0.0
                    entry_time = None

            elif position < 0:
                if long_conditions or not in_range:
                    exit_price = close_val
                if exit_price is not None:
                    exit_commission = exit_price * position_size * (p.commissionPct / 100.0)
                    gross_pnl = (entry_price - exit_price) * position_size
                    balance += gross_pnl - exit_commission - entry_commission
                    net_pnl = gross_pnl - exit_commission - entry_commission
                    entry_value = entry_price * position_size
                    profit_pct = (net_pnl / entry_value * 100.0) if entry_value else None
                    trades.append(
                        TradeRecord(
                            direction="short",
                            side="SHORT",
                            entry_time=entry_time,
                            exit_time=timestamp,
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
                    entry_commission = 0.0
                    entry_time = None

            if (
                not trading_disabled
                and in_range
                and position == 0
                and prev_position == 0
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
                    elif short_conditions:
                        size = math.floor((balance / close_val) / contract_size) * contract_size
                        if size > 0:
                            position = -1
                            position_size = float(size)
                            entry_price = close_val
                            entry_commission = entry_price * position_size * (p.commissionPct / 100.0)
                            entry_time = timestamp

            if i == last_bar_index and position != 0:
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

        metrics.enrich_strategy_result(result, initial_balance=p.initialCapital, risk_free_rate=0.02)

        return result
