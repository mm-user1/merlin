from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core import metrics
from core.backtest_engine import StrategyResult, TradeRecord, build_forced_close_trade
from indicators.ma import sma
from indicators.oscillators import stoch_rsi
from strategies.base import BaseStrategy


@dataclass
class S04Params:
    rsiLen: int = 16
    stochLen: int = 16
    kLen: int = 3
    dLen: int = 3
    obLevel: float = 75.0
    osLevel: float = 15.0
    extLookback: int = 23
    confirmBars: int = 14
    riskPerTrade: float = 2.0
    contractSize: float = 0.01
    initialCapital: float = 100.0
    commissionPct: float = 0.05
    startDate: Optional[pd.Timestamp] = None
    endDate: Optional[pd.Timestamp] = None

    # Note: The to_dict() method was removed in Phase 9-5-1.
    # Use dataclasses.asdict(params) to convert instances to dictionaries.

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[pd.Timestamp]:
        if value in (None, ""):
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "S04Params":
        payload = payload or {}
        return cls(
            rsiLen=int(payload.get("rsiLen", cls.rsiLen)),
            stochLen=int(payload.get("stochLen", cls.stochLen)),
            kLen=int(payload.get("kLen", cls.kLen)),
            dLen=int(payload.get("dLen", cls.dLen)),
            obLevel=float(payload.get("obLevel", cls.obLevel)),
            osLevel=float(payload.get("osLevel", cls.osLevel)),
            extLookback=int(payload.get("extLookback", cls.extLookback)),
            confirmBars=int(payload.get("confirmBars", cls.confirmBars)),
            riskPerTrade=float(payload.get("riskPerTrade", cls.riskPerTrade)),
            contractSize=float(payload.get("contractSize", cls.contractSize)),
            initialCapital=float(payload.get("initialCapital", cls.initialCapital)),
            commissionPct=float(payload.get("commissionPct", cls.commissionPct)),
            startDate=cls._parse_timestamp(payload.get("startDate")),
            endDate=cls._parse_timestamp(payload.get("endDate")),
        )


class S04StochRSI(BaseStrategy):
    STRATEGY_ID = "s04_stochrsi"
    STRATEGY_NAME = "S04 StochRSI"
    STRATEGY_VERSION = "v02"

    @staticmethod
    def run(df: pd.DataFrame, params: Dict[str, Any], trade_start_idx: int = 0) -> StrategyResult:
        p = S04Params.from_dict(params)

        if df.empty:
            return StrategyResult(trades=[], equity_curve=[], balance_curve=[], timestamps=[])

        close = df["Close"]
        high = df["High"]
        low = df["Low"]

        stoch_values = stoch_rsi(close, p.rsiLen, p.stochLen)
        k = sma(stoch_values, p.kLen)
        d = sma(k, p.dLen)

        lowest_low_series = low.rolling(p.extLookback, min_periods=1).min()
        highest_high_series = high.rolling(p.extLookback, min_periods=1).max()
        timestamps_index = list(df.index)
        close_values = close.to_numpy(copy=False)
        high_values = high.to_numpy(copy=False)
        low_values = low.to_numpy(copy=False)
        k_values = k.to_numpy(copy=False)
        d_values = d.to_numpy(copy=False)
        lowest_low_values = lowest_low_series.to_numpy(copy=False)
        highest_high_values = highest_high_series.to_numpy(copy=False)

        balance = p.initialCapital
        position = 0
        prev_position = 0
        position_size = 0.0
        entry_price = np.nan
        stop_price = np.nan
        entry_commission = 0.0
        entry_time: Optional[pd.Timestamp] = None

        os_cross_long_flag = False
        ob_cross_short_flag = False
        swing_low = np.nan
        swing_low_count = 0
        trend_long_flag = False
        swing_high = np.nan
        swing_high_count = 0
        trend_short_flag = False

        trades: List[TradeRecord] = []
        equity_curve: List[float] = []
        balance_curve: List[float] = []
        timestamps: List[pd.Timestamp] = []
        last_bar_index = len(timestamps_index) - 1

        for i in range(len(timestamps_index)):
            timestamp = timestamps_index[i]
            close_val = close_values[i]
            high_val = high_values[i]
            low_val = low_values[i]

            k_curr = k_values[i]
            d_curr = d_values[i]
            k_prev = k_values[i - 1] if i > 0 else np.nan
            d_prev = d_values[i - 1] if i > 0 else np.nan

            bull_cross_in_os = False
            bear_cross_in_ob = False
            reset_os = False
            reset_ob = False

            if not np.isnan(k_curr) and not np.isnan(d_curr) and not np.isnan(k_prev) and not np.isnan(d_prev):
                bull_cross_in_os = (
                    (k_curr > d_curr)
                    and (k_prev <= d_prev)
                    and (k_curr < p.osLevel)
                    and (d_curr < p.osLevel)
                )
                bear_cross_in_ob = (
                    (k_curr < d_curr)
                    and (k_prev >= d_prev)
                    and (k_curr > p.obLevel)
                    and (d_curr > p.obLevel)
                )
                reset_os = (k_curr > p.osLevel) and (k_prev <= p.osLevel)
                reset_ob = (k_curr < p.obLevel) and (k_prev >= p.obLevel)

            if bull_cross_in_os:
                os_cross_long_flag = True
            if reset_os:
                os_cross_long_flag = False

            if bear_cross_in_ob:
                ob_cross_short_flag = True
            if reset_ob:
                ob_cross_short_flag = False

            lowest_low = lowest_low_values[i]
            if np.isnan(swing_low) or lowest_low != swing_low:
                swing_low = lowest_low
                swing_low_count = 0
                trend_long_flag = False

            if not np.isnan(swing_low):
                if low_val > swing_low:
                    swing_low_count += 1
                    if swing_low_count >= p.confirmBars:
                        trend_long_flag = True
                elif low_val < swing_low:
                    swing_low = low_val
                    swing_low_count = 0
                    trend_long_flag = False

            highest_high = highest_high_values[i]
            if np.isnan(swing_high) or highest_high != swing_high:
                swing_high = highest_high
                swing_high_count = 0
                trend_short_flag = False

            if not np.isnan(swing_high):
                if high_val < swing_high:
                    swing_high_count += 1
                    if swing_high_count >= p.confirmBars:
                        trend_short_flag = True
                elif high_val > swing_high:
                    swing_high = high_val
                    swing_high_count = 0
                    trend_short_flag = False

            exit_price: Optional[float] = None
            if position > 0:
                if not np.isnan(stop_price) and low_val <= stop_price:
                    exit_price = stop_price
                if exit_price is None and bear_cross_in_ob:
                    exit_price = close_val
                if exit_price is not None:
                    exit_commission = exit_price * position_size * (p.commissionPct / 100.0)
                    gross_pnl = (exit_price - entry_price) * position_size
                    balance += gross_pnl - exit_commission - entry_commission
                    net_pnl = gross_pnl - exit_commission - entry_commission
                    profit_pct = (
                        (net_pnl / (entry_price * position_size) * 100.0)
                        if entry_price * position_size
                        else None
                    )
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
                    entry_price = np.nan
                    stop_price = np.nan
                    entry_commission = 0.0
                    entry_time = None

            elif position < 0:
                if not np.isnan(stop_price) and high_val >= stop_price:
                    exit_price = stop_price
                if exit_price is None and bull_cross_in_os:
                    exit_price = close_val
                if exit_price is not None:
                    exit_commission = exit_price * position_size * (p.commissionPct / 100.0)
                    gross_pnl = (entry_price - exit_price) * position_size
                    balance += gross_pnl - exit_commission - entry_commission
                    net_pnl = gross_pnl - exit_commission - entry_commission
                    profit_pct = (
                        (net_pnl / (entry_price * position_size) * 100.0)
                        if entry_price * position_size
                        else None
                    )
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
                    entry_price = np.nan
                    stop_price = np.nan
                    entry_commission = 0.0
                    entry_time = None

            trading_window = True
            if p.startDate is not None and timestamp < p.startDate:
                trading_window = False
            if p.endDate is not None and timestamp > p.endDate:
                trading_window = False

            trading_enabled = (i >= trade_start_idx) and trading_window
            if trading_enabled and position == 0 and prev_position == 0:
                if os_cross_long_flag and trend_long_flag and not np.isnan(swing_low):
                    entry_price = close_val
                    stop_price = swing_low
                    stop_distance = entry_price - stop_price
                    if stop_distance > 0:
                        risk_amount = balance * (p.riskPerTrade / 100.0)
                        raw_size = risk_amount / stop_distance if stop_distance else 0.0
                        position_size = (
                            np.floor(raw_size / p.contractSize) * p.contractSize
                            if p.contractSize > 0
                            else 0.0
                        )
                        if position_size > 0:
                            position = 1
                            entry_time = timestamp
                            entry_commission = entry_price * position_size * (p.commissionPct / 100.0)
                        else:
                            entry_price = np.nan
                            stop_price = np.nan
                elif ob_cross_short_flag and trend_short_flag and not np.isnan(swing_high):
                    entry_price = close_val
                    stop_price = swing_high
                    stop_distance = stop_price - entry_price
                    if stop_distance > 0:
                        risk_amount = balance * (p.riskPerTrade / 100.0)
                        raw_size = risk_amount / stop_distance if stop_distance else 0.0
                        position_size = (
                            np.floor(raw_size / p.contractSize) * p.contractSize
                            if p.contractSize > 0
                            else 0.0
                        )
                        if position_size > 0:
                            position = -1
                            entry_time = timestamp
                            entry_commission = entry_price * position_size * (p.commissionPct / 100.0)
                        else:
                            entry_price = np.nan
                            stop_price = np.nan

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
                entry_price = np.nan
                stop_price = np.nan
                entry_commission = 0.0
                entry_time = None

            unrealized = 0.0
            if position > 0:
                unrealized = (close_val - entry_price) * position_size
            elif position < 0:
                unrealized = (entry_price - close_val) * position_size

            equity_value = balance + unrealized
            equity_curve.append(equity_value)
            # Blend unrealized PnL into the balance curve to approximate
            # TradingView-style drawdown while preserving mark-to-market equity tracking.
            balance_curve.append(balance + unrealized * 0.88)
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
