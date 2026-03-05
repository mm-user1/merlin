"""
Metrics calculation module.

This module provides:
- BasicMetrics: Net profit, drawdown, trade statistics
- AdvancedMetrics: Sharpe, RoMaD, Profit Factor, SQN, Ulcer Index, Consistency
- Calculation functions that operate on StrategyResult
- enrich_strategy_result: Helper to compute and attach metrics to StrategyResult

Architectural note: This module ONLY calculates metrics.
It does NOT orchestrate backtests or optimization.
Other modules (backtest_engine, optuna_engine, walkforward_engine) consume these metrics.

Strategies should use enrich_strategy_result() to avoid manual field assignment
and prevent drift where undeclared attributes get set on StrategyResult.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import numpy as np
import pandas as pd
from backtesting import _stats

if TYPE_CHECKING:
    from .backtest_engine import StrategyResult, TradeRecord

logger = logging.getLogger(__name__)


# ============================================================================
# Data Structures
# ============================================================================


@dataclass
class BasicMetrics:
    """
    Basic performance metrics calculated from strategy results.

    These are the fundamental metrics that describe strategy performance:
    - Profitability (net profit, gross profit/loss)
    - Risk (max drawdown)
    - Activity (total trades, win rate)
    - Efficiency (average win/loss sizes)

    All metrics are calculated from the trade list and equity curve.
    """

    net_profit: float
    net_profit_pct: float
    gross_profit: float
    gross_loss: float
    max_drawdown: float
    max_drawdown_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    avg_trade: float
    max_consecutive_losses: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "net_profit": self.net_profit,
            "net_profit_pct": self.net_profit_pct,
            "gross_profit": self.gross_profit,
            "gross_loss": self.gross_loss,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_pct": self.max_drawdown_pct,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": self.win_rate,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "avg_trade": self.avg_trade,
            "max_consecutive_losses": self.max_consecutive_losses,
        }


@dataclass
class AdvancedMetrics:
    """
    Advanced risk-adjusted metrics for optimization and analysis.

    These metrics provide deeper insight into strategy quality:
    - Risk-adjusted returns (Sharpe, Sortino)
    - Efficiency ratios (Profit Factor, RoMaD, SQN)
    - Volatility measures (Ulcer Index)
    - Consistency indicators (stability across sub-periods)

    All values are Optional since they may not be calculable
    (e.g., insufficient sub-period data).
    """

    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    profit_factor: Optional[float] = None
    romad: Optional[float] = None
    sqn: Optional[float] = None
    ulcer_index: Optional[float] = None
    consistency_score: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "profit_factor": self.profit_factor,
            "romad": self.romad,
            "sqn": self.sqn,
            "ulcer_index": self.ulcer_index,
            "consistency_score": self.consistency_score,
        }


@dataclass
class WFAMetrics:
    """
    Walk-Forward Analysis aggregate metrics.

    Aggregates metrics across multiple WFA windows to assess:
    - Average performance across windows
    - Consistency between in-sample and out-of-sample
    - Success rate for OOS profitability
    """

    avg_net_profit_pct: float
    avg_max_drawdown_pct: float
    successful_windows: int
    total_windows: int
    success_rate: float
    avg_sharpe_ratio: Optional[float] = None
    avg_romad: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "avg_net_profit_pct": self.avg_net_profit_pct,
            "avg_max_drawdown_pct": self.avg_max_drawdown_pct,
            "successful_windows": self.successful_windows,
            "total_windows": self.total_windows,
            "success_rate": self.success_rate,
            "avg_sharpe_ratio": self.avg_sharpe_ratio,
            "avg_romad": self.avg_romad,
        }


# ============================================================================
# Helper Functions
# ============================================================================


def _calculate_monthly_returns(
    equity_curve: List[float],
    time_index: pd.DatetimeIndex,
) -> List[float]:
    """
    Calculate monthly percentage returns from equity curve and timestamps.

    This is a COPY of the function from backtest_engine.py and must remain
    bit-exact compatible with the original implementation.
    """
    if not equity_curve or len(equity_curve) != len(time_index):
        return []

    monthly_returns: List[float] = []
    current_month = None
    month_start_equity: Optional[float] = None

    for equity, timestamp in zip(equity_curve, time_index):
        month_key = (timestamp.year, timestamp.month)

        if current_month is None:
            current_month = month_key
            month_start_equity = equity
        elif month_key != current_month:
            if month_start_equity is not None and month_start_equity > 0:
                monthly_return = ((equity / month_start_equity) - 1.0) * 100.0
                monthly_returns.append(monthly_return)

            current_month = month_key
            month_start_equity = equity

    if month_start_equity is not None and month_start_equity > 0 and equity_curve:
        last_equity = equity_curve[-1]
        monthly_return = ((last_equity / month_start_equity) - 1.0) * 100.0
        monthly_returns.append(monthly_return)

    return monthly_returns


def normalize_consistency_segments(
    value: Any,
    default: int = 4,
    min_segments: int = 2,
    max_segments: int = 24,
) -> int:
    """Normalize consistency segment count into a safe integer range."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(min_segments), min(int(max_segments), parsed))


def derive_auto_consistency_segments(
    is_period_days: Any,
    is_consistency_segments: Any,
    target_period_days: Any,
) -> Optional[int]:
    """Derive auto segment count for FT/OOS from IS segment size."""
    try:
        is_days = float(is_period_days)
        target_days = float(target_period_days)
    except (TypeError, ValueError):
        return None

    if is_days <= 0 or target_days <= 0:
        return None

    is_segments = normalize_consistency_segments(is_consistency_segments)
    segment_days = is_days / float(is_segments)
    if segment_days <= 0:
        return None

    derived = int(round(target_days / segment_days))
    if derived < 2:
        return None

    return derived


def _calculate_subperiod_returns(
    equity_curve: List[float],
    n_segments: int,
) -> List[float]:
    """Split equity into shared-boundary segments and compute % return for each."""
    n = len(equity_curve)
    if n_segments < 2 or n < (n_segments + 1):
        return []

    last_idx = n - 1
    returns: List[float] = []

    for i in range(n_segments):
        start_idx = (i * last_idx) // n_segments
        end_idx = ((i + 1) * last_idx) // n_segments
        if end_idx <= start_idx:
            continue

        start_eq = float(equity_curve[start_idx])
        end_eq = float(equity_curve[end_idx])
        if start_eq > 0:
            returns.append(((end_eq / start_eq) - 1.0) * 100.0)

    return returns


def _calculate_profit_factor_value(trades: List[TradeRecord]) -> Optional[float]:
    """Calculate Profit Factor (gross profit / gross loss)."""
    if not trades:
        return None

    gross_profit = sum(t.net_pnl for t in trades if t.net_pnl > 0)
    gross_loss = abs(sum(t.net_pnl for t in trades if t.net_pnl < 0))

    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return float("inf")
    return 1.0


def _calculate_sharpe_ratio_value(
    monthly_returns: List[float],
    risk_free_rate: float = 0.02,
) -> Optional[float]:
    """Calculate annualized Sharpe Ratio from monthly returns."""
    if len(monthly_returns) < 2:
        return None

    monthly_array = np.array(monthly_returns, dtype=float)
    if monthly_array.size < 2:
        return None

    avg_return = float(np.mean(monthly_array))
    sd_return = float(np.std(monthly_array, ddof=0))

    if sd_return == 0:
        return None

    rfr_monthly = (risk_free_rate * 100.0) / 12.0
    sharpe = (avg_return - rfr_monthly) / sd_return
    return sharpe


def _calculate_sortino_ratio_value(
    monthly_returns: List[float],
    risk_free_rate: float = 0.02,
) -> Optional[float]:
    """Calculate annualized Sortino Ratio from monthly returns."""
    if len(monthly_returns) < 2:
        return None

    monthly_array = np.array(monthly_returns, dtype=float)
    if monthly_array.size < 2:
        return None

    rfr_monthly = (risk_free_rate * 100.0) / 12.0
    downside = monthly_array[monthly_array < rfr_monthly] - rfr_monthly
    if downside.size == 0:
        return None

    downside_dev = float(np.std(downside, ddof=0))
    if downside_dev == 0:
        return None

    avg_return = float(np.mean(monthly_array))
    sortino = (avg_return - rfr_monthly) / downside_dev
    return sortino


def _calculate_ulcer_index_value(equity_curve: List[float]) -> Optional[float]:
    """Calculate Ulcer Index (downside volatility measure)."""
    equity_array = np.array(equity_curve, dtype=float)
    if equity_array.size == 0:
        return None

    running_max = np.maximum.accumulate(equity_array)

    with np.errstate(divide="ignore", invalid="ignore"):
        drawdowns = np.where(running_max > 0, equity_array / running_max - 1.0, 0.0)

    drawdown_squared_sum = float(np.square(drawdowns).sum())
    ulcer = math.sqrt(drawdown_squared_sum / equity_array.size) * 100.0

    return ulcer


def _calculate_consistency_score_value(sub_returns: List[float]) -> Optional[float]:
    """Calculate stability-based consistency score from sub-period returns."""
    if len(sub_returns) < 2:
        return None

    values = np.array(sub_returns, dtype=float)
    if values.size < 2:
        return None

    median_ret = float(np.median(values))
    std_ret = float(np.std(values, ddof=0))
    loss_ratio = float(np.sum(values < 0.0)) / float(values.size)
    penalty = 1.0 - (loss_ratio**1.5)
    raw_score = (median_ret / (1.0 + std_ret)) * penalty

    if not math.isfinite(raw_score):
        return None

    return round(raw_score, 4)


def calculate_higher_moments_from_monthly_returns(
    monthly_returns: List[float],
) -> tuple[Optional[float], Optional[float]]:
    """
    Calculate skewness and RAW kurtosis from monthly returns.

    Returns:
        (skewness, raw_kurtosis) or (None, None) if insufficient data.
    """
    if len(monthly_returns) < 3:
        return None, None

    values = np.array(monthly_returns, dtype=float)
    if values.size < 3:
        return None, None

    mean = float(np.mean(values))
    std = float(np.std(values, ddof=0))
    if std == 0.0:
        return None, None

    standardized = (values - mean) / std
    skewness = float(np.mean(standardized**3))
    raw_kurtosis = float(np.mean(standardized**4))

    if not math.isfinite(skewness) or not math.isfinite(raw_kurtosis):
        return None, None

    return skewness, raw_kurtosis


def _calculate_sqn_value(trades: List[TradeRecord]) -> Optional[float]:
    """
    Calculate System Quality Number (Van Tharp).

    SQN = sqrt(N) * mean(trade_pnl) / std(trade_pnl)

    Measures trading system quality by combining profitability and consistency.
    Requires minimum 30 trades for statistical significance.
    """
    if len(trades) < 30:
        return None

    trade_pnl = np.array([t.net_pnl for t in trades], dtype=float)
    if trade_pnl.size < 30:
        return None

    mean_pnl = float(np.mean(trade_pnl))
    std_pnl = float(np.std(trade_pnl, ddof=1))

    if std_pnl == 0.0 or std_pnl < 1e-10:
        return None

    sqn = math.sqrt(trade_pnl.size) * mean_pnl / std_pnl
    return sqn


# ============================================================================
# Main Calculation Functions
# ============================================================================


def calculate_basic(result: StrategyResult, initial_balance: Optional[float] = None) -> BasicMetrics:
    """Calculate basic metrics from a strategy result."""
    balance_curve = result.balance_curve
    trades = result.trades

    if initial_balance is not None:
        starting_balance = initial_balance
    elif balance_curve:
        starting_balance = balance_curve[0]
    else:
        starting_balance = 0.0

    if balance_curve:
        net_profit = balance_curve[-1] - starting_balance
        net_profit_pct = (net_profit / starting_balance * 100.0) if starting_balance != 0 else 0.0
    else:
        net_profit = 0.0
        net_profit_pct = 0.0

    gross_profit = 0.0
    gross_loss = 0.0
    winning_trades = 0
    losing_trades = 0

    for trade in trades:
        if trade.net_pnl > 0:
            gross_profit += trade.net_pnl
            winning_trades += 1
        elif trade.net_pnl < 0:
            gross_loss += abs(trade.net_pnl)
            losing_trades += 1

    max_drawdown_pct = 0.0
    max_drawdown = 0.0

    if balance_curve:
        equity_series = pd.Series(balance_curve).ffill()
        drawdown = 1 - equity_series / equity_series.cummax()
        _, peak_dd = _stats.compute_drawdown_duration_peaks(drawdown)

        if not peak_dd.isna().all():
            max_drawdown_pct = float(peak_dd.max() * 100)

        peak_balance = float(equity_series.cummax().max()) if not equity_series.empty else 0.0
        if peak_balance > 0:
            max_drawdown = max_drawdown_pct / 100.0 * peak_balance

    total_trades = len(trades)
    win_rate = (winning_trades / total_trades * 100.0) if total_trades > 0 else 0.0
    avg_win = (gross_profit / winning_trades) if winning_trades > 0 else 0.0
    avg_loss = (gross_loss / losing_trades) if losing_trades > 0 else 0.0
    avg_trade = (net_profit / total_trades) if total_trades > 0 else 0.0

    max_consecutive_losses = 0
    consecutive_losses = 0
    for trade in trades:
        if trade.net_pnl <= 0:
            consecutive_losses += 1
            if consecutive_losses > max_consecutive_losses:
                max_consecutive_losses = consecutive_losses
        else:
            consecutive_losses = 0

    return BasicMetrics(
        net_profit=net_profit,
        net_profit_pct=net_profit_pct,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        max_drawdown=max_drawdown,
        max_drawdown_pct=max_drawdown_pct,
        total_trades=total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        avg_trade=avg_trade,
        max_consecutive_losses=max_consecutive_losses,
    )


def calculate_advanced(
    result: StrategyResult,
    initial_balance: Optional[float] = None,
    risk_free_rate: float = 0.02,
    consistency_segments: Optional[int] = 4,
) -> AdvancedMetrics:
    """Calculate advanced risk-adjusted metrics from strategy result."""
    trades = result.trades
    equity_curve = result.equity_curve
    timestamps = result.timestamps

    sharpe_ratio = None
    sortino_ratio = None
    profit_factor = None
    romad = None
    sqn = None
    ulcer_index = None
    consistency_score = None

    if trades:
        profit_factor = _calculate_profit_factor_value(trades)
        sqn = _calculate_sqn_value(trades)

    monthly_returns: List[float] = []
    if trades and timestamps:
        time_index = pd.DatetimeIndex(timestamps)
        monthly_returns = _calculate_monthly_returns(equity_curve, time_index)

    if len(monthly_returns) >= 2:
        sharpe_ratio = _calculate_sharpe_ratio_value(monthly_returns, risk_free_rate)
        sortino_ratio = _calculate_sortino_ratio_value(monthly_returns, risk_free_rate)

    normalized_segments: Optional[int] = None
    if consistency_segments is not None:
        normalized_segments = normalize_consistency_segments(consistency_segments)

    if equity_curve and normalized_segments is not None:
        sub_returns = _calculate_subperiod_returns(equity_curve, normalized_segments)
        consistency_score = _calculate_consistency_score_value(sub_returns)

    if equity_curve:
        ulcer_index = _calculate_ulcer_index_value(equity_curve)

    basic = calculate_basic(result, initial_balance)

    if basic.max_drawdown_pct >= 0:
        if abs(basic.max_drawdown_pct) < 1e-9:
            romad = basic.net_profit_pct * 100.0 if basic.net_profit_pct >= 0 else 0.0
        elif basic.max_drawdown_pct != 0:
            romad = basic.net_profit_pct / abs(basic.max_drawdown_pct)
        else:
            romad = 0.0
    else:
        romad = 0.0

    return AdvancedMetrics(
        sharpe_ratio=sharpe_ratio,
        sortino_ratio=sortino_ratio,
        profit_factor=profit_factor,
        romad=romad,
        sqn=sqn,
        ulcer_index=ulcer_index,
        consistency_score=consistency_score,
    )


def enrich_strategy_result(
    result: StrategyResult,
    *,
    initial_balance: Optional[float] = None,
    risk_free_rate: float = 0.02,
) -> tuple[BasicMetrics, AdvancedMetrics]:
    """
    Compute metrics and attach declared fields to StrategyResult.

    Calculates BasicMetrics and AdvancedMetrics, then copies only the
    metric values whose keys match declared StrategyResult dataclass fields.
    This prevents drift where strategies assign undeclared attributes.

    Args:
        result: StrategyResult instance to enrich
        initial_balance: Starting capital for percentage calculations
        risk_free_rate: Annual risk-free rate for Sharpe/Sortino (default 0.02)

    Returns:
        Tuple of (BasicMetrics, AdvancedMetrics) for callers who need full metrics
    """
    basic = calculate_basic(result, initial_balance=initial_balance)
    advanced = calculate_advanced(
        result,
        initial_balance=initial_balance,
        risk_free_rate=risk_free_rate,
    )

    values = {**basic.to_dict(), **advanced.to_dict()}
    allowed = {field.name for field in fields(result)}
    for key, value in values.items():
        if key in allowed:
            setattr(result, key, value)

    return basic, advanced
