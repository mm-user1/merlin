"""Metric reference helpers for Backtester V2.

Phase 0 pins Merlin's current drawdown episode behavior without changing
``core.metrics``. Later fast metric kernels should match these semantics rather
than depending directly on private third-party APIs.

Low-level V2 metric outputs are numeric-only: optional undefined values use
``nan`` and canonical infinite results use ``inf``.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CoreMetrics:
    """Balance-based core metrics needed by V2 execution/search code."""

    start_balance: float
    final_balance: float
    net_profit: float
    net_profit_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate_pct: float
    gross_profit: float
    gross_loss: float
    profit_factor: float
    max_drawdown_pct: float
    max_drawdown: float
    romad: float

    def to_dict(self) -> dict[str, float | int]:
        """Return a JSON-friendly mapping of core metric names to values."""

        return {
            "start_balance": self.start_balance,
            "final_balance": self.final_balance,
            "net_profit": self.net_profit,
            "net_profit_pct": self.net_profit_pct,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate_pct": self.win_rate_pct,
            "gross_profit": self.gross_profit,
            "gross_loss": self.gross_loss,
            "profit_factor": self.profit_factor,
            "max_drawdown_pct": self.max_drawdown_pct,
            "max_drawdown": self.max_drawdown,
            "romad": self.romad,
        }


def drawdown_series_from_equity(equity_curve: Iterable[float]) -> pd.Series:
    """Return Merlin's fractional drawdown series from an equity/balance path."""

    equity = pd.Series(list(equity_curve), dtype="float64").ffill()
    if equity.empty:
        return equity
    with np.errstate(divide="ignore", invalid="ignore"):
        drawdown = 1.0 - equity / equity.cummax()
    return drawdown


def compute_drawdown_duration_peaks_reference(
    drawdown: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """Vendored reference for current drawdown duration/peak behavior.

    This mirrors the ``backtesting==0.6.5`` implementation currently used by
    ``core.metrics.calculate_basic``. One important convention is that a
    trailing unrecovered drawdown with only one drawdown sample is not emitted
    as a separate recovered episode when earlier episodes exist.

    Empty input intentionally returns empty series. The upstream helper raises
    on empty input, but Merlin's metrics path guards empty balance curves before
    calling it.
    """

    if drawdown.empty:
        empty = pd.Series(dtype="float64", index=drawdown.index)
        return empty, empty

    zero_indexes = (drawdown == 0).values.nonzero()[0]
    iloc_values = np.unique(np.r_[zero_indexes, len(drawdown) - 1])
    iloc = pd.Series(iloc_values, index=drawdown.index[iloc_values])
    episodes = iloc.to_frame("iloc").assign(prev=iloc.shift())
    episodes = episodes[episodes["iloc"] > episodes["prev"] + 1].astype(np.int64)

    if not len(episodes):
        replaced = drawdown.replace(0, np.nan)
        return replaced, replaced

    episodes["duration"] = episodes["iloc"].map(drawdown.index.__getitem__) - episodes[
        "prev"
    ].map(drawdown.index.__getitem__)
    episodes["peak_dd"] = episodes.apply(
        lambda row: drawdown.iloc[row["prev"] : row["iloc"] + 1].max(),
        axis=1,
    )
    reindexed = episodes.reindex(drawdown.index)
    return reindexed["duration"], reindexed["peak_dd"]


def max_drawdown_pct_reference(equity_curve: Iterable[float]) -> float:
    """Return max drawdown percent using the pinned episode convention."""

    drawdown = drawdown_series_from_equity(equity_curve)
    if drawdown.empty:
        return 0.0
    _, peak_dd = compute_drawdown_duration_peaks_reference(drawdown)
    if peak_dd.isna().all():
        return 0.0
    return float(peak_dd.max() * 100.0)


def _trade_pnl(trade: Any) -> float:
    if isinstance(trade, Mapping):
        return float(trade["net_pnl"])
    if hasattr(trade, "net_pnl"):
        return float(trade.net_pnl)
    return float(trade)


def _profit_factor(gross_profit: float, gross_loss: float, total_trades: int) -> float:
    if total_trades == 0:
        return float("nan")
    if gross_loss > 0.0:
        return gross_profit / gross_loss
    if gross_profit > 0.0:
        return float("inf")
    return 1.0


def _romad(net_profit_pct: float, max_drawdown_pct: float) -> float:
    if max_drawdown_pct >= 0.0:
        if abs(max_drawdown_pct) < 1e-9:
            return net_profit_pct * 100.0 if net_profit_pct >= 0.0 else 0.0
        return net_profit_pct / abs(max_drawdown_pct)
    return 0.0


def _peak_balance_for_drawdown(balance_curve: list[float]) -> float:
    if not balance_curve:
        return 0.0
    balance = pd.Series(balance_curve, dtype="float64").ffill()
    if balance.empty:
        return 0.0
    peak_balance = float(balance.cummax().max())
    return peak_balance if math.isfinite(peak_balance) else 0.0


def compute_core_metrics_from_balance_and_trades(
    balance_curve: Iterable[float],
    trades: Iterable[Any],
    *,
    initial_balance: Optional[float] = None,
) -> CoreMetrics:
    """Compute Merlin-compatible core metrics from a realized balance curve.

    ``balance_curve`` is the realized balance path used for Merlin net-profit
    and drawdown metrics. Trade inputs may be Merlin ``TradeRecord`` objects,
    mappings with a ``net_pnl`` key, or raw numeric PnL values.
    """

    balances = [float(value) for value in balance_curve]
    pnls = [_trade_pnl(trade) for trade in trades]

    if initial_balance is not None:
        start_balance = float(initial_balance)
    elif balances:
        start_balance = balances[0]
    else:
        start_balance = 0.0

    final_balance = balances[-1] if balances else start_balance
    net_profit = final_balance - start_balance if balances else 0.0
    net_profit_pct = (net_profit / start_balance * 100.0) if start_balance != 0.0 else 0.0

    gross_profit = 0.0
    gross_loss = 0.0
    winning_trades = 0
    losing_trades = 0
    for pnl in pnls:
        if pnl > 0.0:
            gross_profit += pnl
            winning_trades += 1
        elif pnl < 0.0:
            gross_loss += abs(pnl)
            losing_trades += 1

    total_trades = len(pnls)
    win_rate_pct = (winning_trades / total_trades * 100.0) if total_trades else 0.0
    profit_factor = _profit_factor(gross_profit, gross_loss, total_trades)

    max_drawdown_pct = max_drawdown_pct_reference(balances) if balances else 0.0
    peak_balance = _peak_balance_for_drawdown(balances)
    max_drawdown = max_drawdown_pct / 100.0 * peak_balance if peak_balance > 0.0 else 0.0
    romad = _romad(net_profit_pct, max_drawdown_pct)

    return CoreMetrics(
        start_balance=start_balance,
        final_balance=final_balance,
        net_profit=net_profit,
        net_profit_pct=net_profit_pct,
        total_trades=total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        win_rate_pct=win_rate_pct,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        profit_factor=profit_factor,
        max_drawdown_pct=max_drawdown_pct,
        max_drawdown=max_drawdown,
        romad=romad,
    )


__all__ = [
    "CoreMetrics",
    "compute_core_metrics_from_balance_and_trades",
    "compute_drawdown_duration_peaks_reference",
    "drawdown_series_from_equity",
    "max_drawdown_pct_reference",
]
