"""Metric reference helpers for Backtester V2.

Phase 0 pins Merlin's current drawdown episode behavior without changing
``core.metrics``. Later fast metric kernels should match these semantics rather
than depending directly on private third-party APIs.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


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


__all__ = [
    "compute_drawdown_duration_peaks_reference",
    "drawdown_series_from_equity",
    "max_drawdown_pct_reference",
]
