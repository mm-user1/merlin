"""Generic data packing helpers for Backtester V2 execution."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .contracts import Signals
from .kernel import ExecutionData


def build_execution_data(
    df: pd.DataFrame,
    *,
    signals: Signals,
    atr: Any,
    rolling_low: Any,
    rolling_high: Any,
    trail_long: Any | None = None,
    trail_short: Any | None = None,
) -> ExecutionData:
    """Pack a prepared OHLCV frame plus strategy dataprep arrays for the kernel."""

    length = len(df)
    empty_float = np.full(length, np.nan, dtype=float)
    return ExecutionData(
        timestamps=tuple(df.index),
        open=np.asarray(df["Open"].to_numpy(copy=False), dtype=float),
        high=np.asarray(df["High"].to_numpy(copy=False), dtype=float),
        low=np.asarray(df["Low"].to_numpy(copy=False), dtype=float),
        close=np.asarray(df["Close"].to_numpy(copy=False), dtype=float),
        signals=signals,
        atr=np.asarray(atr, dtype=float),
        rolling_low=np.asarray(rolling_low, dtype=float),
        rolling_high=np.asarray(rolling_high, dtype=float),
        trail_long=np.asarray(trail_long, dtype=float) if trail_long is not None else empty_float,
        trail_short=np.asarray(trail_short, dtype=float) if trail_short is not None else empty_float,
    )


__all__ = ["build_execution_data"]
