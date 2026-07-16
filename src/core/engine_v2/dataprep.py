"""Generic data packing helpers for Backtester V2 execution."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .contracts import Signals
from .kernel import ExecutionData


_SIGNAL_NAN_CACHE: dict[int, np.ndarray] = {}


def _shared_nan_array(length: int) -> np.ndarray:
    cached = _SIGNAL_NAN_CACHE.get(length)
    if cached is None:
        cached = np.full(length, np.nan, dtype=float)
        cached.setflags(write=False)
        _SIGNAL_NAN_CACHE[length] = cached
    return cached


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


def build_signal_execution_data(
    df: pd.DataFrame,
    *,
    signals: Signals,
) -> ExecutionData:
    """Pack OHLC plus signal arrays for signal-only execution profiles."""

    length = len(df)
    empty_float = _shared_nan_array(length)
    return ExecutionData(
        timestamps=tuple(df.index),
        open=np.asarray(df["Open"].to_numpy(copy=False), dtype=float),
        high=np.asarray(df["High"].to_numpy(copy=False), dtype=float),
        low=np.asarray(df["Low"].to_numpy(copy=False), dtype=float),
        close=np.asarray(df["Close"].to_numpy(copy=False), dtype=float),
        signals=signals,
        atr=empty_float,
        rolling_low=empty_float,
        rolling_high=empty_float,
        trail_long=empty_float,
        trail_short=empty_float,
    )


__all__ = ["build_execution_data", "build_signal_execution_data"]
