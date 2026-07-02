"""Williams %R indicator helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd


def williams_r(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    length: int,
) -> pd.Series:
    """TradingView-style Williams %R scaled from -100 to 0.

    Values are ``NaN`` until ``length`` bars are available, and remain ``NaN``
    when the high/low range is zero.
    """

    if not (len(high) == len(low) == len(close)):
        raise ValueError("high, low, and close must have equal length.")
    normalized_length = int(length)
    if normalized_length <= 0:
        raise ValueError("length must be greater than zero.")
    highest = high.rolling(normalized_length, min_periods=normalized_length).max().to_numpy(copy=False)
    lowest = low.rolling(normalized_length, min_periods=normalized_length).min().to_numpy(copy=False)
    close_values = close.to_numpy(copy=False)
    denominator = highest - lowest
    values = np.full(len(close), np.nan, dtype=float)
    valid = np.isfinite(denominator) & (denominator != 0.0) & np.isfinite(close_values)
    values[valid] = 100.0 * (close_values[valid] - highest[valid]) / denominator[valid]
    return pd.Series(values, index=close.index)


__all__ = ["williams_r"]
