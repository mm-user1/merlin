"""Causal HA runs and deterministic ATR14; ordinary OHLC owns execution."""
from dataclasses import dataclass

import numpy as np

from core.engine_v2.contracts import Signals
from core.engine_v2.dataprep import build_execution_data


@dataclass(frozen=True)
class Indicators:
    ha_open: np.ndarray
    ha_close: np.ndarray
    colour: np.ndarray
    run: np.ndarray
    atr: np.ndarray


def indicators(frame):
    opening, high, low, close = (frame[c].to_numpy(dtype=np.float64) for c in ("Open", "High", "Low", "Close"))
    size = len(frame)
    ha_close = (opening + high + low + close) / 4.0
    ha_open = np.empty(size, dtype=np.float64)
    colour = np.zeros(size, dtype=np.int8)
    run = np.zeros(size, dtype=np.int64)
    atr = np.full(size, np.nan, dtype=np.float64)
    seed = 0.0
    for i in range(size):
        ha_open[i] = (opening[i] + close[i]) / 2.0 if i == 0 else (ha_open[i-1] + ha_close[i-1]) / 2.0
        colour[i] = 1 if ha_close[i] > ha_open[i] else -1 if ha_close[i] < ha_open[i] else 0
        run[i] = (run[i-1] + 1 if i and colour[i] == colour[i-1] else 1) if colour[i] else 0
        tr = high[i] - low[i] if i == 0 else max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
        if i < 14:
            seed += float(tr)  # Fixed left-to-right float64 fold, independent of builtin sum versions.
            if i == 13:
                atr[i] = seed / 14.0
        else:
            atr[i] = (atr[i-1] * 13.0 + tr) / 14.0
    return Indicators(ha_open, ha_close, colour, run, atr)


def execution_data(frame, params, values, extrema):
    low, high = extrema
    eligible = (np.arange(len(frame)) >= 64) & np.isfinite(values.atr) & np.isfinite(low) & np.isfinite(high)
    event = eligible & (values.run == params["entryRunLength"])
    signals = Signals(event & (values.colour > 0) & params["enableLong"],
                      event & (values.colour < 0) & params["enableShort"])
    return build_execution_data(frame, signals=signals, atr=values.atr, rolling_low=low, rolling_high=high)


def rolling_extrema(frame, length):
    return (frame.Low.astype(float).rolling(length).min().to_numpy(),
            frame.High.astype(float).rolling(length).max().to_numpy())
