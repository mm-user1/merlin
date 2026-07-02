"""
Indicators package for S01 Trailing MA v26.

This package provides technical indicators used by trading strategies:
- Moving Averages (11 types)
- Volatility indicators (ATR)
- Trend indicators (placeholder for future)
- Oscillators (placeholder for future)

All indicators are pure functions that operate on pandas Series/DataFrames.
"""

# Moving Averages
from .ma import (
    sma,
    ema,
    wma,
    hma,
    vwma,
    vwap,
    alma,
    dema,
    kama,
    tma,
    t3,
    get_ma,
    VALID_MA_TYPES,
)

# Volatility
from .volatility import atr

# Oscillators
from .oscillators import rsi, stoch_rsi

# Williams
from .williams import williams_r

__all__ = [
    # Moving Averages
    "sma",
    "ema",
    "wma",
    "hma",
    "vwma",
    "vwap",
    "alma",
    "dema",
    "kama",
    "tma",
    "t3",
    "get_ma",
    "VALID_MA_TYPES",
    # Volatility
    "atr",
    # Oscillators
    "rsi",
    "stoch_rsi",
    # Williams
    "williams_r",
]
