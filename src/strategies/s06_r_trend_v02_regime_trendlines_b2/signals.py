"""Signal and regime helpers for the S06 R-Trend v02 Regime-TL V2 adapter.

The %R signal layer, ATR, stop anchors, and trail bands are identical to the
certified ``s06_r_trend_v02_b2`` package and are imported from it rather than
copied. This module adds the causal translation of the Pine trendline regime
filter (``S_06-R-Trend_v02_Regime-Trendlines.pine``):

- ``ta.pivothigh(len, len)`` / ``ta.pivotlow(len, len)`` confirm a pivot ``len``
  bars after the pivot bar. Ties are not pivots (strict comparison on both
  sides).
- Each confirmed pivot (re)anchors its line at the pivot price; the per-bar
  slope is frozen from ATR(14) sampled at the *confirmation* bar:
  ``slopeFactor * atr / pivotLen``.
- Per bar, in Pine execution order: project both lines from the prior bar's
  anchor state, evaluate breaks against ``close +/- breakBufferX * atr`` with
  the *current* bar's ATR, update the regime state (a direction flips only if
  the opposite side did not break on the same bar; contradictory evidence holds
  the previous regime), consume every broken line (both on a double-break bar),
  then anchor newly confirmed pivots. A pivot confirming on the flip bar arms a
  fresh line that is first evaluated on the next bar.
- ``regimeState`` is 0 only during warm-up (before the first break) and blocks
  all entries while ``useRegime`` is enabled. Gating affects entries only.
"""

from __future__ import annotations

import dataclasses
import math
from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd

from core.engine_v2.contracts import Signals
from core.engine_v2.dataprep import build_execution_data
from core.engine_v2.kernel import ExecutionData
from strategies.s06_r_trend_v02_b2.signals import (
    S06B2Params,
    build_indicator_arrays,
    normalize_parameter_aliases,
)


@dataclass(frozen=True)
class S06RegimeTLParams(S06B2Params):
    useRegime: bool = True
    regimePivotLen: int = 15
    regimeSlopeFactor: float = 1.0
    regimeBreakBufferX: float = 0.0

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any] | None) -> "S06RegimeTLParams":
        d = normalize_parameter_aliases(payload)
        base = S06B2Params.from_dict(d)
        params = cls(
            **{field.name: getattr(base, field.name) for field in dataclasses.fields(S06B2Params)},
            useRegime=cls._coerce_bool(d.get("useRegime"), cls.useRegime),
            regimePivotLen=int(float(d.get("regimePivotLen", cls.regimePivotLen))),
            regimeSlopeFactor=float(d.get("regimeSlopeFactor", cls.regimeSlopeFactor)),
            regimeBreakBufferX=float(d.get("regimeBreakBufferX", cls.regimeBreakBufferX)),
        )
        params.validate_regime()
        return params

    def validate_regime(self) -> None:
        if self.regimePivotLen <= 0:
            raise ValueError("regimePivotLen must be greater than zero.")
        if not math.isfinite(self.regimeSlopeFactor) or self.regimeSlopeFactor <= 0.0:
            raise ValueError("regimeSlopeFactor must be a finite value greater than zero.")
        if not math.isfinite(self.regimeBreakBufferX) or self.regimeBreakBufferX < 0.0:
            raise ValueError("regimeBreakBufferX must be finite and non-negative.")


def pivot_confirmations(values: np.ndarray, pivot_len: int, kind: str) -> np.ndarray:
    """Pine ``ta.pivothigh/ta.pivotlow(pivot_len, pivot_len)`` confirmation array.

    ``result[t]`` holds the pivot price confirmed at bar ``t`` (pivot bar is
    ``t - pivot_len``) or NaN. The pivot value must be strictly beyond every
    other value in the ``2 * pivot_len + 1`` window; equal extremes do not
    confirm a pivot.
    """

    if kind not in {"high", "low"}:
        raise ValueError(f"Unsupported pivot kind: {kind}")
    n = len(values)
    result = np.full(n, np.nan, dtype=float)
    window = 2 * pivot_len + 1
    if n < window:
        return result
    windows = np.lib.stride_tricks.sliding_window_view(values, window)
    center = windows[:, pivot_len]
    others = np.delete(windows, pivot_len, axis=1)
    if kind == "high":
        is_pivot = center > np.max(others, axis=1)
    else:
        is_pivot = center < np.min(others, axis=1)
    confirmation_indices = np.arange(len(windows)) + 2 * pivot_len
    result[confirmation_indices[is_pivot]] = center[is_pivot]
    return result


def regime_state_array(
    close: np.ndarray,
    atr: np.ndarray,
    pivot_high: np.ndarray,
    pivot_low: np.ndarray,
    pivot_len: int,
    slope_factor: float,
    break_buffer_x: float,
) -> np.ndarray:
    """Causal two-state regime: 1 = UP, -1 = DOWN, 0 = warm-up (no break yet)."""

    n = len(close)
    state_array = np.zeros(n, dtype=np.int8)
    state = 0
    has_res = False
    res_bar = 0
    res_price = 0.0
    res_slope = 0.0
    has_sup = False
    sup_bar = 0
    sup_price = 0.0
    sup_slope = 0.0
    for t in range(n):
        atr_value = atr[t]
        flip_up = False
        flip_down = False
        if has_res and math.isfinite(atr_value):
            res_line = res_price - res_slope * (t - res_bar)
            flip_up = close[t] > res_line + break_buffer_x * atr_value
        if has_sup and math.isfinite(atr_value):
            sup_line = sup_price + sup_slope * (t - sup_bar)
            flip_down = close[t] < sup_line - break_buffer_x * atr_value
        if flip_up and not flip_down:
            state = 1
        if flip_down and not flip_up:
            state = -1
        if flip_up:
            has_res = False
        if flip_down:
            has_sup = False
        if math.isfinite(pivot_high[t]) and math.isfinite(atr_value):
            has_res = True
            res_bar = t - pivot_len
            res_price = pivot_high[t]
            res_slope = slope_factor * atr_value / pivot_len
        if math.isfinite(pivot_low[t]) and math.isfinite(atr_value):
            has_sup = True
            sup_bar = t - pivot_len
            sup_price = pivot_low[t]
            sup_slope = slope_factor * atr_value / pivot_len
        state_array[t] = state
    return state_array


def build_regime_state(df: pd.DataFrame, params: S06RegimeTLParams, atr: np.ndarray) -> np.ndarray:
    high = df["High"].to_numpy(copy=False)
    low = df["Low"].to_numpy(copy=False)
    close = df["Close"].to_numpy(copy=False)
    pivot_high = pivot_confirmations(high, params.regimePivotLen, "high")
    pivot_low = pivot_confirmations(low, params.regimePivotLen, "low")
    return regime_state_array(
        close,
        atr,
        pivot_high,
        pivot_low,
        params.regimePivotLen,
        params.regimeSlopeFactor,
        params.regimeBreakBufferX,
    )


def build_regime_indicator_arrays(df: pd.DataFrame, params: S06RegimeTLParams) -> dict[str, np.ndarray]:
    """S06 B2 indicator arrays with regime-gated entry signals.

    ``useRegime=false`` returns the base arrays untouched (exact S06 v02
    baseline); ``useRegime=true`` masks entries with the same-bar regime state,
    matching the Pine gating (``regimeLongOk``/``regimeShortOk``) evaluated
    after the state update on each confirmed bar. Exits are unaffected.
    """

    arrays = build_indicator_arrays(df, params)
    if not params.useRegime:
        return arrays
    regime_state = build_regime_state(df, params, arrays["atr"])
    arrays = dict(arrays)
    arrays["regime_state"] = regime_state
    arrays["long_signal"] = arrays["long_signal"] & (regime_state == 1)
    arrays["short_signal"] = arrays["short_signal"] & (regime_state == -1)
    return arrays


def build_regime_tl_execution_data(df: pd.DataFrame, params: S06RegimeTLParams) -> ExecutionData:
    arrays = build_regime_indicator_arrays(df, params)
    signals = Signals(
        long_entries=arrays["long_signal"],
        short_entries=arrays["short_signal"],
    )
    return build_execution_data(
        df,
        signals=signals,
        atr=arrays["atr"],
        rolling_low=arrays["rolling_low"],
        rolling_high=arrays["rolling_high"],
        trail_long=arrays["trail_long"],
        trail_short=arrays["trail_short"],
    )


__all__ = [
    "S06RegimeTLParams",
    "build_regime_indicator_arrays",
    "build_regime_state",
    "build_regime_tl_execution_data",
    "normalize_parameter_aliases",
    "pivot_confirmations",
    "regime_state_array",
]
