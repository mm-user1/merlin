"""Signal and dataprep helpers for the S06 Backtester V2 adapter."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import numpy as np
import pandas as pd

from core.engine_v2.contracts import Signals
from core.engine_v2.dataprep import build_execution_data
from core.engine_v2.kernel import ExecutionData
from indicators.williams import williams_r


ENTRY_MODES = {"Reversal @ Triangle", "Trend @ Square"}
TRAIL_MA_TYPES = {"SMA", "HMA", "KAMA", "T3"}


@dataclass(frozen=True)
class S06B2Params:
    dateFilter: bool = True
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    entryMode: str = "Reversal @ Triangle"
    enableLong: bool = True
    enableShort: bool = True
    fastLength: int = 21
    fastSmooth: int = 7
    slowLength: int = 112
    slowSmooth: int = 3
    thresholdOS: int = 20
    thresholdOB: int = 20
    stopX: float = 2.0
    stopRR: float = 3.0
    stopLP: int = 2
    stopMaxPct: float = 4.0
    stopMaxDays: int = 4
    riskPerTrade: float = 2.0
    contractSize: float = 0.01
    useTrailMA: bool = True
    trailRR: float = 1.0
    trailMAType: str = "SMA"
    trailMALength: int = 150
    trailMAOffsetEx: float = 0.0
    initialCapital: float = 100.0
    commissionPct: float = 0.05
    warmupBars: int = 1000

    @staticmethod
    def _coerce_bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "y", "on"}:
                return True
            if normalized in {"false", "0", "no", "n", "off"}:
                return False
        return default

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[pd.Timestamp]:
        if value in (None, ""):
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any] | None) -> "S06B2Params":
        d = normalize_parameter_aliases(payload)
        params = cls(
            dateFilter=cls._coerce_bool(d.get("dateFilter"), cls.dateFilter),
            start=cls._parse_timestamp(d.get("start")),
            end=cls._parse_timestamp(d.get("end")),
            entryMode=str(d.get("entryMode", cls.entryMode)),
            enableLong=cls._coerce_bool(d.get("enableLong"), cls.enableLong),
            enableShort=cls._coerce_bool(d.get("enableShort"), cls.enableShort),
            fastLength=int(d.get("fastLength", cls.fastLength)),
            fastSmooth=int(d.get("fastSmooth", cls.fastSmooth)),
            slowLength=int(d.get("slowLength", cls.slowLength)),
            slowSmooth=int(d.get("slowSmooth", cls.slowSmooth)),
            thresholdOS=int(d.get("thresholdOS", cls.thresholdOS)),
            thresholdOB=int(d.get("thresholdOB", cls.thresholdOB)),
            stopX=float(d.get("stopX", cls.stopX)),
            stopRR=float(d.get("stopRR", cls.stopRR)),
            stopLP=int(float(d.get("stopLP", cls.stopLP))),
            stopMaxPct=float(d.get("stopMaxPct", cls.stopMaxPct)),
            stopMaxDays=int(d.get("stopMaxDays", cls.stopMaxDays)),
            riskPerTrade=float(d.get("riskPerTrade", cls.riskPerTrade)),
            contractSize=float(d.get("contractSize", cls.contractSize)),
            useTrailMA=cls._coerce_bool(d.get("useTrailMA"), cls.useTrailMA),
            trailRR=float(d.get("trailRR", cls.trailRR)),
            trailMAType=str(d.get("trailMAType", cls.trailMAType)).upper(),
            trailMALength=int(d.get("trailMALength", cls.trailMALength)),
            trailMAOffsetEx=float(d.get("trailMAOffsetEx", cls.trailMAOffsetEx)),
            initialCapital=float(d.get("initialCapital", cls.initialCapital)),
            commissionPct=float(d.get("commissionPct", cls.commissionPct)),
            warmupBars=int(d.get("warmupBars", cls.warmupBars)),
        )
        params.validate()
        return params

    def validate(self) -> None:
        if self.entryMode not in ENTRY_MODES:
            raise ValueError(f"Invalid entryMode '{self.entryMode}'.")
        if self.trailMAType not in TRAIL_MA_TYPES:
            raise ValueError(f"Invalid trailMAType '{self.trailMAType}'.")
        for name in ("fastLength", "fastSmooth", "slowLength", "slowSmooth", "stopLP"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be greater than zero.")
        if self.trailMALength <= 0 or self.warmupBars < 0:
            raise ValueError("trailMALength must be positive and warmupBars must be non-negative.")
        if not 1 <= self.thresholdOS <= 50 or not 1 <= self.thresholdOB <= 50:
            raise ValueError("thresholdOS and thresholdOB must be between 1 and 50.")
        for name in ("stopX", "stopRR", "stopMaxPct", "stopMaxDays", "riskPerTrade", "contractSize", "trailRR"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"{name} must be a finite value greater than zero.")
        if self.trailMAOffsetEx < 0.0 or not math.isfinite(self.trailMAOffsetEx):
            raise ValueError("trailMAOffsetEx must be finite and non-negative.")
        if self.initialCapital <= 0.0 or not math.isfinite(self.initialCapital):
            raise ValueError("initialCapital must be a finite value greater than zero.")
        if self.commissionPct < 0.0 or not math.isfinite(self.commissionPct):
            raise ValueError("commissionPct must be finite and non-negative.")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("start must not be after end.")


@dataclass(frozen=True)
class SignalEvents:
    overbought: np.ndarray
    oversold: np.ndarray
    ob_trend_start: np.ndarray
    os_trend_start: np.ndarray
    ob_reversal: np.ndarray
    os_reversal: np.ndarray
    long_signal: np.ndarray
    short_signal: np.ndarray


def normalize_parameter_aliases(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normalize baseline/V1-compatible aliases into B2 config names."""

    result = dict(payload or {})
    if "fastSmooth" not in result and "fastSmoothing" in result:
        result["fastSmooth"] = result["fastSmoothing"]
    if "slowSmooth" not in result and "slowSmoothing" in result:
        result["slowSmooth"] = result["slowSmoothing"]
    if "trailMAOffsetEx" not in result and "trailMAOffsetPct" in result:
        result["trailMAOffsetEx"] = result["trailMAOffsetPct"]
    if "stopLP" in result:
        result["stopLP"] = int(float(result["stopLP"]))
    return result


def pine_ema(values: np.ndarray, length: int) -> np.ndarray:
    result = np.full(len(values), np.nan, dtype=float)
    alpha = 2.0 / (length + 1.0)
    previous = math.nan
    for i, raw in enumerate(values):
        value = float(raw)
        if not math.isfinite(value):
            if math.isfinite(previous):
                result[i] = previous
            continue
        previous = value if not math.isfinite(previous) else alpha * value + (1.0 - alpha) * previous
        result[i] = previous
    return result


def pine_rma(values: np.ndarray, length: int) -> np.ndarray:
    result = np.full(len(values), np.nan, dtype=float)
    seed: list[float] = []
    previous = math.nan
    for i, raw in enumerate(values):
        value = float(raw)
        if not math.isfinite(value):
            continue
        if not math.isfinite(previous):
            seed.append(value)
            if len(seed) == length:
                previous = float(sum(seed) / length)
                result[i] = previous
            continue
        previous = (previous * (length - 1) + value) / length
        result[i] = previous
    return result


def pine_wma(series: pd.Series, length: int) -> pd.Series:
    weights = np.arange(1, length + 1, dtype=float)
    return series.rolling(length, min_periods=length).apply(
        lambda values: float(np.dot(values, weights) / weights.sum()),
        raw=True,
    )


def pine_kama(series: pd.Series, length: int) -> pd.Series:
    values = series.to_numpy(copy=False)
    momentum = series.diff(length).abs().to_numpy(copy=False)
    volatility = series.diff().abs().rolling(length, min_periods=length).sum().to_numpy(copy=False)
    result = np.full(len(series), np.nan, dtype=float)
    fast_alpha = 2.0 / 3.0
    slow_alpha = 2.0 / 31.0
    previous = math.nan
    for i, price_raw in enumerate(values):
        price = float(price_raw)
        if not math.isfinite(price) or not math.isfinite(momentum[i]) or not math.isfinite(volatility[i]):
            continue
        efficiency_ratio = momentum[i] / volatility[i] if volatility[i] != 0.0 else 0.0
        alpha = (efficiency_ratio * (fast_alpha - slow_alpha) + slow_alpha) ** 2
        prior = previous if math.isfinite(previous) else price
        previous = alpha * price + (1.0 - alpha) * prior
        result[i] = previous
    return pd.Series(result, index=series.index)


def trail_ma(series: pd.Series, ma_type: str, length: int) -> pd.Series:
    if ma_type == "SMA":
        return series.rolling(length, min_periods=length).mean()
    if ma_type == "HMA":
        half_length = max(1, length // 2)
        sqrt_length = max(1, int(round(math.sqrt(length))))
        return pine_wma(2.0 * pine_wma(series, half_length) - pine_wma(series, length), sqrt_length)
    if ma_type == "KAMA":
        return pine_kama(series, length)
    if ma_type == "T3":
        values = series.to_numpy(copy=False)

        def gd(source: np.ndarray) -> np.ndarray:
            first = pine_ema(source, length)
            second = pine_ema(first, length)
            return first * 1.7 - second * 0.7

        return pd.Series(gd(gd(gd(values))), index=series.index)
    raise ValueError(f"Unsupported trail MA type: {ma_type}")


def signal_events(
    fast_percent_r: np.ndarray,
    slow_percent_r: np.ndarray,
    threshold_os: int,
    threshold_ob: int,
    entry_mode: str,
) -> SignalEvents:
    valid = np.isfinite(fast_percent_r) & np.isfinite(slow_percent_r)
    overbought = valid & (fast_percent_r >= -threshold_ob) & (slow_percent_r >= -threshold_ob)
    oversold = valid & (fast_percent_r <= -100 + threshold_os) & (slow_percent_r <= -100 + threshold_os)
    previous_valid = np.zeros(len(valid), dtype=bool)
    previous_valid[1:] = valid[:-1]
    previous_ob = np.zeros(len(valid), dtype=bool)
    previous_os = np.zeros(len(valid), dtype=bool)
    previous_ob[1:] = overbought[:-1]
    previous_os[1:] = oversold[:-1]

    ob_trend_start = previous_valid & overbought & ~previous_ob
    os_trend_start = previous_valid & oversold & ~previous_os
    ob_reversal = previous_valid & ~overbought & previous_ob
    os_reversal = previous_valid & ~oversold & previous_os
    if entry_mode == "Reversal @ Triangle":
        long_signal = os_reversal
        short_signal = ob_reversal
    elif entry_mode == "Trend @ Square":
        long_signal = ob_trend_start
        short_signal = os_trend_start
    else:
        raise ValueError(f"Unsupported entry mode: {entry_mode}")
    return SignalEvents(
        overbought=overbought,
        oversold=oversold,
        ob_trend_start=ob_trend_start,
        os_trend_start=os_trend_start,
        ob_reversal=ob_reversal,
        os_reversal=os_reversal,
        long_signal=long_signal,
        short_signal=short_signal,
    )


def build_indicator_arrays(df: pd.DataFrame, params: S06B2Params) -> dict[str, np.ndarray]:
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    high_values = high.to_numpy(copy=False)
    low_values = low.to_numpy(copy=False)
    close_values = close.to_numpy(copy=False)

    fast_percent_r = pine_ema(
        williams_r(high, low, close, params.fastLength).to_numpy(copy=False),
        params.fastSmooth,
    )
    slow_percent_r = pine_ema(
        williams_r(high, low, close, params.slowLength).to_numpy(copy=False),
        params.slowSmooth,
    )
    signals = signal_events(
        fast_percent_r,
        slow_percent_r,
        params.thresholdOS,
        params.thresholdOB,
        params.entryMode,
    )

    previous_close = np.roll(close_values, 1)
    if len(previous_close):
        previous_close[0] = np.nan
    true_range = np.maximum.reduce(
        [
            np.abs(high_values - low_values),
            np.abs(high_values - previous_close),
            np.abs(low_values - previous_close),
        ]
    )
    if len(true_range):
        true_range[0] = abs(high_values[0] - low_values[0])
    atr_values = pine_rma(true_range, 14)
    lowest = low.rolling(params.stopLP, min_periods=params.stopLP).min().to_numpy(copy=False)
    highest = high.rolling(params.stopLP, min_periods=params.stopLP).max().to_numpy(copy=False)

    ma_values = trail_ma(close, params.trailMAType, params.trailMALength).to_numpy(copy=False)
    offset_pct = 1.0 + params.trailMAOffsetEx
    trail_long = ma_values * (1.0 - offset_pct / 100.0)
    trail_short = ma_values * (1.0 + offset_pct / 100.0)
    return {
        "atr": atr_values,
        "rolling_low": lowest,
        "rolling_high": highest,
        "trail_long": trail_long,
        "trail_short": trail_short,
        "long_signal": signals.long_signal,
        "short_signal": signals.short_signal,
    }


def build_s06_b2_execution_data(df: pd.DataFrame, params: S06B2Params) -> ExecutionData:
    arrays = build_indicator_arrays(df, params)
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
    "ENTRY_MODES",
    "TRAIL_MA_TYPES",
    "S06B2Params",
    "SignalEvents",
    "build_indicator_arrays",
    "build_s06_b2_execution_data",
    "normalize_parameter_aliases",
    "pine_ema",
    "pine_rma",
    "signal_events",
    "trail_ma",
]
