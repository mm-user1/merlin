"""Signal helpers for the S03 Reversal v11 Regime-ER Backtester V2 adapter."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import numpy as np
import pandas as pd

from core.engine_v2.contracts import Signals
from core.engine_v2.dataprep import build_signal_execution_data
from core.engine_v2.kernel import ExecutionData
from indicators.ma import VALID_MA_TYPES, get_ma


@dataclass(frozen=True)
class S03RegimeERParams:
    dateFilter: bool = True
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    maType3: str = "SMA"
    maLength3: int = 75
    maOffset3: float = 0.2
    useCloseCount: bool = True
    closeCountLong: int = 7
    closeCountShort: int = 5
    useTBands: bool = True
    tBandLongPct: float = 1.0
    tBandShortPct: float = 1.3
    useRegime: bool = True
    regimeErLength: int = 20
    regimeErThresh: float = 0.3
    initialCapital: float = 100.0
    commissionPct: float = 0.05
    positionPct: float = 100.0
    contractSize: float = 0.01
    enableLong: bool = True
    enableShort: bool = True
    useEmergencySL: bool = False
    emergencySlPct: float = 20.0
    emergencySlUpdateBars: int = 16
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
    def from_dict(cls, payload: Mapping[str, Any] | None) -> "S03RegimeERParams":
        d = normalize_parameter_aliases(payload)
        params = cls(
            dateFilter=cls._coerce_bool(d.get("dateFilter"), cls.dateFilter),
            start=cls._parse_timestamp(d.get("start")),
            end=cls._parse_timestamp(d.get("end")),
            maType3=str(d.get("maType3", cls.maType3)).upper(),
            maLength3=int(float(d.get("maLength3", cls.maLength3))),
            maOffset3=float(d.get("maOffset3", cls.maOffset3)),
            useCloseCount=cls._coerce_bool(d.get("useCloseCount"), cls.useCloseCount),
            closeCountLong=int(float(d.get("closeCountLong", cls.closeCountLong))),
            closeCountShort=int(float(d.get("closeCountShort", cls.closeCountShort))),
            useTBands=cls._coerce_bool(d.get("useTBands"), cls.useTBands),
            tBandLongPct=float(d.get("tBandLongPct", cls.tBandLongPct)),
            tBandShortPct=float(d.get("tBandShortPct", cls.tBandShortPct)),
            useRegime=cls._coerce_bool(d.get("useRegime"), cls.useRegime),
            regimeErLength=int(float(d.get("regimeErLength", cls.regimeErLength))),
            regimeErThresh=float(d.get("regimeErThresh", cls.regimeErThresh)),
            initialCapital=float(d.get("initialCapital", cls.initialCapital)),
            commissionPct=float(d.get("commissionPct", cls.commissionPct)),
            positionPct=float(d.get("positionPct", cls.positionPct)),
            contractSize=float(d.get("contractSize", cls.contractSize)),
            enableLong=cls._coerce_bool(d.get("enableLong"), cls.enableLong),
            enableShort=cls._coerce_bool(d.get("enableShort"), cls.enableShort),
            useEmergencySL=cls._coerce_bool(d.get("useEmergencySL"), cls.useEmergencySL),
            emergencySlPct=float(d.get("emergencySlPct", cls.emergencySlPct)),
            emergencySlUpdateBars=int(float(d.get("emergencySlUpdateBars", cls.emergencySlUpdateBars))),
            warmupBars=int(float(d.get("warmupBars", cls.warmupBars))),
        )
        params.validate()
        return params

    def validate(self) -> None:
        if self.maType3 not in VALID_MA_TYPES:
            raise ValueError(f"Unsupported maType3: {self.maType3}")
        if self.maLength3 < 0:
            raise ValueError("maLength3 must be non-negative.")
        if self.closeCountLong < 1 or self.closeCountShort < 1:
            raise ValueError("closeCountLong and closeCountShort must be positive.")
        if self.tBandLongPct <= 0.0 or self.tBandShortPct <= 0.0:
            raise ValueError("tBandLongPct and tBandShortPct must be positive.")
        if self.regimeErLength < 2:
            raise ValueError("regimeErLength must be at least 2.")
        if not math.isfinite(self.regimeErThresh) or self.regimeErThresh <= 0.0:
            raise ValueError("regimeErThresh must be a finite value greater than zero.")
        for name in ("initialCapital", "positionPct", "contractSize"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"{name} must be a finite value greater than zero.")
        if not math.isfinite(self.commissionPct) or self.commissionPct < 0.0:
            raise ValueError("commissionPct must be finite and non-negative.")
        if self.useEmergencySL:
            if not math.isfinite(self.emergencySlPct) or self.emergencySlPct <= 0.0:
                raise ValueError("emergencySlPct must be > 0 when useEmergencySL=true.")
            if self.emergencySlUpdateBars < 1:
                raise ValueError("emergencySlUpdateBars must be >= 1 when useEmergencySL=true.")
        if self.warmupBars < 0:
            raise ValueError("warmupBars must be non-negative.")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("start must not be after end.")


def normalize_parameter_aliases(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normalize baseline/UI aliases into the B2 config names."""

    result = dict(payload or {})
    if "dateFilter" not in result and "useDateFilter" in result:
        result["dateFilter"] = result["useDateFilter"]
    if "start" not in result and "startDate" in result:
        result["start"] = result["startDate"]
    if "end" not in result and "endDate" in result:
        result["end"] = result["endDate"]
    return result


def regime_er_state(
    close: np.ndarray,
    length: int,
    threshold: float,
) -> dict[str, np.ndarray]:
    """Causal Kaufman ER regime state: 1=UP, -1=DOWN, 0=FLAT."""

    n = len(close)
    state_values = np.zeros(n, dtype=np.int8)
    er_values = np.zeros(n, dtype=float)
    net_values = np.full(n, np.nan, dtype=float)
    path_values = np.full(n, np.nan, dtype=float)
    change_abs = np.zeros(n, dtype=float)
    if n > 1:
        change_abs[1:] = np.abs(np.diff(close.astype(float, copy=False)))

    exit_threshold = threshold * 0.5
    state = 0
    for i in range(n):
        if i >= length:
            net = float(close[i] - close[i - length])
            path = float(np.sum(change_abs[i - length + 1 : i + 1]))
            er = abs(net) / path if path != 0.0 else 0.0
            net_values[i] = net
            path_values[i] = path
            er_values[i] = er
            if er > threshold:
                state = 1 if net > 0.0 else -1
            elif er < exit_threshold:
                state = 0
        else:
            er_values[i] = 0.0
            state = 0
        state_values[i] = state
    return {
        "regime_er": er_values,
        "regime_er_net": net_values,
        "regime_er_path": path_values,
        "regime_state": state_values,
    }


def _moving_average(df: pd.DataFrame, params: S03RegimeERParams) -> pd.Series:
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"] if "Volume" in df else None
    ma3 = get_ma(close, params.maType3, params.maLength3, volume, high, low)
    if params.maOffset3 != 0.0:
        ma3 = ma3 * (1.0 + params.maOffset3 / 100.0)
    return ma3


def build_signal_state_arrays(
    df: pd.DataFrame,
    params: Mapping[str, Any] | S03RegimeERParams,
) -> dict[str, np.ndarray]:
    """Build S03 base signals, Regime-ER state, and gated V2 signal arrays."""

    parsed = params if isinstance(params, S03RegimeERParams) else S03RegimeERParams.from_dict(params)
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    ma3 = _moving_average(df, parsed)
    ma3_up_band = ma3 * (1.0 + parsed.tBandLongPct / 100.0)
    ma3_down_band = ma3 * (1.0 - parsed.tBandShortPct / 100.0)

    close_values = close.to_numpy(copy=False)
    high_values = high.to_numpy(copy=False)
    low_values = low.to_numpy(copy=False)
    ma3_values = ma3.to_numpy(copy=False)
    up_values = ma3_up_band.to_numpy(copy=False)
    down_values = ma3_down_band.to_numpy(copy=False)

    n = len(df)
    base_long = np.zeros(n, dtype=np.bool_)
    base_short = np.zeros(n, dtype=np.bool_)
    t_band_state_values = np.zeros(n, dtype=np.int8)
    count_long_values = np.zeros(n, dtype=np.int32)
    count_short_values = np.zeros(n, dtype=np.int32)
    t_band_state = 0
    count_close_long = 0
    count_close_short = 0
    trading_disabled = not (parsed.useCloseCount or parsed.useTBands)

    for i in range(n):
        close_val = float(close_values[i])
        high_val = float(high_values[i])
        low_val = float(low_values[i])
        ma_val = float(ma3_values[i])
        up_band = float(up_values[i])
        down_band = float(down_values[i])

        break_up = False
        break_down = False
        cross_fail = False
        if not np.isnan(up_band) and not np.isnan(down_band):
            break_up = (high_val > up_band) and (close_val > up_band)
            break_down = (low_val < down_band) and (close_val < down_band)
            cross_fail = (high_val >= up_band) and (low_val <= down_band)

        if cross_fail:
            if not np.isnan(ma_val):
                t_band_state = 1 if close_val > ma_val else -1
        else:
            if break_up:
                t_band_state = 1
            elif break_down:
                t_band_state = -1

        if not np.isnan(ma_val):
            if close_val > ma_val:
                count_close_long += 1
                count_close_short = 0
            elif close_val < ma_val:
                count_close_short += 1
                count_close_long = 0
            else:
                count_close_long = 0
                count_close_short = 0
        else:
            count_close_long = 0
            count_close_short = 0

        count_long = True if not parsed.useCloseCount else count_close_long >= parsed.closeCountLong
        count_short = True if not parsed.useCloseCount else count_close_short >= parsed.closeCountShort
        cross_tband_long = True if not parsed.useTBands else t_band_state == 1
        cross_tband_short = True if not parsed.useTBands else t_band_state == -1
        base_long[i] = (not trading_disabled) and count_long and cross_tband_long
        base_short[i] = (not trading_disabled) and count_short and cross_tband_short
        t_band_state_values[i] = t_band_state
        count_long_values[i] = count_close_long
        count_short_values[i] = count_close_short

    regime_arrays = regime_er_state(
        np.asarray(close_values, dtype=float),
        parsed.regimeErLength,
        parsed.regimeErThresh,
    )
    regime_state = regime_arrays["regime_state"]
    if parsed.useRegime:
        long_entries = base_long & (regime_state == 1)
        short_entries = base_short & (regime_state == -1)
        flat_exits = regime_state == 0
    else:
        long_entries = base_long.copy()
        short_entries = base_short.copy()
        flat_exits = np.zeros(n, dtype=np.bool_)

    return {
        "ma3": np.asarray(ma3_values, dtype=float),
        "ma3_up_band": np.asarray(up_values, dtype=float),
        "ma3_down_band": np.asarray(down_values, dtype=float),
        "t_band_state": t_band_state_values,
        "count_close_long": count_long_values,
        "count_close_short": count_short_values,
        "base_long": base_long,
        "base_short": base_short,
        "regime_state": regime_state,
        "regime_er": regime_arrays["regime_er"],
        "regime_er_net": regime_arrays["regime_er_net"],
        "regime_er_path": regime_arrays["regime_er_path"],
        "long_entries": np.asarray(long_entries, dtype=np.bool_),
        "short_entries": np.asarray(short_entries, dtype=np.bool_),
        "long_exits": np.asarray(flat_exits, dtype=np.bool_),
        "short_exits": np.asarray(flat_exits, dtype=np.bool_),
    }


def build_s03_regime_er_signals(
    df: pd.DataFrame,
    params: Mapping[str, Any] | S03RegimeERParams,
) -> Signals:
    arrays = build_signal_state_arrays(df, params)
    return Signals(
        long_entries=arrays["long_entries"],
        short_entries=arrays["short_entries"],
        long_exits=arrays["long_exits"],
        short_exits=arrays["short_exits"],
    )


def build_s03_regime_er_execution_data(
    df: pd.DataFrame,
    params: Mapping[str, Any] | S03RegimeERParams,
) -> ExecutionData:
    return build_signal_execution_data(
        df,
        signals=build_s03_regime_er_signals(df, params),
    )


__all__ = [
    "S03RegimeERParams",
    "build_s03_regime_er_execution_data",
    "build_s03_regime_er_signals",
    "build_signal_state_arrays",
    "normalize_parameter_aliases",
    "regime_er_state",
]

