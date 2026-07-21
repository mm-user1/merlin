"""Signal helpers for the S03 Reversal v11 Regime-ER Backtester V2 adapter."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from core.engine_v2.contracts import Signals
from core.engine_v2.dataprep import build_signal_execution_data
from core.engine_v2.kernel import ExecutionData
from indicators.ma import VALID_MA_TYPES, get_ma

try:  # pragma: no cover - availability is environment-dependent
    import numba
except Exception as exc:  # pragma: no cover - availability is environment-dependent
    numba = None
    NUMBA_IMPORT_ERROR = str(exc)
else:  # pragma: no cover - availability is environment-dependent
    NUMBA_IMPORT_ERROR = None

NUMBA_AVAILABLE = numba is not None


def _compiled_target(func):
    if numba is None:
        return func
    return numba.njit(cache=True)(func)


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


def _regime_er_state_reference(
    close: np.ndarray,
    length: int,
    threshold: float,
) -> dict[str, np.ndarray]:
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


@_compiled_target
def _regime_er_loop_impl(
    close_values: np.ndarray,
    length: int,
    threshold: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    n = len(close_values)
    state_values = np.zeros(n, dtype=np.int8)
    er_values = np.zeros(n, dtype=np.float64)
    net_values = np.empty(n, dtype=np.float64)
    path_values = np.empty(n, dtype=np.float64)
    change_abs = np.zeros(n, dtype=np.float64)
    for i in range(n):
        net_values[i] = np.nan
        path_values[i] = np.nan
    if n > 1:
        for i in range(1, n):
            change_abs[i] = abs(close_values[i] - close_values[i - 1])

    exit_threshold = threshold * 0.5
    state = 0
    for i in range(n):
        if i >= length:
            net = close_values[i] - close_values[i - length]
            path = 0.0
            for j in range(i - length + 1, i + 1):
                path += change_abs[j]
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
    return er_values, net_values, path_values, state_values


def regime_er_state(
    close: np.ndarray,
    length: int,
    threshold: float,
) -> dict[str, np.ndarray]:
    """Causal Kaufman ER regime state: 1=UP, -1=DOWN, 0=FLAT."""

    close_values = np.asarray(close, dtype=float)
    er_values, net_values, path_values, state_values = _regime_er_loop_impl(
        close_values,
        int(length),
        float(threshold),
    )
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


def _moving_average_values(
    df: pd.DataFrame,
    params: S03RegimeERParams,
    ma_cache: dict[tuple[Any, ...], np.ndarray] | None = None,
) -> np.ndarray:
    key = (params.maType3, int(params.maLength3), float(params.maOffset3))
    if ma_cache is not None and key in ma_cache:
        return ma_cache[key]
    values = np.asarray(_moving_average(df, params).to_numpy(copy=False), dtype=float)
    if ma_cache is not None:
        ma_cache[key] = values
    return values


@_compiled_target
def _count_close_loop_impl(
    close_values: np.ndarray,
    ma_values: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    n = len(close_values)
    count_long_values = np.zeros(n, dtype=np.int32)
    count_short_values = np.zeros(n, dtype=np.int32)
    count_close_long = 0
    count_close_short = 0
    for i in range(n):
        close_val = float(close_values[i])
        ma_val = float(ma_values[i])
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
        count_long_values[i] = count_close_long
        count_short_values[i] = count_close_short
    return count_long_values, count_short_values


@_compiled_target
def _t_band_state_loop_impl(
    close_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    ma_values: np.ndarray,
    long_pct: float,
    short_pct: float,
) -> np.ndarray:
    n = len(close_values)
    t_band_state_values = np.zeros(n, dtype=np.int8)
    t_band_state = 0
    up_multiplier = 1.0 + long_pct / 100.0
    down_multiplier = 1.0 - short_pct / 100.0
    for i in range(n):
        close_val = float(close_values[i])
        high_val = float(high_values[i])
        low_val = float(low_values[i])
        ma_val = float(ma_values[i])
        up_band = ma_val * up_multiplier
        down_band = ma_val * down_multiplier

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
        t_band_state_values[i] = t_band_state
    return t_band_state_values


def _signals_from_arrays(arrays: Mapping[str, np.ndarray]) -> Signals:
    return Signals(
        long_entries=arrays["long_entries"],
        short_entries=arrays["short_entries"],
        long_exits=arrays["long_exits"],
        short_exits=arrays["short_exits"],
    )


def build_signal_state_arrays_reference(
    df: pd.DataFrame,
    params: Mapping[str, Any] | S03RegimeERParams,
) -> dict[str, np.ndarray]:
    """Reference S03 signal/state implementation kept for parity tests."""

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

    regime_arrays = _regime_er_state_reference(
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


def _build_signal_state_arrays_optimized(
    df: pd.DataFrame,
    parsed: S03RegimeERParams,
    *,
    ma_cache: dict[tuple[Any, ...], np.ndarray] | None = None,
    count_cache: dict[tuple[Any, ...], tuple[np.ndarray, np.ndarray]] | None = None,
    tband_cache: dict[tuple[Any, ...], np.ndarray] | None = None,
    regime_cache: dict[tuple[Any, ...], dict[str, np.ndarray]] | None = None,
) -> dict[str, np.ndarray]:
    close_values = np.asarray(df["Close"].to_numpy(copy=False), dtype=float)
    high_values = np.asarray(df["High"].to_numpy(copy=False), dtype=float)
    low_values = np.asarray(df["Low"].to_numpy(copy=False), dtype=float)
    ma_key = (parsed.maType3, int(parsed.maLength3), float(parsed.maOffset3))
    ma3_values = _moving_average_values(df, parsed, ma_cache)
    up_values = ma3_values * (1.0 + parsed.tBandLongPct / 100.0)
    down_values = ma3_values * (1.0 - parsed.tBandShortPct / 100.0)

    count_key = ma_key
    count_arrays = count_cache.get(count_key) if count_cache is not None else None
    if count_arrays is None:
        count_arrays = _count_close_loop_impl(close_values, ma3_values)
        if count_cache is not None:
            count_cache[count_key] = count_arrays
    count_long_values, count_short_values = count_arrays

    tband_key = (ma_key, float(parsed.tBandLongPct), float(parsed.tBandShortPct))
    t_band_state_values = tband_cache.get(tband_key) if tband_cache is not None else None
    if t_band_state_values is None:
        t_band_state_values = _t_band_state_loop_impl(
            close_values,
            high_values,
            low_values,
            ma3_values,
            float(parsed.tBandLongPct),
            float(parsed.tBandShortPct),
        )
        if tband_cache is not None:
            tband_cache[tband_key] = t_band_state_values

    n = len(df)
    trading_disabled = not (parsed.useCloseCount or parsed.useTBands)
    if trading_disabled:
        base_long = np.zeros(n, dtype=np.bool_)
        base_short = np.zeros(n, dtype=np.bool_)
    else:
        count_long_ok = (
            count_long_values >= int(parsed.closeCountLong)
            if parsed.useCloseCount
            else np.ones(n, dtype=np.bool_)
        )
        count_short_ok = (
            count_short_values >= int(parsed.closeCountShort)
            if parsed.useCloseCount
            else np.ones(n, dtype=np.bool_)
        )
        tband_long_ok = (
            t_band_state_values == 1
            if parsed.useTBands
            else np.ones(n, dtype=np.bool_)
        )
        tband_short_ok = (
            t_band_state_values == -1
            if parsed.useTBands
            else np.ones(n, dtype=np.bool_)
        )
        base_long = np.asarray(count_long_ok & tband_long_ok, dtype=np.bool_)
        base_short = np.asarray(count_short_ok & tband_short_ok, dtype=np.bool_)

    regime_key = (int(parsed.regimeErLength), float(parsed.regimeErThresh))
    regime_arrays = regime_cache.get(regime_key) if regime_cache is not None else None
    if regime_arrays is None:
        regime_arrays = regime_er_state(close_values, parsed.regimeErLength, parsed.regimeErThresh)
        if regime_cache is not None:
            regime_cache[regime_key] = regime_arrays
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


def build_signal_state_arrays(
    df: pd.DataFrame,
    params: Mapping[str, Any] | S03RegimeERParams,
) -> dict[str, np.ndarray]:
    """Build S03 base signals, Regime-ER state, and gated V2 signal arrays."""

    parsed = params if isinstance(params, S03RegimeERParams) else S03RegimeERParams.from_dict(params)
    return _build_signal_state_arrays_optimized(df, parsed)


def build_s03_regime_er_signals(
    df: pd.DataFrame,
    params: Mapping[str, Any] | S03RegimeERParams,
) -> Signals:
    arrays = build_signal_state_arrays(df, params)
    return _signals_from_arrays(arrays)


def build_s03_regime_er_execution_data(
    df: pd.DataFrame,
    params: Mapping[str, Any] | S03RegimeERParams,
) -> ExecutionData:
    return build_signal_execution_data(
        df,
        signals=build_s03_regime_er_signals(df, params),
    )


def build_s03_regime_er_execution_data_batch(
    df: pd.DataFrame,
    params_list: Sequence[Mapping[str, Any] | S03RegimeERParams],
) -> list[ExecutionData]:
    ma_cache: dict[tuple[Any, ...], np.ndarray] = {}
    count_cache: dict[tuple[Any, ...], tuple[np.ndarray, np.ndarray]] = {}
    tband_cache: dict[tuple[Any, ...], np.ndarray] = {}
    regime_cache: dict[tuple[Any, ...], dict[str, np.ndarray]] = {}
    result: list[ExecutionData] = []
    for params in params_list:
        parsed = params if isinstance(params, S03RegimeERParams) else S03RegimeERParams.from_dict(params)
        arrays = _build_signal_state_arrays_optimized(
            df,
            parsed,
            ma_cache=ma_cache,
            count_cache=count_cache,
            tband_cache=tband_cache,
            regime_cache=regime_cache,
        )
        result.append(build_signal_execution_data(df, signals=_signals_from_arrays(arrays)))
    return result


__all__ = [
    "S03RegimeERParams",
    "build_s03_regime_er_execution_data",
    "build_s03_regime_er_execution_data_batch",
    "build_s03_regime_er_signals",
    "build_signal_state_arrays",
    "build_signal_state_arrays_reference",
    "normalize_parameter_aliases",
    "regime_er_state",
]
