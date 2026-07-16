"""Test-only S03-like signal helpers for certifying signal_reversal V2 execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import numpy as np
import pandas as pd

from core.engine_v2.contracts import Signals
from core.engine_v2.dataprep import build_signal_execution_data
from core.engine_v2.kernel import ExecutionData
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import V2RunResult, run_v2_strategy
from indicators.ma import get_ma


@dataclass(frozen=True)
class S03LikeParams:
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
    positionPct: float = 100.0
    contractSize: float = 0.01
    enableLong: bool = True
    enableShort: bool = True
    useEmergencySL: bool = False
    emergencySlPct: float = 20.0
    emergencySlUpdateBars: int = 16
    initialCapital: float = 100.0
    commissionPct: float = 0.05

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
    def from_dict(cls, payload: Mapping[str, Any] | None) -> "S03LikeParams":
        d = payload or {}
        return cls(
            dateFilter=cls._coerce_bool(d.get("dateFilter"), cls.dateFilter),
            start=cls._parse_timestamp(d.get("start")),
            end=cls._parse_timestamp(d.get("end")),
            maType3=str(d.get("maType3", cls.maType3)),
            maLength3=int(d.get("maLength3", cls.maLength3)),
            maOffset3=float(d.get("maOffset3", cls.maOffset3)),
            useCloseCount=cls._coerce_bool(d.get("useCloseCount"), cls.useCloseCount),
            closeCountLong=int(d.get("closeCountLong", cls.closeCountLong)),
            closeCountShort=int(d.get("closeCountShort", cls.closeCountShort)),
            useTBands=cls._coerce_bool(d.get("useTBands"), cls.useTBands),
            tBandLongPct=float(d.get("tBandLongPct", cls.tBandLongPct)),
            tBandShortPct=float(d.get("tBandShortPct", cls.tBandShortPct)),
            positionPct=float(d.get("positionPct", cls.positionPct)),
            contractSize=float(d.get("contractSize", cls.contractSize)),
            enableLong=cls._coerce_bool(d.get("enableLong"), cls.enableLong),
            enableShort=cls._coerce_bool(d.get("enableShort"), cls.enableShort),
            useEmergencySL=cls._coerce_bool(d.get("useEmergencySL"), cls.useEmergencySL),
            emergencySlPct=float(d.get("emergencySlPct", cls.emergencySlPct)),
            emergencySlUpdateBars=int(d.get("emergencySlUpdateBars", cls.emergencySlUpdateBars)),
            initialCapital=float(d.get("initialCapital", cls.initialCapital)),
            commissionPct=float(d.get("commissionPct", cls.commissionPct)),
        )


SIGNAL_CACHE_PARAM_NAMES = (
    "maType3",
    "maLength3",
    "maOffset3",
    "useCloseCount",
    "closeCountLong",
    "closeCountShort",
    "useTBands",
    "tBandLongPct",
    "tBandShortPct",
)
DATAPREP_CACHE_PARAM_NAMES = SIGNAL_CACHE_PARAM_NAMES


def fixture_config() -> dict[str, Any]:
    return {
        "id": "s03_like_signal_reversal_fixture",
        "name": "S03-like Signal Reversal Fixture",
        "engine": "v2",
        "execution": {
            "topology": "signal_reversal",
            "entryOrder": "market_next_open",
            "sizing": "fixed_pct_equity",
            "exitOnSignal": True,
            "boundary": "strict_close",
            "priceRounding": "none",
            "variantSelector": {
                "param": "useEmergencySL",
                "mapping": {False: "plain", True: "emergency"},
            },
            "variants": {
                "plain": {"stop": "none"},
                "emergency": {"stop": "emergency_pct"},
            },
        },
        "parameters": {
            "maType3": {
                "type": "select",
                "default": "SMA",
                "options": ["SMA", "EMA"],
                "role": "signal",
                "optimize": {"enabled": True},
            },
            "maLength3": {
                "type": "int",
                "default": 75,
                "role": "signal",
                "optimize": {"enabled": True, "min": 2, "max": 3, "step": 1},
            },
            "maOffset3": {"type": "float", "default": 0.2, "role": "signal", "optimize": {"enabled": False}},
            "useCloseCount": {"type": "bool", "default": True, "role": "signal", "optimize": {"enabled": False}},
            "closeCountLong": {
                "type": "int",
                "default": 7,
                "role": "signal",
                "optimize": {"enabled": True, "min": 1, "max": 2, "step": 1},
            },
            "closeCountShort": {"type": "int", "default": 5, "role": "signal", "optimize": {"enabled": False}},
            "useTBands": {"type": "bool", "default": True, "role": "signal", "optimize": {"enabled": False}},
            "tBandLongPct": {"type": "float", "default": 1.0, "role": "signal", "optimize": {"enabled": False}},
            "tBandShortPct": {"type": "float", "default": 1.3, "role": "signal", "optimize": {"enabled": False}},
            "positionPct": {"type": "float", "default": 100.0, "role": "execution", "optimize": {"enabled": False}},
            "contractSize": {"type": "float", "default": 0.01, "role": "execution", "optimize": {"enabled": False}},
            "enableLong": {"type": "bool", "default": True, "role": "execution", "optimize": {"enabled": False}},
            "enableShort": {"type": "bool", "default": True, "role": "execution", "optimize": {"enabled": False}},
            "useEmergencySL": {"type": "bool", "default": False, "role": "execution", "optimize": {"enabled": False}},
            "emergencySlPct": {
                "type": "float",
                "default": 20.0,
                "role": "execution",
                "optimize": {"enabled": True, "default_enabled": False, "min": 10.0, "max": 20.0, "step": 10.0},
            },
            "emergencySlUpdateBars": {
                "type": "int",
                "default": 16,
                "role": "execution",
                "optimize": {"enabled": False},
            },
            "initialCapital": {"type": "float", "default": 100.0, "role": "execution", "optimize": {"enabled": False}},
            "commissionPct": {"type": "float", "default": 0.05, "role": "execution", "optimize": {"enabled": False}},
            "dateFilter": {"type": "bool", "default": True, "role": "runtime", "optimize": {"enabled": False}},
            "start": {"type": "datetime", "default": None, "role": "runtime", "optimize": {"enabled": False}},
            "end": {"type": "datetime", "default": None, "role": "runtime", "optimize": {"enabled": False}},
        },
    }


def default_params_from_config(config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    source = config or fixture_config()
    return {
        str(name): spec["default"]
        for name, spec in source.get("parameters", {}).items()
        if isinstance(spec, Mapping) and "default" in spec
    }


def normalized_params(params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    merged = default_params_from_config()
    merged.update(dict(params or {}))
    return merged


def load_profile():
    return parse_execution_profile(fixture_config())


def build_s03_like_signals(df: pd.DataFrame, params: Mapping[str, Any] | S03LikeParams) -> Signals:
    parsed = params if isinstance(params, S03LikeParams) else S03LikeParams.from_dict(normalized_params(params))
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"] if "Volume" in df else None

    ma3 = get_ma(close, parsed.maType3, parsed.maLength3, volume, high, low)
    if parsed.maOffset3 != 0:
        ma3 = ma3 * (1.0 + parsed.maOffset3 / 100.0)
    ma3_up_band = ma3 * (1.0 + parsed.tBandLongPct / 100.0)
    ma3_down_band = ma3 * (1.0 - parsed.tBandShortPct / 100.0)

    close_values = close.to_numpy(copy=False)
    high_values = high.to_numpy(copy=False)
    low_values = low.to_numpy(copy=False)
    ma3_values = ma3.to_numpy(copy=False)
    up_values = ma3_up_band.to_numpy(copy=False)
    down_values = ma3_down_band.to_numpy(copy=False)

    long_entries = np.zeros(len(df), dtype=bool)
    short_entries = np.zeros(len(df), dtype=bool)
    t_band_state = 0
    count_close_long = 0
    count_close_short = 0
    trading_disabled = not (parsed.useCloseCount or parsed.useTBands)

    for i in range(len(df)):
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
        long_entries[i] = (not trading_disabled) and count_long and cross_tband_long
        short_entries[i] = (not trading_disabled) and count_short and cross_tband_short

    return Signals(long_entries=long_entries, short_entries=short_entries)


def build_v2_execution_data(df: pd.DataFrame, params: Mapping[str, Any]) -> ExecutionData:
    merged = normalized_params(params)
    signals = build_s03_like_signals(df, merged)
    return build_signal_execution_data(df, signals=signals)


def run_s03_like_v2(
    df: pd.DataFrame,
    params: Mapping[str, Any] | None = None,
    *,
    trade_start_idx: int = 0,
) -> V2RunResult:
    merged = normalized_params(params)
    return run_v2_strategy(
        data=build_v2_execution_data(df, merged),
        profile=load_profile(),
        params=merged,
        trade_start_idx=trade_start_idx,
    )


def make_gapless_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if result.empty:
        return result
    close_values = result["Close"].to_numpy(copy=True)
    open_values = result["Open"].to_numpy(copy=True)
    open_values[0] = close_values[0]
    if len(result) > 1:
        open_values[1:] = close_values[:-1]
    result["Open"] = open_values
    result["High"] = np.maximum.reduce(
        [
            result["High"].to_numpy(copy=False),
            result["Open"].to_numpy(copy=False),
            result["Close"].to_numpy(copy=False),
        ]
    )
    result["Low"] = np.minimum.reduce(
        [
            result["Low"].to_numpy(copy=False),
            result["Open"].to_numpy(copy=False),
            result["Close"].to_numpy(copy=False),
        ]
    )
    return result


def synthetic_ohlc(
    closes: list[float],
    *,
    opens: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
) -> pd.DataFrame:
    close_values = np.asarray(closes, dtype=float)
    open_values = np.asarray(opens if opens is not None else closes, dtype=float)
    high_values = np.asarray(highs if highs is not None else np.maximum(open_values, close_values), dtype=float)
    low_values = np.asarray(lows if lows is not None else np.minimum(open_values, close_values), dtype=float)
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Volume": np.full(len(close_values), 1000.0),
        },
        index=pd.date_range("2025-01-01", periods=len(close_values), freq="30min", tz="UTC"),
    )


__all__ = [
    "DATAPREP_CACHE_PARAM_NAMES",
    "S03LikeParams",
    "SIGNAL_CACHE_PARAM_NAMES",
    "build_s03_like_signals",
    "build_v2_execution_data",
    "default_params_from_config",
    "fixture_config",
    "load_profile",
    "make_gapless_ohlc",
    "normalized_params",
    "run_s03_like_v2",
    "synthetic_ohlc",
]
