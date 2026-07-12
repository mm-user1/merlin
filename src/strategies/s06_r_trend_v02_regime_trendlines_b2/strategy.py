"""Thin S06 R-Trend v02 Regime-TL Backtester V2 strategy adapter."""

from __future__ import annotations

import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Mapping

import numpy as np
import pandas as pd

from core.backtest_engine import StrategyResult
from core.engine_v2.contracts import ExecutionProfile
from core.engine_v2.kernel import ExecutionData
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from strategies.base import BaseStrategy

from .signals import S06RegimeTLParams, build_regime_tl_execution_data, normalize_parameter_aliases


SIGNAL_CACHE_PARAM_NAMES = (
    "entryMode",
    "enableLong",
    "enableShort",
    "fastLength",
    "fastSmooth",
    "slowLength",
    "slowSmooth",
    "thresholdOS",
    "thresholdOB",
    "useRegime",
    "regimePivotLen",
    "regimeSlopeFactor",
    "regimeBreakBufferX",
)
DATAPREP_CACHE_PARAM_NAMES = (
    *SIGNAL_CACHE_PARAM_NAMES,
    "stopLP",
    "trailMAType",
    "trailMALength",
    "trailMAOffsetEx",
)


@lru_cache(maxsize=1)
def _load_config_cached() -> dict[str, Any]:
    with (Path(__file__).with_name("config.json")).open(encoding="utf-8") as handle:
        return json.load(handle)


def load_config() -> dict[str, Any]:
    """Return a caller-owned config copy backed by a cached JSON load."""

    return deepcopy(_load_config_cached())


def default_params_from_config(config: Dict[str, Any] | None = None) -> dict[str, Any]:
    payload = config if config is not None else _load_config_cached()
    defaults: dict[str, Any] = {}
    for name, spec in payload.get("parameters", {}).items():
        if isinstance(spec, dict) and "default" in spec:
            defaults[str(name)] = spec["default"]
    return defaults


@lru_cache(maxsize=1)
def load_profile() -> ExecutionProfile:
    return parse_execution_profile(_load_config_cached())


def normalized_params(params: Dict[str, Any] | None = None) -> dict[str, Any]:
    merged = default_params_from_config()
    merged.update(params or {})
    return normalize_parameter_aliases(merged)


def build_v2_execution_data(df: pd.DataFrame, params: Mapping[str, Any]) -> ExecutionData:
    """Build the Regime-TL V2 execution arrays for generic Grid V2."""

    parsed = S06RegimeTLParams.from_dict(normalized_params(dict(params or {})))
    if parsed.dateFilter and parsed.end is not None and not df.empty:
        eligible = np.flatnonzero(df.index <= parsed.end)
        if eligible.size == 0:
            df = df.iloc[0:0].copy()
        else:
            df = df.iloc[: int(eligible[-1]) + 1]
    return build_regime_tl_execution_data(df, parsed)


class S06RTrendV02RegimeTLB2(BaseStrategy):
    STRATEGY_ID = "s06_r_trend_v02_regime_trendlines_b2"
    STRATEGY_NAME = "S06 R-Trend Regime-TL B2"
    STRATEGY_VERSION = "v02-regime-tl-b2"

    @staticmethod
    def run(
        df: pd.DataFrame,
        params: Dict[str, Any],
        trade_start_idx: int = 0,
    ) -> StrategyResult:
        merged_params = default_params_from_config()
        merged_params.update(params or {})
        merged_params = normalize_parameter_aliases(merged_params)
        parsed = S06RegimeTLParams.from_dict(merged_params)

        if parsed.dateFilter and parsed.end is not None and not df.empty:
            eligible = np.flatnonzero(df.index <= parsed.end)
            if eligible.size == 0:
                df = df.iloc[0:0].copy()
            else:
                df = df.iloc[: int(eligible[-1]) + 1]

        data = build_regime_tl_execution_data(df, parsed)
        return run_v2_strategy(
            data=data,
            profile=load_profile(),
            params=merged_params,
            trade_start_idx=trade_start_idx,
        ).strategy_result


__all__ = [
    "DATAPREP_CACHE_PARAM_NAMES",
    "S06RTrendV02RegimeTLB2",
    "SIGNAL_CACHE_PARAM_NAMES",
    "build_v2_execution_data",
    "default_params_from_config",
    "load_config",
    "load_profile",
    "normalized_params",
]
