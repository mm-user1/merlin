"""Thin S03 Reversal v11 Regime-ER Backtester V2 strategy adapter."""

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

from .signals import (
    S03RegimeERParams,
    build_s03_regime_er_execution_data,
    normalize_parameter_aliases,
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
    "useRegime",
    "regimeErLength",
    "regimeErThresh",
)
DATAPREP_CACHE_PARAM_NAMES = SIGNAL_CACHE_PARAM_NAMES


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


def normalized_params(params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    merged = default_params_from_config()
    merged.update(dict(params or {}))
    return normalize_parameter_aliases(merged)


def _truncate_at_end(df: pd.DataFrame, parsed: S03RegimeERParams) -> pd.DataFrame:
    if parsed.dateFilter and parsed.end is not None and not df.empty:
        eligible = np.flatnonzero(df.index <= parsed.end)
        if eligible.size == 0:
            return df.iloc[0:0].copy()
        return df.iloc[: int(eligible[-1]) + 1]
    return df


def build_v2_execution_data(df: pd.DataFrame, params: Mapping[str, Any]) -> ExecutionData:
    """Build the S03 Regime-ER signal-only V2 execution arrays for Grid V2."""

    merged = normalized_params(dict(params or {}))
    parsed = S03RegimeERParams.from_dict(merged)
    return build_s03_regime_er_execution_data(_truncate_at_end(df, parsed), parsed)


class S03ReversalV11RegimeERB2(BaseStrategy):
    STRATEGY_ID = "s03_reversal_v11_regime_er_b2"
    STRATEGY_NAME = "S03 Reversal v11 Regime-ER B2"
    STRATEGY_VERSION = "v11-regime-er-b2"

    @staticmethod
    def run(
        df: pd.DataFrame,
        params: Dict[str, Any],
        trade_start_idx: int = 0,
    ) -> StrategyResult:
        merged_params = normalized_params(params)
        parsed = S03RegimeERParams.from_dict(merged_params)
        data = build_s03_regime_er_execution_data(_truncate_at_end(df, parsed), parsed)
        return run_v2_strategy(
            data=data,
            profile=load_profile(),
            params=merged_params,
            trade_start_idx=trade_start_idx,
        ).strategy_result


__all__ = [
    "DATAPREP_CACHE_PARAM_NAMES",
    "S03ReversalV11RegimeERB2",
    "SIGNAL_CACHE_PARAM_NAMES",
    "build_v2_execution_data",
    "default_params_from_config",
    "load_config",
    "load_profile",
    "normalized_params",
]

