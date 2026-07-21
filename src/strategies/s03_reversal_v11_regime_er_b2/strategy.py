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
    build_s03_regime_er_execution_data_batch,
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
    raw = normalize_parameter_aliases(params or {})
    merged = default_params_from_config()
    merged.update(raw)
    return merged


def _truncate_at_end(df: pd.DataFrame, parsed: S03RegimeERParams) -> pd.DataFrame:
    if parsed.dateFilter and parsed.end is not None and not df.empty:
        eligible = np.flatnonzero(df.index <= parsed.end)
        if eligible.size == 0:
            return df.iloc[0:0].copy()
        last_index = int(eligible[-1])
        if last_index >= len(df) - 1:
            return df
        return df.iloc[: last_index + 1]
    return df


def _truncation_key(parsed: S03RegimeERParams) -> tuple[Any, Any]:
    if not parsed.dateFilter:
        return False, None
    end = parsed.end.isoformat() if parsed.end is not None else None
    return True, end


def build_v2_execution_data(df: pd.DataFrame, params: Mapping[str, Any]) -> ExecutionData:
    """Build the S03 Regime-ER signal-only V2 execution arrays for Grid V2."""

    merged = normalized_params(dict(params or {}))
    parsed = S03RegimeERParams.from_dict(merged)
    return build_s03_regime_er_execution_data(_truncate_at_end(df, parsed), parsed)


def build_v2_execution_data_batch(
    df: pd.DataFrame,
    params_list: Any,
) -> list[ExecutionData]:
    """Build S03 Regime-ER signal-only execution arrays for a batch of params."""

    parsed_list = [
        S03RegimeERParams.from_dict(normalized_params(dict(params or {})))
        for params in params_list
    ]
    if not parsed_list:
        return []
    first_key = _truncation_key(parsed_list[0])
    if all(_truncation_key(parsed) == first_key for parsed in parsed_list):
        return build_s03_regime_er_execution_data_batch(
            _truncate_at_end(df, parsed_list[0]),
            parsed_list,
        )
    return [
        build_s03_regime_er_execution_data(_truncate_at_end(df, parsed), parsed)
        for parsed in parsed_list
    ]


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
    "build_v2_execution_data_batch",
    "default_params_from_config",
    "load_config",
    "load_profile",
    "normalized_params",
]
