"""Thin S06 Backtester V2 strategy adapter."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from core.backtest_engine import StrategyResult
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from strategies.base import BaseStrategy

from .signals import S06B2Params, build_s06_b2_execution_data, normalize_parameter_aliases


def load_config() -> dict[str, Any]:
    with (Path(__file__).with_name("config.json")).open(encoding="utf-8") as handle:
        return json.load(handle)


def default_params_from_config(config: Dict[str, Any] | None = None) -> dict[str, Any]:
    payload = config if config is not None else load_config()
    defaults: dict[str, Any] = {}
    for name, spec in payload.get("parameters", {}).items():
        if isinstance(spec, dict) and "default" in spec:
            defaults[str(name)] = spec["default"]
    return defaults


def normalized_params(params: Dict[str, Any] | None = None) -> dict[str, Any]:
    merged = default_params_from_config()
    merged.update(params or {})
    return normalize_parameter_aliases(merged)


class S06RTrendV02B2(BaseStrategy):
    STRATEGY_ID = "s06_r_trend_v02_b2"
    STRATEGY_NAME = "S06 R-Trend B2"
    STRATEGY_VERSION = "v02-b2"

    @staticmethod
    def run(
        df: pd.DataFrame,
        params: Dict[str, Any],
        trade_start_idx: int = 0,
    ) -> StrategyResult:
        config = load_config()
        merged_params = default_params_from_config(config)
        merged_params.update(params or {})
        merged_params = normalize_parameter_aliases(merged_params)
        parsed = S06B2Params.from_dict(merged_params)

        if parsed.dateFilter and parsed.end is not None and not df.empty:
            eligible = np.flatnonzero(df.index <= parsed.end)
            if eligible.size == 0:
                df = df.iloc[0:0].copy()
            else:
                df = df.iloc[: int(eligible[-1]) + 1]

        data = build_s06_b2_execution_data(df, parsed)
        profile = parse_execution_profile(config)
        return run_v2_strategy(
            data=data,
            profile=profile,
            params=merged_params,
            trade_start_idx=trade_start_idx,
        ).strategy_result


__all__ = [
    "S06RTrendV02B2",
    "default_params_from_config",
    "load_config",
    "normalized_params",
]
