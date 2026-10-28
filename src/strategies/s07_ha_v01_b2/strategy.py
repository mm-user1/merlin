"""Thin S07 adapter; filters, admission and fills are owned by generic core."""
from copy import deepcopy
from functools import lru_cache
import json
from pathlib import Path

from core.entry_filters import MISSING
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from strategies.base import BaseStrategy

from .signals import execution_data, indicators, rolling_extrema

SIGNAL_CACHE_PARAM_NAMES = ("entryRunLength", "enableLong", "enableShort")
DATAPREP_CACHE_PARAM_NAMES = (*SIGNAL_CACHE_PARAM_NAMES, "stopLP")


@lru_cache(maxsize=1)
def _config():
    return json.loads(Path(__file__).with_name("config.json").read_text(encoding="utf-8"))


def load_config():
    return deepcopy(_config())


@lru_cache(maxsize=1)
def load_profile():
    return parse_execution_profile(_config())


def normalized_params(params=None):
    return {**{k: v["default"] for k, v in _config()["parameters"].items()}, **(params or {})}


def build_v2_execution_data(frame, params):
    params = normalized_params(params)
    return execution_data(frame, params, indicators(frame), rolling_extrema(frame, params["stopLP"]))


def build_v2_execution_data_batch(frame, params_list):
    values, extrema = indicators(frame), {}
    result = []
    for raw in params_list:
        params = normalized_params(raw)
        length = params["stopLP"]
        if length not in extrema:
            extrema[length] = rolling_extrema(frame, length)
        result.append(execution_data(frame, params, values, extrema[length]))
    return result


class S07HaV01B2(BaseStrategy):
    STRATEGY_ID = "s07_ha_v01_b2"
    STRATEGY_NAME = "S_07 HA"
    STRATEGY_VERSION = "v01"

    @staticmethod
    def run(frame, params, trade_start_idx=0, *, admission_rules=None, entry_filter_context=MISSING):
        params = normalized_params(params)
        return run_v2_strategy(data=build_v2_execution_data(frame, params), profile=load_profile(),
            params=params, trade_start_idx=trade_start_idx, admission_rules=admission_rules,
            entry_filter_context=entry_filter_context).strategy_result
