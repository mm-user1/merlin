"""Explicitly opted-in version of the admission fixture, including raw dual bars."""
from dataclasses import replace
import numpy as np

from core.entry_filters import MISSING
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from . import strategy as base

STRATEGY_ID = "test_entry_filters_v1"
SIGNAL_CACHE_PARAM_NAMES = base.SIGNAL_CACHE_PARAM_NAMES
DATAPREP_CACHE_PARAM_NAMES = base.DATAPREP_CACHE_PARAM_NAMES


def load_config():
    config = base.load_config()
    config.update(id=STRATEGY_ID, name="Synthetic Entry Filters",
                  entryFilterSupport=dict(version=1, methods=["candleDirection"]))
    config["parameters"]["enableShort"]["default"] = True
    return config


def normalized_params(params=None):
    return {**{k: v["default"] for k, v in load_config()["parameters"].items()}, **(params or {})}


def build_v2_execution_data(df, params):
    data = base.build_v2_execution_data(df, params)
    slots = df.index.as_unit("ns").asi8 // (5*60*10**9)
    dual = slots % (int(params.get("signalEvery", 12))*6) == 0
    return replace(data, signals=replace(data.signals,
        long_entries=data.signals.long_entries | dual, short_entries=data.signals.short_entries | dual))


def build_v2_execution_data_batch(df, params):
    return [build_v2_execution_data(df, p) for p in params]


class FilterTestStrategy:
    STRATEGY_ID = STRATEGY_ID
    STRATEGY_NAME = "Synthetic Entry Filters"
    STRATEGY_VERSION = "test-1"

    @staticmethod
    def run(df, params, trade_start_idx=0, *, admission_rules=None, entry_filter_context=MISSING):
        params = normalized_params(params)
        return run_v2_strategy(data=build_v2_execution_data(df, params), profile=parse_execution_profile(load_config()),
            params=params, trade_start_idx=trade_start_idx, admission_rules=admission_rules,
            entry_filter_context=entry_filter_context).strategy_result
