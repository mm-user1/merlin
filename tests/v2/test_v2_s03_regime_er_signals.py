"""Regime-ER signal, causality, and cache-declaration tests."""

import numpy as np
import pytest

from core.grid_v2 import GridV2Settings, GridV2StrategyHooks, build_grid_v2_plan, estimate_grid_v2_cache
from strategies.s03_reversal_v11_regime_er_b2 import strategy as s03_regime_er_strategy
from strategies.s03_reversal_v11_regime_er_b2.signals import (
    S03RegimeERParams,
    build_signal_state_arrays,
    regime_er_state,
)
from strategies.s03_reversal_v11_regime_er_b2.strategy import (
    DATAPREP_CACHE_PARAM_NAMES,
    SIGNAL_CACHE_PARAM_NAMES,
    load_config,
    normalized_params,
)

from s03_regime_er_test_helpers import (
    BASELINE_START,
    REFERENCE_A,
    REFERENCE_B,
    merged_reference_params,
    prepared_reference_dataset,
    synthetic_ohlc,
    trade_skeleton,
    run_reference,
)


def test_params_parse_baseline_values_and_preserve_pine_defaults():
    baseline = S03RegimeERParams.from_dict(merged_reference_params(REFERENCE_B))
    defaults = S03RegimeERParams.from_dict(normalized_params({}))

    assert baseline.maOffset3 == 0.0
    assert baseline.regimeErLength == 30
    assert baseline.regimeErThresh == 0.4
    assert baseline.emergencySlPct == 10.0
    assert defaults.maOffset3 == 0.2
    assert defaults.regimeErLength == 20
    assert defaults.regimeErThresh == 0.3
    assert defaults.emergencySlPct == 20.0


def test_normalized_params_applies_aliases_before_defaults_and_preserves_canonical_wins():
    aliased = normalized_params(
        {
            "useDateFilter": False,
            "startDate": "2025-02-01T00:00:00Z",
            "endDate": "2026-02-01T00:00:00Z",
        }
    )
    canonical = normalized_params({"dateFilter": True, "useDateFilter": False})

    assert aliased["dateFilter"] is False
    assert aliased["start"] == "2025-02-01T00:00:00Z"
    assert aliased["end"] == "2026-02-01T00:00:00Z"
    assert canonical["dateFilter"] is True


@pytest.mark.parametrize(
    "overrides",
    [
        {"regimeErLength": 1},
        {"regimeErThresh": 0.0},
        {"closeCountLong": 0},
        {"tBandLongPct": 0.0},
        {"useEmergencySL": True, "emergencySlPct": 0.0},
        {"useEmergencySL": True, "emergencySlUpdateBars": 0},
    ],
)
def test_params_reject_invalid_values(overrides):
    with pytest.raises(ValueError):
        S03RegimeERParams.from_dict(normalized_params(overrides))


def test_regime_er_flips_and_resets_like_pine_state_machine():
    close = np.array([1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0])
    arrays = regime_er_state(close, 3, 0.5)

    assert arrays["regime_er"][:3].tolist() == [0.0, 0.0, 0.0]
    assert arrays["regime_state"][:4].tolist() == [0, 0, 0, 0]
    assert arrays["regime_er"][4] == pytest.approx(1.0)
    assert arrays["regime_state"][4:7].tolist() == [1, 1, 1]
    assert arrays["regime_er"][7] == pytest.approx(0.0)
    assert arrays["regime_state"][7] == 0


def test_warmup_flat_blocks_entries_and_emits_flat_exits():
    df = synthetic_ohlc([1.0, 2.0, 3.0, 4.0])
    params = normalized_params(
        {
            "dateFilter": False,
            "maLength3": 2,
            "maOffset3": 0.0,
            "useCloseCount": True,
            "closeCountLong": 1,
            "closeCountShort": 1,
            "useTBands": False,
            "useRegime": True,
            "regimeErLength": 4,
            "regimeErThresh": 0.5,
        }
    )
    arrays = build_signal_state_arrays(df, params)

    assert arrays["regime_state"].tolist() == [0, 0, 0, 0]
    assert arrays["base_long"].any()
    assert not arrays["long_entries"].any()
    assert not arrays["short_entries"].any()
    assert arrays["long_exits"].tolist() == [True, True, True, True]
    assert arrays["short_exits"].tolist() == [True, True, True, True]


def test_use_regime_false_returns_ungated_entries_and_no_flat_exits():
    df = synthetic_ohlc([1.0, 2.0, 3.0, 4.0, 5.0])
    params = normalized_params(
        {
            "dateFilter": False,
            "maLength3": 1,
            "maOffset3": 0.0,
            "useCloseCount": True,
            "closeCountLong": 1,
            "closeCountShort": 1,
            "useTBands": False,
            "useRegime": False,
            "regimeErLength": 3,
            "regimeErThresh": 0.5,
        }
    )
    arrays = build_signal_state_arrays(df, params)

    np.testing.assert_array_equal(arrays["long_entries"], arrays["base_long"])
    np.testing.assert_array_equal(arrays["short_entries"], arrays["base_short"])
    assert not arrays["long_exits"].any()
    assert not arrays["short_exits"].any()


def test_regime_gating_only_allows_directional_entries():
    params = S03RegimeERParams.from_dict(merged_reference_params(REFERENCE_B))
    prepared, _ = prepared_reference_dataset()
    arrays = build_signal_state_arrays(prepared, params)
    state = arrays["regime_state"]

    assert not (arrays["long_entries"] & ~arrays["base_long"]).any()
    assert not (arrays["short_entries"] & ~arrays["base_short"]).any()
    assert (arrays["long_entries"] <= (state == 1)).all()
    assert (arrays["short_entries"] <= (state == -1)).all()
    assert arrays["long_exits"].sum() > 0
    np.testing.assert_array_equal(arrays["long_exits"], state == 0)
    np.testing.assert_array_equal(arrays["short_exits"], state == 0)


def test_regime_state_and_signals_are_prefix_invariant():
    params = S03RegimeERParams.from_dict(merged_reference_params(REFERENCE_B))
    prepared, _ = prepared_reference_dataset()
    prefix_end = BASELINE_START + np.timedelta64(45, "D")
    prefix = prepared.loc[prepared.index <= prefix_end].copy()
    prefix_len = len(prefix)

    full_arrays = build_signal_state_arrays(prepared, params)
    prefix_arrays = build_signal_state_arrays(prefix, params)

    for name in ("regime_state", "long_entries", "short_entries", "long_exits", "short_exits"):
        np.testing.assert_array_equal(prefix_arrays[name], full_arrays[name][:prefix_len])


def test_window_start_invariance_with_larger_warmup_for_reference_b():
    pinned = run_reference(REFERENCE_B, warmup_bars=1000)
    larger = run_reference(REFERENCE_B, warmup_bars=1200)

    assert trade_skeleton(larger.strategy_result) == trade_skeleton(pinned.strategy_result)


def test_signal_cache_names_cover_exactly_config_signal_role_params():
    config = load_config()
    signal_role_params = {
        name
        for name, spec in config["parameters"].items()
        if isinstance(spec, dict) and spec.get("role") == "signal"
    }

    assert set(SIGNAL_CACHE_PARAM_NAMES) == signal_role_params
    assert set(DATAPREP_CACHE_PARAM_NAMES) == signal_role_params


def test_cache_names_exclude_runtime_and_execution_fields():
    forbidden = {
        "dateFilter",
        "start",
        "end",
        "warmupBars",
        "useEmergencySL",
        "emergencySlPct",
        "emergencySlUpdateBars",
        "commissionPct",
        "positionPct",
        "contractSize",
        "initialCapital",
        "enableLong",
        "enableShort",
    }

    assert not forbidden & set(SIGNAL_CACHE_PARAM_NAMES)
    assert not forbidden & set(DATAPREP_CACHE_PARAM_NAMES)


@pytest.mark.parametrize(
    "overrides",
    [
        {"maType3": "EMA"},
        {"maLength3": 50},
        {"maOffset3": 0.5},
        {"useCloseCount": False},
        {"closeCountLong": 4},
        {"closeCountShort": 3},
        {"useTBands": False},
        {"tBandLongPct": 0.5},
        {"tBandShortPct": 0.5},
        {"useRegime": False},
        {"regimeErLength": 20},
        {"regimeErThresh": 0.2},
    ],
)
def test_each_declared_signal_param_changes_signal_or_state_outputs(overrides):
    base = S03RegimeERParams.from_dict(merged_reference_params(REFERENCE_B))
    varied = S03RegimeERParams.from_dict(merged_reference_params(REFERENCE_B, overrides))
    prepared, _ = prepared_reference_dataset()
    base_arrays = build_signal_state_arrays(prepared, base)
    varied_arrays = build_signal_state_arrays(prepared, varied)
    changed = any(
        not np.array_equal(base_arrays[name], varied_arrays[name], equal_nan=True)
        for name in (
            "ma3",
            "t_band_state",
            "base_long",
            "base_short",
            "regime_state",
            "long_entries",
            "short_entries",
            "long_exits",
            "short_exits",
        )
    )

    assert changed, f"{overrides} did not change signal/state outputs"


def test_signal_axis_produces_distinct_grid_signal_cache_groups():
    prepared, trade_start_idx = prepared_reference_dataset()
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_axes=("regimeErLength",)),
        base_params=merged_reference_params(REFERENCE_A),
    )
    hooks = GridV2StrategyHooks.from_strategy(s03_regime_er_strategy)
    estimate = estimate_grid_v2_cache(plan, prepared, trade_start_idx, hooks)

    assert plan.parameter_domains["regimeErLength"].values == (20, 30, 40)
    assert estimate.signal_combo_count == 3
    assert estimate.dataprep_combo_count == 3
