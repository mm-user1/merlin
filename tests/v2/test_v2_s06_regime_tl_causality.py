"""Causality, no-repainting, and cache-declaration invariants for S06 Regime-TL."""

import numpy as np
import pandas as pd
import pytest

from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from strategies.s06_r_trend_v02_regime_trendlines_b2.signals import (
    S06RegimeTLParams,
    build_regime_indicator_arrays,
    build_regime_tl_execution_data,
)
from strategies.s06_r_trend_v02_regime_trendlines_b2.strategy import (
    DATAPREP_CACHE_PARAM_NAMES,
    SIGNAL_CACHE_PARAM_NAMES,
    load_config,
)

from s06_regime_tl_test_helpers import (
    BASELINE_END,
    BASELINE_START,
    REFERENCE_B,
    merged_reference_params,
    prepared_reference_dataset,
    profile_with_rounding,
    run_reference,
    trade_signature,
    trade_skeleton,
)


def test_regime_state_and_gated_signals_are_prefix_invariant():
    """Appending future bars must never change closed-bar regime states or
    gated signals (no lookahead, no repainting)."""

    params = S06RegimeTLParams.from_dict(merged_reference_params(REFERENCE_B))
    prepared, _ = prepared_reference_dataset()
    prefix_end = BASELINE_START + pd.Timedelta(days=45)
    prefix = prepared.loc[prepared.index <= prefix_end].copy()
    prefix_len = len(prefix)

    full_arrays = build_regime_indicator_arrays(prepared, params)
    prefix_arrays = build_regime_indicator_arrays(prefix, params)

    np.testing.assert_array_equal(
        prefix_arrays["regime_state"], full_arrays["regime_state"][:prefix_len]
    )
    for name in ("long_signal", "short_signal"):
        np.testing.assert_array_equal(prefix_arrays[name], full_arrays[name][:prefix_len])


def test_closed_trades_before_cutoff_are_prefix_invariant():
    prefix_end = BASELINE_START + pd.Timedelta(days=45)
    params = merged_reference_params(
        REFERENCE_B,
        {"end": prefix_end.isoformat().replace("+00:00", "Z")},
    )
    parsed = S06RegimeTLParams.from_dict(params)
    full_prepared, trade_start_idx = prepared_reference_dataset()
    prefix_prepared = full_prepared.loc[full_prepared.index <= prefix_end].copy()

    full_data = build_regime_tl_execution_data(full_prepared, parsed)
    prefix_data = build_regime_tl_execution_data(prefix_prepared, parsed)

    config = load_config()
    config["execution"]["boundary"] = "none"
    profile = parse_execution_profile(config)
    prefix_run = run_v2_strategy(
        data=prefix_data, profile=profile, params=params, trade_start_idx=trade_start_idx
    )
    full_run = run_v2_strategy(
        data=full_data, profile=profile, params=params, trade_start_idx=trade_start_idx
    )

    prefix_closed = [
        trade for trade in prefix_run.strategy_result.trades if pd.Timestamp(trade.exit_time) <= prefix_end
    ]
    full_closed = [
        trade for trade in full_run.strategy_result.trades if pd.Timestamp(trade.exit_time) <= prefix_end
    ]
    holder = type("Result", (), {})
    prefix_holder, full_holder = holder(), holder()
    prefix_holder.trades = prefix_closed
    full_holder.trades = full_closed
    assert trade_signature(prefix_holder) == trade_signature(full_holder)


def test_window_start_invariance_with_larger_warmup_for_regime_reference():
    """The regime state machine has unbounded memory in principle; this pins
    that the 1000-bar warmup recipe is converged for the pilot baseline."""

    pinned = run_reference(REFERENCE_B, warmup_bars=1000)
    larger = run_reference(REFERENCE_B, warmup_bars=1500)

    assert trade_skeleton(larger.strategy_result) == trade_skeleton(pinned.strategy_result)
    for larger_trade, pinned_trade in zip(
        larger.strategy_result.trades, pinned.strategy_result.trades
    ):
        assert larger_trade.entry_price == pytest.approx(pinned_trade.entry_price, rel=1e-9, abs=1e-12)
        assert larger_trade.exit_price == pytest.approx(pinned_trade.exit_price, rel=1e-9, abs=1e-12)
        assert larger_trade.net_pnl == pytest.approx(pinned_trade.net_pnl, rel=1e-9, abs=1e-12)


def test_warmup_region_signals_do_not_create_executable_orders():
    params = merged_reference_params(REFERENCE_B)
    parsed = S06RegimeTLParams.from_dict(params)
    prepared, trade_start_idx = prepared_reference_dataset(warmup_bars=1000)
    warmup_only = prepared.iloc[:trade_start_idx].copy()
    data = build_regime_tl_execution_data(warmup_only, parsed)
    run = run_v2_strategy(
        data=data,
        profile=profile_with_rounding("none"),
        params=params,
        trade_start_idx=trade_start_idx,
    )

    assert run.strategy_result.trades == []
    assert run.standing_state.pending_entry_direction == 0


def test_signal_cache_names_cover_exactly_the_config_signal_role_params():
    """Mandatory cache-declaration invariant (B2-TZ 26 §7.2): every config
    param with role="signal" participates in signal cache identity, and no
    runtime/window field leaks into cache identity."""

    config = load_config()
    signal_role_params = {
        name
        for name, spec in config["parameters"].items()
        if isinstance(spec, dict) and spec.get("role") == "signal"
    }

    assert set(SIGNAL_CACHE_PARAM_NAMES) == signal_role_params
    assert {"useRegime", "regimePivotLen", "regimeSlopeFactor", "regimeBreakBufferX"} <= set(
        SIGNAL_CACHE_PARAM_NAMES
    )


def test_dataprep_cache_names_extend_signal_names_with_dataprep_params():
    assert set(SIGNAL_CACHE_PARAM_NAMES) <= set(DATAPREP_CACHE_PARAM_NAMES)
    assert set(DATAPREP_CACHE_PARAM_NAMES) - set(SIGNAL_CACHE_PARAM_NAMES) == {
        "stopLP",
        "trailMAType",
        "trailMALength",
        "trailMAOffsetEx",
    }


def test_cache_names_exclude_runtime_and_window_fields():
    forbidden = {"dateFilter", "start", "end", "warmupBars", "use_backtester"}
    assert not forbidden & set(SIGNAL_CACHE_PARAM_NAMES)
    assert not forbidden & set(DATAPREP_CACHE_PARAM_NAMES)
    config = load_config()
    runtime_params = {
        name
        for name, spec in config["parameters"].items()
        if isinstance(spec, dict) and spec.get("role") == "runtime"
    }
    assert not runtime_params & set(DATAPREP_CACHE_PARAM_NAMES)


def test_regime_params_change_signal_arrays_when_enabled():
    """Behavioral backstop for the cache declaration: each regime numeric
    param materially changes the gated signal arrays while useRegime=true, so
    omitting any of them from the signal cache identity would collide."""

    base = S06RegimeTLParams.from_dict(merged_reference_params(REFERENCE_B))
    prepared, _ = prepared_reference_dataset()
    base_arrays = build_regime_indicator_arrays(prepared, base)

    for overrides in (
        {"regimePivotLen": 10},
        {"regimeSlopeFactor": 1.0},
        {"regimeBreakBufferX": 0.0},
        {"useRegime": False},
    ):
        varied = S06RegimeTLParams.from_dict(merged_reference_params(REFERENCE_B, overrides))
        varied_arrays = build_regime_indicator_arrays(prepared, varied)
        changed = any(
            not np.array_equal(varied_arrays[name], base_arrays[name])
            for name in ("long_signal", "short_signal")
        )
        assert changed, f"{overrides} did not change the gated signal arrays"


def test_end_boundary_matches_baseline_window():
    prepared, trade_start_idx = prepared_reference_dataset()

    assert trade_start_idx == 1000
    assert prepared.index[trade_start_idx] == BASELINE_START
    assert prepared.index[-1] == BASELINE_END
