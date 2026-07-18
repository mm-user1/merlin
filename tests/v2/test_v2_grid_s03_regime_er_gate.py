"""Generic Grid V2 gate for S03 Reversal v11 Regime-ER B2."""

from __future__ import annotations

import math
import os

import pytest

from core.engine_v2.runner import run_v2_strategy
from core.engine_v2.compiled_kernel import compiled_batch_available
from core.grid_v2 import (
    COMPILED_BATCH_KIND,
    GridV2Settings,
    GridV2StrategyHooks,
    build_grid_v2_plan,
    estimate_grid_v2_cache,
    execute_grid_v2_candidates,
)
from core.grid_engine import FAST_GRID_BACKENDS, supports_fast_grid, supports_grid_v2
from strategies import get_strategy, list_strategies
from strategies.s03_reversal_v11_regime_er_b2 import strategy as s03_regime_er_strategy
from strategies.s03_reversal_v11_regime_er_b2.strategy import load_config

from s03_regime_er_test_helpers import (
    REFERENCE_A,
    REFERENCE_B,
    merged_reference_params,
    prepared_reference_dataset,
)


JIT_DISABLED = os.environ.get("NUMBA_DISABLE_JIT", "").strip().lower() in {"1", "true", "yes"}


@pytest.fixture(scope="module")
def prepared_data():
    return prepared_reference_dataset()


@pytest.fixture(scope="module")
def hooks():
    return GridV2StrategyHooks.from_strategy(s03_regime_er_strategy)


def test_strategy_is_registered_without_v1_fast_grid_backend():
    strategy_ids = {item["id"] for item in list_strategies()}

    assert "s03_reversal_v11_regime_er_b2" in strategy_ids
    assert get_strategy("s03_reversal_v11_regime_er_b2").STRATEGY_ID == "s03_reversal_v11_regime_er_b2"
    assert "s03_reversal_v11_regime_er_b2" not in FAST_GRID_BACKENDS
    assert supports_fast_grid("s03_reversal_v11_regime_er_b2") is False
    assert supports_grid_v2("s03_reversal_v11_regime_er_b2") is True


def test_default_plan_keeps_regime_and_emergency_switches_fixed_per_study():
    plan = build_grid_v2_plan(load_config(), base_params=merged_reference_params(REFERENCE_B))

    assert plan.deduped_candidate_count == 2
    assert plan.per_variant_counts == {"plain": 1, "emergency": 1}
    default_axes = set(plan.metadata["default_enabled_axes"])
    assert "useRegime" not in default_axes
    assert "useEmergencySL" not in default_axes
    assert plan.parameter_domains["useRegime"].values == (True,)
    assert plan.parameter_domains["useRegime"].is_axis is False
    assert plan.parameter_domains["useEmergencySL"].is_axis is False


def test_use_regime_cannot_be_enabled_as_grid_axis():
    with pytest.raises(ValueError, match="useRegime"):
        build_grid_v2_plan(
            load_config(),
            GridV2Settings(enabled_axes=("useRegime",)),
            base_params=merged_reference_params(REFERENCE_B),
        )


def test_candidate_identity_is_deterministic_with_ma_type_runtime_subset():
    base_params = merged_reference_params(REFERENCE_B, {"maType3_options": ["SMA", "EMA"]})
    settings = GridV2Settings(
        enabled_variants=("plain",),
        enabled_axes=("maType3", "regimeErLength"),
    )
    first = build_grid_v2_plan(load_config(), settings, base_params=base_params)
    second = build_grid_v2_plan(load_config(), settings, base_params=base_params)

    assert first.deduped_candidate_count == 6
    assert first.parameter_domains["maType3"].values == ("EMA", "SMA")
    assert first.parameter_domains["regimeErLength"].values == (20, 30, 40)
    for index in range(first.deduped_candidate_count):
        left = first.candidate_for_index(index)
        right = second.candidate_for_index(index)
        assert left.candidate_id == index + 1
        assert left.semantic_key == right.semantic_key
        assert dict(left.params) == dict(right.params)
    payload = first.candidate_table.semantic_payload_for_index(0)
    assert payload["params"]["useRegime"] is True
    assert payload["params"]["maType3"] == "EMA"


def test_signal_only_cache_estimate_uses_four_bool_rows_and_no_float_dataprep(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_variants=("emergency",), enabled_axes=("regimeErLength", "emergencySlPct")),
        base_params=merged_reference_params(REFERENCE_B),
    )
    estimate = estimate_grid_v2_cache(plan, df, trade_start_idx, hooks)

    assert plan.deduped_candidate_count == 6
    assert estimate.signal_combo_count == 3
    assert estimate.dataprep_combo_count == 3
    assert estimate.physical_dataprep_stack_rows == 0
    assert estimate.bytes_per_signal_combo == len(df) * 4
    assert estimate.bytes_per_dataprep_combo == 0


def _assert_float_equal(actual, expected):
    actual = float(actual)
    expected = float(expected)
    if math.isnan(expected):
        assert math.isnan(actual)
    elif math.isinf(expected):
        assert math.isinf(actual) and (actual > 0.0) == (expected > 0.0)
    else:
        assert actual == pytest.approx(expected, rel=1e-9, abs=1e-12)


def _assert_rows_equal(compiled_row, reference_row):
    assert compiled_row.candidate_id == reference_row.candidate_id
    assert compiled_row.variant_name == reference_row.variant_name
    assert compiled_row.total_trades == reference_row.total_trades
    assert compiled_row.winning_trades == reference_row.winning_trades
    assert compiled_row.losing_trades == reference_row.losing_trades
    assert compiled_row.max_consecutive_losses == reference_row.max_consecutive_losses
    _assert_float_equal(compiled_row.net_profit_pct, reference_row.net_profit_pct)
    _assert_float_equal(compiled_row.max_drawdown_pct, reference_row.max_drawdown_pct)
    _assert_float_equal(compiled_row.romad, reference_row.romad)
    _assert_float_equal(compiled_row.profit_factor, reference_row.profit_factor)
    _assert_float_equal(compiled_row.win_rate_pct, reference_row.win_rate_pct)
    _assert_float_equal(compiled_row.gross_profit, reference_row.gross_profit)
    _assert_float_equal(compiled_row.gross_loss, reference_row.gross_loss)
    _assert_float_equal(compiled_row.final_balance, reference_row.final_balance)


@pytest.mark.skipif(JIT_DISABLED, reason="compiled Grid V2 parity requires Numba JIT")
@pytest.mark.parametrize(
    ("reference_id", "enabled_variants", "enabled_axes"),
    [
        (REFERENCE_A, ("plain",), ("regimeErLength",)),
        (REFERENCE_B, ("emergency",), ("regimeErLength", "emergencySlPct")),
    ],
)
def test_compiled_grid_v2_subset_matches_reference_backend(
    prepared_data,
    hooks,
    reference_id,
    enabled_variants,
    enabled_axes,
):
    if not compiled_batch_available():
        pytest.skip("Compiled Grid V2 unavailable in this process; rerun in a fresh JIT-on process")
    df, trade_start_idx = prepared_data
    base_params = merged_reference_params(reference_id)
    config = load_config()
    compiled_plan = build_grid_v2_plan(
        config,
        GridV2Settings(enabled_variants=enabled_variants, enabled_axes=enabled_axes, prefer_compiled=True, top_n=2),
        base_params=base_params,
    )
    reference_plan = build_grid_v2_plan(
        config,
        GridV2Settings(enabled_variants=enabled_variants, enabled_axes=enabled_axes, prefer_compiled=False, top_n=2),
        base_params=base_params,
    )
    indices = tuple(range(compiled_plan.deduped_candidate_count))

    compiled = execute_grid_v2_candidates(compiled_plan, df, trade_start_idx, hooks, indices)
    reference = execute_grid_v2_candidates(reference_plan, df, trade_start_idx, hooks, indices)

    assert compiled.metadata["backend_kind"] == COMPILED_BATCH_KIND
    assert compiled.metadata["compiled_batch_used"] is True
    assert len(compiled.rows) == len(reference.rows) == len(indices)
    for compiled_row, reference_row in zip(compiled.rows, reference.rows):
        _assert_rows_equal(compiled_row, reference_row)


def test_selected_candidates_match_public_v2_runner(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_variants=("plain",), enabled_axes=("regimeErLength",), prefer_compiled=False, top_n=2),
        base_params=merged_reference_params(REFERENCE_A),
    )
    indices = tuple(range(plan.deduped_candidate_count))
    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, indices)

    for row in result.rows:
        candidate = plan.candidate_for_index(row.candidate_id - 1)
        params = hooks.normalize_params(dict(candidate.params))
        data = hooks.build_execution_data(df, params)
        run = run_v2_strategy(data=data, profile=plan.profile, params=params, trade_start_idx=trade_start_idx)
        direct = run.strategy_result
        assert row.total_trades == direct.total_trades
        assert row.winning_trades == direct.winning_trades
        _assert_float_equal(row.net_profit_pct, direct.net_profit_pct)
        _assert_float_equal(row.profit_factor, direct.profit_factor)
        _assert_float_equal(row.max_drawdown_pct, direct.max_drawdown_pct)
