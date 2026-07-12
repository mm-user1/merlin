from __future__ import annotations

import math

import pytest

from core.engine_v2.metrics_kernel import compute_core_metrics_from_balance_and_trades
from core.engine_v2.compiled_kernel import compiled_batch_available
from core.engine_v2.runner import run_v2_strategy
from core.grid_v2 import (
    GridV2Settings,
    GridV2StrategyHooks,
    build_grid_v2_plan,
    estimate_grid_v2_cache,
    execute_grid_v2_candidates,
)
from strategies.s06_r_trend_v02_b2 import strategy as s06_b2_strategy
from strategies.s06_r_trend_v02_b2.strategy import load_config

from s06_b2_test_helpers import merged_reference_params, prepared_reference_dataset

BYTES_PER_MB = 1024.0 * 1024.0


@pytest.fixture(scope="module")
def prepared_data():
    return prepared_reference_dataset()


@pytest.fixture(scope="module")
def hooks():
    return GridV2StrategyHooks.from_strategy(s06_b2_strategy)


def _assert_float_equal(actual, expected, *, abs_tol=1e-10):
    actual = float(actual)
    expected = float(expected)
    if math.isnan(expected):
        assert math.isnan(actual)
    elif math.isinf(expected):
        assert math.isinf(actual) and (actual > 0) == (expected > 0)
    else:
        assert actual == pytest.approx(expected, abs=abs_tol)


def _direct_run(plan, candidate, df, trade_start_idx, hooks):
    params = hooks.normalize_params(dict(candidate.params)) if hooks.normalize_params else candidate.params
    data = hooks.build_execution_data(df, params)
    return run_v2_strategy(data=data, profile=plan.profile, params=params, trade_start_idx=trade_start_idx)


def _assert_row_matches_direct(row, direct, initial_balance):
    core = compute_core_metrics_from_balance_and_trades(
        direct.strategy_result.balance_curve,
        direct.strategy_result.trades,
        initial_balance=initial_balance,
    )
    _assert_float_equal(row.net_profit_pct, core.net_profit_pct)
    _assert_float_equal(row.max_drawdown_pct, core.max_drawdown_pct)
    _assert_float_equal(row.romad, core.romad)
    _assert_float_equal(row.profit_factor, core.profit_factor)
    _assert_float_equal(row.win_rate_pct, core.win_rate_pct)
    assert row.total_trades == core.total_trades
    _assert_float_equal(row.final_balance, core.final_balance)


def test_one_candidate_grid_result_equals_direct_v2_run(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_variants=("bracket",), enabled_axes=(), top_n=1),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )

    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks)
    assert len(result.rows) == 1
    direct = _direct_run(plan, plan.candidates[0], df, trade_start_idx, hooks)
    _assert_row_matches_direct(result.rows[0], direct, plan.candidates[0].params["initialCapital"])
    assert len(result.selected) == 1
    assert result.metadata["backend_kind"] in {"compiled_numba", "reference"}
    assert result.metadata["compiled_batch_available"] is compiled_batch_available()


def test_multi_candidate_grid_result_equals_repeated_direct_v2_runs(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_variants=("bracket",), enabled_axes=("stopX",), top_n=2),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )
    indices = (0, 1, 4)

    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, indices)
    assert [row.candidate_id for row in result.rows] == [index + 1 for index in indices]
    for row, index in zip(result.rows, indices):
        direct = _direct_run(plan, plan.candidates[index], df, trade_start_idx, hooks)
        _assert_row_matches_direct(row, direct, plan.candidates[index].params["initialCapital"])


def test_signal_and_dataprep_cache_diagnostics(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_variants=("bracket",), enabled_axes=("stopX",), top_n=1),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )

    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, (0, 1, 2))
    assert result.cache_stats.signal_misses == 1
    assert result.cache_stats.signal_hits == 2
    assert result.cache_stats.dataprep_misses == 1
    assert result.cache_stats.dataprep_hits == 2

    threshold_plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_variants=("bracket",), enabled_axes=("thresholdOS", "stopX"), top_n=1),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )
    threshold_result = execute_grid_v2_candidates(threshold_plan, df, trade_start_idx, hooks, (0, 5))
    assert threshold_result.cache_stats.signal_misses == 2
    assert threshold_result.cache_stats.signal_hits == 0


def test_execute_reuses_cache_key_estimate_without_changing_cache_diagnostics(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_variants=("bracket",), enabled_axes=("thresholdOS", "stopX"), top_n=1),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )
    indices = (0, 1, 5)

    expected = estimate_grid_v2_cache(plan, df, trade_start_idx, hooks, indices)
    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, indices)

    assert result.cache_estimate == expected
    assert result.cache_stats.signal_misses == expected.signal_combo_count
    assert result.cache_stats.signal_hits == len(indices) - expected.signal_combo_count
    assert result.cache_stats.dataprep_misses == expected.dataprep_combo_count
    assert result.cache_stats.dataprep_hits == len(indices) - expected.dataprep_combo_count


def test_s06_b2_full_table_cache_group_counts(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(top_n=0),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )

    estimate = estimate_grid_v2_cache(plan, df, trade_start_idx, hooks)

    assert plan.deduped_candidate_count == 48_480
    assert estimate.signal_combo_count == 1
    assert estimate.dataprep_combo_count == 162
    assert estimate.physical_signal_stack_rows == 162
    assert estimate.physical_dataprep_stack_rows == 162
    assert estimate.output_candidate_count == 48_480
    assert plan._candidates_cache is None
    assert plan.candidate_table.legacy_candidates_materialized_count == 0


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled path required for table metadata.")
def test_compiled_execution_uses_table_without_legacy_or_canonical_materialization(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(
            enabled_variants=("bracket",),
            enabled_axes=("stopX", "stopRR"),
            top_n=1,
            prefer_compiled=True,
        ),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )

    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks)

    assert result.metadata["candidate_table_used"] is True
    assert result.metadata["legacy_candidates_materialized"] == 0
    assert result.metadata["canonical_identities_materialized"] == 0
    assert result.metadata["semantic_keys_materialized"] == len(result.rows)
    assert result.metadata["compiled_execution_mode"] == "stacked"
    assert result.metadata["compiled_config_packing"] == "mapping"
    assert plan._candidates_cache is None
    assert plan.candidate_table.legacy_candidates_materialized_count == 0
    assert plan.candidate_table.canonical_identities_materialized_count == 0


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled path required for table config packing.")
def test_compiled_execution_can_use_table_config_packing_when_requested(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(
            enabled_variants=("bracket",),
            enabled_axes=("stopX",),
            top_n=0,
            prefer_compiled=True,
            compiled_config_packing="table",
        ),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )

    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, candidate_indices=(0, 1))

    assert result.metadata["compiled_execution_mode"] == "stacked"
    assert result.metadata["compiled_config_packing"] == "table"
    assert [row.candidate_id for row in result.rows] == [1, 2]


def test_cache_estimate_uses_worker_multiplier_and_hard_limit(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    base_settings = GridV2Settings(
        enabled_variants=("bracket",),
        enabled_axes=("stopX",),
        worker_multiplier=1,
    )
    doubled_settings = GridV2Settings(
        enabled_variants=("bracket",),
        enabled_axes=("stopX",),
        worker_multiplier=2,
    )
    plan = build_grid_v2_plan(load_config(), base_settings, base_params=merged_reference_params("reference_b_trend_bracket"))
    doubled = build_grid_v2_plan(load_config(), doubled_settings, base_params=merged_reference_params("reference_b_trend_bracket"))

    one = estimate_grid_v2_cache(plan, df, trade_start_idx, hooks, (0, 1))
    two = estimate_grid_v2_cache(doubled, df, trade_start_idx, hooks, (0, 1))
    assert two.estimated_total_mb == pytest.approx(one.estimated_total_mb * 2.0)
    assert one.physical_signal_stack_rows == one.dataprep_combo_count
    assert one.physical_dataprep_stack_rows == one.dataprep_combo_count
    assert one.output_candidate_count == 2
    assert one.estimated_output_mb > 0.0
    assert one.estimated_shared_market_mb > 0.0

    tiny_limit = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_variants=("bracket",), enabled_axes=(), max_signal_cache_mb=0.000001),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )
    build_calls = []

    def forbidden_build(*args, **kwargs):  # noqa: ARG001
        build_calls.append(1)
        raise AssertionError("build_execution_data must not run after a failed cache estimate")

    blocked_hooks = GridV2StrategyHooks(
        build_execution_data=forbidden_build,
        normalize_params=hooks.normalize_params,
        label=hooks.label,
        signal_param_names=hooks.signal_param_names,
        dataprep_param_names=hooks.dataprep_param_names,
        function_fingerprint=hooks.function_fingerprint,
    )
    with pytest.raises(MemoryError, match="cache estimate"):
        execute_grid_v2_candidates(tiny_limit, df, trade_start_idx, blocked_hooks)
    assert build_calls == []


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled stacked path required for stack metadata.")
def test_compiled_stack_estimate_covers_actual_allocation(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    settings = GridV2Settings(
        enabled_variants=("bracket",),
        enabled_axes=("stopX", "stopLP"),
        top_n=0,
        prefer_compiled=True,
    )
    plan = build_grid_v2_plan(
        load_config(),
        settings,
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )

    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks)

    assert result.metadata["compiled_execution_mode"] == "stacked"
    assert result.metadata["stack_row_count"] == result.cache_estimate.dataprep_combo_count
    assert result.metadata["stack_candidate_count"] == len(result.rows)
    actual_nbytes = int(result.metadata["stack_total_nbytes"])
    estimated_nbytes = result.cache_estimate.estimated_total_mb * BYTES_PER_MB
    assert estimated_nbytes >= actual_nbytes


def test_cache_limit_uses_true_stack_estimate_before_building_data(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    base_settings = GridV2Settings(
        enabled_variants=("bracket",),
        enabled_axes=("stopX", "stopLP"),
        top_n=0,
    )
    plan = build_grid_v2_plan(
        load_config(),
        base_settings,
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )
    estimate = estimate_grid_v2_cache(plan, df, trade_start_idx, hooks)
    limited = build_grid_v2_plan(
        load_config(),
        GridV2Settings(
            enabled_variants=("bracket",),
            enabled_axes=("stopX", "stopLP"),
            top_n=0,
            max_signal_cache_mb=max(0.000001, estimate.estimated_total_mb - 0.000001),
        ),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )
    build_calls = []

    def forbidden_build(*args, **kwargs):  # noqa: ARG001
        build_calls.append(1)
        raise AssertionError("build_execution_data must not run after a failed stack estimate")

    blocked_hooks = GridV2StrategyHooks(
        build_execution_data=forbidden_build,
        normalize_params=hooks.normalize_params,
        label=hooks.label,
        signal_param_names=hooks.signal_param_names,
        dataprep_param_names=hooks.dataprep_param_names,
        function_fingerprint=hooks.function_fingerprint,
    )
    with pytest.raises(MemoryError, match="cache estimate"):
        execute_grid_v2_candidates(limited, df, trade_start_idx, blocked_hooks)
    assert build_calls == []


def test_tick_outward_grid_result_matches_direct_v2_tick_run(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(
            enabled_variants=("bracket",),
            enabled_axes=(),
            price_rounding="tick_outward",
            top_n=1,
        ),
        base_params=merged_reference_params("reference_b_trend_bracket"),
    )

    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks)
    direct = _direct_run(plan, plan.candidates[0], df, trade_start_idx, hooks)
    _assert_row_matches_direct(result.rows[0], direct, plan.candidates[0].params["initialCapital"])
