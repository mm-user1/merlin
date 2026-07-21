from __future__ import annotations

import copy
import json
import math
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from core.engine_v2.compiled_kernel import (
    OUTPUT_FINAL_BALANCE,
    OUTPUT_FLAGS,
    OUTPUT_GROSS_LOSS,
    OUTPUT_GROSS_PROFIT,
    OUTPUT_LOSING_TRADES,
    OUTPUT_MAX_CONSECUTIVE_LOSSES,
    OUTPUT_MAX_DRAWDOWN_PCT,
    OUTPUT_MAX_NOTIONAL,
    OUTPUT_NET_PROFIT_PCT,
    OUTPUT_PROFIT_FACTOR,
    OUTPUT_ROMAD,
    OUTPUT_TOTAL_TRADES,
    OUTPUT_WINNING_TRADES,
    OUTPUT_WIN_RATE_PCT,
    OUTPUT_ZERO_SIZE_ENTRY_COUNT,
    compiled_batch_available,
)
from core.engine_v2.compiled_kernel_signal import (
    _pack_signal_config_arrays,
    _signal_mode_state,
    build_signal_stacked_execution_data,
    evaluate_compiled_signal_stacked_batch,
)
from core.engine_v2.contracts import Signals
from core.engine_v2.dataprep import build_signal_execution_data
from core.engine_v2.metrics_kernel import compute_core_metrics_from_balance_and_trades
from core.engine_v2.profile import active_mode_values, parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from core.grid_v2 import (
    COMPILED_BATCH_KIND,
    GridV2Settings,
    GridV2StrategyHooks,
    build_grid_v2_plan,
    estimate_grid_v2_cache,
    execute_grid_v2_candidates,
)

import s03_like_test_helpers as signal_fixture
from s03_like_test_helpers import fixture_config, normalized_params, synthetic_ohlc


BYTES_PER_MB = 1024.0 * 1024.0


@pytest.fixture(scope="module")
def signal_df():
    return synthetic_ohlc(
        [
            100.0,
            102.0,
            104.0,
            106.0,
            92.0,
            90.0,
            88.0,
            86.0,
            104.0,
            106.0,
            108.0,
            82.0,
            80.0,
            78.0,
            100.0,
            102.0,
        ]
    )


@pytest.fixture(scope="module")
def hooks():
    return GridV2StrategyHooks.from_strategy(signal_fixture)


def _base_params(**overrides):
    params = normalized_params(
        {
            "dateFilter": False,
            "start": None,
            "end": None,
            "maType3": "SMA",
            "maLength3": 2,
            "maOffset3": 0.0,
            "useCloseCount": True,
            "closeCountLong": 1,
            "closeCountShort": 1,
            "useTBands": False,
            "positionPct": 100.0,
            "contractSize": 0.01,
            "initialCapital": 100.0,
            "commissionPct": 0.0,
            "emergencySlPct": 10.0,
            "emergencySlUpdateBars": 2,
        }
    )
    params.update(overrides)
    return params


def _require_compiled_available():
    if not compiled_batch_available():
        pytest.skip("Compiled signal path is unavailable in this process.")


def _grid_plan(*, prefer_compiled: bool, top_n: int = 0, max_signal_cache_mb: float = 512.0):
    return build_grid_v2_plan(
        fixture_config(),
        GridV2Settings(
            enabled_axes=("maType3", "emergencySlPct"),
            top_n=top_n,
            prefer_compiled=prefer_compiled,
            max_signal_cache_mb=max_signal_cache_mb,
        ),
        base_params=_base_params(),
    )


def _assert_float_equal(actual, expected, *, rel=1e-9, abs_tol=1e-12):
    actual = float(actual)
    expected = float(expected)
    if math.isnan(expected):
        assert math.isnan(actual)
    elif math.isinf(expected):
        assert math.isinf(actual) and (actual > 0.0) == (expected > 0.0)
    else:
        assert actual == pytest.approx(expected, rel=rel, abs=abs_tol)


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


def _assert_output_matches_reference(data, params):
    profile = parse_execution_profile(fixture_config())
    stacked = build_signal_stacked_execution_data([data], [0])
    compiled = evaluate_compiled_signal_stacked_batch(
        stacked_data=stacked,
        profile=profile,
        params_batch=[params],
        trade_start_idx=0,
    ).outputs[0]
    reference = run_v2_strategy(data=data, profile=profile, params=params, trade_start_idx=0)
    result = reference.strategy_result
    core = compute_core_metrics_from_balance_and_trades(
        result.balance_curve,
        result.trades,
        initial_balance=params.get("initialCapital", 100.0),
    )

    _assert_float_equal(compiled[OUTPUT_NET_PROFIT_PCT], core.net_profit_pct)
    _assert_float_equal(compiled[OUTPUT_MAX_DRAWDOWN_PCT], core.max_drawdown_pct)
    _assert_float_equal(compiled[OUTPUT_ROMAD], core.romad)
    _assert_float_equal(compiled[OUTPUT_PROFIT_FACTOR], core.profit_factor)
    _assert_float_equal(compiled[OUTPUT_WIN_RATE_PCT], core.win_rate_pct)
    _assert_float_equal(compiled[OUTPUT_GROSS_PROFIT], core.gross_profit)
    _assert_float_equal(compiled[OUTPUT_GROSS_LOSS], core.gross_loss)
    _assert_float_equal(compiled[OUTPUT_FINAL_BALANCE], core.final_balance)
    _assert_float_equal(compiled[OUTPUT_MAX_NOTIONAL], reference.guardrail_summary.max_notional)
    assert int(compiled[OUTPUT_TOTAL_TRADES]) == core.total_trades
    assert int(compiled[OUTPUT_WINNING_TRADES]) == core.winning_trades
    assert int(compiled[OUTPUT_LOSING_TRADES]) == core.losing_trades
    assert int(compiled[OUTPUT_ZERO_SIZE_ENTRY_COUNT]) == reference.guardrail_summary.zero_size_entry_count
    assert int(compiled[OUTPUT_FLAGS]) == reference.guardrail_summary.flags

    consecutive = 0
    max_consecutive = 0
    for trade in result.trades:
        if trade.net_pnl <= 0.0:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0
    assert int(compiled[OUTPUT_MAX_CONSECUTIVE_LOSSES]) == max_consecutive


def _signal_data(
    *,
    open_,
    high,
    low,
    close,
    long=None,
    short=None,
    long_exit=None,
    short_exit=None,
):
    df = synthetic_ohlc(close, opens=open_, highs=high, lows=low)
    length = len(df)
    return build_signal_execution_data(
        df,
        signals=Signals(
            long_entries=np.array(long if long is not None else [False] * length, dtype=bool),
            short_entries=np.array(short if short is not None else [False] * length, dtype=bool),
            long_exits=None if long_exit is None else np.array(long_exit, dtype=bool),
            short_exits=None if short_exit is None else np.array(short_exit, dtype=bool),
        ),
    )


def test_signal_topology_plan_counts_identity_and_inactive_emergency_axis():
    plan = _grid_plan(prefer_compiled=False)

    assert plan.deduped_candidate_count == 6
    assert plan.per_variant_counts == {"plain": 2, "emergency": 4}
    assert plan.candidate_table.axis_names_by_variant["plain"] == ("maType3",)
    assert plan.candidate_table.axis_names_by_variant["emergency"] == ("maType3", "emergencySlPct")
    assert "useEmergencySL" not in plan.candidate_table.axis_names

    plain_payload = json.loads(plan.candidate_for_index(0).semantic_key)
    emergency_payload = json.loads(plan.candidate_for_index(2).semantic_key)
    assert plain_payload["variant"] == "plain"
    assert "emergencySlPct" not in plain_payload["params"]
    assert emergency_payload["variant"] == "emergency"
    assert emergency_payload["params"]["emergencySlPct"] == 10.0


def test_signal_grid_reference_rows_match_direct_runs(signal_df, hooks):
    plan = _grid_plan(prefer_compiled=False, top_n=2)
    indices = (0, 2, 5)

    result = execute_grid_v2_candidates(plan, signal_df, 0, hooks, indices)

    assert [row.candidate_id for row in result.rows] == [index + 1 for index in indices]
    assert result.metadata["backend_kind"] == "reference"
    for row, candidate_index in zip(result.rows, indices):
        params = hooks.normalize_params(dict(plan.candidate_table.params_for_index(candidate_index)))
        data = hooks.build_execution_data(signal_df, params)
        direct = run_v2_strategy(data=data, profile=plan.profile, params=params, trade_start_idx=0)
        core = compute_core_metrics_from_balance_and_trades(
            direct.strategy_result.balance_curve,
            direct.strategy_result.trades,
            initial_balance=params["initialCapital"],
        )
        _assert_float_equal(row.net_profit_pct, core.net_profit_pct)
        _assert_float_equal(row.max_drawdown_pct, core.max_drawdown_pct)
        assert row.total_trades == core.total_trades


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled signal path required.")
def test_signal_grid_compiled_rows_match_reference_and_use_mapping_metadata(signal_df, hooks):
    _require_compiled_available()
    compiled_plan = _grid_plan(prefer_compiled=True, top_n=2)
    reference_plan = _grid_plan(prefer_compiled=False, top_n=2)
    indices = (0, 1, 2, 3, 4, 5)

    compiled = execute_grid_v2_candidates(compiled_plan, signal_df, 0, hooks, indices)
    reference = execute_grid_v2_candidates(reference_plan, signal_df, 0, hooks, indices)

    assert compiled.metadata["backend_kind"] == COMPILED_BATCH_KIND
    assert compiled.metadata["compiled_execution_mode"] == "stacked"
    assert compiled.metadata["compiled_config_packing"] == "mapping"
    assert compiled.metadata["stack_dataprep_nbytes"] == 0
    assert compiled.metadata["stack_row_count"] == compiled.cache_estimate.physical_signal_stack_rows
    assert len(compiled.selected) == 2
    for compiled_row, reference_row in zip(compiled.rows, reference.rows):
        _assert_rows_equal(compiled_row, reference_row)


def test_signal_grid_cache_estimate_uses_exit_signal_rows_and_no_float_dataprep(signal_df, hooks):
    plan = _grid_plan(prefer_compiled=True)
    estimate = estimate_grid_v2_cache(plan, signal_df, 0, hooks)

    assert estimate.signal_combo_count == 2
    assert estimate.dataprep_combo_count == 4
    assert estimate.physical_signal_stack_rows == 4
    assert estimate.physical_dataprep_stack_rows == 0
    assert estimate.bytes_per_signal_combo == len(signal_df) * 4 * np.dtype(np.bool_).itemsize
    assert estimate.bytes_per_dataprep_combo == 0
    assert estimate.estimated_dataprep_mb == 0.0

    single_estimate = estimate_grid_v2_cache(plan, signal_df, 0, hooks, candidate_indices=(0,))
    limit_mb = (single_estimate.estimated_total_mb + estimate.estimated_total_mb) / 2.0
    limited_plan = build_grid_v2_plan(
        fixture_config(),
        GridV2Settings(
            enabled_axes=("maType3", "emergencySlPct"),
            max_signal_cache_mb=limit_mb,
        ),
        base_params=_base_params(),
    )
    result = execute_grid_v2_candidates(limited_plan, signal_df, 0, hooks)

    assert len(result.rows) == plan.deduped_candidate_count
    assert result.metadata["chunk_count"] > 1
    assert result.metadata["max_chunk_estimated_mb"] <= limit_mb + 1e-12
    assert result.metadata["signal_stack_rows_built"] >= result.metadata["signal_stack_rows_peak"]


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled signal path required.")
def test_signal_grid_chunked_compiled_rows_match_monolithic_and_selected_enrichment(signal_df, hooks):
    _require_compiled_available()
    monolithic_plan = _grid_plan(prefer_compiled=True, top_n=2)
    estimate = estimate_grid_v2_cache(monolithic_plan, signal_df, 0, hooks)
    single_estimate = estimate_grid_v2_cache(monolithic_plan, signal_df, 0, hooks, candidate_indices=(0,))
    limit_mb = (single_estimate.estimated_total_mb + estimate.estimated_total_mb) / 2.0
    chunked_plan = _grid_plan(
        prefer_compiled=True,
        top_n=2,
        max_signal_cache_mb=limit_mb,
    )

    monolithic = execute_grid_v2_candidates(monolithic_plan, signal_df, 0, hooks)
    chunked = execute_grid_v2_candidates(chunked_plan, signal_df, 0, hooks)

    assert chunked.metadata["chunk_count"] > 1
    assert chunked.metadata["max_chunk_estimated_mb"] <= limit_mb + 1e-12
    assert chunked.metadata["compiled_config_packing"] == "mapping"
    assert chunked.metadata["params_materialized"] < len(chunked.rows)
    for chunked_row, monolithic_row in zip(chunked.rows, monolithic.rows):
        _assert_rows_equal(chunked_row, monolithic_row)
    assert [item.row.candidate_id for item in chunked.selected] == [
        item.row.candidate_id for item in monolithic.selected
    ]
    for chunked_item, monolithic_item in zip(chunked.selected, monolithic.selected):
        assert chunked_item.metrics == monolithic_item.metrics


def test_signal_stacked_payload_defaults_absent_exits_and_validates_shared_market():
    data = _signal_data(
        open_=[100.0, 101.0],
        high=[100.0, 101.0],
        low=[100.0, 101.0],
        close=[100.0, 101.0],
        long=[True, False],
    )
    stacked = build_signal_stacked_execution_data([data], [0, 0])

    assert stacked.row_count == 1
    assert stacked.candidate_count == 2
    assert stacked.dataprep_nbytes == 0
    assert not stacked.long_exits.any()
    assert not stacked.short_exits.any()

    mismatched = _signal_data(
        open_=[100.0, 102.0],
        high=[100.0, 102.0],
        low=[100.0, 102.0],
        close=[100.0, 101.0],
        long=[True, False],
    )
    with pytest.raises(ValueError, match="shared OHLC/timestamps"):
        build_signal_stacked_execution_data([data, mismatched], [0, 1])
    with pytest.raises(ValueError, match="out-of-range"):
        build_signal_stacked_execution_data([data], [1])


def test_signal_execution_data_preserves_datetime_index_and_tuple_timestamps_still_stack():
    data = _signal_data(
        open_=[100.0, 101.0],
        high=[100.0, 101.0],
        low=[100.0, 101.0],
        close=[100.0, 101.0],
        long=[True, False],
    )

    assert isinstance(data.timestamps, pd.DatetimeIndex)

    tuple_data = replace(data, timestamps=tuple(data.timestamps))
    stacked = build_signal_stacked_execution_data([tuple_data], [0])

    assert stacked.row_count == 1
    assert stacked.candidate_count == 1


def test_signal_mode_state_and_config_packer_validation():
    profile = parse_execution_profile(fixture_config())
    plain = normalized_params({"useEmergencySL": False, "emergencySlPct": 0.0, "emergencySlUpdateBars": 0})
    emergency = normalized_params({"useEmergencySL": True})

    assert _signal_mode_state(active_mode_values(profile, plain)) == (False, True, False)
    packed_plain = _pack_signal_config_arrays(profile, [plain])
    assert packed_plain["emergency_stop_enabled"][0] == np.bool_(False)
    assert packed_plain["emergency_sl_pct"][0] == pytest.approx(0.0)

    with pytest.raises(ValueError, match="emergencySlPct"):
        _pack_signal_config_arrays(profile, [{**emergency, "emergencySlPct": 0.0}])

    bad_config = copy.deepcopy(fixture_config())
    bad_config["execution"]["target"] = "rr"
    bad_profile = parse_execution_profile(bad_config)
    with pytest.raises(ValueError, match="target"):
        _signal_mode_state(active_mode_values(bad_profile, bad_profile.parameter_defaults))


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled signal path required.")
@pytest.mark.parametrize(
    ("data", "params"),
    [
        (
            _signal_data(
                open_=[100.0, 100.0, 101.0, 102.0],
                high=[100.0, 100.0, 101.0, 102.0],
                low=[100.0, 100.0, 101.0, 102.0],
                close=[100.0, 101.0, 102.0, 103.0],
                long=[True, False, False, False],
            ),
            _base_params(useEmergencySL=False, contractSize=1.0),
        ),
        (
            _signal_data(
                open_=[100.0, 100.0, 98.0, 100.0],
                high=[100.0, 100.0, 100.0, 100.0],
                low=[100.0, 90.0, 97.0, 100.0],
                close=[100.0, 100.0, 100.0, 100.0],
                long=[True, False, True, False],
            ),
            _base_params(useEmergencySL=True, emergencySlPct=1.0, emergencySlUpdateBars=2, contractSize=1.0),
        ),
        (
            _signal_data(
                open_=[100.0, 100.0, 101.0, 102.0],
                high=[100.0, 100.0, 101.0, 102.0],
                low=[100.0, 100.0, 101.0, 102.0],
                close=[100.0, 100.0, 101.0, 102.0],
                long=[True, False, False, False],
                long_exit=[False, True, False, False],
            ),
            _base_params(useEmergencySL=False, contractSize=1.0),
        ),
        (
            _signal_data(
                open_=[100.0, 100.0, 100.0],
                high=[100.0, 100.0, 100.0],
                low=[100.0, 100.0, 100.0],
                close=[100.0, 100.0, 100.0],
            ),
            _base_params(useEmergencySL=False, contractSize=1.0),
        ),
        (
            _signal_data(
                open_=[100.0, 100.0],
                high=[100.0, 100.0],
                low=[100.0, 100.0],
                close=[100.0, 100.0],
                long=[True, False],
            ),
            _base_params(useEmergencySL=False, initialCapital=1.0, contractSize=10.0),
        ),
    ],
)
def test_compiled_signal_direct_cases_match_reference_runner(data, params):
    _require_compiled_available()
    _assert_output_matches_reference(data, params)


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled signal path required.")
def test_compiled_signal_worker_count_is_deterministic(signal_df, hooks):
    _require_compiled_available()
    plan_one = build_grid_v2_plan(
        fixture_config(),
        GridV2Settings(
            enabled_axes=("maType3", "emergencySlPct"),
            prefer_compiled=True,
            compiled_workers=1,
            top_n=0,
        ),
        base_params=_base_params(),
    )
    plan_two = build_grid_v2_plan(
        fixture_config(),
        GridV2Settings(
            enabled_axes=("maType3", "emergencySlPct"),
            prefer_compiled=True,
            compiled_workers=2,
            top_n=0,
        ),
        base_params=_base_params(),
    )
    indices = (0, 2, 5)

    one = execute_grid_v2_candidates(plan_one, signal_df, 0, hooks, indices)
    two = execute_grid_v2_candidates(plan_two, signal_df, 0, hooks, indices)

    assert one.metadata["compiled_workers"] == 1
    assert two.metadata["compiled_workers"] == 2
    for left, right in zip(one.rows, two.rows):
        _assert_rows_equal(left, right)
