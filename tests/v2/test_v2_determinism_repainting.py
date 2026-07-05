from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import pytest

from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from strategies.s06_r_trend_v02_b2.signals import S06B2Params, build_indicator_arrays, build_s06_b2_execution_data
from strategies.s06_r_trend_v02_b2.strategy import load_config

from s06_b2_test_helpers import (
    BASELINE_END,
    BASELINE_START,
    merged_reference_params,
    profile_with_rounding,
    prepared_reference_dataset,
    run_public_reference,
    run_reference,
    run_signature,
    trade_signature,
)


def _trade_skeleton(result):
    return tuple(
        (
            trade.direction,
            pd.Timestamp(trade.entry_time),
            pd.Timestamp(trade.exit_time),
            trade.size,
        )
        for trade in result.trades
    )


def _trade_float_payload(result):
    return tuple(
        (
            trade.entry_price,
            trade.exit_price,
            trade.net_pnl,
        )
        for trade in result.trades
    )


def test_same_s06_reference_run_is_structurally_deterministic_in_process():
    first = run_reference("reference_b_trend_bracket")
    second = run_reference("reference_b_trend_bracket")

    assert run_signature(first) == run_signature(second)


def test_public_s06_adapter_is_repeatable_in_threaded_isolated_runs():
    with ThreadPoolExecutor(max_workers=4) as executor:
        signatures = list(
            executor.map(
                lambda _: trade_signature(run_public_reference("reference_b_trend_bracket")),
                range(4),
            )
        )

    assert signatures
    assert all(signature == signatures[0] for signature in signatures)


def test_prefix_invariance_for_signals_and_closed_decisions_before_cutoff():
    prefix_end = BASELINE_START + pd.Timedelta(days=45)
    params = merged_reference_params(
        "reference_b_trend_bracket",
        {"end": prefix_end.isoformat().replace("+00:00", "Z")},
    )
    parsed = S06B2Params.from_dict(params)
    full_prepared, trade_start_idx = prepared_reference_dataset()
    prefix_prepared = full_prepared.loc[full_prepared.index <= prefix_end].copy()

    full_data = build_s06_b2_execution_data(full_prepared, parsed)
    prefix_data = build_s06_b2_execution_data(prefix_prepared, parsed)
    prefix_len = len(prefix_prepared)

    np.testing.assert_array_equal(
        prefix_data.signals.long_entries,
        full_data.signals.long_entries[:prefix_len],
    )
    np.testing.assert_array_equal(
        prefix_data.signals.short_entries,
        full_data.signals.short_entries[:prefix_len],
    )

    config = load_config()
    config["execution"]["boundary"] = "none"
    profile = parse_execution_profile(config)
    prefix_run = run_v2_strategy(
        data=prefix_data,
        profile=profile,
        params=params,
        trade_start_idx=trade_start_idx,
    )
    full_run = run_v2_strategy(
        data=full_data,
        profile=profile,
        params=params,
        trade_start_idx=trade_start_idx,
    )

    prefix_closed = [
        trade
        for trade in prefix_run.strategy_result.trades
        if pd.Timestamp(trade.exit_time) <= prefix_end
    ]
    full_closed = [
        trade
        for trade in full_run.strategy_result.trades
        if pd.Timestamp(trade.exit_time) <= prefix_end
    ]
    assert trade_signature(type("Result", (), {"trades": prefix_closed})()) == trade_signature(
        type("Result", (), {"trades": full_closed})()
    )


@pytest.mark.parametrize(
    "reference_id",
    ["reference_b_trend_bracket", "reference_a_reversal_trail"],
)
def test_window_start_invariance_with_larger_warmup_uses_skeleton_exact_float_tolerant(reference_id):
    pinned = run_reference(reference_id, warmup_bars=1000)
    larger = run_reference(reference_id, warmup_bars=1500)

    assert _trade_skeleton(larger.strategy_result) == _trade_skeleton(pinned.strategy_result)
    for larger_values, pinned_values in zip(
        _trade_float_payload(larger.strategy_result),
        _trade_float_payload(pinned.strategy_result),
    ):
        assert larger_values == pytest.approx(pinned_values, rel=1e-9, abs=1e-12)

    params = S06B2Params.from_dict(merged_reference_params(reference_id))
    pinned_prepared, pinned_start_idx = prepared_reference_dataset(warmup_bars=1000)
    larger_prepared, larger_start_idx = prepared_reference_dataset(warmup_bars=1500)
    pinned_arrays = build_indicator_arrays(pinned_prepared, params)
    larger_arrays = build_indicator_arrays(larger_prepared, params)

    pinned_index = pinned_prepared.index[pinned_start_idx:]
    larger_index = larger_prepared.index[larger_start_idx:]
    shared_index = pinned_index.intersection(larger_index)
    assert shared_index[0] == BASELINE_START
    assert shared_index[-1] == BASELINE_END

    for name in ("long_signal", "short_signal"):
        pinned_signals = pd.Series(pinned_arrays[name][pinned_start_idx:], index=pinned_index).loc[shared_index]
        larger_signals = pd.Series(larger_arrays[name][larger_start_idx:], index=larger_index).loc[shared_index]
        np.testing.assert_array_equal(pinned_signals.to_numpy(dtype=bool), larger_signals.to_numpy(dtype=bool))


def test_warmup_region_signals_do_not_create_executable_orders():
    params = merged_reference_params("reference_b_trend_bracket")
    parsed = S06B2Params.from_dict(params)
    prepared, trade_start_idx = prepared_reference_dataset(warmup_bars=1000)
    warmup_only = prepared.iloc[:trade_start_idx].copy()
    data = build_s06_b2_execution_data(warmup_only, parsed)
    run = run_v2_strategy(
        data=data,
        profile=profile_with_rounding("none"),
        params=params,
        trade_start_idx=trade_start_idx,
    )

    assert run.strategy_result.trades == []
    assert run.standing_state.pending_entry_direction == 0
