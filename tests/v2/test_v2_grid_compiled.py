from __future__ import annotations

from copy import deepcopy
import math
import os

import numpy as np
import pandas as pd
import pytest

from core.engine_v2.compiled_kernel import (
    OUTPUT_FINAL_BALANCE,
    OUTPUT_GROSS_LOSS,
    OUTPUT_GROSS_PROFIT,
    OUTPUT_LOSING_TRADES,
    OUTPUT_MAX_CONSECUTIVE_LOSSES,
    OUTPUT_MAX_DRAWDOWN_PCT,
    OUTPUT_NET_PROFIT_PCT,
    OUTPUT_PROFIT_FACTOR,
    OUTPUT_ROMAD,
    OUTPUT_TOTAL_TRADES,
    OUTPUT_WINNING_TRADES,
    OUTPUT_WIN_RATE_PCT,
    compiled_batch_available,
    evaluate_compiled_batch,
)
from core.engine_v2.contracts import Signals
from core.engine_v2.kernel import ExecutionData
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from core.grid_v2 import (
    GridV2Settings,
    GridV2StrategyHooks,
    build_grid_v2_plan,
    deterministic_candidate_subset_indices,
    execute_grid_v2_candidates,
)
from strategies.s06_r_trend_v02_b2 import strategy as s06_b2_strategy
from strategies.s06_r_trend_v02_b2.strategy import load_config, normalized_params

from s06_b2_test_helpers import merged_reference_params, prepared_reference_dataset

COMPILED_SUBSET_LIMIT = 240


pytestmark = pytest.mark.skipif(
    not compiled_batch_available(),
    reason="Numba compiled V2 Grid path is unavailable in this process.",
)


@pytest.fixture(scope="module")
def prepared_data():
    return prepared_reference_dataset()


@pytest.fixture(scope="module")
def hooks():
    return GridV2StrategyHooks.from_strategy(s06_b2_strategy)


def _assert_float_equal(actual, expected):
    actual = float(actual)
    if expected is None:
        assert math.isnan(actual)
        return
    expected = float(expected)
    if math.isnan(expected):
        assert math.isnan(actual)
    elif math.isinf(expected):
        assert math.isinf(actual) and (actual > 0.0) == (expected > 0.0)
    else:
        assert actual == pytest.approx(expected, rel=1e-9, abs=1e-12)


def _assert_rows_equal(compiled_row, reference_row):
    assert compiled_row.candidate_id == reference_row.candidate_id
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


def _config_with_rounding(price_rounding: str) -> dict:
    config = deepcopy(load_config())
    config["execution"]["priceRounding"] = price_rounding
    return config


def _required_subset_indices(plan) -> tuple[int, ...]:
    first_by_variant: dict[str, int] = {}
    default_like: list[int] = []
    defaults = load_config()["parameters"]
    default_params = {
        name: spec.get("default")
        for name, spec in defaults.items()
        if isinstance(spec, dict) and "default" in spec
    }
    for candidate in plan.candidates:
        first_by_variant.setdefault(candidate.variant_name, candidate.candidate_id - 1)
        if all(candidate.params.get(name) == default_params.get(name) for name in candidate.axis_param_names):
            default_like.append(candidate.candidate_id - 1)
    return (
        0,
        len(plan.candidates) - 1,
        *first_by_variant.values(),
        *default_like,
    )


@pytest.mark.parametrize("price_rounding", ["none", "tick_outward"])
def test_compiled_grid_v2_batch_matches_reference_batch_for_certification_subset(
    prepared_data,
    hooks,
    price_rounding,
):
    assert os.environ.get("NUMBA_DISABLE_JIT") not in {"1", "true", "True"}
    df, trade_start_idx = prepared_data
    base_params = merged_reference_params("reference_b_trend_bracket")
    config = _config_with_rounding(price_rounding)
    compiled_plan = build_grid_v2_plan(
        config,
        GridV2Settings(
            prefer_compiled=True,
            top_n=6,
        ),
        base_params=base_params,
    )
    reference_plan = build_grid_v2_plan(
        config,
        GridV2Settings(
            prefer_compiled=False,
            top_n=6,
        ),
        base_params=base_params,
    )
    indices = deterministic_candidate_subset_indices(
        len(compiled_plan.candidates),
        COMPILED_SUBSET_LIMIT,
        required_indices=_required_subset_indices(compiled_plan),
    )

    compiled = execute_grid_v2_candidates(compiled_plan, df, trade_start_idx, hooks, indices)
    reference = execute_grid_v2_candidates(reference_plan, df, trade_start_idx, hooks, indices)

    assert compiled.metadata["backend_kind"] == "compiled_numba"
    assert compiled.metadata["compiled_batch_used"] is True
    assert reference.metadata["backend_kind"] == "reference"
    assert len(compiled.rows) == COMPILED_SUBSET_LIMIT
    assert {row.variant_name for row in compiled.rows} == {"bracket", "trail"}
    for compiled_row, reference_row in zip(compiled.rows, reference.rows):
        _assert_rows_equal(compiled_row, reference_row)


def test_compiled_grid_v2_worker_count_is_deterministic(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    base_params = merged_reference_params("reference_b_trend_bracket")
    config = _config_with_rounding("none")
    settings = {
        "enabled_variants": ("bracket",),
        "enabled_axes": ("stopX", "stopRR"),
        "prefer_compiled": True,
        "top_n": 0,
    }
    one_worker_plan = build_grid_v2_plan(
        config,
        GridV2Settings(**settings, compiled_workers=1),
        base_params=base_params,
    )
    many_worker_plan = build_grid_v2_plan(
        config,
        GridV2Settings(**settings, compiled_workers=2),
        base_params=base_params,
    )
    indices = (0, 1, 5, len(one_worker_plan.candidates) - 1)

    one_worker = execute_grid_v2_candidates(one_worker_plan, df, trade_start_idx, hooks, indices)
    many_workers = execute_grid_v2_candidates(many_worker_plan, df, trade_start_idx, hooks, indices)

    assert one_worker.metadata["compiled_workers"] == 1
    assert many_workers.metadata["compiled_workers"] == 2
    for left, right in zip(one_worker.rows, many_workers.rows):
        _assert_rows_equal(left, right)


def _data(
    *,
    open_,
    high,
    low,
    close,
    long=None,
    short=None,
    atr=None,
    rolling_low=None,
    rolling_high=None,
    trail_long=None,
    trail_short=None,
):
    length = len(open_)
    return ExecutionData(
        timestamps=tuple(pd.date_range("2025-01-01", periods=length, freq="30min", tz="UTC")),
        open=np.array(open_, dtype=float),
        high=np.array(high, dtype=float),
        low=np.array(low, dtype=float),
        close=np.array(close, dtype=float),
        signals=Signals(
            long_entries=np.array(long if long is not None else [False] * length, dtype=bool),
            short_entries=np.array(short if short is not None else [False] * length, dtype=bool),
        ),
        atr=np.array(atr if atr is not None else [0.0] * length, dtype=float),
        rolling_low=np.array(rolling_low if rolling_low is not None else low, dtype=float),
        rolling_high=np.array(rolling_high if rolling_high is not None else high, dtype=float),
        trail_long=np.array(trail_long if trail_long is not None else [np.nan] * length, dtype=float),
        trail_short=np.array(trail_short if trail_short is not None else [np.nan] * length, dtype=float),
    )


def _edge_params(**overrides):
    params = normalized_params(
        {
            "entryMode": "Reversal @ Triangle",
            "enableLong": True,
            "enableShort": True,
            "fastLength": 21,
            "fastSmooth": 7,
            "slowLength": 112,
            "slowSmooth": 3,
            "thresholdOS": 20,
            "thresholdOB": 20,
            "stopX": 0.0,
            "stopRR": 2.0,
            "stopLP": 2,
            "stopMaxPct": 10.0,
            "stopMaxDays": 4,
            "riskPerTrade": 100.0,
            "contractSize": 1.0,
            "useTrailMA": False,
            "trailRR": 1.0,
            "trailMAType": "SMA",
            "trailMALength": 150,
            "trailMAOffsetEx": 0.0,
            "initialCapital": 100.0,
            "commissionPct": 0.0,
            "tickSize": 0.01,
            "dateFilter": False,
        }
    )
    params.update(overrides)
    return params


def _assert_compiled_matches_direct_reference(data: ExecutionData, params: dict):
    profile = parse_execution_profile(load_config())
    compiled = evaluate_compiled_batch(
        data=data,
        profile=profile,
        params_batch=[params],
        trade_start_idx=0,
    ).outputs[0]
    reference = run_v2_strategy(
        data=data,
        profile=profile,
        params=params,
        trade_start_idx=0,
    ).strategy_result

    _assert_float_equal(compiled[OUTPUT_NET_PROFIT_PCT], reference.net_profit_pct)
    _assert_float_equal(compiled[OUTPUT_MAX_DRAWDOWN_PCT], reference.max_drawdown_pct)
    _assert_float_equal(compiled[OUTPUT_ROMAD], reference.romad)
    _assert_float_equal(compiled[OUTPUT_PROFIT_FACTOR], reference.profit_factor)
    reference_win_rate = (
        float(reference.winning_trades) / float(reference.total_trades) * 100.0
        if reference.total_trades
        else 0.0
    )
    _assert_float_equal(compiled[OUTPUT_WIN_RATE_PCT], reference_win_rate)
    _assert_float_equal(compiled[OUTPUT_GROSS_PROFIT], reference.gross_profit)
    _assert_float_equal(compiled[OUTPUT_GROSS_LOSS], reference.gross_loss)
    _assert_float_equal(compiled[OUTPUT_FINAL_BALANCE], reference.balance_curve[-1])
    assert int(compiled[OUTPUT_TOTAL_TRADES]) == reference.total_trades
    assert int(compiled[OUTPUT_WINNING_TRADES]) == reference.winning_trades
    assert int(compiled[OUTPUT_LOSING_TRADES]) == reference.losing_trades

    consecutive = 0
    max_consecutive = 0
    for trade in reference.trades:
        if trade.net_pnl <= 0.0:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0
    assert int(compiled[OUTPUT_MAX_CONSECUTIVE_LOSSES]) == max_consecutive


def test_compiled_grid_v2_edge_cases_match_direct_reference_runner():
    no_trade = _data(
        open_=[100.0, 100.0, 100.0],
        high=[100.0, 101.0, 101.0],
        low=[99.0, 99.0, 99.0],
        close=[100.0, 100.0, 100.0],
    )
    _assert_compiled_matches_direct_reference(no_trade, _edge_params())

    zero_loss = _data(
        open_=[100.0, 100.0],
        high=[100.0, 106.0],
        low=[97.0, 99.0],
        close=[100.0, 105.0],
        long=[True, False],
        rolling_low=[97.0, 99.0],
    )
    _assert_compiled_matches_direct_reference(zero_loss, _edge_params(stopRR=1.0))

    max_days_strict_boundary = _data(
        open_=[100.0, 100.0, 101.0],
        high=[100.0, 101.0, 101.0],
        low=[97.0, 99.0, 99.0],
        close=[100.0, 100.0, 102.0],
        long=[True, False, False],
        rolling_low=[97.0, 99.0, 99.0],
    )
    _assert_compiled_matches_direct_reference(
        max_days_strict_boundary,
        _edge_params(stopRR=10.0, stopMaxDays=1.0 / 48.0),
    )

    episodic_drawdown = _data(
        open_=[100.0, 100.0, 100.0, 108.0, 108.0],
        high=[100.0, 106.0, 100.0, 108.0, 109.0],
        low=[97.0, 99.0, 97.0, 102.0, 102.0],
        close=[100.0, 105.0, 100.0, 108.0, 102.0],
        long=[True, False, True, False, False],
        rolling_low=[97.0, 99.0, 97.0, 102.0, 102.0],
    )
    _assert_compiled_matches_direct_reference(episodic_drawdown, _edge_params(stopRR=1.0))
