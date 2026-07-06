from __future__ import annotations

import math
import os
from pathlib import Path

import pytest

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.grid_v2 import (
    GridV2Settings,
    GridV2StrategyHooks,
    build_grid_v2_plan,
    deterministic_candidate_subset_indices,
    execute_grid_v2_candidates,
)
from core.optuna_engine import OptimizationConfig

from strategies.s06_r_trend_v02.strategy import S06RTrendV02
from strategies.s06_r_trend_v02_b2 import strategy as s06_b2_strategy
from strategies.s06_r_trend_v02_b2.strategy import load_config


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
TRADING_START = "2025-08-01T00:00:00+00:00"
TRADING_END = "2025-12-01T00:00:00+00:00"
GRID_PARAMS = (
    "stopX",
    "stopRR",
    "stopLP",
    "stopMaxPct",
    "stopMaxDays",
    "trailRR",
    "trailMAType",
    "trailMALength",
    "trailMAOffsetEx",
)
T1_SUBSET_LIMIT = 240


def _fast_grid():
    # The V1 fast-grid oracle is imported lazily with JIT disabled. This test
    # compares V1/V2 semantics, while V1 owns separate compiled-vs-interpreted
    # tests. The setting is process-global, so this helper is called only by
    # V1-oracle tests and after the V2 compiled Grid test has run.
    os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
    try:
        import numba

        numba.config.DISABLE_JIT = True
    except Exception:
        pass
    from strategies.s06_r_trend_v02 import fast_grid

    return fast_grid


@pytest.fixture(scope="module")
def prepared_data():
    if not DATA_PATH.exists():
        pytest.skip(f"Reference data not found: {DATA_PATH}")
    return prepare_dataset_with_warmup(
        load_data(str(DATA_PATH)),
        TRADING_START,
        TRADING_END,
        1000,
    )


@pytest.fixture(scope="module")
def hooks():
    return GridV2StrategyHooks.from_strategy(s06_b2_strategy)


def _v1_config() -> OptimizationConfig:
    return OptimizationConfig(
        csv_file=str(DATA_PATH),
        strategy_id="s06_r_trend_v02",
        enabled_params={name: True for name in GRID_PARAMS} | {"thresholdOS": False, "thresholdOB": False},
        param_ranges={},
        param_types={
            "thresholdOS": "int",
            "thresholdOB": "int",
            "stopX": "float",
            "stopRR": "float",
            "stopLP": "int",
            "stopMaxPct": "float",
            "stopMaxDays": "int",
            "trailRR": "float",
            "trailMAType": "select",
            "trailMALength": "int",
            "trailMAOffsetEx": "float",
        },
        fixed_params=_v1_fixed_params(),
        warmup_bars=1000,
        optimization_mode="grid",
        objectives=["net_profit_pct"],
        grid_enabled_modes=["bracket", "trail"],
        grid_budget=1,
    )


def _v1_fixed_params() -> dict:
    return {
        "dateFilter": True,
        "start": TRADING_START,
        "end": TRADING_END,
        "entryMode": "Reversal @ Triangle",
        "enableLong": True,
        "enableShort": True,
        "fastLength": 21,
        "fastSmoothing": 7,
        "slowLength": 112,
        "slowSmoothing": 3,
        "thresholdOS": 20,
        "thresholdOB": 20,
        "stopX": 2.0,
        "stopRR": 3.0,
        "stopLP": 2,
        "stopMaxPct": 6.0,
        "stopMaxDays": 6,
        "riskPerTrade": 2.0,
        "contractSize": 0.01,
        "useTrailMA": True,
        "trailRR": 1.0,
        "trailMAType": "SMA",
        "trailMALength": 150,
        "trailMAOffsetEx": 0.0,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }


def _v2_base_params() -> dict:
    params = _v1_fixed_params()
    params["fastSmooth"] = params.pop("fastSmoothing")
    params["slowSmooth"] = params.pop("slowSmoothing")
    return params


def _v1_candidates():
    fast_grid = _fast_grid()
    config = _v1_config()
    space = fast_grid.build_parameter_space(config)
    allocation = fast_grid.build_allocation(config, space, None)
    return config, fast_grid.generate_candidates(config, space, allocation, seed=99).candidates


def _assert_float_close(actual, expected):
    if expected is None:
        assert math.isnan(float(actual))
        return
    actual = float(actual)
    expected = float(expected)
    if math.isnan(expected):
        assert math.isnan(actual)
    elif math.isinf(expected):
        assert math.isinf(actual) and (actual > 0) == (expected > 0)
    else:
        assert actual == pytest.approx(expected, rel=1e-9, abs=1e-12)


def _default_like_indices(plan) -> tuple[int, ...]:
    base = _v2_base_params()
    indices: list[int] = []
    for candidate in plan.candidates:
        if all(candidate.params.get(name) == base.get(name) for name in candidate.axis_param_names):
            indices.append(candidate.candidate_id - 1)
    return tuple(indices)


def test_s06_t1_reference_subset_metrics_match_v1_fast_grid(prepared_data, hooks):
    fast_grid = _fast_grid()
    if not fast_grid.NUMBA_AVAILABLE:
        pytest.skip("Numba is required by the V1 fast-grid oracle")
    df, trade_start_idx = prepared_data
    v1_config, v1_source = _v1_candidates()
    v2_plan = build_grid_v2_plan(load_config(), base_params=_v2_base_params())
    indices = deterministic_candidate_subset_indices(
        len(v2_plan.candidates),
        T1_SUBSET_LIMIT,
        required_indices=(
            0,
            1,
            479,
            480,
            len(v2_plan.candidates) - 1,
            *_default_like_indices(v2_plan),
        ),
    )

    fast_data = fast_grid.prepare_fast_data(df, trade_start_idx, v1_source)
    v1_results = fast_grid.evaluate_candidates(
        fast_data,
        [v1_source[index] for index in indices],
        n_workers=1,
    )
    v2_result = execute_grid_v2_candidates(v2_plan, df, trade_start_idx, hooks, indices)

    assert [row.candidate_id for row in v2_result.rows] == [index + 1 for index in indices]
    for v1_row, v2_row in zip(v1_results, v2_result.rows):
        _assert_float_close(v2_row.net_profit_pct, v1_row.net_profit_pct)
        _assert_float_close(v2_row.max_drawdown_pct, v1_row.max_drawdown_pct)
        _assert_float_close(v2_row.romad, v1_row.romad)
        _assert_float_close(v2_row.profit_factor, v1_row.profit_factor)
        _assert_float_close(v2_row.win_rate_pct, v1_row.win_rate)
        assert v2_row.total_trades == v1_row.total_trades
        assert v2_row.winning_trades == v1_row.winning_trades
        assert v2_row.losing_trades == v1_row.losing_trades
        assert v2_row.max_consecutive_losses == v1_row.max_consecutive_losses
    assert v1_config.grid_enabled_modes == ["bracket", "trail"]


def _v2_to_v1_params(params: dict) -> dict:
    translated = dict(params)
    translated["fastSmoothing"] = translated.pop("fastSmooth")
    translated["slowSmoothing"] = translated.pop("slowSmooth")
    return translated


def _assert_trade_sequences_match(v2_trades, v1_trades):
    assert len(v2_trades) == len(v1_trades)
    for v2_trade, v1_trade in zip(v2_trades, v1_trades):
        assert v2_trade.direction == v1_trade.direction
        assert v2_trade.entry_time == v1_trade.entry_time
        assert v2_trade.exit_time == v1_trade.exit_time
        assert v2_trade.entry_price == pytest.approx(v1_trade.entry_price, abs=1e-9)
        assert v2_trade.exit_price == pytest.approx(v1_trade.exit_price, abs=1e-9)
        assert v2_trade.size == pytest.approx(v1_trade.size, abs=1e-9)
        assert v2_trade.net_pnl == pytest.approx(v1_trade.net_pnl, abs=1e-9)


def test_s06_t2_selected_candidates_match_v1_slow_strategy(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        base_params=_v2_base_params(),
    )
    indices = deterministic_candidate_subset_indices(
        len(plan.candidates),
        T1_SUBSET_LIMIT,
        required_indices=(
            0,
            1,
            479,
            480,
            len(plan.candidates) - 1,
            *_default_like_indices(plan),
        ),
    )
    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, indices)
    rows_by_id = {row.candidate_id: row for row in result.rows}
    top_rows = sorted(
        [row for row in result.rows if row.status == "ok"],
        key=lambda row: (-float(row.net_profit_pct), row.candidate_id),
    )[:6]
    coverage_ids = {
        next(row.candidate_id for row in result.rows if row.variant_name == "bracket"),
        next(row.candidate_id for row in result.rows if row.variant_name == "trail"),
        *[index + 1 for index in _default_like_indices(plan)],
    }
    selected_ids = list(dict.fromkeys([row.candidate_id for row in top_rows] + sorted(coverage_ids)))

    assert any(rows_by_id[candidate_id].variant_name == "bracket" for candidate_id in selected_ids)
    assert any(rows_by_id[candidate_id].variant_name == "trail" for candidate_id in selected_ids)

    for candidate_id in selected_ids:
        candidate = plan.candidates[candidate_id - 1]
        params = hooks.normalize_params(dict(candidate.params)) if hooks.normalize_params else candidate.params
        data = hooks.build_execution_data(df, params)
        from core.engine_v2.runner import run_v2_strategy

        v2 = run_v2_strategy(data=data, profile=plan.profile, params=params, trade_start_idx=trade_start_idx)
        v1 = S06RTrendV02.run(df, _v2_to_v1_params(dict(candidate.params)), trade_start_idx)
        _assert_trade_sequences_match(v2.strategy_result.trades, v1.trades)
