import math
import shutil
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.grid_engine import (
    GridSettings,
    allocate_mode_budgets,
    apply_fast_grid_dsr,
    build_grid_dsr_results,
    calculate_grid_display_scores,
    format_compact_count,
    parse_grid_budget,
    rank_grid_results,
)
from core.backtest_engine import StrategyResult
from core.metrics import calculate_basic
from core.optuna_engine import ConstraintSpec, OptimizationConfig, OptimizationResult
from core.storage import load_study_from_db, save_grid_study_to_db
from ui.server_services import _build_optimization_config
from strategies.s03_reversal_v10 import fast_grid


def _grid_config(**overrides):
    enabled = {
        "maType3": True,
        "maLength3": True,
        "maOffset3": True,
        "useCloseCount": True,
        "useTBands": True,
        "closeCountLong": True,
        "closeCountShort": True,
        "tBandLongPct": True,
        "tBandShortPct": True,
    }
    ranges = {
        "maLength3": (3, 5, 1),
        "maOffset3": (0, 1, 0.5),
        "closeCountLong": (1, 2, 1),
        "closeCountShort": (1, 1, 1),
        "tBandLongPct": (0.5, 1.0, 0.5),
        "tBandShortPct": (0.5, 1.0, 0.5),
    }
    fixed = {
        "maType3_options": ["SMA", "EMA"],
        "useCloseCount_options": [True, False],
        "useTBands_options": [True, False],
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }
    param_types = {
        "maType3": "select",
        "maLength3": "int",
        "maOffset3": "float",
        "useCloseCount": "bool",
        "useTBands": "bool",
        "closeCountLong": "int",
        "closeCountShort": "int",
        "tBandLongPct": "float",
        "tBandShortPct": "float",
    }
    payload = {
        "csv_file": "unused.csv",
        "strategy_id": "s03_reversal_v10",
        "enabled_params": enabled,
        "param_ranges": ranges,
        "param_types": param_types,
        "fixed_params": fixed,
        "worker_processes": 1,
        "warmup_bars": 20,
        "optimization_mode": "grid",
        "objectives": ["net_profit_pct"],
        "primary_objective": None,
        "grid_budget": 25,
        "grid_seed": 42,
        "grid_top_candidates": 5,
    }
    payload.update(overrides)
    return OptimizationConfig(**payload)


def test_grid_budget_parsing_and_formatting():
    assert parse_grid_budget("200k") == 200_000
    assert parse_grid_budget("1.5M") == 1_500_000
    assert parse_grid_budget(12) == 12
    assert format_compact_count(1_420_000) == "1.42M"

    for invalid in ["", "0", "-1", "20kk", "1.2.3m", "abc", math.inf]:
        with pytest.raises(ValueError):
            parse_grid_budget(invalid)


def test_mode_allocation_caps_and_redistributes_deterministically():
    allocation = allocate_mode_budgets(
        {"cc_only": 2, "tbands_only": 5, "both": 100},
        20,
        method="auto_sqrt_space",
        min_quota=0.10,
    )

    assert allocation.actual_budget == 20
    assert allocation.unused_budget == 0
    assert allocation.mode_budgets["cc_only"] == 2
    assert allocation.mode_budgets["tbands_only"] <= 5
    assert sum(allocation.mode_budgets.values()) == 20

    full = allocate_mode_budgets(
        {"cc_only": 2, "tbands_only": 5, "both": 100},
        "1m",
    )
    assert full.actual_budget == 107
    assert full.unused_budget == 1_000_000 - 107
    assert full.mode_budgets == {"cc_only": 2, "tbands_only": 5, "both": 100}


def test_s03_parameter_space_uses_modes_dependency_collapse_and_offset():
    config = _grid_config()
    space = fast_grid.build_parameter_space(config)

    assert space.mode_space_sizes == {
        "cc_only": 36,
        "tbands_only": 72,
        "both": 144,
    }
    assert space.total_space_size == 252
    assert space.ma_offsets == [0.0, 0.5, 1.0]


def test_s03_parameter_space_rejects_vwap_and_false_false_space():
    config = _grid_config(fixed_params={
        "maType3_options": ["VWAP"],
        "useCloseCount_options": [True],
        "useTBands_options": [False],
    })
    with pytest.raises(ValueError, match="VWAP"):
        fast_grid.build_parameter_space(config)


def test_server_build_config_parses_grid_fields_and_select_options():
    payload = {
        "optimization_mode": "grid",
        "enabled_params": {
            "maType3": True,
            "maLength3": True,
            "useCloseCount": True,
            "useTBands": True,
        },
        "param_ranges": {
            "maType3": {"type": "select", "values": ["SMA", "EMA"]},
            "maLength3": [3, 4, 1],
            "useCloseCount": {"type": "select", "values": [True]},
            "useTBands": {"type": "select", "values": [False]},
        },
        "fixed_params": {
            "dateFilter": False,
            "contractSize": 0.01,
            "initialCapital": 100.0,
            "commissionPct": 0.05,
        },
        "param_types": {
            "maType3": "select",
            "maLength3": "int",
            "useCloseCount": "bool",
            "useTBands": "bool",
        },
        "objectives": ["net_profit_pct"],
        "grid_budget": "1.5k",
        "grid_seed": 77,
        "grid_top_candidates": 3,
        "grid_allocation_method": "manual",
        "grid_manual_percents": {"cc_only": 100, "tbands_only": 0, "both": 0},
    }

    config = _build_optimization_config(
        "dummy.csv",
        payload,
        worker_processes=2,
        strategy_id="s03_reversal_v10",
        warmup_bars=1000,
    )

    assert config.optimization_mode == "grid"
    assert config.grid_budget == 1500
    assert config.grid_seed == 77
    assert config.grid_top_candidates == 3
    assert config.fixed_params["maType3_options"] == ["SMA", "EMA"]
    assert config.fixed_params["useCloseCount_options"] == [True]

    config = _grid_config(fixed_params={
        "maType3_options": ["SMA"],
        "useCloseCount_options": [False],
        "useTBands_options": [False],
    })
    with pytest.raises(ValueError, match="false"):
        fast_grid.build_parameter_space(config)


def test_candidate_generation_exact_mode_coverage_uses_full_enumeration():
    config = _grid_config(
        fixed_params={
            "maType3_options": ["SMA"],
            "useCloseCount_options": [True],
            "useTBands_options": [False],
            "contractSize": 0.01,
            "initialCapital": 100.0,
            "commissionPct": 0.05,
        },
        param_ranges={
            "maLength3": (3, 4, 1),
            "maOffset3": (0, 0, 1),
            "closeCountLong": (1, 2, 1),
            "closeCountShort": (1, 1, 1),
            "tBandLongPct": (0.5, 0.5, 0.5),
            "tBandShortPct": (0.5, 0.5, 0.5),
        },
    )
    space = fast_grid.build_parameter_space(config)
    allocation = allocate_mode_budgets(space.mode_space_sizes, space.mode_space_sizes["cc_only"])
    candidates = fast_grid.generate_candidates(config, space, allocation, seed=7)

    assert candidates.diagnostics["enumerated_full_modes"] == ["cc_only"]
    assert candidates.diagnostics["lhs_modes"] == []
    assert len(candidates.candidates) == space.mode_space_sizes["cc_only"]
    assert len({c.semantic_key for c in candidates.candidates}) == len(candidates.candidates)


def test_candidate_generation_is_deterministic_and_seeded():
    config = _grid_config(grid_budget=12)
    space = fast_grid.build_parameter_space(config)
    allocation = allocate_mode_budgets(space.mode_space_sizes, 12)

    first = fast_grid.generate_candidates(config, space, allocation, seed=11).candidates
    second = fast_grid.generate_candidates(config, space, allocation, seed=11).candidates
    different = fast_grid.generate_candidates(config, space, allocation, seed=12).candidates

    assert [c.semantic_key for c in first] == [c.semantic_key for c in second]
    assert [c.semantic_key for c in first] != [c.semantic_key for c in different]
    assert '"maOffset3"' in first[0].semantic_key


def _sample_df(rows=180):
    index = pd.date_range("2024-01-01", periods=rows, freq="h", tz="UTC")
    x = np.linspace(0, 18 * np.pi, rows)
    close = 100 + np.sin(x) * 3 + np.sin(x * 0.33) * 1.5
    high = close + 0.8
    low = close - 0.8
    return pd.DataFrame(
        {
            "Open": close,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": np.full(rows, 1000.0),
        },
        index=index,
    )


def _synthetic_grid_result(candidate_id, *, net_profit, win_rate=50.0, sharpe=0.0, grid_rank=None):
    result = OptimizationResult(
        params={"candidate": candidate_id},
        net_profit_pct=net_profit,
        max_drawdown_pct=1.0,
        total_trades=5,
        winning_trades=3,
        losing_trades=2,
        win_rate=win_rate,
        romad=net_profit,
        profit_factor=1.5,
        sharpe_ratio=sharpe,
        optuna_trial_number=candidate_id,
        objective_values=[net_profit],
        constraints_satisfied=True,
    )
    result.candidate_id = candidate_id
    result.semantic_key = f"candidate:{candidate_id}"
    result.grid_rank = grid_rank or candidate_id
    result.dsr_track_length = 12
    result.dsr_skewness = 0.0
    result.dsr_kurtosis = 3.0
    return result


def test_fast_grid_legacy_drawdown_ignores_unrecovered_tail_like_slow_metrics():
    balance_path = [100.0, 90.0, 105.0, 80.0]
    slow = calculate_basic(
        StrategyResult(
            trades=[],
            equity_curve=list(balance_path),
            balance_curve=list(balance_path),
            timestamps=list(pd.date_range("2025-01-01", periods=len(balance_path), freq="D", tz="UTC")),
        ),
        initial_balance=100.0,
    )

    assert slow.max_drawdown_pct == pytest.approx(10.0)
    assert fast_grid.legacy_recovered_max_drawdown_pct(balance_path) == pytest.approx(slow.max_drawdown_pct)


def test_grid_dsr_selects_only_from_previous_module_top_n():
    pytest.importorskip("scipy.stats")
    ranked = [_synthetic_grid_result(idx, net_profit=100.0 - idx, sharpe=0.1, grid_rank=idx) for idx in range(1, 51)]
    outside = _synthetic_grid_result(99_999, net_profit=1.0, sharpe=4.0, grid_rank=99_999)
    reference = ranked + [outside]
    eligible = ranked[:20]
    eligible[-1].sharpe_ratio = 2.0

    dsr_selected, dsr_summary = apply_fast_grid_dsr(
        eligible,
        reference_results=reference,
        top_k=5,
    )
    union = build_grid_dsr_results(ranked, limit=5)

    assert dsr_summary["dsr_n_trials"] == 51
    assert outside.candidate_id not in {item.candidate_id for item in dsr_selected}
    assert {item.grid_rank for item in dsr_selected} <= set(range(1, 21))
    assert {item.trial_number for item in union} <= set(range(1, 21))
    assert {item.optuna_rank for item in union} <= set(range(1, 21))

    from core.grid_engine import _union_selected_candidates

    selected_union = _union_selected_candidates(ranked[:50], dsr_selected)
    assert len(selected_union) == 50
    assert all(item.grid_rank <= 50 for item in selected_union)
    assert all("dsr" in item.selection_sources for item in dsr_selected)


def test_grid_dsr_trial_count_uses_all_finite_sharpe_candidates():
    pytest.importorskip("scipy.stats")
    ranked = [
        _synthetic_grid_result(1, net_profit=10.0, sharpe=0.1, grid_rank=1),
        _synthetic_grid_result(2, net_profit=8.0, sharpe=math.nan, grid_rank=2),
        _synthetic_grid_result(3, net_profit=1.0, sharpe=1.0, grid_rank=3),
    ]

    _selected, dsr_summary = apply_fast_grid_dsr(
        ranked[:1],
        reference_results=ranked,
        top_k=1,
    )

    assert dsr_summary["dsr_n_trials"] == 2


def test_grid_display_score_does_not_filter_objective_or_dsr_union():
    objective = _synthetic_grid_result(1, net_profit=10.0, sharpe=0.1, grid_rank=1)
    objective.selection_sources = ["objective"]
    objective.is_objective_selected = True

    dsr_only = _synthetic_grid_result(3, net_profit=1.0, sharpe=2.5, grid_rank=3)
    dsr_only.selection_sources = ["dsr"]
    dsr_only.is_dsr_selected = True
    dsr_only.dsr_rank = 1

    score_config = {
        "filter_enabled": True,
        "min_score_threshold": 99.0,
        "enabled_metrics": {},
        "weights": {},
    }

    results = calculate_grid_display_scores([objective, dsr_only], score_config)

    assert [item.candidate_id for item in results] == [1, 3]
    assert results[0].grid_rank == 1
    assert results[0].selection_sources == ["objective"]
    assert results[1].selection_sources == ["dsr"]
    assert results[1].dsr_rank == 1


def test_multi_objective_ranking_uses_secondary_objective_before_semantic_key():
    constraints = [ConstraintSpec(metric="total_trades", threshold=10.0, enabled=True)]
    lower_key = _synthetic_grid_result(1, net_profit=10.0, win_rate=40.0)
    higher_key = _synthetic_grid_result(2, net_profit=10.0, win_rate=80.0)
    lower_key.semantic_key = "aaa"
    higher_key.semantic_key = "zzz"

    ranked = rank_grid_results(
        [lower_key, higher_key],
        objectives=["net_profit_pct", "win_rate"],
        primary_objective="net_profit_pct",
        constraints=constraints,
    )

    assert [item.candidate_id for item in ranked] == [2, 1]


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required for fast Grid parity")
@pytest.mark.parametrize(
    ("mode", "use_close_count", "use_tbands"),
    [
        ("cc_only", True, False),
        ("tbands_only", False, True),
        ("both", True, True),
    ],
)
def test_fast_grid_selected_candidates_validate_against_slow_path(mode, use_close_count, use_tbands):
    df = _sample_df()
    params = {
        "dateFilter": True,
        "maType3": "SMA",
        "maLength3": 5,
        "maOffset3": 0.5,
        "useCloseCount": use_close_count,
        "useTBands": use_tbands,
        "closeCountLong": 1,
        "closeCountShort": 1,
        "tBandLongPct": 0.2,
        "tBandShortPct": 0.2,
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }
    candidate = fast_grid.GridCandidate(
        candidate_id=1,
        mode=mode,
        params=params,
        semantic_key=fast_grid.candidate_semantic_key(mode, params),
        generation_mode="test",
        diversity_group=f"{mode}|SMA|5",
    )
    data = fast_grid.prepare_fast_data(df, 10, [candidate])
    fast_results = fast_grid.evaluate_candidates(data, [candidate])
    validated = fast_grid.validate_selected_candidates(
        df,
        10,
        fast_results,
        tolerances={
            "net_profit_pct_abs": 0.001,
            "max_drawdown_pct_abs": 0.001,
            "romad_abs": 0.005,
            "win_rate_abs": 0.001,
            "total_trades_abs": 0.0,
            "winning_trades_abs": 0.0,
            "losing_trades_abs": 0.0,
            "max_consecutive_losses_abs": 0.0,
        },
        fail_on_error=True,
    )

    assert validated[0].validation_status == "passed"
    assert validated[0].winning_trades == fast_results[0].winning_trades
    assert validated[0].losing_trades == fast_results[0].losing_trades


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required for fast Grid evaluation")
def test_fast_grid_dsr_disabled_does_not_populate_dsr_fields():
    df = _sample_df()
    params = {
        "dateFilter": True,
        "maType3": "SMA",
        "maLength3": 5,
        "maOffset3": 0.5,
        "useCloseCount": True,
        "useTBands": True,
        "closeCountLong": 1,
        "closeCountShort": 1,
        "tBandLongPct": 0.2,
        "tBandShortPct": 0.2,
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }
    candidate = fast_grid.GridCandidate(
        candidate_id=1,
        mode="both",
        params=params,
        semantic_key=fast_grid.candidate_semantic_key("both", params),
        generation_mode="test",
        diversity_group="both|SMA|5",
    )

    data = fast_grid.prepare_fast_data(df, 10, [candidate])
    result = fast_grid.evaluate_candidates(data, [candidate], needs_dsr=False)[0]

    assert not hasattr(result, "dsr_track_length")
    assert "dsr_track_length" not in result.fast_metrics


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required for fast Grid parallelism")
def test_fast_grid_parallel_evaluation_is_deterministic():
    df = _sample_df(rows=2200)
    config = _grid_config(grid_budget=8)
    space = fast_grid.build_parameter_space(config)
    allocation = allocate_mode_budgets(space.mode_space_sizes, 8)
    candidates = fast_grid.generate_candidates(config, space, allocation, seed=42).candidates
    data = fast_grid.prepare_fast_data(df, 10, candidates)

    single = fast_grid.evaluate_candidates(data, candidates, n_workers=1, needs_dsr=True)
    parallel = fast_grid.evaluate_candidates(data, candidates, n_workers=2, needs_dsr=True)

    assert [item.candidate_id for item in single] == [item.candidate_id for item in parallel]
    attrs = [
        "net_profit_pct",
        "max_drawdown_pct",
        "total_trades",
        "winning_trades",
        "losing_trades",
        "win_rate",
        "profit_factor",
        "romad",
        "sharpe_ratio",
        "dsr_track_length",
        "dsr_skewness",
        "dsr_kurtosis",
    ]
    for left, right in zip(single, parallel):
        for attr in attrs:
            left_value = getattr(left, attr, None)
            right_value = getattr(right, attr, None)
            if left_value is None or right_value is None:
                assert left_value is None and right_value is None
            else:
                assert left_value == pytest.approx(right_value, abs=0.0)


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required for fast Grid DSR metrics")
def test_fast_grid_dsr_sharpe_matches_slow_path():
    df = _sample_df(rows=1800)
    params = {
        "dateFilter": True,
        "maType3": "SMA",
        "maLength3": 5,
        "maOffset3": 0.5,
        "useCloseCount": True,
        "useTBands": True,
        "closeCountLong": 1,
        "closeCountShort": 1,
        "tBandLongPct": 0.2,
        "tBandShortPct": 0.2,
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }
    candidate = fast_grid.GridCandidate(
        candidate_id=1,
        mode="both",
        params=params,
        semantic_key=fast_grid.candidate_semantic_key("both", params),
        generation_mode="test",
        diversity_group="both|SMA|5",
    )
    data = fast_grid.prepare_fast_data(df, 10, [candidate])
    fast_result = fast_grid.evaluate_candidates(data, [candidate], needs_dsr=True)[0]
    slow_result = fast_grid.validate_selected_candidates(
        df,
        10,
        [fast_result],
        tolerances={
            "net_profit_pct_abs": 0.001,
            "max_drawdown_pct_abs": 0.001,
            "romad_abs": 0.005,
            "win_rate_abs": 0.001,
            "total_trades_abs": 0.0,
            "winning_trades_abs": 0.0,
            "losing_trades_abs": 0.0,
            "max_consecutive_losses_abs": 0.0,
        },
        fail_on_error=True,
    )[0]

    assert fast_result.sharpe_ratio is not None
    assert slow_result.sharpe_ratio is not None
    assert fast_result.sharpe_ratio == pytest.approx(slow_result.sharpe_ratio, abs=1e-9)


def test_save_and_load_grid_study_roundtrip():
    temp_dir = Path("tests/.tmp_grid_files") / uuid.uuid4().hex
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_path = temp_dir / "data.csv"
    csv_path.write_text("time,open,high,low,close,Volume\n", encoding="utf-8")

    result = OptimizationResult(
        params={"maType3": "SMA", "maLength3": 5},
        net_profit_pct=12.5,
        max_drawdown_pct=3.0,
        total_trades=4,
        winning_trades=3,
        losing_trades=1,
        win_rate=75.0,
        romad=4.16,
        profit_factor=2.0,
        optuna_trial_number=9,
        objective_values=[12.5],
        constraints_satisfied=True,
    )
    result.candidate_id = 9
    result.semantic_key = '{"maLength3":5,"maOffset3":0,"maType3":"SMA","mode":"cc_only"}'
    result.param_key = result.semantic_key
    result.grid_rank = 1
    result.grid_mode_name = "cc_only"
    result.grid_generation_mode = "full"
    result.diversity_group = "cc_only|SMA|5"
    result.validation_status = "passed"
    result.fast_metrics = {"net_profit_pct": 12.5}
    result.validation_diffs = {"net_profit_pct": {"diff": 0.0}}
    result.selection_sources = ["objective", "dsr"]
    result.is_objective_selected = True
    result.is_dsr_selected = True
    result.dsr_probability = 0.91
    result.dsr_rank = 1
    result.dsr_skewness = 0.1
    result.dsr_kurtosis = 3.2
    result.dsr_track_length = 12
    result.dsr_luck_share_pct = 14.0

    config = _grid_config(
        csv_file=str(csv_path),
        fixed_params={"dateFilter": False, "maType3_options": ["SMA"]},
    )
    summary = {
        "requested_budget": 10,
        "actual_budget": 1,
        "completed_trials": 1,
        "pareto_front_size": None,
        "grid": {
            "preview": {"coverage_pct": 10.0},
            "dsr": {
                "enabled": True,
                "top_k": 1,
                "dsr_n_trials": 9,
                "dsr_mean_sharpe": 0.2,
                "dsr_var_sharpe": 0.03,
            },
        },
    }

    try:
        study_id = save_grid_study_to_db(
            config=config,
            grid_settings=GridSettings(requested_budget=10, top_candidates=1),
            grid_summary=summary,
            trial_results=[result],
            csv_file_path=str(csv_path),
            start_time=0,
        )
        loaded = load_study_from_db(study_id)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    assert loaded["study"]["optimization_mode"] == "grid"
    assert loaded["study"]["optimizer_mode"] == "grid"
    assert loaded["trials"][0]["candidate_id"] == 9
    assert loaded["trials"][0]["grid_rank"] == 1
    assert loaded["trials"][0]["validation_status"] == "passed"
    assert loaded["study"]["dsr_n_trials"] == 9
    assert loaded["trials"][0]["dsr_probability"] == pytest.approx(0.91)
    assert loaded["trials"][0]["selection_sources"] == ["objective", "dsr"]
