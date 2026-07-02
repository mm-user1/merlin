import math
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.grid_engine import (
    GridSettings,
    allocate_mode_budgets,
    apply_fast_grid_dsr,
    build_grid_dsr_results,
    calculate_grid_display_scores,
    compute_grid_dsr_benchmark,
    default_grid_enabled_modes,
    format_compact_count,
    get_fast_grid_backend_metadata,
    normalize_diversity_group_fields,
    parse_grid_budget,
    rank_grid_candidates_by_dsr,
    rank_grid_results,
    resolve_grid_selection_config,
    run_grid_optimization,
    validate_grid_config,
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
        "grid_fast_objectives": ["net_profit_pct", "max_drawdown_pct"],
        "grid_fast_primary_objective": "max_drawdown_pct",
        "grid_slow_refinement_enabled": True,
        "grid_slow_objectives": ["sharpe_ratio", "ulcer_index"],
        "grid_slow_primary_objective": "sharpe_ratio",
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
    assert config.grid_fast_objectives == ["net_profit_pct", "max_drawdown_pct"]
    assert config.grid_fast_primary_objective == "max_drawdown_pct"
    assert config.grid_slow_refinement_enabled is True
    assert config.grid_slow_objectives == ["sharpe_ratio", "ulcer_index"]
    assert config.grid_slow_primary_objective == "sharpe_ratio"

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


def test_grid_dsr_tie_break_uses_previous_module_rank_before_grid_rank(monkeypatch):
    import core.grid_engine as grid_engine

    first_previous = _synthetic_grid_result(10, net_profit=10.0, sharpe=1.0, grid_rank=50)
    second_previous = _synthetic_grid_result(11, net_profit=10.0, sharpe=1.0, grid_rank=1)
    monkeypatch.setattr(grid_engine, "calculate_expected_max_sharpe", lambda *_args, **_kwargs: 0.0)
    monkeypatch.setattr(grid_engine, "calculate_dsr", lambda *_args, **_kwargs: 0.5)

    selected, _summary = apply_fast_grid_dsr(
        [first_previous, second_previous],
        reference_results=[first_previous, second_previous],
        top_k=2,
    )

    assert [item.candidate_id for item in selected] == [10, 11]
    assert [item.dsr_source_rank for item in selected] == [1, 2]
    assert [item.grid_rank for item in selected] == [50, 1]


def test_grid_dsr_helper_matches_apply_fast_grid_dsr(monkeypatch):
    import core.grid_engine as grid_engine

    monkeypatch.setattr(grid_engine, "calculate_expected_max_sharpe", lambda *_args, **_kwargs: 0.25)
    monkeypatch.setattr(grid_engine, "calculate_dsr", lambda sr, sr0, *_args: sr - sr0)

    apply_candidates = [
        _synthetic_grid_result(1, net_profit=10.0, sharpe=0.5, grid_rank=1),
        _synthetic_grid_result(2, net_profit=9.0, sharpe=1.0, grid_rank=2),
        _synthetic_grid_result(3, net_profit=8.0, sharpe=0.2, grid_rank=3),
    ]
    apply_reference = apply_candidates + [
        _synthetic_grid_result(99, net_profit=1.0, sharpe=4.0, grid_rank=99)
    ]

    helper_candidates = [
        _synthetic_grid_result(1, net_profit=10.0, sharpe=0.5, grid_rank=1),
        _synthetic_grid_result(2, net_profit=9.0, sharpe=1.0, grid_rank=2),
        _synthetic_grid_result(3, net_profit=8.0, sharpe=0.2, grid_rank=3),
    ]
    helper_reference = helper_candidates + [
        _synthetic_grid_result(99, net_profit=1.0, sharpe=4.0, grid_rank=99)
    ]

    apply_selected, apply_summary = apply_fast_grid_dsr(
        apply_candidates,
        reference_results=apply_reference,
        top_k=3,
    )
    benchmark = compute_grid_dsr_benchmark(helper_reference)
    helper_selected = rank_grid_candidates_by_dsr(
        helper_candidates,
        dsr_benchmark=benchmark,
        top_k=3,
    )

    assert benchmark["dsr_sr0"] == apply_summary["dsr_sr0"]
    assert benchmark["dsr_n_trials"] == apply_summary["dsr_n_trials"]
    assert [item.candidate_id for item in helper_selected] == [
        item.candidate_id for item in apply_selected
    ]
    assert [item.dsr_rank for item in helper_selected] == [item.dsr_rank for item in apply_selected]
    assert [item.dsr_source_rank for item in helper_selected] == [
        item.dsr_source_rank for item in apply_selected
    ]


def test_grid_dsr_ranking_uses_persisted_benchmark_without_recomputing(monkeypatch):
    import core.grid_engine as grid_engine

    def unexpected_benchmark(*_args, **_kwargs):
        raise AssertionError("Pure DSR ranking must not recompute sr0.")

    monkeypatch.setattr(grid_engine, "calculate_expected_max_sharpe", unexpected_benchmark)
    monkeypatch.setattr(grid_engine, "calculate_dsr", lambda sr, sr0, *_args: sr0 - sr)

    candidates = [
        _synthetic_grid_result(1, net_profit=10.0, sharpe=0.5, grid_rank=1),
        _synthetic_grid_result(2, net_profit=9.0, sharpe=0.1, grid_rank=2),
    ]
    selected = rank_grid_candidates_by_dsr(
        candidates,
        dsr_benchmark={"dsr_sr0": 1.0},
        top_k=2,
    )

    assert [item.candidate_id for item in selected] == [2, 1]
    assert [item.dsr_probability for item in selected] == pytest.approx([0.9, 0.5])


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


def test_fast_grid_default_objective_preserves_net_profit_order():
    results = [
        _synthetic_grid_result(1, net_profit=5.0),
        _synthetic_grid_result(2, net_profit=12.0),
        _synthetic_grid_result(3, net_profit=8.0),
    ]

    ranked = rank_grid_results(
        results,
        objectives=["net_profit_pct"],
        primary_objective=None,
        constraints=[],
    )

    assert [item.candidate_id for item in ranked] == [2, 3, 1]
    assert [item.grid_rank for item in ranked] == [1, 2, 3]


def test_multi_objective_grid_ranks_feasible_pareto_before_non_pareto_and_infeasible():
    constraints = [ConstraintSpec(metric="net_profit_pct", threshold=0.0, enabled=True)]
    pareto_low_dd = _synthetic_grid_result(1, net_profit=12.0, win_rate=55.0)
    pareto_low_dd.max_drawdown_pct = 1.0
    pareto_profit = _synthetic_grid_result(2, net_profit=20.0, win_rate=50.0)
    pareto_profit.max_drawdown_pct = 3.0
    dominated = _synthetic_grid_result(3, net_profit=10.0, win_rate=40.0)
    dominated.max_drawdown_pct = 5.0
    infeasible = _synthetic_grid_result(4, net_profit=-1.0, win_rate=90.0)
    infeasible.max_drawdown_pct = 0.5

    ranked = rank_grid_results(
        [dominated, infeasible, pareto_profit, pareto_low_dd],
        objectives=["max_drawdown_pct", "win_rate", "net_profit_pct"],
        primary_objective="max_drawdown_pct",
        constraints=constraints,
    )

    assert [item.candidate_id for item in ranked] == [1, 2, 3, 4]
    assert ranked[0].is_pareto_optimal is True
    assert ranked[1].is_pareto_optimal is True
    assert ranked[2].is_pareto_optimal is False
    assert ranked[-1].constraints_satisfied is False


def test_grid_selection_config_rejects_advanced_fast_and_composite_objectives(monkeypatch):
    import core.grid_engine as grid_engine

    monkeypatch.setattr(
        grid_engine,
        "_load_backend",
        lambda strategy_id: SimpleNamespace(NUMBA_AVAILABLE=True, NUMBA_IMPORT_ERROR=None),
    )

    with pytest.raises(ValueError, match="fast screening"):
        validate_grid_config(_grid_config(grid_fast_objectives=["sharpe_ratio"]))

    with pytest.raises(ValueError, match="Composite Score"):
        validate_grid_config(_grid_config(grid_fast_objectives=["composite_score"]))

    config = _grid_config(
        grid_fast_objectives=["net_profit_pct", "max_drawdown_pct"],
        grid_fast_primary_objective="max_drawdown_pct",
        grid_slow_refinement_enabled=True,
        grid_slow_objectives=["sharpe_ratio", "ulcer_index"],
        grid_slow_primary_objective="sharpe_ratio",
    )
    selection = resolve_grid_selection_config(config)

    assert selection.fast_objectives == ["net_profit_pct", "max_drawdown_pct"]
    assert selection.fast_primary_objective == "max_drawdown_pct"
    assert selection.final_objectives == ["sharpe_ratio", "ulcer_index"]
    validate_grid_config(config)


def test_grid_slow_refinement_reranks_only_grid_top_candidates(monkeypatch):
    import core.grid_engine as grid_engine

    fast_results = [
        _synthetic_grid_result(1, net_profit=30.0, sharpe=0.1, grid_rank=1),
        _synthetic_grid_result(2, net_profit=20.0, sharpe=0.2, grid_rank=2),
        _synthetic_grid_result(3, net_profit=10.0, sharpe=3.0, grid_rank=3),
    ]
    for result, sqn in zip(fast_results, [1.0, 9.0, 99.0]):
        result.sqn = sqn
        result.ulcer_index = 1.0

    class FakeBackend:
        NUMBA_AVAILABLE = True
        NUMBA_IMPORT_ERROR = None

        @staticmethod
        def build_parameter_space(config):  # noqa: ARG004
            return SimpleNamespace(mode_space_sizes={"cc_only": 3, "tbands_only": 0, "both": 0})

        @staticmethod
        def build_preview(space, allocation):  # noqa: ARG004
            return {"total_space": 3, "coverage_pct": 100.0}

        @staticmethod
        def generate_candidates(config, space, allocation, seed):  # noqa: ARG004
            return SimpleNamespace(candidates=[SimpleNamespace(candidate_id=i) for i in (1, 2, 3)], diagnostics={})

        @staticmethod
        def prepare_fast_data(df, trade_start_idx, candidates):  # noqa: ARG004
            return SimpleNamespace(ma_cache_build_seconds=0.0, ma_cache_entries=0, ma_cache_estimated_mb=0.0)

        @staticmethod
        def evaluate_candidates(data, candidates, *, n_workers, needs_dsr):  # noqa: ARG004
            return list(fast_results)

        @staticmethod
        def validate_selected_candidates(df, trade_start_idx, selected_fast, *, tolerances, fail_on_error):  # noqa: ARG004
            validated = []
            for fast in selected_fast:
                slow = _synthetic_grid_result(
                    fast.candidate_id,
                    net_profit=fast.net_profit_pct,
                    sharpe=fast.sharpe_ratio or 0.0,
                    grid_rank=fast.grid_rank,
                )
                slow.sqn = getattr(fast, "sqn", None)
                slow.ulcer_index = getattr(fast, "ulcer_index", None)
                slow.semantic_key = fast.semantic_key
                slow.candidate_id = fast.candidate_id
                slow.optuna_trial_number = fast.candidate_id
                validated.append(slow)
            return validated

    monkeypatch.setattr(grid_engine, "_load_backend", lambda strategy_id: FakeBackend)
    monkeypatch.setattr(
        grid_engine,
        "_prepare_grid_dataframe",
        lambda config: (pd.DataFrame({"Close": [1.0]}), 0, None, None),
    )

    results, study_id = run_grid_optimization(
        _grid_config(
            grid_top_candidates=2,
            grid_diversity_enabled=False,
            grid_slow_refinement_enabled=True,
            grid_slow_objectives=["sqn"],
            grid_slow_primary_objective=None,
        ),
        save_study=False,
    )

    assert study_id is None
    assert [item.candidate_id for item in results] == [2, 1]
    assert {item.candidate_id for item in results} == {1, 2}
    assert all(item.candidate_id != 3 for item in results)
    assert [item.slow_refinement_rank for item in results] == [1, 2]
    assert [item.grid_rank for item in results] == [2, 1]


def test_fast_only_grid_preserves_pareto_metadata_after_slow_validation(monkeypatch):
    import core.grid_engine as grid_engine

    fast_results = [
        _synthetic_grid_result(1, net_profit=20.0, grid_rank=1),
        _synthetic_grid_result(2, net_profit=15.0, grid_rank=2),
        _synthetic_grid_result(3, net_profit=10.0, grid_rank=3),
    ]
    fast_results[0].max_drawdown_pct = 2.0
    fast_results[1].max_drawdown_pct = 1.0
    fast_results[2].max_drawdown_pct = 5.0

    class FakeBackend:
        NUMBA_AVAILABLE = True
        NUMBA_IMPORT_ERROR = None

        @staticmethod
        def build_parameter_space(config):  # noqa: ARG004
            return SimpleNamespace(mode_space_sizes={"cc_only": 3, "tbands_only": 0, "both": 0})

        @staticmethod
        def build_preview(space, allocation):  # noqa: ARG004
            return {"total_space": 3, "coverage_pct": 100.0}

        @staticmethod
        def generate_candidates(config, space, allocation, seed):  # noqa: ARG004
            return SimpleNamespace(candidates=[SimpleNamespace(candidate_id=i) for i in (1, 2, 3)], diagnostics={})

        @staticmethod
        def prepare_fast_data(df, trade_start_idx, candidates):  # noqa: ARG004
            return SimpleNamespace(ma_cache_build_seconds=0.0, ma_cache_entries=0, ma_cache_estimated_mb=0.0)

        @staticmethod
        def evaluate_candidates(data, candidates, *, n_workers, needs_dsr):  # noqa: ARG004
            return list(fast_results)

        @staticmethod
        def validate_selected_candidates(df, trade_start_idx, selected_fast, *, tolerances, fail_on_error):  # noqa: ARG004
            validated = []
            for fast in selected_fast:
                slow = _synthetic_grid_result(
                    fast.candidate_id,
                    net_profit=fast.net_profit_pct,
                    grid_rank=fast.grid_rank,
                )
                slow.max_drawdown_pct = fast.max_drawdown_pct
                slow.semantic_key = fast.semantic_key
                slow.candidate_id = fast.candidate_id
                slow.optuna_trial_number = fast.candidate_id
                validated.append(slow)
            return validated

    monkeypatch.setattr(grid_engine, "_load_backend", lambda strategy_id: FakeBackend)
    monkeypatch.setattr(
        grid_engine,
        "_prepare_grid_dataframe",
        lambda config: (pd.DataFrame({"Close": [1.0]}), 0, None, None),
    )
    config = _grid_config(
        grid_top_candidates=3,
        grid_diversity_enabled=False,
        grid_fast_objectives=["max_drawdown_pct", "net_profit_pct"],
        grid_fast_primary_objective="max_drawdown_pct",
        grid_slow_refinement_enabled=False,
    )

    results, study_id = run_grid_optimization(config, save_study=False)

    assert study_id is None
    assert [item.candidate_id for item in results] == [2, 1, 3]
    assert [item.is_pareto_optimal for item in results] == [True, True, False]
    assert [item.grid_rank for item in results] == [1, 2, 3]
    assert config.grid_summary["pareto_front_size"] == 2


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


def test_fast_to_slow_validation_preserves_dsr_source_rank(monkeypatch):
    fast_result = _synthetic_grid_result(7, net_profit=10.0, sharpe=1.0, grid_rank=3)
    fast_result.dsr_source_rank = 4

    def fake_run_single_combination(args):  # noqa: ARG001
        return OptimizationResult(
            params=dict(fast_result.params),
            net_profit_pct=fast_result.net_profit_pct,
            max_drawdown_pct=fast_result.max_drawdown_pct,
            total_trades=fast_result.total_trades,
            winning_trades=fast_result.winning_trades,
            losing_trades=fast_result.losing_trades,
            win_rate=fast_result.win_rate,
            profit_factor=fast_result.profit_factor,
            romad=fast_result.romad,
            optuna_trial_number=fast_result.candidate_id,
        )

    monkeypatch.setattr(fast_grid, "_run_single_combination", fake_run_single_combination)
    validated = fast_grid.validate_selected_candidates(
        pd.DataFrame({"Close": [1.0, 1.0]}),
        0,
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
        fail_on_error=False,
    )

    assert validated[0].dsr_source_rank == 4
    assert validated[0].grid_rank == 3


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


def test_save_and_load_grid_study_roundtrip(tmp_path):
    csv_path = tmp_path / "data.csv"
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

    study_id = save_grid_study_to_db(
        config=config,
        grid_settings=GridSettings(requested_budget=10, top_candidates=1),
        grid_summary=summary,
        trial_results=[result],
        csv_file_path=str(csv_path),
        start_time=0,
    )
    loaded = load_study_from_db(study_id)

    assert loaded["study"]["optimization_mode"] == "grid"
    assert loaded["study"]["optimizer_mode"] == "grid"
    assert loaded["trials"][0]["candidate_id"] == 9
    assert loaded["trials"][0]["grid_rank"] == 1
    assert loaded["trials"][0]["validation_status"] == "passed"
    assert loaded["study"]["dsr_n_trials"] == 9
    assert loaded["trials"][0]["dsr_probability"] == pytest.approx(0.91)
    assert loaded["trials"][0]["selection_sources"] == ["objective", "dsr"]


# ---------------------------------------------------------------------------
# Backend metadata: diversity-field shape preservation + default-mode contract
# ---------------------------------------------------------------------------


def test_normalize_diversity_group_fields_preserves_shape_and_rejects_malformed():
    # Flat backends (S03) keep their list[str] field list.
    assert normalize_diversity_group_fields(["mode", "maType3", "maLength3"]) == [
        "mode",
        "maType3",
        "maLength3",
    ]
    # Mode-specific backends (S06) keep their dict[str, list[str]] mapping.
    mapping = {
        "bracket": ["mode", "stopX", "stopRR"],
        "trail": ["mode", "trailMAType", "trailMALength"],
    }
    normalized = normalize_diversity_group_fields(mapping)
    assert normalized == mapping
    # Defensive copy: mutating the result must not touch the source.
    normalized["bracket"].append("injected")
    assert mapping["bracket"] == ["mode", "stopX", "stopRR"]
    # None collapses to an empty list, not an error.
    assert normalize_diversity_group_fields(None) == []
    # Malformed shapes are rejected clearly rather than silently corrupted.
    with pytest.raises(ValueError, match="Malformed diversity_group_fields"):
        normalize_diversity_group_fields({"bracket": "mode,stopX"})
    with pytest.raises(ValueError, match="Malformed diversity_group_fields"):
        normalize_diversity_group_fields(42)


def test_backend_metadata_diversity_group_fields_shapes():
    s03 = get_fast_grid_backend_metadata("s03_reversal_v10")["diversity_group_fields"]
    assert s03 == ["mode", "maType3", "maLength3"]

    s06 = get_fast_grid_backend_metadata("s06_r_trend_v02")["diversity_group_fields"]
    assert s06 == {
        "bracket": ["mode", "stopX", "stopRR"],
        "trail": ["mode", "trailMAType", "trailMALength"],
    }


def test_default_grid_enabled_modes_from_backend_metadata(monkeypatch):
    # S06 (full enumeration) derives ordered default-enabled modes from metadata.
    assert default_grid_enabled_modes("s06_r_trend_v02") == ["bracket", "trail"]
    # S03 declares no explicit modes -> empty (cc/tbands/both allocation untouched).
    assert default_grid_enabled_modes("s03_reversal_v10") == []
    # Unsupported strategies are unaffected.
    assert default_grid_enabled_modes("s04_stochrsi") == []

    # A backend-defined default-disabled mode is excluded while order is preserved.
    import core.grid_engine as grid_engine

    def _fake_metadata(_strategy_id):
        return {
            "modes": [
                {"id": "bracket", "default_enabled": True},
                {"id": "trail", "default_enabled": True},
                {"id": "experimental", "default_enabled": False},
            ]
        }

    monkeypatch.setattr(grid_engine, "get_fast_grid_backend_metadata", _fake_metadata)
    assert grid_engine.default_grid_enabled_modes("s06_r_trend_v02") == ["bracket", "trail"]


def test_default_grid_enabled_modes_rejects_malformed_metadata(monkeypatch):
    import core.grid_engine as grid_engine

    monkeypatch.setattr(
        grid_engine,
        "get_fast_grid_backend_metadata",
        lambda _strategy_id: {"modes": [{"label": "no id"}]},
    )
    with pytest.raises(ValueError, match="missing an 'id'"):
        grid_engine.default_grid_enabled_modes("s06_r_trend_v02")


def _s06_server_payload(**overrides):
    payload = {
        "optimization_mode": "grid",
        "enabled_params": {"stopX": True},
        "param_ranges": {"stopX": [1.0, 2.0, 1.0]},
        "param_types": {"stopX": "float"},
        "fixed_params": {
            "dateFilter": False,
            "contractSize": 0.01,
            "initialCapital": 100.0,
            "commissionPct": 0.05,
        },
        "objectives": ["net_profit_pct"],
        "grid_fast_objectives": ["net_profit_pct"],
        "grid_budget": 10,
        "grid_seed": 42,
        "grid_top_candidates": 5,
    }
    payload.update(overrides)
    return payload


def _build_server_config(strategy_id, **payload_overrides):
    return _build_optimization_config(
        "dummy.csv",
        _s06_server_payload(**payload_overrides),
        worker_processes=1,
        strategy_id=strategy_id,
        warmup_bars=1000,
    )


def test_server_default_modes_use_backend_contract_not_strategy_hardcode():
    # Absent mode field -> backend default-enabled modes, in backend order.
    assert _build_server_config("s06_r_trend_v02").grid_enabled_modes == ["bracket", "trail"]
    # Explicit selections are honored verbatim.
    assert _build_server_config(
        "s06_r_trend_v02", grid_enabled_modes=["bracket"]
    ).grid_enabled_modes == ["bracket"]
    assert _build_server_config(
        "s06_r_trend_v02", grid_enabled_modes=["trail"]
    ).grid_enabled_modes == ["trail"]
    assert _build_server_config(
        "s06_r_trend_v02", grid_enabled_modes=["bracket", "trail"]
    ).grid_enabled_modes == ["bracket", "trail"]
    # Unsupported strategy and S03 keep their existing empty defaulting.
    assert _build_server_config("s04_stochrsi").grid_enabled_modes == []
    assert _build_server_config("s03_reversal_v10").grid_enabled_modes == []


def test_server_explicit_empty_modes_stay_empty_and_backend_rejects_them():
    config = _build_server_config("s06_r_trend_v02", grid_enabled_modes=[])
    # The server does not silently substitute defaults for an explicit empty set.
    assert config.grid_enabled_modes == []
    # Backend validation remains authoritative even if the UI is bypassed.
    from strategies.s06_r_trend_v02 import fast_grid as s06_fast_grid

    with pytest.raises(ValueError, match="at least one"):
        s06_fast_grid.build_parameter_space(config)
