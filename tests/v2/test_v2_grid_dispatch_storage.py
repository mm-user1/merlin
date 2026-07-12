from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.grid_engine import (
    get_grid_v2_backend_metadata,
    preview_grid_parameter_space,
    rank_grid_results,
    run_grid_optimization,
    supports_fast_grid,
    supports_grid_v2,
)
from core.engine_v2.compiled_kernel import compiled_batch_available
from core.engine_v2.contracts import GuardrailSummary
from core.engine_v2.runner import V2RunResult
from core.optuna_engine import MultiObjectiveConfig, OptimizationConfig, OptimizationResult
from core.storage import (
    create_new_db,
    get_active_db_name,
    load_study_from_db,
    set_active_db,
)
from strategies import get_strategy
from strategies.s06_r_trend_v02_b2.strategy import S06RTrendV02B2

from s06_b2_test_helpers import BASELINE_END, BASELINE_START, merged_reference_params


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"


@contextmanager
def _temporary_active_db(label: str):
    previous = get_active_db_name()
    create_new_db(label)
    try:
        yield
    finally:
        set_active_db(previous)


def _v2_grid_config(*, save_size: bool = False, dsr: bool = False) -> OptimizationConfig:
    config = OptimizationConfig(
        csv_file=str(DATA_PATH),
        strategy_id="s06_r_trend_v02_b2",
        enabled_params={"stopX": True} if save_size else {},
        param_ranges={},
        param_types={},
        fixed_params={
            "dateFilter": True,
            "start": BASELINE_START.isoformat().replace("+00:00", "Z"),
            "end": BASELINE_END.isoformat().replace("+00:00", "Z"),
        },
        optimization_mode="grid",
        objectives=["net_profit_pct"],
        grid_top_candidates=2,
        grid_enabled_modes=["bracket"],
        grid_needs_dsr=dsr,
    )
    return config


def _setattr_config(config: OptimizationConfig, **values) -> OptimizationConfig:
    for key, value in values.items():
        setattr(config, key, value)
    return config


def test_grid_v2_dispatch_runs_before_v1_fast_backend_validation():
    assert supports_grid_v2("s06_r_trend_v02_b2") is True
    assert supports_fast_grid("s06_r_trend_v02_b2") is False
    metadata = get_grid_v2_backend_metadata("s06_r_trend_v02_b2")
    assert metadata["engine"] == "v2"

    config = _v2_grid_config(save_size=True)
    results, study_id = run_grid_optimization(config, save_study=False)

    assert study_id is None
    assert len(results) == 2
    assert config.grid_summary["engine"] == "v2"
    assert len(config.optuna_all_results) == config.grid_summary["valid_candidate_count"]
    assert len(config.optuna_all_results) > len(results)
    assert config.grid_summary["grid"]["backend_kind"] in {"compiled_numba", "reference"}
    assert config.grid_summary["grid"]["candidate_table_used"] is True
    assert config.grid_summary["grid"]["candidate_table_row_count"] == config.grid_summary["candidate_count"]
    assert config.grid_summary["grid"]["candidate_table_unique_signal_rows"] == 1
    assert config.grid_summary["grid"]["legacy_candidates_materialized"] == 0
    assert config.grid_summary["grid"]["canonical_identities_materialized"] == 0
    assert config.grid_summary["grid"]["semantic_keys_materialized"] == config.grid_summary["candidate_count"]
    if compiled_batch_available():
        assert config.grid_summary["grid"]["compiled_execution_mode"] == "stacked"
        assert config.grid_summary["grid"]["compiled_config_packing"] == "mapping"
        assert config.grid_summary["grid"]["stack_row_count"] is not None
    assert {result.engine for result in results} == {"v2"}
    assert all(result.grid_generation_mode == "full_enumeration_v2" for result in results)
    assert config.grid_summary["grid"]["cache_estimate"]["worker_multiplier"] == 1
    assert all(getattr(result, "grid_rank", None) for result in config.optuna_all_results)
    assert all(getattr(result, "objective_values", None) for result in config.optuna_all_results)
    assert config.optuna_all_results[0].semantic_key
    assert str(results[0].canonical_identity).startswith("{")


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled path required for slow-enrichment counter.")
def test_grid_v2_dispatch_slow_enriches_selected_rows_once(monkeypatch):
    import core.engine_v2.runner as runner_module
    import core.grid_v2 as grid_v2_module

    original_grid_v2_runner = grid_v2_module.run_v2_strategy
    original_slow_runner = runner_module.run_v2_strategy
    counts = {"grid_v2": 0, "runner": 0}

    def counted_grid_v2_runner(*args, **kwargs):
        counts["grid_v2"] += 1
        return original_grid_v2_runner(*args, **kwargs)

    def counted_slow_runner(*args, **kwargs):
        counts["runner"] += 1
        return original_slow_runner(*args, **kwargs)

    monkeypatch.setattr(grid_v2_module, "run_v2_strategy", counted_grid_v2_runner)
    monkeypatch.setattr(runner_module, "run_v2_strategy", counted_slow_runner)

    config = _v2_grid_config(save_size=True)
    results, _ = run_grid_optimization(config, save_study=False)

    assert len(results) == 2
    assert counts == {"grid_v2": 0, "runner": 2}
    assert config.grid_summary["grid"]["backend_kind"] == "compiled_numba"
    assert config.grid_summary["grid"]["compiled_execution_mode"] == "stacked"
    assert config.grid_summary["grid"]["cache_estimate"]["worker_multiplier"] == 1


@pytest.mark.skipif(not compiled_batch_available(), reason="Compiled path required for guardrail provenance check.")
def test_grid_v2_selected_guardrail_summary_comes_from_slow_reference(monkeypatch):
    import core.engine_v2.runner as runner_module

    original_slow_runner = runner_module.run_v2_strategy

    def sentinel_slow_runner(*args, **kwargs):
        run = original_slow_runner(*args, **kwargs)
        return V2RunResult(
            strategy_result=run.strategy_result,
            guardrail_summary=GuardrailSummary(
                invalid_stop_distance_count=123,
                max_required_leverage=7.5,
                flags=456,
            ),
            standing_state=run.standing_state,
            kernel_result=run.kernel_result,
        )

    monkeypatch.setattr(runner_module, "run_v2_strategy", sentinel_slow_runner)

    config = _v2_grid_config(save_size=True)
    results, _ = run_grid_optimization(config, save_study=False)

    assert len(results) == 2
    assert all(result.guardrail_summary["invalid_stop_distance_count"] == 123 for result in results)
    assert all(result.guardrail_summary["max_required_leverage"] == 7.5 for result in results)
    assert all(result.guardrail_summary["flags"] == 456 for result in results)


def test_grid_v2_dsr_request_fails_clearly():
    with pytest.raises(ValueError, match="V2 Grid DSR is unavailable"):
        run_grid_optimization(_v2_grid_config(dsr=True), save_study=False)


def test_grid_v2_cache_limit_propagates_and_rejects_invalid_values():
    config = _setattr_config(_v2_grid_config(save_size=True), grid_v2_max_cache_mb=2048.0)
    run_grid_optimization(config, save_study=False)

    assert config.grid_summary["grid"]["max_signal_cache_mb"] == 2048.0
    assert config.grid_summary["grid"]["cache_estimate"]["max_signal_cache_mb"] == 2048.0

    with pytest.raises(ValueError, match="Grid V2 max cache MB"):
        run_grid_optimization(
            _setattr_config(_v2_grid_config(save_size=True), grid_v2_max_cache_mb=0.0),
            save_study=False,
        )


def test_grid_v2_objective_directions_are_canonical_for_ranking():
    multi_objective = MultiObjectiveConfig(
        ["total_trades", "max_consecutive_losses"],
        "total_trades",
    )
    assert multi_objective.get_directions() == ["maximize", "minimize"]
    assert multi_objective.get_metric_names() == ["Total Trades", "Max Consecutive Losses"]

    few_trades = OptimizationResult(
        params={"id": "few"},
        net_profit_pct=0.0,
        max_drawdown_pct=0.0,
        total_trades=1,
        win_rate=0.0,
        max_consecutive_losses=0,
    )
    many_trades = OptimizationResult(
        params={"id": "many"},
        net_profit_pct=0.0,
        max_drawdown_pct=0.0,
        total_trades=5,
        win_rate=0.0,
        max_consecutive_losses=3,
    )

    assert rank_grid_results(
        [few_trades, many_trades],
        objectives=["total_trades"],
        primary_objective="total_trades",
        constraints=[],
    )[0] is many_trades
    assert rank_grid_results(
        [few_trades, many_trades],
        objectives=["max_consecutive_losses"],
        primary_objective="max_consecutive_losses",
        constraints=[],
    )[0] is few_trades
    ranked = rank_grid_results(
        [few_trades, many_trades],
        objectives=["total_trades", "max_consecutive_losses"],
        primary_objective="total_trades",
        constraints=[],
    )
    assert all(result.objective_values for result in ranked)


def test_grid_v2_preview_payload_has_full_enumeration_rows_and_select_subset_counts():
    config = _v2_grid_config()
    config.grid_enabled_modes = ["bracket", "trail"]
    config.fixed_params["trailMAType_options"] = ["SMA"]
    preview = preview_grid_parameter_space(config)

    assert preview["profile"] == "full_enumeration_v2"
    assert preview["full_candidate_count"] == 12_480
    assert preview["actual_budget"] == 12_480
    assert preview["coverage_label"] == "100%"
    assert [row["mode"] for row in preview["modes"]] == ["bracket", "trail"]
    assert [row["space_size"] for row in preview["modes"]] == [480, 12_000]
    assert all(row["generation"] == "Full enumeration" for row in preview["modes"])


def test_grid_v2_execution_uses_reduced_select_domain():
    config = _v2_grid_config()
    config.grid_enabled_modes = ["trail"]
    config.grid_top_candidates = 1
    config.fixed_params["trailMAType_options"] = ["SMA"]

    results, _ = run_grid_optimization(config, save_study=False)

    assert config.grid_summary["candidate_count"] == 12_000
    assert config.grid_summary["grid"]["optional_axis_settings"]["select_option_subsets"] == {"trailMAType": ["SMA"]}
    assert len(results) == 1
    assert results[0].params["trailMAType"] == "SMA"
    assert all(not str(key).endswith("_options") for key in results[0].params)


def test_grid_v2_storage_roundtrip_uses_existing_grid_schema():
    with _temporary_active_db("grid_v2_storage_roundtrip"):
        config = _v2_grid_config(save_size=True)
        results, study_id = run_grid_optimization(config, save_study=True)
        loaded = load_study_from_db(study_id)

    assert study_id
    assert len(results) == 2
    assert loaded is not None
    assert loaded["study"]["optimization_mode"] == "grid"
    assert loaded["study"]["grid_summary"]["engine"] == "v2"
    assert loaded["study"]["grid_summary"]["grid"]["compiled_batch_available"] in {True, False}
    if compiled_batch_available():
        assert loaded["study"]["grid_summary"]["grid"]["compiled_execution_mode"] == "stacked"
    assert len(loaded["trials"]) == 2
    assert all(trial["candidate_id"] for trial in loaded["trials"])
    assert all(trial["semantic_key"] for trial in loaded["trials"])


def test_s06_b2_single_backtest_backend_path_matches_direct_strategy():
    params = merged_reference_params("reference_b_trend_bracket")
    raw = load_data(str(DATA_PATH))
    prepared, trade_start_idx = prepare_dataset_with_warmup(
        raw,
        BASELINE_START,
        BASELINE_END,
        1000,
    )
    registry_result = get_strategy("s06_r_trend_v02_b2").run(
        prepared,
        params,
        trade_start_idx=trade_start_idx,
    )
    direct_result = S06RTrendV02B2.run(prepared, params, trade_start_idx=trade_start_idx)

    assert [(t.entry_time, t.exit_time, t.net_pnl) for t in registry_result.trades] == [
        (t.entry_time, t.exit_time, t.net_pnl) for t in direct_result.trades
    ]
