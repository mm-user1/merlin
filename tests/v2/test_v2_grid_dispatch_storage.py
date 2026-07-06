from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.grid_engine import (
    get_grid_v2_backend_metadata,
    run_grid_optimization,
    supports_fast_grid,
    supports_grid_v2,
)
from core.optuna_engine import OptimizationConfig
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
    assert config.grid_summary["grid"]["backend_kind"] in {"compiled_numba", "reference"}
    assert {result.engine for result in results} == {"v2"}
    assert all(result.grid_generation_mode == "full_enumeration_v2" for result in results)


def test_grid_v2_dsr_request_fails_clearly():
    with pytest.raises(ValueError, match="V2 Grid DSR is unavailable"):
        run_grid_optimization(_v2_grid_config(dsr=True), save_study=False)


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
