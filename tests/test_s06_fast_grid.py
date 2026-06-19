from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.grid_engine import (
    _load_backend,
    get_fast_grid_backend_metadata,
    preview_grid_parameter_space,
    run_grid_optimization,
    supports_fast_grid,
)
from core.optuna_engine import OptimizationConfig
from core.storage import load_study_from_db
from strategies.s06_r_trend_v02 import fast_grid
from strategies.s06_r_trend_v02.strategy import S06RTrendV02


PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
TRADING_START = pd.Timestamp("2025-08-01 00:00:00", tz="UTC")
TRADING_END = pd.Timestamp("2025-12-01 00:00:00", tz="UTC")

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


def _config(
    *,
    modes=("bracket", "trail"),
    threshold_os=False,
    threshold_ob=False,
    enabled_overrides=None,
    fixed_overrides=None,
    **overrides,
) -> OptimizationConfig:
    enabled = {name: True for name in GRID_PARAMS}
    enabled.update({"thresholdOS": threshold_os, "thresholdOB": threshold_ob})
    enabled.update(enabled_overrides or {})
    fixed = {
        "dateFilter": True,
        "start": TRADING_START.isoformat(),
        "end": TRADING_END.isoformat(),
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
    fixed.update(fixed_overrides or {})
    payload = {
        "csv_file": str(DATA_PATH),
        "strategy_id": "s06_r_trend_v02",
        "enabled_params": enabled,
        "param_ranges": {},
        "param_types": {
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
        "fixed_params": fixed,
        "worker_processes": 1,
        "warmup_bars": 1000,
        "optimization_mode": "grid",
        "objectives": ["net_profit_pct"],
        "grid_fast_objectives": ["net_profit_pct"],
        "grid_enabled_modes": list(modes),
        "grid_budget": 1,
        "grid_seed": 42,
        "grid_top_candidates": 10,
    }
    payload.update(overrides)
    return OptimizationConfig(**payload)


@pytest.fixture(scope="module")
def reference_data():
    if not DATA_PATH.exists():
        pytest.skip(f"Reference data not found: {DATA_PATH}")
    return prepare_dataset_with_warmup(
        load_data(str(DATA_PATH)),
        TRADING_START,
        TRADING_END,
        1000,
    )


@pytest.fixture
def client():
    from ui.server import app

    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client


def _single_candidate_data(df, trade_start_idx, config):
    space = fast_grid.build_parameter_space(config)
    allocation = fast_grid.build_allocation(config, space, None)
    candidate_set = fast_grid.generate_candidates(config, space, allocation, seed=999)
    data = fast_grid.prepare_fast_data(df, trade_start_idx, candidate_set.candidates)
    return candidate_set.candidates[0], data


def test_backend_discovery_metadata_and_contract_validation(monkeypatch):
    assert supports_fast_grid("s03_reversal_v10")
    assert supports_fast_grid("s06_r_trend_v02")
    assert not supports_fast_grid("s04_stochrsi")
    assert _load_backend("s03_reversal_v10").__name__.endswith("s03_reversal_v10.fast_grid")
    assert _load_backend("s06_r_trend_v02") is fast_grid

    metadata = get_fast_grid_backend_metadata("s06_r_trend_v02")
    assert metadata["profile"] == "full_enumeration"
    assert metadata["supports_partial_coverage"] is False
    assert metadata["supports_seed"] is False
    assert [mode["id"] for mode in metadata["modes"]] == ["bracket", "trail"]

    with pytest.raises(ValueError, match="not supported"):
        _load_backend("s04_stochrsi")

    import core.grid_engine as grid_engine

    monkeypatch.setitem(grid_engine.FAST_GRID_BACKENDS, "broken", "broken.backend")
    monkeypatch.setattr(
        grid_engine.importlib,
        "import_module",
        lambda _name: SimpleNamespace(NUMBA_AVAILABLE=True),
    )
    with pytest.raises(ValueError, match="Malformed Grid backend"):
        _load_backend("broken")


@pytest.mark.parametrize(
    ("modes", "threshold_os", "threshold_ob", "expected"),
    [
        (("bracket",), False, False, 480),
        (("trail",), False, False, 48_000),
        (("bracket", "trail"), False, False, 48_480),
        (("bracket",), True, True, 4_320),
        (("trail",), True, True, 432_000),
        (("bracket", "trail"), True, True, 436_320),
        (("bracket", "trail"), True, False, 145_440),
        (("bracket", "trail"), False, True, 145_440),
    ],
)
def test_exact_s06_space_counts(modes, threshold_os, threshold_ob, expected):
    config = _config(
        modes=modes,
        threshold_os=threshold_os,
        threshold_ob=threshold_ob,
    )
    space = fast_grid.build_parameter_space(config)
    allocation = fast_grid.build_allocation(config, space, None)
    candidate_set = fast_grid.generate_candidates(config, space, allocation, seed=1)

    assert space.total_space_size == expected
    assert allocation.actual_budget == expected
    assert len(candidate_set.candidates) == expected
    assert fast_grid.build_preview(space, allocation)["total_space"] == expected


def test_space_collapse_threshold_domain_identity_order_and_seed_independence():
    with pytest.raises(ValueError, match="at least one"):
        fast_grid.build_parameter_space(_config(modes=()))

    fixed_axes = {name: False for name in GRID_PARAMS}
    bracket = _config(
        modes=("bracket",),
        enabled_overrides=fixed_axes,
        fixed_overrides={"thresholdOS": 27, "thresholdOB": 33},
    )
    bracket_space = fast_grid.build_parameter_space(bracket)
    assert bracket_space.total_space_size == 1
    assert bracket_space.axes["thresholdOS"] == [27]
    assert bracket_space.axes["thresholdOB"] == [33]

    thresholds = fast_grid.build_parameter_space(
        _config(
            modes=("bracket",),
            threshold_os=True,
            threshold_ob=True,
            enabled_overrides={name: False for name in GRID_PARAMS},
        )
    )
    assert thresholds.axes["thresholdOS"] == [20, 30, 40]
    assert thresholds.axes["thresholdOB"] == [20, 30, 40]

    space = fast_grid.build_parameter_space(_config())
    allocation = fast_grid.build_allocation(_config(), space, None)
    first = fast_grid.generate_candidates(_config(), space, allocation, seed=1).candidates
    second = fast_grid.generate_candidates(_config(), space, allocation, seed=999).candidates
    assert first[0].mode == "bracket"
    assert first[479].mode == "bracket"
    assert first[480].mode == "trail"
    sample_indices = [0, 1, 479, 480, 20_000, len(first) - 1]
    assert [first[index].semantic_key for index in sample_indices] == [
        second[index].semantic_key for index in sample_indices
    ]
    assert len({first[index].semantic_key for index in sample_indices}) == len(sample_indices)

    trail_a = _config(
        modes=("trail",),
        enabled_overrides={"stopRR": False},
        fixed_overrides={"stopRR": 1.5},
    )
    trail_b = _config(
        modes=("trail",),
        enabled_overrides={"stopRR": False},
        fixed_overrides={"stopRR": 3.0},
    )
    assert (
        fast_grid.CandidateSequence(fast_grid.build_parameter_space(trail_a))[0].semantic_key
        == fast_grid.CandidateSequence(fast_grid.build_parameter_space(trail_b))[0].semantic_key
    )

    bracket_a = _config(
        modes=("bracket",),
        fixed_overrides={"trailMAType": "SMA", "trailMALength": 50},
    )
    bracket_b = _config(
        modes=("bracket",),
        fixed_overrides={"trailMAType": "T3", "trailMALength": 200},
    )
    assert (
        fast_grid.CandidateSequence(fast_grid.build_parameter_space(bracket_a))[0].semantic_key
        == fast_grid.CandidateSequence(fast_grid.build_parameter_space(bracket_b))[0].semantic_key
    )


def test_preview_ignores_budget_and_seed_for_full_enumeration():
    first = preview_grid_parameter_space(
        _config(grid_budget=1, grid_seed=1)
    )
    second = preview_grid_parameter_space(
        _config(grid_budget=999_999, grid_seed=999)
    )
    assert first == second
    assert first["total_space"] == 48_480
    assert first["coverage_pct"] == 100.0
    assert first["method"] == "Full enumeration"


def test_s06_grid_preview_api_uses_modes_and_exact_space(client):
    config = _config(
        modes=("bracket", "trail"),
        threshold_os=True,
        threshold_ob=True,
    )
    payload = {
        "config": {
            "optimization_mode": "grid",
            "enabled_params": config.enabled_params,
            "param_ranges": config.param_ranges,
            "param_types": config.param_types,
            "fixed_params": config.fixed_params,
            "grid_enabled_modes": config.grid_enabled_modes,
            "grid_fast_objectives": ["net_profit_pct"],
            "grid_budget": 1,
            "grid_seed": 999,
        },
        "strategyId": "s06_r_trend_v02",
        "warmupBars": 1000,
    }
    response = client.post("/api/grid/preview", json=payload)
    assert response.status_code == 200
    preview = response.get_json()["preview"]
    assert preview["total_space"] == 436_320
    assert preview["actual_budget"] == 436_320
    assert preview["coverage_pct"] == 100.0
    assert [row["space_size"] for row in preview["modes"]] == [4_320, 432_000]

    del payload["config"]["grid_enabled_modes"]
    response = client.post("/api/grid/preview", json=payload)
    assert response.status_code == 200
    assert response.get_json()["preview"]["total_space"] == 436_320

    payload["config"]["grid_enabled_modes"] = ["bracket"]
    response = client.post("/api/grid/preview", json=payload)
    assert response.status_code == 200
    assert response.get_json()["preview"]["total_space"] == 4_320

    payload["config"]["grid_enabled_modes"] = []
    response = client.post("/api/grid/preview", json=payload)
    assert response.status_code == 400
    assert "at least one" in response.get_json()["error"]


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
@pytest.mark.parametrize(
    ("mode", "fixed_overrides", "expected"),
    [
        (
            "bracket",
            {
                "entryMode": "Trend @ Square",
                "useTrailMA": False,
                "stopRR": 2.0,
            },
            (25.87428420514485, 9.921155504216738, 48, 21, 1.4381064632227756),
        ),
        (
            "trail",
            {"entryMode": "Reversal @ Triangle", "useTrailMA": True},
            (30.942005419294023, 13.468303210925036, 61, 31, 1.5088788695865385),
        ),
    ],
)
def test_accepted_baselines_and_trade_signatures_match_slow(
    reference_data,
    mode,
    fixed_overrides,
    expected,
):
    df, trade_start_idx = reference_data
    config = _config(
        modes=(mode,),
        enabled_overrides={name: False for name in GRID_PARAMS}
        | {"thresholdOS": False, "thresholdOB": False},
        fixed_overrides=fixed_overrides,
    )
    candidate, data = _single_candidate_data(df, trade_start_idx, config)
    fast_result = fast_grid.evaluate_candidates(data, [candidate])[0]
    validated = fast_grid.validate_selected_candidates(
        df,
        trade_start_idx,
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
    assert (
        fast_result.net_profit_pct,
        fast_result.max_drawdown_pct,
        fast_result.total_trades,
        fast_result.winning_trades,
        fast_result.profit_factor,
    ) == pytest.approx(expected, abs=1e-10)
    assert validated.validation_status == "passed"

    trace = fast_grid.evaluate_candidate_trace(data, candidate)
    slow = S06RTrendV02.run(df, candidate.params, trade_start_idx)
    assert len(trace) == len(slow.trades)
    for fast_trade, slow_trade in zip(trace, slow.trades):
        assert fast_trade["direction"] == slow_trade.direction
        assert fast_trade["entry_time"] == slow_trade.entry_time
        assert fast_trade["exit_time"] == slow_trade.exit_time
        assert fast_trade["entry_price"] == pytest.approx(slow_trade.entry_price, abs=1e-12)
        assert fast_trade["exit_price"] == pytest.approx(slow_trade.exit_price, abs=1e-12)
        assert fast_trade["size"] == pytest.approx(slow_trade.size, abs=1e-12)
        assert fast_trade["net_pnl"] == pytest.approx(slow_trade.net_pnl, abs=1e-12)


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
def test_randomized_real_data_candidate_parity_and_parallel_determinism(reference_data):
    df, trade_start_idx = reference_data
    config = _config()
    space = fast_grid.build_parameter_space(config)
    allocation = fast_grid.build_allocation(config, space, None)
    source = fast_grid.generate_candidates(config, space, allocation, seed=17).candidates
    data = fast_grid.prepare_fast_data(df, trade_start_idx, source)

    rng = np.random.default_rng(20260619)
    indices = sorted(set([0, 479, 480, len(source) - 1] + rng.integers(
        0, len(source), size=8
    ).tolist()))
    candidates = [source[index] for index in indices]
    one_thread = fast_grid.evaluate_candidates(data, candidates, n_workers=1)
    many_threads = fast_grid.evaluate_candidates(data, candidates, n_workers=4)
    assert [item.candidate_id for item in one_thread] == [
        item.candidate_id for item in many_threads
    ]
    for first, second in zip(one_thread, many_threads):
        assert first.semantic_key == second.semantic_key
        assert first.net_profit_pct == second.net_profit_pct
        assert first.max_drawdown_pct == second.max_drawdown_pct
        assert first.total_trades == second.total_trades
        assert first.profit_factor == second.profit_factor

    validated = fast_grid.validate_selected_candidates(
        df,
        trade_start_idx,
        one_thread,
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
    assert all(result.validation_status == "passed" for result in validated)


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
def test_small_s06_grid_run_uses_shared_ranking_dsr_and_slow_validation():
    config = _config(
        enabled_overrides={name: False for name in GRID_PARAMS}
        | {"thresholdOS": False, "thresholdOB": False},
        grid_top_candidates=2,
        grid_diversity_enabled=False,
        grid_needs_dsr=True,
        grid_dsr_top_k=2,
    )
    results, study_id = run_grid_optimization(config, save_study=False)

    assert study_id is None
    assert len(results) == 2
    assert all(result.validation_status == "passed" for result in results)
    assert {result.grid_mode_name for result in results} == {"bracket", "trail"}
    assert config.grid_summary["actual_budget"] == 2
    assert config.grid_summary["grid"]["backend"]["profile"] == "full_enumeration"
    assert config.grid_summary["grid"]["dsr_metric_computation_enabled"] is True
    assert config.optuna_all_results == []


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
def test_small_s06_grid_storage_round_trip():
    config = _config(
        modes=("bracket",),
        enabled_overrides={name: False for name in GRID_PARAMS}
        | {"thresholdOS": False, "thresholdOB": False},
        grid_top_candidates=1,
        grid_diversity_enabled=False,
    )
    results, study_id = run_grid_optimization(config, save_study=True)
    assert study_id
    assert len(results) == 1

    loaded = load_study_from_db(study_id)
    assert loaded["study"]["strategy_id"] == "s06_r_trend_v02"
    assert loaded["study"]["optimization_mode"] == "grid"
    assert loaded["study"]["grid_actual_budget"] == 1
    assert len(loaded["trials"]) == 1
    assert loaded["trials"][0]["grid_mode_name"] == "bracket"
    assert loaded["trials"][0]["validation_status"] == "passed"
