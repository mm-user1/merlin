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

    # The S06 mode-specific diversity mapping must survive shared orchestration
    # (it must not be coerced to its mode-name keys ["bracket", "trail"]).
    expected_fields = {
        "bracket": ["mode", "stopX", "stopRR"],
        "trail": ["mode", "trailMAType", "trailMALength"],
    }
    assert config.grid_summary["grid"]["diversity"]["diversity_group_fields"] == expected_fields
    assert config.grid_summary["grid"]["backend"]["diversity_group_fields"] == expected_fields


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

    # Storage/API round-trip must preserve the mode-specific diversity mapping
    # rather than reducing it to mode names.
    persisted_fields = loaded["study"]["grid_summary"]["grid"]["diversity"][
        "diversity_group_fields"
    ]
    assert persisted_fields == {
        "bracket": ["mode", "stopX", "stopRR"],
        "trail": ["mode", "trailMAType", "trailMALength"],
    }


# ===========================================================================
# Workstream A: direct fast-vs-slow real-kernel execution parity
#
# These tests drive the production S06 Numba scalar/batch kernel with focused
# deterministic synthetic price paths and assert exact parity against
# ``S06RTrendV02.run(...)`` (the slow reference).  The synthetic OHLC arrays are
# built only in tests, but they are fed through the *production* signal/indicator
# pipeline (``prepare_fast_data``) and the *production* candidate structures, so
# the comparison locks the real execution contract rather than two Python
# reimplementations.  Any kernel divergence fails these tests.
# ===========================================================================

# Small fast/slow lengths keep %R warm on short synthetic series; everything
# else mirrors a realistic single candidate.  The mode (bracket/trail) flips
# useTrailMA via GridCandidate.params, exactly as production does.
_SYNTH_OVERRIDES = {
    "dateFilter": False,
    "start": "",
    "end": "",
    "enableLong": True,
    "enableShort": True,
    "fastLength": 5,
    "fastSmoothing": 2,
    "slowLength": 15,
    "slowSmoothing": 2,
    "thresholdOS": 20,
    "thresholdOB": 20,
    "stopX": 1.5,
    "stopRR": 2.0,
    "stopLP": 3,
    "stopMaxPct": 50.0,
    "stopMaxDays": 10,
    "riskPerTrade": 2.0,
    "contractSize": 0.01,
    "trailRR": 1.0,
    "trailMAType": "SMA",
    "trailMALength": 10,
    "trailMAOffsetEx": 0.0,
    "initialCapital": 100.0,
    "commissionPct": 0.05,
}

_STRICT_TOLERANCES = {
    "net_profit_pct_abs": 0.001,
    "max_drawdown_pct_abs": 0.001,
    "romad_abs": 0.005,
    "win_rate_abs": 0.001,
    "total_trades_abs": 0.0,
    "winning_trades_abs": 0.0,
    "losing_trades_abs": 0.0,
    "max_consecutive_losses_abs": 0.0,
}


def _ohlc_frame(closes, *, gaps=None, wick=0.5, freq="4h", start="2025-01-01"):
    """Build a deterministic OHLC frame from a close path.

    Open follows the prior close unless an explicit fractional ``gaps`` jump is
    requested (used to drive Open-gap-through-stop/target branches).  Per-bar
    wick emphasis alternates so both intrabar path orders (high-first and
    low-first) are exercised.
    """
    closes = np.asarray(closes, dtype=float)
    n = len(closes)
    opens = np.empty(n)
    highs = np.empty(n)
    lows = np.empty(n)
    opens[0] = closes[0]
    opens[1:] = closes[:-1]
    for index, fraction in (gaps or {}).items():
        opens[index] = closes[index - 1] * (1.0 + fraction)
    for i in range(n):
        o = opens[i]
        c = closes[i]
        hi = max(o, c)
        lo = min(o, c)
        up_wick = wick * (1.3 if i % 2 == 0 else 0.3)
        down_wick = wick * (0.3 if i % 2 == 0 else 1.3)
        highs[i] = hi + up_wick
        lows[i] = lo - down_wick
    index = pd.date_range(start, periods=n, freq=freq, tz="UTC")
    return pd.DataFrame({"Open": opens, "High": highs, "Low": lows, "Close": closes}, index=index)


def _oscillating_closes(n, *, drift=0.03, seed=20260620, noise=0.2):
    t = np.arange(n)
    rng = np.random.default_rng(seed)
    return 100.0 + 15.0 * np.sin(t / 8.0) + 5.0 * np.sin(t / 2.7) + drift * t + rng.normal(0.0, noise, n)


def _synthetic_market(freq="4h", n=360):
    # Oscillation + drift + engineered gaps -> stops, targets, trails, gap exits,
    # next-Open entry fills, and same-side re-entries.
    closes = _oscillating_closes(n)
    return _ohlc_frame(closes, gaps={120: 0.05, 200: -0.06, 280: 0.04}, freq=freq), 30


def _long_synthetic_market():
    # Longer series so MA(200) becomes finite during the trading window, letting
    # every (trailMAType, trailMALength) cache key actually drive a ratchet.
    closes = _oscillating_closes(540, seed=20260621)
    return _ohlc_frame(closes, gaps={300: 0.05, 420: -0.05}), 210


def _trending_tail_market():
    base = _oscillating_closes(300, drift=0.0, noise=0.0)
    tail = base[-1] + np.arange(1, 41) * 2.0
    return _ohlc_frame(np.concatenate([base, tail])), 30


def _flat_market():
    return _ohlc_frame(np.full(140, 100.0), wick=0.0), 30


def _synth_candidate(df, trade_start_idx, mode, **fixed_overrides):
    config = _config(
        modes=(mode,),
        enabled_overrides={name: False for name in GRID_PARAMS}
        | {"thresholdOS": False, "thresholdOB": False},
        fixed_overrides={**_SYNTH_OVERRIDES, **fixed_overrides},
    )
    candidate, data = _single_candidate_data(df, trade_start_idx, config)
    return candidate, data


def _assert_trades_match(trace, slow_trades):
    assert len(trace) == len(slow_trades)
    for fast_trade, slow_trade in zip(trace, slow_trades):
        assert fast_trade["direction"] == slow_trade.direction
        assert fast_trade["entry_time"] == slow_trade.entry_time
        assert fast_trade["exit_time"] == slow_trade.exit_time
        assert fast_trade["entry_price"] == pytest.approx(slow_trade.entry_price, abs=1e-12)
        assert fast_trade["exit_price"] == pytest.approx(slow_trade.exit_price, abs=1e-12)
        assert fast_trade["size"] == pytest.approx(slow_trade.size, abs=1e-12)
        assert fast_trade["net_pnl"] == pytest.approx(slow_trade.net_pnl, abs=1e-12)


def _parity(df, trade_start_idx, mode, **fixed_overrides):
    """Run the real kernel and slow path for one candidate; assert exact parity."""
    candidate, data = _synth_candidate(df, trade_start_idx, mode, **fixed_overrides)
    fast_result = fast_grid.evaluate_candidates(data, [candidate])[0]
    validated = fast_grid.validate_selected_candidates(
        df,
        trade_start_idx,
        [fast_result],
        tolerances=_STRICT_TOLERANCES,
        fail_on_error=True,  # raises on any fast-vs-slow metric divergence
    )[0]
    assert validated.validation_status == "passed"
    trace = fast_grid.evaluate_candidate_trace(data, candidate)
    slow = S06RTrendV02.run(df, candidate.params, trade_start_idx)
    _assert_trades_match(trace, slow.trades)
    return SimpleNamespace(
        fast=fast_result,
        validated=validated,
        trace=trace,
        slow_trades=slow.trades,
        df=df,
    )


_THRESHOLD_PAIRS = [(20, 20), (20, 40), (40, 20), (40, 40)]


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
@pytest.mark.parametrize("entry_mode", ["Reversal @ Triangle", "Trend @ Square"])
@pytest.mark.parametrize("mode", ["bracket", "trail"])
@pytest.mark.parametrize(("threshold_os", "threshold_ob"), _THRESHOLD_PAIRS)
def test_synthetic_parity_entry_modes_thresholds(entry_mode, mode, threshold_os, threshold_ob):
    df, trade_start_idx = _synthetic_market()
    outcome = _parity(
        df,
        trade_start_idx,
        mode,
        entryMode=entry_mode,
        thresholdOS=threshold_os,
        thresholdOB=threshold_ob,
    )
    # The fixture is designed to trade for the symmetric/baseline threshold pair,
    # guaranteeing the bracket and trail execution branches are actually entered.
    if (threshold_os, threshold_ob) == (20, 20):
        assert outcome.fast.total_trades > 0


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
@pytest.mark.parametrize("mode", ["bracket", "trail"])
@pytest.mark.parametrize(
    ("enable_long", "enable_short"),
    [(True, False), (False, True), (True, True)],
)
def test_synthetic_parity_direction_isolation(mode, enable_long, enable_short):
    df, trade_start_idx = _synthetic_market()
    outcome = _parity(
        df,
        trade_start_idx,
        mode,
        enableLong=enable_long,
        enableShort=enable_short,
    )
    directions = {trade.direction for trade in outcome.slow_trades}
    if enable_long and not enable_short:
        assert directions <= {"long"}
    if enable_short and not enable_long:
        assert directions <= {"short"}
    if outcome.slow_trades:
        # Long and short are both reachable in the both-enabled case.
        assert directions


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
@pytest.mark.parametrize("ma_type", ["SMA", "HMA", "KAMA", "T3"])
@pytest.mark.parametrize("ma_length", [50, 100, 150, 200])
def test_synthetic_parity_all_trail_ma_cache_keys(ma_type, ma_length):
    df, trade_start_idx = _long_synthetic_market()
    outcome = _parity(
        df,
        trade_start_idx,
        "trail",
        trailMAType=ma_type,
        trailMALength=ma_length,
        trailRR=1.5,
    )
    assert outcome.fast.total_trades > 0


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
@pytest.mark.parametrize(
    ("mode", "overrides"),
    [
        # Stop/risk axes at their standard extremes (bracket exercises targets).
        ("bracket", {"stopX": 1.0, "stopRR": 1.5, "stopLP": 2, "stopMaxPct": 8.0, "stopMaxDays": 2}),
        ("bracket", {"stopX": 3.0, "stopRR": 3.0, "stopLP": 4, "stopMaxPct": 8.0, "stopMaxDays": 6}),
        # Trail axes at their standard extremes.
        ("trail", {"trailRR": 1.0, "trailMAOffsetEx": 0.0, "trailMALength": 50, "stopX": 1.0, "stopMaxPct": 8.0}),
        ("trail", {"trailRR": 3.0, "trailMAOffsetEx": 2.0, "trailMALength": 200, "stopX": 3.0, "stopMaxPct": 8.0}),
    ],
)
def test_synthetic_parity_risk_axis_extremes(mode, overrides):
    df, trade_start_idx = _long_synthetic_market()
    _parity(df, trade_start_idx, mode, **overrides)


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
@pytest.mark.parametrize("mode", ["bracket", "trail"])
def test_synthetic_parity_open_gaps_through_active_levels(mode):
    # The fixture injects ~5% Open gaps; parity here locks gap-through-stop and
    # gap-through-target (bracket) and gap-through-trail (trail) at the Open.
    df, trade_start_idx = _synthetic_market()
    gap_indices = [120, 200, 280]
    prior_close = df["Close"].to_numpy()
    opens = df["Open"].to_numpy()
    assert any(abs(opens[i] - prior_close[i - 1]) > 1.0 for i in gap_indices)
    _parity(df, trade_start_idx, mode, stopMaxPct=80.0)


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
@pytest.mark.parametrize("mode", ["bracket", "trail"])
def test_synthetic_parity_max_days_market_close(mode):
    # Day-spaced bars + a 1-day cap force market-close-at-next-Open exits.
    df, trade_start_idx = _synthetic_market(freq="1D")
    outcome = _parity(df, trade_start_idx, mode, stopMaxDays=1, stopRR=50.0, stopMaxPct=80.0)
    assert outcome.fast.total_trades > 0
    opens = df["Open"]
    open_priced_exits = [
        trade
        for trade in outcome.slow_trades
        if trade.exit_time in opens.index
        and abs(trade.exit_price - float(opens.loc[trade.exit_time])) < 1e-9
    ]
    # With a 1-day cap most exits are scheduled market closes executed at Open.
    assert open_priced_exits


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
def test_synthetic_parity_final_bar_force_close():
    # Strong trending tail with an unreachable target leaves a position open at
    # the last included bar, forcing a strict final-bar Close exit.
    df, trade_start_idx = _trending_tail_market()
    outcome = _parity(
        df,
        trade_start_idx,
        "bracket",
        entryMode="Trend @ Square",
        stopRR=50.0,
        stopMaxPct=80.0,
        stopMaxDays=999,
    )
    last_timestamp = df.index[-1]
    final_close_trades = [t for t in outcome.slow_trades if t.exit_time == last_timestamp]
    assert len(final_close_trades) == 1
    assert final_close_trades[0].exit_price == pytest.approx(float(df["Close"].iloc[-1]), abs=1e-12)


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
@pytest.mark.parametrize("mode", ["bracket", "trail"])
def test_synthetic_parity_no_trade_agreement(mode):
    df, trade_start_idx = _flat_market()
    outcome = _parity(df, trade_start_idx, mode)
    assert outcome.fast.total_trades == 0
    assert outcome.slow_trades == []


@pytest.mark.skipif(not fast_grid.NUMBA_AVAILABLE, reason="Numba is required")
def test_synthetic_parity_one_thread_vs_many_threads_identical():
    # Determinism: single-thread and multi-thread batches must agree exactly on
    # metrics and candidate identity for a deterministic synthetic batch.
    df, trade_start_idx = _long_synthetic_market()
    config = _config(
        modes=("bracket", "trail"),
        enabled_overrides={
            "stopX": True,
            "stopRR": True,
            "trailMAType": True,
            "trailMALength": True,
            "stopLP": False,
            "stopMaxPct": False,
            "stopMaxDays": False,
            "trailRR": False,
            "trailMAOffsetEx": False,
            "thresholdOS": False,
            "thresholdOB": False,
        },
        fixed_overrides=_SYNTH_OVERRIDES,
    )
    space = fast_grid.build_parameter_space(config)
    allocation = fast_grid.build_allocation(config, space, None)
    source = fast_grid.generate_candidates(config, space, allocation, seed=3).candidates
    data = fast_grid.prepare_fast_data(df, trade_start_idx, source)
    candidates = list(source)
    one = fast_grid.evaluate_candidates(data, candidates, n_workers=1)
    many = fast_grid.evaluate_candidates(data, candidates, n_workers=4)
    assert [r.candidate_id for r in one] == [r.candidate_id for r in many]
    assert [r.semantic_key for r in one] == [r.semantic_key for r in many]
    for a, b in zip(one, many):
        assert a.net_profit_pct == b.net_profit_pct
        assert a.max_drawdown_pct == b.max_drawdown_pct
        assert a.total_trades == b.total_trades
        assert a.profit_factor == b.profit_factor
    ranked_one = sorted(one, key=lambda r: (-r.net_profit_pct, r.candidate_id))
    ranked_many = sorted(many, key=lambda r: (-r.net_profit_pct, r.candidate_id))
    assert [r.candidate_id for r in ranked_one] == [r.candidate_id for r in ranked_many]
