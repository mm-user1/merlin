import hashlib
import json
import logging
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core import storage
from core.walkforward_engine import (
    OOSStitchedResult,
    WFConfig,
    WFResult,
    WalkForwardEngine,
    WindowResult,
)
from core.optuna_engine import OptimizationResult
from core.post_process import DSRConfig, DSRResult
from core.backtest_engine import StrategyResult
from core.backtest_engine import load_data
from strategies import get_strategy_config


def _build_params_from_config(strategy_id: str):
    config = get_strategy_config(strategy_id)
    parameters = config.get("parameters", {}) if isinstance(config, dict) else {}
    params = {}
    for name, spec in parameters.items():
        if not isinstance(spec, dict):
            continue
        default_value = spec.get("default")
        params[name] = default_value if default_value is not None else 0
    return params


def _build_wf_result(strategy_id: str):
    wf_config = WFConfig(strategy_id=strategy_id, is_period_days=180, oos_period_days=60)
    params = _build_params_from_config(strategy_id)
    engine = WalkForwardEngine(wf_config, {}, {})
    param_id = engine._create_param_id(params)

    window_result = WindowResult(
        window_id=1,
        is_start=pd.Timestamp("2025-01-01", tz="UTC"),
        is_end=pd.Timestamp("2025-06-29", tz="UTC"),
        oos_start=pd.Timestamp("2025-06-30", tz="UTC"),
        oos_end=pd.Timestamp("2025-08-28", tz="UTC"),
        best_params=params,
        param_id=param_id,
        is_net_profit_pct=1.0,
        is_max_drawdown_pct=0.5,
        is_total_trades=5,
        oos_net_profit_pct=2.0,
        oos_max_drawdown_pct=1.0,
        oos_total_trades=4,
        oos_equity_curve=[100.0, 102.0],
        oos_timestamps=[
            pd.Timestamp("2025-06-30", tz="UTC"),
            pd.Timestamp("2025-08-28", tz="UTC"),
        ],
    )

    stitched = OOSStitchedResult(
        final_net_profit_pct=2.0,
        max_drawdown_pct=1.0,
        total_trades=4,
        wfe=100.0,
        oos_win_rate=100.0,
        equity_curve=[100.0, 102.0],
        timestamps=[pd.Timestamp("2025-06-30", tz="UTC"), pd.Timestamp("2025-08-28", tz="UTC")],
        window_ids=[1, 1],
    )

    result = WFResult(
        config=wf_config,
        windows=[window_result],
        stitched_oos=stitched,
        strategy_id=strategy_id,
        total_windows=1,
        trading_start_date=window_result.is_start,
        trading_end_date=window_result.oos_end,
        warmup_bars=wf_config.warmup_bars,
    )

    return result, params, param_id


def test_param_id_generation_s01():
    engine = WalkForwardEngine(WFConfig(strategy_id="s01_trailing_ma"), {}, {})
    params = {"maType": "EMA", "maLength": 45, "closeCountLong": 7}

    expected_hash = hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()[:8]
    assert engine._create_param_id(params) == f"EMA 45_{expected_hash}"


def test_param_id_generation_s04():
    engine = WalkForwardEngine(WFConfig(strategy_id="s04_stochrsi"), {}, {})
    params = {"rsiLen": 16, "stochLen": 20, "kLen": 3}

    expected_hash = hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()[:8]
    assert engine._create_param_id(params) == f"16 20_{expected_hash}"


def test_param_id_falls_back_and_logs_warning(monkeypatch, caplog):
    """
    Ensure _create_param_id logs and falls back to hash when strategy config cannot be read.
    """

    engine = WalkForwardEngine(WFConfig(strategy_id="s01_trailing_ma"), {}, {})
    params = {"maType": "EMA", "maLength": 45, "closeCountLong": 7}
    expected_hash = hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()[:8]

    def raise_value_error(strategy_id):  # noqa: ARG001
        raise ValueError("boom")

    monkeypatch.setattr("strategies.get_strategy_config", raise_value_error)

    with caplog.at_level(logging.WARNING, logger="core.walkforward_engine"):
        param_id = engine._create_param_id(params)

    assert param_id == expected_hash
    assert any("Falling back to hash-only param_id" in record.message for record in caplog.records)


def test_split_data_rolls_forward():
    index = pd.date_range("2025-01-01", periods=40, freq="D", tz="UTC")
    base_row = {"Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.0, "Volume": 100}
    df = pd.DataFrame([base_row for _ in range(len(index))], index=index)

    wf_config = WFConfig(strategy_id="s01_trailing_ma", is_period_days=10, oos_period_days=5)
    engine = WalkForwardEngine(wf_config, {}, {})

    windows = engine.split_data(df, df.index[0], df.index[-1])

    assert len(windows) >= 2
    assert windows[0].oos_start == windows[0].is_end + pd.Timedelta(days=1)
    assert windows[1].is_start == windows[0].is_start + pd.Timedelta(days=5)


def test_stitched_equity_skips_duplicate_points():
    wf_config = WFConfig(strategy_id="s01_trailing_ma", is_period_days=180, oos_period_days=60)
    engine = WalkForwardEngine(wf_config, {}, {})

    windows = [
        WindowResult(
            window_id=1,
            is_start=pd.Timestamp("2025-01-01", tz="UTC"),
            is_end=pd.Timestamp("2025-06-29", tz="UTC"),
            oos_start=pd.Timestamp("2025-06-30", tz="UTC"),
            oos_end=pd.Timestamp("2025-08-28", tz="UTC"),
            best_params={},
            param_id="p1",
            is_net_profit_pct=10.0,
            is_max_drawdown_pct=2.0,
            is_total_trades=5,
            oos_net_profit_pct=5.0,
            oos_max_drawdown_pct=1.0,
            oos_total_trades=3,
            oos_equity_curve=[100.0, 110.0, 120.0],
            oos_timestamps=[
                pd.Timestamp("2025-06-30", tz="UTC"),
                pd.Timestamp("2025-07-30", tz="UTC"),
                pd.Timestamp("2025-08-28", tz="UTC"),
            ],
        ),
        WindowResult(
            window_id=2,
            is_start=pd.Timestamp("2025-06-30", tz="UTC"),
            is_end=pd.Timestamp("2025-12-28", tz="UTC"),
            oos_start=pd.Timestamp("2025-12-29", tz="UTC"),
            oos_end=pd.Timestamp("2026-02-26", tz="UTC"),
            best_params={},
            param_id="p2",
            is_net_profit_pct=8.0,
            is_max_drawdown_pct=2.5,
            is_total_trades=4,
            oos_net_profit_pct=7.0,
            oos_max_drawdown_pct=1.2,
            oos_total_trades=2,
            oos_equity_curve=[100.0, 110.0, 130.0],
            oos_timestamps=[
                pd.Timestamp("2025-12-29", tz="UTC"),
                pd.Timestamp("2026-01-29", tz="UTC"),
                pd.Timestamp("2026-02-26", tz="UTC"),
            ],
        ),
    ]

    stitched = engine._build_stitched_oos_equity(windows)

    assert stitched.equity_curve == pytest.approx([100.0, 110.0, 120.0, 132.0, 156.0])
    assert pytest.approx(stitched.final_net_profit_pct, rel=1e-4) == 56.0


def test_wfe_is_annualized():
    wf_config = WFConfig(strategy_id="s01_trailing_ma", is_period_days=180, oos_period_days=60)
    engine = WalkForwardEngine(wf_config, {}, {})

    windows = [
        WindowResult(
            window_id=1,
            is_start=pd.Timestamp("2025-01-01", tz="UTC"),
            is_end=pd.Timestamp("2025-06-29", tz="UTC"),
            oos_start=pd.Timestamp("2025-06-30", tz="UTC"),
            oos_end=pd.Timestamp("2025-08-28", tz="UTC"),
            best_params={},
            param_id="p1",
            is_net_profit_pct=50.0,
            is_max_drawdown_pct=2.0,
            is_total_trades=5,
            oos_net_profit_pct=20.0,
            oos_max_drawdown_pct=1.0,
            oos_total_trades=3,
            oos_equity_curve=[100.0, 120.0],
            oos_timestamps=[pd.Timestamp("2025-06-30", tz="UTC"), pd.Timestamp("2025-08-28", tz="UTC")],
        ),
        WindowResult(
            window_id=2,
            is_start=pd.Timestamp("2025-06-30", tz="UTC"),
            is_end=pd.Timestamp("2025-12-28", tz="UTC"),
            oos_start=pd.Timestamp("2025-12-29", tz="UTC"),
            oos_end=pd.Timestamp("2026-02-26", tz="UTC"),
            best_params={},
            param_id="p2",
            is_net_profit_pct=50.0,
            is_max_drawdown_pct=2.0,
            is_total_trades=5,
            oos_net_profit_pct=20.0,
            oos_max_drawdown_pct=1.0,
            oos_total_trades=3,
            oos_equity_curve=[100.0, 120.0],
            oos_timestamps=[pd.Timestamp("2025-12-29", tz="UTC"), pd.Timestamp("2026-02-26", tz="UTC")],
        ),
    ]

    stitched = engine._build_stitched_oos_equity(windows)

    assert pytest.approx(stitched.wfe, rel=1e-3) == 120.0


def test_store_top_n_trials_limit():
    engine = WalkForwardEngine(WFConfig(strategy_id="s01_trailing_ma"), {}, {})
    results = []
    for i in range(5):
        results.append(
            OptimizationResult(
                params={"maType": "EMA", "maLength": 10 + i, "closeCountLong": 7},
                net_profit_pct=1.0 + i,
                max_drawdown_pct=0.5,
                total_trades=5,
                optuna_trial_number=i,
            )
        )
    stored = engine._convert_optuna_results_for_storage(results, limit=2)
    assert len(stored) == 2
    assert stored[0]["trial_number"] == 0


def test_run_optuna_on_window_forwards_coverage_mode(monkeypatch):
    index = pd.date_range("2025-01-01", periods=40, freq="D", tz="UTC")
    base_row = {"Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.0, "Volume": 100}
    df = pd.DataFrame([base_row for _ in range(len(index))], index=index)

    wf_config = WFConfig(strategy_id="s01_trailing_ma", is_period_days=10, oos_period_days=5)
    base_template = {
        "enabled_params": {},
        "param_ranges": {},
        "param_types": {},
        "fixed_params": {"dateFilter": False},
        "risk_per_trade_pct": 2.0,
        "contract_size": 0.01,
        "commission_rate": 0.0005,
        "worker_processes": 1,
        "filter_min_profit": False,
        "min_profit_threshold": 0.0,
        "score_config": {},
        "csv_original_name": "OKX_LINKUSDT.P, 30 2025.05.01-2025.11.20.csv",
        "detailed_log": True,
        "trials_log": True,
        "dispatcher_batch_result_processing": False,
        "dispatcher_soft_duplicate_cycle_limit_enabled": False,
        "dispatcher_duplicate_cycle_limit": 42,
        "objectives": ["net_profit_pct"],
        "primary_objective": None,
        "constraints": [],
        "sampler_type": "tpe",
        "population_size": 50,
        "crossover_prob": 0.9,
        "mutation_prob": None,
        "swapping_prob": 0.5,
        "n_startup_trials": 33,
        "coverage_mode": True,
    }
    optuna_settings = {
        "objectives": ["net_profit_pct"],
        "primary_objective": None,
        "constraints": [],
        "budget_mode": "trials",
        "n_trials": 20,
        "time_limit": 3600,
        "convergence_patience": 50,
        "enable_pruning": True,
        "sampler": "tpe",
        "population_size": 50,
        "crossover_prob": 0.9,
        "mutation_prob": None,
        "swapping_prob": 0.5,
        "pruner": "median",
        "warmup_trials": 33,
        "coverage_mode": True,
        "save_study": False,
    }
    engine = WalkForwardEngine(wf_config, base_template, optuna_settings)

    captured = {}

    def fake_run_optuna_optimization(base_config, optuna_cfg):
        captured["base_config"] = base_config
        captured["optuna_cfg"] = optuna_cfg
        return [], None

    monkeypatch.setattr("core.walkforward_engine.run_optuna_optimization", fake_run_optuna_optimization)

    engine._run_optuna_on_window(df, index[0], index[10])

    assert captured["base_config"].coverage_mode is True
    assert captured["base_config"].n_startup_trials == 33
    assert captured["base_config"].csv_original_name == "OKX_LINKUSDT.P, 30 2025.05.01-2025.11.20.csv"
    assert captured["base_config"].detailed_log is True
    assert captured["base_config"].trials_log is True
    assert captured["base_config"].dispatcher_batch_result_processing is False
    assert captured["base_config"].dispatcher_soft_duplicate_cycle_limit_enabled is False
    assert captured["base_config"].dispatcher_duplicate_cycle_limit == 42
    assert captured["optuna_cfg"].coverage_mode is True
    assert captured["optuna_cfg"].warmup_trials == 33


@pytest.mark.slow
def test_run_optuna_on_window_multiprocess_uses_in_memory_worker_csv():
    data_path = Path(__file__).parent.parent / "data" / "raw" / "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"
    if not data_path.exists():
        pytest.skip("Sample data file not available for WFA multiprocess test.")

    df = load_data(str(data_path)).iloc[:1200].copy()
    wf_config = WFConfig(
        strategy_id="s01_trailing_ma",
        is_period_days=60,
        oos_period_days=30,
        warmup_bars=200,
    )
    base_template = {
        "enabled_params": {"maLength": True},
        "param_ranges": {"maLength": (10, 40, 10)},
        "param_types": {"maLength": "int"},
        "fixed_params": {
            "dateFilter": False,
            "maType": "EMA",
            "closeCountLong": 2,
            "closeCountShort": 2,
        },
        "risk_per_trade_pct": 2.0,
        "contract_size": 0.01,
        "commission_rate": 0.0005,
        "worker_processes": 2,
        "filter_min_profit": False,
        "min_profit_threshold": 0.0,
        "score_config": {},
        "objectives": ["net_profit_pct"],
        "primary_objective": None,
        "constraints": [],
        "sampler_type": "tpe",
        "population_size": 50,
        "crossover_prob": 0.9,
        "mutation_prob": None,
        "swapping_prob": 0.5,
        "n_startup_trials": 20,
        "coverage_mode": False,
    }
    optuna_settings = {
        "objectives": ["net_profit_pct"],
        "primary_objective": None,
        "constraints": [],
        "budget_mode": "trials",
        "n_trials": 2,
        "time_limit": 3600,
        "convergence_patience": 10,
        "enable_pruning": False,
        "sampler": "tpe",
        "population_size": 50,
        "crossover_prob": 0.9,
        "mutation_prob": None,
        "swapping_prob": 0.5,
        "pruner": "median",
        "warmup_trials": 20,
        "coverage_mode": False,
    }
    engine = WalkForwardEngine(wf_config, base_template, optuna_settings)

    before_entries = {path.name for path in storage.JOURNAL_DIR.iterdir()}
    results, all_results = engine._run_optuna_on_window(df, df.index[200], df.index[-1])
    after_entries = {path.name for path in storage.JOURNAL_DIR.iterdir()}

    assert after_entries == before_entries
    assert isinstance(results, list)
    assert isinstance(all_results, list)


def test_best_params_source_tracked(monkeypatch):
    index = pd.date_range("2025-01-01", periods=40, freq="D", tz="UTC")
    base_row = {"Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.0, "Volume": 100}
    df = pd.DataFrame([base_row for _ in range(len(index))], index=index)

    optuna_result = OptimizationResult(
        params={"maType": "EMA", "maLength": 20, "closeCountLong": 7},
        net_profit_pct=5.0,
        max_drawdown_pct=1.0,
        total_trades=4,
        optuna_trial_number=12,
        constraints_satisfied=False,
        is_pareto_optimal=True,
    )

    def fake_optuna(self, df_slice, start_time, end_time):  # noqa: ARG001
        return [optuna_result], [optuna_result]

    def fake_dsr(*args, **kwargs):  # noqa: ARG001
        dsr = DSRResult(
            trial_number=12,
            optuna_rank=1,
            params=optuna_result.params,
            original_result=optuna_result,
            dsr_probability=0.5,
            dsr_rank=1,
        )
        return [dsr], {}

    monkeypatch.setattr(WalkForwardEngine, "_run_optuna_on_window", fake_optuna)
    monkeypatch.setattr("core.walkforward_engine.run_dsr_analysis", fake_dsr)

    wf_config = WFConfig(
        strategy_id="s01_trailing_ma",
        is_period_days=10,
        oos_period_days=5,
        warmup_bars=5,
        dsr_config=DSRConfig(enabled=True, top_k=1),
    )
    base_template = {
        "fixed_params": {"dateFilter": False},
        "risk_per_trade_pct": 2.0,
        "contract_size": 0.01,
        "commission_rate": 0.0005,
        "worker_processes": 1,
        "filter_min_profit": False,
        "min_profit_threshold": 0.0,
        "score_config": {},
    }
    engine = WalkForwardEngine(wf_config, base_template, {})

    class FakeStrategy:
        @staticmethod
        def run(df_slice, params, trade_start_idx):  # noqa: ARG001
            return StrategyResult(
                trades=[],
                equity_curve=[100.0],
                balance_curve=[100.0],
                timestamps=[df_slice.index[min(trade_start_idx, len(df_slice) - 1)]],
            )

    engine.strategy_class = FakeStrategy
    result, _study_id = engine.run_wf_optimization(df)

    assert result.windows[0].best_params_source == "dsr"
    assert result.windows[0].is_pareto_optimal is True
    assert result.windows[0].constraints_satisfied is False
def test_walkforward_integration_with_sample_data(monkeypatch):
    data_path = Path(__file__).parent.parent / "data" / "raw" / "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"
    if not data_path.exists():
        pytest.skip("Sample data file not available for integration test.")

    df = load_data(str(data_path))

    strategy_id = "s01_trailing_ma"
    default_params = _build_params_from_config(strategy_id)

    class FakeResult:
        def __init__(self, params):
            self.params = params

    def fake_optuna(self, df_slice, start_time, end_time):  # noqa: ARG001
        fake_results = [FakeResult(default_params)]
        return fake_results, list(fake_results)

    monkeypatch.setattr(WalkForwardEngine, "_run_optuna_on_window", fake_optuna)

    wf_config = WFConfig(
        strategy_id=strategy_id,
        is_period_days=60,
        oos_period_days=30,
        warmup_bars=200,
    )
    base_template = {
        "fixed_params": {"dateFilter": False},
        "risk_per_trade_pct": 2.0,
        "contract_size": 0.01,
        "commission_rate": 0.0005,
    }
    engine = WalkForwardEngine(wf_config, base_template, {})

    result, _study_id = engine.run_wf_optimization(df)

    assert result.total_windows >= 2
    assert result.stitched_oos is not None
    assert result.windows
