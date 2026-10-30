"""Shared request/study builders and diagnostics for server tests."""

from pathlib import Path
from contextlib import contextmanager

import pytest
import pandas as pd

from core.walkforward_engine import OOSStitchedResult, WFConfig, WFResult, WindowResult
from core.storage import create_new_db, get_active_db_name, save_wfa_study_to_db, set_active_db
from strategies import get_strategy_config

REPO_ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def _temporary_active_db(label: str):
    previous_db = get_active_db_name()
    create_new_db(label)
    try:
        yield
    finally:
        set_active_db(previous_db)


def _grid_sidebar_config() -> dict:
    return {
        "strategy_id": "s03_reversal_v10",
        "optimization_mode": "grid",
        "enabled_params": {
            "maType3": True,
            "maLength3": True,
            "maOffset3": False,
            "useCloseCount": True,
            "useTBands": True,
            "closeCountLong": True,
            "closeCountShort": True,
            "tBandLongPct": True,
            "tBandShortPct": True,
        },
        "param_ranges": {
            "maLength3": [3, 4, 1],
            "closeCountLong": [1, 2, 1],
            "closeCountShort": [1, 1, 1],
            "tBandLongPct": [0.5, 1.0, 0.5],
            "tBandShortPct": [0.5, 1.0, 0.5],
        },
        "param_types": {
            "maType3": "select",
            "maLength3": "int",
            "maOffset3": "float",
            "useCloseCount": "bool",
            "useTBands": "bool",
            "closeCountLong": "int",
            "closeCountShort": "int",
            "tBandLongPct": "float",
            "tBandShortPct": "float",
        },
        "fixed_params": {
            "maType3_options": ["SMA"],
            "useCloseCount_options": [True],
            "useTBands_options": [True],
            "contractSize": 0.01,
            "commissionPct": 0.05,
            "dateFilter": False,
        },
        "worker_processes": 6,
        "warmup_bars": 20,
        "risk_per_trade_pct": 2.0,
        "contract_size": 0.01,
        "commission_rate": 0.0005,
        "filter_min_profit": False,
        "min_profit_threshold": 0.0,
        "score_config": {},
        "objectives": ["net_profit_pct"],
        "primary_objective": None,
        "constraints": [],
        "grid_budget": 10,
        "grid_seed": 42,
        "grid_top_candidates": 5,
        "grid_allocation_method": "auto_sqrt_space",
        "grid_min_quota": 0.10,
        "grid_diversity_enabled": True,
        "grid_diversity_max_per_group": 2,
        "grid_strict_validation": True,
        "grid_config": {
            "budget": 10,
            "seed": 42,
            "top_candidates": 5,
            "allocation_method": "auto_sqrt_space",
            "min_quota": 0.10,
        },
    }


_S03_REGIME_ER_MA_TYPES = ["EMA", "SMA", "HMA", "WMA", "ALMA", "KAMA", "TMA", "T3", "DEMA", "VWMA"]


_S03_REGIME_ER_COUNT_AXES = {
    "maType3": True,
    "maLength3": True,
    "useCloseCount": True,
    "closeCountLong": True,
    "closeCountShort": True,
    "useTBands": True,
    "tBandLongPct": True,
    "tBandShortPct": True,
}


def _s03_regime_er_grid_preview_payload(**overrides):
    payload = {
        "strategy_id": "s03_reversal_v11_regime_er_b2",
        "optimization_mode": "grid",
        "enabled_params": dict(_S03_REGIME_ER_COUNT_AXES),
        "param_ranges": {},
        "param_types": {},
        "fixed_params": {
            "dateFilter": False,
            "maType3_options": list(_S03_REGIME_ER_MA_TYPES),
            "useRegime": False,
            "useEmergencySL": False,
        },
        "objectives": ["net_profit_pct"],
        "grid_fast_objectives": ["net_profit_pct"],
        "grid_budget": "200k",
        "grid_top_candidates": 5,
    }
    payload.update(overrides)
    return payload


def _v2_runtime_diagnostic(response):
    body = response.get_json()
    assert isinstance(body, dict)
    assert body.get("error")
    diagnostics = body.get("diagnostics")
    assert isinstance(diagnostics, list) and diagnostics
    return diagnostics[0]


def _build_minimal_optuna_payload():
    return {
        "strategy": "s01_trailing_ma",
        "enabled_params": {},
        "param_ranges": {},
        "fixed_params": {},
        "objectives": ["net_profit_pct"],
        "primary_objective": None,
        "optuna_budget_mode": "trials",
        "optuna_n_trials": 10,
        "optuna_time_limit": 60,
        "optuna_convergence": 10,
    }


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


def _create_wfa_study() -> str:
    data_path = REPO_ROOT / "data" / "raw" / "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"
    if not data_path.exists():
        pytest.skip("Sample data file not available for WFA API tests.")

    strategy_id = "s01_trailing_ma"
    params = _build_params_from_config(strategy_id)
    wf_config = WFConfig(strategy_id=strategy_id, is_period_days=30, oos_period_days=15, warmup_bars=10)

    window = WindowResult(
        window_id=1,
        is_start=pd.Timestamp("2025-05-01", tz="UTC"),
        is_end=pd.Timestamp("2025-05-30", tz="UTC"),
        oos_start=pd.Timestamp("2025-05-31", tz="UTC"),
        oos_end=pd.Timestamp("2025-06-14", tz="UTC"),
        trade_start=pd.Timestamp("2025-05-31", tz="UTC"),
        trade_end=pd.Timestamp("2025-06-14", tz="UTC"),
        best_params=params,
        param_id="test_params",
        is_net_profit_pct=0.0,
        is_max_drawdown_pct=0.0,
        is_total_trades=0,
        oos_net_profit_pct=0.0,
        oos_max_drawdown_pct=0.0,
        oos_total_trades=0,
        oos_equity_curve=[100.0],
        oos_timestamps=[pd.Timestamp("2025-05-31", tz="UTC")],
        is_equity_curve=[100.0],
        is_timestamps=[pd.Timestamp("2025-05-01", tz="UTC")],
        best_params_source="optuna_is",
        available_modules=["optuna_is"],
        optuna_is_trials=[
            {
                "trial_number": 1,
                "params": params,
                "param_id": "test_params",
                "net_profit_pct": 0.0,
                "max_drawdown_pct": 0.0,
                "total_trades": 0,
                "win_rate": 0.0,
                "is_selected": True,
            }
        ],
    )

    stitched = OOSStitchedResult(
        final_net_profit_pct=0.0,
        max_drawdown_pct=0.0,
        total_trades=0,
        wfe=0.0,
        oos_win_rate=0.0,
        equity_curve=[100.0],
        timestamps=[pd.Timestamp("2025-05-31", tz="UTC")],
        window_ids=[1],
    )

    wf_result = WFResult(
        config=wf_config,
        windows=[window],
        stitched_oos=stitched,
        strategy_id=strategy_id,
        total_windows=1,
        trading_start_date=window.is_start,
        trading_end_date=window.oos_end,
        warmup_bars=wf_config.warmup_bars,
    )

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={"fixed_params": {}},
        csv_file_path=str(data_path),
        start_time=0.0,
        score_config=None,
    )
    return study_id
