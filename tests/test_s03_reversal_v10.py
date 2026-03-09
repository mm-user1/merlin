from pathlib import Path

import pandas as pd
import pytest

import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core import backtest_engine, metrics
from strategies.s03_reversal_v10.strategy import S03ReversalV10


PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
TRADING_START = pd.Timestamp("2025-02-01", tz="UTC")
TRADING_END = pd.Timestamp("2026-02-01", tz="UTC")
WARMUP_BARS = 1000


@pytest.fixture(scope="module")
def test_data():
    if not DATA_PATH.exists():
        pytest.skip(f"Test data not found: {DATA_PATH}")
    return backtest_engine.load_data(str(DATA_PATH))


def test_s03_basic_run(test_data):
    df_prepared, trade_start_idx = backtest_engine.prepare_dataset_with_warmup(
        test_data, TRADING_START, TRADING_END, WARMUP_BARS
    )

    params = {
        "dateFilter": True,
        "start": TRADING_START,
        "end": TRADING_END,
        "maType3": "SMA",
        "maLength3": 75,
        "maOffset3": 0.2,
        "useCloseCount": True,
        "closeCountLong": 7,
        "closeCountShort": 5,
        "useTBands": True,
        "tBandLongPct": 1.0,
        "tBandShortPct": 1.3,
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }

    result = S03ReversalV10.run(df_prepared, params, trade_start_idx)

    assert result is not None
    assert isinstance(result.trades, list)
    assert len(result.equity_curve) == len(df_prepared)
    assert len(result.balance_curve) == len(df_prepared)


def test_s03_reference_performance(test_data):
    df_prepared, trade_start_idx = backtest_engine.prepare_dataset_with_warmup(
        test_data, TRADING_START, TRADING_END, WARMUP_BARS
    )

    params = {
        "dateFilter": True,
        "start": TRADING_START,
        "end": TRADING_END,
        "maType3": "SMA",
        "maLength3": 75,
        "maOffset3": 0.2,
        "useCloseCount": True,
        "closeCountLong": 7,
        "closeCountShort": 5,
        "useTBands": True,
        "tBandLongPct": 1.0,
        "tBandShortPct": 1.3,
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }

    result = S03ReversalV10.run(df_prepared, params, trade_start_idx)

    basic = metrics.calculate_basic(result, initial_balance=params["initialCapital"])

    expected_net_profit_pct = 186.61
    expected_max_dd_pct = 35.49
    expected_total_trades = 221

    tolerance = 0.05

    assert abs(basic.net_profit_pct - expected_net_profit_pct) / expected_net_profit_pct <= tolerance, (
        f"Net Profit mismatch: {basic.net_profit_pct}% vs expected {expected_net_profit_pct}%"
    )

    assert abs(basic.max_drawdown_pct - expected_max_dd_pct) / expected_max_dd_pct <= tolerance, (
        f"Max DD mismatch: {basic.max_drawdown_pct}% vs expected {expected_max_dd_pct}%"
    )

    assert abs(basic.total_trades - expected_total_trades) <= 5, (
        f"Total trades mismatch: {basic.total_trades} vs expected {expected_total_trades}"
    )


def test_s03_omitted_close_count_params_match_disabled_close_count_behavior(test_data):
    df_prepared, trade_start_idx = backtest_engine.prepare_dataset_with_warmup(
        test_data, TRADING_START, TRADING_END, WARMUP_BARS
    )

    base_params = {
        "dateFilter": True,
        "start": TRADING_START,
        "end": TRADING_END,
        "maType3": "SMA",
        "maLength3": 75,
        "maOffset3": 0.2,
        "useCloseCount": False,
        "useTBands": True,
        "tBandLongPct": 1.0,
        "tBandShortPct": 1.3,
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }

    explicit = dict(base_params)
    explicit.update({"closeCountLong": 2, "closeCountShort": 7})
    omitted = dict(base_params)

    result_explicit = S03ReversalV10.run(df_prepared, explicit, trade_start_idx)
    result_omitted = S03ReversalV10.run(df_prepared, omitted, trade_start_idx)

    assert result_explicit.equity_curve == pytest.approx(result_omitted.equity_curve)
    assert result_explicit.balance_curve == pytest.approx(result_omitted.balance_curve)
    assert result_explicit.timestamps == result_omitted.timestamps
    assert len(result_explicit.trades) == len(result_omitted.trades)


def test_s03_omitted_tband_params_match_disabled_tband_behavior(test_data):
    df_prepared, trade_start_idx = backtest_engine.prepare_dataset_with_warmup(
        test_data, TRADING_START, TRADING_END, WARMUP_BARS
    )

    base_params = {
        "dateFilter": True,
        "start": TRADING_START,
        "end": TRADING_END,
        "maType3": "SMA",
        "maLength3": 75,
        "maOffset3": 0.2,
        "useCloseCount": True,
        "closeCountLong": 7,
        "closeCountShort": 5,
        "useTBands": False,
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }

    explicit = dict(base_params)
    explicit.update({"tBandLongPct": 0.2, "tBandShortPct": 2.0})
    omitted = dict(base_params)

    result_explicit = S03ReversalV10.run(df_prepared, explicit, trade_start_idx)
    result_omitted = S03ReversalV10.run(df_prepared, omitted, trade_start_idx)

    assert result_explicit.equity_curve == pytest.approx(result_omitted.equity_curve)
    assert result_explicit.balance_curve == pytest.approx(result_omitted.balance_curve)
    assert result_explicit.timestamps == result_omitted.timestamps
    assert len(result_explicit.trades) == len(result_omitted.trades)
