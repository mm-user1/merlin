from __future__ import annotations

import json
from pathlib import Path

import pytest
import pandas as pd

from core import backtest_engine
from strategies.s03_reversal_v11.strategy import S03ReversalV11

from s03_like_test_helpers import (
    make_gapless_ohlc,
    normalized_params,
    run_s03_like_v2,
    synthetic_ohlc,
)


PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
TV_JSON_PATH = (
    PROJECT_ROOT
    / "docs"
    / "_work"
    / "S_03-v11_Update"
    / "reference_tv_s03_v11_emergency_sl_10pct.json"
)
TRADING_START = pd.Timestamp("2025-02-01T00:00:00Z")
TRADING_END = pd.Timestamp("2026-02-01T00:00:00Z")
WARMUP_BARS = 1000


@pytest.fixture(scope="module")
def sui_prepared():
    if not DATA_PATH.exists():
        pytest.skip(f"Test data not found: {DATA_PATH}")
    return backtest_engine.prepare_dataset_with_warmup(
        backtest_engine.load_data(str(DATA_PATH)),
        TRADING_START,
        TRADING_END,
        WARMUP_BARS,
    )


def _sui_params(**overrides):
    params = normalized_params(
        {
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
            "positionPct": 100.0,
            "contractSize": 0.01,
            "initialCapital": 100.0,
            "commissionPct": 0.05,
        }
    )
    params.update(overrides)
    return params


def _synthetic_params(**overrides):
    params = normalized_params(
        {
            "dateFilter": False,
            "start": None,
            "end": None,
            "maLength3": 2,
            "maOffset3": 0.0,
            "useCloseCount": True,
            "closeCountLong": 1,
            "closeCountShort": 1,
            "useTBands": False,
            "positionPct": 100.0,
            "contractSize": 0.01,
            "initialCapital": 100.0,
            "commissionPct": 0.0,
            "useEmergencySL": True,
            "emergencySlPct": 10.0,
            "emergencySlUpdateBars": 16,
        }
    )
    params.update(overrides)
    return params


def _next_timestamp(index, timestamp):
    pos = index.get_loc(timestamp)
    if pos >= len(index) - 1:
        return index[-1]
    return index[pos + 1]


def _gapless_structural_df():
    closes = [100.0, 110.0]
    closes += [110.0] * 17
    closes += [111.0 + i for i in range(16)]
    closes += [127.0, 128.0, 129.0, 130.0, 100.0]
    closes += [
        98.0,
        96.0,
        94.0,
        92.0,
        90.0,
        88.0,
        86.0,
        84.0,
        82.0,
        80.0,
        78.0,
        76.0,
        74.0,
        72.0,
        70.0,
        69.0,
        68.0,
        67.0,
        66.0,
        65.0,
        64.0,
        63.0,
        62.0,
        80.0,
        82.0,
        84.0,
        86.0,
        88.0,
        90.0,
    ]
    df = make_gapless_ohlc(synthetic_ohlc(closes))
    df.iloc[39, df.columns.get_loc("Low")] = 99.0
    df.iloc[63, df.columns.get_loc("High")] = max(df.iloc[63]["High"], 90.0)
    return df


def test_s03_like_v2_gapless_structural_gate_against_v1():
    df = _gapless_structural_df()
    params = _synthetic_params()

    v1 = S03ReversalV11.run(df, params)
    v2 = run_s03_like_v2(df, params).strategy_result

    assert sum(1 for trade in v2.trades if trade.exit_reason == "Emergency SL") == 2
    assert v2.total_trades == v1.total_trades == 3
    assert v2.net_profit_pct == pytest.approx(v1.net_profit_pct)

    for v1_trade, v2_trade in zip(v1.trades, v2.trades):
        assert v2_trade.direction == v1_trade.direction
        assert v2_trade.side == v1_trade.side
        assert v2_trade.size == pytest.approx(v1_trade.size)
        assert v2_trade.entry_price == pytest.approx(v1_trade.entry_price)
        assert v2_trade.exit_price == pytest.approx(v1_trade.exit_price)
        assert v2_trade.net_pnl == pytest.approx(v1_trade.net_pnl)
        assert v2_trade.exit_reason == v1_trade.exit_reason
        assert v2_trade.entry_time == _next_timestamp(df.index, v1_trade.entry_time)
        if v1_trade.exit_reason == "Emergency SL":
            expected_exit_time = v1_trade.exit_time
        else:
            expected_exit_time = _next_timestamp(df.index, v1_trade.exit_time)
        assert v2_trade.exit_time == expected_exit_time


def test_s03_like_v2_tradingview_gate_real_sui(sui_prepared):
    if not TV_JSON_PATH.exists():
        pytest.skip(f"TradingView reference JSON not found: {TV_JSON_PATH}")
    df_prepared, trade_start_idx = sui_prepared
    with TV_JSON_PATH.open(encoding="utf-8") as handle:
        reference = json.load(handle)
    metrics = reference["metrics"]

    result = run_s03_like_v2(
        df_prepared,
        _sui_params(useEmergencySL=True, emergencySlPct=10.0, emergencySlUpdateBars=16),
        trade_start_idx=trade_start_idx,
    ).strategy_result
    emergency_exits = [trade for trade in result.trades if trade.exit_reason == "Emergency SL"]

    assert result.net_profit_pct == pytest.approx(metrics["net_profit_pct"], rel=0.02)
    assert result.max_drawdown_pct == pytest.approx(metrics["max_drawdown_pct"], rel=0.05)
    assert abs(result.total_trades - metrics["closed_trades"]) <= 1
    assert abs(result.winning_trades - metrics["winning_trades"]) <= 2
    assert len(emergency_exits) == reference["trade_export_summary"]["emergency_sl_exit_rows"] == 12


def test_s03_like_v2_no_sl_metric_sanity_against_v1(sui_prepared):
    df_prepared, trade_start_idx = sui_prepared
    params = _sui_params(useEmergencySL=False, emergencySlPct=0.0, emergencySlUpdateBars=0)

    v1 = S03ReversalV11.run(df_prepared, params, trade_start_idx)
    v2 = run_s03_like_v2(df_prepared, params, trade_start_idx=trade_start_idx).strategy_result

    assert v2.total_trades == v1.total_trades
    assert v2.winning_trades == v1.winning_trades
    assert v2.net_profit_pct == pytest.approx(v1.net_profit_pct, rel=0.02)
    assert all(trade.exit_reason is None for trade in v2.trades)
