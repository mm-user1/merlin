from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from strategies.s01_trailing_ma.strategy import S01Params, S01TrailingMA
from strategies.s03_reversal_v10.strategy import S03ReversalV10
from strategies.s04_stochrsi.strategy import S04Params, S04StochRSI


PROJECT_ROOT = Path(__file__).parent.parent

EXPECTED_SIGNATURES = {
    "s01": "190c9f1cfe5222cff6086ec912a375bf42993b625a26d241c199c5d2a8098166",
    "s03": "0c2b2d9d70bf906e85227b5530ade1472c6021ba3c036dbafea4cc0c55632226",
    "s04": "2006d2bb158b39122d5aada0a29406b6da28def594b5b5b63dc41cc4302fdfa8",
}


def _stable_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 12)
    return value


def _result_signature(result) -> str:
    payload = {
        "trade_count": len(result.trades),
        "equity_len": len(result.equity_curve),
        "balance_len": len(result.balance_curve),
        "timestamp_len": len(result.timestamps),
        "trades": [
            {
                "direction": trade.direction,
                "entry_time": _stable_value(trade.entry_time),
                "exit_time": _stable_value(trade.exit_time),
                "entry_price": _stable_value(trade.entry_price),
                "exit_price": _stable_value(trade.exit_price),
                "size": _stable_value(trade.size),
                "net_pnl": _stable_value(trade.net_pnl),
                "profit_pct": _stable_value(trade.profit_pct),
            }
            for trade in result.trades
        ],
        "equity_curve": [_stable_value(value) for value in result.equity_curve],
        "balance_curve": [_stable_value(value) for value in result.balance_curve],
        "timestamps": [_stable_value(value) for value in result.timestamps],
        "metrics": {
            "net_profit_pct": _stable_value(result.net_profit_pct),
            "max_drawdown_pct": _stable_value(result.max_drawdown_pct),
            "total_trades": result.total_trades,
            "profit_factor": _stable_value(result.profit_factor),
            "sharpe_ratio": _stable_value(result.sharpe_ratio),
            "romad": _stable_value(result.romad),
            "ulcer_index": _stable_value(result.ulcer_index),
            "sqn": _stable_value(result.sqn),
            "consistency_score": _stable_value(result.consistency_score),
        },
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


@pytest.fixture(scope="module")
def s01_result():
    baseline_metrics = json.loads((PROJECT_ROOT / "data" / "baseline" / "s01_metrics.json").read_text())
    params_dict = baseline_metrics["parameters"]
    params = S01Params.from_dict(params_dict)
    df = load_data(str(PROJECT_ROOT / "data" / "raw" / "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"))
    start_ts = pd.Timestamp(params_dict["start"], tz="UTC")
    end_ts = pd.Timestamp(params_dict["end"], tz="UTC")
    df_prepared, trade_start_idx = prepare_dataset_with_warmup(
        df, start_ts, end_ts, baseline_metrics["warmup_bars"]
    )
    return S01TrailingMA.run(df_prepared, asdict(params), trade_start_idx)


@pytest.fixture(scope="module")
def s03_result():
    df = load_data(str(PROJECT_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"))
    trading_start = pd.Timestamp("2025-02-01", tz="UTC")
    trading_end = pd.Timestamp("2026-02-01", tz="UTC")
    df_prepared, trade_start_idx = prepare_dataset_with_warmup(df, trading_start, trading_end, 1000)
    params = {
        "dateFilter": True,
        "start": trading_start,
        "end": trading_end,
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
    return S03ReversalV10.run(df_prepared, params, trade_start_idx)


@pytest.fixture(scope="module")
def s04_result():
    df = load_data(
        str(PROJECT_ROOT / "data" / "raw" / "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv")
    ).loc[:pd.Timestamp("2025-10-01", tz="UTC")]
    params = asdict(
        S04Params(
            startDate=pd.Timestamp("2025-06-01", tz="UTC"),
            endDate=pd.Timestamp("2025-10-01", tz="UTC"),
        )
    )
    return S04StochRSI.run(df, params, trade_start_idx=0)


@pytest.mark.regression
@pytest.mark.slow
@pytest.mark.parametrize(
    ("result_fixture", "expected_signature"),
    [
        ("s01_result", EXPECTED_SIGNATURES["s01"]),
        ("s03_result", EXPECTED_SIGNATURES["s03"]),
        ("s04_result", EXPECTED_SIGNATURES["s04"]),
    ],
)
def test_strategy_loop_signature_regression(result_fixture, expected_signature, request):
    result = request.getfixturevalue(result_fixture)
    assert _result_signature(result) == expected_signature


@pytest.mark.regression
@pytest.mark.parametrize(
    "result_fixture",
    ["s01_result", "s03_result", "s04_result"],
)
def test_strategy_results_preserve_timestamp_objects(result_fixture, request):
    result = request.getfixturevalue(result_fixture)
    assert result.timestamps
    assert all(isinstance(ts, pd.Timestamp) for ts in result.timestamps)

    for trade in result.trades:
        assert trade.entry_time is None or isinstance(trade.entry_time, pd.Timestamp)
        assert trade.exit_time is None or isinstance(trade.exit_time, pd.Timestamp)
