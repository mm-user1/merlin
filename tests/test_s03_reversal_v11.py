from __future__ import annotations

import csv
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core import backtest_engine
from core.grid_engine import _load_backend, supports_fast_grid
from core.optuna_engine import OptimizationConfig
from strategies import get_strategy, get_strategy_config
from strategies.s03_reversal_v10 import fast_grid as v10_fast_grid
from strategies.s03_reversal_v10.strategy import S03ReversalV10
from strategies.s03_reversal_v11 import fast_grid as v11_fast_grid
from strategies.s03_reversal_v11.strategy import S03Params, S03ReversalV11


PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
TV_TRADES_PATH = (
    PROJECT_ROOT
    / "docs"
    / "_work"
    / "S_03-v11_Update"
    / "S_03_Reversal_v11_OKX_SUIUSDT.P_2026-07-16.csv"
)
TRADING_START = pd.Timestamp("2025-02-01T00:00:00Z")
TRADING_END = pd.Timestamp("2026-02-01T00:00:00Z")
WARMUP_BARS = 1000
FAST_VALIDATION_TOLERANCES = {
    "net_profit_pct_abs": 0.001,
    "max_drawdown_pct_abs": 0.001,
    "romad_abs": 0.005,
    "win_rate_abs": 0.001,
    "total_trades_abs": 0.0,
    "winning_trades_abs": 0.0,
    "losing_trades_abs": 0.0,
    "max_consecutive_losses_abs": 0.0,
}


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


def _default_s03_params(**overrides) -> dict:
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
    params.update(overrides)
    return params


def _synthetic_df(
    closes: list[float],
    *,
    opens: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
) -> pd.DataFrame:
    values = np.asarray(closes, dtype=float)
    index = pd.date_range("2025-01-01", periods=len(values), freq="h", tz="UTC")
    opens_arr = np.asarray(opens if opens is not None else closes, dtype=float)
    highs_arr = np.asarray(highs if highs is not None else np.maximum(opens_arr, values), dtype=float)
    lows_arr = np.asarray(lows if lows is not None else np.minimum(opens_arr, values), dtype=float)
    return pd.DataFrame(
        {
            "Open": opens_arr,
            "High": highs_arr,
            "Low": lows_arr,
            "Close": values,
            "Volume": np.full(len(values), 1000.0),
        },
        index=index,
    )


def _synthetic_params(**overrides) -> dict:
    params = _default_s03_params(
        dateFilter=False,
        start=None,
        end=None,
        maLength3=2,
        maOffset3=0.0,
        useCloseCount=True,
        closeCountLong=1,
        closeCountShort=1,
        useTBands=False,
        contractSize=0.01,
        initialCapital=100.0,
        commissionPct=0.0,
        useEmergencySL=True,
        emergencySlPct=10.0,
        emergencySlUpdateBars=16,
    )
    params.update(overrides)
    return params


def _trade_tuple(trade) -> tuple:
    return (
        trade.direction,
        trade.side,
        trade.entry_time,
        trade.exit_time,
        trade.entry_price,
        trade.exit_price,
        trade.size,
        trade.net_pnl,
        trade.profit_pct,
        getattr(trade, "exit_reason", None),
    )


def _grid_candidate(params: dict, *, candidate_id: int = 1, mode: str = "both") -> v11_fast_grid.GridCandidate:
    return v11_fast_grid.GridCandidate(
        candidate_id=candidate_id,
        mode=mode,
        params=params,
        semantic_key=v11_fast_grid.candidate_semantic_key(mode, params),
        generation_mode="test",
        diversity_group=f"{mode}|{params.get('maType3', 'SMA')}|{int(params.get('maLength3', 75))}",
    )


def _validate_fast_grid_candidate(
    df: pd.DataFrame,
    trade_start_idx: int,
    params: dict,
    *,
    candidate_id: int = 1,
    mode: str = "both",
) -> tuple:
    candidate = _grid_candidate(params, candidate_id=candidate_id, mode=mode)
    data = v11_fast_grid.prepare_fast_data(df, trade_start_idx, [candidate])
    fast_result = v11_fast_grid.evaluate_candidates(data, [candidate])[0]
    validated = v11_fast_grid.validate_selected_candidates(
        df,
        trade_start_idx,
        [fast_result],
        tolerances=FAST_VALIDATION_TOLERANCES,
        fail_on_error=True,
    )[0]

    assert validated.validation_status == "passed"
    assert fast_result.net_profit_pct == pytest.approx(validated.net_profit_pct, abs=0.001)
    assert fast_result.max_drawdown_pct == pytest.approx(validated.max_drawdown_pct, abs=0.001)
    assert fast_result.total_trades == validated.total_trades
    assert fast_result.winning_trades == validated.winning_trades
    assert fast_result.losing_trades == validated.losing_trades
    assert fast_result.profit_factor == pytest.approx(validated.profit_factor, abs=0.001)
    assert fast_result.romad == pytest.approx(validated.romad, abs=0.005)
    return fast_result, validated


def test_s03_v11_registration_and_default_params():
    config = get_strategy_config("s03_reversal_v11")

    assert get_strategy("s03_reversal_v11") is S03ReversalV11
    assert config["version"] == "v11"
    assert config["parameters"]["useEmergencySL"]["default"] is False
    assert config["parameters"]["emergencySlPct"]["optimize"]["default_enabled"] is False

    params = S03Params.from_dict({})
    assert params.useEmergencySL is False
    assert params.emergencySlPct == 20.0
    assert params.emergencySlUpdateBars == 16


def test_s03_v11_disabled_emergency_matches_v10_trade_for_trade(sui_prepared):
    df_prepared, trade_start_idx = sui_prepared
    v10_result = S03ReversalV10.run(df_prepared, _default_s03_params(), trade_start_idx)
    v11_result = S03ReversalV11.run(
        df_prepared,
        _default_s03_params(useEmergencySL=False, emergencySlPct=10.0, emergencySlUpdateBars=1),
        trade_start_idx,
    )

    assert [_trade_tuple(t) for t in v11_result.trades] == [_trade_tuple(t) for t in v10_result.trades]
    assert v11_result.equity_curve == pytest.approx(v10_result.equity_curve)
    assert v11_result.balance_curve == pytest.approx(v10_result.balance_curve)
    assert v11_result.timestamps == v10_result.timestamps
    assert v11_result.to_dict() == v10_result.to_dict()


def test_s03_v11_disabled_invalid_emergency_params_are_inert(sui_prepared):
    parsed = S03Params.from_dict(
        {
            "useEmergencySL": False,
            "emergencySlPct": 0.0,
            "emergencySlUpdateBars": 0,
        }
    )
    assert parsed.useEmergencySL is False
    assert parsed.emergencySlPct == 0.0
    assert parsed.emergencySlUpdateBars == 0

    df_prepared, trade_start_idx = sui_prepared
    v10_result = S03ReversalV10.run(df_prepared, _default_s03_params(), trade_start_idx)
    v11_result = S03ReversalV11.run(
        df_prepared,
        _default_s03_params(useEmergencySL=False, emergencySlPct=0.0, emergencySlUpdateBars=0),
        trade_start_idx,
    )

    assert [_trade_tuple(t) for t in v11_result.trades] == [_trade_tuple(t) for t in v10_result.trades]
    assert v11_result.to_dict() == v10_result.to_dict()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"emergencySlPct": 0.0}, "emergencySlPct"),
        ({"emergencySlUpdateBars": 0}, "emergencySlUpdateBars"),
    ],
)
def test_s03_v11_enabled_invalid_emergency_params_raise(overrides, message):
    with pytest.raises(ValueError, match=message):
        S03Params.from_dict(_synthetic_params(useEmergencySL=True, **overrides))


def test_s03_v11_emergency_sl_10pct_tradingview_parity(sui_prepared):
    df_prepared, trade_start_idx = sui_prepared
    result = S03ReversalV11.run(
        df_prepared,
        _default_s03_params(useEmergencySL=True, emergencySlPct=10.0, emergencySlUpdateBars=16),
        trade_start_idx,
    )
    emergency_exits = [t for t in result.trades if getattr(t, "exit_reason", None) == "Emergency SL"]

    assert result.net_profit_pct == pytest.approx(213.79, rel=0.02)
    assert result.max_drawdown_pct == pytest.approx(36.35, rel=0.05)
    assert abs(result.total_trades - 222) <= 1
    assert abs(result.winning_trades - 82) <= 2
    assert len(emergency_exits) == 12


def test_s03_v11_reference_csv_integrity():
    if not TV_TRADES_PATH.exists():
        pytest.skip(f"TradingView trade CSV not found: {TV_TRADES_PATH}")

    exit_rows = []
    with TV_TRADES_PATH.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if str(row["Type"]).startswith("Exit"):
                exit_rows.append(row)

    assert len(exit_rows) == 222
    assert sum(1 for row in exit_rows if float(row["Net PnL USDT"]) > 0.0) == 82
    assert sum(1 for row in exit_rows if row["Signal"] == "Emergency SL") == 12
    assert float(exit_rows[-1]["Cumulative PnL %"]) == pytest.approx(213.79, abs=0.01)


def test_emergency_sl_long_ignores_entry_and_next_bar_then_gap_fills_open():
    df = _synthetic_df(
        [100.0, 110.0, 111.0, 112.0],
        opens=[100.0, 110.0, 111.0, 98.0],
        highs=[100.0, 111.0, 112.0, 113.0],
        lows=[100.0, 109.0, 90.0, 98.0],
    )

    result = S03ReversalV11.run(df, _synthetic_params(), force_close_last_bar=False)
    emergency_trades = [t for t in result.trades if t.exit_reason == "Emergency SL"]

    assert len(emergency_trades) == 1
    assert emergency_trades[0].entry_time == df.index[1]
    assert emergency_trades[0].exit_time == df.index[3]
    assert emergency_trades[0].exit_price == 98.0


def test_emergency_sl_short_ignores_entry_and_next_bar_then_gap_fills_open():
    df = _synthetic_df(
        [100.0, 90.0, 89.0, 88.0],
        opens=[100.0, 90.0, 89.0, 101.0],
        highs=[100.0, 91.0, 110.0, 105.0],
        lows=[100.0, 89.0, 88.0, 87.0],
    )

    result = S03ReversalV11.run(df, _synthetic_params(), force_close_last_bar=False)
    emergency_trades = [t for t in result.trades if t.exit_reason == "Emergency SL"]

    assert len(emergency_trades) == 1
    assert emergency_trades[0].entry_time == df.index[1]
    assert emergency_trades[0].exit_time == df.index[3]
    assert emergency_trades[0].exit_price == 101.0


def test_first_ratchet_attempt_occurs_at_k_plus_17_for_update_bars_16():
    closes = [100.0, 110.0] + [111.0 + idx for idx in range(17)]
    df = _synthetic_df(closes, lows=[price - 1.0 for price in closes], highs=[price + 1.0 for price in closes])
    params = _synthetic_params(emergencySlUpdateBars=16)

    before_attempt = S03ReversalV11.run(df.iloc[:18].copy(), params, force_close_last_bar=False)
    at_attempt = S03ReversalV11.run(df.iloc[:19].copy(), params, force_close_last_bar=False)

    assert before_attempt.last_position["sl_price"] == pytest.approx(99.0)
    assert at_attempt.last_position["sl_price"] == pytest.approx(closes[18] * 0.9)


def test_last_position_sl_price_only_populates_when_emergency_enabled():
    df = _synthetic_df(
        [100.0, 110.0, 111.0, 112.0],
        lows=[100.0, 109.0, 110.0, 111.0],
        highs=[100.0, 111.0, 112.0, 113.0],
    )

    enabled = S03ReversalV11.run(df, _synthetic_params(), force_close_last_bar=False)
    disabled = S03ReversalV11.run(
        df,
        _synthetic_params(useEmergencySL=False),
        force_close_last_bar=False,
    )

    assert enabled.last_position["sl_price"] == pytest.approx(99.0)
    assert disabled.last_position["sl_price"] is None


def _grid_config(strategy_id: str, **overrides) -> OptimizationConfig:
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
    enabled.update(overrides.pop("enabled_overrides", {}))
    ranges = {
        "maLength3": (3, 5, 1),
        "maOffset3": (0, 1, 0.5),
        "closeCountLong": (1, 2, 1),
        "closeCountShort": (1, 1, 1),
        "tBandLongPct": (0.5, 1.0, 0.5),
        "tBandShortPct": (0.5, 1.0, 0.5),
    }
    ranges.update(overrides.pop("range_overrides", {}))
    fixed = {
        "maType3_options": ["SMA", "EMA"],
        "useCloseCount_options": [True, False],
        "useTBands_options": [True, False],
        "contractSize": 0.01,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }
    fixed.update(overrides.pop("fixed_overrides", {}))
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
        "useEmergencySL": "bool",
        "emergencySlPct": "float",
        "emergencySlUpdateBars": "int",
    }
    payload = {
        "csv_file": "unused.csv",
        "strategy_id": strategy_id,
        "enabled_params": enabled,
        "param_ranges": ranges,
        "param_types": param_types,
        "fixed_params": fixed,
        "worker_processes": 1,
        "warmup_bars": 20,
        "optimization_mode": "grid",
        "objectives": ["net_profit_pct"],
        "grid_budget": 25,
        "grid_seed": 42,
        "grid_top_candidates": 5,
    }
    payload.update(overrides)
    return OptimizationConfig(**payload)


def test_s03_v11_fast_grid_registration_and_disabled_space_matches_v10():
    assert supports_fast_grid("s03_reversal_v11") is True
    assert _load_backend("s03_reversal_v11").__name__.endswith("s03_reversal_v11.fast_grid")
    assert _load_backend("s03_reversal_v10").__name__.endswith("s03_reversal_v10.fast_grid")

    v10_space = v10_fast_grid.build_parameter_space(_grid_config("s03_reversal_v10"))
    v11_space = v11_fast_grid.build_parameter_space(
        _grid_config(
            "s03_reversal_v11",
            fixed_overrides={"useEmergencySL": False, "emergencySlPct": 10.0, "emergencySlUpdateBars": 1},
        )
    )

    assert v11_space.mode_space_sizes == v10_space.mode_space_sizes
    assert v11_space.total_space_size == v10_space.total_space_size


def test_s03_v11_fast_grid_emergency_axis_and_semantic_identity():
    disabled_a = {
        "maType3": "SMA",
        "maLength3": 5,
        "maOffset3": 0.0,
        "closeCountLong": 1,
        "closeCountShort": 1,
        "tBandLongPct": 0.5,
        "tBandShortPct": 0.5,
        "useEmergencySL": False,
        "emergencySlPct": 10.0,
    }
    disabled_b = {**disabled_a, "emergencySlPct": 20.0, "emergencySlUpdateBars": 1}
    enabled_a = {**disabled_a, "useEmergencySL": True, "emergencySlPct": 10.0, "emergencySlUpdateBars": 16}
    enabled_b = {**enabled_a, "emergencySlPct": 20.0}

    assert v11_fast_grid.candidate_semantic_key("both", disabled_a) == v11_fast_grid.candidate_semantic_key(
        "both",
        disabled_b,
    )
    assert v11_fast_grid.candidate_semantic_key("both", enabled_a) != v11_fast_grid.candidate_semantic_key(
        "both",
        enabled_b,
    )

    config = _grid_config(
        "s03_reversal_v11",
        enabled_overrides={"emergencySlPct": True},
        range_overrides={"emergencySlPct": (10.0, 20.0, 10.0)},
        fixed_overrides={"useEmergencySL": True, "emergencySlUpdateBars": 16},
    )
    space = v11_fast_grid.build_parameter_space(config)

    assert space.axes["emergencySlPct"] == [10.0, 20.0]
    assert space.total_space_size == v10_fast_grid.build_parameter_space(_grid_config("s03_reversal_v10")).total_space_size * 2


@pytest.mark.skipif(not v11_fast_grid.NUMBA_AVAILABLE, reason="Numba is required for S03 v11 fast Grid parity")
@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"emergencySlPct": 0.0}, "emergencySlPct"),
        ({"emergencySlUpdateBars": 0}, "emergencySlUpdateBars"),
    ],
)
def test_s03_v11_fast_grid_rejects_enabled_invalid_emergency_params(overrides, message):
    df = _synthetic_df([100.0, 110.0, 111.0, 112.0])
    params = _synthetic_params(useEmergencySL=True, **overrides)
    candidate = _grid_candidate(params, mode="cc_only")
    data = v11_fast_grid.prepare_fast_data(df, 0, [candidate])

    with pytest.raises(ValueError, match=message):
        v11_fast_grid.evaluate_candidates(data, [candidate])


@pytest.mark.skipif(not v11_fast_grid.NUMBA_AVAILABLE, reason="Numba is required for S03 v11 fast Grid parity")
def test_s03_v11_fast_grid_disabled_invalid_emergency_params_are_inert():
    df = _synthetic_df(
        [100.0, 110.0, 111.0, 112.0, 113.0],
        opens=[100.0, 110.0, 111.0, 98.0, 113.0],
        highs=[100.0, 111.0, 112.0, 113.0, 114.0],
        lows=[100.0, 109.0, 90.0, 98.0, 112.0],
    )
    params = _synthetic_params(useEmergencySL=False, emergencySlPct=0.0, emergencySlUpdateBars=0)

    fast_result, validated = _validate_fast_grid_candidate(df, 0, params, mode="cc_only")

    assert validated.validation_status == "passed"
    assert fast_result.total_trades == validated.total_trades


@pytest.mark.skipif(not v11_fast_grid.NUMBA_AVAILABLE, reason="Numba is required for S03 v11 fast Grid parity")
@pytest.mark.parametrize(
    ("pct", "expected_trades", "expected_wins", "expected_emergency_exits"),
    [
        (10.0, 222, 82, 12),
        (5.0, None, None, None),
    ],
)
def test_s03_v11_real_data_fast_grid_matches_slow_for_emergency_ratchets(
    sui_prepared,
    pct,
    expected_trades,
    expected_wins,
    expected_emergency_exits,
):
    df_prepared, trade_start_idx = sui_prepared
    params = _default_s03_params(useEmergencySL=True, emergencySlPct=pct, emergencySlUpdateBars=16)

    slow_result = S03ReversalV11.run(df_prepared, params, trade_start_idx)
    fast_result, validated = _validate_fast_grid_candidate(
        df_prepared,
        trade_start_idx,
        params,
        candidate_id=int(pct * 10),
        mode="both",
    )
    emergency_exits = [t for t in slow_result.trades if getattr(t, "exit_reason", None) == "Emergency SL"]

    assert fast_result.total_trades == validated.total_trades
    assert fast_result.winning_trades == validated.winning_trades
    assert fast_result.losing_trades == validated.losing_trades
    assert validated.total_trades == slow_result.total_trades
    assert validated.winning_trades == slow_result.winning_trades
    assert validated.losing_trades == slow_result.losing_trades
    if expected_trades is not None:
        assert slow_result.total_trades == expected_trades
        assert slow_result.winning_trades == expected_wins
        assert len(emergency_exits) == expected_emergency_exits
    else:
        assert slow_result.total_trades >= 250
        assert len(emergency_exits) >= 100


@pytest.mark.skipif(not v11_fast_grid.NUMBA_AVAILABLE, reason="Numba is required for S03 v11 fast Grid parity")
@pytest.mark.parametrize(
    ("use_emergency", "pct"),
    [(False, 10.0), (True, 10.0), (True, 20.0)],
)
def test_s03_v11_fast_grid_matches_slow_for_emergency_candidates(use_emergency, pct):
    df = _synthetic_df(
        [100.0, 110.0, 111.0, 112.0, 113.0],
        opens=[100.0, 110.0, 111.0, 98.0, 113.0],
        highs=[100.0, 111.0, 112.0, 113.0, 114.0],
        lows=[100.0, 109.0, 90.0, 98.0, 112.0],
    )
    params = _synthetic_params(useEmergencySL=use_emergency, emergencySlPct=pct)
    fast_result, validated = _validate_fast_grid_candidate(df, 0, params, mode="cc_only")

    assert validated.validation_status == "passed"
    assert validated.total_trades == fast_result.total_trades
    assert validated.winning_trades == fast_result.winning_trades
