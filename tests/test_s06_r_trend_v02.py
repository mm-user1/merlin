from __future__ import annotations

import math
import json
import sys
from dataclasses import fields
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core import metrics
from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.grid_engine import supports_fast_grid
from strategies import get_strategy, get_strategy_config, list_strategies
from strategies.s06_r_trend_v02 import strategy as s06
from strategies.s06_r_trend_v02.strategy import S06Params, S06RTrendV02


PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
TRADING_START = pd.Timestamp("2025-08-01 00:00:00", tz="UTC")
TRADING_END = pd.Timestamp("2025-12-01 00:00:00", tz="UTC")
WARMUP_BARS = 1000


def _base_params(**overrides) -> dict:
    params = {
        "dateFilter": False,
        "entryMode": "Reversal @ Triangle",
        "enableLong": True,
        "enableShort": True,
        "fastLength": 21,
        "fastSmoothing": 7,
        "slowLength": 112,
        "slowSmoothing": 3,
        "thresholdOS": 20,
        "thresholdOB": 20,
        "stopX": 1.0,
        "stopRR": 2.0,
        "stopLP": 2,
        "stopMaxPct": 100.0,
        "stopMaxDays": 100,
        "riskPerTrade": 2.0,
        "contractSize": 1.0,
        "useTrailMA": False,
        "trailRR": 1.0,
        "trailMAType": "SMA",
        "trailMALength": 50,
        "trailMAOffsetEx": 0.0,
        "initialCapital": 100.0,
        "commissionPct": 0.05,
    }
    params.update(overrides)
    return params


def _frame(rows: list[tuple[float, float, float, float]], *, freq: str = "30min") -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=len(rows), freq=freq, tz="UTC")
    return pd.DataFrame(
        {
            "Open": [row[0] for row in rows],
            "High": [row[1] for row in rows],
            "Low": [row[2] for row in rows],
            "Close": [row[3] for row in rows],
            "Volume": np.ones(len(rows)),
        },
        index=index,
    )


def _arrays(
    length: int,
    *,
    long_signals: tuple[int, ...] = (),
    short_signals: tuple[int, ...] = (),
    atr: float = 1.0,
    lowest: float = 99.0,
    highest: float = 101.0,
    trail_long: float | list[float] = 98.0,
    trail_short: float | list[float] = 102.0,
) -> s06._StrategyArrays:
    long_signal = np.zeros(length, dtype=bool)
    short_signal = np.zeros(length, dtype=bool)
    long_signal[list(long_signals)] = True
    short_signal[list(short_signals)] = True

    def values(raw):
        if isinstance(raw, list):
            return np.asarray(raw, dtype=float)
        return np.full(length, raw, dtype=float)

    return s06._StrategyArrays(
        atr=np.full(length, atr, dtype=float),
        lowest=np.full(length, lowest, dtype=float),
        highest=np.full(length, highest, dtype=float),
        trail_long=values(trail_long),
        trail_short=values(trail_short),
        long_signal=long_signal,
        short_signal=short_signal,
    )


def _run_patched(
    monkeypatch,
    df: pd.DataFrame,
    arrays: s06._StrategyArrays,
    params: dict | None = None,
    *,
    trade_start_idx: int = 0,
):
    monkeypatch.setattr(s06, "_build_strategy_arrays", lambda _df, _params: arrays)
    return S06RTrendV02.run(df, params or _base_params(), trade_start_idx)


@pytest.fixture(scope="module")
def reference_data():
    if not DATA_PATH.exists():
        pytest.skip(f"Reference data not found: {DATA_PATH}")
    df = load_data(str(DATA_PATH))
    return prepare_dataset_with_warmup(
        df,
        TRADING_START,
        TRADING_END,
        WARMUP_BARS,
    )


def test_config_identity_discovery_and_grid_backend():
    config = get_strategy_config("s06_r_trend_v02")
    strategy_ids = {item["id"] for item in list_strategies()}

    assert "s06_r_trend_v02" in strategy_ids
    assert get_strategy("s06_r_trend_v02") is S06RTrendV02
    assert config["id"] == S06RTrendV02.STRATEGY_ID
    assert config["name"] == S06RTrendV02.STRATEGY_NAME
    assert config["version"] == S06RTrendV02.STRATEGY_VERSION
    assert config["parameters"]["entryMode"]["options"] == [
        "Reversal @ Triangle",
        "Trend @ Square",
    ]
    assert config["parameters"]["trailMAType"]["options"] == ["SMA", "HMA", "KAMA", "T3"]
    assert config["parameters"]["enableLong"]["default"] is True
    assert config["parameters"]["enableShort"]["default"] is True
    assert "features" not in config
    assert supports_fast_grid("s06_r_trend_v02") is True
    assert config["parameters"]["thresholdOS"]["optimize"]["default_enabled"] is False
    assert config["parameters"]["thresholdOB"]["optimize"]["default_enabled"] is False


def test_config_params_match_dataclass_and_are_camel_case():
    config_params = set(get_strategy_config("s06_r_trend_v02")["parameters"])
    internal = {"use_date_filter", "start", "end"}
    dataclass_params = {field.name for field in fields(S06Params) if field.name not in internal}

    assert config_params == dataclass_params
    assert all("_" not in name for name in dataclass_params)
    assert not hasattr(S06Params, "to_dict")


def test_params_parse_dates_bools_and_reject_invalid_enums():
    params = S06Params.from_dict(
        {
            "dateFilter": "true",
            "enableLong": "0",
            "enableShort": 1,
            "useTrailMA": "off",
            "start": "2025-01-01",
            "end": pd.Timestamp("2025-01-02", tz="Asia/Irkutsk"),
        }
    )
    assert params.use_date_filter is True
    assert params.enableLong is False
    assert params.enableShort is True
    assert params.useTrailMA is False
    assert params.start == pd.Timestamp("2025-01-01", tz="UTC")
    assert params.end == pd.Timestamp("2025-01-01 16:00:00", tz="UTC")

    with pytest.raises(ValueError, match="entryMode"):
        S06Params.from_dict({"entryMode": "invalid"})
    with pytest.raises(ValueError, match="trailMAType"):
        S06Params.from_dict({"trailMAType": "EMA"})
    with pytest.raises(ValueError, match="contractSize"):
        S06Params.from_dict({"contractSize": 0})


def test_strategy_config_api_returns_s06_grid_profile():
    from ui.server import app

    with app.test_client() as client:
        response = client.get("/api/strategy/s06_r_trend_v02/config")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["id"] == "s06_r_trend_v02"
    assert payload["parameter_order"][0] == "entryMode"
    assert payload["grid_optimizer"]["available"] is True
    assert payload["grid_optimizer"]["profile"] == "full_enumeration"
    assert [mode["id"] for mode in payload["grid_optimizer"]["modes"]] == [
        "bracket",
        "trail",
    ]


def test_williams_r_known_values_bounds_and_zero_range():
    high = pd.Series([10.0, 12.0, 14.0, 13.0])
    low = pd.Series([6.0, 7.0, 8.0, 9.0])
    close = pd.Series([8.0, 10.0, 11.0, 12.0])
    values = s06._williams_r(high, low, close, 3)

    assert np.isnan(values[:2]).all()
    assert values[2] == pytest.approx(-37.5)
    assert values[3] == pytest.approx(-28.57142857142857)
    assert np.nanmin(values) >= -100.0
    assert np.nanmax(values) <= 0.0

    flat = pd.Series([5.0, 5.0, 5.0])
    assert np.isnan(s06._williams_r(flat, flat, flat, 2)).all()


def test_pine_ema_preserves_nan_warmup_and_holds_zero_range_gap():
    values = np.array([np.nan, np.nan, -80.0, -60.0, np.nan, -40.0])
    result = s06._pine_ema(values, 3)

    assert np.isnan(result[:2]).all()
    assert result[2:] == pytest.approx([-80.0, -70.0, -70.0, -55.0])


def test_trail_mas_and_offset_bands_are_deterministic():
    series = pd.Series(np.arange(1.0, 81.0))

    sma = s06._trail_ma(series, "SMA", 5)
    hma = s06._trail_ma(series, "HMA", 9)
    kama = s06._trail_ma(series, "KAMA", 10)
    t3 = s06._trail_ma(series, "T3", 5)

    assert sma.iloc[-1] == pytest.approx(78.0)
    assert hma.iloc[-1] == pytest.approx(80.0)
    assert kama.iloc[-1] == pytest.approx(78.75)
    assert t3.iloc[-1] == pytest.approx(78.2)

    offset_pct = 1.0 + 1.5
    assert 100.0 * (1.0 - offset_pct / 100.0) == pytest.approx(97.5)
    assert 100.0 * (1.0 + offset_pct / 100.0) == pytest.approx(102.5)


def test_signal_confluence_and_all_four_event_mappings():
    fast = np.array([np.nan, -50.0, -10.0, -30.0, -90.0, -70.0])
    slow = np.array([np.nan, -50.0, -15.0, -25.0, -85.0, -75.0])
    reversal = s06._signal_events(fast, slow, 20, 20, "Reversal @ Triangle")
    trend = s06._signal_events(fast, slow, 20, 20, "Trend @ Square")

    assert reversal.overbought.tolist() == [False, False, True, False, False, False]
    assert reversal.oversold.tolist() == [False, False, False, False, True, False]
    assert reversal.ob_trend_start[2]
    assert reversal.ob_reversal[3]
    assert reversal.os_trend_start[4]
    assert reversal.os_reversal[5]
    assert reversal.long_signal[5] and reversal.short_signal[3]
    assert trend.long_signal[2] and trend.short_signal[4]


def test_pending_entry_fills_next_open_and_keeps_signal_anchors(monkeypatch):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 104.5, 100.0, 103.0),
            (103.0, 103.0, 103.0, 103.0),
        ]
    )
    result = _run_patched(monkeypatch, df, _arrays(len(df), long_signals=(0,)))
    trade = result.trades[0]

    assert trade.entry_time == df.index[1]
    assert trade.entry_price == pytest.approx(101.0)
    assert trade.exit_price == pytest.approx(104.0)
    assert trade.size == pytest.approx(1.0)


def test_stop_distance_filter_and_contract_floor(monkeypatch):
    df = _frame([(100.0, 101.0, 99.0, 100.0)] * 3)
    rejected = _run_patched(
        monkeypatch,
        df,
        _arrays(len(df), long_signals=(0,), atr=10.0, lowest=90.0),
        _base_params(stopMaxPct=5.0),
    )
    assert rejected.trades == []

    floored = _run_patched(
        monkeypatch,
        df,
        _arrays(len(df), long_signals=(0,), atr=1.0, lowest=99.0),
        _base_params(contractSize=0.6, riskPerTrade=2.0),
    )
    assert floored.trades[0].size == pytest.approx(0.6)


def test_commission_timing_balance_and_trade_net_pnl(monkeypatch):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 102.0, 100.0, 101.0),
            (101.0, 104.5, 100.5, 104.0),
            (104.0, 104.0, 104.0, 104.0),
        ]
    )
    result = _run_patched(monkeypatch, df, _arrays(len(df), long_signals=(0,)))
    trade = result.trades[0]

    entry_commission = 101.0 * 1.0 * 0.0005
    exit_commission = 104.0 * 1.0 * 0.0005
    assert result.balance_curve[1] == pytest.approx(100.0 - entry_commission)
    assert trade.net_pnl == pytest.approx(3.0 - entry_commission - exit_commission)
    assert result.balance_curve[2] == pytest.approx(100.0 + trade.net_pnl)


@pytest.mark.parametrize(
    ("direction", "rows", "expected_exit"),
    [
        (1, [(100, 101, 99, 100), (101, 102, 97, 99), (99, 99, 99, 99)], 98.0),
        (1, [(100, 101, 99, 100), (101, 105, 100, 104), (104, 104, 104, 104)], 104.0),
        (-1, [(100, 101, 99, 100), (99, 103, 98, 101), (101, 101, 101, 101)], 102.0),
        (-1, [(100, 101, 99, 100), (99, 100, 95, 96), (96, 96, 96, 96)], 96.0),
    ],
)
def test_long_short_bracket_stop_and_target(monkeypatch, direction, rows, expected_exit):
    signals = {"long_signals": (0,)} if direction > 0 else {"short_signals": (0,)}
    result = _run_patched(monkeypatch, _frame(rows), _arrays(len(rows), **signals))
    assert result.trades[0].exit_price == pytest.approx(expected_exit)


def test_same_bar_stop_target_collision_follows_ohlc_path(monkeypatch):
    high_first = _frame([(100, 101, 99, 100), (101, 104, 96, 100), (100, 100, 100, 100)])
    low_first = _frame([(100, 101, 99, 100), (100, 104, 97, 100), (100, 100, 100, 100)])

    first = _run_patched(monkeypatch, high_first, _arrays(3, long_signals=(0,)))
    second = _run_patched(monkeypatch, low_first, _arrays(3, long_signals=(0,)))

    assert first.trades[0].exit_price == pytest.approx(104.0)
    assert second.trades[0].exit_price == pytest.approx(98.0)


@pytest.mark.parametrize(
    ("open_price", "expected_exit"),
    [(97.0, 97.0), (105.0, 105.0)],
)
def test_gap_through_active_stop_or_target_fills_at_open(monkeypatch, open_price, expected_exit):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (open_price, max(open_price, 105.0), min(open_price, 97.0), open_price),
            (open_price, open_price, open_price, open_price),
        ]
    )
    result = _run_patched(monkeypatch, df, _arrays(len(df), long_signals=(0,)))
    assert result.trades[0].exit_price == pytest.approx(expected_exit)


def test_long_fill_recalculation_activates_trail_from_confirmed_ohlc(monkeypatch):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 103.0, 100.0, 101.0),
            (101.0, 101.0, 101.0, 101.0),
        ]
    )
    result = _run_patched(
        monkeypatch,
        df,
        _arrays(3, long_signals=(0,), trail_long=[98.0, 101.5, 101.5]),
        _base_params(useTrailMA=True),
    )
    # calc_on_order_fills sees the confirmed fill-bar High, activates the
    # trail after the Open fill, and the marketable stop exits at that Open.
    assert result.trades[0].exit_price == pytest.approx(101.0)


def test_short_fill_recalculation_activates_trail_from_confirmed_ohlc(monkeypatch):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (99.0, 100.0, 97.0, 99.5),
            (99.5, 99.5, 99.5, 99.5),
        ]
    )
    result = _run_patched(
        monkeypatch,
        df,
        _arrays(3, short_signals=(0,), trail_short=[102.0, 98.5, 98.5]),
        _base_params(useTrailMA=True),
    )
    assert result.trades[0].exit_price == pytest.approx(99.0)


@pytest.mark.parametrize(
    ("direction", "rows", "trail_long", "trail_short", "expected_exit"),
    [
        (
            1,
            [
                (100.0, 101.0, 99.0, 100.0),
                (101.0, 101.5, 100.5, 101.0),
                (101.0, 103.0, 100.5, 101.5),
                (101.0, 102.0, 100.0, 101.0),
            ],
            [98.0, 99.0, 101.5, 101.5],
            102.0,
            101.0,
        ),
        (
            -1,
            [
                (100.0, 101.0, 99.0, 100.0),
                (99.0, 99.5, 98.5, 99.0),
                (99.0, 99.5, 97.0, 98.5),
                (99.0, 100.0, 98.0, 99.0),
            ],
            98.0,
            [102.0, 101.0, 98.5, 98.5],
            99.0,
        ),
    ],
)
def test_later_bar_trail_activation_exits_on_next_open_gap(
    monkeypatch,
    direction,
    rows,
    trail_long,
    trail_short,
    expected_exit,
):
    signals = {"long_signals": (0,)} if direction > 0 else {"short_signals": (0,)}
    result = _run_patched(
        monkeypatch,
        _frame(rows),
        _arrays(
            len(rows),
            trail_long=trail_long,
            trail_short=trail_short,
            **signals,
        ),
        _base_params(useTrailMA=True),
    )
    trade = result.trades[0]
    assert trade.exit_time == _frame(rows).index[3]
    assert trade.exit_price == pytest.approx(expected_exit)


@pytest.mark.parametrize(
    ("direction", "rows", "trail_long", "trail_short", "expected_exit"),
    [
        (
            1,
            [
                (100.0, 101.0, 99.0, 100.0),
                (101.0, 101.5, 100.5, 101.0),
                (101.0, 103.0, 100.5, 102.5),
                (103.0, 104.0, 101.5, 102.5),
                (101.0, 102.0, 100.0, 101.0),
            ],
            [90.0, 90.0, 100.0, 102.0, 102.0],
            110.0,
            101.0,
        ),
        (
            -1,
            [
                (100.0, 101.0, 99.0, 100.0),
                (99.0, 99.5, 98.5, 99.0),
                (99.0, 99.5, 97.0, 97.5),
                (97.0, 98.5, 96.0, 97.5),
                (99.0, 100.0, 98.0, 99.0),
            ],
            90.0,
            [110.0, 110.0, 100.0, 98.0, 98.0],
            99.0,
        ),
    ],
)
def test_active_trail_uses_previous_committed_stop_before_current_ratchet(
    monkeypatch,
    direction,
    rows,
    trail_long,
    trail_short,
    expected_exit,
):
    signals = {"long_signals": (0,)} if direction > 0 else {"short_signals": (0,)}
    result = _run_patched(
        monkeypatch,
        _frame(rows),
        _arrays(
            len(rows),
            trail_long=trail_long,
            trail_short=trail_short,
            **signals,
        ),
        _base_params(useTrailMA=True),
    )
    trade = result.trades[0]
    assert trade.exit_time == _frame(rows).index[4]
    assert trade.exit_price == pytest.approx(expected_exit)


def test_previously_active_trail_stop_has_priority_over_current_ratchet(monkeypatch):
    rows = [
        (100.0, 101.0, 99.0, 100.0),
        (101.0, 101.5, 100.5, 101.0),
        (101.0, 103.0, 100.5, 102.5),
        (103.0, 104.0, 99.0, 103.0),
        (103.0, 103.0, 103.0, 103.0),
    ]
    result = _run_patched(
        monkeypatch,
        _frame(rows),
        _arrays(
            len(rows),
            long_signals=(0,),
            trail_long=[90.0, 90.0, 100.0, 102.0, 102.0],
        ),
        _base_params(useTrailMA=True),
    )
    trade = result.trades[0]
    assert trade.exit_time == _frame(rows).index[3]
    assert trade.exit_price == pytest.approx(100.0)


def test_max_days_closes_at_next_open_with_priority(monkeypatch):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 102.0, 100.0, 101.0),
            (101.0, 102.0, 100.0, 101.0),
            (101.0, 102.0, 100.0, 101.0),
            (103.0, 105.0, 102.0, 104.0),
            (104.0, 104.0, 104.0, 104.0),
        ],
        freq="D",
    )
    result = _run_patched(
        monkeypatch,
        df,
        _arrays(len(df), long_signals=(0,), atr=5.0, lowest=95.0),
        _base_params(stopRR=10.0, stopMaxDays=2, contractSize=0.1),
    )
    trade = result.trades[0]
    assert trade.exit_time == df.index[4]
    assert trade.exit_price == pytest.approx(103.0)


def test_max_days_at_final_boundary_uses_final_close(monkeypatch):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 102.0, 100.0, 101.0),
            (101.0, 102.0, 100.0, 101.0),
            (101.0, 103.0, 100.0, 102.0),
        ],
        freq="D",
    )
    result = _run_patched(
        monkeypatch,
        df,
        _arrays(len(df), long_signals=(0,), atr=5.0, lowest=95.0),
        _base_params(stopRR=10.0, stopMaxDays=2, contractSize=0.1),
    )
    trade = result.trades[0]
    assert trade.exit_time == df.index[-1]
    assert trade.exit_price == pytest.approx(102.0)


def test_final_boundary_cancels_unfillable_entry_and_forces_open_position_close(monkeypatch):
    no_fill_df = _frame([(100.0, 101.0, 99.0, 100.0)] * 3)
    no_fill = _run_patched(
        monkeypatch,
        no_fill_df,
        _arrays(3, long_signals=(2,)),
    )
    assert no_fill.trades == []

    force_df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 102.0, 100.0, 101.0),
            (101.0, 103.0, 100.0, 102.0),
        ]
    )
    forced = _run_patched(
        monkeypatch,
        force_df,
        _arrays(3, long_signals=(0,), atr=5.0, lowest=95.0),
        _base_params(stopRR=10.0, contractSize=0.1),
    )
    assert forced.trades[0].exit_time == force_df.index[-1]
    assert forced.trades[0].exit_price == pytest.approx(102.0)
    assert forced.last_position == {}


def test_enable_gating_same_side_reentry_trade_start_and_call_isolation(monkeypatch):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 105.0, 100.0, 104.0),
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 105.0, 100.0, 104.0),
            (104.0, 104.0, 104.0, 104.0),
        ]
    )
    arrays = _arrays(len(df), long_signals=(0, 2))
    disabled = _run_patched(monkeypatch, df, arrays, _base_params(enableLong=False))
    before_start = _run_patched(monkeypatch, df, arrays, trade_start_idx=3)
    first = _run_patched(monkeypatch, df, arrays, _base_params(contractSize=0.1))
    second = _run_patched(monkeypatch, df, arrays, _base_params(contractSize=0.1))

    assert disabled.trades == []
    assert before_start.trades == []
    assert len(first.trades) == 2
    assert first.trades == second.trades
    assert first.balance_curve == pytest.approx(second.balance_curve)


def test_fill_and_exit_bar_does_not_enqueue_stale_signal_reentry(monkeypatch):
    df = _frame(
        [
            (100.0, 101.0, 99.0, 100.0),
            (101.0, 105.0, 100.0, 104.0),
            (101.0, 105.0, 100.0, 104.0),
            (104.0, 104.0, 104.0, 104.0),
        ]
    )
    result = _run_patched(
        monkeypatch,
        df,
        _arrays(len(df), long_signals=(0, 1)),
        _base_params(contractSize=0.1),
    )

    assert len(result.trades) == 1
    assert result.trades[0].entry_time == df.index[1]
    assert result.last_position == {}


def test_mode_inactive_parameters_do_not_change_results(reference_data):
    df, trade_start_idx = reference_data
    trail = _reference_params(useTrailMA=True)
    bracket = _reference_params(entryMode="Trend @ Square", useTrailMA=False, stopRR=2.0)

    trail_a = S06RTrendV02.run(df, trail, trade_start_idx)
    trail_b = S06RTrendV02.run(df, {**trail, "stopRR": 1.5}, trade_start_idx)
    bracket_a = S06RTrendV02.run(df, bracket, trade_start_idx)
    bracket_b = S06RTrendV02.run(
        df,
        {
            **bracket,
            "trailRR": 3.0,
            "trailMAType": "T3",
            "trailMALength": 50,
            "trailMAOffsetEx": 2.0,
        },
        trade_start_idx,
    )

    assert trail_a.trades == trail_b.trades
    assert trail_a.balance_curve == pytest.approx(trail_b.balance_curve)
    assert bracket_a.trades == bracket_b.trades
    assert bracket_a.balance_curve == pytest.approx(bracket_b.balance_curve)


def _reference_params(**overrides) -> dict:
    params = {
        "dateFilter": True,
        "start": TRADING_START,
        "end": TRADING_END,
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
    params.update(overrides)
    return params


def test_reference_b_trend_bracket_matches_tradingview(reference_data):
    df, trade_start_idx = reference_data
    result = S06RTrendV02.run(
        df,
        _reference_params(entryMode="Trend @ Square", useTrailMA=False, stopRR=2.0),
        trade_start_idx,
    )
    basic = metrics.calculate_basic(result, initial_balance=100.0)
    advanced = metrics.calculate_advanced(result, initial_balance=100.0)

    # TradingView: net 25.87%, DD 10.56%, 48 trades, 21 wins, PF 1.438.
    # Merlin DD is realized-balance DD by contract and is asserted separately.
    assert basic.total_trades == 48
    assert basic.winning_trades == 21
    assert basic.net_profit_pct == pytest.approx(25.8742842051, abs=1e-9)
    assert advanced.profit_factor == pytest.approx(1.4381064632, abs=1e-9)
    assert basic.max_drawdown_pct == pytest.approx(9.9211555042, abs=1e-9)


def test_reference_a_reversal_trail_encodes_strict_boundary_result(reference_data):
    df, trade_start_idx = reference_data
    result = S06RTrendV02.run(df, _reference_params(), trade_start_idx)
    basic = metrics.calculate_basic(result, initial_balance=100.0)
    advanced = metrics.calculate_advanced(result, initial_balance=100.0)

    # TradingView: net 31.92%, DD 14.15%, 61 trades, 31 wins, PF 1.525.
    # Merlin closes the final short at 2025-12-01 00:00Z instead of consuming
    # TradingView's delayed 2025-12-01 01:00Z fill outside the requested period.
    # That strict boundary costs 1.0534874805 percentage points versus the
    # delayed Open and explains most of the remaining aggregate difference.
    assert basic.total_trades == 61
    assert basic.winning_trades == 31
    assert basic.net_profit_pct == pytest.approx(30.9420054193, abs=1e-9)
    assert advanced.profit_factor == pytest.approx(1.5088788696, abs=1e-9)
    assert basic.max_drawdown_pct == pytest.approx(13.4683032109, abs=1e-9)
    assert result.trades[-1].exit_time == TRADING_END

    last_trade = result.trades[-1]
    delayed_tv_open = 1.3853
    commission_rate = 0.05 / 100.0
    delayed_net_pnl = (
        (last_trade.entry_price - delayed_tv_open) * last_trade.size
        - last_trade.entry_price * last_trade.size * commission_rate
        - delayed_tv_open * last_trade.size * commission_rate
    )
    boundary_delta = delayed_net_pnl - last_trade.net_pnl
    assert boundary_delta == pytest.approx(1.0534874805, abs=1e-9)
    assert basic.net_profit_pct + boundary_delta == pytest.approx(31.9954928998, abs=1e-9)


def test_result_serialization_preserves_timestamp_objects(reference_data):
    df, trade_start_idx = reference_data
    result = S06RTrendV02.run(
        df,
        _reference_params(entryMode="Trend @ Square", useTrailMA=False, stopRR=2.0),
        trade_start_idx,
    )

    assert all(isinstance(timestamp, pd.Timestamp) for timestamp in result.timestamps)
    assert all(isinstance(trade.entry_time, pd.Timestamp) for trade in result.trades)
    assert all(isinstance(trade.exit_time, pd.Timestamp) for trade in result.trades)
    serialized = result.to_dict()
    assert len(serialized["timestamps"]) == len(result.timestamps)
    assert math.isfinite(serialized["net_profit_pct"])


def test_backtest_and_trade_export_endpoints_support_s06(reference_data):
    del reference_data
    from ui.server import app

    payload = _reference_params(entryMode="Trend @ Square", useTrailMA=False, stopRR=2.0)
    payload["start"] = TRADING_START.isoformat()
    payload["end"] = TRADING_END.isoformat()
    form = {
        "strategy": "s06_r_trend_v02",
        "csvPath": str(DATA_PATH.resolve()),
        "warmupBars": str(WARMUP_BARS),
        "payload": json.dumps(payload),
    }

    with app.test_client() as client:
        response = client.post("/api/backtest", data=form)
        trades_response = client.post("/api/backtest/trades", data=form)

    assert response.status_code == 200
    assert response.get_json()["metrics"]["total_trades"] == 48
    assert trades_response.status_code == 200
    assert "Symbol,Side,Qty,Fill Price,Closing Time" in trades_response.get_data(as_text=True)
