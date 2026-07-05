import math

import numpy as np
import pandas as pd
import pytest

from core.engine_v2.contracts import Signals
from core.engine_v2.kernel import ExecutionData, KernelConfig, run_reference_kernel
from core.engine_v2.price_rounding import (
    PRICE_ROUNDING_TICK_OUTWARD,
    round_level_outward,
    round_to_tick_ceil,
    round_to_tick_floor,
)
from core.engine_v2.profile import active_parameter_names, inactive_parameter_names, parse_execution_profile
from core.engine_v2.runner import build_kernel_config
from strategies.s06_r_trend_v02_b2.strategy import load_config

from s06_b2_test_helpers import iso_timestamp, load_reference, run_reference


def _data(
    *,
    open_,
    high,
    low,
    close,
    long=None,
    short=None,
    atr=None,
    rolling_low=None,
    rolling_high=None,
    trail_long=None,
    trail_short=None,
):
    length = len(open_)
    return ExecutionData(
        timestamps=tuple(pd.date_range("2025-01-01", periods=length, freq="30min", tz="UTC")),
        open=np.array(open_, dtype=float),
        high=np.array(high, dtype=float),
        low=np.array(low, dtype=float),
        close=np.array(close, dtype=float),
        signals=Signals(
            long_entries=np.array(long if long is not None else [False] * length, dtype=bool),
            short_entries=np.array(short if short is not None else [False] * length, dtype=bool),
        ),
        atr=np.array(atr if atr is not None else [0.0] * length, dtype=float),
        rolling_low=np.array(rolling_low if rolling_low is not None else low, dtype=float),
        rolling_high=np.array(rolling_high if rolling_high is not None else high, dtype=float),
        trail_long=np.array(trail_long if trail_long is not None else [np.nan] * length, dtype=float),
        trail_short=np.array(trail_short if trail_short is not None else [np.nan] * length, dtype=float),
    )


def _profile(price_rounding="none"):
    config = {
        "id": "rounding_fixture",
        "engine": "v2",
        "execution": {
            "entryOrder": "market_next_open",
            "stop": "atr_swing",
            "sizing": "risk_per_trade",
            "maxDays": True,
            "margin": "off",
            "boundary": "strict_close",
            "target": "rr",
            "trail": "none",
            "priceRounding": price_rounding,
        },
        "parameters": {
            "stopX": {"type": "float", "default": 0.0, "role": "execution", "optimize": {"enabled": False}},
            "stopLP": {"type": "int", "default": 2, "role": "execution", "optimize": {"enabled": False}},
            "stopMaxPct": {"type": "float", "default": 10.0, "role": "execution", "optimize": {"enabled": False}},
            "stopRR": {"type": "float", "default": 1.0, "role": "execution", "optimize": {"enabled": False}},
            "riskPerTrade": {"type": "float", "default": 100.0, "role": "execution", "optimize": {"enabled": False}},
            "contractSize": {"type": "float", "default": 0.01, "role": "execution", "optimize": {"enabled": False}},
            "stopMaxDays": {"type": "int", "default": 6, "role": "execution", "optimize": {"enabled": False}},
            "tickSize": {"type": "float", "default": 0.01, "role": "execution", "optimize": {"enabled": False}},
        },
    }
    return parse_execution_profile(config)


def test_floor_and_ceil_use_epsilon_without_shifting_grid_prices():
    assert round_to_tick_floor(1.2345, 0.0001) == pytest.approx(1.2345)
    assert round_to_tick_ceil(1.2345, 0.0001) == pytest.approx(1.2345)
    assert round_to_tick_floor(1.23449999999999, 0.0001) == pytest.approx(1.2345)
    assert round_to_tick_ceil(1.23450000000001, 0.0001) == pytest.approx(1.2345)
    assert round_to_tick_floor(1.23449, 0.0001) == pytest.approx(1.2344)
    assert round_to_tick_ceil(1.23451, 0.0001) == pytest.approx(1.2346)


def test_large_magnitude_on_grid_tick_values_do_not_shift():
    price = 43210.5001
    tick_size = 0.0001

    assert round_to_tick_floor(price, tick_size) == pytest.approx(price)
    assert round_to_tick_ceil(price, tick_size) == pytest.approx(price)
    assert round_to_tick_floor(price - 0.00000001, tick_size) == pytest.approx(43210.5)
    assert round_to_tick_ceil(price + 0.00000001, tick_size) == pytest.approx(43210.5002)


def test_outward_level_rounding_floors_below_market_and_ceils_above_market():
    assert round_level_outward(100.019, 0.01, below_market=True) == pytest.approx(100.01)
    assert round_level_outward(100.011, 0.01, below_market=False) == pytest.approx(100.02)


def test_profile_binding_consumes_tick_size_only_when_rounding_is_active():
    config = load_config()
    default_profile = parse_execution_profile(config)

    assert config["execution"]["priceRounding"] == "none"
    assert "tickSize" in inactive_parameter_names(default_profile, default_profile.parameter_defaults)

    config["execution"]["priceRounding"] = "tick_outward"
    tick_profile = parse_execution_profile(config)
    assert "tickSize" in active_parameter_names(tick_profile, tick_profile.parameter_defaults)


def test_runner_validation_rejects_unknown_rounding_and_bad_active_tick_size():
    with pytest.raises(ValueError, match="Unsupported Phase-1 priceRounding mode"):
        build_kernel_config(profile=_profile("nearest"), params=_profile("nearest").parameter_defaults)

    tick_profile = _profile("tick_outward")
    bad_params = dict(tick_profile.parameter_defaults)
    bad_params["tickSize"] = 0.0
    with pytest.raises(ValueError, match="tickSize"):
        build_kernel_config(profile=tick_profile, params=bad_params)

    missing_params = dict(tick_profile.parameter_defaults)
    del missing_params["tickSize"]
    with pytest.raises(ValueError, match="tickSize is required"):
        build_kernel_config(profile=tick_profile, params=missing_params)


def test_inactive_tick_size_is_inert_in_no_rounding_mode():
    profile = _profile("none")
    params = dict(profile.parameter_defaults)
    params["tickSize"] = 0.0
    config = build_kernel_config(profile=profile, params=params)

    assert config.price_rounding_mode == "none"
    assert math.isnan(config.tick_size)


def test_direct_kernel_rejects_unknown_rounding_mode_before_bar_execution():
    empty = _data(open_=[], high=[], low=[], close=[])
    with pytest.raises(ValueError, match="Unsupported priceRounding mode"):
        run_reference_kernel(empty, KernelConfig(price_rounding_mode="bad"))

    signal_free = _data(
        open_=[100.0, 101.0],
        high=[101.0, 102.0],
        low=[99.0, 100.0],
        close=[100.5, 101.5],
    )
    with pytest.raises(ValueError, match="Unsupported priceRounding mode"):
        run_reference_kernel(signal_free, KernelConfig(price_rounding_mode="bad"))


def test_tick_rounding_changes_only_standing_stop_and_target_levels():
    data = _data(
        open_=[100.0, 100.0, 100.0],
        high=[100.0, 102.88, 103.0],
        low=[97.123456, 99.0, 100.0],
        close=[100.0, 102.0, 102.0],
        long=[True, False, False],
        rolling_low=[97.123456, 99.0, 100.0],
    )
    original_open = data.open.copy()
    original_high = data.high.copy()
    original_low = data.low.copy()
    original_close = data.close.copy()

    base_config = KernelConfig(
        initial_capital=100.0,
        risk_per_trade_pct=100.0,
        contract_size=0.01,
        stop_x=0.0,
        reward_risk=1.0,
        max_stop_pct=10.0,
    )
    rounded_config = KernelConfig(
        initial_capital=100.0,
        risk_per_trade_pct=100.0,
        contract_size=0.01,
        stop_x=0.0,
        reward_risk=1.0,
        max_stop_pct=10.0,
        price_rounding_mode=PRICE_ROUNDING_TICK_OUTWARD,
        tick_size=0.01,
    )

    base = run_reference_kernel(data, base_config)
    rounded = run_reference_kernel(data, rounded_config)

    assert base.trades[0].entry_price == rounded.trades[0].entry_price == 100.0
    assert base.trades[0].exit_price == pytest.approx(102.876544)
    assert rounded.trades[0].exit_price == pytest.approx(102.88)
    assert base.trades[0].size == rounded.trades[0].size
    np.testing.assert_array_equal(data.open, original_open)
    np.testing.assert_array_equal(data.high, original_high)
    np.testing.assert_array_equal(data.low, original_low)
    np.testing.assert_array_equal(data.close, original_close)


def test_tick_rounding_applies_to_trail_band_activation_and_ratchet():
    data = _data(
        open_=[100.0, 103.0],
        high=[100.0, 104.0],
        low=[97.0, 101.55],
        close=[100.0, 102.0],
        long=[True, False],
        rolling_low=[97.0, 101.55],
        trail_long=[np.nan, 101.56789],
    )

    base = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=0.01,
            stop_x=0.0,
            max_stop_pct=10.0,
            target_mode="none",
            trail_mode="ma",
            trail_activation_mode="rr",
            trail_activation_rr=1.0,
        ),
    )
    rounded = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=0.01,
            stop_x=0.0,
            max_stop_pct=10.0,
            target_mode="none",
            trail_mode="ma",
            trail_activation_mode="rr",
            trail_activation_rr=1.0,
            price_rounding_mode=PRICE_ROUNDING_TICK_OUTWARD,
            tick_size=0.01,
        ),
    )

    assert base.trades[0].exit_price == pytest.approx(101.56789)
    assert rounded.trades[0].exit_price == pytest.approx(101.56)


def _first_mismatch(rows, trades):
    if len(rows) != len(trades):
        return f"count expected={len(rows)} actual={len(trades)}"
    for index, (row, trade) in enumerate(zip(rows, trades), start=1):
        checks = [
            ("direction", row["direction"], trade.direction, None),
            ("entry_time", row["entry_time_utc"], iso_timestamp(trade.entry_time), None),
            ("exit_time", row["exit_time_utc"], iso_timestamp(trade.exit_time), None),
            ("entry_price", float(row["entry_price_usdt"]), trade.entry_price, 1e-9),
            ("exit_price", float(row["exit_price_usdt"]), trade.exit_price, 1e-9),
            ("size", float(row["size_qty"]), trade.size, 1e-9),
            ("net_pnl", float(row["net_pnl_usdt"]), trade.net_pnl, 0.02),
        ]
        for field, expected, actual, tolerance in checks:
            if tolerance is None:
                if expected != actual:
                    return f"trade {index} {field}: expected={expected} actual={actual}"
            elif abs(expected - actual) > tolerance:
                return f"trade {index} {field}: expected={expected} actual={actual}"
    return None


@pytest.mark.parametrize(
    (
        "reference_id",
        "expected_trades",
        "expected_wins",
        "expected_net",
        "expected_profit_factor",
        "expected_drawdown",
    ),
    [
        ("reference_b_trend_bracket", 48, 21, 25.8746180135, 1.4379099877, 9.9271828348),
        ("reference_a_reversal_trail", 61, 31, 30.8652320330, 1.5073481143, 13.4921966575),
    ],
)
def test_s06_references_match_tradingview_exports_with_tick_outward_rounding(
    reference_id,
    expected_trades,
    expected_wins,
    expected_net,
    expected_profit_factor,
    expected_drawdown,
):
    _, _, rows = load_reference(reference_id)
    run = run_reference(reference_id, price_rounding="tick_outward")
    result = run.strategy_result

    mismatch = _first_mismatch(rows, result.trades)
    assert mismatch is None, mismatch
    assert result.total_trades == expected_trades
    assert result.winning_trades == expected_wins
    assert round(result.winning_trades / result.total_trades * 100.0, 2) == (
        43.75 if reference_id.endswith("trend_bracket") else 50.82
    )
    assert result.net_profit == pytest.approx(expected_net)
    assert result.net_profit_pct == pytest.approx(expected_net)
    assert round(result.net_profit_pct, 2) == round(expected_net, 2)
    assert result.profit_factor == pytest.approx(expected_profit_factor)
    assert round(result.profit_factor, 3) == round(expected_profit_factor, 3)
    assert result.max_drawdown_pct == pytest.approx(expected_drawdown)
    assert run.guardrail_summary.liquidation_count == 0
    assert run.guardrail_summary.margin_reject_count == 0
    assert run.guardrail_summary.no_capital_halt is False
