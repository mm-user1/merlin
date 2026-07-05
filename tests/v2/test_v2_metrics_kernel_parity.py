import math

import pandas as pd
import pytest

from core import metrics
from core.backtest_engine import StrategyResult, TradeRecord
from core.engine_v2.metrics_kernel import compute_core_metrics_from_balance_and_trades

from s06_b2_test_helpers import run_reference


def _result(balance_curve, pnls):
    timestamps = list(pd.date_range("2025-01-01", periods=len(balance_curve), freq="30min", tz="UTC"))
    trades = [TradeRecord(net_pnl=float(pnl)) for pnl in pnls]
    balances = [float(value) for value in balance_curve]
    return StrategyResult(
        trades=trades,
        equity_curve=balances.copy(),
        balance_curve=balances.copy(),
        timestamps=timestamps,
    )


def _assert_optional_metric(actual, expected):
    if expected is None:
        assert math.isnan(actual)
    elif math.isinf(expected):
        assert math.isinf(actual)
    else:
        assert actual == pytest.approx(expected)


def _assert_matches_core(balance_curve, pnls, *, initial_balance=None):
    result = _result(balance_curve, pnls)
    basic = metrics.calculate_basic(result, initial_balance=initial_balance)
    advanced = metrics.calculate_advanced(result, initial_balance=initial_balance)
    v2 = compute_core_metrics_from_balance_and_trades(
        balance_curve,
        result.trades,
        initial_balance=initial_balance,
    )

    expected_start = (
        float(initial_balance)
        if initial_balance is not None
        else float(balance_curve[0])
        if balance_curve
        else 0.0
    )
    expected_final = float(balance_curve[-1]) if balance_curve else expected_start

    assert v2.start_balance == pytest.approx(expected_start)
    assert v2.final_balance == pytest.approx(expected_final)
    assert v2.net_profit == pytest.approx(basic.net_profit)
    assert v2.net_profit_pct == pytest.approx(basic.net_profit_pct)
    assert v2.total_trades == basic.total_trades
    assert v2.winning_trades == basic.winning_trades
    assert v2.losing_trades == basic.losing_trades
    assert v2.win_rate_pct == pytest.approx(basic.win_rate)
    assert v2.gross_profit == pytest.approx(basic.gross_profit)
    assert v2.gross_loss == pytest.approx(basic.gross_loss)
    assert v2.max_drawdown_pct == pytest.approx(basic.max_drawdown_pct)
    assert v2.max_drawdown == pytest.approx(basic.max_drawdown)
    _assert_optional_metric(v2.profit_factor, advanced.profit_factor)
    _assert_optional_metric(v2.romad, advanced.romad)


@pytest.mark.parametrize(
    ("balance_curve", "pnls", "initial_balance"),
    [
        ([100.0, 112.0, 105.0, 121.0], [12.0, -7.0, 16.0], 100.0),
        ([100.0, 100.0], [], 100.0),
        ([100.0, 100.0, 100.0], [], None),
        ([100.0, 105.0, 110.0], [10.0], 100.0),
        ([100.0, 120.0, 90.0, 130.0], [30.0], 100.0),
        ([100.0, 100.0, 100.0], [0.0, 0.0], 100.0),
        ([100.0, 105.0], [5.0, 0.0], 100.0),
        ([100.0, 100.0], [0.0, 0.0, 0.0], 100.0),
    ],
)
def test_core_metrics_match_merlin_metric_semantics(balance_curve, pnls, initial_balance):
    _assert_matches_core(balance_curve, pnls, initial_balance=initial_balance)


def test_empty_balance_curve_uses_explicit_initial_balance_and_nan_profit_factor():
    v2 = compute_core_metrics_from_balance_and_trades([], [], initial_balance=100.0)

    assert v2.start_balance == 100.0
    assert v2.final_balance == 100.0
    assert v2.net_profit == 0.0
    assert math.isnan(v2.profit_factor)


def _assert_run_metrics_match_v2_helper(run):
    result = run.strategy_result
    v2 = compute_core_metrics_from_balance_and_trades(
        result.balance_curve,
        result.trades,
        initial_balance=100.0,
    )

    assert v2.net_profit == pytest.approx(result.net_profit)
    assert v2.net_profit_pct == pytest.approx(result.net_profit_pct)
    assert v2.total_trades == result.total_trades
    assert v2.winning_trades == result.winning_trades
    assert v2.losing_trades == result.losing_trades
    assert v2.profit_factor == pytest.approx(result.profit_factor)
    assert v2.max_drawdown_pct == pytest.approx(result.max_drawdown_pct)
    assert v2.max_drawdown == pytest.approx(result.max_drawdown)
    assert v2.romad == pytest.approx(result.romad)


@pytest.mark.parametrize(
    ("reference_id", "price_rounding"),
    [
        ("reference_b_trend_bracket", "none"),
        ("reference_a_reversal_trail", "none"),
        ("reference_b_trend_bracket", "tick_outward"),
        ("reference_a_reversal_trail", "tick_outward"),
    ],
)
def test_s06_reference_runs_match_v2_metric_helper(reference_id, price_rounding):
    _assert_run_metrics_match_v2_helper(run_reference(reference_id, price_rounding=price_rounding))
