"""TradingView baseline parity for S03 Reversal v11 Regime-ER B2.

The committed TradingView references exercise Pine date-expiry close_all after
the configured end. Production B2 adapters truncate at `end`, so direct Merlin
V2 closes the final open trade at the strict boundary instead of the later
TradingView fill. These tests enforce strict trade parity before that accepted
residual and pin Merlin-convention aggregate values separately.
"""

import pytest

from core.engine_v2.kernel_signal import EMERGENCY_SL_EXIT_REASON

from s03_regime_er_test_helpers import (
    BASELINE_END,
    REFERENCE_A,
    REFERENCE_B,
    iso_timestamp,
    load_reference,
    load_summary,
    run_public_reference,
    run_reference,
    run_reference_without_adapter_truncation,
)


ENTRY_PRICE_TOLERANCE = 1e-9
EXIT_PRICE_TOLERANCE = 0.0001 + 1e-9


def _first_mismatch(rows, trades):
    if len(rows) != len(trades):
        return f"count expected={len(rows)} actual={len(trades)}"
    for index, (row, trade) in enumerate(zip(rows, trades), start=1):
        tv_exit_signal = row["exit_signal"]
        expected_reason = tv_exit_signal
        actual_reason = trade.exit_reason
        if expected_reason != EMERGENCY_SL_EXIT_REASON:
            expected_reason = None
        exit_price_tolerance = (
            EXIT_PRICE_TOLERANCE if tv_exit_signal == EMERGENCY_SL_EXIT_REASON else ENTRY_PRICE_TOLERANCE
        )
        checks = [
            ("direction", row["direction"], trade.direction, None),
            ("entry_time", row["entry_time_utc"], iso_timestamp(trade.entry_time), None),
            ("exit_time", row["exit_time_utc"], iso_timestamp(trade.exit_time), None),
            ("entry_price", float(row["entry_price_usdt"]), trade.entry_price, ENTRY_PRICE_TOLERANCE),
            ("exit_price", float(row["exit_price_usdt"]), trade.exit_price, exit_price_tolerance),
            ("exit_reason", expected_reason, actual_reason, None),
        ]
        for field, expected, actual, tolerance in checks:
            if tolerance is None:
                if expected != actual:
                    return f"trade {index} {field}: expected={expected} actual={actual}"
            elif abs(expected - actual) > tolerance:
                return f"trade {index} {field}: expected={expected} actual={actual}"
    return None


@pytest.mark.parametrize(
    ("reference_id", "expected_total", "expected_wins", "expected_emergency"),
    [
        (REFERENCE_A, 151, 63, 0),
        (REFERENCE_B, 152, 63, 1),
    ],
)
def test_reference_trade_parity_before_final_date_expiry_residual(
    reference_id,
    expected_total,
    expected_wins,
    expected_emergency,
):
    _, _, rows = load_reference(reference_id)
    run = run_reference(reference_id)
    result = run.strategy_result
    summary = load_summary(reference_id)

    mismatch = _first_mismatch(rows[:-1], result.trades[:-1])
    assert mismatch is None, mismatch
    assert result.total_trades == expected_total
    assert result.winning_trades == expected_wins
    assert sum(1 for trade in result.trades if trade.exit_reason == EMERGENCY_SL_EXIT_REASON) == expected_emergency
    assert summary["metrics"]["total_trades"] == expected_total
    assert summary["metrics"]["profitable_trades"] == expected_wins
    assert summary["metrics"]["emergency_sl_exits"] == expected_emergency


@pytest.mark.parametrize("reference_id", [REFERENCE_A, REFERENCE_B])
def test_final_trade_is_only_documented_date_expiry_residual(reference_id):
    _, _, rows = load_reference(reference_id)
    result = run_reference(reference_id).strategy_result
    row = rows[-1]
    trade = result.trades[-1]

    assert trade.direction == row["direction"]
    assert iso_timestamp(trade.entry_time) == row["entry_time_utc"]
    assert trade.entry_price == pytest.approx(float(row["entry_price_usdt"]), abs=ENTRY_PRICE_TOLERANCE)
    assert iso_timestamp(trade.exit_time) == BASELINE_END.isoformat().replace("+00:00", "Z")
    assert row["exit_time_utc"] == "2026-02-01T01:00:00Z"
    assert trade.exit_reason is None


@pytest.mark.parametrize(
    ("reference_id", "expected_net_profit_pct"),
    [
        (REFERENCE_A, 150.8911260100),
        (REFERENCE_B, 154.0231148748),
    ],
)
def test_untruncated_kernel_replay_matches_tradingview_date_expiry_close(
    reference_id,
    expected_net_profit_pct,
):
    """This proof does not change production adapter truncation behavior."""

    result = run_reference_without_adapter_truncation(reference_id).strategy_result
    trade = result.trades[-1]

    assert iso_timestamp(trade.exit_time) == "2026-02-01T01:00:00Z"
    assert trade.exit_price == pytest.approx(1.1608, abs=EXIT_PRICE_TOLERANCE)
    assert result.net_profit_pct == pytest.approx(expected_net_profit_pct)


def test_reference_a_merlin_metrics_are_pinned_with_boundary_residual():
    result = run_reference(REFERENCE_A).strategy_result

    assert result.total_trades == 151
    assert result.winning_trades == 63
    assert result.net_profit_pct == pytest.approx(152.0521772455)
    assert result.profit_factor == pytest.approx(1.8074539749)
    assert result.max_drawdown_pct == pytest.approx(18.1492940138)


def test_reference_b_merlin_metrics_are_pinned_with_boundary_residual():
    result = run_reference(REFERENCE_B).strategy_result

    assert result.total_trades == 152
    assert result.winning_trades == 63
    assert result.net_profit_pct == pytest.approx(155.1986873673)
    assert result.profit_factor == pytest.approx(1.8077375729)
    assert result.max_drawdown_pct == pytest.approx(19.2133837712)


def test_public_strategy_run_matches_v2_reference_helper():
    public = run_public_reference(REFERENCE_B)
    direct = run_reference(REFERENCE_B).strategy_result

    assert public.total_trades == direct.total_trades
    assert public.net_profit_pct == pytest.approx(direct.net_profit_pct)
    assert [(trade.direction, trade.entry_time, trade.exit_time) for trade in public.trades] == [
        (trade.direction, trade.entry_time, trade.exit_time) for trade in direct.trades
    ]
