"""TradingView baseline parity for the S06 Regime-TL pilot import.

Reference A (`useRegime=false`) is the no-regime control; Reference B
(`useRegime=true`) is the primary trendline-regime target. Both use identical
Trend @ Square + bracket inputs on the canonical SUI 30m window.

Assertion policy (strictest realistic, residuals documented):

- trade count, direction sequence, and UTC entry/exit timestamps: exact;
- entry prices: exact (next-open market fills are exported at full precision);
- exit prices: within one tick (0.0001) — TradingView rounds computed
  stop/target fill prices to 4 decimals in the CSV export;
- sizes: within one contract step (0.01) — TradingView export rounding of
  equity-based sizing;
- per-trade net PnL: within 0.01 USDT (follows from the size/price rounding);
- wins and win rate: exact against TradingView;
- net profit / profit factor / max drawdown: pinned Merlin-convention values.
  Known residuals vs the TradingView summary, documented in CERTIFICATION.md:
  - net profit: Merlin 11.3837 vs TV 11.35 (A), 22.3484 vs TV 22.33 (B) —
    TradingView's 2-decimal per-trade PnL export rounding accumulated over
    45/43 trades;
  - profit factor: Merlin 1.1966 vs TV-displayed 1.196 (A), 1.4057 vs 1.405
    (B) — `round(pf, 3)` does NOT reproduce the TV display for this baseline;
  - max drawdown: Merlin realized-balance convention 13.85/11.99 vs TV
    equity/open-excursion convention 15.02/13.19 (see CERTIFICATION.md notes).

This baseline does not exercise the final-boundary close: every position
closes before the end date in both references.
"""

import pytest

from s06_regime_tl_test_helpers import (
    BASELINE_END,
    REFERENCE_A,
    REFERENCE_B,
    iso_timestamp,
    load_reference,
    run_reference,
)


ENTRY_PRICE_TOLERANCE = 1e-9
EXIT_PRICE_TOLERANCE = 0.0001 + 1e-9
SIZE_TOLERANCE = 0.01 + 1e-7
NET_PNL_TOLERANCE = 0.01


def _first_mismatch(rows, trades):
    if len(rows) != len(trades):
        return f"count expected={len(rows)} actual={len(trades)}"
    for index, (row, trade) in enumerate(zip(rows, trades), start=1):
        checks = [
            ("direction", row["direction"], trade.direction, None),
            ("entry_time", row["entry_time_utc"], iso_timestamp(trade.entry_time), None),
            ("exit_time", row["exit_time_utc"], iso_timestamp(trade.exit_time), None),
            ("entry_price", float(row["entry_price_usdt"]), trade.entry_price, ENTRY_PRICE_TOLERANCE),
            ("exit_price", float(row["exit_price_usdt"]), trade.exit_price, EXIT_PRICE_TOLERANCE),
            ("size", float(row["size_qty"]), trade.size, SIZE_TOLERANCE),
            ("net_pnl", float(row["net_pnl_usdt"]), trade.net_pnl, NET_PNL_TOLERANCE),
        ]
        for field, expected, actual, tolerance in checks:
            if tolerance is None:
                if expected != actual:
                    return f"trade {index} {field}: expected={expected} actual={actual}"
            elif abs(expected - actual) > tolerance:
                return f"trade {index} {field}: expected={expected} actual={actual}"
    return None


def test_reference_a_no_regime_control_matches_baseline_export():
    _, _, rows = load_reference(REFERENCE_A)
    run = run_reference(REFERENCE_A)
    result = run.strategy_result

    mismatch = _first_mismatch(rows, result.trades)
    assert mismatch is None, mismatch
    assert result.total_trades == 45
    assert result.winning_trades == 16
    assert round(result.winning_trades / result.total_trades * 100.0, 2) == 35.56
    assert result.net_profit_pct == pytest.approx(11.3836924235)
    assert result.profit_factor == pytest.approx(1.1966147730)
    assert result.max_drawdown_pct == pytest.approx(13.8496161888)
    assert run.guardrail_summary.liquidation_count == 0
    assert run.guardrail_summary.margin_reject_count == 0
    assert run.guardrail_summary.no_capital_halt is False


def test_reference_b_regime_trendlines_matches_baseline_export():
    _, _, rows = load_reference(REFERENCE_B)
    run = run_reference(REFERENCE_B)
    result = run.strategy_result

    mismatch = _first_mismatch(rows, result.trades)
    assert mismatch is None, mismatch
    assert result.total_trades == 43
    assert result.winning_trades == 17
    assert round(result.winning_trades / result.total_trades * 100.0, 2) == 39.53
    assert result.net_profit_pct == pytest.approx(22.3484141712)
    assert result.profit_factor == pytest.approx(1.4057224218)
    assert result.max_drawdown_pct == pytest.approx(11.9928996219)
    assert run.guardrail_summary.liquidation_count == 0
    assert run.guardrail_summary.margin_reject_count == 0
    assert run.guardrail_summary.no_capital_halt is False


@pytest.mark.parametrize("reference_id", [REFERENCE_A, REFERENCE_B])
def test_baseline_does_not_exercise_final_boundary_close(reference_id):
    run = run_reference(reference_id)
    trades = run.strategy_result.trades

    assert trades
    assert iso_timestamp(trades[-1].exit_time) < iso_timestamp(BASELINE_END)


def test_regime_filter_changes_reference_a_trades_into_reference_b_trades():
    """The A→B delta comes only from entry gating: B's entries are a subset of
    dates where A-style signals fired while the regime agreed, and both runs
    share identical execution settings."""

    run_a = run_reference(REFERENCE_A)
    run_b = run_reference(REFERENCE_B)

    entries_a = {iso_timestamp(trade.entry_time) for trade in run_a.strategy_result.trades}
    entries_b = [iso_timestamp(trade.entry_time) for trade in run_b.strategy_result.trades]

    assert run_a.strategy_result.total_trades == 45
    assert run_b.strategy_result.total_trades == 43
    # Gating only blocks/permits entries; it cannot invent signal bars, but a
    # blocked trade can free the position for a later signal that reference A
    # spent inside a position. Every B-only entry must therefore still be a
    # valid signal bar, which the parity assertions above already prove
    # trade-for-trade against TradingView. Here we pin the observed overlap so
    # a silent regime regression cannot keep the count at 43 with different
    # trades.
    overlap = sum(1 for entry in entries_b if entry in entries_a)
    assert overlap == 38
