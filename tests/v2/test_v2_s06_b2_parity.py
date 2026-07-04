import csv
import json
from pathlib import Path

import pandas as pd
import pytest

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.engine_v2.runner import run_v2_strategy
from strategies.s06_r_trend_v02_b2.signals import S06B2Params, build_s06_b2_execution_data
from strategies.s06_r_trend_v02_b2.strategy import load_profile, normalized_params


REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_ROOT = REPO_ROOT / "data" / "baseline_v2" / "s06_r_trend_v02"
BASELINE_RUNTIME_PARAMS = {
    "dateFilter": True,
    "start": "2025-08-01T00:00:00Z",
    "end": "2025-12-01T00:00:00Z",
}


def _csv_rows(path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _load_reference(reference_id):
    reference_dir = BASELINE_ROOT / reference_id
    with (reference_dir / "params.json").open(encoding="utf-8") as handle:
        params = json.load(handle)["strategy_inputs"]
    return reference_dir, params, _csv_rows(reference_dir / "trades_normalized_utc.csv")


def _run_reference(reference_id):
    _, params, _ = _load_reference(reference_id)
    merged = normalized_params({**params, **BASELINE_RUNTIME_PARAMS})
    parsed = S06B2Params.from_dict(merged)
    df = load_data(REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv")
    prepared, trade_start_idx = prepare_dataset_with_warmup(
        df,
        pd.Timestamp("2025-08-01T00:00:00Z"),
        pd.Timestamp("2025-12-01T00:00:00Z"),
        1000,
    )
    data = build_s06_b2_execution_data(prepared, parsed)
    return run_v2_strategy(
        data=data,
        profile=load_profile(),
        params=merged,
        trade_start_idx=trade_start_idx,
    )


def _iso(ts):
    return pd.Timestamp(ts).isoformat().replace("+00:00", "Z")


def _first_mismatch(rows, trades, *, size_tolerance):
    if len(rows) != len(trades):
        return f"count expected={len(rows)} actual={len(trades)}"
    for index, (row, trade) in enumerate(zip(rows, trades), start=1):
        checks = [
            ("direction", row["direction"], trade.direction, None),
            ("entry_time", row["entry_time_utc"], _iso(trade.entry_time), None),
            ("exit_time", row["exit_time_utc"], _iso(trade.exit_time), None),
            ("entry_price", float(row["entry_price_usdt"]), trade.entry_price, 5e-4),
            ("exit_price", float(row["exit_price_usdt"]), trade.exit_price, 5e-4),
            ("size", float(row["size_qty"]), trade.size, size_tolerance),
            ("net_pnl", float(row["net_pnl_usdt"]), trade.net_pnl, 0.02),
        ]
        for field, expected, actual, tolerance in checks:
            if tolerance is None:
                if expected != actual:
                    return f"trade {index} {field}: expected={expected} actual={actual}"
            elif abs(expected - actual) > tolerance:
                return f"trade {index} {field}: expected={expected} actual={actual}"
    return None


def _size_residuals(rows, trades):
    residuals = []
    for index, (row, trade) in enumerate(zip(rows, trades), start=1):
        expected = float(row["size_qty"])
        actual = trade.size
        delta = abs(expected - actual)
        if delta > 1e-9:
            residuals.append((index, expected, actual, delta))
    return residuals


def test_pinned_data_prep_recipe_is_used_for_baseline_parity():
    df = load_data(REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv")
    prepared, trade_start_idx = prepare_dataset_with_warmup(
        df,
        pd.Timestamp("2025-08-01T00:00:00Z"),
        pd.Timestamp("2025-12-01T00:00:00Z"),
        1000,
    )

    assert trade_start_idx == 1000
    assert prepared.index[trade_start_idx] == pd.Timestamp("2025-08-01T00:00:00Z")
    assert prepared.index[-1] == pd.Timestamp("2025-12-01T00:00:00Z")


def test_reference_b_trade_sequence_and_supported_metrics_match_baseline_export():
    _, _, rows = _load_reference("reference_b_trend_bracket")
    run = _run_reference("reference_b_trend_bracket")
    result = run.strategy_result

    mismatch = _first_mismatch(rows, result.trades, size_tolerance=0.0100001)
    assert mismatch is None, mismatch
    assert result.total_trades == 48
    assert result.winning_trades == 21
    assert round(result.winning_trades / result.total_trades * 100.0, 2) == 43.75
    assert round(result.net_profit_pct, 2) == 25.87
    assert round(result.profit_factor, 3) == 1.438
    assert result.max_drawdown_pct == pytest.approx(9.9211555042)
    assert run.guardrail_summary.liquidation_count == 0
    assert run.guardrail_summary.margin_reject_count == 0
    assert run.guardrail_summary.no_capital_halt is False


def test_reference_a_trail_boundary_characterization_and_first_residual():
    _, _, rows = _load_reference("reference_a_reversal_trail")
    run = _run_reference("reference_a_reversal_trail")
    result = run.strategy_result

    assert result.total_trades == 61
    assert result.winning_trades == 31
    assert result.trades[-1].exit_time == pd.Timestamp("2025-12-01T00:00:00Z")
    assert result.trades[-1].exit_price == pytest.approx(1.423)
    assert round(result.winning_trades / result.total_trades * 100.0, 2) == 50.82
    assert result.net_profit_pct == pytest.approx(30.9420054193)
    assert result.profit_factor == pytest.approx(1.5088788696)
    assert result.max_drawdown_pct == pytest.approx(13.4683032109)
    assert run.guardrail_summary.liquidation_count == 0
    assert run.guardrail_summary.margin_reject_count == 0
    assert run.guardrail_summary.no_capital_halt is False

    first_residual = _first_mismatch(rows, result.trades, size_tolerance=1e-9)
    size_residuals = _size_residuals(rows, result.trades)
    assert first_residual == "trade 11 size: expected=24.36 actual=24.37"
    assert len(size_residuals) == 35
    assert max(delta for _, _, _, delta in size_residuals) == pytest.approx(0.03)
    assert size_residuals[-1][:3] == (61, 27.91, 27.93)
