"""Application Fixed parity, actual boundary-crossing positions and calendar views."""
from dataclasses import asdict
import json

import numpy as np
import pandas as pd
import pytest

from tools.strategy_lab.fixed.config import resolve
from tools.strategy_lab.fixed.run import prepare_account, execute_account, run
from tools.strategy_lab.fixed.accounting import finite_json, monthly
from tools.strategy_lab.fixed.storage import open_dataset, read_account
from _fixed_helpers import fixed_pack, request, save_request


def test_application_fixed_date_only_end_and_continuity(fixed_pack, tmp_path, monkeypatch):
    from core import data_input, metrics
    from core.engine_v2 import runner
    from core.grid_engine import run_grid_optimization
    from core.optuna_engine import OptimizationConfig
    from ui.server_data_input import effective_params
    raw = request()
    g = raw["generation"]
    g["periods"][0].update(start="2024-01-31", end="2024-02-02")
    config, resolved = resolve(raw)
    ps, period, instrument = resolved["parameter_sets"][0], g["periods"][0], g["market_data"]["instruments"][0]
    account = prepare_account(raw, config, (ps, 60, period, instrument), fixed_pack)
    lab, _ = execute_account(config, ps, account, fixed_pack)
    prepared = data_input.prepare_input(dict(type="parquet", version=1, pack_root=str(fixed_pack),
        instrument_id=instrument["instrument_id"], timeframe_minutes=60), date_filter=True,
        start=period["start"], end=period["end"], warmup_bars=100, admission_params=ps["params"], entry_filters=[])
    fixed = effective_params(ps["params"], prepared)
    assert fixed["end"] == account["params"]["end"] and "23:59:59" in fixed["end"]
    app_config = OptimizationConfig(csv_file="", strategy_id=g["strategy"]["strategy_id"],
        enabled_params={}, param_ranges={}, param_types={}, fixed_params=fixed, warmup_bars=100,
        optimization_mode="grid", parameterMode="fixed", data_input=prepared.facts,
        admission_rules=prepared.admission_rules, entry_admission=prepared.entry_admission,
        entryFilters={"version":1,"enabled":False,"items":[]}, entry_filter_inputs=prepared.entry_filter_inputs,
        entry_filter_context=prepared.entry_filter_context)
    app_config._portfolio_capture = True
    original = runner.run_v2_strategy
    captured = []
    def capture(**kwargs):
        value = original(**kwargs)
        captured.append((kwargs, value))
        return value
    monkeypatch.setattr(runner, "run_v2_strategy", capture)
    results, _ = run_grid_optimization(app_config, save_study=False, prepared_frame=prepared.frame)
    assert len(results) == len(captured) == 1
    kwargs, app = captured[0]
    assert asdict(lab.basic_metrics) == asdict(app.basic_metrics)
    for name, value in asdict(app.advanced_metrics).items():
        if not name.startswith("sharpe_daily"):
            expected = getattr(lab.advanced_metrics, name)
            assert value == expected or value is not None and expected is not None and np.isnan(value) and np.isnan(expected)
    for name in ("equity_curve", "balance_curve", "timestamps"):
        np.testing.assert_array_equal(getattr(lab.strategy_result, name), getattr(results[0].portfolio_execution, name))
    assert lab.strategy_result.trades == app.strategy_result.trades  # Every trade field, including fees reflected in net P&L.
    assert finite_json(asdict(lab.standing_state)) == finite_json(asdict(app.standing_state))
    enriched = original(**(kwargs | {"compute_sharpe_daily":True, "compute_max_drawdown_mtm":True}))
    assert lab.advanced_metrics.sharpe_daily == enriched.advanced_metrics.sharpe_daily
    assert lab.max_drawdown_mtm_pct == enriched.max_drawdown_mtm_pct
    assert app.advanced_metrics.sharpe_daily is None
    trades = lab.strategy_result.trades
    assert any(t.entry_time > pd.Timestamp("2024-02-02", tz="UTC") for t in trades)
    crossing = [t for t in trades if t.entry_time.month != t.exit_time.month]
    assert crossing and all(t.exit_time != pd.Timestamp("2024-02-01", tz="UTC") for t in crossing)
    sr = lab.strategy_result
    assert trades[-1].exit_time == sr.timestamps[-1]  # The only final forced-close boundary.
    assert sr.balance_curve[-1] == sr.equity_curve[-1] and lab.standing_state.position_direction == 0
    last = trades[-1]
    gross = (last.exit_price-last.entry_price)*last.size*(1 if last.direction == "long" else -1)
    assert last.net_pnl == pytest.approx(gross-(last.entry_price+last.exit_price)*last.size*fixed["commissionPct"]/100)
    assert lab.basic_metrics.net_profit < 0  # A loss remains a valid complete account.
    out = tmp_path / "out"
    run(save_request(tmp_path, raw), fixed_pack, out)
    m = open_dataset(out); a = m["contract"]["accounts"][0]
    saved, (ts, eq, bal), rows = read_account(out, a, m["completed"][a["account_id"]])
    view = metrics._advanced_metric_view(sr, initial_balance=fixed["initialCapital"])
    np.testing.assert_array_equal(eq, view.equity_observations)
    returns = metrics._calculate_monthly_returns(list(view.equity_observations), view.timestamps, initial_equity=view.initial_equity)
    np.testing.assert_allclose([r["equity_return_pct"] for r in rows], returns, rtol=0, atol=1e-12)
    assert all(r["partial_month"] for r in rows) and len(rows) == 2
    assert np.prod(1+np.asarray(returns)/100) == pytest.approx(eq[-1]/saved["metric_anchor"])
    assert sum(r["realized_pnl"] for r in rows) == pytest.approx(lab.basic_metrics.net_profit)
    feb = np.flatnonzero(pd.to_datetime(ts, utc=True).month == 2)[0]
    assert eq[feb] != bal[feb]  # The crossing position is still open at the calendar boundary.
    assert rows[1]["opening_balance"] == bal[feb-1] != saved["initial_capital"]


@pytest.mark.parametrize("end,counts", [("2024-03-31", [696,744]), ("2024-04-01T00:00:00Z", [696,744,1])])
def test_complete_months_and_precise_midnight(fixed_pack, tmp_path, end, counts):
    raw = request();raw["generation"]["periods"][0].update(start="2024-02-01", end=end)
    out = tmp_path / "out"
    run(save_request(tmp_path, raw), fixed_pack, out)
    m = open_dataset(out); a = m["contract"]["accounts"][0]
    result, _, rows = read_account(out, a, m["completed"][a["account_id"]])
    assert [r["observation_count"] for r in rows] == counts
    assert [r["partial_month"] for r in rows] == [False,False] + ([True] if len(counts)==3 else [])


def test_first_observation_and_nonpositive_months():
    from core.metrics import _calculate_monthly_returns
    ts = pd.to_datetime(["2024-01-31T23:00Z", "2024-02-01T00:00Z", "2024-03-01T00:00Z", "2024-04-01T00:00Z"], utc=True).asi8
    eq = np.array([110.,0.,-5.,12.]);bal=np.array([105.,5.,-10.,12.])
    rows = monthly(ts, eq, bal, 100., 60)
    assert rows[0]["equity_return_pct"] == pytest.approx(10.)
    assert rows[0]["realized_pnl"] == 5.
    assert [r["equity_return_pct"] for r in rows[2:]] == [None,None]
    assert [r["return_unavailable_reason"] for r in rows[2:]] == ["nonpositive_opening_equity"]*2
    assert sum(r["realized_pnl"] for r in rows) == -88.
    assert sum(r["equity_change"] for r in rows) == -88.
    core = _calculate_monthly_returns(eq.tolist(), pd.to_datetime(ts, utc=True), initial_equity=100.)
    np.testing.assert_allclose([r["equity_return_pct"] for r in rows if r["equity_return_pct"] is not None], core)
