"""Fixed execution/accounting integration and historical separation."""
import csv
import importlib
import json

import numpy as np
import pytest

from tools.strategy_lab.fixed import config
from tools.strategy_lab.fixed.run import run
from tools.strategy_lab.fixed.storage import open_dataset, read_account
from tools.strategy_lab.fixed.report import report
from tools.strategy_lab.dataset import atomic_write_json, artifact_record, load_json
from _fixed_helpers import fixed_pack, request, save_request, snapshot


def test_ulcer_unit_matches_core_and_both_html_routes(fixed_pack, tmp_path, monkeypatch):
    runner = importlib.import_module("tools.strategy_lab.fixed.run")
    execute = runner.execute_account
    core_values = []

    def capture(*args):
        execution, provenance = execute(*args)
        core_values.append(execution.advanced_metrics.ulcer_index)
        return execution, provenance

    monkeypatch.setattr(runner, "execute_account", capture)
    out = tmp_path / "out"
    run(save_request(tmp_path, request()), fixed_pack, out)
    manifest = open_dataset(out)
    account = manifest["contract"]["accounts"][0]
    result, _, _ = read_account(out, account, manifest["completed"][account["account_id"]])
    value = result["metrics"]["ulcer_index"]
    assert value > 0 and core_values == [value]
    assert result["metric_units"]["ulcer_index"] == "percent"
    saved = snapshot(out)
    view = tmp_path / "view"
    report(out, view)
    for root in (out, view):
        html = (root / "accounts" / account["account_id"] / "report.html").read_text(encoding="utf-8")
        assert f"<td>ulcer_index</td><td>{value}</td><td>percent</td>" in html
    assert snapshot(out) == saved


@pytest.mark.parametrize("partial", [False, True], ids=["complete", "partial"])
def test_historical_ulcer_label_preserved(fixed_pack, tmp_path, monkeypatch, partial):
    runner = importlib.import_module("tools.strategy_lab.fixed.run")
    out = tmp_path / "out"
    path = save_request(tmp_path, request())
    run(path, fixed_pack, out, preflight_only=True)
    pending_index = (out / "series-index.csv").read_bytes()
    run(path, fixed_pack, out, resume=True)
    # Model old saved metadata in this synthetic output, resealing only its claim.
    manifest = open_dataset(out)
    account = manifest["contract"]["accounts"][0]
    claim = manifest["completed"][account["account_id"]]
    result_path = out / claim["result.json"]["path"]
    result = load_json(result_path, "result")
    value = result["metrics"]["ulcer_index"]
    result["metric_units"]["ulcer_index"] = "ratio"
    atomic_write_json(result_path, result)
    claim["result.json"] = artifact_record(result_path, out)
    html_path = result_path.with_name("report.html")
    html_path.write_text(html_path.read_text(encoding="utf-8").replace(
        f"<td>ulcer_index</td><td>{value}</td><td>percent</td>",
        f"<td>ulcer_index</td><td>{value}</td><td>ratio</td>"), encoding="utf-8")
    manifest["complete"] = False
    atomic_write_json(out / "manifest.partial.json", manifest)
    if partial:
        (out / "manifest.json").unlink()
    else:
        manifest["complete"] = True
        atomic_write_json(out / "manifest.json", manifest)
        # A historical completed dataset may retain the baseline's stale index.
        (out / "series-index.csv").write_bytes(pending_index)
    saved = snapshot(out)
    accounts = snapshot(out / "accounts")
    monkeypatch.setattr(runner, "execute_account", lambda *a: pytest.fail("old account executed"))
    monkeypatch.setattr(runner, "publish_account", lambda *a: pytest.fail("old account republished"))
    assert run(path, fixed_pack, out, resume=True)["complete"]
    assert snapshot(out / "accounts") == accounts
    if not partial:
        assert snapshot(out) == saved
    before_report = snapshot(out)
    view = tmp_path / "view"
    assert report(out, view)["complete"]
    assert snapshot(out) == before_report
    with (view / "series-index.csv").open(newline="", encoding="utf-8") as stream:
        row, = csv.DictReader(stream)
    assert row["status"] == "complete" and float(row["net_profit"]) == result["metrics"]["net_profit"]
    for root in (out, view):
        html = (root / "accounts" / account["account_id"] / "report.html").read_text(encoding="utf-8")
        assert f"<td>ulcer_index</td><td>{value}</td><td>ratio</td>" in html
    loaded, _, _ = read_account(out, account, claim)
    assert loaded["metrics"]["ulcer_index"] == value and loaded["metric_units"]["ulcer_index"] == "ratio"


def test_single_account_and_offline_report(fixed_pack, tmp_path):
    path = save_request(tmp_path, request())
    out = tmp_path / "dataset"
    assert run(path, fixed_pack, out)["complete"]
    m = open_dataset(out)
    a = m["contract"]["accounts"][0]
    result, curve, rows = read_account(out, a, m["completed"][a["account_id"]])
    assert len(curve[0]) == 48 and result["prepared_trade_start_idx"] == 100
    assert result["curve_start_idx"] == 0 and result["metrics"]["total_trades"] > 0
    assert result["final_state"]["position_direction"] == 0
    assert sum(r["realized_pnl"] for r in rows) == pytest.approx(result["metrics"]["net_profit"])
    assert result["backend"] == "generic_v2_reference"
    before = snapshot(out)
    assert run(path, fixed_pack, out)["no_op"]
    assert snapshot(out) == before
    view = report(out, tmp_path / "view")
    assert view["completed"] == 1 and snapshot(out) == before
    html = (tmp_path / "view" / "accounts" / a["account_id"] / "report.html").read_text()
    assert "<svg" in html and "<script" not in html and "https://" not in html
    assert "initialCapital" in html and "partial_month" in html


def test_no_trade_is_completed(fixed_pack, tmp_path):
    raw = request()
    raw["generation"]["parameter_sets"][0]["params"] = {"enableLong":False, "enableShort":False}
    out = tmp_path / "out"
    run(save_request(tmp_path, raw), fixed_pack, out)
    m = open_dataset(out)
    a = m["contract"]["accounts"][0]
    result, (_, eq, bal), _ = read_account(out, a, m["completed"][a["account_id"]])
    assert result["metrics"]["total_trades"] == 0 and result["metrics"]["net_profit"] == 0
    assert result["metrics"]["sharpe_ratio"] is None
    np.testing.assert_array_equal(eq, np.full(len(eq), 1000.))
    np.testing.assert_array_equal(eq, bal)


def test_admission_cap_blocked_account_is_retained(fixed_pack, tmp_path):
    raw = request()
    raw["generation"]["parameter_sets"][0]["params"] = {"riskPerTrade":100., "maxEntryLeverage":1.}
    out = tmp_path / "out"
    run(save_request(tmp_path, raw), fixed_pack, out)
    m = open_dataset(out);a = m["contract"]["accounts"][0]
    result, (_, eq, _), _ = read_account(out, a, m["completed"][a["account_id"]])
    assert result["metrics"]["total_trades"] == 0
    assert result["guardrails"]["margin_reject_count"] > 0
    assert result["entry_diagnostics"]["maxEntryLeverageUsed"] is None
    assert np.all(eq == 1000.)


@pytest.mark.parametrize("mutation", [
    lambda g:g.update(timeframes_minutes=[25]),
    lambda g:g["periods"][0].update(warmup_bars=99),
    lambda g:g["periods"][0].update(warmup_bars=True),
    lambda g:g["periods"][0].update(warmup_bars=5001),
    lambda g:g["parameter_sets"][0]["params"].update(enableLong="yes"),
    lambda g:g["parameter_sets"][0]["params"].update(stopX=[1.,2.]),
    lambda g:g["parameter_sets"][0]["params"].update(stopX=float("nan")),
    lambda g:g["parameter_sets"][0]["params"].update(bogus=1),
    lambda g:g["parameter_sets"][0]["params"].update(warmupBars=100),
    lambda g:g["parameter_sets"][0]["params"].update(contractSize=.01),
    lambda g:g["parameter_sets"][0]["params"].update(maxEntryLeverage=12),
    lambda g:g["parameter_sets"][0]["params"].update(maxEntryLeverage=.5),
    lambda g:g["execution"].update(entry_filters=[{"enabled":True}]),
    lambda g:g["strategy"].update(grid_v2_engine_version="future"),
    lambda g:g["resources"].update(cache_budget_mb=100),
    lambda g:g.update(expected_run_count=2),
])
def test_strict_boundary(mutation):
    raw = request()
    mutation(raw["generation"])
    with pytest.raises(ValueError):
        config.resolve(raw)


def test_literal_scalars_and_defaults():
    raw = request()
    raw["generation"]["parameter_sets"][0]["params"] = {"stopX":2.17, "stopRR":2.33}
    _, resolved = config.resolve(raw)
    values = resolved["parameter_sets"][0]["params"]
    assert values["stopX"] == 2.17 and values["stopRR"] == 2.33
    assert "dateFilter" not in values


def test_unsupported_capability_refused():
    raw = request()
    raw["generation"]["strategy"].update(strategy_id="s03_reversal_v11_regime_er_b2", strategy_version="v11-regime-er-b2")
    with pytest.raises(ValueError, match="capability"):
        config.resolve(raw)
