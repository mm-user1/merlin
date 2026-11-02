"""Generic configuration, real Fixed adapter parity and historical format boundaries."""
from copy import deepcopy
from dataclasses import asdict, replace
import importlib
import json

import numpy as np
import pytest

from tools.strategy_lab.fixed.config import resolve
from tools.strategy_lab.fixed.storage import open_dataset, read_account, RESULT_V2, contract_identity
from tools.strategy_lab.fixed.report import report
from tools.strategy_lab.dataset import atomic_write_json, artifact_record
from tools.strategy_lab.config import canonical_sha256
from tools.strategy_lab.filters import effective
from _filter_helpers import packs, fixed_request, editor, save_request, snapshot

runner = importlib.import_module("tools.strategy_lab.fixed.run")


@pytest.mark.parametrize("version,m", [(1, 1), (2, 1), (2, 2)])
def test_filtered_application_parity(packs, tmp_path, monkeypatch, version, m):
    from core import data_input
    from core.engine_v2 import runner as core_runner
    from core.grid_engine import run_grid_optimization
    from core.optuna_engine import OptimizationConfig
    from core.entry_filters import OFF
    from core.grid_v2 import GridV2StrategyHooks
    from strategies.s07_ha_v01_b2 import strategy
    from ui.server_data_input import effective_params
    target, filters = packs
    raw = fixed_request(filters, version=version, m=m)
    config, resolved = resolve(raw)
    g = raw["generation"]
    ps, period, instrument = resolved["parameter_sets"][0], g["periods"][0], g["market_data"]["instruments"][0]
    items = resolved["entry_filters"]
    account = runner.prepare_account(raw, config, (ps, 60, period, instrument), target)
    lab, _ = runner.execute_account(config, ps, account, target, items)
    prepared = runner.replay(account, target, items)
    fixed = effective_params(ps["params"], prepared)
    app_config = OptimizationConfig(csv_file="", strategy_id=g["strategy"]["strategy_id"], enabled_params={},
        param_ranges={}, param_types={}, fixed_params=fixed, warmup_bars=100, optimization_mode="grid", parameterMode="fixed",
        data_input=prepared.facts, admission_rules=prepared.admission_rules, entry_admission=prepared.entry_admission,
        entryFilters=g["execution"]["entryFilters"], entry_filter_inputs=prepared.entry_filter_inputs,
        entry_filter_context=prepared.entry_filter_context)
    app_config._portfolio_capture = True
    original = core_runner.run_v2_strategy
    captures = []
    def capture(**kwargs):
        result = original(**kwargs)
        captures.append((kwargs, result))
        return result
    monkeypatch.setattr(core_runner, "run_v2_strategy", capture)
    results, _ = run_grid_optimization(app_config, save_study=False, prepared_frame=prepared.frame)
    assert len(results) == len(captures) == 1
    kwargs, app = captures[0]
    enriched = original(**(kwargs | {"compute_sharpe_daily": True, "compute_max_drawdown_mtm": True}))
    assert asdict(lab.basic_metrics) == asdict(app.basic_metrics)
    for name, value in asdict(lab.advanced_metrics).items():
        expected = getattr(enriched.advanced_metrics, name)
        assert value == expected or value is not None and expected is not None and np.isnan(value) and np.isnan(expected)
    assert lab.max_drawdown_mtm_pct == enriched.max_drawdown_mtm_pct
    assert lab.strategy_result.trades == app.strategy_result.trades
    for name in ("equity_curve", "balance_curve", "timestamps"):
        np.testing.assert_array_equal(getattr(lab.strategy_result, name), getattr(app.strategy_result, name))
    assert asdict(lab.standing_state) == asdict(app.standing_state)
    signals = GridV2StrategyHooks.from_strategy(strategy, config=config, entry_filter_context=OFF).build_execution_data(prepared.frame, account["params"]).signals
    context = prepared.entry_filter_context
    blocked = np.count_nonzero((signals.long_entries & ~context.permit_long | signals.short_entries & ~context.permit_short)[prepared.trade_start_idx:])
    assert blocked > 0
    if m > 1:
        assert lab.basic_metrics.total_trades > 0
    out = tmp_path / "out"
    runner.run(save_request(tmp_path, raw), target, out)
    manifest = open_dataset(out)
    account = manifest["contract"]["accounts"][0]
    saved = read_account(out, account, manifest["completed"][account["account_id"]], RESULT_V2)[0]
    assert saved["metrics"]["total_trades"] == lab.basic_metrics.total_trades
    assert manifest["contract"]["resolved"]["entry_filters"] == items
    print(json.dumps({"version": version, "break": m, "trades": lab.basic_metrics.total_trades, "blocked_raw_entries": int(blocked)}))


def test_effective_identity_and_fixed_context_guard(packs, tmp_path, monkeypatch):
    from core.entry_filters import OFF
    target, filters = packs
    raw = fixed_request(filters)
    config, resolved = resolve(raw)
    def account(value):
        cfg, res = resolve(value)
        g = value["generation"]
        return runner.prepare_account(value, cfg, (res["parameter_sets"][0], 60, g["periods"][0], g["market_data"]["instruments"][0]), target)
    original = account(raw)
    equivalent = deepcopy(raw)
    records = equivalent["generation"]["execution"]["entryFilters"]["items"]
    records.append(deepcopy(records[0])); records[-1]["id"] = "duplicate"
    records.append({"id": "inactive", "enabled": False, "unknown": "unused"})
    records.reverse()
    assert effective(equivalent) == effective(raw)
    assert account(equivalent)["account_id"] == original["account_id"]
    off = account(fixed_request())
    assert off["account_id"] != original["account_id"]
    assert off["plan_fingerprint"] != original["plan_fingerprint"]
    assert off["candidate"] == original["candidate"]
    out = tmp_path / "out"
    runner.run(save_request(tmp_path, raw), target, out, preflight_only=True)
    before = snapshot(out)
    with pytest.raises(ValueError, match="contract changed"):
        runner.run(save_request(tmp_path, equivalent, "equivalent.json"), target, out, resume=True)
    assert snapshot(out) == before
    build = runner.build_execution
    def wrong_plan(config, ps, prepared, items=()):
        plan, hooks, params, candidate = build(config, ps, prepared, items)
        return replace(plan, settings=replace(plan.settings, entry_filter_config=())), hooks, params, candidate
    monkeypatch.setattr(runner, "build_execution", wrong_plan)
    with pytest.raises(ValueError, match="context conflicts"):
        runner.execute_account(config, resolved["parameter_sets"][0], original, target, effective(raw))
    monkeypatch.setattr(runner, "build_execution", build)
    replay = runner.replay
    def missing(*args):
        return replace(replay(*args), entry_filter_context=None)
    monkeypatch.setattr(runner, "replay", missing)
    with pytest.raises(ValueError, match="context is missing/invalid"):
        runner.execute_account(config, resolved["parameter_sets"][0], original, target, effective(raw))


@pytest.mark.parametrize("mutation", [
    lambda e: e["items"][0].update(type="unknown"),
    lambda e: e["items"][0].update(version=3),
    lambda e: e["items"][0]["parameters"].update(confirmationBars=True),
    lambda e: e["items"][0]["parameters"].update(confirmationBars=7),
    lambda e: e["items"][0]["parameters"].update(breakBars=3),
    lambda e: e["items"][0]["parameters"].update(breakBars=1.5),
])
def test_active_configuration_refused(packs, mutation):
    raw = fixed_request(packs[1]); mutation(raw["generation"]["execution"]["entryFilters"])
    with pytest.raises(ValueError): resolve(raw)


def test_new_off_and_disabled_dependencies(packs, tmp_path, monkeypatch):
    target, _ = packs
    new = fixed_request()
    new["generation"]["execution"]["entryFilters"]["items"] = [{"id": "retained", "garbage": "inactive"}]
    old = deepcopy(new); old["schema_version"] = "strategy_lab_fixed_runspec_v1"
    old["generation"]["execution"].pop("entryFilters"); old["generation"]["execution"]["entry_filters"] = []
    outputs = []
    for label, raw in (("old", old), ("new", new)):
        out = tmp_path / label; runner.run(save_request(tmp_path, raw, label+".json"), target, out)
        outputs.append(out)
    a, b = (next(o.glob("accounts/*/curve.parquet")).read_bytes() for o in outputs)
    assert a == b
    before = snapshot(outputs[1])
    from core import data_input
    from tools.strategy_lab import config
    for module, name in ((data_input, "prepare_input"), (data_input, "replay_input"), (config, "get_strategy_config")):
        monkeypatch.setattr(module, name, lambda *a, **k: pytest.fail("offline/live dependency"))
    report(outputs[1], tmp_path / "view")
    assert snapshot(outputs[1]) == before
    html = next((tmp_path / "view").glob("accounts/*/report.html")).read_text(encoding="utf-8")
    assert "entry_filter_key" in html and canonical_sha256([]) in html and "Filter Off" in html


def test_active_capability_refused_before_source_io(packs, monkeypatch):
    from tools.strategy_lab import config as shared
    from core import data_input
    original = shared._validate_strategy
    def unsupported(*args, **kwargs):
        config, profile = original(*args, **kwargs)
        config = deepcopy(config); config.pop("entryFilterSupport")
        return config, profile
    monkeypatch.setattr(shared, "_validate_strategy", unsupported)
    monkeypatch.setattr(data_input, "prepare_input", lambda *a, **k: pytest.fail("unsupported filter reached I/O"))
    with pytest.raises(ValueError, match="does not support"):
        resolve(fixed_request(packs[1]))


@pytest.mark.parametrize("case", ["result", "pair", "origin", "policy", "instrument", "effective"])
def test_resealed_filtered_history_refused(packs, tmp_path, case):
    target, filters = packs; out = tmp_path / "out"
    runner.run(save_request(tmp_path, fixed_request(filters)), target, out)
    manifest = open_dataset(out); account = manifest["contract"]["accounts"][0]
    claim = manifest["completed"][account["account_id"]]
    result_path = out / claim["result.json"]["path"]
    result = json.loads(result_path.read_text())
    if case == "result": result["schema_version"] = "strategy_lab_fixed_result_v1"
    elif case == "pair": manifest["schema_version"] = "strategy_lab_fixed_dataset_v1"
    elif case == "effective": manifest["contract"]["resolved"]["entry_filters"] = []
    else:
        pin = account["entry_filter_inputs"]["sources"][0]
        if case == "origin": pin["bounds"]["origin"] = "2024-02-01T00:00:00+00:00"
        elif case == "policy": pin["policy"] = "unknown"
        else: pin["instrument"]["instrument_id"] = "BYBIT_OTHERUSDT"
        result["account"] = account
    atomic_write_json(result_path, result); claim["result.json"] = artifact_record(result_path, out)
    manifest["contract_sha256"] = contract_identity(manifest["contract"])
    atomic_write_json(out / "manifest.json", manifest)
    before = snapshot(out)
    with pytest.raises(ValueError): open_dataset(out)
    assert snapshot(out) == before
