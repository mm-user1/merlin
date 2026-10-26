"""Real HTTP/storage/replay and metadata-only filter admission, synthetic inputs."""
from copy import deepcopy
import json

import numpy as np
import pytest

from core import storage
from core.entry_filters import OFF, effective_config
from core.entry_filter_input import describe_saved
from tests.fixtures.admission_strategy import filtered as strategy
from tests.fixtures.admission_strategy.bootstrap import installed, publish, grid_payload


def config_for(source, enabled=True):
    config = grid_payload()
    config["fixed_params"].update(dateFilter=True, start="2024-01-03", end="2024-01-17", enableShort=True)
    config["entryFilters"] = dict(version=1, enabled=enabled, items=[dict(id="f1", type="candleDirection", version=1,
        enabled=True, parameters=dict(confirmationBars=2), source={**source, "timeframe_minutes": 60})])
    return config


@pytest.mark.parametrize("enabled", [False, True])
def test_backtest_download_explicit_context(client, tmp_path, enabled):
    from core.engine_v2.admission import run_strategy
    from ui.server_data_input import replay_config
    source = publish(tmp_path / "pack", rows=5000, turn_day=6)
    config = config_for(source, enabled)
    fields = dict(strategy=strategy.STRATEGY_ID, source=json.dumps(source), payload=json.dumps(config["fixed_params"]),
                  entryFilters=json.dumps(config["entryFilters"]), warmupBars="100")
    with installed(filtered=True):
        response = client.post("/api/backtest", data=fields)
        assert response.status_code == 200, response.get_data(as_text=True)
        body = response.get_json()
        replay = {"data_input": body["data_input"], "entry_admission": body["entry_admission"],
            "fixed_params": body["parameters"], "entryFilters": config["entryFilters"], "entry_filter_inputs": body["entry_filter_inputs"]}
        prepared = replay_config(replay)
        direct = run_strategy(strategy.FilterTestStrategy, prepared.frame, body["parameters"], prepared.trade_start_idx,
                              admission_rules=prepared.admission_rules, entry_filter_context=prepared.entry_filter_context)
        assert body["metrics"]["total_trades"] == len(direct.trades) > 0
        assert body["entry_diagnostics"] == direct.entry_diagnostics
        fields.update(dataInput=json.dumps(body["data_input"]), entryAdmission=json.dumps(body["entry_admission"]))
        for invalid in (None, {}, {"version": 2, "state": "off"}):
            fields["entryFilterInputs"] = json.dumps(invalid)
            assert client.post("/api/backtest/trades", data=fields).status_code == 400
        fields.pop("entryFilterInputs")
        assert client.post("/api/backtest/trades", data=fields).status_code == 400
        fields["entryFilterInputs"] = json.dumps(body["entry_filter_inputs"])
        download = client.post("/api/backtest/trades", data=fields)
        assert download.status_code == 200, download.get_data(as_text=True)
        assert download.data == client.post("/api/backtest/trades", data=fields).data
        if enabled:
            fields["entryFilterInputs"] = json.dumps({"version": 1, "state": "off"})
            assert client.post("/api/backtest/trades", data=fields).status_code == 400


def test_optimize_workers_manual_reconstruction_and_relocation(client, tmp_path):
    import shutil
    from core.engine_v2.admission import run_strategy
    from ui.server_data_input import replay_oos_input
    from ui.server_services import _resolve_stored_execution_context
    source = publish(tmp_path / "pack", rows=5000, turn_day=6)
    config = config_for(source)
    config["postProcess"] = dict(enabled=True, ftPeriodDays=3, topK=2, ftThresholdPct=-100000,
                                 stressTest=dict(enabled=True, topK=2))
    config["oosTest"] = dict(enabled=True, periodDays=3, topK=2)
    with installed(filtered=True):
        queue = dict(items=[dict(id="filtered", index=1, strategyId=strategy.STRATEGY_ID,
            sources=[source], sourceCursor=0, config=config)], nextIndex=2, runtime={"active": False})
        assert client.put("/api/queue", json=queue).status_code == 200
        restored = client.get("/api/queue").get_json()["items"][0]
        assert restored["config"]["entryFilters"] == config["entryFilters"]
        config, source = restored["config"], restored["sources"][0]
        response = client.post("/api/optimize", data=dict(strategy=strategy.STRATEGY_ID, source=json.dumps(source),
            config=json.dumps(config), warmupBars="100"))
        assert response.status_code == 200, response.get_data(as_text=True)
        study_id = response.get_json()["study_id"]
        saved = storage.load_study_from_db(study_id)
        study, trial = saved["study"], saved["trials"][0]
        pin = study["config_json"]["entry_filter_inputs"]
        assert pin["state"] == "filtered" and "entry_filter_context" not in study["config_json"]
        assert set(trial["entry_diagnostics"]) == {"is", "ft", "oos_test"}
        frame, idx, dates, rules, filters = replay_oos_input(study, trial)
        context = _resolve_stored_execution_context(study)
        params = context.params_for(trial["params"], operation_start=dates["start"], operation_end=dates["end"])
        direct = run_strategy(strategy.FilterTestStrategy, frame, params, idx, admission_rules=rules, entry_filter_context=filters)
        assert trial["entry_diagnostics"]["oos_test"] == {**direct.entry_diagnostics, "scope": "oos_test"}
        assert trial["oos_test_total_trades"] == len(direct.trades) > 0
        base = f"/api/studies/{study_id}/trials/{trial['trial_number']}"
        curve = client.post(base + "/equity", json={"module": "oos_test"})
        assert curve.status_code == 200, curve.get_data(as_text=True)
        actual = curve.get_json()
        assert len(actual["equity"]) > 0
        for module, endpoint in (("optuna", "trades"), ("forward_test", "ft-trades"), ("oos_test", "oos-trades"), ("stress_test", "trades")):
            assert client.post(base + "/equity", json={"module": module}).status_code == 200
            assert client.post(base + "/" + endpoint, json={}).status_code == 200
        manual = client.post(f"/api/studies/{study_id}/test", json=dict(dataSource="original_parquet",
            startDate="2024-01-05", endDate="2024-01-08", trialNumbers=[trial["trial_number"]], sourceTab="optuna"))
        assert manual.status_code == 200, manual.get_data(as_text=True)
        manual_id = manual.get_json()["test_id"]
        own = storage.load_manual_test_results(study_id, manual_id)["results_json"]["config"]
        assert own["entry_filter_inputs"]["sources"][0]["bounds"] != pin["sources"][0]["bounds"]
        assert own["entryFilters"] == config["entryFilters"]
        assert client.post(base + "/equity", json=dict(module="manual_tests", testId=manual_id)).status_code == 200
        relocated = tmp_path / "relocated"
        shutil.copytree(source["pack_root"], relocated)
        original = effective_config(config["entryFilters"])[0]["source"]["pack_root"]
        result = client.post(f"/api/studies/{study_id}/locate-parquet-pack",
                             json=dict(packRoot=str(relocated), dependency=original))
        assert result.status_code == 200, result.get_data(as_text=True)
        assert storage.load_study_from_db(study_id)["study"]["config_json"]["entry_filter_inputs"] == pin
        assert client.post(base + "/equity", json={"module": "oos_test"}).get_json() == actual


def test_preview_no_candles_disabled_and_unsupported(client, tmp_path, monkeypatch):
    from data_provider import data
    source = publish(tmp_path / "pack", rows=5000)
    config = {**config_for(source), "strategy_id": strategy.STRATEGY_ID}
    def no_read(*args, **kwargs):
        raise AssertionError("Preview loaded candles")
    monkeypatch.setattr(data._ReadSession, "load_slice", no_read)
    with installed(filtered=True):
        response = client.post("/api/grid/preview", json=dict(strategy=strategy.STRATEGY_ID, source=source, config=config))
        assert response.status_code == 200, response.get_data(as_text=True)
        config["entryFilters"]["enabled"] = False
        config["entryFilters"]["items"][0]["source"]["pack_root"] = str(tmp_path / "absent")
        assert client.post("/api/grid/preview", json=dict(strategy=strategy.STRATEGY_ID, source=source, config=config)).status_code == 200


def test_offline_description_unknown_dominates():
    assert describe_saved({}) == dict(key="off", text="Filter Off")
    assert describe_saved({"entryFilters": None})["key"] is None
    assert describe_saved({"entryFilters": {"version": 1, "enabled": False, "items": []}})["text"] == "Unavailable"


def test_filter_outside_allowed_root_and_disabled_no_io(client, tmp_path, monkeypatch):
    from ui import server_data_input, server_services
    approved = tmp_path / "approved"
    source = publish(approved / "target", rows=5000)
    other = publish(tmp_path / "outside", rows=5000)
    monkeypatch.setattr(server_services, "CSV_ALLOWED_ROOTS", [approved])
    monkeypatch.setattr(server_data_input, "DEFAULT_PARQUET_ROOT", approved)
    config = {**config_for(other), "strategy_id": strategy.STRATEGY_ID}
    with installed(filtered=True):
        fields = dict(strategy=strategy.STRATEGY_ID, source=source, config=config)
        result = client.post("/api/grid/preview", json=fields)
        assert result.status_code in (400, 403) and "outside allowed roots" in result.get_data(as_text=True)
        config["entryFilters"]["enabled"] = False
        assert client.post("/api/grid/preview", json=fields).status_code == 200


def test_filter_symlink_cannot_escape_allowed_root(client, tmp_path, monkeypatch):
    from ui import server_data_input, server_services
    approved = tmp_path / "approved"
    source = publish(approved / "target", rows=5000)
    other = publish(tmp_path / "outside", rows=5000)
    alias = approved / "alias"
    alias.symlink_to(other["pack_root"], target_is_directory=True)
    monkeypatch.setattr(server_services, "CSV_ALLOWED_ROOTS", [approved])
    monkeypatch.setattr(server_data_input, "DEFAULT_PARQUET_ROOT", approved)
    config = {**config_for({**other, "pack_root": str(alias)}), "strategy_id": strategy.STRATEGY_ID}
    with installed(filtered=True):
        result = client.post("/api/grid/preview", json=dict(strategy=strategy.STRATEGY_ID, source=source, config=config))
    assert result.status_code in (400, 403) and "outside allowed roots" in result.get_data(as_text=True)


def test_noncapable_refuses_active_filters_and_injected_replay(client, tmp_path):
    from tests.fixtures.admission_strategy import strategy as legacy
    source = publish(tmp_path / "pack", rows=5000)
    config = config_for(source)
    fields = dict(strategy=legacy.STRATEGY_ID, source=json.dumps(source), payload=json.dumps(config["fixed_params"]), warmupBars="100")
    with installed():
        original = client.post("/api/backtest", data=fields)
        assert original.status_code == 200
        body = original.get_json()
        fields["entryFilters"] = json.dumps(config["entryFilters"])
        assert client.post("/api/backtest", data=fields).status_code == 400
        fields.update(dataInput=json.dumps(body["data_input"]), entryAdmission=json.dumps(body["entry_admission"]))
        assert client.post("/api/backtest/trades", data=fields).status_code == 400
        fields.pop("entryFilters")
        fields["entryFilterInputs"] = json.dumps({"version": 1, "state": "off"})
        assert client.post("/api/backtest/trades", data=fields).status_code == 400


@pytest.mark.parametrize("adaptive", [False, True])
def test_wfa_context_windows_and_offline_summary(client, tmp_path, adaptive):
    source = publish(tmp_path / "pack", rows=8000, turn_day=6)
    config = config_for(source)
    config["fixed_params"]["end"] = "2024-01-26"
    fields = dict(strategy=strategy.STRATEGY_ID, source=json.dumps(source), config=json.dumps(config), warmupBars="100",
        wf_store_top_n_trials="2", wf_adaptive_mode=str(adaptive).lower(), wf_is_period_days="5", wf_oos_period_days="4",
        wf_max_oos_period_days="4", wf_min_oos_trades="1", wf_check_interval_trades="1")
    with installed(filtered=True):
        response = client.post("/api/walkforward", data=fields)
        assert response.status_code == 200, response.get_data(as_text=True)
        study_id = response.get_json()["study_id"]
        saved = storage.load_study_from_db(study_id)
        assert saved["windows"]
        pin = saved["study"]["config_json"]["entry_filter_inputs"]
        assert pin["state"] == "filtered"
        for window in saved["windows"][:2]:
            endpoint = f"/api/studies/{study_id}/wfa/windows/{window['window_number']}/equity"
            a = client.post(endpoint, json=dict(period="oos"))
            b = client.post(endpoint, json=dict(period="oos"))
            assert a.status_code == 200, a.get_data(as_text=True)
            assert a.get_json() == b.get_json()
        summary = client.get("/api/analytics/summary").get_json()
        row = next(s for s in summary["studies"] if s["study_id"] == study_id)
        assert row["entry_filters"] == describe_saved(saved["study"]["config_json"])
        assert row["entry_filters"]["key"] not in (None, "off")
