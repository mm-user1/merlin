"""Fixed HTTP execution, transport, compatibility and offline presentation."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from core import storage
from ui import server_routes_run, server_services

STRATEGY = "s03_reversal_v11_regime_er_b2"


def payload(**overrides):
    result = dict(parameterMode="fixed", optimization_mode="grid", enabled_params={}, param_ranges={},
        fixed_params=dict(dateFilter=False, useRegime=False, useEmergencySL=False, maLength3=5), worker_processes=1)
    result.update(overrides)
    return result


@pytest.mark.parametrize("extra", [
    {"parameterMode": None}, {"parameterMode": "other"}, {"parameterMode": True},
    {"enabled_params": {"maLength3": True}},
    {"param_ranges": {"maLength3": [5, 10, 1]}}, {"fixed_params": {"maType3_options": ["SMA"]}},
    {"postProcess": {"enabled": True}}, {"postProcess": {"dsrEnabled": True}},
    {"postProcess": {"stressTest": {"enabled": True}}},
    {"constraints": [{"metric": "total_trades", "enabled": True, "threshold": 10}]},
    {"filter_min_profit": True}, {"score_config": {"filter_enabled": True}},
    {"grid_slow_refinement_enabled": True}, {"gridSlowRefinementEnabled": "true"},
    {"grid_config": {"slow_refinement_enabled": True}}, {"wf_adaptive_mode": True},
])
@pytest.mark.parametrize("endpoint", ["/api/optimize", "/api/walkforward", "/api/grid/preview"])
def test_refusal_before_input_or_output(client, monkeypatch, extra, endpoint):
    def forbidden(*args, **kwargs):
        pytest.fail("Refused Fixed request reached dataset preparation")
    monkeypatch.setattr(server_routes_run, "prepare_request", forbidden)
    monkeypatch.setattr(server_routes_run, "preview_request_rules", forbidden)
    config = payload(**extra)
    if endpoint.endswith("preview"):
        response = client.post(endpoint, json=dict(strategyId=STRATEGY, config=config))
    else:
        response = client.post(endpoint, data=dict(strategy=STRATEGY, config=json.dumps(config), warmupBars="100"))
    assert response.status_code == 400, response.get_data(as_text=True)


def test_v1_and_form_adaptive_refuse_before_io(client, monkeypatch):
    monkeypatch.setattr(server_routes_run, "prepare_request", lambda *a, **k: pytest.fail("pack IO"))
    for endpoint in ("/api/optimize", "/api/walkforward"):
        response = client.post(endpoint, data=dict(strategy="s03_reversal_v10", config=json.dumps(payload())))
        assert response.status_code == 400 and "V2_INVALID_PARAMETER_MODE" in response.get_data(as_text=True)
        response = client.post(endpoint, data=dict(strategy=STRATEGY, config=json.dumps(payload(optimization_mode="optuna"))))
        assert response.status_code == 400 and "V2_INVALID_PARAMETER_MODE" in response.get_data(as_text=True)
    response = client.post("/api/walkforward", data=dict(strategy=STRATEGY, config=json.dumps(payload()), wf_adaptive_mode="true"))
    assert response.status_code == 400 and "Adaptive" in response.get_data(as_text=True)


def test_preview_neutralizes_stale_values_and_preserves_unmarked_counts(client):
    config = payload(grid_budget="bad", grid_seed={}, grid_top_candidates=None,
        grid_min_quota="bad", grid_manual_percents="bad", grid_diversity_max_per_group="bad",
        objectives=[], grid_fast_objectives=["bad"], grid_v2_planning_policy="bad",
        enabled_params={"maLength3": "false"}, postProcess={"enabled": "false"})
    response = client.post("/api/grid/preview", json=dict(strategyId=STRATEGY, config=config))
    assert response.status_code == 200, response.get_data(as_text=True)
    assert response.get_json()["preview"]["planned_candidate_count"] == 1
    for marker in ({}, {"parameterMode": "search"}):
        search = dict(optimization_mode="grid", enabled_params={}, param_ranges={}, fixed_params={}, **marker)
        response = client.post("/api/grid/preview", json=dict(strategyId=STRATEGY, config=search))
        assert response.status_code == 200, response.get_data(as_text=True)
        assert response.get_json()["preview"]["planned_candidate_count"] == 7397280


def test_dataclass_absence_and_explicit_null():
    config = server_services._build_optimization_config("unused.csv", payload(), 1, STRATEGY, 100)
    config.parameterMode = None
    assert "parameterMode" not in storage._prepare_study_config_payload(config)
    with pytest.raises(ValueError, match="parameterMode"):
        storage._prepare_study_config_payload({"parameterMode": None})


def make_csv(tmp_path):
    x = np.arange(24 * 155)
    price = 100 + 3 * np.sin(x / 17)
    path = tmp_path / "synthetic.csv"
    pd.DataFrame(dict(time=pd.date_range("2024-01-01", periods=len(x), freq="h", tz="UTC").as_unit("s").asi8,
        open=price, high=price+1, low=price-1, close=price+.1, volume=np.ones(len(x)))).to_csv(path, index=False)
    return path


def test_csv_optimize_oos_and_stored_preview(client, tmp_path, monkeypatch):
    from core import grid_engine
    monkeypatch.setattr(grid_engine, "rank_grid_results", lambda *a, **k: pytest.fail("ranking"))
    monkeypatch.setattr(server_routes_run, "run_forward_test", lambda *a, **k: pytest.fail("FT"))
    config = payload(oosTest=dict(enabled=True, periodDays=10, topK=1))
    config["fixed_params"].update(enableLong=False, enableShort=False)
    response = client.post("/api/optimize", data=dict(strategy=STRATEGY, csvPath=str(make_csv(tmp_path)),
        config=json.dumps(config), warmupBars="100"))
    assert response.status_code == 200, response.get_data(as_text=True)
    study_id = response.get_json()["study_id"]
    saved = storage.load_study_from_db(study_id)
    study, trials = saved["study"], saved["trials"]
    assert len(trials) == 1 and trials[0]["total_trades"] == 0
    assert trials[0]["oos_test_total_trades"] == 0
    assert study["config_json"]["parameterMode"] == "fixed"
    assert server_services._derive_grid_preview(study["config_json"], study)["planned_candidate_count"] == 1
    view = server_services.build_grid_settings_view(study)
    assert view["available"] and view["allocation_rows"] == []
    assert {row["key"] for row in view["rows"]} == {"Parameter Mode", "Result", "Workers", "Runtime"}
    base = f"/api/studies/{study_id}/trials/{trials[0]['trial_number']}"
    assert client.post(base + "/equity", json={"module": "oos_test"}).status_code == 200
    assert client.post(base + "/trades", json={}).status_code == 200


@pytest.mark.parametrize("months", [False, True])
def test_wfa_marker_and_singleton_in_every_window(client, tmp_path, months):
    config = payload()
    config["fixed_params"].update(dateFilter=True, start="2024-01-01", end="2024-06-01",
                                   enableLong=False, enableShort=False)
    fields = dict(strategy=STRATEGY, csvPath=str(make_csv(tmp_path)), config=json.dumps(config), warmupBars="100",
        wf_is_period_days="30", wf_oos_period_days="20", wf_store_top_n_trials="1")
    if months:
        fields.update(wf_period_unit="months", wf_is_period_months="2", wf_oos_period_months="1")
    response = client.post("/api/walkforward", data=fields)
    assert response.status_code == 200, response.get_data(as_text=True)
    study_id = response.get_json()["study_id"]
    saved = storage.load_study_from_db(study_id)
    study = saved["study"]
    assert study["config_json"]["parameterMode"] == "fixed"
    assert server_services._derive_grid_preview(study["config_json"], study)["planned_candidate_count"] == 1
    assert client.get("/api/analytics/summary").status_code == 200
    assert len(saved["windows"]) >= 2
    effective = []
    for window in saved["windows"]:
        assert window["module_status"]["grid_v2"]["candidate_count"] == 1
        assert window["oos_total_trades"] == 0
        effective.append({k: v for k, v in window["best_params"].items() if k not in {"start", "end", "dateFilter", "warmupBars"}})
    assert all(params == effective[0] for params in effective)
    damaged = deepcopy(study["config_json"])
    damaged["parameterMode"] = None
    with storage.get_db_connection() as conn:
        conn.execute("UPDATE studies SET config_json=? WHERE study_id=?", (json.dumps(damaged), study_id))
        conn.commit()
    response = client.get("/api/analytics/summary")
    assert response.status_code == 200
    view = server_services.build_grid_settings_view({**study, "config_json": damaged})
    assert not view["available"] and view["unavailable_reason"] == "invalid_parameter_mode"


@pytest.mark.parametrize("blocked_by", ["filter", "cap"])
def test_parquet_filter_cap_manual_and_pin_refusal(client, tmp_path, blocked_by):
    from tests.fixtures.admission_strategy.bootstrap import installed, publish, grid_payload
    from tests.fixtures.admission_strategy import filtered as strategy
    from data_provider import manifest
    source = publish(tmp_path / "pack", rows=5000)
    config = grid_payload()
    config.update(parameterMode="fixed", enabled_params={}, param_ranges={}, grid_slow_refinement_enabled=False,
        worker_processes=1, oosTest=dict(enabled=True, periodDays=3, topK=1))
    config["fixed_params"].update(dateFilter=True, start="2024-01-03", end="2024-01-17",
        maxEntryLeverage=.001 if blocked_by == "cap" else 8,
        enableLong=blocked_by != "filter", enableShort=True)
    config["entryFilters"] = dict(version=1, enabled=True, items=[dict(id="f", type="candleDirection", version=1,
        enabled=True, parameters=dict(confirmationBars=2), source={**source, "timeframe_minutes": 60})])
    with installed(filtered=True):
        response = client.post("/api/optimize", data=dict(strategy=strategy.STRATEGY_ID, source=json.dumps(source),
            config=json.dumps(config), warmupBars="100"))
        assert response.status_code == 200, response.get_data(as_text=True)
        study_id = response.get_json()["study_id"]
        saved = storage.load_study_from_db(study_id)
        study, trial = saved["study"], saved["trials"][0]
        assert len(saved["trials"]) == 1 and trial["total_trades"] == 0
        assert trial["oos_test_total_trades"] == 0
        assert (trial["entry_diagnostics"]["is"]["leverageCapSkippedEntries"] > 0) == (blocked_by == "cap")
        assert server_services._derive_grid_preview(study["config_json"], study)["planned_candidate_count"] == 1
        base = f"/api/studies/{study_id}/trials/{trial['trial_number']}"
        assert client.post(base + "/equity", json={"module": "oos_test"}).status_code == 200
        assert client.post(base + "/oos-trades", json={}).status_code == 200
        response = client.post(f"/api/studies/{study_id}/test", json=dict(dataSource="original_parquet",
            startDate="2024-01-05", endDate="2024-01-08", trialNumbers=[trial["trial_number"]], sourceTab="optuna"))
        assert response.status_code == 200, response.get_data(as_text=True)
        manual = storage.load_manual_test_results(study_id, response.get_json()["test_id"])
        assert manual["results_json"]["config"]["parameterMode"] == "fixed"
        if blocked_by == "cap":
            response = client.post("/api/walkforward", data=dict(strategy=strategy.STRATEGY_ID, source=json.dumps(source),
                config=json.dumps(config), warmupBars="100", wf_store_top_n_trials="1", wf_is_period_days="5", wf_oos_period_days="3"))
            assert response.status_code == 200, response.get_data(as_text=True)
            wfa = storage.load_study_from_db(response.get_json()["study_id"])
            assert wfa["study"]["config_json"]["entryFilters"] == study["config_json"]["entryFilters"]
            assert len(wfa["windows"]) >= 2
            for window in wfa["windows"]:
                assert window["best_params"]["maxEntryLeverage"] == .001
                assert window["module_status"]["grid_v2"]["candidate_count"] == 1
                assert window["oos_total_trades"] == 0
        # Mutate only the synthetic pack to prove strict original-source replay.
        document = manifest.read_manifest(source["pack_root"])
        rules = document["instruments"][0]["instrument_rules"]
        rules["quantity_step"] = rules["raw_contract_fields"]["qtyStep"] = "0.2"
        manifest.write_manifest(source["pack_root"], document)
        response = client.post(base + "/equity", json={"module": "optuna"})
        assert response.status_code == 400 and "changed" in response.get_data(as_text=True)
