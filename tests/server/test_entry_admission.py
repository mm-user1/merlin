"""New-mode application transport and saved scope, using only synthetic packs."""
import json
from pathlib import Path

import pytest
from core import storage
from tests.fixtures.admission_strategy.bootstrap import installed, publish, grid_payload
from tests.fixtures.admission_strategy.strategy import STRATEGY_ID


def test_backtest_pin_and_fail_closed_inputs(client, tmp_path, monkeypatch):
    from core.engine_v2 import admission
    from tests.fixtures.admission_strategy.strategy import rule_entry
    from tests._replay_rules_helpers import change_rules
    from ui.server_data_input import replay_config
    with pytest.raises(ValueError, match='policy'):
        replay_config({}, rules='unknown')
    source = publish(tmp_path / "pack")
    params = grid_payload()["fixed_params"]
    params.pop("maxEntryLeverage")
    fields = dict(strategy=STRATEGY_ID, source=json.dumps(source), payload=json.dumps(params), warmupBars="100")
    with installed():
        response = client.post("/api/backtest", data=fields)
        assert response.status_code == 200, response.get_data(as_text=True)
        body = response.get_json()
        assert body["parameters"]["maxEntryLeverage"] == 8
        assert body["entry_diagnostics"]["maxEntryLeverageUsed"] > 0
        fields.update(dataInput=json.dumps(body["data_input"]), payload=json.dumps(body["parameters"]))
        assert client.post("/api/backtest/trades", data=fields).status_code == 400
        fields["entryAdmission"] = json.dumps(body["entry_admission"])
        for route in ('/api/backtest', '/api/backtest/trades'):
            assert client.post(route, data=fields).status_code == 200
        original_run = admission.run_strategy
        def forbidden(*args, **kwargs):
            raise AssertionError('Invalid Start rules must refuse before execution')
        with monkeypatch.context() as patch:
            patch.setattr(admission, 'run_strategy', forbidden)
            forged = admission.admission_envelope(admission.AdmissionRules.from_entry(rule_entry(step='.0001')), 8)
            admission.read_admission_envelope(forged, 8)  # Internally valid, but not current pack rules.
            for route in ('/api/backtest', '/api/backtest/trades'):
                denied = client.post(route, data={**fields, 'entryAdmission':json.dumps(forged), 'rules':'saved'})
                assert denied.status_code == 400 and 'rules changed' in denied.get_data(as_text=True)
            change_rules(source, step='.2')
            for route in ('/api/backtest', '/api/backtest/trades'):
                denied = client.post(route, data=fields)
                assert denied.status_code == 400 and 'rules changed' in denied.get_data(as_text=True)
        assert admission.run_strategy is original_run
        fresh = client.post('/api/backtest', data={k:v for k,v in fields.items() if k not in ('dataInput','entryAdmission')})
        assert fresh.status_code == 200, fresh.get_data(as_text=True)
        assert fresh.get_json()['entry_admission']['rules']['base_step'] == '0.2'
        assert fresh.get_json()['metrics'] != body['metrics']
        csv = dict(strategy=STRATEGY_ID, csvPath="unused.csv", payload=json.dumps(params))
        denied = client.post("/api/backtest", data=csv)
        assert denied.status_code == 400 and "Parquet" in denied.get_data(as_text=True)


@pytest.mark.parametrize("notional", ["0", "5", None])
def test_partial_cap_is_oos_storage_matches_direct_reference(client, tmp_path, notional):
    from core.backtest_engine import align_date_bounds, prepare_dataset_with_warmup
    from core.engine_v2.admission import run_strategy, aggregate_entries
    from tests.fixtures.admission_strategy.strategy import AdmissionTestStrategy
    from ui.server_data_input import replay_config, replay_oos_input
    from ui.server_services import _resolve_stored_execution_context
    source = publish(tmp_path / "pack", rows=4000, turn_day=6, notional=notional)
    config = grid_payload()
    config["fixed_params"].update(riskPerTrade=3., maxEntryLeverage=3.35,
                                   dateFilter=True, start="2024-01-02", end="2024-01-14T21:15:00Z")
    config["oosTest"] = dict(enabled=True, periodDays=7, topK=2)
    with installed():
        response = client.post("/api/optimize", data=dict(strategy=STRATEGY_ID, source=json.dumps(source),
            config=json.dumps(config), warmupBars="100"))
        assert response.status_code == 200, response.get_data(as_text=True)
        saved = storage.load_study_from_db(response.get_json()["study_id"])
        study = saved["study"]
        context = _resolve_stored_execution_context(study)
        prepared = replay_config(study["config_json"])
        for trial in saved["trials"]:
            params = context.params_for(trial["params"])
            start, end = align_date_bounds(prepared.frame.index, params["start"], params["end"])
            frame, idx = prepare_dataset_with_warmup(prepared.frame, start, end, context.warmup_bars)
            oos_frame, oos_idx, dates, rules, filters = replay_oos_input(study, trial)
            oos_params = context.params_for(trial["params"], operation_start=dates["start"], operation_end=dates["end"])
            for scope, bars, origin, values in (("is", frame, idx, params), ("oos_test", oos_frame, oos_idx, oos_params)):
                direct = run_strategy(AdmissionTestStrategy, bars, values, origin, admission_rules=rules)
                expected = {**direct.entry_diagnostics, "scope": scope}
                assert direct.trades and expected["leverageCapSkippedEntries"] > 0
                assert origin == 100
                assert all(event.fill_time_ns >= bars.index[origin].value for event in direct.entry_events)
                assert trial["entry_diagnostics"][scope] == expected
            oos = trial["entry_diagnostics"]["oos_test"]
            assert aggregate_entries([(trial["trial_number"], oos)] * 2, scope="oos_test") == oos
            assert trial["entry_diagnostics"]["is"]["leverageCapSkippedEntries"] != oos["leverageCapSkippedEntries"]
            print("partial cap", notional, trial["trial_number"], trial["entry_diagnostics"])


def test_grid_postprocess_manual_and_offline_diagnostics(client, tmp_path, monkeypatch):
    from copy import deepcopy
    from core import data_input
    from core.engine_v2.admission import run_strategy
    from core.export import export_trades_csv
    from tests.fixtures.admission_strategy.strategy import AdmissionTestStrategy
    from tests._replay_rules_helpers import change_rules, assert_current_rules_differ
    from ui.server_data_input import trial_execution
    from ui.server_services import _run_export_execution, _project_execution_equity
    source = publish(tmp_path / "pack", rows=5000)
    config = grid_payload()
    config["postProcess"] = dict(enabled=True, ftPeriodDays=3, topK=2, ftThresholdPct=-100000,
        stressTest=dict(enabled=True, topK=2))
    config["oosTest"] = dict(enabled=True, periodDays=3, topK=2)
    with installed():
        response = client.post("/api/optimize", data=dict(strategy=STRATEGY_ID, source=json.dumps(source),
            config=json.dumps(config), warmupBars="100"))
        assert response.status_code == 200, response.get_data(as_text=True)
        study_id = response.get_json()["study_id"]
        saved = storage.load_study_from_db(study_id)
        trial = saved["trials"][0]
        assert set(trial["entry_diagnostics"]) == {"is", "ft", "oos_test"}
        number = trial["trial_number"]
        originals = {}
        for module, trades in (("optuna", "trades"), ("forward_test", "ft-trades"), ("oos_test", "oos-trades"), ("stress_test", "trades")):
            base = f"/api/studies/{study_id}/trials/{number}"
            replay = client.post(base + "/equity", json={"module": module})
            assert replay.status_code == 200, replay.get_data(as_text=True)
            download = client.post(base + "/" + trades, json={})
            assert download.status_code == 200
            originals[module] = (trades, replay.get_json(), download.data)
        from data_provider import manifest
        root = Path(source["pack_root"])
        document = manifest.read_manifest(root)
        rules = document["instruments"][0]["instrument_rules"]
        rules["quantity_step"] = rules["raw_contract_fields"]["qtyStep"] = "0.2"
        manifest.write_manifest(root, document)
        for module, (trades, curve, csv) in originals.items():
            assert client.post(base + '/equity', json={'module':module}).get_json() == curve
            assert client.post(base + '/' + trades, json={}).data == csv
        assert storage.load_study_from_db(study_id) == saved
        manual = client.post(f"/api/studies/{study_id}/test", json=dict(dataSource="original_parquet",
            startDate="2024-01-03", endDate="2024-01-06", trialNumbers=[number], sourceTab="optuna"))
        assert manual.status_code == 200, manual.get_data(as_text=True)
        test_id = manual.get_json()["test_id"]
        stored = storage.load_manual_test_results(study_id, test_id)["results_json"]
        assert stored['config']['entry_admission'] == saved['study']['config_json']['entry_admission']
        assert stored["results"][0]["entry_diagnostics"]["scope"] == "manual"
        replay = client.post(base + "/equity", json=dict(module="manual_tests", testId=test_id))
        assert replay.status_code == 200, replay.get_data(as_text=True)
        manual_row = storage.load_manual_test_results(study_id, test_id)
        manual_csv = client.post(f'/api/studies/{study_id}/tests/{test_id}/trials/{number}/mt-trades')
        assert manual_csv.status_code == 200
        execution = trial_execution(saved['study'], trial, 'manual_tests', manual=manual_row)
        original, error = _run_export_execution(**execution)
        assert error is None and original.trades
        # Same original frame/params, but independently resolved current B rules.
        from core.backtest_engine import align_date_bounds, prepare_dataset_with_warmup
        frame = execution['prepared_frame']; params = execution['params']
        start, end = align_date_bounds(frame.index, params['start'], params['end'])
        frame, idx = prepare_dataset_with_warmup(frame, start, end, execution['warmup_bars'])
        current = data_input.prepare_input(source, date_filter=False, admission_params={})
        historical = assert_current_rules_differ(AdmissionTestStrategy, frame, params, idx, original,
                                                admission_rules=current.admission_rules)
        manual_request = dict(dataSource='original_parquet', startDate='2024-01-03', endDate='2024-01-06',
                              trialNumbers=[number], sourceTab='optuna')
        # Faithful old-policy publication: execute/save B while parent remains A.
        prepare = data_input.prepare_manual_input
        def old_policy(pin, **kwargs):
            return prepare(pin, **{**kwargs, 'entry_admission':current.entry_admission})
        with monkeypatch.context() as patch:
            patch.setattr(data_input, 'prepare_manual_input', old_policy)
            old = client.post(f'/api/studies/{study_id}/test', json=manual_request)
        assert old.status_code == 200, old.get_data(as_text=True)
        historical_id = old.get_json()['test_id']
        historical_row = storage.load_manual_test_results(study_id, historical_id)
        assert historical_row['results_json']['config']['entry_admission'] == current.entry_admission
        change_rules(source, step='.3')  # C differs from both parent A and historical manual B.
        assert client.post(base+'/equity', json=dict(module='manual_tests',testId=test_id)).get_json() == replay.get_json()
        assert client.post(f'/api/studies/{study_id}/tests/{test_id}/trials/{number}/mt-trades').data == manual_csv.data
        assert storage.load_manual_test_results(study_id,test_id) == manual_row
        expected_equity, expected_dates = _project_execution_equity(historical, params, params['initialCapital'])
        actual = client.post(base+'/equity', json=dict(module='manual_tests',testId=historical_id))
        assert actual.get_json() == dict(equity=expected_equity,timestamps=expected_dates,reference_capital=params['initialCapital'])
        exported = client.post(f'/api/studies/{study_id}/tests/{historical_id}/trials/{number}/mt-trades')
        assert exported.status_code == 200
        assert exported.get_data(as_text=True) == export_trades_csv(historical.trades, symbol='BYBIT:AAAUSDT')
        assert storage.load_manual_test_results(study_id, historical_id) == historical_row
        located = client.post(f'/api/studies/{study_id}/locate-parquet-pack',json={'packRoot':str(root),'testId':historical_id})
        assert located.status_code == 200, located.get_data(as_text=True)
        located_row = storage.load_manual_test_results(study_id,historical_id)
        assert located_row['results_json']['config'] == {**historical_row['results_json']['config'],'data_input_root_override':str(root)}
        # Own envelope and fixed cap agree, but cannot override the parent cap.
        bad = deepcopy(located_row['results_json'])
        bad['config']['fixed_params']['maxEntryLeverage'] = bad['config']['entry_admission']['maxEntryLeverage'] = 7
        with storage.get_db_connection() as conn:
            conn.execute('UPDATE manual_tests SET results_json=? WHERE id=?',(json.dumps(bad),historical_id));conn.commit()
        before = storage.load_manual_test_results(study_id,historical_id)
        for dependency in ('target', 'invalid-filter'):
            denied = client.post(f'/api/studies/{study_id}/locate-parquet-pack',json={'packRoot':str(root),'testId':historical_id,'dependency':dependency})
            assert denied.status_code == 400 and 'maxEntryLeverage' in denied.get_data(as_text=True)
        assert client.post(base+'/equity',json=dict(module='manual_tests',testId=historical_id)).status_code == 400
        assert client.post(f'/api/studies/{study_id}/tests/{historical_id}/trials/{number}/mt-trades').status_code == 400
        assert storage.load_manual_test_results(study_id,historical_id) == before
        pinned = saved["study"]["config_json"]
        pinned["data_input_root_override"] = str(tmp_path / "missing")
        storage.update_study_config_json(study_id, pinned)
        offline = client.get(f"/api/studies/{study_id}")
        assert offline.status_code == 200
        assert offline.get_json()["trials"][0]["entry_diagnostics"] == trial["entry_diagnostics"]
        pinned.pop("entry_admission")
        storage.update_study_config_json(study_id, pinned)
        assert client.post(base + "/equity", json={}).status_code == 400
        assert client.get(f"/api/studies/{study_id}").status_code == 200


def test_queue_cap_and_separate_instrument_rules(client, tmp_path):
    sources = [publish(tmp_path/"bybit", rows=100, step="0.1"),
               publish(tmp_path/"okx", rows=100, venue="OKX", step="2", minimum="2")]
    payload = grid_payload()
    payload["fixed_params"]["maxEntryLeverage"] = 2.5
    item = dict(index=1, sources=sources, config=payload, label="Synthetic admission")
    assert client.put("/api/queue", json={"items": [item]}).status_code == 200
    restored = client.get("/api/queue").get_json()["items"][0]
    assert restored["config"]["fixed_params"]["maxEntryLeverage"] == 2.5
    with installed():
        rules = []
        for source in restored["sources"]:
            response = client.post("/api/backtest", data=dict(strategy=STRATEGY_ID, source=json.dumps(source),
                payload=json.dumps(restored["config"]["fixed_params"]), warmupBars="100"))
            assert response.status_code == 200, response.get_data(as_text=True)
            body = response.get_json()
            assert body["parameters"]["maxEntryLeverage"] == 2.5
            rules.append(body["entry_admission"]["rules"])
        assert [r["base_step"] for r in rules] == ["0.1", "0.2"]


@pytest.mark.parametrize("adaptive", [False, True])
def test_new_mode_wfa_diagnostics_replay_analytics(client, tmp_path, adaptive):
    source = publish(tmp_path / "pack", rows=5000 if adaptive else 288 * 100)
    if not adaptive:
        source["timeframe_minutes"] = 60
    config = grid_payload()
    config["postProcess"] = dict(enabled=True, ftPeriodDays=2, topK=2, ftThresholdPct=-100000, ftRejectAction="no_trade")
    fields = dict(strategy=STRATEGY_ID, source=json.dumps(source), config=json.dumps(config), warmupBars="100", wf_store_top_n_trials="2")
    if adaptive:
        fields.update(wf_adaptive_mode="true", wf_is_period_days="5", wf_oos_period_days="3", wf_max_oos_period_days="3",
                      wf_min_oos_trades="1", wf_check_interval_trades="1")
    else:
        fields.update(wf_period_unit="months", wf_is_period_months="1", wf_oos_period_months="1")
    with installed():
        response = client.post("/api/walkforward", data=fields)
        assert response.status_code == 200, response.get_data(as_text=True)
        study_id = response.get_json()["study_id"]
        saved = storage.load_study_from_db(study_id)
        assert saved["stitched_oos"]["entry_diagnostics"] is not None
        for window in saved["windows"]:
            assert set(window["entry_diagnostics"]) == {"is", "oos"}
            details = client.get(f"/api/studies/{study_id}/wfa/windows/{window['window_number']}").get_json()
            assert all(t["module_metrics"]["entry_diagnostics"] is not None for t in details["modules"]["optuna_is"])
        first = saved["windows"][0]["window_number"]
        replay = client.post(f"/api/studies/{study_id}/wfa/windows/{first}/equity", json={"period": "oos"})
        assert replay.status_code == 200, replay.get_data(as_text=True)
        summary = client.get("/api/analytics/summary").get_json()
        study = next(item for item in summary["studies"] if item["study_id"] == study_id)
        assert study["entry_diagnostics"] == saved["stitched_oos"]["entry_diagnostics"]


@pytest.mark.parametrize("reject", [False, True])
def test_adaptive_real_delayed_or_rejected_admission(client, tmp_path, reject):
    source = publish(tmp_path / "pack", rows=5000, turn_day=5)
    config = grid_payload()
    config["postProcess"] = dict(enabled=True, ftPeriodDays=2, topK=2, ftThresholdPct=0,
        ftRejectAction="no_trade" if reject else "cooldown_reoptimize", ftRejectCooldownDays=1,
        ftRejectMaxAttempts=3, ftRejectMinRemainingOosDays=1)
    fields = dict(strategy=STRATEGY_ID, source=json.dumps(source), config=json.dumps(config), warmupBars="100",
        wf_store_top_n_trials="2", wf_adaptive_mode="true", wf_is_period_days="5", wf_oos_period_days="5",
        wf_max_oos_period_days="5", wf_min_oos_trades="1", wf_check_interval_trades="1")
    with installed():
        response = client.post("/api/walkforward", data=fields)
        assert response.status_code == 200, response.get_data(as_text=True)
        saved = storage.load_study_from_db(response.get_json()["study_id"])
    windows = saved["windows"]
    if reject:
        refused = [w for w in windows if w["window_status"] == "no_trade"]
        assert refused
        assert all(w["entry_diagnostics"]["oos"]["maxEntryLeverageUsed"] is None and
                   w["entry_diagnostics"]["oos"]["leverageCapSkippedEntries"] == 0 for w in refused)
    else:
        delayed = [w for w in windows if w["window_status"] == "traded" and w["entry_delay_days"] > 0]
        assert delayed
        assert all(w["entry_diagnostics"]["oos"]["maxEntryLeverageUsed"] > 0 for w in delayed)
