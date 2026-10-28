"""Actual Parquet route execution against isolated synthetic storage."""
import json

import pytest

from core import storage
from tests._data_input_helpers import publish
from strategies import get_strategy_config

STRATEGY = "s03_reversal_v11_regime_er_b2"


from tests._data_input_helpers import grid_payload


def test_start_exposes_default_pack_without_reading_it(client, monkeypatch, tmp_path):
    from ui import server_routes_data
    missing = tmp_path / 'absent-pack'
    monkeypatch.setattr(server_routes_data, 'DEFAULT_PARQUET_ROOT', missing)
    response = client.get('/')
    assert response.status_code == 200
    assert f'data-default-pack="{missing}"' in response.get_data(as_text=True)
    assert not missing.exists()


def test_browser_exposes_existing_instrument_roles(client, tmp_path):
    source = publish(tmp_path / 'roles-pack')
    response = client.get('/api/parquet/browse', query_string={'path': source['pack_root']})
    assert response.status_code == 200
    assert response.get_json()['pack']['instruments'][0]['roles'] == ['trading']


def test_start_backtest_and_pinned_download(client, tmp_path):
    source = publish(tmp_path / "pack")
    params = grid_payload()["fixed_params"]
    fields = {"strategy": STRATEGY, "source": json.dumps(source), "payload": json.dumps(params), "warmupBars": "100"}
    response = client.post("/api/backtest", data=fields)
    assert response.status_code == 200, response.get_data(as_text=True)
    pin = response.get_json()["data_input"]
    assert pin["bounds"]["trade_start_idx"] == 0
    denied = client.post("/api/backtest/trades", data=fields)
    assert denied.status_code == 400
    fields["dataInput"] = json.dumps(pin)
    downloaded = client.post("/api/backtest/trades", data=fields)
    assert downloaded.status_code == 200, downloaded.get_data(as_text=True)


def test_grid_persistence_replay_manual_relocation_and_offline_view(client, tmp_path):
    source = publish(tmp_path / "pack", rows=3000)
    response = client.post("/api/optimize", data={"strategy": STRATEGY, "source": json.dumps(source),
        "config": json.dumps(grid_payload()), "warmupBars": "100"})
    assert response.status_code == 200, response.get_data(as_text=True)
    study_id = response.get_json()["study_id"]
    assert study_id
    saved = storage.load_study_from_db(study_id)
    study = saved["study"]
    assert study["csv_file_path"] == study["csv_file_name"] == ""
    assert "TEST_AAA-USDT-SWAP, 5m" in study["study_name"]
    pin = study["config_json"]["data_input"]
    trial = saved["trials"][0]["trial_number"]
    equity = client.post(f"/api/studies/{study_id}/trials/{trial}/equity", json={"module": "optuna"})
    assert equity.status_code == 200, equity.get_data(as_text=True)
    assert equity.get_json()["equity"]
    manual = client.post(f"/api/studies/{study_id}/test", json={"dataSource": "original_parquet",
        "startDate": "2024-01-03", "endDate": "2024-01-06", "trialNumbers": [trial], "sourceTab": "optuna"})
    assert manual.status_code == 200, manual.get_data(as_text=True)
    test_id = manual.get_json()["test_id"]
    stored_manual = storage.load_manual_test_results(study_id, test_id)
    assert stored_manual["data_source"] == "original_parquet"
    assert "data_input" in stored_manual["results_json"]["config"]
    for endpoint, body in [(f"trials/{trial}/equity", {"module": "manual_tests", "testId": test_id}),
                           (f"tests/{test_id}/trials/{trial}/mt-trades", {})]:
        replay = client.post(f"/api/studies/{study_id}/{endpoint}", json=body)
        assert replay.status_code == 200, replay.get_data(as_text=True)
    replacement = publish(tmp_path / "replacement", rows=3300)
    relocated = client.post(f"/api/studies/{study_id}/locate-parquet-pack", json={"packRoot": replacement["pack_root"]})
    assert relocated.status_code == 200, relocated.get_data(as_text=True)
    actual = storage.load_study_from_db(study_id)["study"]["config_json"]
    assert actual["data_input"] == pin
    assert actual["data_input_root_override"] == replacement["pack_root"]
    manual_relocated = client.post(f"/api/studies/{study_id}/locate-parquet-pack",
                                  json={"packRoot": replacement["pack_root"], "testId": test_id})
    assert manual_relocated.status_code == 200
    assert storage.load_manual_test_results(study_id, test_id)["results_json"]["config"]["data_input_root_override"] == replacement["pack_root"]
    changed = publish(tmp_path / "changed", rows=3300, change=300)
    denied = client.post(f"/api/studies/{study_id}/locate-parquet-pack", json={"packRoot": changed["pack_root"]})
    assert denied.status_code == 400
    actual["data_input_root_override"] = str(tmp_path / "missing-pack")
    storage.update_study_config_json(study_id, actual)
    offline = client.get(f"/api/studies/{study_id}")
    assert offline.status_code == 200
    assert offline.get_json()["study"]["data_source_label"] == "TEST_AAA-USDT-SWAP, 5m"
    assert client.post(f"/api/studies/{study_id}/trials/{trial}/equity", json={}).status_code == 400
    assert client.post(f"/api/studies/{study_id}/trials/{trial}/trades", json={}).status_code == 400
    assert client.post(f"/api/studies/{study_id}/update-csv-path", json={"csvPath": "missing"}).status_code == 400
    actual["data_input"] = None
    storage.update_study_config_json(study_id, actual)
    assert client.get(f"/api/studies/{study_id}").status_code == 200
    assert client.post(f"/api/studies/{study_id}/trials/{trial}/equity", json={}).status_code == 400


def test_queue_preserves_invalid_typed_positions(client, tmp_path):
    invalid = {"type": "parquet", "version": 99, "path": "misleading.csv"}
    unknown = {"type": "future", "path": str(tmp_path / "old.csv")}
    payload = {"items": [{"index": 1, "sources": [invalid, unknown], "sourceCursor": 1,
                          "lastError": "x" * 1200}], "nextIndex": 2}
    response = client.put("/api/queue", json=payload)
    assert response.status_code == 200
    item = client.get("/api/queue").get_json()["items"][0]
    assert item["sources"] == [invalid, unknown] and item["sourceCursor"] == 1
    assert len(item["lastError"]) == 1000


def test_browse_ordinary_directory_and_v1_rejection(client, tmp_path):
    response = client.get("/api/parquet/browse", query_string={"path": str(tmp_path)})
    assert response.status_code == 200
    assert not (tmp_path / ".pack-lock").exists()
    source = publish(tmp_path / "pack")
    pack = client.get("/api/parquet/browse", query_string={"path": source["pack_root"]}).get_json()
    assert pack["entries"] == []  # Internal storage directories are not source choices.
    assert [item["instrument_id"] for item in pack["pack"]["instruments"]] == [source["instrument_id"]]
    rejected = client.post("/api/backtest", data={"strategy": "s01_trailing_ma", "source": json.dumps(source), "payload": "{}"})
    assert rejected.status_code == 400 and "V2" in rejected.get_data(as_text=True)


@pytest.mark.parametrize("adaptive", [False, True])
def test_real_wfa_persists_and_runs_ft(client, tmp_path, adaptive):
    source = publish(tmp_path / "pack", rows=5000 if adaptive else 288 * 110)
    if not adaptive:
        source["timeframe_minutes"] = 60
    config = grid_payload()
    config["postProcess"] = {"enabled": True, "ftPeriodDays": 2, "topK": 2,
                             "ftThresholdPct": -100000, "ftRejectAction": "no_trade"}
    fields = {"strategy": STRATEGY, "source": json.dumps(source), "config": json.dumps(config),
              "warmupBars": "100", "wf_store_top_n_trials": "2"}
    if adaptive:
        fields.update(wf_adaptive_mode="true", wf_is_period_days="5", wf_oos_period_days="3",
                      wf_max_oos_period_days="3", wf_min_oos_trades="1", wf_check_interval_trades="1")
    else:
        fields.update(wf_period_unit="months", wf_is_period_months="1", wf_oos_period_months="1")
    response = client.post("/api/walkforward", data=fields)
    assert response.status_code == 200, response.get_data(as_text=True)
    study_id = response.get_json()["study_id"]
    assert study_id
    saved = storage.load_study_from_db(study_id)
    assert saved["study"]["config_json"]["data_input"]["bounds"]["start_origin"] == "metadata"
    assert saved["windows"]
    for window in saved["windows"]:
        number = window["window_number"]
        details = client.get(f"/api/studies/{study_id}/wfa/windows/{number}").get_json()
        assert details["window"]["module_status"]["forward_test"]["ran"] is True
        assert len(details["modules"]["forward_test"]) == 2
    for path in (f"wfa/windows/{saved['windows'][0]['window_number']}/equity",
                 f"wfa/windows/{saved['windows'][0]['window_number']}/trades", "wfa/trades"):
        replay = client.post(f"/api/studies/{study_id}/{path}", json={"period": "oos"})
        assert replay.status_code == 200, replay.get_data(as_text=True)


def test_grid_postprocess_uses_resolved_union_with_filter_off(client, tmp_path, monkeypatch):
    from data_provider import data
    source = publish(tmp_path / "pack", rows=5000, triangle=True)
    config = grid_payload()
    config["fixed_params"].update(dateFilter=False, start="2030-01-01", end="2031-01-01")
    config["postProcess"] = {"enabled": True, "ftPeriodDays": 3, "topK": 2, "ftThresholdPct": -100000,
                             "stressTest": {"enabled": True, "topK": 2}}
    config["oosTest"] = {"enabled": True, "periodDays": 3, "topK": 2}
    reads = []
    original = data.read_ohlcv_rows
    def read(*args, **kwargs):
        reads.append(kwargs)
        return original(*args, **kwargs)
    monkeypatch.setattr(data, "read_ohlcv_rows", read)
    response = client.post("/api/optimize", data={"strategy": STRATEGY, "source": json.dumps(source),
        "config": json.dumps(config), "warmupBars": "100"})
    assert response.status_code == 200, response.get_data(as_text=True)
    assert len(reads) == 1
    saved = storage.load_study_from_db(response.get_json()["study_id"])
    pin = saved["study"]["config_json"]["data_input"]
    assert pin["bounds"]["start_origin"] == pin["bounds"]["end_origin"] == "metadata"
    assert pin["bounds"]["evaluation_start"].startswith("2024-")
    assert len([t for t in saved["trials"] if t.get("ft_rank")]) == 2
    assert len([t for t in saved["trials"] if t.get("oos_test_source_rank")]) == 2
    trial = saved["trials"][0]["trial_number"]
    for module, trade in [("forward_test", "ft-trades"), ("oos_test", "oos-trades"), ("stress_test", "trades")]:
        base = f"/api/studies/{saved['study']['study_id']}/trials/{trial}"
        response = client.post(base + "/equity", json={"module": module})
        assert response.status_code == 200, response.get_data(as_text=True)
        response = client.post(base + "/" + trade, json={})
        assert response.status_code == 200, response.get_data(as_text=True)


def test_v2_dsr_rejection_is_preserved(client, tmp_path):
    source = publish(tmp_path / "pack")
    config = grid_payload()
    config["postProcess"] = {"dsrEnabled": True}
    response = client.post("/api/optimize", data={"strategy": STRATEGY, "source": json.dumps(source),
        "config": json.dumps(config), "warmupBars": "100"})
    assert response.status_code == 400
    assert "DSR is unavailable" in response.get_data(as_text=True)


def test_default_root_and_resolved_containment(client, tmp_path, monkeypatch):
    from ui import server_data_input, server_services
    source = publish(tmp_path / "approved")
    monkeypatch.setattr(server_services, "CSV_ALLOWED_ROOTS", [])
    monkeypatch.setattr(server_data_input, "DEFAULT_PARQUET_ROOT", tmp_path / "approved")
    assert client.get("/api/parquet/browse").status_code == 200
    escaped = client.get("/api/parquet/browse", query_string={"path": str(tmp_path / "approved" / "..")})
    assert escaped.status_code in (400, 403)
    assert not (tmp_path / ".pack-lock").exists()


def test_documented_direct_server_entrypoint(tmp_path):
    import os
    from pathlib import Path
    import subprocess
    import sys
    repo = Path(__file__).resolve().parents[2]
    script = '''
import sys,runpy
from pathlib import Path
sys.path[:0] = [str(Path(sys.argv[1])/'src'), str(Path(sys.argv[1])/'src/ui')]
from core import storage
scratch=Path(sys.argv[2])
storage.STORAGE_DIR=scratch/'storage'
storage.JOURNAL_DIR=scratch/'journals'
storage._active_db_path=storage.STORAGE_DIR/'test.db'
storage.DB_INITIALIZED=False
from flask import Flask
def checked_run(app, **kwargs):
    import server_services
    server_services.CSV_ALLOWED_ROOTS=[scratch]
    server_services.QUEUE_STORAGE_FILE=scratch/'queue.json'
    with app.test_client() as client:
        response=client.get('/api/parquet/browse',query_string={'path':str(scratch)})
        assert response.status_code == 200, response.get_data(as_text=True)
        assert response.get_json()['pack'] is None
Flask.run=checked_run
runpy.run_path(str(Path(sys.argv[1])/'src/ui/server.py'),run_name='__main__')
'''
    result = subprocess.run([sys.executable, "-B", "-c", script, str(repo), str(tmp_path)],
                            capture_output=True, text=True, timeout=45, env=dict(os.environ))
    assert result.returncode == 0, result.stdout + result.stderr


def test_parquet_symlink_cannot_escape_allowed_root(client, tmp_path, monkeypatch):
    from ui import server_data_input, server_services
    approved = tmp_path / "approved"
    approved.mkdir()
    publish(tmp_path / "outside")
    (approved / "alias").symlink_to(tmp_path / "outside", target_is_directory=True)
    monkeypatch.setattr(server_services, "CSV_ALLOWED_ROOTS", [])
    monkeypatch.setattr(server_data_input, "DEFAULT_PARQUET_ROOT", approved)
    response = client.get("/api/parquet/browse", query_string={"path": str(approved / "alias")})
    assert response.status_code == 403
    assert client.get("/api/parquet/browse").get_json()["entries"] == []
