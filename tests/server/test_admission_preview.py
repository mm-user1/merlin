"""Grid Preview reads current rules under one session, without candle admission."""
from contextlib import contextmanager
import json
import multiprocessing as mp
from pathlib import Path

import pytest

from core import data_input
from core.grid_engine import preview_grid_parameter_space
from data_provider import data, manifest
from tests._data_input_helpers import hold_pack
from tests.fixtures.admission_strategy import strategy
from tests.fixtures.admission_strategy.bootstrap import installed, publish, grid_payload
from ui import server_data_input, server_routes_run, server_services


def test_preview_metadata_only_and_launch_still_admits_candles(client, tmp_path, monkeypatch):
    source = publish(tmp_path / "pack", rows=100)
    config = {**grid_payload(), "strategy_id": strategy.STRATEGY_ID}
    with installed():
        prepared = data_input.prepare_input(source, date_filter=False, admission_params={})
        expected_config = server_services._build_optimization_config(
            "grid-preview.csv", config, 2, strategy.STRATEGY_ID, 100)
        expected_config.admission_rules = prepared.admission_rules
        expected = preview_grid_parameter_space(expected_config)
        from core.grid_v2 import GridV2Settings, build_grid_v2_plan
        from dataclasses import replace
        settings = GridV2Settings(enabled_axes=("signalEvery",), admission_rules=prepared.admission_rules)
        full_plan = build_grid_v2_plan(strategy.load_config(), settings, config["fixed_params"])
        sessions = []
        original = data.read_session

        @contextmanager
        def observed(root):
            with original(root) as session:
                sessions.append(session)
                yield session

        def forbidden(*args, **kwargs):
            pytest.fail("Preview attempted candle I/O or preparation")

        with monkeypatch.context() as patch:
            patch.setattr(data, "read_session", observed)
            for obj, names in ((data, ("read_ohlcv_rows", "verify_instrument_files", "resample_complete_groups")),
                               (data._ReadSession, ("load_slice",)),
                               (manifest, ("file_sha256",)),
                               (data_input, ("prepare_input",)),
                               (server_data_input, ("prepare_input", "prepare_request")),
                               (server_routes_run, ("prepare_request",)),
                               (strategy, ("build_v2_execution_data",))):
                for name in names:
                    patch.setattr(obj, name, forbidden)
            for body in (dict(json=dict(config=config, source=source, warmupBars=100)),
                         dict(data=dict(config=json.dumps(config), source=json.dumps(source), warmupBars="100"))):
                response = client.post("/api/grid/preview", **body)
                assert response.status_code == 200, response.get_data(as_text=True)
                assert response.get_json() == {"preview": expected}
            metadata_rules = data_input.preview_admission_rules(source)
            assert metadata_rules == prepared.admission_rules
            metadata_plan = build_grid_v2_plan(strategy.load_config(), replace(settings, admission_rules=metadata_rules), config["fixed_params"])
            assert metadata_plan.plan_fingerprint == full_plan.plan_fingerprint
            assert [metadata_plan.candidate_table.semantic_key_for_index(i) for i in range(len(metadata_plan.candidate_table))] == [
                full_plan.candidate_table.semantic_key_for_index(i) for i in range(len(full_plan.candidate_table))]
            assert len(sessions) == 3 and all(not s._active for s in sessions)
        # A manifest can describe a missing candle file. Preview is not data certification.
        root = Path(source["pack_root"])
        entry = manifest.read_manifest(root)["instruments"][0]
        (root / entry["file"]).unlink()
        assert client.post("/api/grid/preview", json=dict(config=config, source=source)).status_code == 200
        launch = client.post("/api/optimize", data=dict(strategy=strategy.STRATEGY_ID,
            config=json.dumps(config), source=json.dumps(source), warmupBars="100"))
        assert launch.status_code == 400, launch.get_data(as_text=True)
        assert Path(entry["file"]).name in launch.get_data(as_text=True)


@pytest.mark.parametrize("problem", ["missing_source", "csv", "version", "relative", "outside", "missing_root",
    "instrument", "timeframe", "missing_rules", "bad_rules", "unsupported_rules", "unreadable", "cap", "date"])
def test_preview_invalid_inputs_are_client_errors(client, tmp_path, monkeypatch, problem):
    source = publish(tmp_path / "pack", rows=100)
    config = {**grid_payload(), "strategy_id": strategy.STRATEGY_ID}
    payload = dict(config=config, source=source)
    root = Path(source["pack_root"])
    document = manifest.read_manifest(root)
    entry = document["instruments"][0]
    if problem == "missing_source": payload.pop("source")
    elif problem == "csv": source["type"] = "csv"
    elif problem == "version": source["version"] = 2
    elif problem == "relative": source["pack_root"] = "relative"
    elif problem == "outside": monkeypatch.setattr(server_data_input, "allowed_roots", lambda: [tmp_path / "approved"])
    elif problem == "missing_root": source["pack_root"] = str(tmp_path / "missing")
    elif problem == "instrument": source["instrument_id"] = "BYBIT_BBBUSDT"
    elif problem == "timeframe": source["timeframe_minutes"] = 7
    elif problem == "cap": config["fixed_params"]["maxEntryLeverage"] = 0
    elif problem == "date": config["fixed_params"].update(dateFilter=True, start="invalid")
    elif problem == "missing_rules": entry.pop("instrument_rules")
    elif problem == "bad_rules": entry["instrument_rules"]["quantity_step"] = "bad"
    elif problem == "unsupported_rules": entry["instrument_rules"]["schema_version"] = 2
    elif problem == "unreadable": entry["verification"]["volume_quote_verified"] = False
    if problem in {"missing_rules", "bad_rules", "unsupported_rules", "unreadable"}:
        # Unmanaged pack metadata allows opaque rule snapshots; Merlin must reject them.
        manifest.write_manifest(root, document)
    with installed():
        response = client.post("/api/grid/preview", json=payload)
    assert response.status_code == 400, response.get_data(as_text=True)
    error = response.get_json()["error"]
    assert "strategy ID" not in error, error
    expected = {"missing_source": "Parquet", "csv": "Parquet", "version": "version",
                "relative": "absolute", "outside": "outside", "instrument": "absent",
                "timeframe": "Timeframe", "missing_rules": "instrument_rules", "bad_rules": "quantity_step",
                "unsupported_rules": "schema_version", "unreadable": "unreadable", "cap": "maxEntryLeverage"}
    if problem in expected:
        assert expected[problem] in error, error


def test_preview_busy_pending_access_and_cancellation(client, tmp_path, monkeypatch):
    source = publish(tmp_path / "pack", rows=100)
    payload = dict(config={**grid_payload(), "strategy_id": strategy.STRATEGY_ID}, source=source)
    context = mp.get_context("spawn")
    ready, release = context.Event(), context.Event()
    process = context.Process(target=hold_pack, args=(source["pack_root"], ready, release))
    with installed():
        process.start()
        try:
            assert ready.wait(20)
            response = client.post("/api/grid/preview", json=payload)
            assert response.status_code == 400 and "busy" in response.get_json()["error"]
        finally:
            release.set()
            process.join(20)
            if process.is_alive():
                process.terminate()
                process.join()
        assert process.exitcode == 0
        # An invalid pending marker is refused by the actual provider inspect path.
        marker = Path(source["pack_root"]) / manifest.UPDATE_MARKER_NAME
        marker.write_text("{}", encoding="utf-8")
        response = client.post("/api/grid/preview", json=payload)
        assert response.status_code == 400 and "journal" in response.get_json()["error"]
        marker.unlink()
        # Also cover inspect's valid-pending report without fabricating market data.
        original = data._ReadSession.inspect
        with monkeypatch.context() as patch:
            def pending(session, **kwargs):
                return {**original(session, **kwargs), "update_in_progress": True}
            patch.setattr(data._ReadSession, "inspect", pending)
            response = client.post("/api/grid/preview", json=payload)
            assert response.status_code == 400 and "recover or abort" in response.get_json()["error"]
        with monkeypatch.context() as patch:
            def denied(*args, **kwargs):
                raise PermissionError("synthetic denied lock access")
            patch.setattr(data, "read_session", denied)
            response = client.post("/api/grid/preview", json=payload)
            assert response.status_code == 400 and "denied lock access" in response.get_json()["error"]
        with data_input.pack_session(source["pack_root"]):
            with pytest.raises(data_input.DataInputError, match="cancelled"):
                data_input.preview_admission_rules(source, cancelled=lambda: True)
        assert client.post("/api/grid/preview", json=payload).status_code == 200


def test_legacy_preview_does_not_resolve_rules(client, monkeypatch):
    from tests._data_input_helpers import grid_payload as legacy_payload, STRATEGY
    monkeypatch.setattr(server_routes_run, "preview_request_rules",
                        lambda *a, **k: pytest.fail("Legacy Preview must not resolve admission rules"))
    response = client.post("/api/grid/preview", json={**legacy_payload(), "strategy_id": STRATEGY})
    assert response.status_code == 200, response.get_data(as_text=True)
