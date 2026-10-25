"""Small diagnostic CSV/Parquet timings; semantic checks, no speed thresholds."""
import json
import time

import pandas as pd
import pytest

from core.data_input import prepare_input
from core import grid_engine, storage, walkforward_engine
from tests._data_input_helpers import publish, grid_payload, STRATEGY


@pytest.mark.parametrize("mode", ["grid", "wfa"])
def test_csv_parquet_preparation_and_compute_timings(client, tmp_path, monkeypatch, mode, record_property):
    from ui import server_routes_run
    from data_provider import data
    source = publish(tmp_path / "pack", rows=5000, triangle=True)
    prepared = prepare_input(source, date_filter=False)
    frame = prepared.frame.copy()
    frame.insert(0, "time", frame.index.asi8 // 10**9)
    csv_path = tmp_path / "synthetic.csv"
    frame.to_csv(csv_path, index=False)
    from core.backtest_engine import load_data
    pd.testing.assert_frame_equal(prepared.frame, load_data(csv_path), check_names=False, check_exact=True)
    measurements = []
    current = {}
    def measure(name, fn):
        def timed(*args, **kwargs):
            started = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                current[name] = current.get(name, 0) + time.perf_counter() - started
                current[name + "_calls"] = current.get(name + "_calls", 0) + 1
        return timed
    monkeypatch.setattr(server_routes_run, "prepare_request", measure("pack_preparation", server_routes_run.prepare_request))
    monkeypatch.setattr(server_routes_run, "load_data", measure("csv_preparation", server_routes_run.load_data))
    monkeypatch.setattr(grid_engine, "load_data", measure("csv_preparation", grid_engine.load_data))
    monkeypatch.setattr(server_routes_run, "run_optimization", measure("engine", server_routes_run.run_optimization))
    monkeypatch.setattr(walkforward_engine.WalkForwardEngine, "run_wf_optimization", measure("engine", walkforward_engine.WalkForwardEngine.run_wf_optimization))
    monkeypatch.setattr(data, "read_ohlcv_rows", measure("bounded_read", data.read_ohlcv_rows))
    for kind in ("csv", "parquet"):
        current = {"source_kind": kind, "mode": mode}
        fields = {"strategy": STRATEGY, "warmupBars": "100", "config": json.dumps(grid_payload())}
        fields.update({"csvPath": str(csv_path)} if kind == "csv" else {"source": json.dumps(source)})
        if mode == "wfa":
            fields.update(wf_is_period_days="5", wf_oos_period_days="3")
        started = time.perf_counter()
        response = client.post("/api/walkforward" if mode == "wfa" else "/api/optimize", data=fields)
        current["request_seconds"] = time.perf_counter() - started
        assert response.status_code == 200, response.get_data(as_text=True)
        saved = storage.load_study_from_db(response.get_json()["study_id"])
        current["persisted_results"] = len(saved["windows"] if mode == "wfa" else saved["trials"])
        if kind == "parquet":
            assert current["bounded_read_calls"] == 1
            assert current.get("csv_preparation_calls", 0) == 0
        measurements.append(current)
    record_property("diagnostic_timings", json.dumps(measurements))
    print("A2_TIMINGS " + json.dumps(measurements))
