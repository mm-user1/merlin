import io
import sys
import csv
import json
import uuid
import logging
from contextlib import contextmanager
from pathlib import Path

import pytest
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ui.server import app
from core.backtest_engine import TradeRecord
from core.walkforward_engine import OOSStitchedResult, WFConfig, WFResult, WindowResult
from core.storage import (
    create_new_db,
    delete_study,
    get_active_db_name,
    get_db_connection,
    save_wfa_study_to_db,
    set_active_db,
)
from strategies import get_strategy_config


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client


def test_core_logger_console_handler_is_configured_once():
    from ui import server as server_module

    core_logger = logging.getLogger("core")
    marked_handlers_before = [
        handler for handler in core_logger.handlers if getattr(handler, "_merlin_core_console_handler", False)
    ]

    server_module._configure_core_console_logging()

    marked_handlers_after = [
        handler for handler in core_logger.handlers if getattr(handler, "_merlin_core_console_handler", False)
    ]

    assert marked_handlers_before
    assert len(marked_handlers_after) == len(marked_handlers_before)
    assert core_logger.level <= logging.INFO


@contextmanager
def _temporary_active_db(label: str):
    previous_db = get_active_db_name()
    create_new_db(label)
    try:
        yield
    finally:
        set_active_db(previous_db)


def _insert_analytics_study(
    *,
    study_id: str,
    study_name: str,
    strategy_id: str = "s01_trailing_ma",
    strategy_version: str | None = "2.0",
    optimization_mode: str = "wfa",
    csv_file_name: str | None = "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv",
    adaptive_mode: int | None = 1,
    is_period_days: int | None = 60,
    config_json: dict | None = None,
    dataset_start_date: str | None = "2025-01-01",
    dataset_end_date: str | None = "2025-01-31",
    stitched_oos_net_profit_pct: float | None = 5.0,
    stitched_oos_max_drawdown_pct: float | None = 2.0,
    stitched_oos_total_trades: int | None = 100,
    stitched_oos_winning_trades: int | None = 55,
    best_value: float | None = 1.2,
    profitable_windows: int | None = 3,
    total_windows: int | None = 5,
    stitched_oos_win_rate: float | None = 60.0,
    median_window_profit: float | None = 1.0,
    median_window_wr: float | None = 58.0,
    stitched_oos_equity_curve: list | None = None,
    stitched_oos_timestamps_json: list | None = None,
    created_at: str | None = None,
    completed_at: str | None = None,
):
    config_payload = config_json
    if config_payload is None:
        config_payload = {"wfa": {"oos_period_days": 30}}

    columns = [
        "study_id",
        "study_name",
        "strategy_id",
        "strategy_version",
        "optimization_mode",
        "csv_file_name",
        "adaptive_mode",
        "is_period_days",
        "config_json",
        "dataset_start_date",
        "dataset_end_date",
        "stitched_oos_net_profit_pct",
        "stitched_oos_max_drawdown_pct",
        "stitched_oos_total_trades",
        "stitched_oos_winning_trades",
        "best_value",
        "profitable_windows",
        "total_windows",
        "stitched_oos_win_rate",
        "median_window_profit",
        "median_window_wr",
        "stitched_oos_equity_curve",
        "stitched_oos_timestamps_json",
    ]
    values = [
        study_id,
        study_name,
        strategy_id,
        strategy_version,
        optimization_mode,
        csv_file_name,
        adaptive_mode,
        is_period_days,
        json.dumps(config_payload) if isinstance(config_payload, dict) else config_payload,
        dataset_start_date,
        dataset_end_date,
        stitched_oos_net_profit_pct,
        stitched_oos_max_drawdown_pct,
        stitched_oos_total_trades,
        stitched_oos_winning_trades,
        best_value,
        profitable_windows,
        total_windows,
        stitched_oos_win_rate,
        median_window_profit,
        median_window_wr,
        json.dumps(stitched_oos_equity_curve)
        if isinstance(stitched_oos_equity_curve, list)
        else stitched_oos_equity_curve,
        json.dumps(stitched_oos_timestamps_json)
        if isinstance(stitched_oos_timestamps_json, list)
        else stitched_oos_timestamps_json,
    ]

    if created_at is not None:
        columns.append("created_at")
        values.append(created_at)
    if completed_at is not None:
        columns.append("completed_at")
        values.append(completed_at)

    with get_db_connection() as conn:
        conn.execute(
            f"INSERT INTO studies ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(values))})",
            tuple(values),
        )
        conn.commit()


def _insert_analytics_wfa_window(
    *,
    study_id: str,
    window_number: int,
    window_id: str | None = None,
    oos_start_ts: str | None = None,
    oos_start_date: str | None = None,
    is_end_ts: str | None = None,
    is_end_date: str | None = None,
):
    resolved_window_id = window_id or f"{study_id}_w{window_number}"
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO wfa_windows (
                window_id,
                study_id,
                window_number,
                best_params_json,
                oos_start_ts,
                oos_start_date,
                is_end_ts,
                is_end_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_window_id,
                study_id,
                int(window_number),
                json.dumps({}),
                oos_start_ts,
                oos_start_date,
                is_end_ts,
                is_end_date,
            ),
        )
        conn.commit()


def test_csv_import_s01_parameters(client):
    csv_content = "parameter,value\nmaType,ema\nmaLength,45\n"

    response = client.post(
        "/api/presets/import-csv",
        data={
            "file": (io.BytesIO(csv_content.encode("utf-8")), "params.csv"),
            "strategy": "s01_trailing_ma",
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["values"]["maType"] == "EMA"
    assert payload["values"]["maLength"] == 45


def test_csv_import_s04_parameters(client):
    csv_content = "parameter,value\nrsiLen,16\nstochLen,20\n"

    response = client.post(
        "/api/presets/import-csv",
        data={
            "file": (io.BytesIO(csv_content.encode("utf-8")), "params.csv"),
            "strategy": "s04_stochrsi",
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["values"]["rsiLen"] == 16
    assert payload["values"]["stochLen"] == 20


def test_csv_import_without_strategy_uses_first_available(client, monkeypatch):
    import strategies

    monkeypatch.setattr(
        strategies,
        "list_strategies",
        lambda: [{"id": "s04_stochrsi", "name": "S04 StochRSI"}],
    )

    csv_content = "parameter,value\nrsiLen,16\n"

    response = client.post(
        "/api/presets/import-csv",
        data={"file": (io.BytesIO(csv_content.encode("utf-8")), "params.csv")},
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["values"]["rsiLen"] == 16


def test_csv_import_fails_when_no_strategy_typing(monkeypatch, client):
    import strategies

    # Simulate discovery failure (no strategies available).
    monkeypatch.setattr(strategies, "list_strategies", lambda: [])

    csv_content = "parameter,value\nmaLength,45\n"

    response = client.post(
        "/api/presets/import-csv",
        data={"file": (io.BytesIO(csv_content.encode("utf-8")), "params.csv")},
        content_type="multipart/form-data",
    )

    assert response.status_code == 400
    message = response.get_data(as_text=True).lower()
    assert "parameter types are unavailable" in message
    assert "maLength".lower() in message


def test_csv_import_fails_when_strategy_config_unloadable(monkeypatch, client):
    import strategies

    monkeypatch.setattr(strategies, "get_strategy_config", lambda _sid: (_ for _ in ()).throw(ValueError("boom")))

    csv_content = "parameter,value\nmaLength,45\n"

    response = client.post(
        "/api/presets/import-csv",
        data={
            "file": (io.BytesIO(csv_content.encode("utf-8")), "params.csv"),
            "strategy": "s01_trailing_ma",
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 400
    message = response.get_data(as_text=True)
    assert "s01_trailing_ma" in message
    assert "parameter types are unavailable" in message


def test_csv_import_rejects_invalid_int(client):
    csv_content = "parameter,value\nmaLength,abc\n"

    response = client.post(
        "/api/presets/import-csv",
        data={
            "file": (io.BytesIO(csv_content.encode("utf-8")), "params.csv"),
            "strategy": "s01_trailing_ma",
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert payload["error"] == "Invalid numeric values in CSV."
    assert any("maLength" in detail for detail in payload["details"])


def test_csv_import_rejects_invalid_float(client):
    csv_content = "parameter,value\nstopLongX,abc\n"

    response = client.post(
        "/api/presets/import-csv",
        data={
            "file": (io.BytesIO(csv_content.encode("utf-8")), "params.csv"),
            "strategy": "s01_trailing_ma",
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert payload["error"] == "Invalid numeric values in CSV."
    assert any("stopLongX" in detail for detail in payload["details"])


def test_csv_import_stops_on_mixed_valid_and_invalid_numbers(client):
    csv_content = "parameter,value\nmaLength,abc\nstopLongX,2.5\n"

    response = client.post(
        "/api/presets/import-csv",
        data={
            "file": (io.BytesIO(csv_content.encode("utf-8")), "params.csv"),
            "strategy": "s01_trailing_ma",
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert payload["error"] == "Invalid numeric values in CSV."
    assert any("maLength" in detail for detail in payload["details"])
    # Ensure even valid numeric fields do not get applied when an error is present.
    assert "values" not in payload


def _build_minimal_optuna_payload():
    return {
        "strategy": "s01_trailing_ma",
        "enabled_params": {},
        "param_ranges": {},
        "fixed_params": {},
        "objectives": ["net_profit_pct"],
        "primary_objective": None,
        "optuna_budget_mode": "trials",
        "optuna_n_trials": 10,
        "optuna_time_limit": 60,
        "optuna_convergence": 10,
    }


def test_optuna_sanitize_defaults():
    from ui import server as server_module

    payload = _build_minimal_optuna_payload()
    config = server_module._build_optimization_config(
        "dummy.csv",
        payload,
        worker_processes=1,
        strategy_id="s01_trailing_ma",
        warmup_bars=1000,
    )
    assert config.sanitize_enabled is True
    assert config.sanitize_trades_threshold == 0


def test_optuna_coverage_mode_parsed():
    from ui import server as server_module

    payload = _build_minimal_optuna_payload()
    payload["coverage_mode"] = True
    config = server_module._build_optimization_config(
        "dummy.csv",
        payload,
        worker_processes=1,
        strategy_id="s01_trailing_ma",
        warmup_bars=1000,
    )
    assert config.coverage_mode is True


def test_optuna_trials_log_parsed():
    from ui import server as server_module

    payload = _build_minimal_optuna_payload()
    payload["trials_log"] = True
    config = server_module._build_optimization_config(
        "dummy.csv",
        payload,
        worker_processes=1,
        strategy_id="s01_trailing_ma",
        warmup_bars=1000,
    )
    assert config.trials_log is True


def test_optuna_dispatcher_controls_parsed():
    from ui import server as server_module

    payload = _build_minimal_optuna_payload()
    payload["dispatcher_batch_result_processing"] = False
    payload["dispatcher_soft_duplicate_cycle_limit_enabled"] = False
    payload["dispatcher_duplicate_cycle_limit"] = 42

    config = server_module._build_optimization_config(
        "dummy.csv",
        payload,
        worker_processes=1,
        strategy_id="s01_trailing_ma",
        warmup_bars=1000,
    )

    assert config.dispatcher_batch_result_processing is False
    assert config.dispatcher_soft_duplicate_cycle_limit_enabled is False
    assert config.dispatcher_duplicate_cycle_limit == 42


def test_optuna_save_study_payload_is_ignored(caplog):
    from ui import server as server_module

    payload = _build_minimal_optuna_payload()
    payload["optuna_save_study"] = True

    with caplog.at_level("WARNING"):
        config = server_module._build_optimization_config(
            "dummy.csv",
            payload,
            worker_processes=1,
            strategy_id="s01_trailing_ma",
            warmup_bars=1000,
        )

    assert not hasattr(config, "optuna_save_study")
    assert any("optuna_save_study" in record.message for record in caplog.records)


def test_optuna_score_config_migrates_legacy_consistency_bounds():
    from ui import server as server_module

    payload = _build_minimal_optuna_payload()
    payload["score_config"] = {
        "enabled_metrics": {"consistency": True},
        "weights": {"consistency": 1.0},
        "metric_bounds": {"consistency": {"min": 0.0, "max": 100.0}},
    }

    config = server_module._build_optimization_config(
        "dummy.csv",
        payload,
        worker_processes=1,
        strategy_id="s01_trailing_ma",
        warmup_bars=1000,
    )

    assert config.score_config["metric_bounds"]["consistency"] == {"min": -1.0, "max": 1.0}


def _ensure_local_test_tmp_dir() -> Path:
    path = Path(__file__).parent / ".tmp_server_cancel"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _ensure_local_queue_tmp_dir() -> Path:
    path = Path(__file__).parent / ".tmp_server_queue"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _patch_queue_storage_path(monkeypatch, filename: str) -> Path:
    from ui import server_services

    queue_file = _ensure_local_queue_tmp_dir() / filename
    if queue_file.exists():
        queue_file.unlink()

    monkeypatch.setattr(
        server_services,
        "_queue_storage_file_path",
        lambda: queue_file,
    )
    return queue_file


def test_queue_api_roundtrip_persists_in_file_storage(client, monkeypatch):
    queue_file = _patch_queue_storage_path(monkeypatch, "queue_roundtrip.json")

    payload = {
        "items": [
            {
                "id": "q_test_1",
                "index": 1,
                "label": "#1 example",
                "sources": [{"type": "path", "path": r"C:\data\file_1.csv"}],
                "sourceCursor": 0,
                "successCount": 0,
                "failureCount": 0,
            }
        ],
        "nextIndex": 2,
        "runtime": {"active": False, "updatedAt": 0},
    }

    response_put = client.put("/api/queue", json=payload)
    assert response_put.status_code == 200
    put_data = response_put.get_json()
    assert put_data["nextIndex"] == 2
    assert len(put_data["items"]) == 1
    assert queue_file.exists()

    response_get = client.get("/api/queue")
    assert response_get.status_code == 200
    get_data = response_get.get_json()
    assert len(get_data["items"]) == 1
    assert get_data["items"][0]["id"] == "q_test_1"
    assert get_data["items"][0]["sources"][0]["path"] == r"C:\data\file_1.csv"

    response_delete = client.delete("/api/queue")
    assert response_delete.status_code == 200
    delete_data = response_delete.get_json()
    assert delete_data["items"] == []
    assert delete_data["nextIndex"] == 1
    assert delete_data["runtime"]["active"] is False
    assert not queue_file.exists()


def test_queue_api_empty_items_removes_queue_file(client, monkeypatch):
    queue_file = _patch_queue_storage_path(monkeypatch, "queue_empty_cleanup.json")

    seed_payload = {
        "items": [
            {
                "id": "q_test_2",
                "index": 1,
                "label": "#1 seed",
                "sources": [{"type": "path", "path": r"C:\data\seed.csv"}],
            }
        ],
        "nextIndex": 2,
        "runtime": {"active": True, "updatedAt": 123},
    }

    response_seed = client.put("/api/queue", json=seed_payload)
    assert response_seed.status_code == 200
    assert queue_file.exists()

    response_clear = client.put(
        "/api/queue",
        json={
            "items": [],
            "nextIndex": 999,
            "runtime": {"active": True, "updatedAt": 999},
        },
    )
    assert response_clear.status_code == 200
    clear_data = response_clear.get_json()
    assert clear_data["items"] == []
    assert clear_data["nextIndex"] == 1
    assert clear_data["runtime"]["active"] is False
    assert clear_data["runtime"]["updatedAt"] == 0
    assert not queue_file.exists()


def test_queue_api_rejects_non_object_payload(client):
    response = client.put("/api/queue", json=["not", "an", "object"])
    assert response.status_code == 400
    payload = response.get_json()
    assert "json object" in payload["error"].lower()


def test_optimize_cancelled_run_cleans_up_saved_study(client, monkeypatch):
    from ui import server_routes_run

    csv_path = _ensure_local_test_tmp_dir() / "opt_cancel.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-01-01 00:00:00,1,1,1,1,1\n",
        encoding="utf-8",
    )

    deleted_studies = []

    monkeypatch.setattr(server_routes_run, "_resolve_csv_path", lambda _raw: csv_path)
    monkeypatch.setattr(server_routes_run, "run_optimization", lambda _cfg: ([], "study_cancel_opt"))
    monkeypatch.setattr(server_routes_run, "_is_run_cancelled", lambda run_id: run_id == "run_cancel_opt")
    monkeypatch.setattr(
        server_routes_run,
        "delete_study",
        lambda study_id: deleted_studies.append(study_id) or True,
    )

    payload = _build_minimal_optuna_payload()
    payload["primary_objective"] = "net_profit_pct"

    response = client.post(
        "/api/optimize",
        data={
            "strategy": "s01_trailing_ma",
            "csvPath": str(csv_path),
            "runId": "run_cancel_opt",
            "config": json.dumps(payload),
        },
    )

    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "cancelled"
    assert data["run_id"] == "run_cancel_opt"
    assert data["study_id"] is None
    assert deleted_studies == ["study_cancel_opt"]


def test_walkforward_cancelled_run_cleans_up_saved_study(client, monkeypatch):
    from ui import server_routes_run
    import core.walkforward_engine as walkforward_engine

    csv_path = _ensure_local_test_tmp_dir() / "wfa_cancel.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-01-01 00:00:00,1,1,1,1,1\n",
        encoding="utf-8",
    )

    df = pd.DataFrame(
        {
            "open": [1.0, 1.1, 1.2],
            "high": [1.0, 1.1, 1.2],
            "low": [1.0, 1.1, 1.2],
            "close": [1.0, 1.1, 1.2],
            "volume": [1.0, 1.0, 1.0],
        },
        index=pd.to_datetime(
            ["2026-01-01 00:00:00", "2026-01-01 01:00:00", "2026-01-01 02:00:00"],
            utc=True,
        ),
    )

    class DummyWalkForwardEngine:
        def __init__(self, *_args, **_kwargs):
            pass

        def run_wf_optimization(self, _dataframe):
            return None, "study_cancel_wfa"

    deleted_studies = []
    monkeypatch.setattr(server_routes_run, "_resolve_csv_path", lambda _raw: csv_path)
    monkeypatch.setattr(server_routes_run, "load_data", lambda _path: df)
    monkeypatch.setattr(server_routes_run, "_is_run_cancelled", lambda run_id: run_id == "run_cancel_wfa")
    monkeypatch.setattr(
        server_routes_run,
        "delete_study",
        lambda study_id: deleted_studies.append(study_id) or True,
    )
    monkeypatch.setattr(walkforward_engine, "WalkForwardEngine", DummyWalkForwardEngine)

    payload = _build_minimal_optuna_payload()
    payload["primary_objective"] = "net_profit_pct"

    response = client.post(
        "/api/walkforward",
        data={
            "strategy": "s01_trailing_ma",
            "csvPath": str(csv_path),
            "runId": "run_cancel_wfa",
            "config": json.dumps(payload),
        },
    )

    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "cancelled"
    assert data["run_id"] == "run_cancel_wfa"
    assert data["study_id"] is None
    assert deleted_studies == ["study_cancel_wfa"]


def _build_params_from_config(strategy_id: str):
    config = get_strategy_config(strategy_id)
    parameters = config.get("parameters", {}) if isinstance(config, dict) else {}
    params = {}
    for name, spec in parameters.items():
        if not isinstance(spec, dict):
            continue
        default_value = spec.get("default")
        params[name] = default_value if default_value is not None else 0
    return params


def _create_wfa_study() -> str:
    data_path = Path(__file__).parent.parent / "data" / "raw" / "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"
    if not data_path.exists():
        pytest.skip("Sample data file not available for WFA API tests.")

    strategy_id = "s01_trailing_ma"
    params = _build_params_from_config(strategy_id)
    wf_config = WFConfig(strategy_id=strategy_id, is_period_days=30, oos_period_days=15, warmup_bars=10)

    window = WindowResult(
        window_id=1,
        is_start=pd.Timestamp("2025-05-01", tz="UTC"),
        is_end=pd.Timestamp("2025-05-30", tz="UTC"),
        oos_start=pd.Timestamp("2025-05-31", tz="UTC"),
        oos_end=pd.Timestamp("2025-06-14", tz="UTC"),
        best_params=params,
        param_id="test_params",
        is_net_profit_pct=0.0,
        is_max_drawdown_pct=0.0,
        is_total_trades=0,
        oos_net_profit_pct=0.0,
        oos_max_drawdown_pct=0.0,
        oos_total_trades=0,
        oos_equity_curve=[100.0],
        oos_timestamps=[pd.Timestamp("2025-05-31", tz="UTC")],
        is_equity_curve=[100.0],
        is_timestamps=[pd.Timestamp("2025-05-01", tz="UTC")],
        best_params_source="optuna_is",
        available_modules=["optuna_is"],
        optuna_is_trials=[
            {
                "trial_number": 1,
                "params": params,
                "param_id": "test_params",
                "net_profit_pct": 0.0,
                "max_drawdown_pct": 0.0,
                "total_trades": 0,
                "win_rate": 0.0,
                "is_selected": True,
            }
        ],
    )

    stitched = OOSStitchedResult(
        final_net_profit_pct=0.0,
        max_drawdown_pct=0.0,
        total_trades=0,
        wfe=0.0,
        oos_win_rate=0.0,
        equity_curve=[100.0],
        timestamps=[pd.Timestamp("2025-05-31", tz="UTC")],
        window_ids=[1],
    )

    wf_result = WFResult(
        config=wf_config,
        windows=[window],
        stitched_oos=stitched,
        strategy_id=strategy_id,
        total_windows=1,
        trading_start_date=window.is_start,
        trading_end_date=window.oos_end,
        warmup_bars=wf_config.warmup_bars,
    )

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={"fixed_params": {}},
        csv_file_path=str(data_path),
        start_time=0.0,
        score_config=None,
    )
    return study_id


def test_get_wfa_window_details(client):
    study_id = _create_wfa_study()
    response = client.get(f"/api/studies/{study_id}/wfa/windows/1")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["window"]["window_number"] == 1
    assert payload["window"]["oos_actual_days"] is None
    assert payload["window"]["trigger_type"] is None
    assert "optuna_is" in payload["modules"]


def test_generate_wfa_window_equity(client):
    study_id = _create_wfa_study()
    response = client.post(
        f"/api/studies/{study_id}/wfa/windows/1/equity",
        json={"period": "is"},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert "equity_curve" in payload


def test_download_wfa_window_trades(client):
    study_id = _create_wfa_study()
    response = client.post(
        f"/api/studies/{study_id}/wfa/windows/1/trades",
        json={"period": "oos"},
    )
    assert response.status_code == 200
    assert response.headers.get("Content-Type", "").startswith("text/csv")


def test_resolve_wfa_period_oos_prefers_precise_timestamp():
    from ui.server_services import _resolve_wfa_period

    window = {
        "oos_start_date": "2025-01-01",
        "oos_end_date": "2025-01-02",
        "oos_start_ts": "2025-01-01T00:00:00+00:00",
        "oos_end_ts": "2025-01-02T12:00:00+00:00",
    }
    start, end, error = _resolve_wfa_period(window, "oos")
    assert error is None
    assert start == "2025-01-01T00:00:00+00:00"
    assert end == "2025-01-02T12:00:00+00:00"

    # Backward compatibility: when ts fields are missing, date fields are used.
    legacy_window = {
        "oos_start_date": "2025-01-01",
        "oos_end_date": "2025-01-02",
    }
    legacy_start, legacy_end, legacy_error = _resolve_wfa_period(legacy_window, "oos")
    assert legacy_error is None
    assert legacy_start == "2025-01-01"
    assert legacy_end == "2025-01-02"


def test_download_wfa_window_trades_respects_stored_oos_trade_count(client, monkeypatch):
    import ui.server_routes_data as routes_data

    csv_path = Path(__file__).parent / "_tmp_wfa_window_trades.csv"
    csv_path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")

    try:
        study_id = "wfa_window_count_limit"
        study_data = {
            "study": {
                "study_id": study_id,
                "study_name": "wfa_window_count_limit",
                "optimization_mode": "wfa",
                "adaptive_mode": 1,
                "strategy_id": "s01_trailing_ma",
                "csv_file_path": str(csv_path),
                "csv_file_name": "OKX_TESTUSDT.csv",
                "warmup_bars": 0,
                "config_json": {"fixed_params": {}},
            },
            "windows": [
                {
                    "window_number": 1,
                    "window_id": f"{study_id}_w1",
                    "best_params": {},
                    "oos_start_ts": "2025-01-01T00:00:00+00:00",
                    "oos_end_ts": "2025-01-02T23:00:00+00:00",
                    "oos_start_date": "2025-01-01",
                    "oos_end_date": "2025-01-02",
                    "oos_total_trades": 1,
                }
            ],
        }

        monkeypatch.setattr(
            routes_data,
            "load_study_from_db",
            lambda sid: study_data if sid == study_id else None,
        )

        fake_trades = [
            TradeRecord(
                direction="long",
                entry_time=pd.Timestamp("2025-01-02 10:00:00+00:00"),
                exit_time=pd.Timestamp("2025-01-02 10:30:00+00:00"),
                entry_price=100.0,
                exit_price=101.0,
                size=1.0,
            ),
            TradeRecord(
                direction="long",
                entry_time=pd.Timestamp("2025-01-02 20:00:00+00:00"),
                exit_time=pd.Timestamp("2025-01-02 20:30:00+00:00"),
                entry_price=100.0,
                exit_price=99.0,
                size=1.0,
            ),
        ]

        monkeypatch.setattr(
            routes_data,
            "_run_trade_export",
            lambda **_kwargs: (fake_trades, None),
        )

        response = client.post(
            f"/api/studies/{study_id}/wfa/windows/1/trades",
            json={"period": "oos"},
        )
        assert response.status_code == 200

        rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
        # Header + 2 rows (entry/exit) for exactly one trade after cap.
        assert len(rows) == 3
    finally:
        if csv_path.exists():
            csv_path.unlink()


def test_download_wfa_trades_uses_precise_oos_bounds(client, monkeypatch):
    import strategies
    import ui.server_routes_data as routes_data

    csv_path = Path(__file__).parent / "_tmp_wfa_precise_bounds.csv"
    csv_path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")

    try:
        study_id = "wfa_precise_bounds"
        study_data = {
            "study": {
                "study_id": study_id,
                "study_name": "wfa_precise_bounds",
                "optimization_mode": "wfa",
                "adaptive_mode": 1,
                "strategy_id": "s01_trailing_ma",
                "csv_file_path": str(csv_path),
                "csv_file_name": "OKX_TESTUSDT.csv",
                "warmup_bars": 0,
                "config_json": {"fixed_params": {}},
            },
            "windows": [
                {
                    "window_number": 1,
                    "best_params": {},
                    "oos_start_date": "2025-01-01",
                    "oos_end_date": "2025-01-02",
                    "oos_start_ts": "2025-01-01T00:00:00+00:00",
                    "oos_end_ts": "2025-01-02T12:00:00+00:00",
                    "oos_total_trades": 1,
                }
            ],
        }

        monkeypatch.setattr(
            routes_data,
            "load_study_from_db",
            lambda sid: study_data if sid == study_id else None,
        )

        index = pd.date_range("2025-01-01 00:00:00+00:00", periods=72, freq="h")
        df = pd.DataFrame(
            {
                "Open": 1.0,
                "High": 1.0,
                "Low": 1.0,
                "Close": 1.0,
                "Volume": 1.0,
            },
            index=index,
        )
        monkeypatch.setattr(routes_data, "load_data", lambda _: df)
        monkeypatch.setattr(routes_data, "prepare_dataset_with_warmup", lambda data, *_: (data, 0))

        class FakeResult:
            def __init__(self, trades):
                self.trades = trades

        class FakeStrategy:
            @staticmethod
            def run(*_args, **_kwargs):
                return FakeResult(
                    [
                        TradeRecord(
                            direction="long",
                            entry_time=pd.Timestamp("2025-01-02 10:00:00+00:00"),
                            exit_time=pd.Timestamp("2025-01-02 10:30:00+00:00"),
                            entry_price=100.0,
                            exit_price=101.0,
                            size=1.0,
                        ),
                        TradeRecord(
                            direction="long",
                            entry_time=pd.Timestamp("2025-01-02 20:00:00+00:00"),
                            exit_time=pd.Timestamp("2025-01-02 20:30:00+00:00"),
                            entry_price=100.0,
                            exit_price=99.0,
                            size=1.0,
                        ),
                    ]
                )

        monkeypatch.setattr(strategies, "get_strategy", lambda _sid: FakeStrategy)

        response = client.post(f"/api/studies/{study_id}/wfa/trades")
        assert response.status_code == 200

        rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
        # Header + 2 rows (entry/exit) for exactly one trade within precise OOS end.
        assert len(rows) == 3
    finally:
        if csv_path.exists():
            csv_path.unlink()


def test_analytics_page_renders(client):
    response = client.get("/analytics")
    assert response.status_code == 200
    assert "Analytics" in response.get_data(as_text=True)


def test_analytics_window_boundaries_endpoint_success(client):
    with _temporary_active_db(f"analytics_boundaries_{uuid.uuid4().hex[:8]}"):
        study_id = "wfa_boundaries_1"
        _insert_analytics_study(
            study_id=study_id,
            study_name="WFA_BOUNDARIES_1",
            optimization_mode="wfa",
        )
        _insert_analytics_wfa_window(
            study_id=study_id,
            window_number=1,
            oos_start_date="2025-01-15",
        )
        _insert_analytics_wfa_window(
            study_id=study_id,
            window_number=2,
            oos_start_ts="2025-02-01T00:00:00+00:00",
        )
        _insert_analytics_wfa_window(
            study_id=study_id,
            window_number=3,
            is_end_date="2025-03-20",
        )
        _insert_analytics_wfa_window(
            study_id=study_id,
            window_number=4,
        )

        response = client.get(f"/api/analytics/studies/{study_id}/window-boundaries")
        assert response.status_code == 200
        payload = response.get_json()

        assert payload["study_id"] == study_id
        assert payload["boundaries"] == [
            {
                "window_id": f"{study_id}_w1",
                "window_number": 1,
                "time": "2025-01-15",
                "label": "W1",
            },
            {
                "window_id": f"{study_id}_w2",
                "window_number": 2,
                "time": "2025-02-01T00:00:00+00:00",
                "label": "W2",
            },
            {
                "window_id": f"{study_id}_w3",
                "window_number": 3,
                "time": "2025-03-20",
                "label": "W3",
            },
        ]


def test_analytics_window_boundaries_endpoint_rejects_non_wfa(client):
    with _temporary_active_db(f"analytics_boundaries_non_wfa_{uuid.uuid4().hex[:8]}"):
        study_id = "optuna_boundaries_1"
        _insert_analytics_study(
            study_id=study_id,
            study_name="OPTUNA_BOUNDARIES_1",
            optimization_mode="optuna",
        )

        response = client.get(f"/api/analytics/studies/{study_id}/window-boundaries")
        assert response.status_code == 400
        assert "only for WFA studies" in response.get_json()["error"]


def test_analytics_window_boundaries_endpoint_returns_404_for_missing_study(client):
    response = client.get("/api/analytics/studies/missing_study/window-boundaries")
    assert response.status_code == 404
    assert response.get_json()["error"] == "Study not found."


def test_analytics_equity_endpoint_success(client):
    with _temporary_active_db(f"analytics_equity_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_eq_1",
            study_name="WFA_EQ_1",
            optimization_mode="wfa",
            stitched_oos_equity_curve=[100.0, 120.0],
            stitched_oos_timestamps_json=[
                "2025-01-01T00:00:00+00:00",
                "2025-02-15T00:00:00+00:00",
            ],
        )
        _insert_analytics_study(
            study_id="wfa_eq_2",
            study_name="WFA_EQ_2",
            optimization_mode="wfa",
            stitched_oos_equity_curve=[100.0, 80.0],
            stitched_oos_timestamps_json=[
                "2025-01-01T00:00:00+00:00",
                "2025-02-15T00:00:00+00:00",
            ],
        )

        response = client.post(
            "/api/analytics/equity",
            json={"study_ids": ["wfa_eq_1", "wfa_eq_2"]},
        )
        assert response.status_code == 200
        payload = response.get_json()

        assert isinstance(payload["curve"], list)
        assert len(payload["curve"]) == len(payload["timestamps"])
        assert payload["studies_used"] == 2
        assert payload["selected_count"] == 2
        assert payload["missing_study_ids"] == []
        assert payload["profit_pct"] == pytest.approx(0.0, abs=1e-6)


def test_analytics_equity_endpoint_rejects_missing_payload(client):
    response = client.post("/api/analytics/equity")
    assert response.status_code == 400
    assert "Expected JSON payload." in response.get_json()["error"]


def test_analytics_equity_endpoint_rejects_non_array_study_ids(client):
    response = client.post(
        "/api/analytics/equity",
        json={"study_ids": "not-array"},
    )
    assert response.status_code == 400
    assert "study_ids must be an array." in response.get_json()["error"]


def test_analytics_equity_endpoint_rejects_empty_study_ids(client):
    response = client.post(
        "/api/analytics/equity",
        json={"study_ids": []},
    )
    assert response.status_code == 400
    assert "non-empty array" in response.get_json()["error"]


def test_analytics_equity_endpoint_rejects_study_ids_over_cap(client):
    response = client.post(
        "/api/analytics/equity",
        json={"study_ids": [f"id_{i}" for i in range(501)]},
    )
    assert response.status_code == 400
    assert "Maximum allowed is 500" in response.get_json()["error"]


def test_analytics_equity_endpoint_no_overlap_returns_warning(client):
    with _temporary_active_db(f"analytics_equity_no_overlap_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_eq_no_1",
            study_name="WFA_EQ_NO_1",
            optimization_mode="wfa",
            stitched_oos_equity_curve=[100.0, 110.0],
            stitched_oos_timestamps_json=[
                "2025-01-01T00:00:00+00:00",
                "2025-01-02T00:00:00+00:00",
            ],
        )
        _insert_analytics_study(
            study_id="wfa_eq_no_2",
            study_name="WFA_EQ_NO_2",
            optimization_mode="wfa",
            stitched_oos_equity_curve=[100.0, 95.0],
            stitched_oos_timestamps_json=[
                "2025-01-10T00:00:00+00:00",
                "2025-01-11T00:00:00+00:00",
            ],
        )

        response = client.post(
            "/api/analytics/equity",
            json={"study_ids": ["wfa_eq_no_1", "wfa_eq_no_2"]},
        )
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["curve"] is None
        assert payload["warning"] == "Selected studies have no overlapping time period."


def test_analytics_equity_batch_endpoint_success(client):
    with _temporary_active_db(f"analytics_equity_batch_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_eq_b1",
            study_name="WFA_EQ_B1",
            optimization_mode="wfa",
            stitched_oos_equity_curve=[100.0, 105.0],
            stitched_oos_timestamps_json=[
                "2025-01-01T00:00:00+00:00",
                "2025-02-10T00:00:00+00:00",
            ],
        )
        _insert_analytics_study(
            study_id="wfa_eq_b2",
            study_name="WFA_EQ_B2",
            optimization_mode="wfa",
            stitched_oos_equity_curve=[100.0, 95.0],
            stitched_oos_timestamps_json=[
                "2025-01-01T00:00:00+00:00",
                "2025-02-10T00:00:00+00:00",
            ],
        )

        response = client.post(
            "/api/analytics/equity/batch",
            json={
                "groups": [
                    {"group_id": "all", "study_ids": ["wfa_eq_b1", "wfa_eq_b2"]},
                    {"group_id": "subset", "study_ids": ["wfa_eq_b1"]},
                    {"group_id": "empty", "study_ids": []},
                ]
            },
        )
        assert response.status_code == 200
        payload = response.get_json()
        assert isinstance(payload["results"], list)
        by_id = {item["group_id"]: item for item in payload["results"]}
        assert set(by_id.keys()) == {"all", "subset", "empty"}
        assert by_id["all"]["studies_used"] == 2
        assert by_id["subset"]["studies_used"] == 1
        assert by_id["empty"]["curve"] is None


def test_analytics_summary_empty_db_returns_expected_message(client):
    with _temporary_active_db(f"analytics_empty_{uuid.uuid4().hex[:8]}"):
        response = client.get("/api/analytics/summary")
        assert response.status_code == 200
        payload = response.get_json()

        assert payload["studies"] == []
        assert payload["db_name"] == get_active_db_name()
        assert payload["research_info"]["total_studies"] == 0
        assert payload["research_info"]["wfa_studies"] == 0
        assert payload["research_info"]["message"] == "No WFA studies found in this database."


def test_analytics_summary_optuna_only_returns_expected_message(client):
    with _temporary_active_db(f"analytics_optuna_only_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="optuna_only_1",
            study_name="OPTUNA_ONLY_1",
            optimization_mode="optuna",
            config_json={},
        )

        response = client.get("/api/analytics/summary")
        assert response.status_code == 200
        payload = response.get_json()

        assert payload["studies"] == []
        assert payload["research_info"]["total_studies"] == 1
        assert payload["research_info"]["wfa_studies"] == 0
        assert payload["research_info"]["message"] == (
            "Analytics requires WFA studies. This database contains only Optuna studies."
        )


def test_analytics_summary_wfa_phase1_contract(client):
    with _temporary_active_db(f"analytics_wfa_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_a1",
            study_name="WFA_A1",
            strategy_id="s01_trailing_ma",
            strategy_version="2.1",
            optimization_mode="wfa",
            csv_file_name="OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv",
            adaptive_mode=1,
            is_period_days=60,
            config_json={"wfa": {"oos_period_days": 30}},
            dataset_start_date="2025-01-01",
            dataset_end_date="2025-01-31",
            stitched_oos_net_profit_pct=10.0,
            stitched_oos_max_drawdown_pct=4.0,
            stitched_oos_total_trades=120,
            stitched_oos_winning_trades=67,
            best_value=45.0,
            profitable_windows=4,
            total_windows=5,
            stitched_oos_win_rate=80.0,
            median_window_profit=2.0,
            median_window_wr=55.0,
            stitched_oos_equity_curve=[100.0, 101.5, 110.0],
            stitched_oos_timestamps_json=[
                "2025-01-01T00:00:00+00:00",
                "2025-01-10T00:00:00+00:00",
                "2025-01-31T00:00:00+00:00",
            ],
        )
        _insert_analytics_study(
            study_id="wfa_a2",
            study_name="WFA_A2",
            strategy_id="s02_breakout",
            strategy_version="v3.0",
            optimization_mode="wfa",
            csv_file_name="OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv",
            adaptive_mode=0,
            is_period_days=None,
            config_json={"wfa": {"oos_period_days": 30}},
            dataset_start_date="2025-01-01",
            dataset_end_date="2025-01-31",
            stitched_oos_net_profit_pct=-3.0,
            stitched_oos_max_drawdown_pct=2.5,
            stitched_oos_total_trades=80,
            stitched_oos_winning_trades=35,
            best_value=20.0,
            profitable_windows=1,
            total_windows=5,
            stitched_oos_win_rate=20.0,
            median_window_profit=-0.5,
            median_window_wr=48.0,
            stitched_oos_equity_curve=[100.0, 99.2],
            stitched_oos_timestamps_json=["2025-01-01T00:00:00+00:00"],
        )
        _insert_analytics_study(
            study_id="wfa_b1",
            study_name="WFA_B1",
            strategy_id="custom_strategy",
            strategy_version=None,
            optimization_mode="WFA",
            csv_file_name="OKX_BTCUSDT.P, 1h 2025.05.01-2025.11.20.csv",
            adaptive_mode=None,
            is_period_days=None,
            config_json={},
            dataset_start_date="2025-02-01",
            dataset_end_date="2025-02-28",
            stitched_oos_net_profit_pct=4.5,
            stitched_oos_max_drawdown_pct=1.1,
            stitched_oos_total_trades=40,
            stitched_oos_winning_trades=20,
            best_value=None,
            profitable_windows=0,
            total_windows=0,
            stitched_oos_win_rate=None,
            median_window_profit=0.0,
            median_window_wr=None,
            stitched_oos_equity_curve=None,
            stitched_oos_timestamps_json=None,
        )
        _insert_analytics_study(
            study_id="optuna_aux",
            study_name="OPTUNA_AUX",
            optimization_mode="optuna",
            config_json={},
        )

        response = client.get("/api/analytics/summary")
        assert response.status_code == 200
        payload = response.get_json()

        assert payload["db_name"] == get_active_db_name()
        assert payload["research_info"]["total_studies"] == 4
        assert payload["research_info"]["wfa_studies"] == 3
        assert "message" not in payload["research_info"]

        studies = payload["studies"]
        assert [row["study_id"] for row in studies] == ["wfa_a1", "wfa_a2", "wfa_b1"]

        first = studies[0]
        assert first["strategy"] == "S01 v2.1"
        assert first["symbol"] == "LINKUSDT.P"
        assert first["tf"] == "15m"
        assert first["wfa_mode"] == "Adaptive"
        assert first["is_oos"] == "60/30"
        assert first["has_equity_curve"] is True
        assert len(first["equity_curve"]) == 3
        assert len(first["equity_timestamps"]) == 3

        second = studies[1]
        assert second["strategy"] == "S02 v3.0"
        assert second["wfa_mode"] == "Fixed"
        assert second["is_oos"] == "?/30"
        assert second["has_equity_curve"] is False
        assert second["equity_curve"] == []
        assert second["equity_timestamps"] == []

        third = studies[2]
        assert third["strategy"] == "custom_strategy"
        assert third["symbol"] == "BTCUSDT.P"
        assert third["tf"] == "1h"
        assert third["wfa_mode"] == "Unknown"
        assert third["is_oos"] == "N/A"

        info = payload["research_info"]
        assert info["strategies"] == ["S01 v2.1", "S02 v3.0", "custom_strategy"]
        assert info["symbols"] == ["BTCUSDT.P", "LINKUSDT.P"]
        assert info["timeframes"] == ["15m", "1h"]
        assert info["wfa_modes"] == ["Fixed", "Adaptive", "Unknown"]
        assert info["is_oos_periods"] == ["60/30", "?/30", "N/A"]
        assert info["data_periods"] == [
            {"start": "2025-01-01", "end": "2025-01-31", "days": 30, "count": 2},
            {"start": "2025-02-01", "end": "2025-02-28", "days": 27, "count": 1},
        ]


def test_analytics_summary_includes_study_name_and_timestamps(client):
    with _temporary_active_db(f"analytics_timestamps_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_ts_1",
            study_name="S01_OKX_SOLUSDT.P, 1h 2025.01.01-2025.01.31_WFA (3)",
            dataset_start_date="2025-01-01",
            dataset_end_date="2025-01-31",
            created_at="2026-02-20 12:00:00",
            completed_at="2026-02-20 12:05:00",
        )
        _insert_analytics_study(
            study_id="wfa_ts_2",
            study_name="S03_OKX_ETHUSDT.P, 240 2025.01.01-2025.01.31_WFA",
            dataset_start_date="2025-01-01",
            dataset_end_date="2025-01-31",
            created_at=None,
            completed_at="2026-02-21 09:30:00",
        )
        with get_db_connection() as conn:
            conn.execute("UPDATE studies SET created_at = NULL WHERE study_id = ?", ("wfa_ts_2",))
            conn.commit()

        response = client.get("/api/analytics/summary")
        assert response.status_code == 200
        payload = response.get_json()
        studies = payload["studies"]
        by_id = {row["study_id"]: row for row in studies}

        first = by_id["wfa_ts_1"]
        assert first["study_name"] == "S01_OKX_SOLUSDT.P, 1h 2025.01.01-2025.01.31_WFA (3)"
        assert first["created_at"] == "2026-02-20 12:00:00"
        assert first["completed_at"] == "2026-02-20 12:05:00"
        assert isinstance(first["created_at_epoch"], int)
        assert isinstance(first["completed_at_epoch"], int)
        assert first["created_at_epoch"] > 0
        assert first["completed_at_epoch"] > 0
        assert first["wfa_settings"]["run_time_seconds"] == 300

        second = by_id["wfa_ts_2"]
        assert second["study_name"] == "S03_OKX_ETHUSDT.P, 240 2025.01.01-2025.01.31_WFA"
        assert second["created_at"] is None
        assert second["created_at_epoch"] is None
        assert second["completed_at"] == "2026-02-21 09:30:00"
        assert isinstance(second["completed_at_epoch"], int)
        assert second["completed_at_epoch"] > 0
        assert second["wfa_settings"]["run_time_seconds"] is None


def test_analytics_summary_includes_focus_settings_payload(client):
    with _temporary_active_db(f"analytics_focus_settings_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_focus_1",
            study_name="WFA_FOCUS_1",
            optimization_mode="wfa",
            adaptive_mode=1,
            is_period_days=60,
            config_json={
                "objectives": ["net_profit_pct", "max_drawdown_pct"],
                "primary_objective": "net_profit_pct",
                "constraints": [{"enabled": True, "metric": "total_trades", "threshold": 30}],
                "worker_processes": 4,
                "filter_min_profit": True,
                "min_profit_threshold": 12.5,
                "score_config": {
                    "filter_enabled": True,
                    "min_score_threshold": 77.5,
                },
                "optuna_config": {
                    "budget_mode": "trials",
                    "n_trials": 500,
                    "time_limit": 3600,
                    "convergence_patience": 50,
                    "sampler": "tpe",
                    "enable_pruning": False,
                    "pruner": "median",
                    "warmup_trials": 132,
                    "coverage_mode": True,
                    "dispatcher_batch_result_processing": False,
                    "dispatcher_soft_duplicate_cycle_limit_enabled": True,
                    "dispatcher_duplicate_cycle_limit": 18,
                    "sanitize_enabled": True,
                    "sanitize_trades_threshold": 3,
                },
                "wfa": {
                    "is_period_days": 90,
                    "oos_period_days": 30,
                    "adaptive_mode": True,
                    "max_oos_period_days": 120,
                    "min_oos_trades": 7,
                    "check_interval_trades": 4,
                    "cusum_threshold": 5.5,
                    "dd_threshold_multiplier": 1.7,
                    "inactivity_multiplier": 6.2,
                },
            },
        )
        _insert_analytics_study(
            study_id="wfa_focus_2",
            study_name="WFA_FOCUS_2",
            optimization_mode="wfa",
            adaptive_mode=None,
            is_period_days=None,
            config_json={},
        )
        with get_db_connection() as conn:
            conn.execute(
                """
                UPDATE studies
                SET
                    max_oos_period_days = 110,
                    min_oos_trades = 9,
                    check_interval_trades = 8,
                    cusum_threshold = 4.4,
                    dd_threshold_multiplier = 1.8,
                    inactivity_multiplier = 7.2,
                    optimization_time_seconds = 3661
                WHERE study_id = 'wfa_focus_1'
                """
            )
            conn.execute(
                """
                UPDATE studies
                SET
                    budget_mode = 'time',
                    time_limit = 1800,
                    sampler_type = 'random',
                    sanitize_enabled = 0,
                    sanitize_trades_threshold = 11,
                    filter_min_profit = 1,
                    min_profit_threshold = 9.0,
                    optimization_time_seconds = 95,
                    score_config_json = '{"filter_enabled": 1, "min_score_threshold": 73.5}'
                WHERE study_id = 'wfa_focus_2'
                """
            )
            conn.commit()

        response = client.get("/api/analytics/summary")
        assert response.status_code == 200
        payload = response.get_json()
        studies = payload["studies"]
        by_id = {row["study_id"]: row for row in studies}

        first = by_id["wfa_focus_1"]
        assert first["optuna_settings"]["objectives"] == ["net_profit_pct", "max_drawdown_pct"]
        assert first["optuna_settings"]["primary_objective"] == "net_profit_pct"
        assert first["optuna_settings"]["budget_mode"] == "trials"
        assert first["optuna_settings"]["n_trials"] == 500
        assert first["optuna_settings"]["sampler_type"] == "tpe"
        assert first["optuna_settings"]["enable_pruning"] is False
        assert first["optuna_settings"]["pruner"] == "median"
        assert first["optuna_settings"]["warmup_trials"] == 132
        assert first["optuna_settings"]["coverage_mode"] is True
        assert first["optuna_settings"]["dispatcher_batch_result_processing"] is False
        assert first["optuna_settings"]["dispatcher_soft_duplicate_cycle_limit_enabled"] is True
        assert first["optuna_settings"]["dispatcher_duplicate_cycle_limit"] == 18
        assert first["optuna_settings"]["workers"] == 4
        assert first["optuna_settings"]["sanitize_enabled"] is True
        assert first["optuna_settings"]["sanitize_trades_threshold"] == 3
        assert first["optuna_settings"]["filter_min_profit"] is True
        assert first["optuna_settings"]["min_profit_threshold"] == 12.5
        assert first["optuna_settings"]["score_filter_enabled"] is True
        assert first["optuna_settings"]["score_min_threshold"] == 77.5

        assert first["wfa_settings"]["is_period_days"] == 60
        assert first["wfa_settings"]["oos_period_days"] == 30
        assert first["wfa_settings"]["adaptive_mode"] is True
        assert first["wfa_settings"]["max_oos_period_days"] == 110
        assert first["wfa_settings"]["min_oos_trades"] == 9
        assert first["wfa_settings"]["check_interval_trades"] == 8
        assert first["wfa_settings"]["cusum_threshold"] == 4.4
        assert first["wfa_settings"]["dd_threshold_multiplier"] == 1.8
        assert first["wfa_settings"]["inactivity_multiplier"] == 7.2
        assert first["wfa_settings"]["run_time_seconds"] == 3661

        second = by_id["wfa_focus_2"]
        assert second["optuna_settings"]["budget_mode"] == "time"
        assert second["optuna_settings"]["time_limit"] == 1800
        assert second["optuna_settings"]["sampler_type"] == "random"
        assert second["optuna_settings"]["warmup_trials"] is None
        assert second["optuna_settings"]["coverage_mode"] is None
        assert second["optuna_settings"]["dispatcher_batch_result_processing"] is None
        assert second["optuna_settings"]["dispatcher_soft_duplicate_cycle_limit_enabled"] is None
        assert second["optuna_settings"]["dispatcher_duplicate_cycle_limit"] is None
        assert second["optuna_settings"]["sanitize_enabled"] is False
        assert second["optuna_settings"]["sanitize_trades_threshold"] == 11
        assert second["optuna_settings"]["filter_min_profit"] is True
        assert second["optuna_settings"]["min_profit_threshold"] == 9.0
        assert second["optuna_settings"]["score_filter_enabled"] is True
        assert second["optuna_settings"]["score_min_threshold"] == 73.5
        assert second["wfa_settings"]["adaptive_mode"] is None
        assert second["wfa_settings"]["run_time_seconds"] == 95


def test_analytics_sets_crud_and_reorder(client):
    with _temporary_active_db(f"analytics_sets_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_set_1",
            study_name="WFA_SET_1",
            optimization_mode="wfa",
        )
        _insert_analytics_study(
            study_id="wfa_set_2",
            study_name="WFA_SET_2",
            optimization_mode="wfa",
        )

        create_response = client.post(
            "/api/analytics/sets",
            json={"name": "First Set", "study_ids": ["wfa_set_1", "wfa_set_2"]},
        )
        assert create_response.status_code == 201
        created_first = create_response.get_json()
        assert created_first["name"] == "First Set"
        assert created_first["study_ids"] == ["wfa_set_1", "wfa_set_2"]

        second_response = client.post(
            "/api/analytics/sets",
            json={"name": "Second Set", "study_ids": ["wfa_set_2"]},
        )
        assert second_response.status_code == 201
        created_second = second_response.get_json()

        list_response = client.get("/api/analytics/sets")
        assert list_response.status_code == 200
        payload = list_response.get_json()
        assert [item["name"] for item in payload["sets"]] == ["First Set", "Second Set"]

        update_response = client.put(
            f"/api/analytics/sets/{created_first['id']}",
            json={"name": "First Set Updated", "study_ids": ["wfa_set_1"]},
        )
        assert update_response.status_code == 200
        assert update_response.get_json()["ok"] is True

        reorder_response = client.put(
            "/api/analytics/sets/reorder",
            json={"order": [created_second["id"], created_first["id"]]},
        )
        assert reorder_response.status_code == 200
        assert reorder_response.get_json()["ok"] is True

        list_after_reorder = client.get("/api/analytics/sets").get_json()
        assert [item["id"] for item in list_after_reorder["sets"]] == [
            created_second["id"],
            created_first["id"],
        ]
        by_id = {item["id"]: item for item in list_after_reorder["sets"]}
        assert by_id[created_first["id"]]["name"] == "First Set Updated"
        assert by_id[created_first["id"]]["study_ids"] == ["wfa_set_1"]

        delete_response = client.delete(f"/api/analytics/sets/{created_first['id']}")
        assert delete_response.status_code == 200
        assert delete_response.get_json()["ok"] is True

        final_payload = client.get("/api/analytics/sets").get_json()
        assert [item["id"] for item in final_payload["sets"]] == [created_second["id"]]


def test_analytics_sets_create_auto_suffixes_duplicate_names(client):
    with _temporary_active_db(f"analytics_sets_duplicates_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_dup_1",
            study_name="WFA_DUP_1",
            optimization_mode="wfa",
        )

        first = client.post(
            "/api/analytics/sets",
            json={"name": "Duplicate Set", "study_ids": ["wfa_dup_1"]},
        )
        assert first.status_code == 201
        assert first.get_json()["name"] == "Duplicate Set"

        second = client.post(
            "/api/analytics/sets",
            json={"name": "Duplicate Set", "study_ids": ["wfa_dup_1"]},
        )
        assert second.status_code == 201
        assert second.get_json()["name"] == "Duplicate Set (1)"

        third = client.post(
            "/api/analytics/sets",
            json={"name": "Duplicate Set", "study_ids": ["wfa_dup_1"]},
        )
        assert third.status_code == 201
        assert third.get_json()["name"] == "Duplicate Set (2)"

        payload = client.get("/api/analytics/sets").get_json()
        assert [item["name"] for item in payload["sets"]] == [
            "Duplicate Set",
            "Duplicate Set (1)",
            "Duplicate Set (2)",
        ]


def test_analytics_sets_reject_non_wfa_study_ids(client):
    with _temporary_active_db(f"analytics_sets_validation_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="optuna_not_allowed",
            study_name="OPTUNA_NOT_ALLOWED",
            optimization_mode="optuna",
        )
        response = client.post(
            "/api/analytics/sets",
            json={"name": "Invalid Set", "study_ids": ["optuna_not_allowed"]},
        )
        assert response.status_code == 400
        payload = response.get_json()
        assert "non-WFA" in payload["error"]


def test_analytics_sets_members_cascade_on_study_delete(client):
    with _temporary_active_db(f"analytics_sets_cascade_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_cascade_1",
            study_name="WFA_CASCADE_1",
            optimization_mode="wfa",
        )
        create_response = client.post(
            "/api/analytics/sets",
            json={"name": "Cascade Set", "study_ids": ["wfa_cascade_1"]},
        )
        assert create_response.status_code == 201

        assert delete_study("wfa_cascade_1") is True

        payload = client.get("/api/analytics/sets").get_json()
        assert payload["sets"][0]["name"] == "Cascade Set"
        assert payload["sets"][0]["study_ids"] == []


def test_analytics_sets_reorder_requires_complete_order(client):
    with _temporary_active_db(f"analytics_sets_reorder_{uuid.uuid4().hex[:8]}"):
        _insert_analytics_study(
            study_id="wfa_r1",
            study_name="WFA_R1",
            optimization_mode="wfa",
        )
        _insert_analytics_study(
            study_id="wfa_r2",
            study_name="WFA_R2",
            optimization_mode="wfa",
        )
        first = client.post(
            "/api/analytics/sets",
            json={"name": "R1", "study_ids": ["wfa_r1"]},
        ).get_json()
        client.post(
            "/api/analytics/sets",
            json={"name": "R2", "study_ids": ["wfa_r2"]},
        )

        bad_reorder = client.put(
            "/api/analytics/sets/reorder",
            json={"order": [first["id"]]},
        )
        assert bad_reorder.status_code == 400
        assert "exactly once" in bad_reorder.get_json()["error"]


@pytest.mark.parametrize("threshold", [-1, "bad"])
def test_optuna_sanitize_threshold_validation(threshold):
    from ui import server as server_module

    payload = _build_minimal_optuna_payload()
    payload["sanitize_trades_threshold"] = threshold

    with pytest.raises(ValueError):
        server_module._build_optimization_config(
            "dummy.csv",
            payload,
            worker_processes=1,
            strategy_id="s01_trailing_ma",
            warmup_bars=1000,
        )
