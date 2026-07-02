import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core import storage
from ui import server_routes_data
from ui.server import app


@pytest.fixture
def isolated_storage(monkeypatch, tmp_path):
    # tmp_path is a per-test pytest temp directory outside the repository, so a
    # failed or interrupted run never leaves artifacts under tests/.
    storage_dir = tmp_path / "storage"
    journal_dir = storage_dir / "journals"
    storage_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(storage, "STORAGE_DIR", storage_dir)
    monkeypatch.setattr(storage, "JOURNAL_DIR", journal_dir)
    monkeypatch.setattr(storage, "_active_db_path", storage_dir / "seed.db")
    monkeypatch.setattr(storage, "DB_INITIALIZED", False)

    def _configure_connection_for_tests(conn):
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row

    monkeypatch.setattr(storage, "_configure_connection", _configure_connection_for_tests)
    try:
        yield storage_dir
    finally:
        storage.DB_INITIALIZED = False


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client


def test_list_db_files_sorted_and_active(isolated_storage, monkeypatch):
    first = isolated_storage / "2026-01-01_010101_first.db"
    second = isolated_storage / "2026-01-02_020202_second.db"
    first.touch()
    second.touch()
    storage._active_db_path = first

    ctimes = {first.name: 100.0, second.name: 200.0}
    monkeypatch.setattr(storage.os.path, "getctime", lambda p: ctimes[Path(p).name])

    payload = storage.list_db_files()
    assert [row["name"] for row in payload] == [second.name, first.name]
    assert payload[0]["active"] is False
    assert payload[1]["active"] is True


def test_set_active_db_existing(isolated_storage):
    first = storage.create_new_db("one")
    second = storage.create_new_db("two")
    storage.set_active_db(first)
    assert storage.get_active_db_name() == first
    storage.set_active_db(second)
    assert storage.get_active_db_name() == second


def test_set_active_db_nonexistent_raises(isolated_storage):
    with pytest.raises(ValueError, match="not found"):
        storage.set_active_db("missing.db")


def test_set_active_db_path_traversal_rejected(isolated_storage):
    with pytest.raises(ValueError, match="Invalid database filename"):
        storage.set_active_db("../evil.db")


def test_create_new_db_with_label_sanitizes_and_initializes(isolated_storage):
    filename = storage.create_new_db('link 15m:*?"<>|')
    assert re.match(r"^\d{4}-\d{2}-\d{2}_\d{6}_link-15m\.db$", filename)
    assert (isolated_storage / filename).exists()
    with storage.get_db_connection() as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='studies'")
        assert cursor.fetchone() is not None


def test_create_new_db_without_label_uses_timestamp_name(isolated_storage):
    filename = storage.create_new_db("")
    assert re.match(r"^\d{4}-\d{2}-\d{2}_\d{6}\.db$", filename)
    assert storage.get_active_db_name() == filename
    assert (isolated_storage / filename).exists()


def test_get_db_connection_uses_snapshot_path(isolated_storage, monkeypatch):
    first = isolated_storage / "2026-01-01_000001_a.db"
    second = isolated_storage / "2026-01-01_000002_b.db"
    storage._active_db_path = first
    storage.init_database(db_path=first)
    storage.init_database(db_path=second)

    original_init_database = storage.init_database

    def patched_init_database(db_path=None):
        original_init_database(db_path=db_path)
        storage._active_db_path = second

    monkeypatch.setattr(storage, "init_database", patched_init_database)

    with storage.get_db_connection() as conn:
        db_path = conn.execute("PRAGMA database_list").fetchone()["file"]

    assert Path(db_path).resolve() == first.resolve()
    assert storage._active_db_path.resolve() == second.resolve()


def test_databases_endpoints_list_create_switch(client, isolated_storage):
    first = storage.create_new_db("one")
    second = storage.create_new_db("two")
    storage.set_active_db(first)

    list_response = client.get("/api/databases")
    assert list_response.status_code == 200
    list_payload = list_response.get_json()
    assert list_payload["active"] == first
    assert {row["name"] for row in list_payload["databases"]} >= {first, second}

    create_response = client.post("/api/databases", json={"label": "three"})
    assert create_response.status_code == 200
    create_payload = create_response.get_json()
    created_name = create_payload["filename"]
    assert create_payload["active"] == created_name
    assert (isolated_storage / created_name).exists()

    switch_response = client.post("/api/databases/active", json={"filename": first})
    assert switch_response.status_code == 200
    assert switch_response.get_json()["active"] == first


@pytest.mark.parametrize(
    "method,path,payload",
    [
        ("post", "/api/databases/active", {"filename": "x.db"}),
        ("post", "/api/databases", {"label": "x"}),
    ],
)
def test_databases_mutations_blocked_while_running(client, monkeypatch, method, path, payload):
    monkeypatch.setattr(server_routes_data, "_get_optimization_state", lambda: {"status": "running"})
    response = getattr(client, method)(path, json=payload)
    assert response.status_code == 409


def test_optimize_invalid_config_does_not_switch_database(client, isolated_storage):
    first = storage.create_new_db("current")
    second = storage.create_new_db("target")
    storage.set_active_db(first)

    csv_path = isolated_storage / "input.csv"
    csv_path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")

    response = client.post(
        "/api/optimize",
        data={
            "csvPath": str(csv_path),
            "dbTarget": second,
            "config": json.dumps({"objectives": [], "primary_objective": None}),
        },
    )
    assert response.status_code == 400
    assert storage.get_active_db_name() == first


def test_optimize_rejects_new_db_target_without_explicit_create(client, isolated_storage):
    current = storage.create_new_db("current")
    storage.set_active_db(current)

    csv_path = isolated_storage / "input.csv"
    csv_path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")
    db_files_before = {path.name for path in isolated_storage.glob("*.db")}

    response = client.post(
        "/api/optimize",
        data={
            "strategy": "s01_trailing_ma",
            "csvPath": str(csv_path),
            "dbTarget": "new",
            "dbLabel": "should-not-be-created",
            "config": json.dumps(
                {
                    "strategy": "s01_trailing_ma",
                    "enabled_params": {},
                    "param_ranges": {},
                    "fixed_params": {},
                    "objectives": ["net_profit_pct"],
                    "primary_objective": None,
                    "optuna_budget_mode": "trials",
                    "optuna_n_trials": 1,
                    "optuna_time_limit": 60,
                    "optuna_convergence": 10,
                }
            ),
        },
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert "create and select a database" in (payload.get("error") or "").lower()
    assert storage.get_active_db_name() == current
    db_files_after = {path.name for path in isolated_storage.glob("*.db")}
    assert db_files_after == db_files_before
