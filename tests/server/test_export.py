"""Server export contracts."""

import io
import csv
import hashlib
import json
import uuid
from copy import deepcopy
from pathlib import Path

import pytest
import pandas as pd

from core.backtest_engine import TradeRecord
from core.storage import get_db_connection

from ._helpers import _create_wfa_study, _temporary_active_db, _v2_runtime_diagnostic


def _insert_lancelot_export_study(
    *,
    study_id: str,
    study_name: str,
    optimization_mode: str,
    csv_file_path: str,
    csv_file_name: str,
    strategy_id: str = "s03_reversal_v10",
    strategy_version: str = "v10",
    warmup_bars: int = 1000,
    config_json: dict | None = None,
):
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO studies (
                study_id,
                study_name,
                strategy_id,
                strategy_version,
                optimization_mode,
                csv_file_path,
                csv_file_name,
                warmup_bars,
                config_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                study_id,
                study_name,
                strategy_id,
                strategy_version,
                optimization_mode,
                csv_file_path,
                csv_file_name,
                int(warmup_bars),
                json.dumps(config_json or {"fixed_params": {}}),
            ),
        )
        conn.commit()


def _insert_lancelot_export_trial(*, study_id: str, trial_number: int, params: dict):
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO trials (
                study_id,
                trial_number,
                params_json,
                objective_values_json,
                constraint_values_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                study_id,
                int(trial_number),
                json.dumps(params),
                json.dumps([]),
                json.dumps([]),
            ),
        )
        conn.commit()


def _insert_lancelot_export_wfa_window(
    *,
    study_id: str,
    window_number: int,
    best_params: dict,
    best_params_source: str = "optuna_is",
    is_best_trial_number: int | None = None,
):
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO wfa_windows (
                window_id,
                study_id,
                window_number,
                best_params_json,
                best_params_source,
                is_best_trial_number
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                f"{study_id}_w{window_number}",
                study_id,
                int(window_number),
                json.dumps(best_params),
                best_params_source,
                is_best_trial_number,
            ),
        )
        conn.commit()


def _insert_lancelot_export_wfa_trial(
    *,
    study_id: str,
    window_number: int,
    module_type: str,
    trial_number: int,
    params: dict,
    is_selected: bool,
):
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO wfa_window_trials (
                window_id,
                module_type,
                trial_number,
                params_json,
                objective_values_json,
                constraint_values_json,
                is_selected,
                module_metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"{study_id}_w{window_number}",
                module_type,
                int(trial_number),
                json.dumps(params),
                json.dumps([]),
                json.dumps([]),
                1 if is_selected else 0,
                json.dumps({}),
            ),
        )
        conn.commit()


def _insert_stored_execution_inventory(
    *, study_id: str, csv_path: Path, config_json: dict | str
) -> None:
    _insert_lancelot_export_study(
        study_id=study_id,
        study_name=study_id,
        strategy_id="s06_r_trend_v02_b2",
        strategy_version="v02-b2",
        optimization_mode="optuna",
        csv_file_path=str(csv_path),
        csv_file_name="OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv",
        warmup_bars=999,
        config_json=config_json if isinstance(config_json, dict) else {},
    )
    with get_db_connection() as conn:
        if isinstance(config_json, str):
            conn.execute(
                "UPDATE studies SET config_json = ? WHERE study_id = ?",
                (config_json, study_id),
            )
        conn.execute(
            """
            UPDATE studies
            SET ft_enabled = 1,
                ft_start_date = '2025-05-01',
                ft_end_date = '2025-05-10',
                oos_test_enabled = 1,
                oos_test_start_date = '2025-06-01',
                oos_test_end_date = '2025-06-10'
            WHERE study_id = ?
            """,
            (study_id,),
        )
        conn.commit()
    _insert_lancelot_export_trial(
        study_id=study_id,
        trial_number=7,
        params={
            "fastLength": 50,
            "dateFilter": True,
            "start": "candidate-start",
            "end": "candidate-end",
            "warmupBars": 9999,
        },
    )


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


def test_download_wfa_window_trades_respects_stored_oos_trade_count(client, monkeypatch, tmp_path):
    import ui.server_routes_data as routes_data

    csv_path = tmp_path / "_tmp_wfa_window_trades.csv"
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


@pytest.mark.parametrize('all_no_trade', [False, True])
def test_ordinary_wfa_export_skips_explicit_no_trade(client, monkeypatch, tmp_path, all_no_trade):
    from core import storage
    from core.engine_v2 import admission
    from tests._portfolio_helpers import STRATEGY, payload, publish

    with _temporary_active_db('no_trade_' + uuid.uuid4().hex):
        pool = publish(tmp_path/'pack')
        launched = client.post('/api/walkforward',data=dict(strategy=STRATEGY,
            config=json.dumps(payload()),source=json.dumps(pool[0]),warmupBars='100',
            wf_period_unit='months',wf_is_period_months='1',wf_oos_period_months='1'))
        assert launched.status_code == 200,launched.get_data(as_text=True)
        study_id = launched.get_json()['study_id']
        saved = storage.load_study_from_db(study_id)
        assert not saved['study']['config_json'].get('portfolio')
        first,second = saved['windows']
        assert first['oos_total_trades'] > 0
        expected = client.post(f"/api/studies/{study_id}/wfa/windows/{first['window_number']}/trades",json={'period':'oos'})
        assert expected.status_code == 200,expected.get_data(as_text=True)
        with storage.get_db_connection() as conn:
            for window in (first,second) if all_no_trade else (second,):
                conn.execute("UPDATE wfa_windows SET window_status='no_trade', no_trade_reason='ft_reject_action_no_trade', oos_total_trades=0 WHERE study_id=? AND window_number=?",
                             (study_id,window['window_number']))
            conn.commit()
        before = storage.load_study_from_db(study_id)
        calls=[];real=admission.run_strategy
        def execute(*args,**kwargs):
            calls.append(dict(args[2]))
            return real(*args,**kwargs)
        monkeypatch.setattr(admission,'run_strategy',execute)
        exported = client.post(f'/api/studies/{study_id}/wfa/trades')
        assert exported.status_code == 200,exported.get_data(as_text=True)
        if all_no_trade:
            assert calls == []
            assert exported.get_data(as_text=True) == 'Symbol,Side,Qty,Fill Price,Closing Time\n'
        else:
            assert len(calls) == 1
            assert exported.get_data(as_text=True) == expected.get_data(as_text=True)
        assert storage.load_study_from_db(study_id) == before


def test_download_wfa_trades_uses_precise_oos_bounds(client, monkeypatch, tmp_path):
    import strategies
    import ui.server_routes_data as routes_data

    csv_path = tmp_path / "_tmp_wfa_precise_bounds.csv"
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


def test_stored_execution_endpoint_inventory_uses_shared_runtime_and_reads_tests(
    client, monkeypatch, tmp_path
):
    from core.storage import get_study_trial, load_study_from_db
    from ui import server_routes_data, server_data_input

    csv_path = tmp_path / "stored_execution_inventory.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2025-01-01T00:00:00Z,1,1,1,1,1\n",
        encoding="utf-8",
    )
    values = {
        "dateFilter": False,
        "start": "2025-01-01T00:00:00Z",
        "end": "2025-02-01T00:00:00Z",
        "warmupBars": 20,
    }
    config = {
        "fixed_params": {"slowLength": 70},
        "v2_runtime": {
            "schema_version": "v2_runtime_metadata_v1",
            "contract_version": "v2_runtime_contract_v1",
            "values": values,
            "diagnostics": [],
            "validation_warnings": [],
        },
    }
    frame = pd.DataFrame(
        {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1.0},
        index=pd.date_range("2025-01-01", "2025-08-01", freq="D", tz="UTC"),
    )
    config["csv_oos_replay"] = dict(version=1, start="2025-06-01T00:00:00Z",
        end="2025-06-10T23:59:59.999999Z", preparation_start="2025-05-12T00:00:00Z",
        preparation_end="2025-06-10T00:00:00Z", warmup_bars=20, trade_start_idx=20)
    captured = []

    def fake_period_test(**kwargs):
        candidate = kwargs["trials"][0]["params"]
        captured.append(
            (
                "manual",
                kwargs["execution_params_resolver"](
                    candidate, kwargs["start_ts"], kwargs["end_ts"]
                ),
                kwargs["warmup_bars"],
            )
        )
        return []

    def fake_trade_export(**kwargs):
        captured.append(("trade", deepcopy(kwargs["params"]), kwargs["warmup_bars"]))
        return [], None

    monkeypatch.setattr(server_routes_data, "load_data", lambda _path: frame)
    monkeypatch.setattr(server_data_input, "load_data", lambda _path: frame)
    monkeypatch.setattr(server_routes_data, "run_period_test_for_trials", fake_period_test)
    monkeypatch.setattr(server_routes_data, "_run_trade_export", fake_trade_export)

    with _temporary_active_db(f"stored_inventory_{uuid.uuid4().hex[:8]}"):
        study_id = "stored_execution_inventory"
        _insert_stored_execution_inventory(
            study_id=study_id,
            csv_path=csv_path,
            config_json=config,
        )
        with get_db_connection() as conn:
            conn.execute("UPDATE trials SET ft_rank=1, oos_test_total_trades=0, oos_test_net_profit_pct=0 WHERE study_id=?", (study_id,))
            conn.commit()
        original_study = deepcopy(load_study_from_db(study_id)["study"])
        original_trial = deepcopy(get_study_trial(study_id, 7))
        studies_response = client.get("/api/studies")
        study_response = client.get(f"/api/studies/{study_id}")
        assert studies_response.status_code == study_response.status_code == 200
        listed_study = next(
            item
            for item in studies_response.get_json()["studies"]
            if item["study_id"] == study_id
        )
        loaded_study = study_response.get_json()["study"]
        assert listed_study["optimization_mode"] == "optuna"
        assert loaded_study["optimization_mode"] == "optuna"
        assert loaded_study["strategy_id"] == "s06_r_trend_v02_b2"

        manual = client.post(
            f"/api/studies/{study_id}/test",
            json={
                "dataSource": "original_csv",
                "startDate": "2025-07-01",
                "endDate": "2025-07-03",
                "trialNumbers": [7],
                "sourceTab": "optuna",
                "testName": "inventory",
            },
        )
        assert manual.status_code == 200
        test_id = manual.get_json()["test_id"]
        listed = client.get(f"/api/studies/{study_id}/tests")
        detail = client.get(f"/api/studies/{study_id}/tests/{test_id}")
        assert listed.status_code == detail.status_code == 200
        assert listed.get_json()["tests"][0]["id"] == test_id
        assert detail.get_json()["results_json"]["config"]["start_date"] == "2025-07-01"

        routes = [
            f"/api/studies/{study_id}/trials/7/trades",
            f"/api/studies/{study_id}/trials/7/ft-trades",
            f"/api/studies/{study_id}/trials/7/oos-trades",
            f"/api/studies/{study_id}/tests/{test_id}/trials/7/mt-trades",
        ]
        for route in routes:
            assert client.post(route).status_code == 200

        assert captured[0] == (
            "manual",
            {
                "slowLength": 70,
                "fastLength": 50,
                "dateFilter": True,
                "start": "2025-07-01T00:00:00Z",
                "end": "2025-07-03T00:00:00Z",
            },
            20,
        )
        exported = [item[1] for item in captured[1:]]
        assert exported[0] == {
            "slowLength": 70,
            "fastLength": 50,
            "dateFilter": False,
            "start": None,
            "end": None,
        }
        assert exported[1]["start"] == "2025-05-01T00:00:00Z"
        assert exported[1]["end"] == "2025-05-10T23:59:59.999999Z"
        assert exported[2]["start"] == "2025-06-01T00:00:00Z"
        assert exported[2]["end"] == "2025-06-10T23:59:59.999999Z"
        assert exported[3]["start"] == "2025-07-01T00:00:00Z"
        assert exported[3]["end"] == "2025-07-03T23:59:59.999999Z"
        assert all(item[2] == 20 for item in captured)
        assert load_study_from_db(study_id)["study"] == original_study
        assert get_study_trial(study_id, 7) == original_trial


@pytest.mark.parametrize(
    ("route_suffix", "payload"),
    [
        (
            "/test",
            {
                "dataSource": "original_csv",
                "startDate": "2025-07-01",
                "endDate": "2025-07-03",
                "trialNumbers": [7],
                "sourceTab": "optuna",
            },
        ),
        ("/trials/7/trades", None),
        ("/trials/7/ft-trades", None),
        ("/trials/7/oos-trades", None),
        ("/tests/1/trials/7/mt-trades", None),
    ],
)
def test_stored_execution_inventory_corruption_stops_before_csv(
    client, tmp_path, route_suffix, payload
):
    csv_path = tmp_path / "must_not_be_read.csv"
    assert not csv_path.exists()
    with _temporary_active_db(f"stored_corrupt_{uuid.uuid4().hex[:8]}"):
        study_id = "stored_corrupt"
        _insert_stored_execution_inventory(
            study_id=study_id,
            csv_path=csv_path,
            config_json="{not-json",
        )
        response = client.post(
            f"/api/studies/{study_id}{route_suffix}",
            json=payload,
        )

    assert response.status_code == 400
    diagnostic = _v2_runtime_diagnostic(response)
    assert diagnostic["code"] == "V2_STORED_CONFIG_INCOMPATIBLE"
    assert diagnostic["path"] == "config_json"


def test_v1_stored_trade_export_keeps_legacy_merge_semantics(
    client, monkeypatch, tmp_path
):
    from ui import server_routes_data

    csv_path = tmp_path / "v1_stored_trade.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2025-01-01T00:00:00Z,1,1,1,1,1\n",
        encoding="utf-8",
    )
    captured = {}
    monkeypatch.setattr(
        server_routes_data,
        "_run_trade_export",
        lambda **kwargs: (captured.update(deepcopy(kwargs)) or [], None),
    )
    with _temporary_active_db(f"v1_stored_trade_{uuid.uuid4().hex[:8]}"):
        study_id = "v1_stored_trade"
        _insert_lancelot_export_study(
            study_id=study_id,
            study_name=study_id,
            strategy_id="s03_reversal_v10",
            strategy_version="v10",
            optimization_mode="optuna",
            csv_file_path=str(csv_path),
            csv_file_name="OKX_LINKUSDT.P, 15 2025.01.01-2025.01.02.csv",
            warmup_bars=20,
            config_json={
                "fixed_params": {
                    "maType3": "HMA",
                    "maType3_options": ["SMA", "HMA"],
                    "dateFilter": False,
                }
            },
        )
        _insert_lancelot_export_trial(
            study_id=study_id,
            trial_number=7,
            params={"maLength3": 75, "dateFilter": True, "start": "candidate"},
        )

        loaded = client.get(f"/api/studies/{study_id}")
        assert loaded.status_code == 200
        assert loaded.get_json()["study"]["optimization_mode"] == "optuna"
        assert loaded.get_json()["study"]["strategy_id"] == "s03_reversal_v10"
        response = client.post(f"/api/studies/{study_id}/trials/7/trades")

    assert response.status_code == 200
    assert captured["params"] == {
        "maType3": "HMA",
        "maType3_options": ["SMA", "HMA"],
        "dateFilter": False,
        "maLength3": 75,
        "start": None,
        "end": None,
    }
    assert captured["warmup_bars"] == 20


def test_export_lancelot_bundle_from_optuna_trial(client, tmp_path):
    csv_path = tmp_path / "_tmp_lancelot_export_optuna.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n2025-01-01T00:00:00Z,1,1,1,1,1\n",
        encoding="utf-8",
    )

    try:
        with _temporary_active_db(f"bundle_export_optuna_{uuid.uuid4().hex[:8]}"):
            study_id = "bundle_optuna_1"
            _insert_lancelot_export_study(
                study_id=study_id,
                study_name="bundle_optuna_1",
                optimization_mode="optuna",
                csv_file_path=str(csv_path),
                csv_file_name="OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv",
                config_json={
                    "fixed_params": {
                        "maType3": "HMA",
                        "maType3_options": ["SMA", "HMA"],
                        "undeclaredControl": "must-not-export",
                        "dateFilter": True,
                        "warmupBars": 9999,
                    }
                },
            )
            _insert_lancelot_export_trial(
                study_id=study_id,
                trial_number=42,
                params={"closeCountLong": 3, "useTBands": True},
            )

            response = client.post(
                f"/api/studies/{study_id}/export/lancelot",
                json={"trialNumber": 42},
            )

            assert response.status_code == 200
            payload = response.get_json()
            assert payload["bundleSchemaVersion"] == 2
            assert payload["strategyId"] == "s03_reversal_v10"
            assert payload["strategyVersion"] == "v10"
            assert payload["symbol"] == "LINK/USDT:USDT"
            assert payload["timeframe"] == "15m"
            assert payload["warmupBars"] == 1000
            assert payload["exportMode"] == "live"
            assert payload["params"] == {
                "closeCountLong": 3,
                "useTBands": True,
                "dateFilter": False,
                "start": None,
                "end": None,
            }
            assert payload["source"]["studyId"] == study_id
            assert payload["source"]["trialNumber"] == 42
            assert payload["source"]["studyName"] == "bundle_optuna_1"
            assert payload["source"]["exportedAt"].endswith("Z")
            assert payload["source"]["merlinVersion"]
            assert payload["source"]["merlinCommit"]
            assert payload["source"]["dataFingerprint"] == (
                f"sha256:{hashlib.sha256(csv_path.read_bytes()).hexdigest()}"
            )
    finally:
        if csv_path.exists():
            csv_path.unlink()


def test_export_lancelot_bundle_from_grid_candidate(client, tmp_path):
    csv_path = tmp_path / "_tmp_lancelot_export_grid.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n2025-01-01T00:00:00Z,1,1,1,1,1\n",
        encoding="utf-8",
    )

    try:
        with _temporary_active_db(f"bundle_export_grid_{uuid.uuid4().hex[:8]}"):
            study_id = "bundle_grid_1"
            _insert_lancelot_export_study(
                study_id=study_id,
                study_name="bundle_grid_1",
                optimization_mode="grid",
                csv_file_path=str(csv_path),
                csv_file_name="OKX_SOLUSDT.P, 5 2025.05.01-2025.11.20.csv",
                warmup_bars=500,
                config_json={
                    "fixed_params": {
                        "maType3": "HMA",
                        "useCloseCount_options": [True, False],
                        "undeclaredControl": "must-not-export",
                    }
                },
            )
            _insert_lancelot_export_trial(
                study_id=study_id,
                trial_number=9,
                params={"maType3": "EMA", "maLength3": 75, "useCloseCount": True},
            )

            response = client.post(
                f"/api/studies/{study_id}/export/lancelot",
                json={"trialNumber": 9},
            )

            assert response.status_code == 200
            payload = response.get_json()
            assert payload["symbol"] == "SOL/USDT:USDT"
            assert payload["timeframe"] == "5m"
            assert payload["warmupBars"] == 500
            assert payload["params"] == {
                "maType3": "EMA",
                "maLength3": 75,
                "useCloseCount": True,
                "dateFilter": False,
                "start": None,
                "end": None,
            }
            assert payload["source"]["trialNumber"] == 9
    finally:
        if csv_path.exists():
            csv_path.unlink()


@pytest.mark.parametrize(
    ("strategy_id", "strategy_version", "optimization_mode", "selection"),
    [
        ("s06_r_trend_v02", "v02", "optuna", {"trialNumber": 3}),
        ("s06_r_trend_v02_b2", "v02-b2", "wfa", {"windowNumber": 1}),
    ],
)
def test_lancelot_rejects_unsupported_v1_and_v2_before_export_work(
    client,
    monkeypatch,
    tmp_path,
    strategy_id,
    strategy_version,
    optimization_mode,
    selection,
):
    from core import storage
    from ui import server_routes_data

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("unsupported Lancelot export continued past strategy gate")

    for name in (
        "load_study_from_db",
        "_resolve_stored_execution_context",
        "_resolve_csv_path",
        "get_study_trial",
        "_find_wfa_window",
        "load_wfa_window_trials",
        "build_lancelot_partial_bundle",
    ):
        monkeypatch.setattr(server_routes_data, name, fail_if_called)
    monkeypatch.setattr(storage, "backfill_stitched_oos_metadata", fail_if_called)

    missing_csv = tmp_path / "must_not_be_read.csv"
    assert not missing_csv.exists()
    with _temporary_active_db(f"unsupported_lancelot_{uuid.uuid4().hex[:8]}"):
        study_id = f"unsupported_{strategy_id}_{optimization_mode}"
        _insert_lancelot_export_study(
            study_id=study_id,
            study_name=study_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            optimization_mode=optimization_mode,
            csv_file_path=str(missing_csv),
            csv_file_name="must_not_be_read.csv",
            config_json={"fixed_params": {}},
        )
        response = client.post(
            f"/api/studies/{study_id}/export/lancelot", json=selection
        )

    assert response.status_code == 400
    payload = response.get_json()
    assert "currently supports only 's03_reversal_v10'" in payload["error"]
    diagnostic = _v2_runtime_diagnostic(response)
    assert diagnostic == {
        "severity": "error",
        "code": "LANCELOT_EXPORT_STRATEGY_UNSUPPORTED",
        "strategy_id": strategy_id,
        "path": "strategy_id",
        "variant": None,
        "message": payload["error"],
    }


def test_lancelot_supported_identity_disappearing_before_full_load_returns_404(
    client, monkeypatch
):
    from ui import server_routes_data

    monkeypatch.setattr(
        server_routes_data,
        "load_study_identity_from_db",
        lambda _study_id: {
            "study_id": "disappearing-study",
            "strategy_id": "s03_reversal_v10",
        },
    )
    monkeypatch.setattr(server_routes_data, "load_study_from_db", lambda _study_id: None)

    response = client.post(
        "/api/studies/disappearing-study/export/lancelot",
        json={"trialNumber": 1},
    )

    assert response.status_code == 404
    assert response.get_json() == {"error": "Study not found."}


def test_lancelot_strategy_change_between_identity_and_full_load_is_rejected(
    client, monkeypatch
):
    from ui import server_routes_data

    monkeypatch.setattr(
        server_routes_data,
        "load_study_identity_from_db",
        lambda _study_id: {
            "study_id": "changed-study",
            "strategy_id": "s03_reversal_v10",
        },
    )
    monkeypatch.setattr(
        server_routes_data,
        "load_study_from_db",
        lambda _study_id: {"study": {"strategy_id": "s06_r_trend_v02_b2"}},
    )

    response = client.post(
        "/api/studies/changed-study/export/lancelot",
        json={"trialNumber": 1},
    )

    assert response.status_code == 400
    diagnostic = _v2_runtime_diagnostic(response)
    assert diagnostic["code"] == "LANCELOT_EXPORT_STRATEGY_UNSUPPORTED"
    assert diagnostic["strategy_id"] == "s06_r_trend_v02_b2"
    assert diagnostic["path"] == "strategy_id"


def test_export_lancelot_bundle_from_wfa_window_uses_window_trial_number(client, tmp_path):
    csv_path = tmp_path / "_tmp_lancelot_export_wfa.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n2025-01-01T00:00:00Z,1,1,1,1,1\n",
        encoding="utf-8",
    )

    try:
        with _temporary_active_db(f"bundle_export_wfa_{uuid.uuid4().hex[:8]}"):
            study_id = "bundle_wfa_1"
            _insert_lancelot_export_study(
                study_id=study_id,
                study_name="bundle_wfa_1",
                optimization_mode="wfa",
                csv_file_path=str(csv_path),
                csv_file_name="OKX_BTCUSDT.P, 1h 2025.05.01-2025.11.20.csv",
                warmup_bars=1500,
                config_json={
                    "fixed_params": {
                        "maType3": "HMA",
                        "useTBands_options": [True, False],
                        "undeclaredControl": "must-not-export",
                    }
                },
            )
            _insert_lancelot_export_wfa_window(
                study_id=study_id,
                window_number=1,
                best_params={"maLength3": 75},
                is_best_trial_number=7,
            )

            response = client.post(
                f"/api/studies/{study_id}/export/lancelot",
                json={"windowNumber": 1},
            )

            assert response.status_code == 200
            payload = response.get_json()
            assert payload["symbol"] == "BTC/USDT:USDT"
            assert payload["timeframe"] == "1h"
            assert payload["warmupBars"] == 1500
            assert payload["params"] == {
                "maLength3": 75,
                "dateFilter": False,
                "start": None,
                "end": None,
            }
            assert payload["source"]["trialNumber"] == 7
    finally:
        if csv_path.exists():
            csv_path.unlink()


def test_export_lancelot_bundle_from_wfa_window_falls_back_to_selected_module_trial(client, tmp_path):
    csv_path = tmp_path / "_tmp_lancelot_export_wfa_selected.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n2025-01-01T00:00:00Z,1,1,1,1,1\n",
        encoding="utf-8",
    )

    try:
        with _temporary_active_db(f"bundle_export_wfa_sel_{uuid.uuid4().hex[:8]}"):
            study_id = "bundle_wfa_selected_1"
            _insert_lancelot_export_study(
                study_id=study_id,
                study_name="bundle_wfa_selected_1",
                optimization_mode="wfa",
                csv_file_path=str(csv_path),
                csv_file_name="OKX_ETHUSDT.P, 60 2025.05.01-2025.11.20.csv",
            )
            _insert_lancelot_export_wfa_window(
                study_id=study_id,
                window_number=1,
                best_params={"maLength3": 125},
                best_params_source="forward_test",
                is_best_trial_number=None,
            )
            _insert_lancelot_export_wfa_trial(
                study_id=study_id,
                window_number=1,
                module_type="forward_test",
                trial_number=19,
                params={"maLength3": 125},
                is_selected=True,
            )

            response = client.post(
                f"/api/studies/{study_id}/export/lancelot",
                json={"windowNumber": 1},
            )

            assert response.status_code == 200
            payload = response.get_json()
            assert payload["symbol"] == "ETH/USDT:USDT"
            assert payload["timeframe"] == "1h"
            assert payload["source"]["trialNumber"] == 19
    finally:
        if csv_path.exists():
            csv_path.unlink()


def test_export_lancelot_bundle_requires_selection_payload(client, tmp_path):
    csv_path = tmp_path / "_tmp_lancelot_export_missing.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n2025-01-01T00:00:00Z,1,1,1,1,1\n",
        encoding="utf-8",
    )

    try:
        with _temporary_active_db(f"bundle_export_missing_{uuid.uuid4().hex[:8]}"):
            study_id = "bundle_missing_selection"
            _insert_lancelot_export_study(
                study_id=study_id,
                study_name="bundle_missing_selection",
                optimization_mode="optuna",
                csv_file_path=str(csv_path),
                csv_file_name="OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv",
            )

            response = client.post(
                f"/api/studies/{study_id}/export/lancelot",
                json={},
            )

            assert response.status_code == 400
            assert response.get_json()["error"] == "trialNumber is required for bundle export."
    finally:
        if csv_path.exists():
            csv_path.unlink()
