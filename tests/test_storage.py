import json
import sqlite3
import time
import uuid
from copy import deepcopy
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from core import storage
from core.storage import (
    create_new_db,
    create_study_set,
    delete_study_sets,
    get_active_db_name,
    get_or_build_all_studies_analytics_cache,
    get_or_build_study_set_analytics_cache,
    get_db_connection,
    list_study_sets,
    list_study_sets_with_analytics_cache,
    load_study_from_db,
    load_wfa_window_trials,
    save_dsr_results,
    save_grid_study_to_db,
    save_optuna_study_to_db,
    reorder_study_sets,
    save_wfa_study_to_db,
    set_active_db,
    update_study_config_json,
    update_study_sets_color,
    update_study_set,
)
from core.metrics import _calculate_r2_consistency
from core.grid_engine import GRID_V2_SUPPORTED_FAST_OBJECTIVES, GridSettings
from core.engine_v2 import V2ValidationError, build_v2_runtime_metadata
from core.optuna_engine import OBJECTIVE_DIRECTIONS as CORE_OBJECTIVE_DIRECTIONS
from core.optuna_engine import OptimizationConfig, OptimizationResult
from core.post_process import DSRConfig, DSRResult, PostProcessConfig, StressTestConfig
from core.walkforward_engine import OOSStitchedResult, WFConfig, WFResult, WindowResult


@contextmanager
def _temporary_active_db(label: str):
    previous_db = get_active_db_name()
    create_new_db(label)
    try:
        yield
    finally:
        set_active_db(previous_db)


def test_load_study_identity_is_one_read_only_select(monkeypatch):
    with _temporary_active_db(f"identity_{uuid.uuid4().hex[:8]}"):
        with get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO studies (
                    study_id, study_name, strategy_id, optimization_mode
                ) VALUES (?, ?, ?, ?)
                """,
                ("identity-study", "Identity Study", "s03_reversal_v10", "optuna"),
            )
            conn.commit()

        statements = []
        connect_calls = []
        original_connect = storage.sqlite3.connect

        class RecordingConnection:
            def __init__(self, connection):
                self.connection = connection

            @property
            def row_factory(self):
                return self.connection.row_factory

            @row_factory.setter
            def row_factory(self, value):
                self.connection.row_factory = value

            def execute(self, sql, params):
                statements.append((" ".join(sql.split()), params))
                return self.connection.execute(sql, params)

            def commit(self):
                raise AssertionError("identity lookup must not commit")

            def close(self):
                self.connection.close()

        def recording_connect(*args, **kwargs):
            connect_calls.append((args, kwargs))
            return RecordingConnection(original_connect(*args, **kwargs))

        monkeypatch.setattr(storage.sqlite3, "connect", recording_connect)

        assert storage.load_study_identity_from_db("identity-study") == {
            "study_id": "identity-study",
            "strategy_id": "s03_reversal_v10",
        }
        assert storage.load_study_identity_from_db("missing-study") is None

        assert statements == [
            (
                "SELECT study_id, strategy_id FROM studies WHERE study_id = ?",
                ("identity-study",),
            ),
            (
                "SELECT study_id, strategy_id FROM studies WHERE study_id = ?",
                ("missing-study",),
            ),
        ]
        assert len(connect_calls) == 2
        assert all(call_kwargs["uri"] is True for _, call_kwargs in connect_calls)
        assert all("mode=ro" in call_args[0] for call_args, _ in connect_calls)


def _build_dummy_wfa_result():
    wf_config = WFConfig(strategy_id="s01_trailing_ma", is_period_days=10, oos_period_days=5)
    params = {"maType": "EMA", "maLength": 50, "closeCountLong": 7}

    window = WindowResult(
        window_id=1,
        is_start=pd.Timestamp("2025-01-01", tz="UTC"),
        is_end=pd.Timestamp("2025-01-10", tz="UTC"),
        oos_start=pd.Timestamp("2025-01-11", tz="UTC"),
        oos_end=pd.Timestamp("2025-01-15", tz="UTC"),
        best_params=params,
        param_id="EMA 50_test",
        is_net_profit_pct=1.0,
        is_max_drawdown_pct=0.5,
        is_total_trades=1,
        oos_net_profit_pct=2.0,
        oos_max_drawdown_pct=0.7,
        oos_total_trades=2,
        oos_winning_trades=1,
        oos_equity_curve=[100.0, 102.0],
        oos_timestamps=[
            pd.Timestamp("2025-01-11", tz="UTC"),
            pd.Timestamp("2025-01-15", tz="UTC"),
        ],
        is_best_trial_number=1,
        is_equity_curve=[100.0, 101.0],
        is_timestamps=[
            pd.Timestamp("2025-01-01", tz="UTC"),
            pd.Timestamp("2025-01-10", tz="UTC"),
        ],
        best_params_source="optuna_is",
        available_modules=["optuna_is"],
        is_pareto_optimal=True,
        constraints_satisfied=False,
        is_win_rate=50.0,
        is_sharpe_daily=1.25,
        is_sharpe_daily_observations=7,
        is_sharpe_daily_active_days=0,
        oos_win_rate=50.0,
        oos_sharpe_daily=None,
        oos_sharpe_daily_observations=4,
        oos_sharpe_daily_active_days=0,
        optuna_is_trials=[
            {
                "trial_number": 1,
                "params": params,
                "param_id": "EMA 50_test",
                "net_profit_pct": 1.0,
                "max_drawdown_pct": 0.5,
                "total_trades": 1,
                "win_rate": 50.0,
                "sharpe_daily": 1.25,
                "sharpe_daily_observations": 7,
                "sharpe_daily_active_days": 0,
                "is_selected": True,
            }
        ],
    )

    stitched = OOSStitchedResult(
        final_net_profit_pct=2.0,
        max_drawdown_pct=0.7,
        total_trades=2,
        wfe=100.0,
        oos_win_rate=100.0,
        equity_curve=[100.0, 102.0],
        timestamps=[
            pd.Timestamp("2025-01-11", tz="UTC"),
            pd.Timestamp("2025-01-15", tz="UTC"),
        ],
        window_ids=[1, 1],
    )

    wf_result = WFResult(
        config=wf_config,
        windows=[window],
        stitched_oos=stitched,
        strategy_id="s01_trailing_ma",
        total_windows=1,
        trading_start_date=window.is_start,
        trading_end_date=window.oos_end,
        warmup_bars=wf_config.warmup_bars,
    )
    return wf_result


def _runtime_metadata_for_storage():
    return build_v2_runtime_metadata(
        {
            "dateFilter": False,
            "start": None,
            "end": None,
            "warmupBars": 20,
        },
        strategy_id="s06_r_trend_v02_b2",
    )


def _storage_optimization_config(*, strategy_id="s06_r_trend_v02_b2"):
    return OptimizationConfig(
        csv_file="storage.csv",
        csv_original_name="OKX_LINKUSDT.P, 15 2025.01.01-2025.01.02.csv",
        strategy_id=strategy_id,
        enabled_params={},
        param_ranges={},
        param_types={},
        fixed_params={"dateFilter": False},
        warmup_bars=20,
    )


def test_runtime_metadata_persists_through_optuna_grid_and_wfa_writers(tmp_path):
    csv_path = tmp_path / "OKX_LINKUSDT.P, 15 2025.01.01-2025.01.02.csv"
    csv_path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")
    metadata = _runtime_metadata_for_storage()
    original = deepcopy(metadata)

    with _temporary_active_db(f"runtime_writers_{uuid.uuid4().hex[:8]}"):
        optuna_config = _storage_optimization_config()
        optuna_config.v2_runtime = deepcopy(metadata)
        optuna_id = save_optuna_study_to_db(
            None,
            optuna_config,
            SimpleNamespace(
                objectives=["net_profit_pct"],
                primary_objective="net_profit_pct",
                constraints=[],
                sampler_config={},
                budget_mode="trials",
                n_trials=0,
                time_limit=None,
                convergence_patience=None,
                sanitize_enabled=True,
                sanitize_trades_threshold=0,
            ),
            [],
            str(csv_path),
            time.time(),
        )

        grid_ids = []
        for policy in ("full", "sampled"):
            grid_config = _storage_optimization_config()
            grid_config.v2_runtime = deepcopy(metadata)
            grid_config.grid_v2_planning_policy = policy
            grid_ids.append(
                save_grid_study_to_db(
                    config=grid_config,
                    grid_settings=GridSettings(requested_budget=1, top_candidates=1),
                    grid_summary={"engine": "v2", "requested_planning_policy": policy},
                    trial_results=[],
                    csv_file_path=str(csv_path),
                    start_time=time.time(),
                )
            )

        wf_result = _build_dummy_wfa_result()
        wf_result.strategy_id = "s06_r_trend_v02_b2"
        wf_result.config.strategy_id = "s06_r_trend_v02_b2"
        wfa_id = save_wfa_study_to_db(
            wf_result,
            {
                "strategy_id": "s06_r_trend_v02_b2",
                "fixed_params": {"dateFilter": False},
                "v2_runtime": deepcopy(metadata),
            },
            str(csv_path),
            time.time(),
        )

        for study_id in (optuna_id, *grid_ids, wfa_id):
            loaded = load_study_from_db(study_id)
            assert loaded["study"]["config_json"]["v2_runtime"] == metadata
        assert all(
            "v2_runtime" not in window
            for window in load_study_from_db(wfa_id)["windows"]
        )
        assert metadata == original


def test_v1_writer_omits_runtime_and_config_rewrite_preserves_carrier(tmp_path):
    csv_path = tmp_path / "OKX_LINKUSDT.P, 15 2025.01.01-2025.01.02.csv"
    csv_path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")
    metadata = _runtime_metadata_for_storage()
    with _temporary_active_db(f"runtime_rewrite_{uuid.uuid4().hex[:8]}"):
        v1_config = _storage_optimization_config(strategy_id="s03_reversal_v10")
        v1_id = save_optuna_study_to_db(
            None,
            v1_config,
            SimpleNamespace(objectives=[], constraints=[], sampler_config={}),
            [],
            str(csv_path),
            time.time(),
        )
        assert "v2_runtime" not in load_study_from_db(v1_id)["study"]["config_json"]

        v2_config = _storage_optimization_config()
        v2_config.v2_runtime = deepcopy(metadata)
        v2_id = save_optuna_study_to_db(
            None,
            v2_config,
            SimpleNamespace(objectives=[], constraints=[], sampler_config={}),
            [],
            str(csv_path),
            time.time(),
        )
        updated = load_study_from_db(v2_id)["study"]["config_json"]
        updated["postProcess"] = {"enabled": True}
        updated["oosTest"] = {"enabled": True}
        assert update_study_config_json(v2_id, updated) is True
        assert load_study_from_db(v2_id)["study"]["config_json"]["v2_runtime"] == metadata


def test_malformed_runtime_metadata_creates_no_study_row(tmp_path):
    csv_path = tmp_path / "OKX_LINKUSDT.P, 15 2025.01.01-2025.01.02.csv"
    csv_path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")
    config = _storage_optimization_config()
    config.v2_runtime = {"schema_version": "broken"}
    with _temporary_active_db(f"runtime_invalid_{uuid.uuid4().hex[:8]}"):
        with pytest.raises(V2ValidationError) as caught:
            save_optuna_study_to_db(
                None,
                config,
                SimpleNamespace(objectives=[], constraints=[], sampler_config={}),
                [],
                str(csv_path),
                time.time(),
            )
        assert caught.value.diagnostics[0].code == "V2_RUNTIME_METADATA_INVALID"
        with get_db_connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM studies").fetchone()[0] == 0


def test_optuna_daily_sharpe_trial_storage_roundtrip(tmp_path):
    csv_path = tmp_path / "daily.csv"
    csv_path.write_text("timestamp,open,high,low,close,volume\n", encoding="utf-8")
    config = _storage_optimization_config(strategy_id="s03_reversal_v10")
    result = OptimizationResult(
        params={"x": 1},
        net_profit_pct=1.0,
        max_drawdown_pct=0.5,
        total_trades=0,
        sharpe_daily=None,
        sharpe_daily_observations=9,
        sharpe_daily_active_days=0,
        objective_values=[1.0],
        optuna_trial_number=1,
    )
    optuna_config = SimpleNamespace(
        objectives=["net_profit_pct"],
        primary_objective=None,
        constraints=[],
        sampler_config={},
        budget_mode="trials",
        n_trials=1,
        time_limit=None,
        convergence_patience=None,
        sanitize_enabled=True,
        sanitize_trades_threshold=0,
    )

    with _temporary_active_db(f"daily_trial_{uuid.uuid4().hex[:8]}"):
        study_id = save_optuna_study_to_db(
            None,
            config,
            optuna_config,
            [result],
            str(csv_path),
            time.time(),
        )
        trial = load_study_from_db(study_id)["trials"][0]

    assert trial["sharpe_daily"] is None
    assert trial["sharpe_daily_observations"] == 9
    assert trial["sharpe_daily_active_days"] == 0
    assert isinstance(trial["sharpe_daily_observations"], int)
    assert isinstance(trial["sharpe_daily_active_days"], int)


def _build_grid_storage_config(csv_path: Path) -> OptimizationConfig:
    return OptimizationConfig(
        csv_file=str(csv_path),
        strategy_id="s03_reversal_v10",
        enabled_params={},
        param_ranges={},
        param_types={},
        fixed_params={
            "dateFilter": False,
            "start": "2025-01-01",
            "end": "2025-01-31",
        },
        optimization_mode="grid",
        objectives=["net_profit_pct"],
        grid_budget=10,
        grid_top_candidates=2,
    )


def _build_grid_storage_result(
    candidate_id: int,
    *,
    grid_rank: int,
    net_profit_pct: float,
    selection_sources: list[str],
) -> OptimizationResult:
    result = OptimizationResult(
        params={"candidate": candidate_id},
        net_profit_pct=net_profit_pct,
        max_drawdown_pct=1.0,
        total_trades=10,
        winning_trades=6,
        losing_trades=4,
        win_rate=60.0,
        romad=net_profit_pct,
        profit_factor=1.5,
        sharpe_ratio=0.5,
        optuna_trial_number=candidate_id,
        objective_values=[net_profit_pct],
        constraints_satisfied=True,
    )
    result.candidate_id = candidate_id
    result.semantic_key = f"candidate:{candidate_id}"
    result.param_key = result.semantic_key
    result.grid_rank = grid_rank
    result.selection_sources = list(selection_sources)
    result.is_objective_selected = "objective" in selection_sources
    result.is_dsr_selected = "dsr" in selection_sources
    result.validation_status = "passed"
    return result


def test_diagnostic_updates_only_changed_content_and_clear_stale_scopes(tmp_path):
    diag = dict(version=1, scope="is", maxEntryLeverageUsed=2., leverageCapSkippedEntries=3)
    other = {**diag, "scope": "ft"}
    raw = json.dumps({"ft": other, "is": diag}, indent=2)
    with sqlite3.connect(tmp_path / "diagnostics.db") as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE trials (study_id TEXT, trial_number INTEGER, entry_diagnostics_json TEXT)")
        conn.executemany("INSERT INTO trials VALUES (?, ?, ?)", [
            ("s", 1, None), ("s", 2, raw), ("s", 3, json.dumps({"ft": other})),
            ("elsewhere", 2, raw), ("s", 4, "malformed legacy JSON")])
        updates = []
        conn.set_trace_callback(lambda sql: updates.append(sql) if sql.startswith("UPDATE trials SET entry_diagnostics_json") else None)

        def write(scope, executions):
            updates.clear()
            storage._write_trial_diagnostics(conn, "s", scope, executions)
            return len(updates)

        assert write("is", [(2, {**diag, "scope": "execution"})]) == 0
        assert write("oos_test", []) == 0  # Legacy NULL and malformed fallback stay unchanged.
        changed = {**diag, "leverageCapSkippedEntries": 4}
        assert write("is", [(2, changed)]) == 1
        assert "trial_number = 2" in updates[0]
        assert write("is", [(2, changed)]) == 0
        assert write("ft", [(2, None)]) == 2  # Explicit None and omitted trial both clear.
        rows = dict(conn.execute("SELECT trial_number, entry_diagnostics_json FROM trials WHERE study_id='s'"))
        assert json.loads(rows[2]) == {"is": changed} and rows[3] is None
        assert write("is", []) == 1  # Last scope -> SQL NULL.
        assert conn.execute("SELECT entry_diagnostics_json FROM trials WHERE study_id='s' AND trial_number=2").fetchone()[0] is None
        assert conn.execute("SELECT entry_diagnostics_json FROM trials WHERE study_id='elsewhere'").fetchone()[0] == raw
        assert conn.execute("SELECT entry_diagnostics_json FROM trials WHERE study_id='s' AND trial_number=4").fetchone()[0] == "malformed legacy JSON"


def test_public_diagnostic_scope_saves_and_readback(tmp_path, monkeypatch):
    path = tmp_path / "synthetic.csv"
    path.write_text("time,open,high,low,close,Volume\n", encoding="utf-8")
    result = _build_grid_storage_result(1, grid_rank=1, net_profit_pct=1., selection_sources=["objective"])
    diag = dict(version=1, scope="is", maxEntryLeverageUsed=2., leverageCapSkippedEntries=7)
    result.entry_diagnostics = diag
    with _temporary_active_db("diagnostic_scopes"):
        study_id = save_grid_study_to_db(config=_build_grid_storage_config(path),
            grid_settings=GridSettings(requested_budget=1, top_candidates=1), grid_summary={},
            trial_results=[result], csv_file_path=str(path), start_time=0.)
        assert load_study_from_db(study_id)["trials"][0]["entry_diagnostics"] == {"is": diag}
        statements = []
        original = storage.get_db_connection

        @contextmanager
        def recorded():
            with original() as conn:
                conn.set_trace_callback(lambda sql: statements.append(sql) if sql.startswith("UPDATE trials SET entry_diagnostics_json") else None)
                yield conn
        monkeypatch.setattr(storage, "get_db_connection", recorded)
        ft_options = dict(ft_enabled=True, ft_period_days=1, ft_top_k=1, ft_sort_metric=None,
            ft_threshold_pct=None, ft_reject_action=None, ft_reject_cooldown_days=None,
            ft_reject_max_attempts=None, ft_reject_min_remaining_oos_days=None,
            ft_start_date="2024-01-02", ft_end_date="2024-01-03", is_period_days=1)
        oos_options = dict(oos_enabled=True, oos_period_days=1, oos_top_k=1,
            oos_start_date="2024-01-03", oos_end_date="2024-01-04", oos_source_module="optuna")
        ft = dict(trial_number=1, ft_net_profit_pct=2., ft_total_trades=1, entry_diagnostics=diag)
        oos = dict(trial_number=1, test_metrics=dict(net_profit_pct=3., total_trades=2), entry_diagnostics=diag)
        for repeat in range(2):
            statements.clear()
            assert storage.save_forward_test_results(study_id, [ft], **ft_options)
            assert storage.save_oos_test_results(study_id, [oos], **oos_options)
            assert len(statements) == (2 if repeat == 0 else 0)
        saved = load_study_from_db(study_id)["trials"][0]
        assert saved["ft_net_profit_pct"] == 2. and saved["oos_test_net_profit_pct"] == 3.
        assert saved["entry_diagnostics"] == {s: {**diag, "scope": s} for s in ("is", "ft", "oos_test")}
        assert storage.save_forward_test_results(study_id, [], **ft_options)
        assert storage.save_oos_test_results(study_id, [{**oos, "entry_diagnostics": None}], **oos_options)
        assert load_study_from_db(study_id)["trials"][0]["entry_diagnostics"] == {"is": diag}


def test_storage_objective_directions_cover_grid_v2_fast_objectives():
    missing = sorted(GRID_V2_SUPPORTED_FAST_OBJECTIVES - set(storage.OBJECTIVE_DIRECTIONS))
    assert missing == []
    assert storage.OBJECTIVE_DIRECTIONS["total_trades"] == "maximize"
    assert storage.OBJECTIVE_DIRECTIONS["max_consecutive_losses"] == "minimize"
    assert storage.OBJECTIVE_DIRECTIONS == CORE_OBJECTIVE_DIRECTIONS


def test_save_grid_study_persists_grid_v2_objective_directions(tmp_path):
    csv_path = tmp_path / "BTCUSDT_2025.01.01_data.csv"
    csv_path.write_text("time,open,high,low,close,Volume\n", encoding="utf-8")
    config = _build_grid_storage_config(csv_path)
    config.objectives = ["max_consecutive_losses", "total_trades"]
    config.primary_objective = "max_consecutive_losses"
    result = _build_grid_storage_result(
        1,
        grid_rank=1,
        net_profit_pct=1.0,
        selection_sources=["objective"],
    )
    result.objective_values = [3, 10]
    result.max_consecutive_losses = 3
    result.total_trades = 10
    result.sharpe_daily = 1.75
    result.sharpe_daily_observations = 8
    result.sharpe_daily_active_days = 0
    summary = {
        "requested_budget": 1,
        "actual_budget": 1,
        "completed_trials": 1,
        "grid": {"preview": {"coverage_pct": 100.0}},
    }

    with _temporary_active_db("grid_direction_metadata"):
        study_id = save_grid_study_to_db(
            config=config,
            grid_settings=GridSettings(requested_budget=1, top_candidates=1),
            grid_summary=summary,
            trial_results=[result],
            csv_file_path=str(csv_path),
            start_time=0.0,
        )
        loaded = load_study_from_db(study_id)

    assert loaded["study"]["objectives"] == ["max_consecutive_losses", "total_trades"]
    assert loaded["study"]["directions"] == ["minimize", "maximize"]
    assert loaded["trials"][0]["sharpe_daily"] == 1.75
    assert loaded["trials"][0]["sharpe_daily_observations"] == 8
    assert loaded["trials"][0]["sharpe_daily_active_days"] == 0


def test_load_study_sort_fallback_minimizes_max_consecutive_losses():
    with _temporary_active_db("sort_max_consecutive_losses"):
        with get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO studies (
                    study_id,
                    study_name,
                    strategy_id,
                    optimization_mode,
                    objectives_json,
                    primary_objective
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "sort_mcl",
                    "SORT_MCL",
                    "s03_reversal_v10",
                    "optuna",
                    json.dumps(["max_consecutive_losses"]),
                    "max_consecutive_losses",
                ),
            )
            for trial_number, losses in ((1, 5), (2, 2)):
                conn.execute(
                    """
                    INSERT INTO trials (
                        study_id,
                        trial_number,
                        params_json,
                        objective_values_json,
                        constraint_values_json,
                        max_consecutive_losses
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "sort_mcl",
                        trial_number,
                        json.dumps({"candidate": trial_number}),
                        json.dumps([losses]),
                        json.dumps([]),
                        losses,
                    ),
                )
            conn.commit()

        loaded = load_study_from_db("sort_mcl")

    assert [trial["trial_number"] for trial in loaded["trials"]] == [2, 1]


def test_wfa_window_trials_table_created():
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='wfa_window_trials'"
        )
        assert cursor.fetchone() is not None


def test_study_sets_tables_created():
    with get_db_connection() as conn:
        sets_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='study_sets'"
        ).fetchone()
        members_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='study_set_members'"
        ).fetchone()
        analytics_cache_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='analytics_group_cache'"
        ).fetchone()
        set_columns = {row["name"] for row in conn.execute("PRAGMA table_info(study_sets)").fetchall()}
        analytics_cache_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(analytics_group_cache)").fetchall()
        }
        assert sets_table is not None
        assert members_table is not None
        assert analytics_cache_table is not None
        assert "color_token" in set_columns
        assert "group_key" in analytics_cache_columns
        assert "group_type" in analytics_cache_columns
        assert "set_id" in analytics_cache_columns
        assert "members_hash" in analytics_cache_columns
        assert "curve_json" in analytics_cache_columns
        assert "timestamps_json" in analytics_cache_columns
        assert "ann_profit_pct" in analytics_cache_columns
        assert "profit_pct" in analytics_cache_columns
        assert "max_drawdown_pct" in analytics_cache_columns
        assert "consistency_full" in analytics_cache_columns
        assert "consistency_recent" in analytics_cache_columns
        assert "computed_at" in analytics_cache_columns


def test_wfa_window_new_columns():
    with get_db_connection() as conn:
        cursor = conn.execute("PRAGMA table_info(wfa_windows)")
        columns = {row["name"] for row in cursor.fetchall()}
    assert "best_params_source" in columns
    assert "available_modules" in columns
    assert "optimization_start_date" in columns
    assert "optimization_start_ts" in columns
    assert "ft_start_date" in columns
    assert "ft_start_ts" in columns
    assert "is_pareto_optimal" in columns
    assert "constraints_satisfied" in columns
    assert "is_start_ts" in columns
    assert "is_end_ts" in columns
    assert "oos_start_ts" in columns
    assert "oos_end_ts" in columns
    assert "trigger_type" in columns
    assert "cusum_final" in columns
    assert "cusum_threshold" in columns
    assert "dd_threshold" in columns
    assert "oos_actual_days" in columns
    assert "cooldown_days_applied" in columns
    assert "oos_elapsed_days" in columns
    assert "oos_winning_trades" in columns
    assert "trade_start_date" in columns
    assert "trade_end_date" in columns
    assert "trade_start_ts" in columns
    assert "trade_end_ts" in columns
    assert "entry_delay_days" in columns
    assert "ft_retry_attempts_used" in columns
    assert "remaining_oos_days_at_entry" in columns
    assert "window_status" in columns
    assert "no_trade_reason" in columns
    assert "grid_dsr_enabled" in columns
    assert "grid_dsr_top_k" in columns
    assert "grid_dsr_n_trials" in columns
    assert "grid_dsr_mean_sharpe" in columns
    assert "grid_dsr_var_sharpe" in columns
    assert "grid_dsr_sr0" in columns
    assert "grid_valid_candidate_count" in columns
    assert "grid_selected_candidate_count" in columns
    assert "is_sharpe_daily" in columns
    assert "is_sharpe_daily_observations" in columns
    assert "is_sharpe_daily_active_days" in columns
    assert "oos_sharpe_daily" in columns
    assert "oos_sharpe_daily_observations" in columns
    assert "oos_sharpe_daily_active_days" in columns


def test_daily_sharpe_schema_is_additive_idempotent_for_legacy_window_trials():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE wfa_windows (
            window_id TEXT PRIMARY KEY,
            study_id TEXT NOT NULL,
            window_number INTEGER NOT NULL,
            best_params_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE wfa_window_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_id TEXT NOT NULL,
            module_type TEXT NOT NULL,
            trial_number INTEGER NOT NULL,
            params_json TEXT NOT NULL,
            source_rank INTEGER,
            module_rank INTEGER,
            is_selected INTEGER DEFAULT 0
        )
        """
    )

    storage._ensure_wfa_schema_updated(conn)
    storage._ensure_wfa_schema_updated(conn)

    columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(wfa_window_trials)").fetchall()
    }
    assert {
        "sharpe_daily",
        "sharpe_daily_observations",
        "sharpe_daily_active_days",
    } <= columns
    conn.execute(
        "INSERT INTO wfa_windows (window_id, study_id, window_number, best_params_json) "
        "VALUES ('w1', 's1', 1, '{}')"
    )
    conn.execute(
        """
        INSERT INTO wfa_window_trials (
            window_id, module_type, trial_number, params_json,
            sharpe_daily, sharpe_daily_observations, sharpe_daily_active_days
        ) VALUES ('w1', 'optuna_is', 1, '{}', 1.5, 4, 0)
        """
    )
    saved = conn.execute(
        "SELECT sharpe_daily, sharpe_daily_observations, sharpe_daily_active_days "
        "FROM wfa_window_trials"
    ).fetchone()
    assert tuple(saved) == (1.5, 4, 0)
    assert isinstance(saved[1], int)
    assert isinstance(saved[2], int)
    conn.close()


def test_studies_stitched_columns():
    with get_db_connection() as conn:
        cursor = conn.execute("PRAGMA table_info(studies)")
        columns = {row["name"] for row in cursor.fetchall()}
    assert "stitched_oos_equity_curve" in columns
    assert "stitched_oos_timestamps_json" in columns
    assert "stitched_oos_window_ids_json" in columns
    assert "stitched_oos_net_profit_pct" in columns
    assert "stitched_oos_max_drawdown_pct" in columns
    assert "stitched_oos_total_trades" in columns
    assert "stitched_oos_winning_trades" in columns
    assert "stitched_oos_win_rate" in columns
    assert "stitched_oos_consistency_full" in columns
    assert "stitched_oos_consistency_recent" in columns
    assert "profitable_windows" in columns
    assert "total_windows" in columns
    assert "median_window_profit" in columns
    assert "median_window_wr" in columns
    assert "worst_window_profit" in columns
    assert "worst_window_dd" in columns
    assert "adaptive_mode" in columns
    assert "max_oos_period_days" in columns
    assert "min_oos_trades" in columns
    assert "check_interval_trades" in columns
    assert "cusum_threshold" in columns
    assert "dd_threshold_multiplier" in columns
    assert "inactivity_multiplier" in columns
    assert "cooldown_enabled" in columns
    assert "cooldown_days" in columns
    assert "ft_threshold_pct" in columns
    assert "ft_reject_action" in columns
    assert "ft_reject_cooldown_days" in columns
    assert "ft_reject_max_attempts" in columns
    assert "ft_reject_min_remaining_oos_days" in columns
    assert "stitched_oos_start_ts" in columns
    assert "stitched_oos_end_ts" in columns
    assert "stitched_oos_point_count" in columns


def test_trials_ft_gate_columns_exist():
    with get_db_connection() as conn:
        cursor = conn.execute("PRAGMA table_info(trials)")
        columns = {row["name"] for row in cursor.fetchall()}

    assert "ft_passes_threshold" in columns


def test_save_wfa_study_with_trials():
    wf_result = _build_dummy_wfa_result()
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    study_data = load_study_from_db(study_id)
    assert study_data is not None
    assert study_data["study"]["optimization_mode"] == "wfa"
    assert study_data["windows"]

    window = study_data["windows"][0]
    assert window.get("best_params_source") == "optuna_is"
    assert window.get("is_pareto_optimal") is True
    assert window.get("constraints_satisfied") is False
    assert window.get("oos_winning_trades") == 1
    assert window.get("grid_dsr_enabled") is None
    assert window.get("grid_dsr_top_k") is None
    assert window.get("grid_dsr_n_trials") is None
    assert window.get("grid_dsr_mean_sharpe") is None
    assert window.get("grid_dsr_var_sharpe") is None
    assert window.get("grid_dsr_sr0") is None

    study = study_data["study"]
    assert study.get("stitched_oos_winning_trades") == 1
    assert study.get("profitable_windows") == 1
    assert study.get("total_windows") == 1
    assert study.get("median_window_profit") == 2.0
    assert study.get("median_window_wr") == 50.0
    assert study.get("worst_window_profit") == 2.0
    assert study.get("worst_window_dd") == 0.7
    assert study.get("stitched_oos_start_ts") == "2025-01-11T00:00:00+00:00"
    assert study.get("stitched_oos_end_ts") == "2025-01-15T00:00:00+00:00"
    assert study.get("stitched_oos_point_count") == 2


def test_calendar_month_wfa_storage_uses_null_day_column_and_exact_config():
    wf_result = _build_dummy_wfa_result()
    wf_result.config = WFConfig(
        strategy_id="s01_trailing_ma",
        period_unit="months",
        is_period_days=None,
        oos_period_days=None,
        is_period_months=2,
        oos_period_months=1,
    )
    expected_boundaries = (
        wf_result.windows[0].is_start,
        wf_result.windows[0].is_end,
        wf_result.windows[0].oos_start,
        wf_result.windows[0].oos_end,
    )
    wfa_config = {
        "period_unit": "months",
        "is_period_months": 2,
        "oos_period_months": 1,
        "adaptive_mode": False,
    }

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={"wfa": wfa_config},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    loaded = load_study_from_db(study_id)

    assert loaded["study"]["is_period_days"] is None
    assert loaded["study"]["config_json"]["wfa"] == wfa_config
    window = loaded["windows"][0]
    assert (
        pd.Timestamp(window["is_start_ts"]),
        pd.Timestamp(window["is_end_ts"]),
        pd.Timestamp(window["oos_start_ts"]),
        pd.Timestamp(window["oos_end_ts"]),
    ) == expected_boundaries


def test_save_wfa_grid_dsr_replay_fields_and_candidate_metrics():
    wf_result = _build_dummy_wfa_result()
    wf_result.strategy_id = "s03_reversal_v10"
    wf_result.config.strategy_id = "s03_reversal_v10"
    window = wf_result.windows[0]
    window.best_params_source = "grid"
    window.available_modules = ["optuna_is", "dsr"]
    window.grid_dsr_enabled = True
    window.grid_dsr_top_k = 50
    window.grid_dsr_n_trials = 1000
    window.grid_dsr_mean_sharpe = 0.22
    window.grid_dsr_var_sharpe = 0.033
    window.grid_dsr_sr0 = 0.44
    window.grid_valid_candidate_count = 50
    window.grid_selected_candidate_count = 50
    window.optuna_is_trials = [
        {
            "trial_number": 101,
            "params": {"candidate": 101},
            "param_id": "candidate-101",
            "module_rank": 1,
            "net_profit_pct": 3.0,
            "max_drawdown_pct": 0.5,
            "total_trades": 10,
            "sharpe_ratio": 1.1,
            "is_selected": True,
            "module_metrics": {
                "grid_rank": 1,
                "semantic_key": "candidate:101",
                "candidate_id": 101,
                "dsr_skewness": 0.12,
                "dsr_kurtosis": 3.2,
                "dsr_track_length": 15,
                "dsr_probability": 0.91,
                "dsr_luck_share_pct": 4.0,
            },
        },
        {
            "trial_number": 102,
            "params": {"candidate": 102},
            "param_id": "candidate-102",
            "module_rank": 2,
            "net_profit_pct": 2.0,
            "max_drawdown_pct": 0.6,
            "total_trades": 9,
            "sharpe_ratio": 0.9,
            "module_metrics": {
                "grid_rank": 2,
                "semantic_key": "candidate:102",
                "candidate_id": 102,
                "dsr_skewness": 0.15,
                "dsr_kurtosis": 3.4,
                "dsr_track_length": 14,
            },
        },
    ]

    with _temporary_active_db("wfa_grid_dsr_replay_fields"):
        study_id = save_wfa_study_to_db(
            wf_result=wf_result,
            config={"optimization_mode": "grid"},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )

        study_data = load_study_from_db(study_id)
        assert study_data is not None
        loaded_window = study_data["windows"][0]
        assert loaded_window["grid_dsr_enabled"] == 1
        assert loaded_window["grid_dsr_top_k"] == 50
        assert loaded_window["grid_dsr_n_trials"] == 1000
        assert loaded_window["grid_dsr_mean_sharpe"] == pytest.approx(0.22)
        assert loaded_window["grid_dsr_var_sharpe"] == pytest.approx(0.033)
        assert loaded_window["grid_dsr_sr0"] == pytest.approx(0.44)
        assert loaded_window["grid_valid_candidate_count"] == 50
        assert loaded_window["grid_selected_candidate_count"] == 50

        modules = load_wfa_window_trials(loaded_window["window_id"])
        optuna_is = modules["optuna_is"]
        assert len(optuna_is) == 2
        for trial in optuna_is:
            metrics = trial["module_metrics"]
            assert metrics["semantic_key"].startswith("candidate:")
            assert metrics["candidate_id"] in {101, 102}
            assert "dsr_skewness" in metrics
            assert "dsr_kurtosis" in metrics
            assert "dsr_track_length" in metrics


def test_legacy_wfa_windows_schema_migrates_grid_dsr_columns():
    legacy_db = storage.STORAGE_DIR / f"legacy_wfa_windows_{uuid.uuid4().hex}.db"
    with sqlite3.connect(legacy_db) as conn:
        conn.execute(
            """
            CREATE TABLE wfa_windows (
                window_id TEXT PRIMARY KEY,
                study_id TEXT NOT NULL,
                window_number INTEGER NOT NULL,
                best_params_json TEXT NOT NULL
            )
            """
        )

    original_initialized = storage.DB_INITIALIZED
    storage.DB_INITIALIZED = False
    try:
        storage.init_database(db_path=legacy_db)
    finally:
        storage.DB_INITIALIZED = original_initialized

    with sqlite3.connect(legacy_db) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(wfa_windows)").fetchall()}

    assert "grid_dsr_enabled" in columns
    assert "grid_dsr_top_k" in columns
    assert "grid_dsr_n_trials" in columns
    assert "grid_dsr_mean_sharpe" in columns
    assert "grid_dsr_var_sharpe" in columns
    assert "grid_dsr_sr0" in columns
    assert "grid_valid_candidate_count" in columns
    assert "grid_selected_candidate_count" in columns


def test_save_dsr_results_preserves_grid_precomputed_fields_when_not_clearing(tmp_path):
    csv_path = tmp_path / "BTCUSDT_2025.01.01_data.csv"
    csv_path.write_text("time,open,high,low,close,Volume\n", encoding="utf-8")

    objective = _build_grid_storage_result(
        1,
        grid_rank=1,
        net_profit_pct=12.0,
        selection_sources=["objective"],
    )
    objective.dsr_probability = 0.42
    objective.dsr_skewness = 0.11
    objective.dsr_kurtosis = 3.11
    objective.dsr_track_length = 12
    objective.dsr_luck_share_pct = 14.0

    dsr_candidate = _build_grid_storage_result(
        2,
        grid_rank=4,
        net_profit_pct=5.0,
        selection_sources=["dsr"],
    )
    dsr_candidate.dsr_probability = 0.91
    dsr_candidate.dsr_rank = 1
    dsr_candidate.dsr_skewness = 0.21
    dsr_candidate.dsr_kurtosis = 3.21
    dsr_candidate.dsr_track_length = 12
    dsr_candidate.dsr_luck_share_pct = 7.0

    config = _build_grid_storage_config(csv_path)
    summary = {
        "requested_budget": 10,
        "actual_budget": 10,
        "completed_trials": 2,
        "pareto_front_size": None,
        "grid": {
            "preview": {"coverage_pct": 100.0},
            "dsr": {
                "enabled": True,
                "top_k": 1,
                "dsr_n_trials": 10,
                "dsr_mean_sharpe": 0.2,
                "dsr_var_sharpe": 0.03,
            },
        },
    }

    with _temporary_active_db("grid_dsr_preserve_fields"):
        study_id = save_grid_study_to_db(
            config=config,
            grid_settings=GridSettings(requested_budget=10, top_candidates=2),
            grid_summary=summary,
            trial_results=[objective, dsr_candidate],
            csv_file_path=str(csv_path),
            start_time=0.0,
        )

        save_dsr_results(
            study_id,
            [
                DSRResult(
                    trial_number=2,
                    optuna_rank=4,
                    params=dsr_candidate.params,
                    original_result=dsr_candidate,
                    dsr_probability=0.95,
                    dsr_rank=1,
                    dsr_skewness=0.25,
                    dsr_kurtosis=3.25,
                    dsr_track_length=14,
                    dsr_luck_share_pct=5.0,
                )
            ],
            dsr_enabled=True,
            dsr_top_k=1,
            dsr_n_trials=20,
            dsr_mean_sharpe=0.33,
            dsr_var_sharpe=0.044,
            clear_existing=False,
        )

        loaded = load_study_from_db(study_id)

    trials = {trial["trial_number"]: trial for trial in loaded["trials"]}
    preserved = trials[1]
    updated = trials[2]

    assert preserved["selection_sources"] == ["objective"]
    assert preserved["dsr_probability"] == pytest.approx(0.42)
    assert preserved["dsr_rank"] is None
    assert preserved["dsr_skewness"] == pytest.approx(0.11)
    assert preserved["dsr_kurtosis"] == pytest.approx(3.11)
    assert preserved["dsr_track_length"] == 12
    assert preserved["dsr_luck_share_pct"] == pytest.approx(14.0)

    assert updated["selection_sources"] == ["dsr"]
    assert updated["dsr_probability"] == pytest.approx(0.95)
    assert updated["dsr_rank"] == 1
    assert updated["dsr_skewness"] == pytest.approx(0.25)
    assert updated["dsr_kurtosis"] == pytest.approx(3.25)
    assert updated["dsr_track_length"] == 14
    assert updated["dsr_luck_share_pct"] == pytest.approx(5.0)

    assert loaded["study"]["dsr_enabled"] == 1
    assert loaded["study"]["dsr_top_k"] == 1
    assert loaded["study"]["dsr_n_trials"] == 20
    assert loaded["study"]["dsr_mean_sharpe"] == pytest.approx(0.33)
    assert loaded["study"]["dsr_var_sharpe"] == pytest.approx(0.044)


def test_study_sets_storage_roundtrip():
    wf_result_a = _build_dummy_wfa_result()
    study_id_a = save_wfa_study_to_db(
        wf_result=wf_result_a,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    wf_result_b = _build_dummy_wfa_result()
    wf_result_b.windows[0].window_id = 2
    study_id_b = save_wfa_study_to_db(
        wf_result=wf_result_b,
        config={},
        csv_file_path="",
        start_time=time.time(),
        score_config=None,
    )

    created = create_study_set("Storage Roundtrip Set", [study_id_a, study_id_b])
    assert created["name"] == "Storage Roundtrip Set"
    assert created["color_token"] is None
    assert created["study_ids"] == [study_id_a, study_id_b]

    updated = update_study_set(
        created["id"],
        name="Storage Roundtrip Set v2",
        study_ids=[study_id_b],
        color_token="blue",
    )
    assert updated["name"] == "Storage Roundtrip Set v2"
    assert updated["color_token"] == "blue"
    assert updated["study_ids"] == [study_id_b]

    second = create_study_set("Storage Roundtrip Set v3", [study_id_a])
    reorder_study_sets([second["id"], created["id"]])

    sets = list_study_sets()
    assert [entry["id"] for entry in sets[:2]] == [second["id"], created["id"]]
    assert sets[0]["color_token"] is None
    assert sets[1]["color_token"] == "blue"


def test_save_wfa_study_persists_stitched_consistency_scores():
    wf_result = _build_dummy_wfa_result()
    curve = [100.0, 104.0, 108.0, 112.0, 116.0, 120.0, 119.0, 117.0, 115.0]
    timestamps = [pd.Timestamp(f"2025-03-{day:02d}", tz="UTC") for day in range(1, 10)]
    wf_result.stitched_oos.equity_curve = curve
    wf_result.stitched_oos.timestamps = timestamps
    wf_result.stitched_oos.window_ids = list(range(1, 10))

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    loaded = load_study_from_db(study_id)
    assert loaded is not None
    expected_full = _calculate_r2_consistency(curve)
    expected_recent = _calculate_r2_consistency(curve[-3:])

    study = loaded["study"]
    stitched = loaded["stitched_oos"]
    assert study.get("stitched_oos_consistency_full") == pytest.approx(expected_full, abs=1e-6)
    assert study.get("stitched_oos_consistency_recent") == pytest.approx(expected_recent, abs=1e-6)
    assert stitched.get("consistency_full") == pytest.approx(expected_full, abs=1e-6)
    assert stitched.get("consistency_recent") == pytest.approx(expected_recent, abs=1e-6)


def test_load_study_backfills_missing_stitched_consistency_for_legacy_rows():
    with _temporary_active_db("storage_stitched_consistency_backfill"):
        curve = [100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 109.0, 107.0, 105.0]
        timestamps = [f"2025-04-{day:02d}T00:00:00+00:00" for day in range(1, 10)]
        expected_full = _calculate_r2_consistency(curve)
        expected_recent = _calculate_r2_consistency(curve[-3:])

        with get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO studies (
                    study_id,
                    study_name,
                    strategy_id,
                    optimization_mode,
                    stitched_oos_equity_curve,
                    stitched_oos_timestamps_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "legacy_wfa_consistency",
                    "LEGACY_WFA_CONSISTENCY",
                    "s01_trailing_ma",
                    "wfa",
                    json.dumps(curve),
                    json.dumps(timestamps),
                ),
            )
            conn.commit()

        loaded = load_study_from_db("legacy_wfa_consistency")
        assert loaded is not None
        assert loaded["study"].get("stitched_oos_consistency_full") == pytest.approx(expected_full, abs=1e-6)
        assert loaded["study"].get("stitched_oos_consistency_recent") == pytest.approx(expected_recent, abs=1e-6)

        with get_db_connection() as conn:
            row = conn.execute(
                """
                SELECT
                    stitched_oos_start_ts,
                    stitched_oos_end_ts,
                    stitched_oos_point_count,
                    stitched_oos_consistency_full,
                    stitched_oos_consistency_recent
                FROM studies
                WHERE study_id = ?
                """,
                ("legacy_wfa_consistency",),
            ).fetchone()

        assert row is not None
        assert row["stitched_oos_start_ts"] == timestamps[0]
        assert row["stitched_oos_end_ts"] == timestamps[-1]
        assert row["stitched_oos_point_count"] == len(timestamps)
        assert row["stitched_oos_consistency_full"] == pytest.approx(expected_full, abs=1e-6)
        assert row["stitched_oos_consistency_recent"] == pytest.approx(expected_recent, abs=1e-6)


def test_study_sets_reject_invalid_color_token():
    wf_result = _build_dummy_wfa_result()
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    created = create_study_set("Invalid Color Set", [study_id])
    with pytest.raises(ValueError):
        update_study_set(created["id"], color_token="magenta")


def test_study_sets_color_token_can_be_cleared():
    wf_result = _build_dummy_wfa_result()
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    created = create_study_set("Clear Color Set", [study_id], color_token="teal")
    assert created["color_token"] == "teal"

    cleared = update_study_set(created["id"], color_token=None)
    assert cleared["color_token"] is None


def test_study_sets_rename_auto_suffixes_duplicate_names():
    study_id = save_wfa_study_to_db(
        wf_result=_build_dummy_wfa_result(),
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    first = create_study_set("Rename Duplicate", [study_id])
    second = create_study_set("Rename Duplicate", [study_id])
    target = create_study_set("Rename Target", [study_id])

    updated = update_study_set(target["id"], name="Rename Duplicate")
    assert updated["name"] == "Rename Duplicate (2)"

    same_name = update_study_set(second["id"], name="Rename Duplicate")
    assert same_name["name"] == "Rename Duplicate (1)"

    by_id = {entry["id"]: entry for entry in list_study_sets()}
    assert by_id[first["id"]]["name"] == "Rename Duplicate"
    assert by_id[second["id"]]["name"] == "Rename Duplicate (1)"
    assert by_id[target["id"]]["name"] == "Rename Duplicate (2)"


def test_study_sets_bulk_color_update_and_delete():
    first_study = save_wfa_study_to_db(
        wf_result=_build_dummy_wfa_result(),
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    second_study = save_wfa_study_to_db(
        wf_result=_build_dummy_wfa_result(),
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    first = create_study_set("Bulk Color First", [first_study], color_token="blue")
    second = create_study_set("Bulk Color Second", [second_study], color_token="teal")

    updated = update_study_sets_color([first["id"], second["id"]], "rose")
    assert [item["id"] for item in updated] == [first["id"], second["id"]]
    assert [item["color_token"] for item in updated] == ["rose", "rose"]

    deleted_count = delete_study_sets([first["id"], second["id"]])
    assert deleted_count == 2
    remaining_ids = {entry["id"] for entry in list_study_sets()}
    assert first["id"] not in remaining_ids
    assert second["id"] not in remaining_ids


def test_study_set_analytics_cache_roundtrip_and_invalidation():
    with _temporary_active_db("storage_cache_roundtrip"):
        first_result = _build_dummy_wfa_result()
        first_study_id = save_wfa_study_to_db(
            wf_result=first_result,
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )

        second_result = _build_dummy_wfa_result()
        second_result.windows[0].window_id = 2
        second_result.windows[0].param_id = "EMA 75_test"
        second_result.windows[0].best_params = {"maType": "EMA", "maLength": 75, "closeCountLong": 7}
        second_result.windows[0].oos_net_profit_pct = 5.0
        second_result.windows[0].oos_equity_curve = [100.0, 105.0]
        second_result.stitched_oos.final_net_profit_pct = 5.0
        second_result.stitched_oos.equity_curve = [100.0, 105.0]
        second_study_id = save_wfa_study_to_db(
            wf_result=second_result,
            config={},
            csv_file_path="",
            start_time=1.0,
            score_config=None,
        )

        created = create_study_set("Cache Set", [first_study_id, second_study_id])

        initial_cache = get_or_build_study_set_analytics_cache(created["id"])
        repeated_cache = get_or_build_study_set_analytics_cache(created["id"])
        assert initial_cache["selected_count"] == 2
        assert initial_cache["has_curve"] is True
        assert initial_cache["computed_at"] == repeated_cache["computed_at"]
        assert len(initial_cache["curve"]) == len(initial_cache["timestamps"]) == 2

        updated = update_study_set(created["id"], study_ids=[first_study_id])
        refreshed_cache = get_or_build_study_set_analytics_cache(updated["id"])
        assert refreshed_cache["selected_count"] == 1
        assert refreshed_cache["profit_pct"] == pytest.approx(2.0)
        assert refreshed_cache["computed_at"] != initial_cache["computed_at"]

        sets_payload = list_study_sets_with_analytics_cache()
        assert sets_payload["all_metrics"]["selected_count"] == 2
        assert sets_payload["sets"][0]["metrics"]["selected_count"] == 1
        assert sets_payload["sets"][0]["metrics"]["profit_pct"] == pytest.approx(2.0)


def test_study_set_analytics_cache_includes_recent_and_full_consistency():
    with _temporary_active_db("storage_cache_consistency"):
        curve = [100.0, 104.0, 108.0, 112.0, 116.0, 120.0, 119.0, 117.0, 115.0]
        timestamps = [pd.Timestamp(f"2025-01-{day:02d}", tz="UTC") for day in range(1, 10)]

        wf_result = _build_dummy_wfa_result()
        wf_result.windows[0].window_id = 11
        wf_result.windows[0].oos_equity_curve = curve
        wf_result.windows[0].oos_timestamps = timestamps
        wf_result.stitched_oos.equity_curve = curve
        wf_result.stitched_oos.timestamps = timestamps
        wf_result.stitched_oos.final_net_profit_pct = 15.0
        wf_result.stitched_oos.max_drawdown_pct = 4.1667

        study_id = save_wfa_study_to_db(
            wf_result=wf_result,
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )

        created = create_study_set("Consistency Set", [study_id])
        cache_payload = get_or_build_study_set_analytics_cache(created["id"])
        summary_payload = list_study_sets_with_analytics_cache()["sets"][0]["metrics"]

        expected_full = _calculate_r2_consistency(curve)
        expected_recent = _calculate_r2_consistency(curve[-3:])

        assert cache_payload["consistency_full"] == pytest.approx(expected_full, abs=1e-6)
        assert cache_payload["consistency_recent"] == pytest.approx(expected_recent, abs=1e-6)
        assert summary_payload["consistency_full"] == pytest.approx(expected_full, abs=1e-6)
        assert summary_payload["consistency_recent"] == pytest.approx(expected_recent, abs=1e-6)


def test_study_set_analytics_cache_legacy_rows_compute_missing_consistency():
    with _temporary_active_db("storage_cache_legacy_consistency"):
        curve = [100.0, 104.0, 108.0, 112.0, 116.0, 120.0, 119.0, 117.0, 115.0]
        timestamps = [pd.Timestamp(f"2025-02-{day:02d}", tz="UTC") for day in range(1, 10)]

        wf_result = _build_dummy_wfa_result()
        wf_result.windows[0].window_id = 12
        wf_result.windows[0].oos_equity_curve = curve
        wf_result.windows[0].oos_timestamps = timestamps
        wf_result.stitched_oos.equity_curve = curve
        wf_result.stitched_oos.timestamps = timestamps
        wf_result.stitched_oos.final_net_profit_pct = 15.0
        wf_result.stitched_oos.max_drawdown_pct = 4.1667

        study_id = save_wfa_study_to_db(
            wf_result=wf_result,
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )

        created = create_study_set("Legacy Consistency Set", [study_id])
        initial_cache = get_or_build_study_set_analytics_cache(created["id"])

        with get_db_connection() as conn:
            conn.execute(
                """
                UPDATE analytics_group_cache
                SET
                    consistency_full = NULL,
                    consistency_recent = NULL
                WHERE group_key = ?
                """,
                (f"set:{created['id']}",),
            )
            conn.commit()

        refreshed_cache = get_or_build_study_set_analytics_cache(created["id"])
        summary_payload = list_study_sets_with_analytics_cache()["sets"][0]["metrics"]

        assert refreshed_cache["computed_at"] == initial_cache["computed_at"]
        assert refreshed_cache["consistency_full"] == pytest.approx(
            initial_cache["consistency_full"],
            abs=1e-6,
        )
        assert refreshed_cache["consistency_recent"] == pytest.approx(
            initial_cache["consistency_recent"],
            abs=1e-6,
        )
        assert summary_payload["consistency_full"] == pytest.approx(
            initial_cache["consistency_full"],
            abs=1e-6,
        )
        assert summary_payload["consistency_recent"] == pytest.approx(
            initial_cache["consistency_recent"],
            abs=1e-6,
        )


def test_all_studies_analytics_cache_invalidates_after_new_wfa_study_saved():
    with _temporary_active_db("storage_all_cache_invalidation"):
        save_wfa_study_to_db(
            wf_result=_build_dummy_wfa_result(),
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )
        first_cache = get_or_build_all_studies_analytics_cache()
        assert first_cache["selected_count"] == 1
        assert first_cache["has_curve"] is True

        second_result = _build_dummy_wfa_result()
        second_result.windows[0].window_id = 2
        second_result.windows[0].param_id = "EMA 90_test"
        second_result.windows[0].best_params = {"maType": "EMA", "maLength": 90, "closeCountLong": 7}
        save_wfa_study_to_db(
            wf_result=second_result,
            config={},
            csv_file_path="",
            start_time=2.0,
            score_config=None,
        )

        second_cache = get_or_build_all_studies_analytics_cache()
        assert second_cache["selected_count"] == 2
        assert second_cache["computed_at"] != first_cache["computed_at"]


def test_save_wfa_study_layer1_aggregates_multi_window():
    wf_config = WFConfig(strategy_id="s01_trailing_ma", is_period_days=10, oos_period_days=5)
    windows = [
        WindowResult(
            window_id=1,
            is_start=pd.Timestamp("2025-01-01", tz="UTC"),
            is_end=pd.Timestamp("2025-01-10", tz="UTC"),
            oos_start=pd.Timestamp("2025-01-11", tz="UTC"),
            oos_end=pd.Timestamp("2025-01-15", tz="UTC"),
            best_params={"maType": "EMA", "maLength": 50, "closeCountLong": 7},
            param_id="p1",
            is_net_profit_pct=1.0,
            is_max_drawdown_pct=1.0,
            is_total_trades=2,
            oos_net_profit_pct=6.0,
            oos_max_drawdown_pct=12.0,
            oos_total_trades=4,
            oos_winning_trades=3,
            oos_equity_curve=[100.0, 106.0],
            oos_timestamps=[
                pd.Timestamp("2025-01-11", tz="UTC"),
                pd.Timestamp("2025-01-15", tz="UTC"),
            ],
            oos_win_rate=75.0,
        ),
        WindowResult(
            window_id=2,
            is_start=pd.Timestamp("2025-01-06", tz="UTC"),
            is_end=pd.Timestamp("2025-01-15", tz="UTC"),
            oos_start=pd.Timestamp("2025-01-16", tz="UTC"),
            oos_end=pd.Timestamp("2025-01-20", tz="UTC"),
            best_params={"maType": "EMA", "maLength": 50, "closeCountLong": 7},
            param_id="p1",
            is_net_profit_pct=1.0,
            is_max_drawdown_pct=1.0,
            is_total_trades=2,
            oos_net_profit_pct=-2.0,
            oos_max_drawdown_pct=30.0,
            oos_total_trades=5,
            oos_winning_trades=1,
            oos_equity_curve=[100.0, 98.0],
            oos_timestamps=[
                pd.Timestamp("2025-01-16", tz="UTC"),
                pd.Timestamp("2025-01-20", tz="UTC"),
            ],
            oos_win_rate=20.0,
        ),
    ]
    stitched = OOSStitchedResult(
        final_net_profit_pct=3.88,
        max_drawdown_pct=8.0,
        total_trades=9,
        wfe=10.0,
        oos_win_rate=50.0,
        equity_curve=[100.0, 106.0, 103.88],
        timestamps=[
            pd.Timestamp("2025-01-11", tz="UTC"),
            pd.Timestamp("2025-01-15", tz="UTC"),
            pd.Timestamp("2025-01-20", tz="UTC"),
        ],
        window_ids=[1, 1, 2],
    )
    wf_result = WFResult(
        config=wf_config,
        windows=windows,
        stitched_oos=stitched,
        strategy_id="s01_trailing_ma",
        total_windows=2,
        trading_start_date=pd.Timestamp("2025-01-01", tz="UTC"),
        trading_end_date=pd.Timestamp("2025-01-20", tz="UTC"),
        warmup_bars=wf_config.warmup_bars,
    )

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    loaded = load_study_from_db(study_id)
    assert loaded is not None
    study = loaded["study"]

    assert study.get("stitched_oos_winning_trades") == 4
    assert study.get("profitable_windows") == 1
    assert study.get("total_windows") == 2
    assert study.get("median_window_profit") == 2.0
    assert study.get("median_window_wr") == 47.5
    assert study.get("worst_window_profit") == -2.0
    assert study.get("worst_window_dd") == 30.0


def test_save_wfa_study_persists_optuna_and_wfa_metadata():
    wf_result = _build_dummy_wfa_result()
    wf_result.config.is_period_days = 12
    wf_result.config.adaptive_mode = True
    wf_result.config.max_oos_period_days = 120
    wf_result.config.min_oos_trades = 7
    wf_result.config.check_interval_trades = 4
    wf_result.config.cusum_threshold = 6.5
    wf_result.config.dd_threshold_multiplier = 1.8
    wf_result.config.inactivity_multiplier = 6.0
    wf_result.config.cooldown_enabled = True
    wf_result.config.cooldown_days = 15
    wf_result.config.post_process = PostProcessConfig(
        enabled=True,
        ft_period_days=14,
        top_k=10,
        sort_metric="profit_degradation",
        ft_threshold_pct=-5.0,
        ft_reject_action="cooldown_reoptimize",
        ft_reject_cooldown_days=5,
        ft_reject_max_attempts=2,
        ft_reject_min_remaining_oos_days=10,
    )
    wf_result.config.dsr_config = DSRConfig(enabled=True, top_k=18)
    wf_result.config.stress_test_config = StressTestConfig(
        enabled=True,
        top_k=7,
        failure_threshold=0.65,
        sort_metric="profit_retention",
    )
    wf_result.windows[0].trigger_type = "cusum"
    wf_result.windows[0].oos_actual_days = 4.0
    wf_result.windows[0].cooldown_days_applied = 15.0
    wf_result.windows[0].oos_elapsed_days = 19.0
    wf_result.windows[0].trade_start = pd.Timestamp("2025-01-13", tz="UTC")
    wf_result.windows[0].trade_end = pd.Timestamp("2025-01-15", tz="UTC")
    wf_result.windows[0].entry_delay_days = 2.0
    wf_result.windows[0].ft_retry_attempts_used = 1
    wf_result.windows[0].remaining_oos_days_at_entry = 3.0
    wf_result.windows[0].window_status = "traded"

    config = {
        "sampler_type": "nsga2",
        "population_size": 64,
        "crossover_prob": 0.8,
        "mutation_prob": 0.2,
        "swapping_prob": 0.4,
        "optuna_config": {
            "budget_mode": "trials",
            "n_trials": 300,
            "time_limit": 1800,
            "convergence_patience": 75,
            "sampler": "nsga2",
            "sampler_type": "nsga2",
            "population_size": 64,
            "crossover_prob": 0.8,
            "mutation_prob": 0.2,
            "swapping_prob": 0.4,
            "pruner": "median",
        },
        "wfa": {
            "is_period_days": 10,
            "oos_period_days": 5,
            "adaptive_mode": True,
            "cooldown_enabled": True,
            "cooldown_days": 15,
        },
    }

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config=config,
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    loaded = load_study_from_db(study_id)
    assert loaded is not None
    study = loaded["study"]

    assert study.get("is_period_days") == 12
    assert study.get("sampler_type") == "nsga2"
    assert study.get("population_size") == 64
    assert study.get("crossover_prob") == 0.8
    assert study.get("mutation_prob") == 0.2
    assert study.get("swapping_prob") == 0.4
    assert study.get("budget_mode") == "trials"
    assert study.get("n_trials") == 300
    assert study.get("time_limit") == 1800
    assert study.get("convergence_patience") == 75

    assert study.get("adaptive_mode") == 1
    assert study.get("max_oos_period_days") == 120
    assert study.get("min_oos_trades") == 7
    assert study.get("check_interval_trades") == 4
    assert study.get("cusum_threshold") == 6.5
    assert study.get("dd_threshold_multiplier") == 1.8
    assert study.get("inactivity_multiplier") == 6.0
    assert study.get("cooldown_enabled") == 1
    assert study.get("cooldown_days") == 15
    assert study.get("ft_enabled") == 1
    assert study.get("ft_period_days") == 14
    assert study.get("ft_top_k") == 10
    assert study.get("ft_sort_metric") == "profit_degradation"
    assert study.get("ft_threshold_pct") == -5.0
    assert study.get("ft_reject_action") == "cooldown_reoptimize"
    assert study.get("ft_reject_cooldown_days") == 5
    assert study.get("ft_reject_max_attempts") == 2
    assert study.get("ft_reject_min_remaining_oos_days") == 10
    assert study.get("dsr_enabled") == 1
    assert study.get("dsr_top_k") == 18
    assert study.get("st_enabled") == 1
    assert study.get("st_top_k") == 7
    assert study.get("st_failure_threshold") == 0.65
    assert study.get("st_sort_metric") == "profit_retention"

    config_json = study.get("config_json") or {}
    assert config_json.get("optuna_config", {}).get("pruner") == "median"
    assert config_json.get("wfa", {}).get("oos_period_days") == 5
    assert config_json.get("wfa", {}).get("cooldown_enabled") is True
    assert config_json.get("wfa", {}).get("cooldown_days") == 15

    window = loaded["windows"][0]
    assert window.get("trigger_type") == "cusum"
    assert window.get("oos_actual_days") == 4.0
    assert window.get("cooldown_days_applied") == 15.0
    assert window.get("oos_elapsed_days") == 19.0
    assert window.get("trade_start_ts") == "2025-01-13T00:00:00+00:00"
    assert window.get("trade_end_ts") == "2025-01-15T00:00:00+00:00"
    assert window.get("entry_delay_days") == 2.0
    assert window.get("ft_retry_attempts_used") == 1
    assert window.get("remaining_oos_days_at_entry") == 3.0
    assert window.get("window_status") == "traded"
    assert window.get("is_sharpe_daily") == 1.25
    assert window.get("is_sharpe_daily_observations") == 7
    assert window.get("is_sharpe_daily_active_days") == 0
    assert window.get("oos_sharpe_daily") is None
    assert window.get("oos_sharpe_daily_observations") == 4
    assert window.get("oos_sharpe_daily_active_days") == 0

    window_trials = load_wfa_window_trials(window["window_id"])["optuna_is"]
    assert window_trials[0]["sharpe_daily"] == 1.25
    assert window_trials[0]["sharpe_daily_observations"] == 7
    assert window_trials[0]["sharpe_daily_active_days"] == 0


def test_save_wfa_study_persists_runtime_seconds():
    wf_result = _build_dummy_wfa_result()
    start_time = time.time() - 2.0
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=start_time,
        score_config=None,
    )

    loaded = load_study_from_db(study_id)
    assert loaded is not None
    runtime = loaded["study"].get("optimization_time_seconds")
    assert runtime is not None
    assert runtime >= 0
    assert runtime < 600


@pytest.mark.parametrize("invalid", [1.5, "7", True, -1])
def test_wfa_storage_rejects_malformed_daily_diagnostics(invalid):
    wf_result = _build_dummy_wfa_result()
    wf_result.windows[0].is_sharpe_daily_observations = invalid

    with pytest.raises(RuntimeError, match="is_sharpe_daily_observations"):
        save_wfa_study_to_db(
            wf_result=wf_result,
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )


def test_load_wfa_window_trials():
    wf_result = _build_dummy_wfa_result()
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    window_id = f"{study_id}_w1"
    modules = load_wfa_window_trials(window_id)
    assert "optuna_is" in modules
    assert modules["optuna_is"]
    assert modules["optuna_is"][0]["trial_number"] == 1


def test_wfa_window_timestamp_precision_persisted():
    wf_result = _build_dummy_wfa_result()
    window = wf_result.windows[0]
    window.is_start = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    window.is_end = pd.Timestamp("2025-01-10 09:15:00", tz="UTC")
    window.oos_start = pd.Timestamp("2025-01-11 06:45:00", tz="UTC")
    window.oos_end = pd.Timestamp("2025-01-15 12:30:00", tz="UTC")
    window.optimization_start = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    window.optimization_end = pd.Timestamp("2025-01-09 23:00:00", tz="UTC")
    window.ft_start = pd.Timestamp("2025-01-09 23:00:00", tz="UTC")
    window.ft_end = pd.Timestamp("2025-01-10 09:15:00", tz="UTC")

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    loaded = load_study_from_db(study_id)
    assert loaded is not None
    stored = loaded["windows"][0]

    assert stored.get("is_start_ts") == "2025-01-01T00:00:00+00:00"
    assert stored.get("is_end_ts") == "2025-01-10T09:15:00+00:00"
    assert stored.get("oos_start_ts") == "2025-01-11T06:45:00+00:00"
    assert stored.get("oos_end_ts") == "2025-01-15T12:30:00+00:00"
    assert stored.get("optimization_start_ts") == "2025-01-01T00:00:00+00:00"
    assert stored.get("optimization_end_ts") == "2025-01-09T23:00:00+00:00"
    assert stored.get("ft_start_ts") == "2025-01-09T23:00:00+00:00"
    assert stored.get("ft_end_ts") == "2025-01-10T09:15:00+00:00"
