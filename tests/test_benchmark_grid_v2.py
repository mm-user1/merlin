import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import benchmark_grid_v2 as benchmark


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG = REPO_ROOT / "tools/benchmark_configs/s06_b2_sui_baseline_grid.json"
CORRECTED_DB = REPO_ROOT / "src/storage/2026-07-06_233217_backtester-v2-test.db"


def _minimal_payload():
    return {
        "strategy": "s06_r_trend_v02_b2",
        "optimization_mode": "grid",
        "enabled_params": {"stopX": True},
        "param_ranges": {"stopX": [1.0, 3.0, 0.5]},
        "fixed_params": {
            "dateFilter": True,
            "start": "2025-08-01T00:00:00Z",
            "end": "2025-12-01T00:00:00Z",
        },
        "objectives": ["net_profit_pct"],
        "primary_objective": "net_profit_pct",
        "grid_needs_dsr": False,
    }


def _create_wfa_fixture_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE studies (
                study_id TEXT PRIMARY KEY,
                study_name TEXT,
                strategy_id TEXT,
                optimization_mode TEXT,
                optimizer_mode TEXT,
                config_json TEXT,
                csv_file_path TEXT,
                csv_file_name TEXT,
                total_windows INTEGER,
                optimization_time_seconds INTEGER,
                stitched_oos_net_profit_pct REAL,
                stitched_oos_max_drawdown_pct REAL,
                stitched_oos_total_trades INTEGER,
                stitched_oos_winning_trades INTEGER,
                stitched_oos_win_rate REAL,
                grid_summary_json TEXT,
                created_at TEXT
            );
            CREATE TABLE wfa_windows (
                study_id TEXT,
                window_number INTEGER,
                grid_valid_candidate_count INTEGER,
                grid_selected_candidate_count INTEGER,
                module_status_json TEXT
            );
            """
        )
        base_config = {
            "optimization_mode": "grid",
            "worker_processes": 6,
            "grid_budget": 48480,
            "grid_top_candidates": 10,
            "grid_enabled_modes": ["bracket", "trail"],
            "fixed_params": {"trailMAType_options": ["SMA", "HMA", "KAMA", "T3"]},
        }
        conn.execute(
            """
            INSERT INTO studies VALUES (
                'old-study', 'Old WFA', 's06_r_trend_v02_b2', 'wfa', 'grid', ?,
                'external.csv', 'external.csv', 1, 160, 10.5, 3.2, 42, 21, 50.0, '', '2026-01-01'
            )
            """,
            (json.dumps(base_config),),
        )
        conn.execute(
            """
            INSERT INTO studies VALUES (
                'new-study', 'New WFA', 's06_r_trend_v02_b2', 'wfa', 'grid', ?,
                'external.csv', 'external.csv', 2, 120, 10.5, 3.2, 42, 21, 50.0, '', '2026-01-02'
            )
            """,
            (json.dumps(base_config),),
        )
        conn.execute(
            """
            INSERT INTO studies VALUES (
                'partial-study', 'Partial WFA', 's06_r_trend_v02_b2', 'wfa', 'grid', ?,
                'external.csv', 'external.csv', 2, 140, 10.5, 3.2, 42, 21, 50.0, '', '2026-01-03'
            )
            """,
            (json.dumps(base_config),),
        )
        conn.execute(
            """
            INSERT INTO wfa_windows VALUES (
                'old-study', 1, 48480, 10, '{"optuna_is": {"enabled": true}}'
            )
            """
        )
        diagnostics_1 = {
            "grid_v2": {
                "engine": "v2",
                "backend_kind": "compiled_numba",
                "compiled_batch_used": True,
                "compiled_workers": 6,
                "candidate_count": 48480,
                "valid_candidate_count": 48480,
                "selected_candidate_count": 10,
                "candidate_generation_seconds": 0.1,
                "plan_build_seconds": 0.08,
                "plan_reuse_lookup_seconds": 0.01,
                "runtime_rebase_seconds": 0.0,
                "data_prepare_seconds": 0.2,
                "fast_evaluation_seconds": 0.3,
                "fast_result_materialization_seconds": 0.04,
                "ranking_seconds": 0.05,
                "slow_validation_seconds": 0.4,
                "slow_refinement_seconds": 0.05,
                "total_seconds": 1.0,
                "candidates_per_second": 1234.5,
                "plan_reuse_enabled": True,
                "plan_reuse_hit": False,
                "plan_build_count": 1,
                "plan_reuse_hit_count": 0,
                "plan_reuse_miss_count": 1,
                "chunk_count": 1,
                "max_chunk_candidates": 48480,
                "max_chunk_estimated_mb": 42.0,
                "chunk_estimated_mb": 42.0,
                "configured_limit_mb": 512.0,
                "full_run_estimated_signal_mb": 9.25,
                "signal_stack_rows_built": 162,
                "signal_stack_rows_peak": 162,
                "full_population_result_object_note": "Full-population materialization remains O(candidates).",
            }
        }
        diagnostics_2 = {
            "grid_v2": {
                "engine": "v2",
                "backend_kind": "compiled_numba",
                "compiled_batch_used": True,
                "compiled_workers": 6,
                "candidate_count": 48480,
                "valid_candidate_count": 48480,
                "selected_candidate_count": 10,
                "candidate_generation_seconds": 0.3,
                "plan_build_seconds": 0.0,
                "plan_reuse_lookup_seconds": 0.02,
                "runtime_rebase_seconds": 0.03,
                "data_prepare_seconds": 0.4,
                "fast_evaluation_seconds": 0.7,
                "fast_result_materialization_seconds": 0.06,
                "ranking_seconds": 0.07,
                "slow_validation_seconds": 0.8,
                "slow_refinement_seconds": 0.07,
                "total_seconds": 2.2,
                "candidates_per_second": 2469.0,
                "plan_reuse_enabled": True,
                "plan_reuse_hit": True,
                "plan_build_count": 1,
                "plan_reuse_hit_count": 1,
                "plan_reuse_miss_count": 1,
                "chunk_count": 2,
                "max_chunk_candidates": 24240,
                "max_chunk_estimated_mb": 21.0,
                "chunk_estimated_mb": 21.0,
                "configured_limit_mb": 512.0,
                "full_run_estimated_signal_mb": 9.25,
                "signal_stack_rows_built": 324,
                "signal_stack_rows_peak": 162,
                "full_population_result_object_note": "Full-population materialization remains O(candidates).",
            }
        }
        conn.execute(
            "INSERT INTO wfa_windows VALUES ('new-study', 1, 48480, 10, ?)",
            (json.dumps(diagnostics_1),),
        )
        conn.execute(
            "INSERT INTO wfa_windows VALUES ('new-study', 2, 48480, 10, ?)",
            (json.dumps(diagnostics_2),),
        )
        partial_diagnostics = {
            "grid_v2": {
                "engine": "v2",
                "backend_kind": "compiled_numba",
                "compiled_batch_used": True,
                "compiled_workers": 6,
                "candidate_generation_seconds": 0.5,
                "data_prepare_seconds": 0.6,
                "fast_evaluation_seconds": 0.7,
                "slow_validation_seconds": 0.8,
                "total_seconds": 2.6,
            }
        }
        conn.execute(
            "INSERT INTO wfa_windows VALUES ('partial-study', 1, 48480, 10, ?)",
            (json.dumps(partial_diagnostics),),
        )
        conn.execute(
            """
            INSERT INTO wfa_windows VALUES (
                'partial-study', 2, 48480, 10, '{"optuna_is": {"enabled": true}}'
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_example_config_builds_through_canonical_builder_and_previews_full_count():
    payload = benchmark.load_benchmark_payload(EXAMPLE_CONFIG)
    csv_path = benchmark.resolve_csv_path(payload)

    config = benchmark.build_direct_grid_config(
        payload,
        csv_path=csv_path,
        worker_processes=1,
        strategy_id="s06_r_trend_v02_b2",
        warmup_bars=1000,
    )
    count, preview = benchmark.preview_candidate_count(config)

    assert count == 48_480
    assert preview["mode_space_sizes"] == {"bracket": 480, "trail": 48_000}
    assert config.fixed_params["dateFilter"] is True
    assert config.fixed_params["start"] == "2025-08-01T00:00:00Z"
    assert config.fixed_params["end"] == "2025-12-01T00:00:00Z"
    assert config.fixed_params["trailMAType_options"] == ["SMA", "HMA", "KAMA", "T3"]
    assert config.warmup_bars == 1000
    assert config.worker_processes == 1


def test_missing_required_payload_fields_fail_clearly():
    with pytest.raises(ValueError, match="enabled_params"):
        benchmark.validate_direct_grid_payload({"optimization_mode": "grid", "fixed_params": {}})


def test_build_direct_grid_config_uses_canonical_builder(monkeypatch, tmp_path):
    payload = _minimal_payload()
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("time,Open,High,Low,Close,Volume\n", encoding="utf-8")
    calls = []

    def fake_builder(*, csv_file, payload, worker_processes, strategy_id, warmup_bars):
        calls.append(
            {
                "csv_file": csv_file,
                "payload": payload,
                "worker_processes": worker_processes,
                "strategy_id": strategy_id,
                "warmup_bars": warmup_bars,
            }
        )
        return SimpleNamespace(
            strategy_id=strategy_id,
            fixed_params=payload["fixed_params"],
            warmup_bars=warmup_bars,
            worker_processes=worker_processes,
        )

    monkeypatch.setattr(benchmark, "build_optimization_config_via_ui", fake_builder)

    config = benchmark.build_direct_grid_config(
        payload,
        csv_path=csv_path,
        worker_processes=6,
        strategy_id="s06_r_trend_v02_b2",
        warmup_bars=1000,
    )

    assert calls
    assert calls[0]["csv_file"] == csv_path
    assert calls[0]["worker_processes"] == 6
    assert calls[0]["strategy_id"] == "s06_r_trend_v02_b2"
    assert calls[0]["warmup_bars"] == 1000
    assert config.fixed_params["start"] == "2025-08-01T00:00:00Z"


def test_direct_benchmark_reads_summary_from_config_and_top_selected_result(monkeypatch, tmp_path):
    payload = _minimal_payload()
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("time,Open,High,Low,Close,Volume\n", encoding="utf-8")
    config = SimpleNamespace(
        strategy_id="s06_r_trend_v02_b2",
        fixed_params=payload["fixed_params"],
        warmup_bars=1000,
        worker_processes=6,
    )

    def fake_build_config(*args, **kwargs):  # noqa: ARG001
        return config

    def fake_preview(config_arg):  # noqa: ARG001
        return {"full_candidate_count": 48_480, "mode_space_sizes": {"bracket": 480, "trail": 48_000}}

    def fake_run(config_arg, *, save_study):
        assert save_study is False
        config_arg.grid_summary = {
            "engine": "v2",
            "candidate_count": 48_480,
            "valid_candidate_count": 48_480,
            "selected_candidate_count": 1,
            "grid": {
                "backend_kind": "compiled_numba",
                "compiled_batch_used": True,
                "compiled_workers": 6,
                "candidate_count": 48_480,
                "valid_candidate_count": 48_480,
                "selected_candidate_count": 1,
                "cache_estimate": {"estimated_total_mb": 12.5},
                "cache_stats": {"signal_misses": 1, "dataprep_misses": 2},
                "timings": {
                    "candidate_generation_seconds": 0.1,
                    "plan_build_seconds": 0.08,
                    "plan_reuse_lookup_seconds": 0.0,
                    "runtime_rebase_seconds": 0.0,
                    "data_prepare_seconds": 0.2,
                    "fast_evaluation_seconds": 0.3,
                    "fast_result_materialization_seconds": 0.04,
                    "ranking_seconds": 0.05,
                    "slow_validation_seconds": 0.4,
                    "slow_refinement_seconds": 0.05,
                    "total_seconds": 1.0,
                },
                "candidates_per_second": 1234.5,
                "chunk_count": 2,
                "max_chunk_candidates": 30000,
                "max_chunk_estimated_mb": 510.5,
                "chunk_estimated_mb": 510.5,
                "configured_limit_mb": 512.0,
                "full_run_estimated_signal_mb": 600.0,
                "signal_stack_rows_built": 60000,
                "signal_stack_rows_peak": 30000,
                "full_population_result_object_note": "Full-population materialization remains O(candidates).",
            },
        }
        selected = SimpleNamespace(
            candidate_id=7,
            optuna_trial_number=7,
            params={"stopX": 2.0},
            net_profit_pct=12.3,
            max_drawdown_pct=4.5,
            total_trades=9,
            objective_values=[12.3],
            constraint_values=[],
            constraints_satisfied=True,
        )
        return [selected], {"not": "the_summary"}

    monkeypatch.setattr(benchmark, "build_direct_grid_config", fake_build_config)
    monkeypatch.setattr(benchmark, "preview_grid_parameter_space", fake_preview)
    monkeypatch.setattr(benchmark, "run_grid_optimization", fake_run)
    monkeypatch.setattr(benchmark, "collect_environment_metadata", lambda: {"python": "test"})

    report = benchmark.run_direct_grid_benchmark(
        payload=payload,
        csv_path=csv_path,
        workers=[6],
        warmup_runs=0,
        runs=1,
        strategy_id="s06_r_trend_v02_b2",
        warmup_bars=1000,
        expected_candidate_count=48_480,
    )

    json.dumps(report)
    run = report["runs"][0]
    assert report["schema_version"] == 1
    assert run["candidate_count"] == 48_480
    assert run["timings"]["slow_refinement_seconds"] == pytest.approx(0.05)
    assert run["timing_fields"]["plan_build_seconds"] == pytest.approx(0.08)
    assert run["timing_fields"]["fast_result_materialization_seconds"] == pytest.approx(0.04)
    assert run["timing_fields"]["ranking_seconds"] == pytest.approx(0.05)
    assert run["chunk_fields"]["chunk_count"] == 2
    assert run["chunk_fields"]["max_chunk_candidates"] == 30000
    assert run["chunk_fields"]["full_run_estimated_signal_mb"] == pytest.approx(600.0)
    assert run["chunk_fields"]["full_population_result_object_note"]
    assert run["candidates_per_second"] == pytest.approx(1234.5)
    assert run["grid_summary"]["grid"]["cache_stats"]["dataprep_misses"] == 2
    assert run["top_result"]["candidate_id"] == 7
    assert run["top_result"]["params"] == {"stopX": 2.0}
    assert run["top_result"]["metrics"]["net_profit_pct"] == pytest.approx(12.3)


def test_inspect_wfa_db_fixture_detects_absent_present_and_partial_diagnostics(tmp_path):
    db_path = tmp_path / "wfa_fixture.db"
    _create_wfa_fixture_db(db_path)

    report = benchmark.inspect_wfa_db(db_path)
    json.dumps(report)

    by_id = {study["study_id"]: study for study in report["studies"]}
    assert by_id["old-study"]["diagnostics"]["status"] == "absent"
    assert by_id["new-study"]["diagnostics"]["status"] == "present"
    assert by_id["partial-study"]["diagnostics"]["status"] == "partial"
    assert by_id["partial-study"]["diagnostics"]["stable_keys_missing"] == [
        "candidate_generation_seconds",
        "candidates_per_second",
        "data_prepare_seconds",
        "fast_evaluation_seconds",
        "slow_validation_seconds",
        "total_seconds",
    ]
    assert by_id["new-study"]["window_counts"]["valid_min"] == 48_480
    assert by_id["new-study"]["select_option_subsets"]["trailMAType_options"] == [
        "SMA",
        "HMA",
        "KAMA",
        "T3",
    ]
    aggregates = by_id["new-study"]["diagnostics"]["timing_aggregates"]
    assert aggregates["candidate_generation_seconds"]["count"] == 2
    assert aggregates["candidate_generation_seconds"]["min"] == pytest.approx(0.1)
    assert aggregates["candidate_generation_seconds"]["max"] == pytest.approx(0.3)
    assert aggregates["candidate_generation_seconds"]["mean"] == pytest.approx(0.2)
    assert aggregates["candidate_generation_seconds"]["sum"] == pytest.approx(0.4)
    assert aggregates["fast_evaluation_seconds"]["count"] == 2
    assert aggregates["fast_evaluation_seconds"]["min"] == pytest.approx(0.3)
    assert aggregates["fast_evaluation_seconds"]["max"] == pytest.approx(0.7)
    assert aggregates["fast_evaluation_seconds"]["mean"] == pytest.approx(0.5)
    assert aggregates["fast_evaluation_seconds"]["sum"] == pytest.approx(1.0)
    assert aggregates["total_seconds"]["sum"] == pytest.approx(3.2)
    assert aggregates["slow_refinement_seconds"]["sum"] == pytest.approx(0.12)
    assert aggregates["plan_build_seconds"]["sum"] == pytest.approx(0.08)
    assert aggregates["plan_reuse_lookup_seconds"]["sum"] == pytest.approx(0.03)
    assert aggregates["runtime_rebase_seconds"]["sum"] == pytest.approx(0.03)
    assert aggregates["fast_result_materialization_seconds"]["sum"] == pytest.approx(0.10)
    assert aggregates["ranking_seconds"]["sum"] == pytest.approx(0.12)
    assert aggregates["candidates_per_second"]["count"] == 2
    assert aggregates["candidates_per_second"]["mean"] == pytest.approx(1851.75)
    assert "sum" not in aggregates["candidates_per_second"]
    plan_reuse = by_id["new-study"]["diagnostics"]["plan_reuse"]
    assert plan_reuse["windows_with_fields"] == 2
    assert plan_reuse["hit_windows"] == 1
    assert plan_reuse["miss_windows"] == 1
    assert plan_reuse["count_aggregates"]["plan_build_count"]["max"] == pytest.approx(1.0)
    assert plan_reuse["count_aggregates"]["plan_reuse_hit_count"]["max"] == pytest.approx(1.0)
    assert plan_reuse["count_aggregates"]["plan_reuse_miss_count"]["max"] == pytest.approx(1.0)
    chunks = by_id["new-study"]["diagnostics"]["chunk_aggregates"]
    assert chunks["chunk_count"]["mean"] == pytest.approx(1.5)
    assert chunks["max_chunk_candidates"]["min"] == pytest.approx(24_240)
    assert chunks["max_chunk_candidates"]["max"] == pytest.approx(48_480)
    assert chunks["max_chunk_estimated_mb"]["mean"] == pytest.approx(31.5)
    assert chunks["full_run_estimated_signal_mb"]["mean"] == pytest.approx(9.25)
    assert by_id["new-study"]["diagnostics"]["full_population_result_object_note_windows"] == 2

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM studies").fetchone()[0] == 3


def test_inspect_wfa_db_readonly_immutable_does_not_create_sidecars_or_mutate(tmp_path):
    db_path = tmp_path / "wfa_fixture.db"
    _create_wfa_fixture_db(db_path)
    sidecars = [
        Path(str(db_path) + "-wal"),
        Path(str(db_path) + "-shm"),
    ]
    before_mtime_ns = db_path.stat().st_mtime_ns
    assert not any(path.exists() for path in sidecars)

    report = benchmark.inspect_wfa_db(db_path)

    assert report["studies"]
    assert db_path.stat().st_mtime_ns == before_mtime_ns
    assert not any(path.exists() for path in sidecars)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM studies").fetchone()[0] == 3


def test_real_corrected_db_inspection_reports_absent_diagnostics():
    if not CORRECTED_DB.exists():
        pytest.skip("Corrected WFA comparison DB is not present in this checkout.")

    report = benchmark.inspect_wfa_db(
        CORRECTED_DB,
        study_ids=["c4662e90-4afc-451e-9964-0e1456efb20f"],
    )

    assert len(report["studies"]) == 1
    study = report["studies"][0]
    assert study["strategy_id"] == "s06_r_trend_v02_b2"
    assert study["diagnostics"]["status"] == "absent"
    assert study["window_counts"]["valid_min"] == 48_480
    assert study["window_counts"]["selected_min"] == 10


def test_cli_help_and_inspect_smoke(tmp_path):
    db_path = tmp_path / "wfa_fixture.db"
    output_path = tmp_path / "inspect.json"
    _create_wfa_fixture_db(db_path)

    help_result = subprocess.run(
        [sys.executable, "tools/benchmark_grid_v2.py", "--help"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert help_result.returncode == 0
    assert "direct-grid" in help_result.stdout
    assert "inspect-wfa-db" in help_result.stdout

    inspect_result = subprocess.run(
        [
            sys.executable,
            "tools/benchmark_grid_v2.py",
            "inspect-wfa-db",
            "--db",
            str(db_path),
            "--output-json",
            str(output_path),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert inspect_result.returncode == 0, inspect_result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert len(payload["studies"]) == 3
