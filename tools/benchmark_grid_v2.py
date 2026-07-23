"""Grid V2 benchmark and WFA diagnostics inspection tooling.

JSON output schema version 1:

{
  "schema_version": 1,
  "command": "direct-grid" | "inspect-wfa-db",
  "environment": {},
  "runs": [
    {
      "worker_processes": 6,
      "run_index": 1,
      "warmup": false,
      "measured_wall_seconds": 0.0,
      "grid_summary": {},
      "timings": {},
      "top_result": {}
    }
  ]
}
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import platform
import sqlite3
import statistics
import sys
import time
from collections.abc import Mapping, Sequence
from importlib import metadata
from pathlib import Path
from typing import Any
from urllib.parse import quote


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

SCHEMA_VERSION = 1
DEFAULT_STRATEGY_ID = "s06_r_trend_v02_b2"
DEFAULT_EXPECTED_S06_B2_CANDIDATES = 48_480
DEFAULT_COMPARISON_DB = Path("src/storage/2026-07-06_233217_backtester-v2-test.db")

STABLE_WFA_GRID_V2_TIMING_KEYS = (
    "candidate_generation_seconds",
    "data_prepare_seconds",
    "fast_evaluation_seconds",
    "slow_validation_seconds",
    "total_seconds",
    "candidates_per_second",
)
OPTIONAL_WFA_GRID_V2_TIMING_KEYS = (
    "slow_refinement_seconds",
    "plan_build_seconds",
    "plan_reuse_lookup_seconds",
    "runtime_rebase_seconds",
    "cache_key_build_seconds",
    "signal_build_seconds",
    "stack_build_seconds",
    "compiled_batch_seconds",
    "fast_result_materialization_seconds",
    "ranking_seconds",
)
WFA_RATE_TIMING_KEYS = {"candidates_per_second"}
WFA_GRID_V2_PLAN_REUSE_COUNT_KEYS = (
    "plan_build_count",
    "plan_reuse_hit_count",
    "plan_reuse_miss_count",
)
WFA_GRID_V2_CHUNK_KEYS = (
    "chunk_count",
    "max_chunk_candidates",
    "max_chunk_estimated_mb",
    "chunk_estimated_mb",
    "configured_limit_mb",
    "full_run_estimated_signal_mb",
    "signal_stack_rows_built",
    "signal_stack_rows_peak",
)

TOP_RESULT_METRICS = (
    "net_profit_pct",
    "max_drawdown_pct",
    "total_trades",
    "winning_trades",
    "losing_trades",
    "win_rate",
    "gross_profit",
    "gross_loss",
    "profit_factor",
    "romad",
    "sharpe_ratio",
    "sortino_ratio",
    "sqn",
    "ulcer_index",
    "consistency_score",
    "max_consecutive_losses",
    "score",
)


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _jsonable(value: Any) -> Any:
    try:
        import numpy as np
    except Exception:  # pragma: no cover - numpy is a project dependency
        np = None
    try:
        import pandas as pd
    except Exception:  # pragma: no cover - pandas is a project dependency
        pd = None

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if np is not None:
        if isinstance(value, np.generic):
            return _jsonable(value.item())
        if isinstance(value, np.ndarray):
            return [_jsonable(item) for item in value.tolist()]
    if pd is not None and isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "__dict__"):
        return _jsonable(vars(value))
    return str(value)


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    if _is_relative_to(resolved, REPO_ROOT):
        return resolved.relative_to(REPO_ROOT).as_posix()
    return str(resolved)


def load_json_file(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as exc:
        raise ValueError(f"JSON file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return payload


def load_benchmark_payload(path: Path | str) -> dict[str, Any]:
    resolved = resolve_path(path, description="benchmark config")
    payload = load_json_file(resolved)
    validate_direct_grid_payload(payload)
    return payload


def validate_direct_grid_payload(payload: Mapping[str, Any]) -> None:
    if not isinstance(payload, Mapping):
        raise ValueError("Benchmark payload must be a JSON object.")
    if not isinstance(payload.get("enabled_params"), dict):
        raise ValueError("Benchmark payload must include enabled_params as a dictionary.")
    fixed_params = payload.get("fixed_params")
    if not isinstance(fixed_params, dict):
        raise ValueError("Benchmark payload must include fixed_params as a dictionary.")
    for key in ("dateFilter", "start", "end"):
        if key not in fixed_params:
            raise ValueError(f"Benchmark fixed_params must include {key!r}.")
    if fixed_params.get("dateFilter") is not True:
        raise ValueError("Benchmark fixed_params.dateFilter must be true.")
    mode = str(payload.get("optimization_mode", "")).strip().lower()
    if mode != "grid":
        raise ValueError("Benchmark payload optimization_mode must be 'grid'.")
    if bool(payload.get("grid_needs_dsr", False)):
        raise ValueError("Grid V2 DSR must stay disabled for this benchmark.")


def resolve_path(path: Path | str, *, description: str, must_exist: bool = True) -> Path:
    candidate = Path(path)
    resolved = candidate if candidate.is_absolute() else REPO_ROOT / candidate
    resolved = resolved.resolve()
    if must_exist and not resolved.exists():
        raise ValueError(f"{description.capitalize()} does not exist: {resolved}")
    return resolved


def resolve_csv_path(payload: Mapping[str, Any], cli_csv: Path | str | None = None) -> Path:
    raw = (
        cli_csv
        or payload.get("csv_file")
        or payload.get("csv_path")
        or payload.get("dataset")
        or payload.get("dataset_path")
    )
    if not raw:
        raise ValueError("Benchmark payload must include csv_file, or pass --csv.")

    path = Path(raw)
    resolved = path.resolve() if path.is_absolute() else (REPO_ROOT / path).resolve()
    if cli_csv is None and not _is_relative_to(resolved, REPO_ROOT):
        raise ValueError(
            "CSV paths embedded in benchmark config must stay inside the repository. "
            "Pass --csv explicitly to benchmark an external file."
        )
    if not resolved.exists():
        raise ValueError(f"CSV file does not exist: {resolved}")
    return resolved


def build_optimization_config_via_ui(
    *,
    csv_file: Path,
    payload: dict[str, Any],
    worker_processes: int,
    strategy_id: str,
    warmup_bars: int,
) -> Any:
    from ui.server_services import _build_optimization_config

    return _build_optimization_config(
        csv_file=str(csv_file),
        payload=payload,
        worker_processes=worker_processes,
        strategy_id=strategy_id,
        warmup_bars=warmup_bars,
    )


def build_direct_grid_config(
    payload: Mapping[str, Any],
    *,
    csv_path: Path,
    worker_processes: int,
    strategy_id: str | None = None,
    warmup_bars: int | None = None,
) -> Any:
    validate_direct_grid_payload(payload)
    payload_copy = copy.deepcopy(dict(payload))
    resolved_strategy_id = (
        strategy_id
        or payload_copy.get("strategy_id")
        or payload_copy.get("strategy")
        or DEFAULT_STRATEGY_ID
    )
    resolved_warmup_bars = int(
        warmup_bars if warmup_bars is not None else payload_copy.get("warmup_bars", 1000)
    )
    return build_optimization_config_via_ui(
        csv_file=csv_path,
        payload=payload_copy,
        worker_processes=int(worker_processes),
        strategy_id=str(resolved_strategy_id),
        warmup_bars=resolved_warmup_bars,
    )


def preview_grid_parameter_space(config: Any) -> dict[str, Any]:
    from core.grid_engine import preview_grid_parameter_space as _preview_grid_parameter_space

    return _preview_grid_parameter_space(config)


def preview_candidate_count(config: Any) -> tuple[int, dict[str, Any]]:
    preview = preview_grid_parameter_space(config)
    for key in ("full_candidate_count", "candidate_count", "total_space"):
        value = preview.get(key)
        if value is not None:
            return int(value), preview
    raise ValueError("Grid preview did not report a candidate count.")


def run_grid_optimization(config: Any, *, save_study: bool) -> tuple[list[Any], Any]:
    from core.grid_engine import run_grid_optimization as _run_grid_optimization

    return _run_grid_optimization(config, save_study=save_study)


def parse_workers(raw: str | Sequence[int]) -> list[int]:
    if isinstance(raw, str):
        values = [item.strip() for item in raw.split(",")]
    else:
        values = [str(item) for item in raw]
    workers: list[int] = []
    for item in values:
        if not item:
            continue
        try:
            value = int(item)
        except ValueError as exc:
            raise ValueError(f"Invalid worker count: {item!r}") from exc
        if value < 1:
            raise ValueError("Worker counts must be positive integers.")
        workers.append(value)
    if not workers:
        raise ValueError("At least one worker count is required.")
    return workers


def expected_candidate_count_for_payload(
    payload: Mapping[str, Any],
    *,
    strategy_id: str,
    cli_expected: int | None = None,
) -> int | None:
    if cli_expected is not None:
        return int(cli_expected)
    raw = payload.get("expected_candidate_count")
    if raw is not None:
        return int(raw)
    if strategy_id == DEFAULT_STRATEGY_ID:
        return DEFAULT_EXPECTED_S06_B2_CANDIDATES
    return None


def _validate_candidate_count(
    *,
    actual: int,
    expected: int | None,
    allow_reduced_domain: bool,
) -> None:
    if expected is None or actual == expected:
        return
    if allow_reduced_domain and actual < expected:
        return
    raise ValueError(
        f"Benchmark config produced {actual:,} candidates; expected {expected:,}. "
        "Use --allow-reduced-domain only for intentional reduced-domain benchmark payloads."
    )


def collect_environment_metadata() -> dict[str, Any]:
    env = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "python": sys.version.replace("\n", " "),
        "NUMBA_NUM_THREADS": os.environ.get("NUMBA_NUM_THREADS"),
        "NUMBA_DISABLE_JIT": os.environ.get("NUMBA_DISABLE_JIT"),
        "NUMBA_THREADING_LAYER": os.environ.get("NUMBA_THREADING_LAYER"),
    }
    for package in ("numpy", "pandas", "numba"):
        try:
            env[f"{package}_version"] = metadata.version(package)
        except metadata.PackageNotFoundError:
            env[f"{package}_version"] = None
    try:
        import numba

        env["numba_get_num_threads"] = numba.get_num_threads()
    except Exception:
        env["numba_get_num_threads"] = None
    return env


def _top_result_summary(result: Any | None) -> dict[str, Any]:
    if result is None:
        return {}
    candidate_id = getattr(result, "candidate_id", None)
    trial_number = getattr(result, "optuna_trial_number", None)
    metrics = {name: getattr(result, name, None) for name in TOP_RESULT_METRICS}
    return _jsonable(
        {
            "candidate_id": candidate_id if candidate_id is not None else trial_number,
            "optuna_trial_number": trial_number,
            "params": getattr(result, "params", None),
            "metrics": metrics,
            "objective_values": getattr(result, "objective_values", None),
            "constraint_values": getattr(result, "constraint_values", None),
            "constraints_satisfied": getattr(result, "constraints_satisfied", None),
            "is_pareto_optimal": getattr(result, "is_pareto_optimal", None),
        }
    )


def _build_direct_run_record(
    *,
    config: Any,
    csv_path: Path,
    selected_results: Sequence[Any],
    worker_processes: int,
    run_index: int,
    warmup: bool,
    measured_wall_seconds: float,
    preview: Mapping[str, Any],
) -> dict[str, Any]:
    summary = getattr(config, "grid_summary", None)
    if not isinstance(summary, Mapping):
        raise RuntimeError("run_grid_optimization did not populate config.grid_summary.")
    grid_summary = summary.get("grid") if isinstance(summary.get("grid"), Mapping) else {}
    timings = grid_summary.get("timings") if isinstance(grid_summary.get("timings"), Mapping) else {}
    fixed_params = getattr(config, "fixed_params", {}) or {}
    top_result = selected_results[0] if selected_results else None

    record = {
        "worker_processes": int(worker_processes),
        "run_index": int(run_index),
        "warmup": bool(warmup),
        "measured_wall_seconds": float(measured_wall_seconds),
        "strategy_id": getattr(config, "strategy_id", None),
        "engine": summary.get("engine"),
        "csv_path": _display_path(csv_path),
        "date_range": {
            "dateFilter": fixed_params.get("dateFilter"),
            "start": fixed_params.get("start") or grid_summary.get("start"),
            "end": fixed_params.get("end") or grid_summary.get("end"),
        },
        "warmup_bars": getattr(config, "warmup_bars", None),
        "backend_kind": grid_summary.get("backend_kind"),
        "compiled_batch_used": grid_summary.get("compiled_batch_used"),
        "compiled_workers": grid_summary.get("compiled_workers"),
        "candidate_count": summary.get("candidate_count", grid_summary.get("candidate_count")),
        "valid_candidate_count": summary.get(
            "valid_candidate_count", grid_summary.get("valid_candidate_count")
        ),
        "selected_candidate_count": summary.get(
            "selected_candidate_count", grid_summary.get("selected_candidate_count")
        ),
        "preview_candidate_count": preview.get("full_candidate_count")
        or preview.get("candidate_count")
        or preview.get("total_space"),
        "cache_estimate": grid_summary.get("cache_estimate"),
        "cache_stats": grid_summary.get("cache_stats"),
        "timings": dict(timings),
        "timing_fields": {
            key: timings.get(key)
            for key in (
                "candidate_generation_seconds",
                "plan_build_seconds",
                "plan_reuse_lookup_seconds",
                "runtime_rebase_seconds",
                "data_prepare_seconds",
                "fast_evaluation_seconds",
                "fast_result_materialization_seconds",
                "ranking_seconds",
                "slow_validation_seconds",
                "slow_refinement_seconds",
                "total_seconds",
            )
        },
        "chunk_fields": {
            key: grid_summary.get(key)
            for key in (
                "chunk_count",
                "max_chunk_candidates",
                "max_chunk_estimated_mb",
                "chunk_estimated_mb",
                "configured_limit_mb",
                "full_run_estimated_signal_mb",
                "signal_stack_rows_built",
                "signal_stack_rows_peak",
                "full_population_result_object_note",
            )
        },
        "candidates_per_second": grid_summary.get("candidates_per_second"),
        "selected_result_count": len(selected_results),
        "top_result": _top_result_summary(top_result),
        "grid_summary": summary,
    }
    return _jsonable(record)


def _stats_for_measured_runs(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    measured_by_worker: dict[int, list[float]] = {}
    for run in runs:
        if run.get("warmup"):
            continue
        worker = int(run["worker_processes"])
        measured_by_worker.setdefault(worker, []).append(float(run["measured_wall_seconds"]))
    stats: dict[str, Any] = {}
    for worker, values in measured_by_worker.items():
        stats[str(worker)] = {
            "runs": len(values),
            "min_wall_seconds": min(values),
            "max_wall_seconds": max(values),
            "mean_wall_seconds": statistics.fmean(values),
        }
    return stats


def run_direct_grid_benchmark(
    *,
    payload: Mapping[str, Any],
    csv_path: Path,
    workers: Sequence[int],
    warmup_runs: int,
    runs: int,
    strategy_id: str | None = None,
    warmup_bars: int | None = None,
    expected_candidate_count: int | None = None,
    allow_reduced_domain: bool = False,
) -> dict[str, Any]:
    if warmup_runs < 0:
        raise ValueError("warmup_runs must be >= 0.")
    if runs < 1:
        raise ValueError("runs must be >= 1.")

    resolved_strategy_id = (
        strategy_id or payload.get("strategy_id") or payload.get("strategy") or DEFAULT_STRATEGY_ID
    )
    resolved_warmup_bars = int(
        warmup_bars if warmup_bars is not None else payload.get("warmup_bars", 1000)
    )
    expected_count = expected_candidate_count_for_payload(
        payload,
        strategy_id=str(resolved_strategy_id),
        cli_expected=expected_candidate_count,
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "command": "direct-grid",
        "environment": collect_environment_metadata(),
        "config": {
            "strategy_id": resolved_strategy_id,
            "csv_path": _display_path(csv_path),
            "warmup_bars": resolved_warmup_bars,
            "workers": list(workers),
            "warmup_runs": warmup_runs,
            "runs": runs,
            "expected_candidate_count": expected_count,
            "allow_reduced_domain": bool(allow_reduced_domain),
        },
        "runs": [],
    }

    for worker in workers:
        for index in range(1, warmup_runs + runs + 1):
            warmup = index <= warmup_runs
            run_index = index if warmup else index - warmup_runs
            config = build_direct_grid_config(
                payload,
                csv_path=csv_path,
                worker_processes=int(worker),
                strategy_id=str(resolved_strategy_id),
                warmup_bars=resolved_warmup_bars,
            )
            count, preview = preview_candidate_count(config)
            _validate_candidate_count(
                actual=count,
                expected=expected_count,
                allow_reduced_domain=allow_reduced_domain,
            )
            started = time.perf_counter()
            selected_results, _study_id = run_grid_optimization(config, save_study=False)
            measured_wall_seconds = time.perf_counter() - started
            record = _build_direct_run_record(
                config=config,
                csv_path=csv_path,
                selected_results=selected_results,
                worker_processes=int(worker),
                run_index=run_index,
                warmup=warmup,
                measured_wall_seconds=measured_wall_seconds,
                preview=preview,
            )
            report["runs"].append(record)

    report["summary_stats"] = _stats_for_measured_runs(report["runs"])
    return _jsonable(report)


def _readonly_sqlite_connection(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.resolve()
    if not resolved.exists():
        raise ValueError(f"SQLite database does not exist: {resolved}")
    # Benchmark inputs are frozen/checkpointed DB snapshots. immutable=1 keeps
    # inspection read-only and avoids -wal/-shm sidecars; do not use this helper
    # for live SQLite DBs with uncheckpointed WAL frames.
    uri = f"file:{quote(resolved.as_posix(), safe='/:')}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _select_exprs(available: set[str], names: Sequence[str]) -> str:
    return ", ".join(name if name in available else f"NULL AS {name}" for name in names)


def _parse_json_object(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _numeric_aggregate(values: Sequence[float], *, include_sum: bool) -> dict[str, Any] | None:
    if not values:
        return None
    aggregate: dict[str, Any] = {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": statistics.fmean(values),
    }
    if include_sum:
        aggregate["sum"] = math.fsum(values)
    return aggregate


def _window_count_summary(conn: sqlite3.Connection, study_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_windows,
            COUNT(grid_valid_candidate_count) AS valid_count_windows,
            MIN(grid_valid_candidate_count) AS valid_min,
            MAX(grid_valid_candidate_count) AS valid_max,
            AVG(grid_valid_candidate_count) AS valid_avg,
            COUNT(grid_selected_candidate_count) AS selected_count_windows,
            MIN(grid_selected_candidate_count) AS selected_min,
            MAX(grid_selected_candidate_count) AS selected_max,
            AVG(grid_selected_candidate_count) AS selected_avg
        FROM wfa_windows
        WHERE study_id = ?
        """,
        (study_id,),
    ).fetchone()
    return _jsonable(dict(row or {}))


def _diagnostics_summary(conn: sqlite3.Connection, study_id: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT module_status_json
        FROM wfa_windows
        WHERE study_id = ?
        ORDER BY window_number
        """,
        (study_id,),
    ).fetchall()
    total_windows = len(rows)
    windows_with_grid_v2 = 0
    windows_with_all_stable_keys = 0
    missing_keys: set[str] = set()
    backend_kinds: set[str] = set()
    compiled_workers: set[int] = set()
    aggregate_keys = (*STABLE_WFA_GRID_V2_TIMING_KEYS, *OPTIONAL_WFA_GRID_V2_TIMING_KEYS)
    timing_values: dict[str, list[float]] = {key: [] for key in aggregate_keys}
    plan_reuse_windows = 0
    plan_reuse_hit_windows = 0
    plan_reuse_miss_windows = 0
    plan_reuse_count_values: dict[str, list[float]] = {
        key: [] for key in WFA_GRID_V2_PLAN_REUSE_COUNT_KEYS
    }
    chunk_values: dict[str, list[float]] = {key: [] for key in WFA_GRID_V2_CHUNK_KEYS}
    full_population_note_windows = 0
    for row in rows:
        module_status = _parse_json_object(row["module_status_json"])
        grid_v2 = module_status.get("grid_v2")
        if not isinstance(grid_v2, dict):
            missing_keys.update(STABLE_WFA_GRID_V2_TIMING_KEYS)
            continue
        windows_with_grid_v2 += 1
        if grid_v2.get("backend_kind") is not None:
            backend_kinds.add(str(grid_v2.get("backend_kind")))
        if grid_v2.get("compiled_workers") is not None:
            try:
                compiled_workers.add(int(grid_v2.get("compiled_workers")))
            except (TypeError, ValueError):
                pass
        missing_for_window = {
            key for key in STABLE_WFA_GRID_V2_TIMING_KEYS if grid_v2.get(key) is None
        }
        if not missing_for_window:
            windows_with_all_stable_keys += 1
        missing_keys.update(missing_for_window)
        for key in aggregate_keys:
            numeric = _finite_float(grid_v2.get(key))
            if numeric is not None:
                timing_values[key].append(numeric)
        plan_reuse_enabled = grid_v2.get("plan_reuse_enabled")
        if plan_reuse_enabled is not None:
            plan_reuse_windows += 1
        if plan_reuse_enabled is True:
            if grid_v2.get("plan_reuse_hit") is True:
                plan_reuse_hit_windows += 1
            elif grid_v2.get("plan_reuse_hit") is False:
                plan_reuse_miss_windows += 1
        for key in WFA_GRID_V2_PLAN_REUSE_COUNT_KEYS:
            numeric = _finite_float(grid_v2.get(key))
            if numeric is not None:
                plan_reuse_count_values[key].append(numeric)
        for key in WFA_GRID_V2_CHUNK_KEYS:
            numeric = _finite_float(grid_v2.get(key))
            if numeric is not None:
                chunk_values[key].append(numeric)
        if grid_v2.get("full_population_result_object_note"):
            full_population_note_windows += 1

    if windows_with_grid_v2 == 0:
        status = "absent"
    elif windows_with_all_stable_keys == windows_with_grid_v2 == total_windows:
        status = "present"
    else:
        status = "partial"

    timing_aggregates = {}
    for key in aggregate_keys:
        aggregate = _numeric_aggregate(
            timing_values[key],
            include_sum=key not in WFA_RATE_TIMING_KEYS,
        )
        if aggregate is not None:
            timing_aggregates[key] = aggregate
    plan_reuse_count_aggregates = {}
    for key, values in plan_reuse_count_values.items():
        aggregate = _numeric_aggregate(values, include_sum=False)
        if aggregate is not None:
            plan_reuse_count_aggregates[key] = aggregate
    chunk_aggregates = {}
    for key, values in chunk_values.items():
        aggregate = _numeric_aggregate(values, include_sum=False)
        if aggregate is not None:
            chunk_aggregates[key] = aggregate

    return {
        "status": status,
        "total_windows": total_windows,
        "windows_with_grid_v2": windows_with_grid_v2,
        "windows_with_all_stable_keys": windows_with_all_stable_keys,
        "stable_keys": list(STABLE_WFA_GRID_V2_TIMING_KEYS),
        "optional_timing_keys": list(OPTIONAL_WFA_GRID_V2_TIMING_KEYS),
        "stable_keys_missing": sorted(missing_keys),
        "timing_aggregates": timing_aggregates,
        "plan_reuse": {
            "windows_with_fields": plan_reuse_windows,
            "hit_windows": plan_reuse_hit_windows,
            "miss_windows": plan_reuse_miss_windows,
            "count_aggregates": plan_reuse_count_aggregates,
        },
        "chunk_keys": list(WFA_GRID_V2_CHUNK_KEYS),
        "chunk_aggregates": chunk_aggregates,
        "full_population_result_object_note_windows": full_population_note_windows,
        "backend_kinds": sorted(backend_kinds),
        "compiled_workers": sorted(compiled_workers),
    }


def _study_rows(conn: sqlite3.Connection, study_ids: Sequence[str] | None) -> list[sqlite3.Row]:
    study_cols = _table_columns(conn, "studies")
    names = (
        "study_id",
        "study_name",
        "strategy_id",
        "optimization_mode",
        "optimizer_mode",
        "config_json",
        "csv_file_path",
        "csv_file_name",
        "total_windows",
        "optimization_time_seconds",
        "stitched_oos_net_profit_pct",
        "stitched_oos_max_drawdown_pct",
        "stitched_oos_total_trades",
        "stitched_oos_winning_trades",
        "stitched_oos_win_rate",
        "grid_summary_json",
        "created_at",
    )
    query = f"SELECT {_select_exprs(study_cols, names)} FROM studies"
    params: list[Any] = []
    clauses = ["LOWER(COALESCE(optimization_mode, '')) = 'wfa'"]
    if study_ids:
        placeholders = ", ".join("?" for _ in study_ids)
        clauses.append(f"study_id IN ({placeholders})")
        params.extend(study_ids)
    query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY created_at, study_id"
    return conn.execute(query, params).fetchall()


def inspect_wfa_db(
    db_path: Path,
    *,
    study_ids: Sequence[str] | None = None,
    compare_pairs: Sequence[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    with _readonly_sqlite_connection(db_path) as conn:
        studies: list[dict[str, Any]] = []
        for row in _study_rows(conn, study_ids):
            cfg = _parse_json_object(row["config_json"])
            fixed_params = cfg.get("fixed_params") if isinstance(cfg.get("fixed_params"), dict) else {}
            select_subsets = {
                key: value for key, value in fixed_params.items() if str(key).endswith("_options")
            }
            window_summary = _window_count_summary(conn, row["study_id"])
            diagnostics = _diagnostics_summary(conn, row["study_id"])
            grid_summary = _parse_json_object(row["grid_summary_json"])
            studies.append(
                _jsonable(
                    {
                        "study_id": row["study_id"],
                        "study_name": row["study_name"],
                        "strategy_id": row["strategy_id"],
                        "optimization_mode": row["optimization_mode"],
                        "optimizer_mode": row["optimizer_mode"] or cfg.get("optimization_mode"),
                        "worker_processes": cfg.get("worker_processes"),
                        "grid_budget": cfg.get("grid_budget"),
                        "grid_top_candidates": cfg.get("grid_top_candidates"),
                        "grid_enabled_modes": cfg.get("grid_enabled_modes"),
                        "select_option_subsets": select_subsets,
                        "csv_file_path": row["csv_file_path"],
                        "csv_file_name": row["csv_file_name"],
                        "total_windows": row["total_windows"] or window_summary.get("total_windows"),
                        "optimization_time_seconds": row["optimization_time_seconds"],
                        "stitched_oos": {
                            "net_profit_pct": row["stitched_oos_net_profit_pct"],
                            "max_drawdown_pct": row["stitched_oos_max_drawdown_pct"],
                            "total_trades": row["stitched_oos_total_trades"],
                            "winning_trades": row["stitched_oos_winning_trades"],
                            "win_rate": row["stitched_oos_win_rate"],
                        },
                        "window_counts": window_summary,
                        "diagnostics": diagnostics,
                        "study_grid_summary_present": bool(grid_summary),
                    }
                )
            )

        study_by_id = {study["study_id"]: study for study in studies}
        comparisons = []
        for left_id, right_id in compare_pairs or ():
            left = study_by_id.get(left_id)
            right = study_by_id.get(right_id)
            if not left or not right:
                comparisons.append(
                    {
                        "left_study_id": left_id,
                        "right_study_id": right_id,
                        "status": "missing_study",
                    }
                )
                continue
            left_time = left.get("optimization_time_seconds")
            right_time = right.get("optimization_time_seconds")
            ratio = None
            if left_time not in (None, 0) and right_time is not None:
                ratio = float(right_time) / float(left_time)
            comparisons.append(
                {
                    "left_study_id": left_id,
                    "right_study_id": right_id,
                    "left_seconds": left_time,
                    "right_seconds": right_time,
                    "right_over_left_ratio": ratio,
                    "status": "ok",
                }
            )

    return _jsonable(
        {
            "schema_version": SCHEMA_VERSION,
            "command": "inspect-wfa-db",
            "db_path": _display_path(db_path),
            "environment": collect_environment_metadata(),
            "studies": studies,
            "comparisons": comparisons,
        }
    )


def _format_count_summary(window_counts: Mapping[str, Any], prefix: str) -> str:
    count_windows = int(window_counts.get(f"{prefix}_count_windows") or 0)
    if count_windows == 0:
        return "not captured"
    min_value = window_counts.get(f"{prefix}_min")
    max_value = window_counts.get(f"{prefix}_max")
    avg_value = window_counts.get(f"{prefix}_avg")
    if min_value == max_value:
        return f"{int(min_value):,} x{count_windows}"
    avg_text = f"{float(avg_value):,.1f}" if avg_value is not None else "-"
    return f"{int(min_value):,}/{avg_text}/{int(max_value):,}"


def _format_mean_seconds(aggregate: Mapping[str, Any] | None) -> str:
    if not aggregate:
        return "-"
    value = aggregate.get("mean")
    if value is None:
        return "-"
    return f"{float(value):.3f}s"


def _format_mean_rate(aggregate: Mapping[str, Any] | None) -> str:
    if not aggregate:
        return "-"
    value = aggregate.get("mean")
    if value is None:
        return "-"
    return f"{float(value):,.1f}"


def print_direct_report(report: Mapping[str, Any]) -> None:
    config = report.get("config", {})
    print("Grid V2 direct benchmark")
    print(f"  strategy: {config.get('strategy_id')}")
    print(f"  csv: {config.get('csv_path')}")
    print(f"  warmup_bars: {config.get('warmup_bars')}")
    print(f"  workers: {config.get('workers')}")
    for run in report.get("runs", []):
        label = "warmup" if run.get("warmup") else "run"
        timings = run.get("timings") if isinstance(run.get("timings"), dict) else {}
        print(
            "  "
            f"{label} workers={run.get('worker_processes')} "
            f"run={run.get('run_index')} "
            f"wall={float(run.get('measured_wall_seconds') or 0.0):.3f}s "
            f"total={float(timings.get('total_seconds') or 0.0):.3f}s "
            f"fast={float(timings.get('fast_evaluation_seconds') or 0.0):.3f}s "
            f"cps={run.get('candidates_per_second')} "
            f"candidates={run.get('candidate_count')} "
            f"chunks={(run.get('chunk_fields') or {}).get('chunk_count')}"
        )
        top = run.get("top_result") if isinstance(run.get("top_result"), dict) else {}
        if top:
            metrics = top.get("metrics") if isinstance(top.get("metrics"), dict) else {}
            print(
                "    "
                f"top_candidate={top.get('candidate_id')} "
                f"net={metrics.get('net_profit_pct')} "
                f"dd={metrics.get('max_drawdown_pct')} "
                f"trades={metrics.get('total_trades')}"
            )


def print_wfa_report(report: Mapping[str, Any]) -> None:
    print(f"WFA DB inspection: {report.get('db_path')}")
    studies = report.get("studies") if isinstance(report.get("studies"), list) else []
    if not studies:
        print("  No WFA studies found.")
        return
    header = (
        "study_id                              strategy                 time  workers "
        "windows valid              selected           diagnostics"
    )
    print(header)
    print("-" * len(header))
    for study in studies:
        counts = study.get("window_counts") if isinstance(study.get("window_counts"), dict) else {}
        diagnostics = study.get("diagnostics") if isinstance(study.get("diagnostics"), dict) else {}
        print(
            f"{study.get('study_id')} "
            f"{str(study.get('strategy_id') or '')[:24]:24} "
            f"{str(study.get('optimization_time_seconds') or '-'):>5} "
            f"{str(study.get('worker_processes') or '-'):>7} "
            f"{str(study.get('total_windows') or counts.get('total_windows') or '-'):>7} "
            f"{_format_count_summary(counts, 'valid'):18} "
            f"{_format_count_summary(counts, 'selected'):18} "
            f"{diagnostics.get('status')}"
        )
        subsets = study.get("select_option_subsets") or {}
        if subsets:
            print(f"  select subsets: {json.dumps(subsets, sort_keys=True)}")
        stitched = study.get("stitched_oos") if isinstance(study.get("stitched_oos"), dict) else {}
        if stitched:
            print(
                "  stitched OOS: "
                f"net={stitched.get('net_profit_pct')} "
                f"dd={stitched.get('max_drawdown_pct')} "
                f"trades={stitched.get('total_trades')} "
                f"wr={stitched.get('win_rate')}"
            )
        aggregates = diagnostics.get("timing_aggregates")
        if isinstance(aggregates, dict) and aggregates:
            print(
                "  grid_v2 timing mean: "
                f"total={_format_mean_seconds(aggregates.get('total_seconds'))} "
                f"fast={_format_mean_seconds(aggregates.get('fast_evaluation_seconds'))} "
                f"cps={_format_mean_rate(aggregates.get('candidates_per_second'))}"
            )
        chunks = diagnostics.get("chunk_aggregates")
        if isinstance(chunks, dict) and chunks:
            print(
                "  grid_v2 chunk mean: "
                f"count={_format_mean_rate(chunks.get('chunk_count'))} "
                f"max_candidates={_format_mean_rate(chunks.get('max_chunk_candidates'))} "
                f"max_mb={_format_mean_rate(chunks.get('max_chunk_estimated_mb'))}"
            )
        plan_reuse = diagnostics.get("plan_reuse")
        if isinstance(plan_reuse, dict) and int(plan_reuse.get("windows_with_fields") or 0):
            print(
                "  grid_v2 plan reuse: "
                f"fields={plan_reuse.get('windows_with_fields')} "
                f"hits={plan_reuse.get('hit_windows')} "
                f"misses={plan_reuse.get('miss_windows')}"
            )
    for comparison in report.get("comparisons", []):
        print(
            "comparison "
            f"{comparison.get('left_study_id')} -> {comparison.get('right_study_id')}: "
            f"{comparison.get('right_over_left_ratio')}"
        )


def parse_compare_pairs(raw_pairs: Sequence[str] | None) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for raw in raw_pairs or ():
        if ":" not in raw:
            raise ValueError(f"Comparison must use LEFT:RIGHT study ids: {raw!r}")
        left, right = raw.split(":", 1)
        left = left.strip()
        right = right.strip()
        if not left or not right:
            raise ValueError(f"Comparison must include both study ids: {raw!r}")
        pairs.append((left, right))
    return pairs


def write_json_report(path: Path | str, report: Mapping[str, Any]) -> None:
    output_path = Path(path)
    if not output_path.is_absolute():
        output_path = REPO_ROOT / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(_jsonable(report), handle, indent=2, sort_keys=True)
        handle.write("\n")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark Grid V2 and inspect saved WFA Grid diagnostics."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    direct = subparsers.add_parser(
        "direct-grid",
        help="Run a direct Grid V2 benchmark from an optimize-style JSON payload.",
    )
    direct.add_argument("--config", required=True, help="Benchmark JSON payload.")
    direct.add_argument("--csv", help="Override CSV path from the benchmark payload.")
    direct.add_argument("--strategy-id", help=f"Strategy id, default {DEFAULT_STRATEGY_ID}.")
    direct.add_argument("--workers", default="1", help="Comma-separated worker counts, e.g. 1,6.")
    direct.add_argument("--warmup-runs", type=int, default=1, help="Warmup runs per worker.")
    direct.add_argument("--runs", type=int, default=2, help="Measured runs per worker.")
    direct.add_argument("--warmup-bars", type=int, help="Override warmup bars passed to the builder.")
    direct.add_argument(
        "--expected-candidate-count",
        type=int,
        help="Expected full-grid candidate count. Defaults to payload value or 48,480 for S06 B2.",
    )
    direct.add_argument(
        "--allow-reduced-domain",
        action="store_true",
        help="Allow candidate counts below the expected full-grid count.",
    )
    direct.add_argument("--output-json", help="Optional path for machine-readable JSON output.")

    inspect = subparsers.add_parser(
        "inspect-wfa-db",
        help="Inspect saved WFA studies and Grid V2 diagnostics without rerunning WFA.",
    )
    inspect.add_argument(
        "--db",
        default=str(DEFAULT_COMPARISON_DB),
        help="SQLite DB path. Opened read-only.",
    )
    inspect.add_argument(
        "--study-id",
        action="append",
        dest="study_ids",
        help="Restrict to a study id. May be repeated.",
    )
    inspect.add_argument(
        "--compare",
        action="append",
        help="Compare two study ids as LEFT:RIGHT and report time ratio. May be repeated.",
    )
    inspect.add_argument("--output-json", help="Optional path for machine-readable JSON output.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "direct-grid":
            payload = load_benchmark_payload(args.config)
            csv_path = resolve_csv_path(payload, args.csv)
            report = run_direct_grid_benchmark(
                payload=payload,
                csv_path=csv_path,
                workers=parse_workers(args.workers),
                warmup_runs=int(args.warmup_runs),
                runs=int(args.runs),
                strategy_id=args.strategy_id,
                warmup_bars=args.warmup_bars,
                expected_candidate_count=args.expected_candidate_count,
                allow_reduced_domain=bool(args.allow_reduced_domain),
            )
            print_direct_report(report)
        elif args.command == "inspect-wfa-db":
            db_path = resolve_path(args.db, description="SQLite database")
            report = inspect_wfa_db(
                db_path,
                study_ids=args.study_ids,
                compare_pairs=parse_compare_pairs(args.compare),
            )
            print_wfa_report(report)
        else:  # pragma: no cover - argparse prevents this branch
            parser.error(f"Unknown command: {args.command}")
            return 2
        if args.output_json:
            write_json_report(args.output_json, report)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
