import io
import json
import math
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import logging
import pandas as pd
from flask import current_app, has_app_context, jsonify, request, send_file

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.backtest_engine import (
    align_date_bounds,
    load_data,
    parse_timestamp_utc,
    prepare_dataset_with_warmup,
)
from core.export import export_trades_csv
from core.optuna_engine import (
    CONSTRAINT_OPERATORS,
    OBJECTIVE_DIRECTIONS,
    OBJECTIVE_DISPLAY_NAMES,
    OptimizationConfig,
    OptimizationResult,
    run_optimization,
)
from core.post_process import (
    DSRConfig,
    PostProcessConfig,
    StressTestConfig,
    calculate_period_dates,
    calculate_is_period_days,
    run_dsr_analysis,
    run_forward_test,
    run_stress_test,
)
from core.testing import run_period_test_for_trials, select_oos_source_candidates
from core.storage import (
    delete_manual_test,
    delete_study,
    get_study_trial,
    list_manual_tests,
    list_studies,
    load_manual_test_results,
    load_study_from_db,
    load_wfa_window_trials,
    save_dsr_results,
    save_forward_test_results,
    save_stress_test_results,
    save_manual_test_to_db,
    save_oos_test_results,
    update_csv_path,
    update_study_config_json,
)


OPTIMIZATION_STATE_LOCK = threading.Lock()
LAST_OPTIMIZATION_STATE: Dict[str, Any] = {
    "status": "idle",
    "updated_at": None,
}
CANCELLED_RUNS_TTL_SECONDS = 24 * 60 * 60
CANCELLED_RUNS_MAX_SIZE = 2048
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
CANCELLED_RUNS: Dict[str, float] = {}


def _is_path_within_root(path: Path, root: Path) -> bool:
    path_norm = os.path.normcase(str(path))
    root_norm = os.path.normcase(str(root))
    try:
        return os.path.commonpath([path_norm, root_norm]) == root_norm
    except ValueError:
        return False


def _collect_allowed_csv_roots(default_root: str) -> List[Path]:
    env_value = os.getenv("MERLIN_CSV_ALLOWED_ROOTS", "")
    raw_values = [item.strip() for item in re.split(r"[;\r\n]+", env_value) if item.strip()]
    if default_root:
        raw_values.append(default_root)

    roots: List[Path] = []
    seen: set[str] = set()
    for raw in raw_values:
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        try:
            resolved = candidate.resolve(strict=False)
        except OSError:
            continue
        key = os.path.normcase(str(resolved))
        if key in seen:
            continue
        seen.add(key)
        roots.append(resolved)
    return roots


DEFAULT_CSV_ROOT = (
    os.getenv("MERLIN_DEFAULT_CSV_ROOT")
    or r"C:\Users\mt\Desktop\Strategy\S_Python\Market Data_PY"
).strip()
CSV_ALLOWED_ROOTS = _collect_allowed_csv_roots(DEFAULT_CSV_ROOT)
# Kept as a public flag for API metadata, but absolute csvPath is now mandatory.
STRICT_CSV_PATH_MODE = True
QUEUE_STATE_PATTERN = re.compile(r"^[A-Za-z]:[\\/]|^\\\\[^\\]|^/")
QUEUE_STORAGE_FILE = Path(__file__).resolve().parent.parent / "storage" / "queue.json"


def _is_csv_path_allowed(path: Path) -> bool:
    if not CSV_ALLOWED_ROOTS:
        return True
    for root in CSV_ALLOWED_ROOTS:
        if _is_path_within_root(path, root):
            return True
    return False


def _resolve_csv_directory(raw_path: Optional[str]) -> Path:
    raw_value = str(raw_path or DEFAULT_CSV_ROOT or "").strip()
    if not raw_value:
        raise ValueError("CSV directory path is empty.")
    candidate = Path(raw_value).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise FileNotFoundError(str(candidate)) from exc
    if not resolved.is_dir():
        raise NotADirectoryError(str(resolved))
    if not _is_csv_path_allowed(resolved):
        raise PermissionError("CSV directory is outside allowed roots.")
    return resolved


def _list_csv_directory(raw_path: Optional[str]) -> Dict[str, Any]:
    directory = _resolve_csv_directory(raw_path)
    entries: List[Dict[str, Any]] = []

    for child in directory.iterdir():
        try:
            stat = child.stat()
        except OSError:
            continue

        if child.is_dir():
            entries.append(
                {
                    "name": child.name,
                    "path": str(child),
                    "kind": "dir",
                    "size": None,
                    "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                }
            )
            continue

        if not child.is_file() or child.suffix.lower() != ".csv":
            continue

        entries.append(
            {
                "name": child.name,
                "path": str(child),
                "kind": "file",
                "size": int(stat.st_size),
                "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )

    entries.sort(key=lambda item: (0 if item["kind"] == "dir" else 1, item["name"].lower()))

    parent_path: Optional[str] = None
    parent = directory.parent
    if parent != directory and _is_csv_path_allowed(parent):
        parent_path = str(parent)

    return {
        "current_path": str(directory),
        "parent_path": parent_path,
        "entries": entries,
        "default_root": DEFAULT_CSV_ROOT,
        "strict_path_mode": STRICT_CSV_PATH_MODE,
        "allowed_roots": [str(root) for root in CSV_ALLOWED_ROOTS],
    }


def _default_queue_state() -> Dict[str, Any]:
    return {
        "items": [],
        "nextIndex": 1,
        "runtime": {
            "active": False,
            "updatedAt": 0,
        },
    }


def _queue_storage_file_path() -> Path:
    return QUEUE_STORAGE_FILE


def _is_absolute_filesystem_path(raw_path: Any) -> bool:
    value = str(raw_path or "").strip()
    if not value:
        return False
    return bool(QUEUE_STATE_PATTERN.match(value))


def _normalize_queue_source(raw_source: Any) -> Optional[Dict[str, str]]:
    if isinstance(raw_source, str):
        path = raw_source
    elif isinstance(raw_source, dict):
        path = raw_source.get("path") or raw_source.get("csvPath") or ""
    else:
        return None

    normalized_path = str(path or "").strip()
    if not _is_absolute_filesystem_path(normalized_path):
        return None

    return {
        "type": "path",
        "path": normalized_path,
    }


def _normalize_queue_sources(raw_sources: Any) -> List[Dict[str, str]]:
    sources: List[Dict[str, str]] = []
    if not isinstance(raw_sources, list):
        return sources

    for raw_source in raw_sources:
        normalized = _normalize_queue_source(raw_source)
        if normalized:
            sources.append(normalized)
    return sources


def _normalize_queue_runtime(raw_runtime: Any) -> Dict[str, Any]:
    if not isinstance(raw_runtime, dict):
        return {"active": False, "updatedAt": 0}

    try:
        updated_at = int(raw_runtime.get("updatedAt") or 0)
    except (TypeError, ValueError):
        updated_at = 0

    return {
        "active": bool(raw_runtime.get("active")),
        "updatedAt": max(0, updated_at),
    }


def _compute_queue_next_index(items: List[Dict[str, Any]], candidate_next_index: Any) -> int:
    if not items:
        return 1

    max_index = max(int(item.get("index") or 0) for item in items)
    try:
        candidate = int(candidate_next_index)
    except (TypeError, ValueError):
        candidate = 0
    if candidate > max_index:
        return candidate
    return max(1, max_index + 1)


def _normalize_queue_item(raw_item: Any, fallback_index: int) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_item, dict):
        return None

    sources = _normalize_queue_sources(raw_item.get("sources"))
    if not sources:
        return None

    try:
        index = int(raw_item.get("index"))
    except (TypeError, ValueError):
        index = fallback_index
    index = max(1, index)

    item = json.loads(json.dumps(raw_item))
    item["index"] = index
    item["sources"] = sources

    try:
        source_cursor = int(item.get("sourceCursor") or 0)
    except (TypeError, ValueError):
        source_cursor = 0
    item["sourceCursor"] = max(0, min(len(sources), source_cursor))

    try:
        success_count = int(item.get("successCount") or 0)
    except (TypeError, ValueError):
        success_count = 0
    item["successCount"] = max(0, success_count)

    try:
        failure_count = int(item.get("failureCount") or 0)
    except (TypeError, ValueError):
        failure_count = 0
    item["failureCount"] = max(0, failure_count)

    label = str(item.get("label") or "").strip()
    if not label:
        item["label"] = f"#{index}"

    return item


def _normalize_queue_payload(raw_payload: Any) -> Dict[str, Any]:
    if not isinstance(raw_payload, dict):
        return _default_queue_state()

    raw_items = raw_payload.get("items")
    items: List[Dict[str, Any]] = []
    if isinstance(raw_items, list):
        for idx, raw_item in enumerate(raw_items):
            normalized_item = _normalize_queue_item(raw_item, idx + 1)
            if normalized_item:
                items.append(normalized_item)

    next_index = _compute_queue_next_index(
        items,
        raw_payload.get("nextIndex"),
    )

    runtime = _normalize_queue_runtime(raw_payload.get("runtime"))
    if not items:
        runtime = {"active": False, "updatedAt": 0}

    return {
        "items": items,
        "nextIndex": next_index,
        "runtime": runtime,
    }


def _load_queue_state() -> Dict[str, Any]:
    path = _queue_storage_file_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return _default_queue_state()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        try:
            path.unlink()
        except OSError:
            pass
        return _default_queue_state()

    normalized = _normalize_queue_payload(parsed)
    if not normalized.get("items"):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    return normalized


def _save_queue_state(raw_payload: Any) -> Dict[str, Any]:
    normalized = _normalize_queue_payload(raw_payload)
    path = _queue_storage_file_path()

    if not normalized.get("items"):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return _default_queue_state()

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(normalized, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, path)
    return normalized


def _clear_queue_state() -> Dict[str, Any]:
    path = _queue_storage_file_path()
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    return _default_queue_state()




def _get_logger():
    return current_app.logger if has_app_context() else logging.getLogger(__name__)

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _set_optimization_state(payload: Dict[str, Any]) -> None:
    with OPTIMIZATION_STATE_LOCK:
        normalized = json.loads(json.dumps(payload))
        normalized["updated_at"] = _utc_now_iso()
        LAST_OPTIMIZATION_STATE.clear()
        LAST_OPTIMIZATION_STATE.update(normalized)


def _get_optimization_state() -> Dict[str, Any]:
    with OPTIMIZATION_STATE_LOCK:
        return json.loads(json.dumps(LAST_OPTIMIZATION_STATE))


def _normalize_run_id(raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if RUN_ID_PATTERN.fullmatch(value):
        return value
    return ""


def _cleanup_cancelled_runs_locked(now_ts: Optional[float] = None) -> None:
    if not CANCELLED_RUNS:
        return

    current = float(time.time() if now_ts is None else now_ts)
    ttl_cutoff = current - CANCELLED_RUNS_TTL_SECONDS
    stale_keys = [run_id for run_id, timestamp in CANCELLED_RUNS.items() if timestamp < ttl_cutoff]
    for run_id in stale_keys:
        CANCELLED_RUNS.pop(run_id, None)

    overflow = len(CANCELLED_RUNS) - CANCELLED_RUNS_MAX_SIZE
    if overflow <= 0:
        return

    for run_id, _ in sorted(CANCELLED_RUNS.items(), key=lambda item: item[1])[:overflow]:
        CANCELLED_RUNS.pop(run_id, None)


def _register_cancelled_run(run_id: str) -> None:
    normalized_run_id = _normalize_run_id(run_id)
    if not normalized_run_id:
        return
    with OPTIMIZATION_STATE_LOCK:
        _cleanup_cancelled_runs_locked()
        CANCELLED_RUNS[normalized_run_id] = time.time()


def _clear_cancelled_run(run_id: str) -> None:
    normalized_run_id = _normalize_run_id(run_id)
    if not normalized_run_id:
        return
    with OPTIMIZATION_STATE_LOCK:
        _cleanup_cancelled_runs_locked()
        CANCELLED_RUNS.pop(normalized_run_id, None)


def _is_run_cancelled(run_id: str) -> bool:
    normalized_run_id = _normalize_run_id(run_id)
    if not normalized_run_id:
        return False
    with OPTIMIZATION_STATE_LOCK:
        _cleanup_cancelled_runs_locked()
        return normalized_run_id in CANCELLED_RUNS


def _parse_warmup_bars(raw_value: Any, default: int = 1000) -> int:
    try:
        warmup_bars = int(raw_value)
    except (TypeError, ValueError):
        warmup_bars = default
    return max(100, min(5000, warmup_bars))


def _execute_backtest_request(strategy_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[Tuple[str, HTTPStatus]]]:
    """Execute one backtest run from current Flask request payload."""

    warmup_bars = _parse_warmup_bars(request.form.get("warmupBars", "1000"))

    csv_path_raw = (request.form.get("csvPath") or "").strip()
    data_source = None
    opened_file = None
    csv_name = ""

    def _close_opened_file() -> None:
        nonlocal opened_file
        if not opened_file:
            return
        try:
            opened_file.close()
        except OSError:  # pragma: no cover - defensive
            pass
        opened_file = None

    if not csv_path_raw:
        return None, ("CSV path is required.", HTTPStatus.BAD_REQUEST)

    try:
        resolved_path = _resolve_csv_path(csv_path_raw)
    except FileNotFoundError:
        return None, ("CSV file not found.", HTTPStatus.BAD_REQUEST)
    except IsADirectoryError:
        return None, ("CSV path must point to a file.", HTTPStatus.BAD_REQUEST)
    except PermissionError as exc:
        return None, (str(exc), HTTPStatus.FORBIDDEN)
    except ValueError as exc:
        message = str(exc).strip() or "CSV path is required."
        return None, (message, HTTPStatus.BAD_REQUEST)
    except OSError:
        return None, ("Failed to access CSV file.", HTTPStatus.BAD_REQUEST)
    try:
        opened_file = resolved_path.open("rb")
    except OSError:
        return None, ("Failed to access CSV file.", HTTPStatus.BAD_REQUEST)
    data_source = opened_file
    csv_name = resolved_path.name

    payload_raw = request.form.get("payload", "{}")
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        _close_opened_file()
        return None, ("Invalid payload JSON.", HTTPStatus.BAD_REQUEST)
    if not isinstance(payload, dict):
        _close_opened_file()
        return None, ("Invalid payload JSON.", HTTPStatus.BAD_REQUEST)

    from strategies import get_strategy

    try:
        strategy_class = get_strategy(strategy_id)
    except ValueError as exc:
        _close_opened_file()
        return None, (str(exc), HTTPStatus.BAD_REQUEST)

    try:
        df = load_data(data_source)
    except ValueError as exc:
        _close_opened_file()
        return None, (str(exc), HTTPStatus.BAD_REQUEST)
    except Exception:  # pragma: no cover - defensive
        _close_opened_file()
        _get_logger().exception("Failed to load CSV")
        return None, ("Failed to load CSV data.", HTTPStatus.INTERNAL_SERVER_ERROR)
    finally:
        _close_opened_file()

    trade_start_idx = 0
    use_date_filter = bool(payload.get("dateFilter", False))
    start_raw = payload.get("start")
    end_raw = payload.get("end")

    if use_date_filter and (start_raw is not None or end_raw is not None):
        start, end = align_date_bounds(df.index, start_raw, end_raw)
        if start_raw not in (None, "") and start is None:
            return None, ("Invalid start date.", HTTPStatus.BAD_REQUEST)
        if end_raw not in (None, "") and end is None:
            return None, ("Invalid end date.", HTTPStatus.BAD_REQUEST)
        try:
            df, trade_start_idx = prepare_dataset_with_warmup(
                df, start, end, warmup_bars
            )
        except Exception:  # pragma: no cover - defensive
            _get_logger().exception("Failed to prepare dataset with warmup")
            return None, ("Failed to prepare dataset.", HTTPStatus.INTERNAL_SERVER_ERROR)

    try:
        _validate_strategy_params(strategy_id, payload)
    except ValueError as exc:
        return None, (str(exc), HTTPStatus.BAD_REQUEST)

    try:
        result = strategy_class.run(df, payload, trade_start_idx)
    except ValueError as exc:
        return None, (str(exc), HTTPStatus.BAD_REQUEST)
    except Exception:  # pragma: no cover - defensive
        _get_logger().exception("Backtest execution failed")
        return None, ("Backtest execution failed.", HTTPStatus.INTERNAL_SERVER_ERROR)

    return {
        "result": result,
        "payload": payload,
        "csv_name": csv_name,
        "warmup_bars": warmup_bars,
    }, None


def _run_trade_export(
    *,
    strategy_id: str,
    csv_path: str,
    params: Dict[str, Any],
    warmup_bars: int,
) -> Tuple[Optional[List[Any]], Optional[str]]:
    from strategies import get_strategy

    try:
        strategy_class = get_strategy(strategy_id)
    except ValueError as exc:
        return None, str(exc)

    try:
        df = load_data(csv_path)
    except Exception as exc:
        return None, str(exc)

    trade_start_idx = 0
    payload = dict(params or {})
    if payload.get("dateFilter"):
        start_raw = payload.get("start")
        end_raw = payload.get("end")
        start, end = align_date_bounds(df.index, start_raw, end_raw)
        if start_raw not in (None, "") and start is None:
            return None, "Invalid start date."
        if end_raw not in (None, "") and end is None:
            return None, "Invalid end date."
        payload["start"] = start
        payload["end"] = end
        try:
            df, trade_start_idx = prepare_dataset_with_warmup(
                df, start, end, int(warmup_bars)
            )
        except Exception:
            return None, "Failed to prepare dataset with warmup."

    try:
        result = strategy_class.run(df, payload, trade_start_idx)
    except Exception as exc:
        return None, str(exc)

    return result.trades, None


def _run_equity_export(
    *,
    strategy_id: str,
    csv_path: str,
    params: Dict[str, Any],
    warmup_bars: int,
) -> Tuple[Optional[List[float]], Optional[List[str]], Optional[str]]:
    from strategies import get_strategy

    try:
        strategy_class = get_strategy(strategy_id)
    except ValueError as exc:
        return None, None, str(exc)

    try:
        df = load_data(csv_path)
    except Exception as exc:
        return None, None, str(exc)

    trade_start_idx = 0
    payload = dict(params or {})
    if payload.get("dateFilter"):
        start_raw = payload.get("start")
        end_raw = payload.get("end")
        start, end = align_date_bounds(df.index, start_raw, end_raw)
        if start_raw not in (None, "") and start is None:
            return None, None, "Invalid start date."
        if end_raw not in (None, "") and end is None:
            return None, None, "Invalid end date."
        payload["start"] = start
        payload["end"] = end
        try:
            df, trade_start_idx = prepare_dataset_with_warmup(
                df, start, end, int(warmup_bars)
            )
        except Exception:
            return None, None, "Failed to prepare dataset with warmup."

    try:
        result = strategy_class.run(df, payload, trade_start_idx)
    except Exception as exc:
        return None, None, str(exc)

    # Match /api/backtest preference: use equity_curve first, then balance_curve.
    equity_curve = list(result.equity_curve or result.balance_curve or [])
    timestamps = [
        ts.isoformat() if hasattr(ts, "isoformat") else ts for ts in (result.timestamps or [])
    ]
    return equity_curve, timestamps, None


def _send_trades_csv(
    *,
    trades: List[Any],
    csv_path: str,
    study: Dict[str, Any],
    filename: str,
) -> object:
    from core.export import _extract_symbol_from_csv_filename

    csv_name = ""
    if csv_path:
        path_obj = Path(csv_path)
        name = path_obj.name
        parent = path_obj.parent.name
        if parent == "merlin_uploads" or name.startswith("upload_"):
            csv_name = study.get("csv_file_name") or name
        else:
            csv_name = name
    else:
        csv_name = study.get("csv_file_name") or ""
    symbol = _extract_symbol_from_csv_filename(csv_name)
    csv_content = export_trades_csv(trades, symbol=symbol)
    buffer = io.BytesIO(csv_content.encode("utf-8"))
    buffer.seek(0)

    return send_file(
        buffer,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


def _get_parameter_types(strategy_id: str) -> Dict[str, str]:
    """Load parameter types from strategy configuration."""

    from strategies import get_strategy_config

    config = get_strategy_config(strategy_id)
    parameters = config.get("parameters", {}) if isinstance(config, dict) else {}

    param_types: Dict[str, str] = {}
    for param_name, param_spec in parameters.items():
        if not isinstance(param_spec, dict):
            continue
        param_types[param_name] = str(param_spec.get("type", "float"))

    return param_types


def _resolve_strategy_id_from_request() -> Tuple[Optional[str], Optional[object]]:
    from strategies import list_strategies

    json_payload = request.get_json(silent=True) if request.is_json else None
    strategy_id = request.form.get("strategy")

    if not strategy_id and isinstance(json_payload, dict):
        strategy_id = json_payload.get("strategy")

    if strategy_id:
        return strategy_id, None

    available = list_strategies()
    if available:
        return available[0]["id"], None

    return None, (jsonify({"error": "No strategies available."}), HTTPStatus.INTERNAL_SERVER_ERROR)


SCORE_METRIC_KEYS: Tuple[str, ...] = (
    "romad",
    "sharpe",
    "pf",
    "ulcer",
    "sqn",
    "consistency",
)

DEFAULT_OPTIMIZER_SCORE_CONFIG: Dict[str, Any] = {
    "filter_enabled": False,
    "min_score_threshold": 60.0,
    "weights": {
        "romad": 0.25,
        "sharpe": 0.20,
        "pf": 0.20,
        "ulcer": 0.15,
        "sqn": 0.10,
        "consistency": 0.10,
    },
    "enabled_metrics": {
        "romad": True,
        "sharpe": True,
        "pf": True,
        "ulcer": True,
        "sqn": True,
        "consistency": True,
    },
    "invert_metrics": {"ulcer": True},
    "normalization_method": "minmax",
    "metric_bounds": {
        "romad": {"min": 0.0, "max": 10.0},
        "sharpe": {"min": -1.0, "max": 3.0},
        "pf": {"min": 0.0, "max": 5.0},
        "ulcer": {"min": 0.0, "max": 20.0},
        "sqn": {"min": -2.0, "max": 7.0},
        "consistency": {"min": -1.0, "max": 1.0},
    },
}


def validate_objectives_config(
    objectives: List[str],
    primary_objective: Optional[str],
) -> Tuple[bool, Optional[str]]:
    if not objectives or len(objectives) < 1:
        return False, "At least 1 objective is required."
    if len(objectives) > 6:
        return False, "Maximum 6 objectives allowed."
    for obj in objectives:
        if obj not in OBJECTIVE_DIRECTIONS:
            return False, f"Unknown objective: {obj}"
    if len(objectives) > 1:
        if not primary_objective:
            return False, "Primary objective required for multi-objective optimization."
        if primary_objective not in objectives:
            return False, "Primary objective must be one of the selected objectives."
    return True, None


def validate_constraints_config(
    constraints: List[Dict[str, Any]],
) -> Tuple[bool, Optional[str]]:
    for i, spec in enumerate(constraints or []):
        if not isinstance(spec, dict):
            return False, f"Constraint {i + 1}: Invalid constraint format"
        metric = spec.get("metric")
        threshold = spec.get("threshold")
        enabled = spec.get("enabled", False)
        if not enabled:
            continue
        if metric not in CONSTRAINT_OPERATORS:
            return False, f"Constraint {i + 1}: Unknown metric '{metric}'"
        if threshold is None:
            return False, f"Constraint {i + 1}: Threshold is required"
        try:
            float(threshold)
        except (TypeError, ValueError):
            return False, f"Constraint {i + 1}: Threshold must be a number"
    return True, None


def validate_sampler_config(
    sampler_type: str,
    population_size: Optional[int],
    crossover_prob: Optional[float],
) -> Tuple[bool, Optional[str]]:
    valid_samplers = {"tpe", "nsga2", "nsga3", "random"}
    if sampler_type not in valid_samplers:
        return False, f"Unknown sampler: {sampler_type}"
    if sampler_type in ("nsga2", "nsga3"):
        if population_size is not None:
            if population_size < 2:
                return False, "Population size must be at least 2"
            if population_size > 1000:
                return False, "Population size must be at most 1000"
        if crossover_prob is not None:
            if not (0.0 <= crossover_prob <= 1.0):
                return False, "Crossover probability must be between 0 and 1"
    return True, None

PRESETS_DIR = Path(__file__).resolve().parent.parent / "presets"
DEFAULT_PRESET_NAME = "defaults"
VALID_PRESET_NAME_RE = re.compile(r"^[A-Za-z0-9 _\-]{1,64}$")

# Default preset containing only date fields.
# Strategy/backtest parameters are added dynamically from payload.
DEFAULT_PRESET: Dict[str, Any] = {
    "dateFilter": True,
    "start": None,
    "end": None,
}
BOOL_FIELDS = {"dateFilter"}
INT_FIELDS = set()
FLOAT_FIELDS = set()

LIST_FIELDS: set = set()
STRING_FIELDS = {"start", "end"}
ALLOWED_PRESET_FIELDS = None  # None = accept all fields (strategy/backtest params included)


def _clone_default_template() -> Dict[str, Any]:
    # Use minimal defaults only. Strategy defaults are in strategy.py.
    return json.loads(json.dumps(DEFAULT_PRESET))


def _ensure_presets_directory() -> None:
    PRESETS_DIR.mkdir(parents=True, exist_ok=True)
    defaults_path = PRESETS_DIR / f"{DEFAULT_PRESET_NAME}.json"
    if not defaults_path.exists():
        _write_preset(DEFAULT_PRESET_NAME, DEFAULT_PRESET)


def _validate_preset_name(name: str) -> str:
    if not isinstance(name, str):
        raise ValueError("Preset name must be a string.")
    normalized = name.strip()
    if not normalized:
        raise ValueError("Preset name cannot be empty.")
    if normalized.lower() == DEFAULT_PRESET_NAME:
        raise ValueError("Use the defaults endpoint to overwrite default settings.")
    if not VALID_PRESET_NAME_RE.match(normalized):
        raise ValueError(
            "Preset name may only contain letters, numbers, spaces, hyphens, and underscores."
        )
    return normalized


def _preset_path(name: str) -> Path:
    safe_name = Path(name).name
    return PRESETS_DIR / f"{safe_name}.json"


def _write_preset(name: str, values: Dict[str, Any]) -> None:
    path = _preset_path(name)
    serialized = json.loads(json.dumps(values))
    with path.open("w", encoding="utf-8") as handle:
        json.dump(serialized, handle, ensure_ascii=False, indent=2, sort_keys=False)


def _load_preset(name: str) -> Dict[str, Any]:
    path = _preset_path(name)
    if not path.exists():
        raise FileNotFoundError(name)
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("Preset file is corrupted.")
    return data


def _list_presets() -> List[Dict[str, Any]]:
    presets: List[Dict[str, Any]] = []
    for path in sorted(PRESETS_DIR.glob("*.json")):
        name = path.stem
        presets.append({"name": name, "is_default": name.lower() == DEFAULT_PRESET_NAME})
    return presets


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
    return False


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        if not math.isfinite(value):
            if math.isinf(value):
                return "inf" if value > 0 else "-inf"
            return "nan"
        return value
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _split_timestamp(value: str) -> Tuple[str, str]:
    normalized = (value or "").strip()
    if not normalized:
        return "", ""
    candidate = normalized.replace(" ", "T", 1)
    candidate = candidate.rstrip("Zz")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        if "T" in normalized:
            date_part, _, time_part = normalized.partition("T")
        elif " " in normalized:
            date_part, _, time_part = normalized.partition(" ")
        else:
            return normalized, ""
        return date_part.strip(), time_part.strip()
    date_part = parsed.date().isoformat()
    if parsed.time().second == 0 and parsed.time().microsecond == 0:
        time_part = parsed.time().strftime("%H:%M")
    else:
        time_part = parsed.time().strftime("%H:%M:%S")
    return date_part, time_part


def _convert_import_value(name: str, raw_value: str) -> Any:
    if name in BOOL_FIELDS:
        return _coerce_bool(raw_value)
    if name in INT_FIELDS:
        try:
            return int(round(float(raw_value)))
        except (TypeError, ValueError):
            return 0
    if name in FLOAT_FIELDS:
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return 0.0
    return raw_value


def _parse_csv_parameter_block(file_storage) -> Tuple[Dict[str, Any], List[str], List[str]]:
    content = file_storage.read()
    if isinstance(content, bytes):
        text = content.decode("utf-8-sig", errors="replace")
    else:
        text = str(content)

    lines = text.splitlines()
    csv_parameters: Dict[str, Any] = {}
    applied: List[str] = []

    header_seen = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if header_seen:
                break
            continue
        if not header_seen:
            header_seen = True
            continue
        name, _, value = line.partition(",")
        param_name = name.strip()
        if not param_name:
            continue
        csv_parameters[param_name] = value.strip()

    updates: Dict[str, Any] = {}
    # Use strategy config to drive type-aware parsing so imports stay generic across strategies.
    strategy_id = request.form.get("strategy")
    if not strategy_id and request.is_json:
        payload = request.get_json(silent=True) or {}
        if isinstance(payload, dict):
            strategy_id = payload.get("strategy")

    param_types: Dict[str, str] = {}
    strategy_resolution_error = None
    if not strategy_id:
        try:
            from strategies import list_strategies

            available = list_strategies()
            if available:
                strategy_id = available[0]["id"]
        except Exception:
            strategy_id = None
            strategy_resolution_error = (
                "Strategy not provided and no strategies discovered to infer parameter types."
            )

    if strategy_id:
        try:
            from strategies import get_strategy_config

            config = get_strategy_config(strategy_id)
            config_parameters = config.get("parameters", {}) if isinstance(config, dict) else {}
            for param_name, param_spec in config_parameters.items():
                if not isinstance(param_spec, dict):
                    continue
                param_types[param_name] = str(param_spec.get("type", "float")).lower()
        except Exception:
            param_types = {}
            strategy_resolution_error = (
                f"Strategy '{strategy_id}' configuration could not be loaded for type inference."
            )

    # If strategy typing is unavailable, refuse to silently import strategy-specific parameters.
    if not param_types:
        # Fields that are safe to import without strategy typing.
        untyped_allowed_fields = {"start", "end", "dateFilter"}
        missing_typed_fields = [
            name for name in csv_parameters.keys() if name not in untyped_allowed_fields
        ]
        if missing_typed_fields:
            reason = strategy_resolution_error or "Parameter types unavailable."
            formatted = ", ".join(sorted(missing_typed_fields))
            raise ValueError(
                f"Cannot import CSV because strategy parameter types are unavailable. "
                f"Unsupported fields without typing: {formatted}. {reason}"
            )

    errors: List[str] = []

    for name, raw_value in csv_parameters.items():
        if name == "start":
            date_part, time_part = _split_timestamp(raw_value)
            if date_part:
                updates["startDate"] = date_part
                applied.append("startDate")
            if time_part:
                updates["startTime"] = time_part
                applied.append("startTime")
            continue
        if name == "end":
            date_part, time_part = _split_timestamp(raw_value)
            if date_part:
                updates["endDate"] = date_part
                applied.append("endDate")
            if time_part:
                updates["endTime"] = time_part
                applied.append("endTime")
            continue

        param_type = param_types.get(name, "")
        if param_type in {"select", "options"}:
            value = str(raw_value or "").strip().upper()
            if value:
                updates[name] = value
                applied.append(name)
            continue
        if param_type == "int":
            try:
                updates[name] = int(round(float(raw_value)))
            except (TypeError, ValueError):
                errors.append(f"{name}: expected integer, got '{raw_value}'")
            else:
                applied.append(name)
            continue
        if param_type == "float":
            try:
                updates[name] = float(raw_value)
            except (TypeError, ValueError):
                errors.append(f"{name}: expected number, got '{raw_value}'")
            else:
                applied.append(name)
            continue
        if param_type in {"bool", "boolean"}:
            updates[name] = _coerce_bool(raw_value)
            applied.append(name)
            continue

        converted = _convert_import_value(name, raw_value)
        updates[name] = converted
        applied.append(name)

    return updates, applied, errors


def _validate_strategy_params(strategy_id: str, params: Dict[str, Any]) -> None:
    """Validate and coerce strategy parameters based on config definitions."""

    from strategies import get_strategy_config

    try:
        config = get_strategy_config(strategy_id)
    except Exception:
        return

    definitions = config.get("parameters", {}) if isinstance(config, dict) else {}
    if not isinstance(definitions, dict):
        return

    for name, definition in definitions.items():
        if not isinstance(definition, dict):
            continue

        value = params.get(name)
        if value is None:
            continue

        param_type = definition.get("type", "float")

        if param_type == "int":
            if not isinstance(value, int):
                try:
                    params[name] = int(value)
                except (TypeError, ValueError):
                    raise ValueError(f"{name} must be an integer")
        elif param_type == "float":
            if not isinstance(value, (int, float)):
                try:
                    params[name] = float(value)
                except (TypeError, ValueError):
                    raise ValueError(f"{name} must be a number")
        elif param_type in {"select", "options"}:
            options = definition.get("options", [])
            if options and value not in options:
                raise ValueError(f"{name} must be one of {options}, got {value}")
        elif param_type == "bool":
            if not isinstance(value, bool):
                params[name] = bool(value)

        if param_type in {"int", "float"}:
            min_value = definition.get("min")
            max_value = definition.get("max")
            numeric_value = params.get(name)
            if min_value is not None and numeric_value < min_value:
                raise ValueError(f"{name} must be >= {min_value}")
            if max_value is not None and numeric_value > max_value:
                raise ValueError(f"{name} must be <= {max_value}")


_ensure_presets_directory()


def _normalize_preset_payload(values: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(values, dict):
        raise ValueError("Preset values must be provided as a dictionary.")
    normalized = _clone_default_template()
    for key, value in values.items():
        if key in LIST_FIELDS:
            if isinstance(value, (list, tuple)):
                cleaned = [str(item).strip().upper() for item in value if str(item).strip()]
            elif isinstance(value, str) and value.strip():
                cleaned = [value.strip().upper()]
            else:
                cleaned = []
            if cleaned:
                normalized[key] = cleaned
            continue
        if key in BOOL_FIELDS:
            normalized[key] = _coerce_bool(value)
            continue
        if key in INT_FIELDS:
            try:
                converted = int(round(float(value)))
            except (TypeError, ValueError):
                continue
            if key == "workerProcesses":
                if converted < 1:
                    converted = 1
                elif converted > 32:
                    converted = 32
            normalized[key] = converted
            continue
        if key in FLOAT_FIELDS:
            try:
                converted_float = float(value)
            except (TypeError, ValueError):
                continue
            if key == "minProfitThreshold":
                converted_float = max(0.0, min(99000.0, converted_float))
            normalized[key] = converted_float
            continue
        if key in STRING_FIELDS:
            normalized[key] = str(value).strip()
            continue
        normalized[key] = value
    return normalized


def _resolve_csv_path(raw_path: str) -> Path:
    if raw_path is None:
        raise ValueError("CSV path is empty.")
    raw_value = str(raw_path).strip()
    if not raw_value:
        raise ValueError("CSV path is empty.")
    candidate = Path(raw_value).expanduser()
    if not candidate.is_absolute():
        raise ValueError("CSV path must be absolute.")
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise FileNotFoundError(str(candidate)) from exc
    if not resolved.is_file():
        raise IsADirectoryError(str(resolved))
    if not _is_csv_path_allowed(resolved):
        raise PermissionError("CSV path is outside allowed roots.")
    return resolved


def _validate_csv_for_study(csv_path: str, study: Dict[str, Any]) -> Tuple[bool, List[str], Optional[str]]:
    warnings: List[str] = []
    try:
        df = load_data(csv_path)
    except Exception as exc:
        return False, warnings, str(exc)

    expected_start = study.get("dataset_start_date")
    expected_end = study.get("dataset_end_date")
    if expected_start:
        try:
            expected_start_ts = pd.Timestamp(expected_start).date()
            if df.index[0].date() != expected_start_ts:
                warnings.append(
                    f"Dataset start date differs (expected {expected_start}, got {df.index[0].date()})."
                )
        except Exception:
            warnings.append("Could not validate dataset start date.")
    if expected_end:
        try:
            expected_end_ts = pd.Timestamp(expected_end).date()
            if df.index[-1].date() != expected_end_ts:
                warnings.append(
                    f"Dataset end date differs (expected {expected_end}, got {df.index[-1].date()})."
                )
        except Exception:
            warnings.append("Could not validate dataset end date.")

    original_name = study.get("csv_file_name")
    if original_name:
        selected_name = Path(csv_path).name
        if selected_name != original_name:
            warnings.append(
                f"Filename differs from original ({original_name} vs {selected_name})."
            )

    return True, warnings, None


def _build_trial_metrics(trial: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    def get(field: str, default: Any = None) -> Any:
        key = f"{prefix}{field}" if prefix else field
        return trial.get(key, default)

    return {
        "net_profit_pct": get("net_profit_pct") or 0.0,
        "max_drawdown_pct": get("max_drawdown_pct") or 0.0,
        "total_trades": get("total_trades") or 0,
        "win_rate": get("win_rate") or 0.0,
        "max_consecutive_losses": get("max_consecutive_losses") or 0,
        "sharpe_ratio": get("sharpe_ratio"),
        "sortino_ratio": get("sortino_ratio"),
        "romad": get("romad"),
        "profit_factor": get("profit_factor"),
        "ulcer_index": get("ulcer_index"),
        "sqn": get("sqn"),
        "consistency_score": get("consistency_score"),
    }


def _find_wfa_window(study_data: Dict[str, Any], window_number: int) -> Optional[Dict[str, Any]]:
    for window in study_data.get("windows") or []:
        if int(window.get("window_number") or 0) == int(window_number):
            return window
    return None


def _resolve_wfa_period(
    window: Dict[str, Any],
    period: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    def _normalize_boundary(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def _resolve_boundary(*, ts_key: str, date_key: str, legacy_key: Optional[str] = None) -> Optional[str]:
        exact = _normalize_boundary(window.get(ts_key))
        if exact:
            return exact
        date_value = _normalize_boundary(window.get(date_key))
        if date_value:
            return date_value
        if legacy_key:
            return _normalize_boundary(window.get(legacy_key))
        return None

    period = (period or "").lower()
    if period == "optuna_is":
        start = _resolve_boundary(
            ts_key="optimization_start_ts",
            date_key="optimization_start_date",
        ) or _resolve_boundary(
            ts_key="is_start_ts",
            date_key="is_start_date",
        )
        end = _resolve_boundary(
            ts_key="optimization_end_ts",
            date_key="optimization_end_date",
        ) or _resolve_boundary(
            ts_key="is_end_ts",
            date_key="is_end_date",
        )
    elif period == "is":
        start = _resolve_boundary(
            ts_key="is_start_ts",
            date_key="is_start_date",
        )
        end = _resolve_boundary(
            ts_key="is_end_ts",
            date_key="is_end_date",
        )
    elif period == "ft":
        start = _resolve_boundary(
            ts_key="ft_start_ts",
            date_key="ft_start_date",
        )
        end = _resolve_boundary(
            ts_key="ft_end_ts",
            date_key="ft_end_date",
        )
    elif period == "oos":
        start = _resolve_boundary(
            ts_key="oos_start_ts",
            date_key="oos_start_date",
            legacy_key="oos_start",
        )
        end = _resolve_boundary(
            ts_key="oos_end_ts",
            date_key="oos_end_date",
            legacy_key="oos_end",
        )
    elif period == "both":
        start = _resolve_boundary(
            ts_key="is_start_ts",
            date_key="is_start_date",
        )
        end = _resolve_boundary(
            ts_key="oos_end_ts",
            date_key="oos_end_date",
            legacy_key="oos_end",
        )
    else:
        return None, None, "Invalid period."

    if not start or not end:
        return None, None, "Missing period date range."
    return start, end, None


def _build_optimization_config(
    csv_file,
    payload: dict,
    worker_processes=None,
    strategy_id=None,
    warmup_bars: Optional[int] = None,
) -> OptimizationConfig:
    if not isinstance(payload, dict):
        raise ValueError("Invalid optimization config payload.")

    from strategies import list_strategies

    def _parse_bool(value, default=False):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y", "on"}:
                return True
            if lowered in {"false", "0", "no", "n", "off"}:
                return False
        return default

    def _sanitize_score_config(raw_config: Any) -> Dict[str, Any]:
        source = raw_config if isinstance(raw_config, dict) else {}
        normalized = json.loads(json.dumps(DEFAULT_OPTIMIZER_SCORE_CONFIG))
        legacy_consistency_bounds = {"min": 0.0, "max": 100.0}

        filter_value = source.get("filter_enabled")
        normalized["filter_enabled"] = _parse_bool(
            filter_value, normalized.get("filter_enabled", False)
        )

        try:
            threshold = float(source.get("min_score_threshold"))
        except (TypeError, ValueError):
            threshold = normalized.get("min_score_threshold", 0.0)
        normalized["min_score_threshold"] = max(0.0, min(100.0, threshold))

        weights_raw = source.get("weights")
        if isinstance(weights_raw, dict):
            weights: Dict[str, float] = {}
            for key in SCORE_METRIC_KEYS:
                try:
                    weight_value = float(weights_raw.get(key, normalized["weights"].get(key, 0.0)))
                except (TypeError, ValueError):
                    weight_value = normalized["weights"].get(key, 0.0)
                weights[key] = max(0.0, min(1.0, weight_value))
            normalized["weights"].update(weights)

        enabled_raw = source.get("enabled_metrics")
        if isinstance(enabled_raw, dict):
            enabled: Dict[str, bool] = {}
            for key in SCORE_METRIC_KEYS:
                enabled[key] = _parse_bool(
                    enabled_raw.get(key, normalized["enabled_metrics"].get(key, False)),
                    normalized["enabled_metrics"].get(key, False),
                )
            normalized["enabled_metrics"].update(enabled)

        invert_raw = source.get("invert_metrics")
        invert_flags: Dict[str, bool] = {}
        if isinstance(invert_raw, dict):
            for key in SCORE_METRIC_KEYS:
                invert_flags[key] = _parse_bool(
                    invert_raw.get(key, False),
                    False,
                )
        else:
            for key in SCORE_METRIC_KEYS:
                invert_flags[key] = normalized["invert_metrics"].get(key, False)
        normalized["invert_metrics"] = {
            key: value for key, value in invert_flags.items() if value
        }

        normalization_value = source.get("normalization_method")
        if isinstance(normalization_value, str) and normalization_value.strip():
            normalized["normalization_method"] = normalization_value.strip().lower()

        bounds_raw = source.get("metric_bounds")
        if isinstance(bounds_raw, dict):
            bounds: Dict[str, Dict[str, float]] = {}
            for metric_key in SCORE_METRIC_KEYS:
                if metric_key in bounds_raw and isinstance(bounds_raw[metric_key], dict):
                    metric_bounds = bounds_raw[metric_key]
                    try:
                        bounds[metric_key] = {
                            "min": float(
                                metric_bounds.get(
                                    "min", normalized["metric_bounds"][metric_key]["min"]
                                )
                            ),
                            "max": float(
                                metric_bounds.get(
                                    "max", normalized["metric_bounds"][metric_key]["max"]
                                )
                            ),
                        }
                    except (TypeError, ValueError, KeyError):
                        bounds[metric_key] = normalized["metric_bounds"].get(
                            metric_key, {"min": 0.0, "max": 100.0}
                        )
                else:
                    bounds[metric_key] = normalized["metric_bounds"].get(
                        metric_key, {"min": 0.0, "max": 100.0}
                    )
            consistency_bounds = bounds.get("consistency")
            if (
                consistency_bounds
                and math.isclose(consistency_bounds.get("min", 0.0), legacy_consistency_bounds["min"])
                and math.isclose(consistency_bounds.get("max", 0.0), legacy_consistency_bounds["max"])
            ):
                bounds["consistency"] = dict(normalized["metric_bounds"]["consistency"])
            normalized["metric_bounds"] = bounds

        return normalized

    if strategy_id is None:
        strategy_id = payload.get("strategy")

    if not strategy_id:
        available_strategies = list_strategies()
        if available_strategies:
            strategy_id = available_strategies[0]["id"]
        else:
            raise ValueError("Strategy ID is required for optimization.")

    if warmup_bars is None:
        warmup_bars_raw = payload.get("warmup_bars", 1000)
        try:
            warmup_bars = int(warmup_bars_raw)
            warmup_bars = max(100, min(5000, warmup_bars))
        except (TypeError, ValueError):
            warmup_bars = 1000
    else:
        try:
            warmup_bars = max(100, min(5000, int(warmup_bars)))
        except (TypeError, ValueError):
            warmup_bars = 1000

    enabled_params = payload.get("enabled_params")
    if not isinstance(enabled_params, dict):
        raise ValueError("enabled_params must be a dictionary.")

    param_ranges_raw = payload.get("param_ranges", {})
    if not isinstance(param_ranges_raw, dict):
        raise ValueError("param_ranges must be a dictionary.")
    param_ranges = {}
    select_range_options: Dict[str, List[Any]] = {}
    for name, values in param_ranges_raw.items():
        if isinstance(values, dict):
            range_type = str(values.get("type", "")).lower()
            if range_type in {"select", "options"}:
                raw_options = values.get("values") or values.get("options") or []
                if isinstance(raw_options, (list, tuple)):
                    normalized = [opt for opt in raw_options if str(opt).strip()]
                    if normalized:
                        select_range_options[name] = normalized
                continue
            raise ValueError(f"Unsupported range specification for parameter '{name}'.")

        if not isinstance(values, (list, tuple)) or len(values) != 3:
            raise ValueError(f"Invalid range for parameter '{name}'.")
        start, stop, step = values
        param_ranges[name] = (float(start), float(stop), float(step))

    fixed_params = payload.get("fixed_params", {})
    if not isinstance(fixed_params, dict):
        raise ValueError("fixed_params must be a dictionary.")

    try:
        strategy_param_types = _get_parameter_types(strategy_id)
    except Exception as exc:
        _get_logger().warning(
            "Could not load parameter types for %s: %s", strategy_id, exc
        )
        strategy_param_types = {}
    payload_param_types = payload.get("param_types", {})
    if isinstance(payload_param_types, dict):
        merged_param_types = {**strategy_param_types, **payload_param_types}
    else:
        merged_param_types = strategy_param_types

    for name, options in select_range_options.items():
        if not options:
            continue
        key = f"{name}_options"
        existing = fixed_params.get(key)
        if not existing:
            fixed_params[key] = list(options)

    risk_per_trade = payload.get("risk_per_trade_pct", 2.0)
    contract_size = payload.get("contract_size", 0.01)
    commission_rate = payload.get("commission_rate", 0.0005)

    filter_min_profit_raw = payload.get("filter_min_profit")
    filter_min_profit = _parse_bool(filter_min_profit_raw, False)

    threshold_raw = payload.get("min_profit_threshold", 0.0)
    try:
        min_profit_threshold = float(threshold_raw)
    except (TypeError, ValueError):
        min_profit_threshold = 0.0
    min_profit_threshold = max(0.0, min(99000.0, min_profit_threshold))

    if hasattr(csv_file, "seek"):
        try:
            csv_file.seek(0)
        except Exception:  # pragma: no cover - defensive
            pass
    elif hasattr(csv_file, "stream") and hasattr(csv_file.stream, "seek"):
        csv_file.stream.seek(0)
    worker_processes_value = 6 if worker_processes is None else int(worker_processes)
    if worker_processes_value < 1:
        worker_processes_value = 1
    elif worker_processes_value > 32:
        worker_processes_value = 32

    score_config_payload = payload.get("score_config")
    score_config = _sanitize_score_config(score_config_payload)
    detailed_log = _parse_bool(payload.get("detailed_log", False), False)

    optimization_mode_raw = payload.get("optimization_mode", "optuna")
    optimization_mode = str(optimization_mode_raw).strip().lower() or "optuna"
    if optimization_mode != "optuna":
        raise ValueError("Grid Search has been removed. Use Optuna optimization only.")

    objectives = payload.get("objectives", [])
    if not isinstance(objectives, list):
        objectives = []
    primary_objective = payload.get("primary_objective")

    optuna_budget_mode = str(payload.get("optuna_budget_mode", "trials")).strip().lower()

    try:
        optuna_n_trials = int(payload.get("optuna_n_trials", 500))
    except (TypeError, ValueError):
        optuna_n_trials = 500

    try:
        optuna_time_limit = int(payload.get("optuna_time_limit", 3600))
    except (TypeError, ValueError):
        optuna_time_limit = 3600

    try:
        optuna_convergence = int(payload.get("optuna_convergence", 50))
    except (TypeError, ValueError):
        optuna_convergence = 50

    try:
        n_startup_trials = int(payload.get("n_startup_trials", 20))
    except (TypeError, ValueError):
        n_startup_trials = 20
    coverage_mode = _parse_bool(payload.get("coverage_mode", False), False)

    optuna_enable_pruning = _parse_bool(payload.get("optuna_enable_pruning", True), True)
    optuna_pruner = str(payload.get("optuna_pruner", "median")).strip().lower()
    optuna_save_study = _parse_bool(payload.get("optuna_save_study", False), False)
    if optuna_save_study:
        _get_logger().warning("Ignoring deprecated optuna_save_study request; raw Optuna persistence is disabled.")

    sanitize_enabled = _parse_bool(payload.get("sanitize_enabled", True), True)
    sanitize_trades_threshold_raw = payload.get("sanitize_trades_threshold", 0)
    try:
        sanitize_trades_threshold = int(sanitize_trades_threshold_raw)
    except (TypeError, ValueError):
        raise ValueError("sanitize_trades_threshold must be a non-negative integer.")
    if sanitize_trades_threshold < 0:
        raise ValueError("sanitize_trades_threshold must be >= 0.")

    sampler_type = str(payload.get("sampler", "tpe")).strip().lower()
    population_size = payload.get("population_size")
    crossover_prob = payload.get("crossover_prob")
    mutation_prob = payload.get("mutation_prob")
    swapping_prob = payload.get("swapping_prob")

    def _parse_optional_int(value):
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _parse_optional_float(value):
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    population_size = _parse_optional_int(population_size)
    crossover_prob = _parse_optional_float(crossover_prob)
    mutation_prob = _parse_optional_float(mutation_prob)
    swapping_prob = _parse_optional_float(swapping_prob)

    allowed_budget_modes = {"trials", "time", "convergence"}
    allowed_pruners = {"median", "percentile", "patient", "none"}

    if optuna_budget_mode not in allowed_budget_modes:
        raise ValueError(f"Invalid Optuna budget mode: {optuna_budget_mode}")
    if optuna_pruner not in allowed_pruners:
        raise ValueError(f"Invalid Optuna pruner: {optuna_pruner}")

    optuna_n_trials = max(10, optuna_n_trials)
    optuna_time_limit = max(60, optuna_time_limit)
    optuna_convergence = max(10, optuna_convergence)
    n_startup_trials = max(0, n_startup_trials)

    if len(objectives) > 1:
        optuna_enable_pruning = False

    optuna_params: Dict[str, Any] = {
        "objectives": objectives,
        "primary_objective": primary_objective,
        "constraints": payload.get("constraints", []),
        "sampler_type": sampler_type,
        "population_size": population_size,
        "crossover_prob": crossover_prob,
        "mutation_prob": mutation_prob,
        "swapping_prob": swapping_prob,
        "n_startup_trials": n_startup_trials,
        "coverage_mode": coverage_mode,
        "optuna_budget_mode": optuna_budget_mode,
        "optuna_n_trials": optuna_n_trials,
        "optuna_time_limit": optuna_time_limit,
        "optuna_convergence": optuna_convergence,
        "optuna_enable_pruning": optuna_enable_pruning,
        "optuna_pruner": optuna_pruner,
        "sanitize_enabled": sanitize_enabled,
        "sanitize_trades_threshold": sanitize_trades_threshold,
    }

    config = OptimizationConfig(
        csv_file=csv_file,
        strategy_id=str(strategy_id),
        enabled_params=enabled_params,
        param_ranges=param_ranges,
        param_types=merged_param_types,
        fixed_params=fixed_params,
        worker_processes=worker_processes_value,
        warmup_bars=int(warmup_bars),
        contract_size=float(contract_size),
        commission_rate=float(commission_rate),
        risk_per_trade_pct=float(risk_per_trade),
        filter_min_profit=filter_min_profit,
        min_profit_threshold=min_profit_threshold,
        score_config=score_config,
        detailed_log=detailed_log,
        optimization_mode=optimization_mode,
        objectives=objectives,
        primary_objective=primary_objective,
        constraints=payload.get("constraints", []),
        sanitize_enabled=sanitize_enabled,
        sanitize_trades_threshold=sanitize_trades_threshold,
        sampler_type=sampler_type,
        population_size=population_size if population_size is not None else 50,
        crossover_prob=crossover_prob if crossover_prob is not None else 0.9,
        mutation_prob=mutation_prob if mutation_prob is not None else None,
        swapping_prob=swapping_prob if swapping_prob is not None else 0.5,
        n_startup_trials=n_startup_trials,
        coverage_mode=coverage_mode,
    )

    if optimization_mode == "optuna":
        for key, value in optuna_params.items():
            setattr(config, key, value)

    return config



