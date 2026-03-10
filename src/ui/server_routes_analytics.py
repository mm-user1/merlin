import json
import math
import re
import time
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from flask import jsonify, render_template, request

from core.analytics import aggregate_equity_curves
from core.storage import (
    create_study_set,
    delete_study_set,
    delete_study_sets,
    get_active_db_name,
    get_db_connection,
    list_study_sets,
    reorder_study_sets,
    update_study_sets_color,
    update_study_set,
)


def register_routes(app):
    analytics_equity_cache: Dict[Tuple[str, Tuple[str, ...]], Tuple[float, Dict[str, Any]]] = {}
    analytics_equity_cache_ttl_seconds = 10.0
    analytics_equity_cache_max_entries = 256
    analytics_equity_max_study_ids = 500
    analytics_equity_chunk_size = 200

    def _parse_date_flexible(date_str: Any) -> Optional[datetime]:
        """Parse date in YYYY-MM-DD or YYYY.MM.DD format."""
        value = str(date_str or "").strip()
        if not value:
            return None
        for fmt in ("%Y-%m-%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _period_days(start_date: Any, end_date: Any) -> Optional[int]:
        start = _parse_date_flexible(start_date)
        end = _parse_date_flexible(end_date)
        if start is None or end is None:
            return None
        return max(0, (end - start).days)

    def _date_sort_key(date_str: Any) -> Tuple[int, Any]:
        parsed = _parse_date_flexible(date_str)
        if parsed is not None:
            return (0, parsed)
        return (1, str(date_str or ""))

    def _safe_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed):
            return None
        return parsed

    def _safe_int(value: Any) -> Optional[int]:
        parsed = _safe_float(value)
        if parsed is None:
            return None
        return int(round(parsed))

    def _safe_bool(value: Any) -> Optional[bool]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            parsed = _safe_int(value)
            if parsed is None:
                return None
            return bool(parsed)
        if isinstance(value, str):
            token = value.strip().lower()
            if token in {"1", "true", "yes", "y", "on"}:
                return True
            if token in {"0", "false", "no", "n", "off"}:
                return False
        return None

    def _parse_json_dict(raw_value: Any) -> Dict[str, Any]:
        if isinstance(raw_value, dict):
            return raw_value
        if not raw_value:
            return {}
        try:
            parsed = json.loads(raw_value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        if isinstance(parsed, dict):
            return parsed
        return {}

    def _parse_json_array(raw_value: Any) -> List[Any]:
        if isinstance(raw_value, list):
            return raw_value
        if not raw_value:
            return []
        try:
            parsed = json.loads(raw_value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
        if isinstance(parsed, list):
            return parsed
        return []

    def _timeframe_to_minutes(value: Any) -> float:
        token = str(value or "").strip().lower()
        if not token:
            return float("inf")
        m = re.match(r"^(\d+)(m)?$", token)
        if m:
            return float(m.group(1))
        m = re.match(r"^(\d+)h$", token)
        if m:
            return float(int(m.group(1)) * 60)
        m = re.match(r"^(\d+)d$", token)
        if m:
            return float(int(m.group(1)) * 1440)
        m = re.match(r"^(\d+)w$", token)
        if m:
            return float(int(m.group(1)) * 10080)
        return float("inf")

    def _normalize_tf(tf_str: str) -> str:
        token = str(tf_str or "").strip()
        if not token:
            return ""
        lower = token.lower()
        if lower.endswith(("h", "d", "w")):
            if lower.endswith("d"):
                return lower[:-1] + "D"
            return lower
        if not lower.endswith("m"):
            return lower
        try:
            minutes = int(lower[:-1])
        except ValueError:
            return lower
        if minutes >= 1440 and minutes % 1440 == 0:
            return f"{minutes // 1440}D"
        if minutes >= 60 and minutes % 60 == 0:
            return f"{minutes // 60}h"
        return f"{minutes}m"

    def _parse_csv_filename(csv_file_name: Any) -> Tuple[Optional[str], Optional[str]]:
        """Strict Merlin parser for symbol/timeframe from csv_file_name."""
        value = str(csv_file_name or "").strip()
        if not value:
            return None, None
        name = Path(value).name

        # Numeric TF: "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"
        match = re.match(r"^[^_]*_([^,]+),\s*(\d+)\s", name)
        if match:
            symbol = match.group(1).strip()
            tf_minutes = int(match.group(2))
            tf_map = {
                1: "1m",
                5: "5m",
                15: "15m",
                30: "30m",
                60: "1h",
                120: "2h",
                240: "4h",
                1440: "1D",
            }
            return symbol, tf_map.get(tf_minutes, f"{tf_minutes}m")

        # Human TF: "OKX_LINKUSDT.P, 1h 2025.05.01-2025.11.20.csv"
        match = re.match(r"^[^_]*_([^,]+),\s*(\d+[mhdwMHDW])\s", name)
        if match:
            symbol = match.group(1).strip()
            tf = _normalize_tf(match.group(2))
            return symbol, tf

        return None, None

    def _format_strategy_label(strategy_id: Any, strategy_version: Any) -> str:
        strategy_raw = str(strategy_id or "").strip()
        version_raw = str(strategy_version or "").strip()
        if not strategy_raw:
            return "Unknown"
        match = re.match(r"^s(\d+)_", strategy_raw, re.IGNORECASE)
        if match:
            strategy_label = f"S{int(match.group(1)):02d}"
        else:
            strategy_label = strategy_raw
        if version_raw:
            version_label = version_raw if version_raw.lower().startswith("v") else f"v{version_raw}"
            return f"{strategy_label} {version_label}"
        return strategy_label

    def _format_wfa_mode(adaptive_mode: Any) -> str:
        adaptive_int = _safe_int(adaptive_mode)
        if adaptive_int == 0:
            return "Fixed"
        if adaptive_int == 1:
            return "Adaptive"
        return "Unknown"

    def _format_wfa_mode_bool(adaptive_mode: Optional[bool]) -> str:
        if adaptive_mode is True:
            return "Adaptive"
        if adaptive_mode is False:
            return "Fixed"
        return "Unknown"

    def _extract_oos_period_days(config_json_value: Any) -> Optional[int]:
        config_payload = _parse_json_dict(config_json_value)
        wfa_payload = config_payload.get("wfa")
        if not isinstance(wfa_payload, dict):
            return None
        return _safe_int(wfa_payload.get("oos_period_days"))

    def _json_error(message: str, status: HTTPStatus) -> object:
        return jsonify({"error": message}), status

    def _parse_study_ids_payload(payload: Any) -> List[str]:
        if payload is None:
            return []
        if not isinstance(payload, list):
            raise ValueError("study_ids must be an array.")
        values: List[str] = []
        seen = set()
        for raw in payload:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            values.append(value)
        return values

    def _parse_set_ids_payload(payload: Any) -> List[int]:
        if not isinstance(payload, list):
            raise ValueError("set_ids must be an array.")
        values: List[int] = []
        seen = set()
        for raw in payload:
            try:
                value = int(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("set_ids must contain integer set IDs.") from exc
            if value <= 0 or value in seen:
                continue
            seen.add(value)
            values.append(value)
        if not values:
            raise ValueError("set_ids must contain at least one set ID.")
        return values

    def _chunked(values: Sequence[str], chunk_size: int) -> Iterable[List[str]]:
        start = 0
        total = len(values)
        while start < total:
            yield list(values[start : start + chunk_size])
            start += chunk_size

    def _build_equity_cache_key(study_ids: Sequence[str]) -> Tuple[str, Tuple[str, ...]]:
        db_name = str(get_active_db_name() or "")
        normalized = tuple(sorted(study_ids))
        return db_name, normalized

    def _cache_get_equity(cache_key: Tuple[str, Tuple[str, ...]]) -> Optional[Dict[str, Any]]:
        cached = analytics_equity_cache.get(cache_key)
        if not cached:
            return None

        cached_at, payload = cached
        if (time.monotonic() - cached_at) > analytics_equity_cache_ttl_seconds:
            analytics_equity_cache.pop(cache_key, None)
            return None

        return dict(payload)

    def _cache_set_equity(cache_key: Tuple[str, Tuple[str, ...]], payload: Dict[str, Any]) -> None:
        if len(analytics_equity_cache) >= analytics_equity_cache_max_entries:
            oldest_key = min(analytics_equity_cache, key=lambda key: analytics_equity_cache[key][0])
            analytics_equity_cache.pop(oldest_key, None)
        analytics_equity_cache[cache_key] = (time.monotonic(), dict(payload))

    def _load_equity_rows(study_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        rows_by_id: Dict[str, Dict[str, Any]] = {}
        if not study_ids:
            return rows_by_id

        with get_db_connection() as conn:
            for chunk in _chunked(study_ids, analytics_equity_chunk_size):
                placeholders = ",".join("?" for _ in chunk)
                cursor = conn.execute(
                    f"""
                    SELECT
                        study_id,
                        stitched_oos_equity_curve,
                        stitched_oos_timestamps_json
                    FROM studies
                    WHERE study_id IN ({placeholders})
                      AND LOWER(COALESCE(optimization_mode, '')) = 'wfa'
                    """,
                    tuple(chunk),
                )
                for row in cursor.fetchall():
                    row_dict = dict(row)
                    rows_by_id[str(row_dict.get("study_id") or "")] = row_dict
        return rows_by_id

    def _compute_equity_for_study_ids(study_ids: Sequence[str]) -> Dict[str, Any]:
        cache_key = _build_equity_cache_key(study_ids)
        cached = _cache_get_equity(cache_key)
        if cached is not None:
            return cached

        rows_by_id = _load_equity_rows(study_ids)
        studies_data: List[Dict[str, Any]] = []
        missing_study_ids: List[str] = []

        for study_id in study_ids:
            row = rows_by_id.get(study_id)
            if not row:
                missing_study_ids.append(study_id)
                continue
            studies_data.append(
                {
                    "equity_curve": _parse_json_array(row.get("stitched_oos_equity_curve")),
                    "timestamps": _parse_json_array(row.get("stitched_oos_timestamps_json")),
                }
            )

        result = dict(aggregate_equity_curves(studies_data))
        if missing_study_ids:
            result["studies_excluded"] = int(result.get("studies_excluded") or 0) + len(missing_study_ids)
            warning = str(result.get("warning") or "").strip()
            missing_note = f"{len(missing_study_ids)} selected studies were not found."
            result["warning"] = f"{warning} {missing_note}".strip() if warning else missing_note

        result["selected_count"] = len(study_ids)
        result["missing_study_ids"] = missing_study_ids

        _cache_set_equity(cache_key, result)
        return dict(result)

    @app.get("/api/analytics/sets")
    def analytics_sets_list() -> object:
        return jsonify({"sets": list_study_sets()})

    @app.post("/api/analytics/sets")
    def analytics_sets_create() -> object:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _json_error("Expected JSON payload.", HTTPStatus.BAD_REQUEST)

        name = payload.get("name")
        study_ids_raw = payload.get("study_ids")
        try:
            study_ids = _parse_study_ids_payload(study_ids_raw)
            created = create_study_set(name, study_ids, color_token=payload.get("color_token"))
        except ValueError as exc:
            return _json_error(str(exc), HTTPStatus.BAD_REQUEST)

        return jsonify(created), HTTPStatus.CREATED

    @app.put("/api/analytics/sets/<int:set_id>")
    def analytics_sets_update(set_id: int) -> object:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _json_error("Expected JSON payload.", HTTPStatus.BAD_REQUEST)

        kwargs: Dict[str, Any] = {}
        if "name" in payload:
            kwargs["name"] = payload.get("name")
        if "study_ids" in payload:
            try:
                kwargs["study_ids"] = _parse_study_ids_payload(payload.get("study_ids"))
            except ValueError as exc:
                return _json_error(str(exc), HTTPStatus.BAD_REQUEST)
        if "sort_order" in payload:
            kwargs["sort_order"] = payload.get("sort_order")
        if "color_token" in payload:
            kwargs["color_token"] = payload.get("color_token")

        if not kwargs:
            return _json_error("No fields provided to update.", HTTPStatus.BAD_REQUEST)

        try:
            update_study_set(set_id=set_id, **kwargs)
        except ValueError as exc:
            message = str(exc)
            status = HTTPStatus.NOT_FOUND if "not found" in message.lower() else HTTPStatus.BAD_REQUEST
            return _json_error(message, status)

        return jsonify({"ok": True})

    @app.delete("/api/analytics/sets/<int:set_id>")
    def analytics_sets_delete(set_id: int) -> object:
        if not delete_study_set(set_id):
            return _json_error("Study set not found.", HTTPStatus.NOT_FOUND)
        return jsonify({"ok": True})

    @app.put("/api/analytics/sets/bulk-color")
    def analytics_sets_bulk_color() -> object:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _json_error("Expected JSON payload.", HTTPStatus.BAD_REQUEST)

        try:
            set_ids = _parse_set_ids_payload(payload.get("set_ids"))
            update_study_sets_color(set_ids, payload.get("color_token"))
        except ValueError as exc:
            message = str(exc)
            status = HTTPStatus.NOT_FOUND if "unknown study set ids" in message.lower() else HTTPStatus.BAD_REQUEST
            return _json_error(message, status)

        return jsonify({"ok": True})

    @app.post("/api/analytics/sets/bulk-delete")
    def analytics_sets_bulk_delete() -> object:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _json_error("Expected JSON payload.", HTTPStatus.BAD_REQUEST)

        try:
            set_ids = _parse_set_ids_payload(payload.get("set_ids"))
            deleted = delete_study_sets(set_ids)
        except ValueError as exc:
            message = str(exc)
            status = HTTPStatus.NOT_FOUND if "unknown study set ids" in message.lower() else HTTPStatus.BAD_REQUEST
            return _json_error(message, status)

        return jsonify({"ok": True, "deleted": deleted})

    @app.put("/api/analytics/sets/reorder")
    def analytics_sets_reorder() -> object:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _json_error("Expected JSON payload.", HTTPStatus.BAD_REQUEST)
        if "order" not in payload:
            return _json_error("Missing order array.", HTTPStatus.BAD_REQUEST)

        order = payload.get("order")
        try:
            reorder_study_sets(order)
        except ValueError as exc:
            return _json_error(str(exc), HTTPStatus.BAD_REQUEST)

        return jsonify({"ok": True})

    @app.route("/analytics")
    def analytics_page() -> object:
        return render_template("analytics.html")

    @app.post("/api/analytics/equity")
    def analytics_equity() -> object:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _json_error("Expected JSON payload.", HTTPStatus.BAD_REQUEST)

        try:
            study_ids = _parse_study_ids_payload(payload.get("study_ids"))
        except ValueError as exc:
            return _json_error(str(exc), HTTPStatus.BAD_REQUEST)

        if not study_ids:
            return _json_error(
                "study_ids is required and must be a non-empty array.",
                HTTPStatus.BAD_REQUEST,
            )
        if len(study_ids) > analytics_equity_max_study_ids:
            return _json_error(
                f"Too many study_ids. Maximum allowed is {analytics_equity_max_study_ids}.",
                HTTPStatus.BAD_REQUEST,
            )

        return jsonify(_compute_equity_for_study_ids(study_ids))

    @app.post("/api/analytics/equity/batch")
    def analytics_equity_batch() -> object:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _json_error("Expected JSON payload.", HTTPStatus.BAD_REQUEST)

        groups_payload = payload.get("groups")
        if not isinstance(groups_payload, list):
            return _json_error("groups must be an array.", HTTPStatus.BAD_REQUEST)
        if not groups_payload:
            return _json_error("groups must be a non-empty array.", HTTPStatus.BAD_REQUEST)

        group_specs: List[Tuple[str, List[str]]] = []
        for group in groups_payload:
            if not isinstance(group, dict):
                return _json_error("Each group must be an object.", HTTPStatus.BAD_REQUEST)
            group_id = str(group.get("group_id") or "").strip()
            if not group_id:
                return _json_error("Each group must include non-empty group_id.", HTTPStatus.BAD_REQUEST)
            try:
                study_ids = _parse_study_ids_payload(group.get("study_ids"))
            except ValueError as exc:
                return _json_error(str(exc), HTTPStatus.BAD_REQUEST)
            if len(study_ids) > analytics_equity_max_study_ids:
                return _json_error(
                    f"group '{group_id}' exceeds max study_ids ({analytics_equity_max_study_ids}).",
                    HTTPStatus.BAD_REQUEST,
                )
            group_specs.append((group_id, study_ids))

        results: List[Dict[str, Any]] = []
        for group_id, study_ids in group_specs:
            if not study_ids:
                group_result = aggregate_equity_curves([])
                group_result["selected_count"] = 0
                group_result["missing_study_ids"] = []
            else:
                group_result = _compute_equity_for_study_ids(study_ids)
            results.append(
                {
                    "group_id": group_id,
                    **group_result,
                }
            )

        return jsonify({"results": results})

    @app.get("/api/analytics/studies/<string:study_id>/window-boundaries")
    def analytics_study_window_boundaries(study_id: str) -> object:
        normalized_study_id = str(study_id or "").strip()
        if not normalized_study_id:
            return _json_error("study_id is required.", HTTPStatus.BAD_REQUEST)

        with get_db_connection() as conn:
            study_row = conn.execute(
                """
                SELECT study_id, optimization_mode
                FROM studies
                WHERE study_id = ?
                """,
                (normalized_study_id,),
            ).fetchone()

            if not study_row:
                return _json_error("Study not found.", HTTPStatus.NOT_FOUND)

            optimization_mode = str(study_row["optimization_mode"] or "").strip().lower()
            if optimization_mode != "wfa":
                return _json_error(
                    "Window boundaries are available only for WFA studies.",
                    HTTPStatus.BAD_REQUEST,
                )

            cursor = conn.execute(
                """
                SELECT
                    window_id,
                    window_number,
                    oos_start_ts,
                    oos_start_date,
                    is_end_ts,
                    is_end_date
                FROM wfa_windows
                WHERE study_id = ?
                ORDER BY window_number ASC
                """,
                (normalized_study_id,),
            )
            rows = cursor.fetchall()

        boundaries: List[Dict[str, Any]] = []
        for row in rows:
            row_dict = dict(row)
            boundary_time = (
                str(row_dict.get("oos_start_ts") or "").strip()
                or str(row_dict.get("oos_start_date") or "").strip()
                or str(row_dict.get("is_end_ts") or "").strip()
                or str(row_dict.get("is_end_date") or "").strip()
            )
            if not boundary_time:
                continue

            window_number = _safe_int(row_dict.get("window_number"))
            label = f"W{window_number}" if window_number is not None else f"W{len(boundaries) + 1}"
            boundaries.append(
                {
                    "window_id": row_dict.get("window_id"),
                    "window_number": window_number,
                    "time": boundary_time,
                    "label": label,
                }
            )

        return jsonify(
            {
                "study_id": normalized_study_id,
                "boundaries": boundaries,
            }
        )

    @app.get("/api/analytics/summary")
    def analytics_summary() -> object:
        with get_db_connection() as conn:
            total_studies_row = conn.execute("SELECT COUNT(*) AS count FROM studies").fetchone()
            total_studies = int(total_studies_row["count"] if total_studies_row else 0)

            cursor = conn.execute(
                """
                SELECT
                    study_id,
                    study_name,
                    strategy_id,
                    strategy_version,
                    created_at,
                    completed_at,
                    CAST(strftime('%s', created_at) AS INTEGER) AS created_at_epoch,
                    CAST(strftime('%s', completed_at) AS INTEGER) AS completed_at_epoch,
                    optimization_time_seconds,
                    csv_file_name,
                    adaptive_mode,
                    is_period_days,
                    max_oos_period_days,
                    min_oos_trades,
                    check_interval_trades,
                    cusum_threshold,
                    dd_threshold_multiplier,
                    inactivity_multiplier,
                    budget_mode,
                    n_trials,
                    time_limit,
                    convergence_patience,
                    sampler_type,
                    objectives_json,
                    primary_objective,
                    constraints_json,
                    score_config_json,
                    filter_min_profit,
                    min_profit_threshold,
                    sanitize_enabled,
                    sanitize_trades_threshold,
                    config_json,
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
                    stitched_oos_equity_curve,
                    stitched_oos_timestamps_json
                FROM studies
                WHERE LOWER(COALESCE(optimization_mode, '')) = 'wfa'
                """
            )
            rows = cursor.fetchall()

        studies: List[Dict[str, Any]] = []
        data_period_counts: Dict[Tuple[str, str], int] = {}
        strategy_values: set[str] = set()
        symbol_values: set[str] = set()
        timeframe_values: set[str] = set()
        wfa_mode_values: set[str] = set()
        is_oos_values: set[str] = set()

        for row in rows:
            row_dict = dict(row)
            config_payload = _parse_json_dict(row_dict.get("config_json"))
            optuna_config = _parse_json_dict(config_payload.get("optuna_config"))
            wfa_config = _parse_json_dict(config_payload.get("wfa"))
            config_objectives = _parse_json_array(config_payload.get("objectives"))
            config_constraints = _parse_json_array(config_payload.get("constraints"))
            row_objectives = _parse_json_array(row_dict.get("objectives_json"))
            row_constraints = _parse_json_array(row_dict.get("constraints_json"))

            strategy_label = _format_strategy_label(
                row_dict.get("strategy_id"),
                row_dict.get("strategy_version"),
            )
            strategy_values.add(strategy_label)

            symbol, tf = _parse_csv_filename(row_dict.get("csv_file_name"))
            if symbol:
                symbol_values.add(symbol)
            if tf:
                timeframe_values.add(tf)

            adaptive_mode_bool = _safe_bool(row_dict.get("adaptive_mode"))
            if adaptive_mode_bool is None:
                adaptive_mode_bool = _safe_bool(wfa_config.get("adaptive_mode"))
            if adaptive_mode_bool is None:
                adaptive_mode_bool = _safe_bool(config_payload.get("adaptive_mode"))

            wfa_mode = _format_wfa_mode_bool(adaptive_mode_bool)
            if wfa_mode == "Unknown":
                wfa_mode = _format_wfa_mode(row_dict.get("adaptive_mode"))
            wfa_mode_values.add(wfa_mode)

            is_period_days = _safe_int(row_dict.get("is_period_days"))
            oos_period_days = _safe_int(wfa_config.get("oos_period_days"))
            if oos_period_days is None:
                oos_period_days = _extract_oos_period_days(row_dict.get("config_json"))
            if is_period_days is None and oos_period_days is None:
                is_oos = "N/A"
            else:
                is_oos = (
                    f"{is_period_days if is_period_days is not None else '?'}"
                    f"/{oos_period_days if oos_period_days is not None else '?'}"
                )
            is_oos_values.add(is_oos)

            dataset_start = str(row_dict.get("dataset_start_date") or "")
            dataset_end = str(row_dict.get("dataset_end_date") or "")
            key = (dataset_start, dataset_end)
            data_period_counts[key] = data_period_counts.get(key, 0) + 1

            profit_pct = _safe_float(row_dict.get("stitched_oos_net_profit_pct"))
            if profit_pct is None:
                profit_pct = 0.0
            max_dd_pct = _safe_float(row_dict.get("stitched_oos_max_drawdown_pct"))
            if max_dd_pct is None:
                max_dd_pct = 0.0
            total_trades = _safe_int(row_dict.get("stitched_oos_total_trades"))
            if total_trades is None:
                total_trades = 0
            winning_trades = _safe_int(row_dict.get("stitched_oos_winning_trades"))

            profitable_windows = _safe_int(row_dict.get("profitable_windows"))
            if profitable_windows is None:
                profitable_windows = 0
            total_windows = _safe_int(row_dict.get("total_windows"))
            if total_windows is None:
                total_windows = 0
            stitched_win_rate = _safe_float(row_dict.get("stitched_oos_win_rate"))
            if total_windows > 0:
                profitable_windows_pct = (profitable_windows / total_windows) * 100.0
            elif stitched_win_rate is not None:
                profitable_windows_pct = stitched_win_rate
            else:
                profitable_windows_pct = 0.0

            equity_curve = _parse_json_array(row_dict.get("stitched_oos_equity_curve"))
            equity_timestamps = _parse_json_array(row_dict.get("stitched_oos_timestamps_json"))
            has_equity_curve = len(equity_curve) > 0 and len(equity_curve) == len(equity_timestamps)
            if not has_equity_curve:
                equity_curve = []
                equity_timestamps = []

            sampler_config = _parse_json_dict(optuna_config.get("sampler_config"))
            sampler_type = (
                sampler_config.get("sampler_type")
                or optuna_config.get("sampler_type")
                or optuna_config.get("sampler")
                or row_dict.get("sampler_type")
            )
            warmup_trials = _safe_int(optuna_config.get("warmup_trials"))
            if warmup_trials is None:
                warmup_trials = _safe_int(config_payload.get("n_startup_trials"))
            if warmup_trials is None:
                warmup_trials = _safe_int(sampler_config.get("n_startup_trials"))

            coverage_mode = _safe_bool(optuna_config.get("coverage_mode"))
            if coverage_mode is None:
                coverage_mode = _safe_bool(config_payload.get("coverage_mode"))
            dispatcher_batch_result_processing = _safe_bool(
                optuna_config.get("dispatcher_batch_result_processing")
            )
            if dispatcher_batch_result_processing is None:
                dispatcher_batch_result_processing = _safe_bool(
                    config_payload.get("dispatcher_batch_result_processing")
                )
            dispatcher_soft_duplicate_cycle_limit_enabled = _safe_bool(
                optuna_config.get("dispatcher_soft_duplicate_cycle_limit_enabled")
            )
            if dispatcher_soft_duplicate_cycle_limit_enabled is None:
                dispatcher_soft_duplicate_cycle_limit_enabled = _safe_bool(
                    config_payload.get("dispatcher_soft_duplicate_cycle_limit_enabled")
                )
            dispatcher_duplicate_cycle_limit = _safe_int(
                optuna_config.get("dispatcher_duplicate_cycle_limit")
            )
            if dispatcher_duplicate_cycle_limit is None:
                dispatcher_duplicate_cycle_limit = _safe_int(
                    config_payload.get("dispatcher_duplicate_cycle_limit")
                )
            workers_value = _safe_int(config_payload.get("worker_processes"))
            if workers_value is None:
                workers_value = _safe_int(config_payload.get("workerProcesses"))

            score_config = _parse_json_dict(config_payload.get("score_config"))
            if not score_config:
                score_config = _parse_json_dict(optuna_config.get("score_config"))
            if not score_config:
                score_config = _parse_json_dict(row_dict.get("score_config_json"))
            score_filter_enabled = _safe_bool(score_config.get("filter_enabled"))
            if score_filter_enabled is None:
                score_filter_enabled = False

            created_at_epoch = _safe_int(row_dict.get("created_at_epoch"))
            completed_at_epoch = _safe_int(row_dict.get("completed_at_epoch"))
            run_time_seconds = _safe_int(row_dict.get("optimization_time_seconds"))
            if run_time_seconds is None and created_at_epoch is not None and completed_at_epoch is not None:
                run_time_seconds = max(0, completed_at_epoch - created_at_epoch)

            studies.append(
                {
                    "study_id": row_dict.get("study_id"),
                    "study_name": row_dict.get("study_name"),
                    "strategy": strategy_label,
                    "strategy_id": row_dict.get("strategy_id"),
                    "strategy_version": row_dict.get("strategy_version"),
                    "created_at": row_dict.get("created_at"),
                    "completed_at": row_dict.get("completed_at"),
                    "created_at_epoch": created_at_epoch,
                    "completed_at_epoch": completed_at_epoch,
                    "symbol": symbol,
                    "tf": tf,
                    "wfa_mode": wfa_mode,
                    "is_oos": is_oos,
                    "dataset_start_date": dataset_start,
                    "dataset_end_date": dataset_end,
                    "profit_pct": profit_pct,
                    "max_dd_pct": max_dd_pct,
                    "total_trades": total_trades,
                    "winning_trades": winning_trades,
                    "wfe_pct": _safe_float(row_dict.get("best_value")),
                    "total_windows": total_windows,
                    "profitable_windows": profitable_windows,
                    "profitable_windows_pct": profitable_windows_pct,
                    "median_window_profit": _safe_float(row_dict.get("median_window_profit")),
                    "median_window_wr": _safe_float(row_dict.get("median_window_wr")),
                    "has_equity_curve": has_equity_curve,
                    "equity_curve": equity_curve,
                    "equity_timestamps": equity_timestamps,
                    "optuna_settings": {
                        "objectives": list(config_objectives or row_objectives or []),
                        "primary_objective": (
                            config_payload.get("primary_objective")
                            or row_dict.get("primary_objective")
                        ),
                        "constraints": list(config_constraints or row_constraints or []),
                        "budget_mode": (
                            optuna_config.get("budget_mode")
                            or row_dict.get("budget_mode")
                        ),
                        "n_trials": (
                            _safe_int(optuna_config.get("n_trials"))
                            if _safe_int(optuna_config.get("n_trials")) is not None
                            else _safe_int(row_dict.get("n_trials"))
                        ),
                        "time_limit": (
                            _safe_int(optuna_config.get("time_limit"))
                            if _safe_int(optuna_config.get("time_limit")) is not None
                            else _safe_int(row_dict.get("time_limit"))
                        ),
                        "convergence_patience": (
                            _safe_int(optuna_config.get("convergence_patience"))
                            if _safe_int(optuna_config.get("convergence_patience")) is not None
                            else _safe_int(row_dict.get("convergence_patience"))
                        ),
                        "sampler_type": sampler_type,
                        "enable_pruning": _safe_bool(optuna_config.get("enable_pruning")),
                        "pruner": optuna_config.get("pruner"),
                        "warmup_trials": warmup_trials,
                        "coverage_mode": coverage_mode,
                        "dispatcher_batch_result_processing": dispatcher_batch_result_processing,
                        "dispatcher_soft_duplicate_cycle_limit_enabled": dispatcher_soft_duplicate_cycle_limit_enabled,
                        "dispatcher_duplicate_cycle_limit": dispatcher_duplicate_cycle_limit,
                        "workers": workers_value,
                        "sanitize_enabled": (
                            _safe_bool(optuna_config.get("sanitize_enabled"))
                            if _safe_bool(optuna_config.get("sanitize_enabled")) is not None
                            else _safe_bool(row_dict.get("sanitize_enabled"))
                        ),
                        "sanitize_trades_threshold": (
                            _safe_int(optuna_config.get("sanitize_trades_threshold"))
                            if _safe_int(optuna_config.get("sanitize_trades_threshold")) is not None
                            else _safe_int(row_dict.get("sanitize_trades_threshold"))
                        ),
                        "filter_min_profit": (
                            _safe_bool(config_payload.get("filter_min_profit"))
                            if _safe_bool(config_payload.get("filter_min_profit")) is not None
                            else _safe_bool(row_dict.get("filter_min_profit"))
                        ),
                        "min_profit_threshold": (
                            _safe_float(config_payload.get("min_profit_threshold"))
                            if _safe_float(config_payload.get("min_profit_threshold")) is not None
                            else _safe_float(row_dict.get("min_profit_threshold"))
                        ),
                        "score_filter_enabled": score_filter_enabled,
                        "score_min_threshold": _safe_float(score_config.get("min_score_threshold")),
                    },
                    "wfa_settings": {
                        "is_period_days": (
                            is_period_days
                            if is_period_days is not None
                            else _safe_int(wfa_config.get("is_period_days"))
                        ),
                        "oos_period_days": oos_period_days,
                        "adaptive_mode": adaptive_mode_bool,
                        "max_oos_period_days": (
                            _safe_int(row_dict.get("max_oos_period_days"))
                            if _safe_int(row_dict.get("max_oos_period_days")) is not None
                            else _safe_int(wfa_config.get("max_oos_period_days"))
                        ),
                        "min_oos_trades": (
                            _safe_int(row_dict.get("min_oos_trades"))
                            if _safe_int(row_dict.get("min_oos_trades")) is not None
                            else _safe_int(wfa_config.get("min_oos_trades"))
                        ),
                        "check_interval_trades": (
                            _safe_int(row_dict.get("check_interval_trades"))
                            if _safe_int(row_dict.get("check_interval_trades")) is not None
                            else _safe_int(wfa_config.get("check_interval_trades"))
                        ),
                        "cusum_threshold": (
                            _safe_float(row_dict.get("cusum_threshold"))
                            if _safe_float(row_dict.get("cusum_threshold")) is not None
                            else _safe_float(wfa_config.get("cusum_threshold"))
                        ),
                        "dd_threshold_multiplier": (
                            _safe_float(row_dict.get("dd_threshold_multiplier"))
                            if _safe_float(row_dict.get("dd_threshold_multiplier")) is not None
                            else _safe_float(wfa_config.get("dd_threshold_multiplier"))
                        ),
                        "inactivity_multiplier": (
                            _safe_float(row_dict.get("inactivity_multiplier"))
                            if _safe_float(row_dict.get("inactivity_multiplier")) is not None
                            else _safe_float(wfa_config.get("inactivity_multiplier"))
                        ),
                        "run_time_seconds": run_time_seconds,
                    },
                }
            )

        studies.sort(
            key=lambda study: (
                _date_sort_key(study.get("dataset_start_date")),
                _date_sort_key(study.get("dataset_end_date")),
                -(_safe_float(study.get("profit_pct")) or 0.0),
                str(study.get("study_id") or ""),
            )
        )

        data_periods = []
        for (start, end), count in sorted(
            data_period_counts.items(),
            key=lambda item: (_date_sort_key(item[0][0]), _date_sort_key(item[0][1])),
        ):
            data_periods.append(
                {
                    "start": start,
                    "end": end,
                    "days": _period_days(start, end),
                    "count": count,
                }
            )

        wfa_mode_order = {"Fixed": 0, "Adaptive": 1, "Unknown": 2}
        wfa_modes = sorted(wfa_mode_values, key=lambda value: (wfa_mode_order.get(value, 99), value))

        timeframes = sorted(
            timeframe_values,
            key=lambda value: (_timeframe_to_minutes(value), str(value)),
        )

        research_info: Dict[str, Any] = {
            "total_studies": total_studies,
            "wfa_studies": len(studies),
            "strategies": sorted(strategy_values),
            "symbols": sorted(symbol_values),
            "timeframes": timeframes,
            "wfa_modes": wfa_modes,
            "is_oos_periods": sorted(is_oos_values),
            "data_periods": data_periods,
        }

        if len(studies) == 0:
            if total_studies == 0:
                research_info["message"] = "No WFA studies found in this database."
            else:
                research_info["message"] = (
                    "Analytics requires WFA studies. This database contains only Optuna studies."
                )

        return jsonify(
            {
                "db_name": get_active_db_name(),
                "studies": studies,
                "research_info": research_info,
            }
        )
