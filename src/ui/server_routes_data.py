import io
import re
import tempfile
import threading
import time
from datetime import timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from flask import jsonify, render_template, request, send_file

from core.backtest_engine import (
    align_date_bounds,
    load_data,
    parse_timestamp_utc,
    prepare_dataset_with_warmup,
)
from core.bundle_export import build_lancelot_partial_bundle
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
    create_new_db,
    delete_manual_test,
    delete_study,
    get_active_db_name,
    get_db_connection,
    get_study_trial,
    list_db_files,
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
    set_active_db,
    update_csv_path,
    update_study_config_json,
)

try:
    from .server_services import (
        DEFAULT_PRESET_NAME,
        DEFAULT_CSV_ROOT,
        _build_optimization_config,
        _build_trial_metrics,
        _clear_queue_state,
        _find_wfa_window,
        _get_optimization_state,
        _get_parameter_types,
        _json_safe,
        _load_queue_state,
        _list_csv_directory,
        _list_presets,
        _load_preset,
        _normalize_preset_payload,
        _parse_csv_parameter_block,
        _preset_path,
        _save_queue_state,
        _resolve_csv_path,
        _resolve_strategy_id_from_request,
        _resolve_wfa_period,
        _run_equity_export,
        _run_trade_export,
        _send_trades_csv,
        _set_optimization_state,
        _validate_csv_for_study,
        _validate_preset_name,
        _validate_strategy_params,
        _write_preset,
        validate_constraints_config,
        validate_objectives_config,
        validate_sampler_config,
    )
except ImportError:
    from server_services import (
        DEFAULT_PRESET_NAME,
        DEFAULT_CSV_ROOT,
        _build_optimization_config,
        _build_trial_metrics,
        _clear_queue_state,
        _find_wfa_window,
        _get_optimization_state,
        _get_parameter_types,
        _json_safe,
        _load_queue_state,
        _list_csv_directory,
        _list_presets,
        _load_preset,
        _normalize_preset_payload,
        _parse_csv_parameter_block,
        _preset_path,
        _save_queue_state,
        _resolve_csv_path,
        _resolve_strategy_id_from_request,
        _resolve_wfa_period,
        _run_equity_export,
        _run_trade_export,
        _send_trades_csv,
        _set_optimization_state,
        _validate_csv_for_study,
        _validate_preset_name,
        _validate_strategy_params,
        _write_preset,
        validate_constraints_config,
        validate_objectives_config,
        validate_sampler_config,
    )


def register_routes(app):
    def _trade_time_ns(value: Any) -> int:
        if value is None:
            return -1
        try:
            return int(value.value)
        except Exception:
            return -1

    def _normalize_wfa_oos_trades(
        trades: List[Any],
        *,
        start: Any,
        end: Any,
        expected_total: Optional[Any] = None,
    ) -> List[Any]:
        """Normalize adaptive OOS trades to match stored window semantics."""
        start_ts = parse_timestamp_utc(start)
        end_ts = parse_timestamp_utc(end)

        normalized: List[Any] = []
        for trade in list(trades or []):
            entry_time = getattr(trade, "entry_time", None)
            exit_time = getattr(trade, "exit_time", None)
            if entry_time is None or exit_time is None:
                continue
            if start_ts is not None and entry_time < start_ts:
                continue
            if end_ts is not None and entry_time > end_ts:
                continue
            if end_ts is not None and exit_time > end_ts:
                continue
            normalized.append(trade)

        normalized.sort(
            key=lambda trade: (
                _trade_time_ns(getattr(trade, "exit_time", None)),
                _trade_time_ns(getattr(trade, "entry_time", None)),
            )
        )

        try:
            limit = max(0, int(expected_total)) if expected_total is not None else None
        except (TypeError, ValueError):
            limit = None

        if limit is not None and len(normalized) > limit:
            normalized = normalized[:limit]

        return normalized

    def _prepend_wfa_flat_equity_prefix(
        equity_curve: List[float],
        timestamps: List[str],
        *,
        scheduled_start: Any,
        live_start: Any,
    ) -> Tuple[List[float], List[str]]:
        scheduled_start_ts = parse_timestamp_utc(scheduled_start)
        live_start_ts = parse_timestamp_utc(live_start)
        if scheduled_start_ts is None or live_start_ts is None or live_start_ts <= scheduled_start_ts:
            return list(equity_curve or []), list(timestamps or [])

        prefixed_curve: List[float] = [100.0]
        prefixed_timestamps: List[str] = [scheduled_start_ts.isoformat()]

        first_live_ts = parse_timestamp_utc(timestamps[0]) if timestamps else None
        if first_live_ts is None or first_live_ts > live_start_ts:
            prefixed_curve.append(100.0)
            prefixed_timestamps.append(live_start_ts.isoformat())

        prefixed_curve.extend(list(equity_curve or []))
        prefixed_timestamps.extend(list(timestamps or []))

        deduped_curve: List[float] = []
        deduped_timestamps: List[str] = []
        for curve_value, timestamp in zip(prefixed_curve, prefixed_timestamps):
            if deduped_timestamps and timestamp == deduped_timestamps[-1]:
                deduped_curve[-1] = curve_value
            else:
                deduped_curve.append(curve_value)
                deduped_timestamps.append(timestamp)

        return deduped_curve, deduped_timestamps

    def _resolve_csv_path_for_response(
        raw_path: Any,
        *,
        missing_error: str = "CSV file is required.",
        not_found_error: str = "CSV file not found.",
    ) -> Tuple[Optional[str], Optional[Tuple[object, HTTPStatus]]]:
        raw_value = str(raw_path or "").strip()
        if not raw_value:
            return None, (jsonify({"error": missing_error}), HTTPStatus.BAD_REQUEST)
        try:
            resolved = _resolve_csv_path(raw_value)
        except FileNotFoundError:
            return None, (jsonify({"error": not_found_error}), HTTPStatus.BAD_REQUEST)
        except IsADirectoryError:
            return None, (jsonify({"error": "CSV path must point to a file."}), HTTPStatus.BAD_REQUEST)
        except PermissionError as exc:
            return None, (jsonify({"error": str(exc)}), HTTPStatus.FORBIDDEN)
        except ValueError as exc:
            message = str(exc).strip() or missing_error
            return None, (jsonify({"error": message}), HTTPStatus.BAD_REQUEST)
        except OSError:
            return None, (jsonify({"error": "Failed to access CSV file."}), HTTPStatus.BAD_REQUEST)
        return str(resolved), None

    @app.route("/")
    def index() -> object:
        return render_template("index.html")



    @app.route("/results")
    def results_page() -> object:
        return render_template("results.html")

    @app.get("/api/csv/browse")
    def browse_csv_directory() -> object:
        raw_path = (request.args.get("path") or "").strip()
        target_path = raw_path or DEFAULT_CSV_ROOT
        try:
            payload = _list_csv_directory(target_path)
        except FileNotFoundError:
            return jsonify({"error": f"Directory not found: {target_path}"}), HTTPStatus.BAD_REQUEST
        except NotADirectoryError:
            return jsonify({"error": f"Path is not a directory: {target_path}"}), HTTPStatus.BAD_REQUEST
        except PermissionError:
            return jsonify({"error": "Directory is outside allowed roots."}), HTTPStatus.FORBIDDEN
        except ValueError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST
        except OSError:
            return jsonify({"error": "Failed to access CSV directory."}), HTTPStatus.BAD_REQUEST
        return jsonify(payload)


    @app.get("/api/databases")
    def list_databases() -> object:
        return jsonify({
            "databases": list_db_files(),
            "active": get_active_db_name(),
        })


    @app.post("/api/databases/active")
    def switch_database() -> object:
        state = _get_optimization_state()
        if state.get("status") == "running":
            return (
                jsonify({"error": "Cannot switch database while optimization is running."}),
                HTTPStatus.CONFLICT,
            )

        body = request.get_json(silent=True) or {}
        filename = (body.get("filename") or "").strip()
        if not filename:
            return jsonify({"error": "Missing filename."}), HTTPStatus.BAD_REQUEST
        if not filename.endswith(".db"):
            return jsonify({"error": "Invalid filename."}), HTTPStatus.BAD_REQUEST

        try:
            set_active_db(filename)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST

        return jsonify({"active": get_active_db_name()})


    @app.post("/api/databases")
    def create_database() -> object:
        state = _get_optimization_state()
        if state.get("status") == "running":
            return (
                jsonify({"error": "Cannot create database while optimization is running."}),
                HTTPStatus.CONFLICT,
            )

        body = request.get_json(silent=True) or {}
        label = (body.get("label") or "").strip()

        try:
            filename = create_new_db(label)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST

        return jsonify({"filename": filename, "active": filename})


    @app.get("/api/queue")
    def get_queue_state_endpoint() -> object:
        try:
            payload = _load_queue_state()
        except OSError:
            return jsonify({"error": "Failed to load queue state."}), HTTPStatus.INTERNAL_SERVER_ERROR
        return jsonify(payload)


    @app.put("/api/queue")
    def save_queue_state_endpoint() -> object:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "Queue payload must be a JSON object."}), HTTPStatus.BAD_REQUEST

        try:
            normalized = _save_queue_state(payload)
        except OSError:
            return jsonify({"error": "Failed to save queue state."}), HTTPStatus.INTERNAL_SERVER_ERROR
        return jsonify(normalized)


    @app.delete("/api/queue")
    def clear_queue_state_endpoint() -> object:
        try:
            normalized = _clear_queue_state()
        except OSError:
            return jsonify({"error": "Failed to clear queue state."}), HTTPStatus.INTERNAL_SERVER_ERROR
        return jsonify(normalized)



    @app.get("/api/studies")
    def list_studies_endpoint() -> object:
        return jsonify({"studies": list_studies()})



    @app.get("/api/studies/<string:study_id>")
    def get_study_endpoint(study_id: str) -> object:
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND
        return jsonify(_json_safe(study_data))



    @app.delete("/api/studies/<string:study_id>")
    def delete_study_endpoint(study_id: str) -> object:
        deleted = delete_study(study_id)
        if not deleted:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND
        return ("", HTTPStatus.NO_CONTENT)



    @app.post("/api/studies/<string:study_id>/update-csv-path")
    def update_study_csv_path_endpoint(study_id: str) -> object:
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        csv_path_raw = None
        if request.is_json:
            payload = request.get_json(silent=True) or {}
            if isinstance(payload, dict):
                csv_path_raw = payload.get("csvPath") or payload.get("csv_file_path")
        if csv_path_raw is None:
            csv_path_raw = request.form.get("csvPath")

        new_path, error_response = _resolve_csv_path_for_response(
            csv_path_raw,
            missing_error="csvPath is required.",
            not_found_error="CSV file not found.",
        )
        if error_response:
            return error_response

        is_valid, warnings, error = _validate_csv_for_study(new_path, study_data["study"])
        if not is_valid:
            return jsonify({"error": error or "CSV validation failed."}), HTTPStatus.BAD_REQUEST

        updated = update_csv_path(study_id, new_path)
        if not updated:
            return jsonify({"error": "Failed to update CSV path."}), HTTPStatus.INTERNAL_SERVER_ERROR

        return jsonify({"status": "updated", "warnings": warnings, "csv_file_path": new_path})


    @app.post("/api/studies/<string:study_id>/test")
    def run_manual_test_endpoint(study_id: str) -> object:
        payload = request.get_json(silent=True) if request.is_json else None
        if not isinstance(payload, dict):
            return jsonify({"error": "Invalid manual test payload."}), HTTPStatus.BAD_REQUEST

        data_source = payload.get("dataSource")
        csv_path = payload.get("csvPath")
        start_date = payload.get("startDate")
        end_date = payload.get("endDate")
        trial_numbers = payload.get("trialNumbers") or []
        source_tab = payload.get("sourceTab")
        test_name = payload.get("testName")

        if data_source not in {"original_csv", "new_csv"}:
            return jsonify({"error": "dataSource must be 'original_csv' or 'new_csv'."}), HTTPStatus.BAD_REQUEST
        if source_tab not in {"optuna", "forward_test", "dsr", "stress_test"}:
            return (
                jsonify({"error": "sourceTab must be 'optuna', 'forward_test', 'dsr', or 'stress_test'."}),
                HTTPStatus.BAD_REQUEST,
            )
        if not start_date or not end_date:
            return jsonify({"error": "startDate and endDate are required."}), HTTPStatus.BAD_REQUEST
        if not isinstance(trial_numbers, list) or not trial_numbers:
            return jsonify({"error": "trialNumbers must be a non-empty array."}), HTTPStatus.BAD_REQUEST

        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND
        study = study_data["study"]
        if study.get("optimization_mode") != "optuna":
            return jsonify({"error": "Manual tests are supported only for Optuna studies."}), HTTPStatus.BAD_REQUEST

        if data_source == "original_csv":
            csv_path = study.get("csv_file_path")
            if not csv_path:
                return jsonify({"error": "Original CSV path is missing."}), HTTPStatus.BAD_REQUEST
        elif not csv_path:
            return jsonify({"error": "csvPath is required when dataSource is 'new_csv'."}), HTTPStatus.BAD_REQUEST

        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file not found.",
            not_found_error="CSV file not found.",
        )
        if error_response:
            return error_response

        start_ts = parse_timestamp_utc(start_date)
        end_ts = parse_timestamp_utc(end_date)
        if start_ts is None or end_ts is None:
            return jsonify({"error": "Invalid startDate/endDate."}), HTTPStatus.BAD_REQUEST

        test_period_days = max(0, (end_ts - start_ts).days)
        if test_period_days <= 0:
            return jsonify({"error": "Test period must be at least 1 day."}), HTTPStatus.BAD_REQUEST

        config = study.get("config_json") or {}
        fixed_params = config.get("fixed_params") or {}
        warmup_bars = study.get("warmup_bars") or config.get("warmup_bars") or 1000

        trials = study_data.get("trials") or []
        trial_map = {int(t.get("trial_number")): t for t in trials}
        missing = [n for n in trial_numbers if int(n) not in trial_map]
        if missing:
            return jsonify({"error": f"Trials not found: {', '.join(map(str, missing))}."}), HTTPStatus.BAD_REQUEST

        baseline_rank_map: Dict[int, int] = {}
        if source_tab == "optuna":
            for idx, trial in enumerate(trials, 1):
                number = int(trial.get("trial_number") or 0)
                if number:
                    baseline_rank_map[number] = idx
        elif source_tab == "forward_test":
            for trial in trials:
                number = int(trial.get("trial_number") or 0)
                rank = trial.get("ft_rank")
                if number and rank:
                    baseline_rank_map[number] = int(rank)
        elif source_tab == "dsr":
            for trial in trials:
                number = int(trial.get("trial_number") or 0)
                rank = trial.get("dsr_rank")
                if number and rank:
                    baseline_rank_map[number] = int(rank)
        else:
            for trial in trials:
                number = int(trial.get("trial_number") or 0)
                rank = trial.get("st_rank")
                if number and rank:
                    baseline_rank_map[number] = int(rank)

        if not baseline_rank_map:
            for idx, trial in enumerate(trials, 1):
                number = int(trial.get("trial_number") or 0)
                if number:
                    baseline_rank_map[number] = idx

        try:
            df = load_data(csv_path)
        except Exception as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST

        aligned_start, aligned_end = align_date_bounds(df.index, start_date, end_date)
        if aligned_start is None or aligned_end is None:
            return jsonify({"error": "Invalid startDate/endDate."}), HTTPStatus.BAD_REQUEST
        start_ts = aligned_start
        end_ts = aligned_end
        test_period_days = max(0, (end_ts - start_ts).days)
        if test_period_days <= 0:
            return jsonify({"error": "Test period must be at least 1 day."}), HTTPStatus.BAD_REQUEST

        baseline_period_days = None
        if source_tab == "forward_test":
            baseline_period_days = study.get("ft_period_days")
        if baseline_period_days is None:
            baseline_period_days = study.get("is_period_days")
        if baseline_period_days is None:
            baseline_period_days = calculate_is_period_days(config) or 0

        trials_to_test = [trial_map[int(number)] for number in trial_numbers]

        def resolve_original_metrics(trial: Dict[str, Any]) -> Dict[str, Any]:
            if source_tab == "forward_test":
                return _build_trial_metrics(trial, prefix="ft_")
            return _build_trial_metrics(trial)

        try:
            results_payload = run_period_test_for_trials(
                df=df,
                strategy_id=study.get("strategy_id"),
                warmup_bars=int(warmup_bars),
                fixed_params=fixed_params,
                start_ts=start_ts,
                end_ts=end_ts,
                trials=trials_to_test,
                baseline_period_days=int(baseline_period_days or 0),
                test_period_days=int(test_period_days),
                original_metrics_resolver=resolve_original_metrics,
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST

        for idx, item in enumerate(results_payload, 1):
            trial_number = item.get("trial_number")
            baseline_rank = baseline_rank_map.get(trial_number)
            if baseline_rank is not None:
                item["comparison"]["rank_change"] = baseline_rank - idx
            else:
                item["comparison"]["rank_change"] = None

        degradations = [item["comparison"].get("profit_degradation", 0.0) for item in results_payload]
        best_deg = max(degradations) if degradations else None
        worst_deg = min(degradations) if degradations else None

        results_json = {
            "config": {
                "data_source": data_source,
                "csv_path": csv_path,
                "start_date": start_date,
                "end_date": end_date,
                "period_days": int(test_period_days),
            },
            "results": results_payload,
        }

        trials_tested_csv = ",".join(str(int(n)) for n in trial_numbers)
        test_id = save_manual_test_to_db(
            study_id=study_id,
            test_name=test_name,
            data_source=data_source,
            csv_path=csv_path,
            start_date=start_date,
            end_date=end_date,
            source_tab=source_tab,
            trials_count=len(results_payload),
            trials_tested_csv=trials_tested_csv,
            best_profit_degradation=best_deg,
            worst_profit_degradation=worst_deg,
            results_json=results_json,
        )

        return jsonify(
            {
                "status": "success",
                "test_id": test_id,
                "summary": {
                    "trials_count": len(results_payload),
                    "best_profit_degradation": best_deg,
                    "worst_profit_degradation": worst_deg,
                },
            }
        )



    @app.get("/api/studies/<string:study_id>/tests")
    def list_manual_tests_endpoint(study_id: str) -> object:
        if not study_id:
            return jsonify({"error": "Study ID is required."}), HTTPStatus.BAD_REQUEST
        return jsonify({"tests": list_manual_tests(study_id)})



    @app.get("/api/studies/<string:study_id>/tests/<int:test_id>")
    def get_manual_test_results_endpoint(study_id: str, test_id: int) -> object:
        result = load_manual_test_results(study_id, test_id)
        if not result:
            return jsonify({"error": "Manual test not found."}), HTTPStatus.NOT_FOUND
        return jsonify(result)



    @app.delete("/api/studies/<string:study_id>/tests/<int:test_id>")
    def delete_manual_test_endpoint(study_id: str, test_id: int) -> object:
        deleted = delete_manual_test(study_id, test_id)
        if not deleted:
            return jsonify({"error": "Manual test not found."}), HTTPStatus.NOT_FOUND
        return ("", HTTPStatus.NO_CONTENT)



    @app.post("/api/studies/<string:study_id>/trials/<int:trial_number>/trades")
    def download_trial_trades(study_id: str, trial_number: int) -> object:
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        if study.get("optimization_mode") != "optuna":
            return jsonify({"error": "Trade export is only supported for Optuna studies."}), HTTPStatus.BAD_REQUEST

        csv_path = study.get("csv_file_path")
        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file is missing for this study.",
            not_found_error="CSV file is missing for this study.",
        )
        if error_response:
            return error_response

        trial = get_study_trial(study_id, trial_number)
        if not trial:
            return jsonify({"error": "Trial not found."}), HTTPStatus.NOT_FOUND

        config = study.get("config_json") or {}
        fixed_params = config.get("fixed_params") or {}

        params = {**fixed_params, **(trial.get("params") or {})}
        warmup_bars = study.get("warmup_bars") or config.get("warmup_bars") or 1000

        trades, error = _run_trade_export(
            strategy_id=study.get("strategy_id"),
            csv_path=csv_path,
            params=params,
            warmup_bars=warmup_bars,
        )
        if error:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        filename = f"{study.get('study_name', 'study')}_trial_{trial_number}_trades.csv"
        return _send_trades_csv(
            trades=trades or [],
            csv_path=csv_path,
            study=study,
            filename=filename,
        )



    @app.post("/api/studies/<string:study_id>/trials/<int:trial_number>/ft-trades")
    def download_forward_test_trades(study_id: str, trial_number: int) -> object:
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        if study.get("optimization_mode") != "optuna":
            return jsonify({"error": "Trade export is only supported for Optuna studies."}), HTTPStatus.BAD_REQUEST
        if not study.get("ft_enabled"):
            return jsonify({"error": "Forward test is not enabled for this study."}), HTTPStatus.BAD_REQUEST

        csv_path = study.get("csv_file_path")
        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file is missing for this study.",
            not_found_error="CSV file is missing for this study.",
        )
        if error_response:
            return error_response

        trial = get_study_trial(study_id, trial_number)
        if not trial:
            return jsonify({"error": "Trial not found."}), HTTPStatus.NOT_FOUND

        ft_start = study.get("ft_start_date")
        ft_end = study.get("ft_end_date")
        if not ft_start or not ft_end:
            return jsonify({"error": "Forward test date range is missing."}), HTTPStatus.BAD_REQUEST

        config = study.get("config_json") or {}
        fixed_params = config.get("fixed_params") or {}
        params = {**fixed_params, **(trial.get("params") or {})}
        params["dateFilter"] = True
        params["start"] = ft_start
        params["end"] = ft_end

        warmup_bars = study.get("warmup_bars") or config.get("warmup_bars") or 1000

        trades, error = _run_trade_export(
            strategy_id=study.get("strategy_id"),
            csv_path=csv_path,
            params=params,
            warmup_bars=warmup_bars,
        )
        if error:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        filename = f"{study.get('study_name', 'study')}_trial_{trial_number}_ft_trades.csv"
        return _send_trades_csv(
            trades=trades or [],
            csv_path=csv_path,
            study=study,
            filename=filename,
        )



    @app.post("/api/studies/<string:study_id>/trials/<int:trial_number>/oos-trades")
    def download_oos_test_trades(study_id: str, trial_number: int) -> object:
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        if study.get("optimization_mode") != "optuna":
            return jsonify({"error": "Trade export is only supported for Optuna studies."}), HTTPStatus.BAD_REQUEST
        if not study.get("oos_test_enabled"):
            return jsonify({"error": "OOS Test is not enabled for this study."}), HTTPStatus.BAD_REQUEST

        csv_path = study.get("csv_file_path")
        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file is missing for this study.",
            not_found_error="CSV file is missing for this study.",
        )
        if error_response:
            return error_response

        trial = get_study_trial(study_id, trial_number)
        if not trial:
            return jsonify({"error": "Trial not found."}), HTTPStatus.NOT_FOUND

        oos_start = study.get("oos_test_start_date")
        oos_end = study.get("oos_test_end_date")
        if not oos_start or not oos_end:
            return jsonify({"error": "OOS Test date range is missing."}), HTTPStatus.BAD_REQUEST

        config = study.get("config_json") or {}
        fixed_params = config.get("fixed_params") or {}
        params = {**fixed_params, **(trial.get("params") or {})}
        params["dateFilter"] = True
        params["start"] = oos_start
        params["end"] = oos_end

        warmup_bars = study.get("warmup_bars") or config.get("warmup_bars") or 1000

        trades, error = _run_trade_export(
            strategy_id=study.get("strategy_id"),
            csv_path=csv_path,
            params=params,
            warmup_bars=warmup_bars,
        )
        if error:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        filename = f"{study.get('study_name', 'study')}_trial_{trial_number}_oos_trades.csv"
        return _send_trades_csv(
            trades=trades or [],
            csv_path=csv_path,
            study=study,
            filename=filename,
        )



    @app.post("/api/studies/<string:study_id>/tests/<int:test_id>/trials/<int:trial_number>/mt-trades")
    def download_manual_test_trades(study_id: str, test_id: int, trial_number: int) -> object:
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        if study.get("optimization_mode") != "optuna":
            return jsonify({"error": "Manual trade export is only supported for Optuna studies."}), HTTPStatus.BAD_REQUEST

        test = load_manual_test_results(study_id, test_id)
        if not test:
            return jsonify({"error": "Manual test not found."}), HTTPStatus.NOT_FOUND

        csv_path = test.get("csv_path")
        if not csv_path and test.get("data_source") == "original_csv":
            csv_path = study.get("csv_file_path")
        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file is missing for this manual test.",
            not_found_error="CSV file is missing for this manual test.",
        )
        if error_response:
            return error_response

        trial = get_study_trial(study_id, trial_number)
        if not trial:
            return jsonify({"error": "Trial not found."}), HTTPStatus.NOT_FOUND
        trials_tested_csv = test.get("trials_tested_csv") or ""
        if trials_tested_csv:
            try:
                tested = {int(item.strip()) for item in trials_tested_csv.split(",") if item.strip()}
            except ValueError:
                tested = set()
            if tested and int(trial_number) not in tested:
                return jsonify({"error": "Trial not included in this manual test."}), HTTPStatus.BAD_REQUEST

        start_date = test.get("start_date")
        end_date = test.get("end_date")
        if not start_date or not end_date:
            return jsonify({"error": "Manual test date range is missing."}), HTTPStatus.BAD_REQUEST

        config = study.get("config_json") or {}
        fixed_params = config.get("fixed_params") or {}
        params = {**fixed_params, **(trial.get("params") or {})}
        params["dateFilter"] = True
        params["start"] = start_date
        params["end"] = end_date

        warmup_bars = study.get("warmup_bars") or config.get("warmup_bars") or 1000

        trades, error = _run_trade_export(
            strategy_id=study.get("strategy_id"),
            csv_path=csv_path,
            params=params,
            warmup_bars=warmup_bars,
        )
        if error:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        filename = f"{study.get('study_name', 'study')}_test_{test_id}_trial_{trial_number}_mt_trades.csv"
        return _send_trades_csv(
            trades=trades or [],
            csv_path=csv_path,
            study=study,
            filename=filename,
        )



    @app.post("/api/studies/<string:study_id>/export/lancelot")
    def export_lancelot_bundle_endpoint(study_id: str) -> object:
        if not request.is_json:
            return jsonify({"error": "Expected JSON payload."}), HTTPStatus.BAD_REQUEST

        payload = request.get_json(silent=True) or {}
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        csv_path = study.get("csv_file_path")
        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file is missing for this study.",
            not_found_error="CSV file is missing for this study.",
        )
        if error_response:
            return error_response

        params: Dict[str, Any] = {}
        source_trial_number = 0
        mode = str(study.get("optimization_mode") or "").lower()

        if mode == "wfa":
            raw_window_number = payload.get("windowNumber")
            if raw_window_number in (None, ""):
                return jsonify({"error": "windowNumber is required for WFA bundle export."}), HTTPStatus.BAD_REQUEST
            try:
                window_number = int(raw_window_number)
            except (TypeError, ValueError):
                return jsonify({"error": "windowNumber must be an integer."}), HTTPStatus.BAD_REQUEST

            window = _find_wfa_window(study_data, window_number)
            if not window:
                return jsonify({"error": "WFA window not found."}), HTTPStatus.NOT_FOUND

            params = dict(window.get("best_params") or {})
            source_trial_number = int(window.get("is_best_trial_number") or 0)

            if source_trial_number <= 0:
                window_id = window.get("window_id") or f"{study_id}_w{window_number}"
                modules = load_wfa_window_trials(window_id)
                preferred_module = str(window.get("best_params_source") or "optuna_is")
                module_trials = modules.get(preferred_module) or []
                selected_trial = next((trial for trial in module_trials if trial.get("is_selected")), None)
                if selected_trial is None and module_trials:
                    selected_trial = module_trials[0]
                if selected_trial:
                    source_trial_number = int(selected_trial.get("trial_number") or 0)
        elif mode == "optuna":
            raw_trial_number = payload.get("trialNumber")
            if raw_trial_number in (None, ""):
                return jsonify({"error": "trialNumber is required for bundle export."}), HTTPStatus.BAD_REQUEST
            try:
                source_trial_number = int(raw_trial_number)
            except (TypeError, ValueError):
                return jsonify({"error": "trialNumber must be an integer."}), HTTPStatus.BAD_REQUEST

            trial = get_study_trial(study_id, source_trial_number)
            if not trial:
                return jsonify({"error": "Trial not found."}), HTTPStatus.NOT_FOUND
            params = dict(trial.get("params") or {})
        else:
            return jsonify({"error": "Bundle export is only supported for Optuna and WFA studies."}), HTTPStatus.BAD_REQUEST

        try:
            bundle = build_lancelot_partial_bundle(
                study=study,
                params=params,
                trial_number=source_trial_number,
                csv_path=csv_path,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST
        except OSError:
            return jsonify({"error": "Failed to read CSV file for bundle export."}), HTTPStatus.BAD_REQUEST

        return jsonify(_json_safe(bundle))



    @app.get("/api/studies/<string:study_id>/wfa/windows/<int:window_number>")
    def get_wfa_window_details(study_id: str, window_number: int) -> object:
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        if study.get("optimization_mode") != "wfa":
            return jsonify({"error": "Window details only available for WFA studies."}), HTTPStatus.BAD_REQUEST

        window = _find_wfa_window(study_data, window_number)
        if not window:
            return jsonify({"error": "WFA window not found."}), HTTPStatus.NOT_FOUND

        window_id = window.get("window_id") or f"{study_id}_w{window_number}"
        modules = load_wfa_window_trials(window_id)

        available_modules = window.get("available_modules")
        if not isinstance(available_modules, list):
            available_modules = list(modules.keys()) if modules else ["optuna_is"]

        window_payload = {
            "window_number": window.get("window_number"),
            "window_id": window_id,
            "is_start_date": window.get("is_start_date"),
            "is_end_date": window.get("is_end_date"),
            "oos_start_date": window.get("oos_start_date"),
            "oos_end_date": window.get("oos_end_date"),
            "optimization_start_date": window.get("optimization_start_date"),
            "optimization_end_date": window.get("optimization_end_date"),
            "ft_start_date": window.get("ft_start_date"),
            "ft_end_date": window.get("ft_end_date"),
            "best_params": window.get("best_params") or {},
            "param_id": window.get("param_id"),
            "best_params_source": window.get("best_params_source") or "optuna_is",
            "is_pareto_optimal": window.get("is_pareto_optimal"),
            "constraints_satisfied": window.get("constraints_satisfied"),
            "available_modules": available_modules,
            "module_status": window.get("module_status") or {},
            "selection_chain": window.get("selection_chain") or {},
            "is_metrics": _build_trial_metrics(window, "is_"),
            "oos_metrics": _build_trial_metrics(window, "oos_"),
            "trigger_type": window.get("trigger_type"),
            "cusum_final": window.get("cusum_final"),
            "cusum_threshold": window.get("cusum_threshold"),
            "dd_threshold": window.get("dd_threshold"),
            "oos_actual_days": window.get("oos_actual_days"),
            "cooldown_days_applied": window.get("cooldown_days_applied"),
            "oos_elapsed_days": window.get("oos_elapsed_days"),
            "trade_start_date": window.get("trade_start_date"),
            "trade_end_date": window.get("trade_end_date"),
            "entry_delay_days": window.get("entry_delay_days"),
            "ft_retry_attempts_used": window.get("ft_retry_attempts_used"),
            "remaining_oos_days_at_entry": window.get("remaining_oos_days_at_entry"),
            "window_status": window.get("window_status"),
            "no_trade_reason": window.get("no_trade_reason"),
        }

        return jsonify({"window": _json_safe(window_payload), "modules": _json_safe(modules)})



    @app.post("/api/studies/<string:study_id>/wfa/windows/<int:window_number>/equity")
    def generate_wfa_window_equity(study_id: str, window_number: int) -> object:
        if not request.is_json:
            return jsonify({"error": "Expected JSON payload."}), HTTPStatus.BAD_REQUEST
        payload = request.get_json(silent=True) or {}

        module_type = payload.get("moduleType")
        trial_number = payload.get("trialNumber")
        period = payload.get("period") or "is"

        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        if study.get("optimization_mode") != "wfa":
            return jsonify({"error": "Equity export is only supported for WFA studies."}), HTTPStatus.BAD_REQUEST

        window = _find_wfa_window(study_data, window_number)
        if not window:
            return jsonify({"error": "WFA window not found."}), HTTPStatus.NOT_FOUND

        window_id = window.get("window_id") or f"{study_id}_w{window_number}"
        fixed_params = (study.get("config_json") or {}).get("fixed_params") or {}
        params = window.get("best_params") or {}

        if module_type and module_type != "oos_result":
            modules = load_wfa_window_trials(window_id)
            module_trials = modules.get(module_type) or []
            if trial_number is None:
                return jsonify({"error": "trialNumber is required for module equity."}), HTTPStatus.BAD_REQUEST
            match = next(
                (trial for trial in module_trials if int(trial.get("trial_number") or -1) == int(trial_number)),
                None,
            )
            if not match:
                return jsonify({"error": "Trial not found for module."}), HTTPStatus.NOT_FOUND
            params = match.get("params") or {}

        start, end, error = _resolve_wfa_period(window, period)
        if error:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        scheduled_start = start
        scheduled_end = end
        actual_start = start
        actual_end = end
        is_window_oos = (period or "").lower() == "oos" and (not module_type or module_type == "oos_result")
        if is_window_oos:
            actual_start = (
                window.get("trade_start_ts")
                or window.get("trade_start_date")
                or actual_start
            )
            actual_end = (
                window.get("trade_end_ts")
                or window.get("trade_end_date")
                or actual_end
            )
            if str(window.get("window_status") or "").lower() == "no_trade":
                start_ts = parse_timestamp_utc(scheduled_start)
                end_ts = parse_timestamp_utc(scheduled_end)
                timestamps = []
                equity_curve = []
                if start_ts is not None:
                    timestamps.append(start_ts.isoformat())
                    equity_curve.append(100.0)
                if end_ts is not None and start_ts is not None and end_ts > start_ts:
                    timestamps.append(end_ts.isoformat())
                    equity_curve.append(100.0)
                return jsonify({"equity_curve": equity_curve, "timestamps": timestamps})

        csv_path = study.get("csv_file_path")
        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file is missing for this study.",
            not_found_error="CSV file is missing for this study.",
        )
        if error_response:
            return error_response

        warmup_bars = study.get("warmup_bars") or (study.get("config_json") or {}).get("warmup_bars") or 1000

        merged_params = {**fixed_params, **params}
        merged_params["dateFilter"] = True
        merged_params["start"] = actual_start
        merged_params["end"] = actual_end

        equity_curve, timestamps, error = _run_equity_export(
            strategy_id=study.get("strategy_id"),
            csv_path=csv_path,
            params=merged_params,
            warmup_bars=int(warmup_bars),
        )
        if error:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        if is_window_oos:
            equity_curve, timestamps = _prepend_wfa_flat_equity_prefix(
                equity_curve or [],
                timestamps or [],
                scheduled_start=scheduled_start,
                live_start=actual_start,
            )

        return jsonify({"equity_curve": equity_curve or [], "timestamps": timestamps or []})



    @app.post("/api/studies/<string:study_id>/wfa/windows/<int:window_number>/trades")
    def download_wfa_window_trades(study_id: str, window_number: int) -> object:
        if not request.is_json:
            return jsonify({"error": "Expected JSON payload."}), HTTPStatus.BAD_REQUEST
        payload = request.get_json(silent=True) or {}

        module_type = payload.get("moduleType")
        trial_number = payload.get("trialNumber")
        period = payload.get("period") or "oos"

        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        if study.get("optimization_mode") != "wfa":
            return jsonify({"error": "Trade export is only supported for WFA studies."}), HTTPStatus.BAD_REQUEST

        window = _find_wfa_window(study_data, window_number)
        if not window:
            return jsonify({"error": "WFA window not found."}), HTTPStatus.NOT_FOUND

        window_id = window.get("window_id") or f"{study_id}_w{window_number}"
        fixed_params = (study.get("config_json") or {}).get("fixed_params") or {}
        params = window.get("best_params") or {}

        if module_type and module_type != "oos_result":
            modules = load_wfa_window_trials(window_id)
            module_trials = modules.get(module_type) or []
            if trial_number is None:
                return jsonify({"error": "trialNumber is required for module trades."}), HTTPStatus.BAD_REQUEST
            match = next(
                (trial for trial in module_trials if int(trial.get("trial_number") or -1) == int(trial_number)),
                None,
            )
            if not match:
                return jsonify({"error": "Trial not found for module."}), HTTPStatus.NOT_FOUND
            params = match.get("params") or {}

        start, end, error = _resolve_wfa_period(window, period)
        if error:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        is_window_oos = (period or "").lower() == "oos" and (not module_type or module_type == "oos_result")
        if is_window_oos and str(window.get("window_status") or "").lower() == "no_trade":
            trades = []
            module_label = module_type or "window"
            filename = (
                f"{study.get('study_name', 'study')}_wfa_window_{window_number}_"
                f"{module_label}_{period}_trades.csv"
            )
            return _send_trades_csv(
                trades=trades,
                csv_path=study.get("csv_file_path") or "",
                study=study,
                filename=filename,
            )

        actual_start = start
        actual_end = end
        if is_window_oos:
            actual_start = window.get("trade_start_ts") or window.get("trade_start_date") or start
            actual_end = window.get("trade_end_ts") or window.get("trade_end_date") or end

        csv_path = study.get("csv_file_path")
        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file is missing for this study.",
            not_found_error="CSV file is missing for this study.",
        )
        if error_response:
            return error_response

        warmup_bars = study.get("warmup_bars") or (study.get("config_json") or {}).get("warmup_bars") or 1000

        merged_params = {**fixed_params, **params}
        merged_params["dateFilter"] = True
        merged_params["start"] = actual_start
        merged_params["end"] = actual_end

        trades, error = _run_trade_export(
            strategy_id=study.get("strategy_id"),
            csv_path=csv_path,
            params=merged_params,
            warmup_bars=int(warmup_bars),
        )
        if error:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        if (
            bool(study.get("adaptive_mode"))
            and (period or "").lower() == "oos"
            and (not module_type or module_type == "oos_result")
        ):
            trades = _normalize_wfa_oos_trades(
                trades or [],
                start=actual_start,
                end=actual_end,
                expected_total=window.get("oos_total_trades"),
            )

        module_label = module_type or "window"
        filename = (
            f"{study.get('study_name', 'study')}_wfa_window_{window_number}_"
            f"{module_label}_{period}_trades.csv"
        )
        return _send_trades_csv(
            trades=trades or [],
            csv_path=csv_path,
            study=study,
            filename=filename,
        )



    @app.post("/api/studies/<string:study_id>/wfa/trades")
    def download_wfa_trades(study_id: str) -> object:
        study_data = load_study_from_db(study_id)
        if not study_data:
            return jsonify({"error": "Study not found."}), HTTPStatus.NOT_FOUND

        study = study_data["study"]
        if study.get("optimization_mode") != "wfa":
            return jsonify({"error": "Trade export is only supported for WFA studies."}), HTTPStatus.BAD_REQUEST

        csv_path = study.get("csv_file_path")
        csv_path, error_response = _resolve_csv_path_for_response(
            csv_path,
            missing_error="CSV file is missing for this study.",
            not_found_error="CSV file is missing for this study.",
        )
        if error_response:
            return error_response

        windows = study_data.get("windows") or []
        if not windows:
            return jsonify({"error": "No WFA windows available for this study."}), HTTPStatus.BAD_REQUEST

        config = study.get("config_json") or {}
        fixed_params = config.get("fixed_params") or {}
        warmup_bars = study.get("warmup_bars") or config.get("warmup_bars") or 1000

        from strategies import get_strategy

        try:
            strategy_class = get_strategy(study.get("strategy_id"))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST

        try:
            df = load_data(csv_path)
        except Exception as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST

        adaptive_mode = bool(study.get("adaptive_mode"))
        all_trades = []
        for window in windows:
            start_raw, end_raw, error = _resolve_wfa_period(window, "oos")
            if error:
                continue
            start, end = align_date_bounds(df.index, start_raw, end_raw)
            if start is None or end is None:
                continue

            params = {**fixed_params, **(window.get("best_params") or {})}
            params["dateFilter"] = True
            params["start"] = start
            params["end"] = end

            try:
                df_prepared, trade_start_idx = prepare_dataset_with_warmup(
                    df, start, end, int(warmup_bars)
                )
            except Exception:
                return jsonify({"error": "Failed to prepare dataset with warmup."}), HTTPStatus.BAD_REQUEST

            try:
                result = strategy_class.run(df_prepared, params, trade_start_idx)
            except Exception as exc:
                return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST

            if adaptive_mode:
                window_trades = _normalize_wfa_oos_trades(
                    result.trades or [],
                    start=start,
                    end=end,
                    expected_total=window.get("oos_total_trades"),
                )
            else:
                window_trades = [
                    trade
                    for trade in result.trades
                    if trade.entry_time and start <= trade.entry_time <= end
                ]
            all_trades.extend(window_trades)


        if not adaptive_mode:
            all_trades.sort(key=lambda t: t.entry_time or pd.Timestamp.min)

        from core.export import _extract_symbol_from_csv_filename

        symbol = _extract_symbol_from_csv_filename(study.get("csv_file_name") or "")
        csv_content = export_trades_csv(all_trades, symbol=symbol)
        buffer = io.BytesIO(csv_content.encode("utf-8"))
        buffer.seek(0)

        filename = f"{study.get('study_name', 'study')}_wfa_oos_trades.csv"
        return send_file(
            buffer,
            mimetype="text/csv",
            as_attachment=True,
            download_name=filename,
        )



    @app.get("/api/presets")
    def list_presets_endpoint() -> object:
        presets = _list_presets()
        return jsonify({"presets": presets})



    @app.get("/api/presets/<string:name>")
    def load_preset_endpoint(name: str) -> object:
        target = Path(name).stem
        try:
            values = _load_preset(target)
        except FileNotFoundError:
            return ("Preset not found.", HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            app.logger.exception("Failed to load preset '%s'", name)
            return (str(exc), HTTPStatus.INTERNAL_SERVER_ERROR)
        return jsonify({"name": target, "values": values})



    @app.post("/api/presets")
    def create_preset_endpoint() -> object:
        if not request.is_json:
            return ("Expected JSON body.", HTTPStatus.BAD_REQUEST)
        payload = request.get_json() or {}
        try:
            name = _validate_preset_name(payload.get("name"))
        except ValueError as exc:
            return (str(exc), HTTPStatus.BAD_REQUEST)

        normalized_name_lower = name.lower()
        for entry in _list_presets():
            if entry["name"].lower() == normalized_name_lower:
                return ("Preset with this name already exists.", HTTPStatus.CONFLICT)

        try:
            values = _normalize_preset_payload(payload.get("values", {}))
        except ValueError as exc:
            return (str(exc), HTTPStatus.BAD_REQUEST)

        try:
            _write_preset(name, values)
        except Exception:  # pragma: no cover - defensive
            app.logger.exception("Failed to save preset '%s'", name)
            return ("Failed to save preset.", HTTPStatus.INTERNAL_SERVER_ERROR)

        return jsonify({"name": name, "values": values}), HTTPStatus.CREATED



    @app.put("/api/presets/<string:name>")
    def overwrite_preset_endpoint(name: str) -> object:
        if not request.is_json:
            return ("Expected JSON body.", HTTPStatus.BAD_REQUEST)
        try:
            normalized_name = _validate_preset_name(name)
        except ValueError as exc:
            return (str(exc), HTTPStatus.BAD_REQUEST)

        preset_path = _preset_path(normalized_name)
        if not preset_path.exists():
            return ("Preset not found.", HTTPStatus.NOT_FOUND)

        payload = request.get_json() or {}
        try:
            values = _normalize_preset_payload(payload.get("values", {}))
        except ValueError as exc:
            return (str(exc), HTTPStatus.BAD_REQUEST)

        try:
            _write_preset(normalized_name, values)
        except Exception:  # pragma: no cover - defensive
            app.logger.exception("Failed to overwrite preset '%s'", name)
            return ("Failed to save preset.", HTTPStatus.INTERNAL_SERVER_ERROR)

        return jsonify({"name": normalized_name, "values": values})



    @app.put("/api/presets/defaults")
    def overwrite_defaults_endpoint() -> object:
        if not request.is_json:
            return ("Expected JSON body.", HTTPStatus.BAD_REQUEST)
        payload = request.get_json() or {}
        try:
            values = _normalize_preset_payload(payload.get("values", {}))
        except ValueError as exc:
            return (str(exc), HTTPStatus.BAD_REQUEST)

        try:
            _write_preset(DEFAULT_PRESET_NAME, values)
        except Exception:  # pragma: no cover - defensive
            app.logger.exception("Failed to overwrite default preset")
            return ("Failed to save default preset.", HTTPStatus.INTERNAL_SERVER_ERROR)

        return jsonify({"name": DEFAULT_PRESET_NAME, "values": values})



    @app.delete("/api/presets/<string:name>")
    def delete_preset_endpoint(name: str) -> object:
        target = Path(name).stem
        if target.lower() == DEFAULT_PRESET_NAME:
            return ("Default preset cannot be deleted.", HTTPStatus.BAD_REQUEST)
        path = _preset_path(target)
        if not path.exists():
            return ("Preset not found.", HTTPStatus.NOT_FOUND)
        try:
            path.unlink()
        except Exception:  # pragma: no cover - defensive
            app.logger.exception("Failed to delete preset '%s'", name)
            return ("Failed to delete preset.", HTTPStatus.INTERNAL_SERVER_ERROR)
        return ("", HTTPStatus.NO_CONTENT)



    @app.post("/api/presets/import-csv")
    def import_preset_from_csv() -> object:
        if "file" not in request.files:
            return ("CSV file is required.", HTTPStatus.BAD_REQUEST)
        csv_file = request.files["file"]
        if not csv_file or csv_file.filename == "":
            return ("CSV file is required.", HTTPStatus.BAD_REQUEST)
        try:
            updates, applied, errors = _parse_csv_parameter_block(csv_file)
        except ValueError as exc:
            return (str(exc), HTTPStatus.BAD_REQUEST)
        except Exception:  # pragma: no cover - defensive
            app.logger.exception("Failed to parse CSV for preset import")
            return ("Failed to parse CSV file.", HTTPStatus.BAD_REQUEST)
        if errors:
            return (
                jsonify({"error": "Invalid numeric values in CSV.", "details": errors}),
                HTTPStatus.BAD_REQUEST,
            )
        if not updates:
            return ("No fixed parameters found in CSV.", HTTPStatus.BAD_REQUEST)
        return jsonify({"values": updates, "applied": applied})



    @app.get("/api/strategies")
    def list_strategies_endpoint() -> object:
        """
        List all available strategies.

        Returns:
            JSON: {
                "strategies": [
                    {
                        "id": "s01_trailing_ma",
                        "name": "S01 Trailing MA",
                        "version": "v26",
                        "description": "...",
                        "author": "..."
                    }
                ]
            }
        """
        from strategies import list_strategies

        strategies = list_strategies()
        return jsonify({"strategies": strategies})


    @app.route("/api/strategy/<strategy_id>/config", methods=["GET"])
    def get_strategy_config_single(strategy_id: str):
        """Return strategy configuration for frontend rendering.

        Args:
            strategy_id: Strategy identifier (e.g., "s01_trailing_ma")

        Returns:
            JSON response with strategy configuration
        """
        try:
            from strategies import get_strategy_config

            config = get_strategy_config(strategy_id)
            parameters = config.get("parameters", {}) if isinstance(config, dict) else {}
            parameter_order = list(parameters.keys()) if isinstance(parameters, dict) else []
            group_order = []
            if isinstance(parameters, dict):
                for key in parameter_order:
                    definition = parameters.get(key, {})
                    group = definition.get("group") if isinstance(definition, dict) else None
                    group = group or "Other"
                    if group not in group_order:
                        group_order.append(group)

            payload = dict(config or {})
            payload["parameter_order"] = parameter_order
            payload["group_order"] = group_order
            return jsonify(payload), HTTPStatus.OK

        except FileNotFoundError:
            return (
                jsonify({"error": f"Strategy '{strategy_id}' not found"}),
                HTTPStatus.NOT_FOUND,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to load config for %s", strategy_id)
            return (
                jsonify({"error": f"Failed to load strategy config: {str(exc)}"}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )



    @app.get("/api/strategies/<string:strategy_id>")
    def get_strategy_metadata_endpoint(strategy_id: str) -> object:
        """
        Get strategy metadata (lightweight version without full parameters).

        Args:
            strategy_id: Strategy identifier

        Returns:
            JSON: {
                "id": "s01_trailing_ma",
                "name": "S01 Trailing MA",
                "version": "v26",
                "description": "...",
                "parameter_count": 25
            }

        Errors:
            404: Strategy not found
        """
        from strategies import get_strategy_config

        try:
            config = get_strategy_config(strategy_id)
            return jsonify({
                "id": config.get('id'),
                "name": config.get('name'),
                "version": config.get('version'),
                "description": config.get('description'),
                "author": config.get('author', ''),
                "parameter_count": len(config.get('parameters', {}))
            })
        except ValueError as e:
            return (str(e), HTTPStatus.NOT_FOUND)


    if __name__ == "__main__":
        app.run(host="0.0.0.0", port=5000, debug=False)
