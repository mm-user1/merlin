import io
import json
import math
import re
import tempfile
import threading
import time
from datetime import datetime, timezone
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
    get_active_db_name,
    get_study_trial,
    list_manual_tests,
    list_studies,
    load_manual_test_results,
    load_study_from_db,
    load_wfa_window_trials,
    set_active_db,
    save_dsr_results,
    save_forward_test_results,
    save_stress_test_results,
    save_manual_test_to_db,
    save_oos_test_results,
    update_csv_path,
    update_study_config_json,
)

try:
    from .server_services import (
        DEFAULT_PRESET_NAME,
        _build_optimization_config,
        _build_trial_metrics,
        _clear_cancelled_run,
        _execute_backtest_request,
        _find_wfa_window,
        _get_optimization_state,
        _get_parameter_types,
        _is_run_cancelled,
        _json_safe,
        _list_presets,
        _load_preset,
        _normalize_run_id,
        _normalize_preset_payload,
        _parse_csv_parameter_block,
        _preset_path,
        _register_cancelled_run,
        _resolve_csv_path,
        _resolve_strategy_id_from_request,
        _resolve_wfa_period,
        _run_equity_export,
        _run_trade_export,
        _send_trades_csv,
        _set_optimization_state,
        _validate_csv_for_study,
        _validate_preset_name,
        _write_preset,
        validate_constraints_config,
        validate_objectives_config,
        validate_sampler_config,
    )
except ImportError:
    from server_services import (
        DEFAULT_PRESET_NAME,
        _build_optimization_config,
        _build_trial_metrics,
        _clear_cancelled_run,
        _execute_backtest_request,
        _find_wfa_window,
        _get_optimization_state,
        _get_parameter_types,
        _is_run_cancelled,
        _json_safe,
        _list_presets,
        _load_preset,
        _normalize_run_id,
        _normalize_preset_payload,
        _parse_csv_parameter_block,
        _preset_path,
        _register_cancelled_run,
        _resolve_csv_path,
        _resolve_strategy_id_from_request,
        _resolve_wfa_period,
        _run_equity_export,
        _run_trade_export,
        _send_trades_csv,
        _set_optimization_state,
        _validate_csv_for_study,
        _validate_preset_name,
        _write_preset,
        validate_constraints_config,
        validate_objectives_config,
        validate_sampler_config,
    )


def register_routes(app):

    @app.get("/api/optimization/status")
    def optimization_status() -> object:
        return jsonify(_get_optimization_state())



    @app.post("/api/optimization/cancel")
    def optimization_cancel() -> object:
        requested_run_id = _normalize_run_id(
            request.form.get("runId")
            or request.form.get("run_id")
            or request.args.get("run_id")
        )
        state = _get_optimization_state()
        active_run_id = _normalize_run_id(state.get("run_id") or state.get("runId"))
        run_id = requested_run_id or active_run_id
        if run_id:
            _register_cancelled_run(run_id)
            state["cancelled_run_id"] = run_id
        state["status"] = "cancelled"
        _set_optimization_state(state)
        payload: Dict[str, Any] = {"status": "cancelled"}
        if run_id:
            payload["run_id"] = run_id
        return jsonify(payload)

    def _resolve_request_run_id(form_data: Any) -> str:
        raw_run_id = ""
        if form_data is not None:
            raw_run_id = (
                form_data.get("runId")
                or form_data.get("run_id")
                or ""
            )
        if not raw_run_id:
            raw_run_id = request.args.get("run_id") or ""

        normalized = _normalize_run_id(raw_run_id)
        if normalized:
            return normalized
        return f"run_{time.time_ns()}"

    def _apply_db_target_from_form(form_data) -> Optional[object]:
        db_target = (form_data.get("dbTarget") or "").strip()
        if not db_target:
            return None
        try:
            if db_target == "new":
                return (
                    jsonify(
                        {
                            "error": (
                                "Please create and select a database in Database Target "
                                "before running optimization or Walk-Forward."
                            )
                        }
                    ),
                    HTTPStatus.BAD_REQUEST,
                )
            elif db_target != get_active_db_name():
                set_active_db(db_target)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST
        return None



    @app.post("/api/walkforward")
    def run_walkforward_optimization() -> object:
        """Run Walk-Forward Analysis"""
        data = request.form
        run_id = _resolve_request_run_id(data)
        _clear_cancelled_run(run_id)
        csv_path_raw = (data.get("csvPath") or "").strip()
        data_source = None
        data_path = ""
        original_csv_name = ""

        if not csv_path_raw:
            return jsonify({"error": "CSV path is required."}), HTTPStatus.BAD_REQUEST

        try:
            resolved_path = _resolve_csv_path(csv_path_raw)
            data_source = str(resolved_path)
            data_path = str(resolved_path)
            original_csv_name = Path(resolved_path).name
        except FileNotFoundError:
            return jsonify({"error": "CSV file not found."}), HTTPStatus.BAD_REQUEST
        except IsADirectoryError:
            return jsonify({"error": "CSV path must point to a file."}), HTTPStatus.BAD_REQUEST
        except PermissionError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.FORBIDDEN
        except ValueError as exc:
            message = str(exc).strip() or "CSV path is required."
            return jsonify({"error": message}), HTTPStatus.BAD_REQUEST
        except OSError:
            return jsonify({"error": "Failed to access CSV file."}), HTTPStatus.BAD_REQUEST

        config_raw = data.get("config")
        if not config_raw:
            return jsonify({"error": "Missing optimization config."}), HTTPStatus.BAD_REQUEST

        try:
            config_payload = json.loads(config_raw)
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid optimization config JSON."}), HTTPStatus.BAD_REQUEST

        post_process_payload = config_payload.get("postProcess")
        if not isinstance(post_process_payload, dict):
            post_process_payload = {}

        objectives = config_payload.get("objectives", [])
        primary_objective = config_payload.get("primary_objective")
        valid, error = validate_objectives_config(objectives, primary_objective)
        if not valid:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        constraints = config_payload.get("constraints", [])
        valid, error = validate_constraints_config(constraints)
        if not valid:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        sampler_type = str(config_payload.get("sampler", "tpe")).strip().lower()
        population_size = config_payload.get("population_size")
        crossover_prob = config_payload.get("crossover_prob")
        try:
            population_size_val = int(population_size) if population_size is not None else None
        except (TypeError, ValueError):
            return jsonify({"error": "Population size must be a number."}), HTTPStatus.BAD_REQUEST
        try:
            crossover_prob_val = float(crossover_prob) if crossover_prob is not None else None
        except (TypeError, ValueError):
            return jsonify({"error": "Crossover probability must be a number."}), HTTPStatus.BAD_REQUEST

        valid, error = validate_sampler_config(sampler_type, population_size_val, crossover_prob_val)
        if not valid:
            return jsonify({"error": error}), HTTPStatus.BAD_REQUEST

        strategy_id, error_response = _resolve_strategy_id_from_request()
        if error_response:
            return error_response

        warmup_bars_raw = data.get("warmupBars", "1000")
        try:
            warmup_bars = int(warmup_bars_raw)
            warmup_bars = max(100, min(5000, warmup_bars))
        except (TypeError, ValueError):
            warmup_bars = 1000

        try:
            optimization_config = _build_optimization_config(
                data_source,
                config_payload,
                warmup_bars=warmup_bars,
                strategy_id=strategy_id,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST
        except Exception:  # pragma: no cover - defensive
            app.logger.exception("Failed to build optimization config for walk-forward")
            return jsonify({"error": "Failed to prepare optimization config."}), HTTPStatus.INTERNAL_SERVER_ERROR

        if optimization_config.optimization_mode != "optuna":
            return jsonify({"error": "Walk-Forward requires Optuna optimization mode."}), HTTPStatus.BAD_REQUEST

        if hasattr(data_source, "seek"):
            try:
                data_source.seek(0)
            except Exception:  # pragma: no cover - defensive
                pass

        try:
            df = load_data(data_source)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST
        except Exception:  # pragma: no cover - defensive
            app.logger.exception("Failed to load CSV for walk-forward")
            return jsonify({"error": "Failed to load CSV data."}), HTTPStatus.INTERNAL_SERVER_ERROR

        # Apply date filtering for Walk-Forward Analysis
        use_date_filter = optimization_config.fixed_params.get('dateFilter', False)
        start_date = optimization_config.fixed_params.get('start')
        end_date = optimization_config.fixed_params.get('end')

        if use_date_filter and start_date is not None and end_date is not None:
            try:
                # Ensure dates are pandas Timestamps with UTC timezone
                start_ts, end_ts = align_date_bounds(df.index, start_date, end_date)
                if start_ts is None or end_ts is None:
                    return jsonify({"error": "Invalid date filter range."}), HTTPStatus.BAD_REQUEST

                # IMPORTANT: Add warmup period before start_ts for Walk-Forward Analysis
                # The first WFA window will start from start_ts, so it needs historical data.
                # Use the user-specified warmup bars (default 1000) as-is to avoid strategy-specific logic.

                # Find the index of start_ts in the dataframe
                start_idx = df.index.searchsorted(start_ts)

                # Calculate warmup_start_idx (go back warmup_bars, but not before 0)
                warmup_start_idx = max(0, start_idx - warmup_bars)

                # Get the actual warmup start timestamp
                warmup_start_ts = df.index[warmup_start_idx]

                # Filter dataframe: include warmup period before start_ts
                df_filtered = df[(df.index >= warmup_start_ts) & (df.index <= end_ts)].copy()

                # Check that we have enough data in the ACTUAL trading period (start_ts to end_ts)
                df_trading_period = df[(df.index >= start_ts) & (df.index <= end_ts)]
                if len(df_trading_period) < 1000:
                    return jsonify({
                        "error": f"Selected date range contains only {len(df_trading_period)} bars. Need at least 1000 bars for Walk-Forward Analysis."
                    }), HTTPStatus.BAD_REQUEST

                df = df_filtered
                actual_warmup_bars = start_idx - warmup_start_idx
                print(f"Walk-Forward: Using date-filtered data with warmup: {len(df)} bars total")
                print(f"  Warmup period: {actual_warmup_bars} bars from {warmup_start_ts} to {start_ts}")
                print(f"  Trading period: {len(df_trading_period)} bars from {start_ts} to {end_ts}")

            except Exception as e:
                return jsonify({"error": f"Failed to apply date filter: {str(e)}"}), HTTPStatus.BAD_REQUEST

        optimization_config.warmup_bars = warmup_bars
        optimization_config.csv_original_name = original_csv_name

        base_template = {
            "enabled_params": json.loads(json.dumps(optimization_config.enabled_params)),
            "param_ranges": json.loads(json.dumps(optimization_config.param_ranges)),
            "param_types": json.loads(json.dumps(optimization_config.param_types)),
            "fixed_params": json.loads(json.dumps(optimization_config.fixed_params)),
            "risk_per_trade_pct": float(optimization_config.risk_per_trade_pct),
            "contract_size": float(optimization_config.contract_size),
            "commission_rate": float(optimization_config.commission_rate),
            "worker_processes": int(optimization_config.worker_processes),
            "filter_min_profit": bool(optimization_config.filter_min_profit),
            "min_profit_threshold": float(optimization_config.min_profit_threshold),
            "score_config": json.loads(json.dumps(optimization_config.score_config or {})),
            "strategy_id": optimization_config.strategy_id,
            "warmup_bars": optimization_config.warmup_bars,
            "csv_original_name": original_csv_name,
            "objectives": list(getattr(optimization_config, "objectives", []) or []),
            "primary_objective": getattr(optimization_config, "primary_objective", None),
            "constraints": json.loads(json.dumps(getattr(optimization_config, "constraints", []) or [])),
            "sampler_type": getattr(optimization_config, "sampler_type", "tpe"),
            "population_size": getattr(optimization_config, "population_size", None),
            "crossover_prob": getattr(optimization_config, "crossover_prob", None),
            "mutation_prob": getattr(optimization_config, "mutation_prob", None),
            "swapping_prob": getattr(optimization_config, "swapping_prob", None),
            "n_startup_trials": getattr(optimization_config, "n_startup_trials", 20),
            "coverage_mode": bool(getattr(optimization_config, "coverage_mode", False)),
        }
        if post_process_payload:
            base_template["postProcess"] = post_process_payload

        optuna_settings = {
            "objectives": list(getattr(optimization_config, "objectives", []) or []),
            "primary_objective": getattr(optimization_config, "primary_objective", None),
            "constraints": json.loads(json.dumps(getattr(optimization_config, "constraints", []) or [])),
            "budget_mode": getattr(optimization_config, "optuna_budget_mode", "trials"),
            "n_trials": int(getattr(optimization_config, "optuna_n_trials", 100)),
            "time_limit": int(getattr(optimization_config, "optuna_time_limit", 3600)),
            "convergence_patience": int(getattr(optimization_config, "optuna_convergence", 50)),
            "enable_pruning": bool(getattr(optimization_config, "optuna_enable_pruning", True)),
            "sampler": getattr(optimization_config, "sampler_type", "tpe"),
            "population_size": getattr(optimization_config, "population_size", None),
            "crossover_prob": getattr(optimization_config, "crossover_prob", None),
            "mutation_prob": getattr(optimization_config, "mutation_prob", None),
            "swapping_prob": getattr(optimization_config, "swapping_prob", None),
            "pruner": getattr(optimization_config, "optuna_pruner", "median"),
            "warmup_trials": int(getattr(optimization_config, "n_startup_trials", 20)),
            "coverage_mode": bool(getattr(optimization_config, "coverage_mode", False)),
        }
        base_template["optuna_config"] = json.loads(json.dumps(optuna_settings))

        try:
            is_period_days = int(data.get("wf_is_period_days", 90))
            oos_period_days = int(data.get("wf_oos_period_days", 30))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid Walk-Forward parameters."}), HTTPStatus.BAD_REQUEST

        is_period_days = max(1, min(3650, is_period_days))
        oos_period_days = max(1, min(3650, oos_period_days))

        adaptive_raw = data.get("wf_adaptive_mode", False)
        if isinstance(adaptive_raw, str):
            adaptive_mode = adaptive_raw.strip().lower() in {"true", "1", "yes", "on"}
        else:
            adaptive_mode = bool(adaptive_raw)

        try:
            max_oos_period_days = int(data.get("wf_max_oos_period_days", 90))
        except (TypeError, ValueError):
            max_oos_period_days = 90
        max_oos_period_days = max(30, min(365, max_oos_period_days))

        try:
            min_oos_trades = int(data.get("wf_min_oos_trades", 5))
        except (TypeError, ValueError):
            min_oos_trades = 5
        min_oos_trades = max(2, min(50, min_oos_trades))

        try:
            check_interval_trades = int(data.get("wf_check_interval_trades", 3))
        except (TypeError, ValueError):
            check_interval_trades = 3
        check_interval_trades = max(1, min(20, check_interval_trades))

        try:
            cusum_threshold = float(data.get("wf_cusum_threshold", 5.0))
        except (TypeError, ValueError):
            cusum_threshold = 5.0
        cusum_threshold = max(1.0, min(20.0, cusum_threshold))

        try:
            dd_threshold_multiplier = float(data.get("wf_dd_threshold_multiplier", 1.5))
        except (TypeError, ValueError):
            dd_threshold_multiplier = 1.5
        dd_threshold_multiplier = max(1.0, min(5.0, dd_threshold_multiplier))

        try:
            inactivity_multiplier = float(data.get("wf_inactivity_multiplier", 5.0))
        except (TypeError, ValueError):
            inactivity_multiplier = 5.0
        inactivity_multiplier = max(2.0, min(20.0, inactivity_multiplier))

        try:
            store_top_n_trials = int(data.get("wf_store_top_n_trials", 50))
        except (TypeError, ValueError):
            store_top_n_trials = 50
        store_top_n_trials = max(10, min(500, store_top_n_trials))

        from core.walkforward_engine import WFConfig, WalkForwardEngine

        post_process_config = None
        if post_process_payload.get("enabled"):
            post_process_config = PostProcessConfig(
                enabled=True,
                ft_period_days=int(post_process_payload.get("ftPeriodDays", 15)),
                top_k=int(post_process_payload.get("topK", 10)),
                sort_metric=str(post_process_payload.get("sortMetric", "profit_degradation")),
                warmup_bars=warmup_bars,
            )

        dsr_config = None
        if post_process_payload.get("dsrEnabled"):
            try:
                dsr_top_k = int(post_process_payload.get("dsrTopK", 20))
            except (TypeError, ValueError):
                dsr_top_k = 20
            dsr_config = DSRConfig(
                enabled=True,
                top_k=dsr_top_k,
                warmup_bars=warmup_bars,
            )

        st_config = None
        st_payload = post_process_payload.get("stressTest")
        if isinstance(st_payload, dict) and st_payload.get("enabled"):
            try:
                st_top_k = int(st_payload.get("topK", 5))
            except (TypeError, ValueError):
                st_top_k = 5
            try:
                threshold_raw = float(st_payload.get("failureThreshold", 0.7))
            except (TypeError, ValueError):
                threshold_raw = 0.7
            failure_threshold = threshold_raw / 100.0 if threshold_raw > 1 else threshold_raw
            st_config = StressTestConfig(
                enabled=True,
                top_k=st_top_k,
                failure_threshold=failure_threshold,
                sort_metric=str(st_payload.get("sortMetric", "profit_retention")),
                warmup_bars=warmup_bars,
            )

        wf_config = WFConfig(
            is_period_days=is_period_days,
            oos_period_days=oos_period_days,
            warmup_bars=warmup_bars,
            strategy_id=strategy_id,
            post_process=post_process_config,
            dsr_config=dsr_config,
            stress_test_config=st_config,
            store_top_n_trials=store_top_n_trials,
            adaptive_mode=adaptive_mode,
            max_oos_period_days=max_oos_period_days,
            min_oos_trades=min_oos_trades,
            check_interval_trades=check_interval_trades,
            cusum_threshold=cusum_threshold,
            dd_threshold_multiplier=dd_threshold_multiplier,
            inactivity_multiplier=inactivity_multiplier,
        )

        base_template["adaptive_mode"] = adaptive_mode
        base_template["max_oos_period_days"] = max_oos_period_days
        base_template["min_oos_trades"] = min_oos_trades
        base_template["check_interval_trades"] = check_interval_trades
        base_template["cusum_threshold"] = cusum_threshold
        base_template["dd_threshold_multiplier"] = dd_threshold_multiplier
        base_template["inactivity_multiplier"] = inactivity_multiplier

        base_template["wfa"] = {
            "is_period_days": is_period_days,
            "oos_period_days": oos_period_days,
            "store_top_n_trials": store_top_n_trials,
            "adaptive_mode": adaptive_mode,
            "max_oos_period_days": max_oos_period_days,
            "min_oos_trades": min_oos_trades,
            "check_interval_trades": check_interval_trades,
            "cusum_threshold": cusum_threshold,
            "dd_threshold_multiplier": dd_threshold_multiplier,
            "inactivity_multiplier": inactivity_multiplier,
        }
        engine = WalkForwardEngine(wf_config, base_template, optuna_settings, csv_file_path=data_path)

        db_apply_error = _apply_db_target_from_form(data)
        if db_apply_error:
            return db_apply_error

        _set_optimization_state(
            {
                "status": "running",
                "mode": "wfa",
                "run_id": run_id,
                "strategy_id": strategy_id,
                "data_path": data_path,
                "config": config_payload,
                "wfa": {
                    "is_period_days": is_period_days,
                    "oos_period_days": oos_period_days,
                    "store_top_n_trials": store_top_n_trials,
                    "adaptive_mode": adaptive_mode,
                    "max_oos_period_days": max_oos_period_days,
                    "min_oos_trades": min_oos_trades,
                    "check_interval_trades": check_interval_trades,
                    "cusum_threshold": cusum_threshold,
                    "dd_threshold_multiplier": dd_threshold_multiplier,
                    "inactivity_multiplier": inactivity_multiplier,
                },
            }
        )

        try:
            result, study_id = engine.run_wf_optimization(df)
        except ValueError as exc:
            _set_optimization_state(
                {
                    "status": "error",
                    "mode": "wfa",
                    "run_id": run_id,
                    "strategy_id": strategy_id,
                    "error": str(exc),
                }
            )
            return jsonify({"error": str(exc)}), HTTPStatus.BAD_REQUEST
        except Exception:  # pragma: no cover - defensive
            _set_optimization_state(
                {
                    "status": "error",
                    "mode": "wfa",
                    "run_id": run_id,
                    "strategy_id": strategy_id,
                    "error": "Walk-forward optimization failed.",
                }
            )
            app.logger.exception("Walk-forward optimization failed")
            return jsonify({"error": "Walk-forward optimization failed."}), HTTPStatus.INTERNAL_SERVER_ERROR

        if _is_run_cancelled(run_id):
            if study_id:
                try:
                    delete_study(study_id)
                except Exception:  # pragma: no cover - defensive
                    app.logger.exception("Failed to cleanup cancelled WFA study %s", study_id)
            _clear_cancelled_run(run_id)
            _set_optimization_state(
                {
                    "status": "cancelled",
                    "mode": "wfa",
                    "run_id": run_id,
                    "strategy_id": strategy_id,
                    "data_path": data_path,
                    "study_id": None,
                }
            )
            return jsonify(
                {
                    "status": "cancelled",
                    "mode": "wfa",
                    "run_id": run_id,
                    "strategy_id": strategy_id,
                    "data_path": data_path,
                    "study_id": None,
                    "active_db": get_active_db_name(),
                }
            )

        stitched_oos = result.stitched_oos

        response_payload = {
            "status": "success",
            "summary": {
                "total_windows": result.total_windows,
                "stitched_oos_net_profit_pct": round(result.stitched_oos.final_net_profit_pct, 2),
                "stitched_oos_max_drawdown_pct": round(result.stitched_oos.max_drawdown_pct, 2),
                "stitched_oos_total_trades": result.stitched_oos.total_trades,
                "wfe": round(result.stitched_oos.wfe, 2),
                "oos_win_rate": round(result.stitched_oos.oos_win_rate, 1),
            },
            "mode": "wfa",
            "run_id": run_id,
            "strategy_id": strategy_id,
            "data_path": data_path,
            "study_id": study_id,
            "active_db": get_active_db_name(),
        }

        _set_optimization_state(
            {
                "status": "completed",
                "mode": "wfa",
                "run_id": run_id,
                "strategy_id": strategy_id,
                "data_path": data_path,
                "summary": response_payload.get("summary", {}),
                "study_id": study_id,
            }
        )
        _clear_cancelled_run(run_id)

        return jsonify(response_payload)



    @app.post("/api/backtest")
    def run_backtest() -> object:
        strategy_id, error_response = _resolve_strategy_id_from_request()
        if error_response:
            return error_response

        execution, error = _execute_backtest_request(strategy_id)
        if error:
            return error
        result = execution["result"]
        payload = execution["payload"]

        return jsonify({
            "metrics": result.to_dict(),
            "parameters": payload,
        })


    @app.post("/api/backtest/trades")
    def download_backtest_trades() -> object:
        strategy_id, error_response = _resolve_strategy_id_from_request()
        if error_response:
            return error_response

        execution, error = _execute_backtest_request(strategy_id)
        if error:
            return error

        result = execution["result"]
        csv_name = str(execution.get("csv_name") or "")
        source_stem = Path(csv_name).stem if csv_name else "dataset"
        safe_source = re.sub(r"[^A-Za-z0-9._-]+", "_", source_stem).strip("_") or "dataset"
        safe_strategy = re.sub(r"[^A-Za-z0-9._-]+", "_", strategy_id).strip("_") or "strategy"
        filename = f"backtest_{safe_strategy}_{safe_source}_trades.csv"

        return _send_trades_csv(
            trades=result.trades or [],
            csv_path=csv_name,
            study={},
            filename=filename,
        )


    @app.post("/api/optimize")
    def run_optimization_endpoint() -> object:
        csv_path_raw = (request.form.get("csvPath") or "").strip()
        data_path = ""
        source_name = ""

        if not csv_path_raw:
            return ("CSV path is required.", HTTPStatus.BAD_REQUEST)

        try:
            resolved_path = _resolve_csv_path(csv_path_raw)
        except FileNotFoundError:
            return ("CSV file not found.", HTTPStatus.BAD_REQUEST)
        except IsADirectoryError:
            return ("CSV path must point to a file.", HTTPStatus.BAD_REQUEST)
        except PermissionError as exc:
            return (str(exc), HTTPStatus.FORBIDDEN)
        except ValueError as exc:
            message = str(exc).strip() or "CSV path is required."
            return (message, HTTPStatus.BAD_REQUEST)
        except OSError:
            return ("Failed to access CSV file.", HTTPStatus.BAD_REQUEST)
        data_source = str(resolved_path)
        data_path = str(resolved_path)
        source_name = Path(resolved_path).name

        config_raw = request.form.get("config")
        if not config_raw:
            return ("Optimization config is required.", HTTPStatus.BAD_REQUEST)
        try:
            config_payload = json.loads(config_raw)
        except json.JSONDecodeError:
            return ("Invalid optimization config JSON.", HTTPStatus.BAD_REQUEST)

        post_process_payload = config_payload.get("postProcess")
        if not isinstance(post_process_payload, dict):
            post_process_payload = {}

        objectives = config_payload.get("objectives", [])
        primary_objective = config_payload.get("primary_objective")
        valid, error = validate_objectives_config(objectives, primary_objective)
        if not valid:
            return (error, HTTPStatus.BAD_REQUEST)

        constraints = config_payload.get("constraints", [])
        valid, error = validate_constraints_config(constraints)
        if not valid:
            return (error, HTTPStatus.BAD_REQUEST)

        sampler_type = str(config_payload.get("sampler", "tpe")).strip().lower()
        population_size = config_payload.get("population_size")
        crossover_prob = config_payload.get("crossover_prob")
        try:
            population_size_val = int(population_size) if population_size is not None else None
        except (TypeError, ValueError):
            return ("Population size must be a number.", HTTPStatus.BAD_REQUEST)
        try:
            crossover_prob_val = float(crossover_prob) if crossover_prob is not None else None
        except (TypeError, ValueError):
            return ("Crossover probability must be a number.", HTTPStatus.BAD_REQUEST)

        valid, error = validate_sampler_config(sampler_type, population_size_val, crossover_prob_val)
        if not valid:
            return (error, HTTPStatus.BAD_REQUEST)

        strategy_id, error_response = _resolve_strategy_id_from_request()
        if error_response:
            return error_response

        warmup_bars_raw = request.form.get("warmupBars", "1000")
        try:
            warmup_bars = int(warmup_bars_raw)
            warmup_bars = max(100, min(5000, warmup_bars))
        except (TypeError, ValueError):
            warmup_bars = 1000
        run_id = _resolve_request_run_id(request.form)
        _clear_cancelled_run(run_id)

        oos_payload = config_payload.get("oosTest")
        if not isinstance(oos_payload, dict):
            oos_payload = {}

        ft_enabled = bool(post_process_payload.get("enabled", False))
        dsr_enabled = bool(post_process_payload.get("dsrEnabled", False))
        st_payload = post_process_payload.get("stressTest")
        if not isinstance(st_payload, dict):
            st_payload = {}
        st_enabled = bool(st_payload.get("enabled", False))
        oos_enabled = bool(oos_payload.get("enabled", False))
        try:
            dsr_top_k = int(post_process_payload.get("dsrTopK", 20))
        except (TypeError, ValueError):
            dsr_top_k = 20
        try:
            oos_period_days = int(oos_payload.get("periodDays", 30))
        except (TypeError, ValueError):
            oos_period_days = 30
        try:
            oos_top_k = int(oos_payload.get("topK", 20))
        except (TypeError, ValueError):
            oos_top_k = 20
        oos_top_k = max(1, min(10000, oos_top_k))
        ft_start = None
        ft_end = None
        oos_start = None
        oos_end = None
        is_days = None
        ft_days = None
        oos_days = None

        if ft_enabled or oos_enabled:
            fixed_params_payload = config_payload.get("fixed_params") or {}
            config_payload["fixed_params"] = fixed_params_payload

            original_user_start = fixed_params_payload.get("start")
            original_user_end = fixed_params_payload.get("end")
            user_start = parse_timestamp_utc(original_user_start)
            user_end = parse_timestamp_utc(original_user_end)

            if user_start is None or user_end is None:
                try:
                    df_temp = load_data(data_source)
                except Exception as exc:
                    return (f"Failed to load CSV for period split: {exc}", HTTPStatus.BAD_REQUEST)
                user_start = df_temp.index.min()
                user_end = df_temp.index.max()

            if user_start is None or user_end is None:
                return ("Failed to determine date range.", HTTPStatus.BAD_REQUEST)

            ft_period_days = None
            if ft_enabled:
                try:
                    ft_period_days = int(post_process_payload.get("ftPeriodDays", 30))
                except (TypeError, ValueError):
                    return ("Invalid FT period days.", HTTPStatus.BAD_REQUEST)

            if oos_enabled:
                oos_period_days = max(1, min(3650, oos_period_days))
            if ft_period_days is not None:
                ft_period_days = max(1, min(3650, ft_period_days))

            try:
                period_dates = calculate_period_dates(
                    user_start,
                    user_end,
                    ft_enabled=ft_enabled,
                    ft_period_days=ft_period_days,
                    oos_enabled=oos_enabled,
                    oos_period_days=oos_period_days,
                )
            except ValueError as exc:
                return (str(exc), HTTPStatus.BAD_REQUEST)

            ft_start = period_dates.get("ft_start")
            ft_end = period_dates.get("ft_end")
            oos_start = period_dates.get("oos_start")
            oos_end = period_dates.get("oos_end")
            is_days = period_dates.get("is_days")
            ft_days = period_dates.get("ft_days")
            oos_days = period_dates.get("oos_days")

            fixed_params_payload["dateFilter"] = True
            if not fixed_params_payload.get("start"):
                fixed_params_payload["start"] = user_start.isoformat()
            fixed_params_payload["end"] = period_dates["is_end"].isoformat()

        fixed_params_payload = config_payload.get("fixed_params") or {}
        is_start_date = fixed_params_payload.get("start")
        is_end_date = fixed_params_payload.get("end")

        try:
            worker_processes_raw = config_payload.get("worker_processes")
            if worker_processes_raw is None:
                worker_processes_raw = config_payload.get("workerProcesses")
            if worker_processes_raw is None:
                worker_processes = 6
            else:
                try:
                    worker_processes = int(worker_processes_raw)
                except (TypeError, ValueError):
                    return ("Invalid worker process count.", HTTPStatus.BAD_REQUEST)
                if worker_processes < 1:
                    worker_processes = 1
                elif worker_processes > 32:
                    worker_processes = 32

            optimization_config = _build_optimization_config(
                data_source,
                config_payload,
                worker_processes,
                strategy_id,
                warmup_bars,
            )
        except ValueError as exc:
            _set_optimization_state({
                "status": "error",
                "mode": "optuna",
                "run_id": run_id,
                "error": str(exc),
            })
            return (str(exc), HTTPStatus.BAD_REQUEST)
        except Exception:  # pragma: no cover - defensive
            _set_optimization_state({
                "status": "error",
                "mode": "optuna",
                "run_id": run_id,
                "error": "Failed to prepare optimization config.",
            })
            app.logger.exception("Failed to construct optimization config")
            return ("Failed to prepare optimization config.", HTTPStatus.INTERNAL_SERVER_ERROR)

        optimization_config.csv_original_name = source_name
        optimization_config.ft_enabled = ft_enabled
        if ft_enabled:
            optimization_config.ft_period_days = ft_days
            optimization_config.ft_top_k = int(post_process_payload.get("topK", 10))
            optimization_config.ft_sort_metric = post_process_payload.get("sortMetric", "profit_degradation")
            optimization_config.ft_start_date = ft_start.strftime("%Y-%m-%d") if ft_start else None
            optimization_config.ft_end_date = ft_end.strftime("%Y-%m-%d") if ft_end else None
        if ft_enabled or oos_enabled:
            optimization_config.is_period_days = is_days

        db_apply_error = _apply_db_target_from_form(request.form)
        if db_apply_error:
            return db_apply_error

        _set_optimization_state({
            "status": "running",
            "mode": "optuna",
            "run_id": run_id,
            "strategy_id": optimization_config.strategy_id,
            "data_path": data_path,
            "source_name": source_name,
            "warmup_bars": warmup_bars,
            "config": config_payload,
        })

        results: List[OptimizationResult] = []
        optimization_metadata: Optional[Dict[str, Any]] = None
        study_id: Optional[str] = None
        all_results: List[OptimizationResult] = []

        def _finalize_cancelled_optuna_run(study_to_cleanup: Optional[str]) -> object:
            if study_to_cleanup:
                try:
                    delete_study(study_to_cleanup)
                except Exception:  # pragma: no cover - defensive
                    app.logger.exception("Failed to cleanup cancelled study %s", study_to_cleanup)
            _clear_cancelled_run(run_id)
            _set_optimization_state(
                {
                    "status": "cancelled",
                    "mode": "optuna",
                    "run_id": run_id,
                    "strategy_id": optimization_config.strategy_id,
                    "data_path": data_path,
                    "source_name": source_name,
                    "warmup_bars": optimization_config.warmup_bars,
                    "config": config_payload,
                    "study_id": None,
                }
            )
            return jsonify(
                {
                    "status": "cancelled",
                    "mode": "optuna",
                    "run_id": run_id,
                    "study_id": None,
                    "strategy_id": optimization_config.strategy_id,
                    "data_path": data_path,
                    "active_db": get_active_db_name(),
                }
            )

        try:
            start_time = time.time()
            results, study_id = run_optimization(optimization_config)
            if _is_run_cancelled(run_id):
                return _finalize_cancelled_optuna_run(study_id)
            all_results = list(getattr(optimization_config, "optuna_all_results", []))
            end_time = time.time()

            optimization_time_seconds = max(0.0, end_time - start_time)
            minutes = int(optimization_time_seconds // 60)
            seconds = int(optimization_time_seconds % 60)
            optimization_time_str = f"{minutes}m {seconds}s"

            summary = getattr(optimization_config, "optuna_summary", {})
            total_trials = int(summary.get("total_trials", getattr(optimization_config, "optuna_n_trials", 0)))
            completed_trials = int(summary.get("completed_trials", len(results)))
            pruned_trials = int(summary.get("pruned_trials", 0))
            best_value = summary.get("best_value")
            best_values = summary.get("best_values")

            if best_value is None and best_values is None and results:
                best_result = results[0]
                if getattr(best_result, "objective_values", None):
                    if len(best_result.objective_values) > 1:
                        best_values = dict(
                            zip(
                                getattr(optimization_config, "objectives", []) or [],
                                best_result.objective_values,
                            )
                        )
                    else:
                        best_value = best_result.objective_values[0]

            best_value_str = "-"
            if best_values:
                parts = []
                for metric, value in best_values.items():
                    label = OBJECTIVE_DISPLAY_NAMES.get(metric, metric)
                    try:
                        formatted = f"{float(value):.4f}"
                    except (TypeError, ValueError):
                        formatted = str(value)
                    parts.append(f"{label}={formatted}")
                best_value_str = ", ".join(parts) if parts else "-"
            elif best_value is not None:
                try:
                    best_value_str = f"{float(best_value):.4f}"
                except (TypeError, ValueError):
                    best_value_str = str(best_value)

            objectives = getattr(optimization_config, "objectives", []) or []
            primary_objective = getattr(optimization_config, "primary_objective", None)
            objective_label = (
                OBJECTIVE_DISPLAY_NAMES.get(objectives[0], objectives[0])
                if len(objectives) == 1
                else "Multi-objective"
            )

            optimization_metadata = {
                "method": "Optuna",
                "target": objective_label,
                "objectives": objectives,
                "primary_objective": primary_objective,
                "total_trials": total_trials,
                "completed_trials": completed_trials,
                "pruned_trials": pruned_trials,
                "best_trial_number": summary.get("best_trial_number"),
                "best_value": best_value_str,
                "pareto_front_size": summary.get("pareto_front_size"),
                "optimization_time": optimization_time_str,
            }
        except ValueError as exc:
            _set_optimization_state({
                "status": "error",
                "mode": "optuna",
                "run_id": run_id,
                "strategy_id": optimization_config.strategy_id,
                "error": str(exc),
            })
            return (str(exc), HTTPStatus.BAD_REQUEST)
        except Exception:  # pragma: no cover - defensive
            _set_optimization_state({
                "status": "error",
                "mode": "optuna",
                "run_id": run_id,
                "strategy_id": optimization_config.strategy_id,
                "error": "Optimization execution failed.",
            })
            app.logger.exception("Optimization run failed")
            return ("Optimization execution failed.", HTTPStatus.INTERNAL_SERVER_ERROR)

        if _is_run_cancelled(run_id):
            return _finalize_cancelled_optuna_run(study_id)

        if study_id:
            study_data = load_study_from_db(study_id) or {}
            config_json = (study_data.get("study") or {}).get("config_json") or {}
            if post_process_payload:
                config_json["postProcess"] = post_process_payload
            if oos_payload:
                config_json["oosTest"] = oos_payload
            if post_process_payload or oos_payload:
                update_study_config_json(study_id, config_json)

        dsr_results: List[Any] = []
        if _is_run_cancelled(run_id):
            return _finalize_cancelled_optuna_run(study_id)
        if dsr_enabled and study_id:
            dsr_config = DSRConfig(
                enabled=True,
                top_k=dsr_top_k,
                warmup_bars=warmup_bars,
            )
            dsr_results, dsr_summary = run_dsr_analysis(
                optuna_results=results,
                all_results=all_results or results,
                config=dsr_config,
                n_trials_total=completed_trials,
                csv_path=data_path,
                strategy_id=strategy_id,
                fixed_params=config_payload.get("fixed_params") or {},
                warmup_bars=warmup_bars,
                score_config=getattr(optimization_config, "score_config", None),
                filter_min_profit=bool(getattr(optimization_config, "filter_min_profit", False)),
                min_profit_threshold=float(getattr(optimization_config, "min_profit_threshold", 0.0) or 0.0),
            )
            save_dsr_results(
                study_id,
                dsr_results,
                dsr_enabled=True,
                dsr_top_k=dsr_top_k,
                dsr_n_trials=dsr_summary.get("dsr_n_trials"),
                dsr_mean_sharpe=dsr_summary.get("dsr_mean_sharpe"),
                dsr_var_sharpe=dsr_summary.get("dsr_var_sharpe"),
            )

        ft_results: List[Any] = []
        if _is_run_cancelled(run_id):
            return _finalize_cancelled_optuna_run(study_id)
        if ft_enabled and study_id:
            ft_candidates = results
            if dsr_results:
                ft_candidates = [item.original_result for item in dsr_results]
            ft_source = "dsr" if dsr_results else "optuna"

            pp_config = PostProcessConfig(
                enabled=True,
                ft_period_days=int(ft_days or 0),
                top_k=int(post_process_payload.get("topK", 10)),
                sort_metric=str(post_process_payload.get("sortMetric", "profit_degradation")),
                warmup_bars=warmup_bars,
            )
            ft_results = run_forward_test(
                csv_path=data_path,
                strategy_id=strategy_id,
                optuna_results=ft_candidates,
                config=pp_config,
                is_period_days=int(is_days or 0),
                ft_period_days=int(ft_days or 0),
                ft_start_date=ft_start.strftime("%Y-%m-%d") if ft_start else "",
                ft_end_date=ft_end.strftime("%Y-%m-%d") if ft_end else "",
                n_workers=worker_processes,
            )
            save_forward_test_results(
                study_id,
                ft_results,
                ft_enabled=True,
                ft_period_days=int(ft_days or 0),
                ft_top_k=int(post_process_payload.get("topK", 10)),
                ft_sort_metric=str(post_process_payload.get("sortMetric", "profit_degradation")),
                ft_start_date=ft_start.strftime("%Y-%m-%d") if ft_start else None,
                ft_end_date=ft_end.strftime("%Y-%m-%d") if ft_end else None,
                is_period_days=int(is_days or 0),
                ft_source=ft_source,
            )

        st_results: List[Any] = []
        if _is_run_cancelled(run_id):
            return _finalize_cancelled_optuna_run(study_id)
        if st_enabled and study_id:
            try:
                from strategies import get_strategy_config

                strategy_config_json = get_strategy_config(strategy_id)
            except Exception as exc:
                strategy_config_json = {}
                app.logger.warning("Failed to load strategy config for stress test: %s", exc)

            try:
                st_top_k = int(st_payload.get("topK", 5))
            except (TypeError, ValueError):
                st_top_k = 5
            try:
                threshold_raw = float(st_payload.get("failureThreshold", 0.7))
            except (TypeError, ValueError):
                threshold_raw = 0.7
            failure_threshold = threshold_raw / 100.0 if threshold_raw > 1 else threshold_raw

            stress_test_config = StressTestConfig(
                enabled=True,
                top_k=st_top_k,
                failure_threshold=failure_threshold,
                sort_metric=str(st_payload.get("sortMetric", "profit_retention")),
                warmup_bars=warmup_bars,
            )

            st_candidates = results
            st_source = "optuna"
            if ft_enabled and ft_results:
                st_candidates = ft_results
                st_source = "ft"
            elif dsr_results:
                st_candidates = dsr_results
                st_source = "dsr"

            st_results, st_summary = run_stress_test(
                csv_path=data_path,
                strategy_id=strategy_id,
                source_results=st_candidates,
                config=stress_test_config,
                is_start_date=is_start_date,
                is_end_date=is_end_date,
                fixed_params=fixed_params_payload,
                config_json=strategy_config_json,
                n_workers=worker_processes,
            )
            save_stress_test_results(
                study_id,
                st_results,
                st_summary,
                stress_test_config,
                st_source=st_source,
            )

        oos_results_payload: List[Dict[str, Any]] = []
        if _is_run_cancelled(run_id):
            return _finalize_cancelled_optuna_run(study_id)
        if oos_enabled and study_id:
            if not oos_start or not oos_end:
                raise ValueError("OOS Test enabled but OOS period could not be determined.")

            _set_optimization_state({
                "status": "running",
                "mode": "optuna",
                "run_id": run_id,
                "study_id": study_id,
                "stage": "oos_test",
                "message": "Running OOS Test...",
            })

            source_module, candidates = select_oos_source_candidates(
                optuna_results=results,
                dsr_results=dsr_results,
                ft_results=ft_results,
                st_results=st_results,
                st_ran=bool(st_enabled),
            )

            if oos_top_k:
                candidates = candidates[: int(oos_top_k)]

            if not candidates:
                if source_module == "stress_test":
                    raise ValueError("Stress Test produced no OK candidates; OOS Test skipped.")
                raise ValueError("No candidates available for OOS Test.")

            study_data = load_study_from_db(study_id) or {}
            trial_rows = study_data.get("trials") or []
            trial_map = {int(t.get("trial_number")): t for t in trial_rows if t.get("trial_number") is not None}

            trials_to_test: List[Dict[str, Any]] = []
            source_rank_map: Dict[int, int] = {}
            for candidate in candidates:
                trial_number = int(candidate.get("trial_number") or 0)
                if trial_number <= 0:
                    continue
                trial = trial_map.get(trial_number)
                if not trial:
                    continue
                source_rank_map[trial_number] = int(candidate.get("source_rank") or len(source_rank_map) + 1)
                trials_to_test.append(trial)

            if not trials_to_test:
                raise ValueError("No matching trials found for OOS Test candidates.")

            try:
                df_oos = load_data(data_path)
            except Exception as exc:
                raise ValueError(f"Failed to load CSV for OOS Test: {exc}") from exc

            aligned_start, aligned_end = align_date_bounds(df_oos.index, oos_start, oos_end)
            if aligned_start is None or aligned_end is None:
                raise ValueError("Invalid OOS Test date range.")

            oos_start_ts = aligned_start
            oos_end_ts = aligned_end
            test_period_days = max(0, (oos_end_ts - oos_start_ts).days)
            if test_period_days <= 0:
                raise ValueError("OOS Test period must be at least 1 day.")

            baseline_period_days = None
            if source_module == "forward_test":
                baseline_period_days = int(ft_days or 0)
            if baseline_period_days is None or baseline_period_days <= 0:
                baseline_period_days = int(is_days or 0)
            if baseline_period_days <= 0:
                baseline_period_days = calculate_is_period_days(config_payload) or 0

            def resolve_original_metrics(trial: Dict[str, Any]) -> Dict[str, Any]:
                if source_module == "forward_test":
                    return _build_trial_metrics(trial, prefix="ft_")
                return _build_trial_metrics(trial)

            oos_results_payload = run_period_test_for_trials(
                df=df_oos,
                strategy_id=strategy_id,
                warmup_bars=int(warmup_bars),
                fixed_params=fixed_params_payload,
                start_ts=oos_start_ts,
                end_ts=oos_end_ts,
                trials=trials_to_test,
                baseline_period_days=int(baseline_period_days),
                test_period_days=int(test_period_days),
                original_metrics_resolver=resolve_original_metrics,
            )

            for idx, item in enumerate(oos_results_payload, 1):
                trial_number = int(item.get("trial_number") or 0)
                item["oos_test_source"] = source_module
                item["oos_test_source_rank"] = source_rank_map.get(trial_number) or idx

            save_oos_test_results(
                study_id,
                oos_results_payload,
                oos_enabled=True,
                oos_period_days=int(oos_days or oos_period_days),
                oos_top_k=int(oos_top_k),
                oos_start_date=oos_start_ts.strftime("%Y-%m-%d"),
                oos_end_date=oos_end_ts.strftime("%Y-%m-%d"),
                oos_source_module=source_module,
            )

        if _is_run_cancelled(run_id):
            return _finalize_cancelled_optuna_run(study_id)

        _clear_cancelled_run(run_id)
        _set_optimization_state(
            {
                "status": "completed",
                "mode": "optuna",
                "run_id": run_id,
                "strategy_id": optimization_config.strategy_id,
                "data_path": data_path,
                "source_name": source_name,
                "warmup_bars": optimization_config.warmup_bars,
                "config": config_payload,
                "summary": optimization_metadata or {},
                "study_id": study_id,
            }
        )

        return jsonify(
            {
                "status": "success",
                "mode": "optuna",
                "run_id": run_id,
                "study_id": study_id,
                "summary": optimization_metadata or {},
                "strategy_id": optimization_config.strategy_id,
                "data_path": data_path,
                "active_db": get_active_db_name(),
            }
        )


    # ============================================
    # STRATEGY MANAGEMENT ENDPOINTS
    # ============================================


