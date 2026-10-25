"""Source admission and pack browsing for Merlin routes."""
from pathlib import Path
import json
import re

import pandas as pd

from core.backtest_engine import prepare_dataset_with_warmup

from core.data_input import (
    DataInputError, normalize_source, prepare_input, replay_input, validate_pin,
    inspect_pack, is_pack_directory,
)

DEFAULT_PARQUET_ROOT = Path(__file__).resolve().parents[2] / "data" / "parquet-data"


def allowed_roots():
    if __package__:
        from . import server_services as services
    else:
        import server_services as services
    return [*services.CSV_ALLOWED_ROOTS, DEFAULT_PARQUET_ROOT.resolve()]


def resolve_root(raw, *, must_exist=True):
    if __package__:
        from .server_services import _is_path_within_root
    else:
        from server_services import _is_path_within_root
    root = Path(raw).expanduser()
    if not root.is_absolute():
        raise DataInputError("Parquet directory must be an absolute path.")
    root = root.resolve(strict=must_exist)
    if not any(_is_path_within_root(root, allowed.resolve()) for allowed in allowed_roots()):
        raise PermissionError("Parquet directory is outside allowed roots.")
    if must_exist and not root.is_dir():
        raise DataInputError("Parquet pack root must be a directory.")
    return root


def request_source(values):
    if "source" not in values:
        return None
    raw = values["source"]
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError as exc:
            raise DataInputError("Invalid source JSON.") from exc
    return normalize_source(raw)


def prepare_request(values, strategy_context, params, warmup_bars, *, cancelled=None):
    source = request_source(values)
    if source is None:
        return None
    if not strategy_context.is_v2:
        raise DataInputError("Parquet inputs require Backtester V2. Switch engine or use CSV.")
    source["pack_root"] = str(resolve_root(source["pack_root"]))
    return prepare_input(source, date_filter=bool(params.get("dateFilter", False)),
                         start=params.get("start"), end=params.get("end"),
                         warmup_bars=warmup_bars, cancelled=cancelled)


def effective_params(params, prepared):
    result = dict(params)
    bounds = prepared.facts["bounds"]
    result.update(dateFilter=True, start=bounds["evaluation_start"], end=bounds["evaluation_end"])
    return result


def replay_config(config, *, cancelled=None):
    if "data_input" not in config:
        return None
    source = validate_pin(config["data_input"])
    root = resolve_root(config.get("data_input_root_override", source["pack_root"]))
    return replay_input(config["data_input"], root_override=str(root), cancelled=cancelled)


def execution_input(study, manual=None):
    if __package__:
        from .server_services import _resolve_csv_path
    else:
        from server_services import _resolve_csv_path
    config = study.get("config_json") or {}
    if manual is not None:
        config = (manual.get("results_json") or {}).get("config") or {}
    prepared = replay_config(config)
    if prepared is not None:
        return "", prepared
    if manual is not None:
        if manual.get("data_source") == "original_parquet":
            raise DataInputError("Manual Parquet test is missing its saved input pin.")
        path = manual.get("csv_path")
        if not path and manual.get("data_source") == "original_csv":
            if "data_input" in (study.get("config_json") or {}):
                raise DataInputError("Manual test has no own input pin; CSV fallback is unavailable.")
            path = study.get("csv_file_path")
    else:
        path = study.get("csv_file_path")
    return str(_resolve_csv_path(path)), None


def replay_oos_input(study, trial):
    """Reconstruct only the recorded OOS preparation, inside the original input pin."""
    if trial.get("oos_test_total_trades") is None or trial.get("oos_test_net_profit_pct") is None:
        raise DataInputError("OOS result is unavailable for this trial.")
    config = study.get("config_json") or {}
    if "parquet_oos_replay" not in config:
        raise DataInputError("This study predates exact OOS replay metadata; a new study is required for OOS reconstruction.")
    facts = config["parquet_oos_replay"]
    try:
        if not isinstance(facts, dict) or type(facts.get("version")) is not int or facts["version"] != 1:
            raise ValueError("unsupported or missing version")
        dates = {}
        for key in ("start", "end", "preparation_start", "preparation_end"):
            value = facts[key]
            if not isinstance(value, str) or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|\+00:00)", value
            ):
                raise ValueError(f"{key} must be a full UTC timestamp")
            dates[key] = pd.Timestamp(value)
        for key in ("trade_start_idx", "warmup_bars"):
            if type(facts[key]) is not int or facts[key] < 0:
                raise ValueError(f"{key} must be a nonnegative integer")
        if dates["start"] > dates["end"] or dates["preparation_start"] > dates["preparation_end"]:
            raise ValueError("timestamps are out of order")
        validate_pin(config["data_input"])
        bounds = config["data_input"]["bounds"]
        if not (pd.Timestamp(bounds["evaluation_start"]) <= dates["start"] <= dates["end"]
                <= pd.Timestamp(bounds["evaluation_end"])
                and pd.Timestamp(bounds["warmup_start"]) <= dates["preparation_start"]
                <= dates["preparation_end"] < pd.Timestamp(bounds["end"])):
            raise ValueError("OOS preparation is outside the parent input pin")
    except (KeyError, TypeError, ValueError) as exc:
        raise DataInputError(f"Invalid parquet_oos_replay: {exc}. OOS reconstruction is blocked.") from exc
    prepared = replay_config(config)
    frame, idx = prepare_dataset_with_warmup(
        prepared.frame, dates["start"], dates["end"], facts["warmup_bars"]
    )
    if (frame.empty or idx != facts["trade_start_idx"] or not 0 <= idx < len(frame)
            or frame.index[0] != dates["preparation_start"]
            or frame.index[-1] != dates["preparation_end"]):
        raise DataInputError("OOS preparation conflicts with saved parquet_oos_replay facts; reconstruction refused.")
    return frame, idx, dates


def browse(raw):
    root = resolve_root(raw or str(DEFAULT_PARQUET_ROOT))
    # A pack is selected through its manifest, never by descending into its files.
    metadata = inspect_pack(root) if is_pack_directory(root) else None
    entries = []
    for child in (() if metadata is not None else root.iterdir()):
        if not child.is_dir():
            continue
        try:
            resolved = resolve_root(str(child))
        except (PermissionError, OSError, ValueError):
            continue
        entries.append({"name": child.name, "path": str(resolved), "kind": "dir"})
    parent = None
    if root.parent != root:
        try:
            parent = str(resolve_root(str(root.parent)))
        except (PermissionError, OSError, ValueError):
            pass
    return {"current_path": str(root), "parent_path": parent,
            "entries": sorted(entries, key=lambda item: item["name"].lower()),
            "pack": metadata, "default_root": str(DEFAULT_PARQUET_ROOT),
            "allowed_roots": [str(p) for p in allowed_roots()]}


def register_routes(app):
    from flask import jsonify, request
    from core import storage
    if __package__:
        from .server_services import _resolve_stored_execution_context, _run_equity_export
    else:
        from server_services import _resolve_stored_execution_context, _run_equity_export

    @app.post("/api/studies/<study_id>/locate-parquet-pack")
    def locate_pack(study_id):
        saved = storage.load_study_from_db(study_id)
        if not saved:
            return jsonify(error="Study not found."), 404
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify(error="Expected a JSON object with packRoot and optional positive testId."), 400
        test_id = body.get("testId")
        if test_id is not None:
            if isinstance(test_id, str) and re.fullmatch(r"[0-9]+", test_id):
                # SQLite IDs cannot exceed a signed 64-bit integer.
                digits = test_id.lstrip("0")
                test_id = int(digits) if len(digits) <= 19 and digits else 0
                if len(digits) > 19:
                    return jsonify(error="Manual test not found."), 404
            if type(test_id) is not int or test_id <= 0:
                return jsonify(error="testId must be a positive integer or digit-only string."), 400
            if test_id > 2**63 - 1:
                return jsonify(error="Manual test not found."), 404
        config = saved["study"].get("config_json") or {}
        manual = None
        if test_id is not None:
            manual = storage.load_manual_test_results(study_id, test_id)
            if not manual:
                return jsonify(error="Manual test not found."), 404
            config = (manual.get("results_json") or {}).get("config") or {}
        try:
            if "data_input" not in config:
                raise DataInputError("This result has no Parquet input pin.")
            root = resolve_root(body.get("packRoot", ""))
            replay_input(config["data_input"], root_override=str(root))
            updated = {**config, "data_input_root_override": str(root)}
            if manual is None:
                storage.update_study_config_json(study_id, updated)
            else:
                results = {**manual["results_json"], "config": updated}
                with storage.get_db_connection() as conn:
                    conn.execute("UPDATE manual_tests SET results_json = ? WHERE study_id = ? AND id = ?",
                                 (json.dumps(results), study_id, manual["id"]))
                    conn.commit()
        except (ValueError, OSError) as exc:
            return jsonify(error=str(exc)), 400
        return jsonify(status="updated", pack_root=str(root))

    @app.post("/api/studies/<study_id>/trials/<int:trial_number>/equity")
    def trial_equity(study_id, trial_number):
        saved = storage.load_study_from_db(study_id)
        if not saved:
            return jsonify(error="Study not found."), 404
        study = saved["study"]
        trial = storage.get_study_trial(study_id, trial_number)
        if not trial or study.get("optimization_mode") not in {"grid", "optuna"}:
            return jsonify(error="Trial not found."), 404
        body = request.get_json(silent=True) or {}
        module = body.get("module", "optuna")
        manual = None
        start = end = None
        try:
            context = _resolve_stored_execution_context(study)
            if module == "forward_test":
                if not study.get("ft_enabled") or not trial.get("ft_rank"):
                    raise DataInputError("Forward test result is unavailable.")
                start, end = study.get("ft_start_date"), study.get("ft_end_date")
            elif module == "oos_test":
                if not study.get("oos_test_enabled"):
                    raise DataInputError("OOS result is unavailable.")
                start, end = study.get("oos_test_start_date"), study.get("oos_test_end_date")
            elif module == "manual_tests":
                manual = storage.load_manual_test_results(study_id, body.get("testId"))
                if not manual:
                    raise DataInputError("Manual test not found.")
                results = manual.get("results_json") or {}
                selected = next((r for r in results.get("results", []) if r.get("trial_number") == trial_number), None)
                if selected is None:
                    raise DataInputError("Trial is absent from this manual test.")
                start, end = manual.get("start_date"), manual.get("end_date")
            elif module not in {"optuna", "grid", "dsr", "stress_test"}:
                raise DataInputError("Unknown result module.")
            export_options = {}
            if module == "oos_test" and "data_input" in (study.get("config_json") or {}):
                frame, idx, dates = replay_oos_input(study, trial)
                csv_path = ""
                start, end = dates["start"], dates["end"]
                export_options = {"prepared_frame": frame, "prepared_trade_start_idx": idx}
            else:
                csv_path, prepared = execution_input(study, manual)
                if prepared is not None:
                    export_options = {"prepared_frame": prepared.frame}
            params = context.params_for(trial.get("params") or {}, operation_start=start, operation_end=end)
            equity, timestamps, error = _run_equity_export(
                strategy_id=study["strategy_id"], csv_path=csv_path, params=params,
                warmup_bars=context.warmup_bars,
                **export_options)
            if error:
                raise DataInputError(error)
        except (ValueError, TypeError, OSError) as exc:
            return jsonify(error=str(exc)), 400
        return jsonify(equity=equity, timestamps=timestamps)
