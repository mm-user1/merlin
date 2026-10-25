"""Source admission and pack browsing for Merlin routes."""
from pathlib import Path
import json

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
        body = request.get_json(silent=True) or {}
        config = saved["study"].get("config_json") or {}
        manual = None
        if body.get("testId") is not None:
            manual = storage.load_manual_test_results(study_id, body["testId"])
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
            csv_path, prepared = execution_input(study, manual)
            params = context.params_for(trial.get("params") or {}, operation_start=start, operation_end=end)
            equity, timestamps, error = _run_equity_export(
                strategy_id=study["strategy_id"], csv_path=csv_path, params=params,
                warmup_bars=context.warmup_bars,
                **({"prepared_frame": prepared.frame} if prepared is not None else {}))
            if error:
                raise DataInputError(error)
        except (ValueError, TypeError, OSError) as exc:
            return jsonify(error=str(exc)), 400
        return jsonify(equity=equity, timestamps=timestamps)
