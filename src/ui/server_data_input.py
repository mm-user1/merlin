"""Source admission and pack browsing for Merlin routes."""
from pathlib import Path
import json
import re

import pandas as pd

from core.backtest_engine import prepare_dataset_with_warmup
from core.entry_filters import MISSING, capability, effective_config, source_locator
from core.entry_filter_input import metadata_entry, saved_effective

from core.data_input import (
    DataInputError, normalize_source, prepare_input, replay_input, validate_pin,
    inspect_pack, is_pack_directory, preview_admission_rules,
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


def request_filters(values, strategy_context):
    raw = values.get("entryFilters", MISSING)
    if raw is MISSING and "config" in values:
        config = values["config"]
        config = json.loads(config) if isinstance(config, str) else config
        if isinstance(config, dict):
            raw = config.get("entryFilters", MISSING)
    raw = json.loads(raw) if isinstance(raw, str) else raw
    effective = effective_config(raw)
    methods = capability(strategy_context.config)
    if effective and not methods:
        raise DataInputError("This strategy does not support Entry Filters; turn them Off or select a capable strategy.")
    if "entryFilterInputs" in values and not methods:
        raise DataInputError("This strategy does not support entryFilterInputs.")
    for item in effective:
        resolve_root(item["source"]["pack_root"])
    return effective if methods else MISSING


def prepare_request(values, strategy_context, params, warmup_bars, *, cancelled=None):
    from core.engine_v2.admission import uses_admission, execution_admission
    admission = uses_admission(strategy_context.config)
    filters = request_filters(values, strategy_context)
    source = request_source(values)
    if source is None:
        if admission:
            raise DataInputError("risk_bracket_v1 requires a Parquet source with instrument rules; CSV is unsupported.")
        return None
    if not strategy_context.is_v2:
        raise DataInputError("Parquet inputs require Backtester V2. Switch engine or use CSV.")
    source["pack_root"] = str(resolve_root(source["pack_root"]))
    prepared = prepare_input(source, date_filter=bool(params.get("dateFilter", False)),
                         start=params.get("start"), end=params.get("end"),
                         warmup_bars=warmup_bars, cancelled=cancelled,
                         admission_params=params if admission else None, entry_filters=filters)
    if admission:
        execution_admission(strategy_context.profile, params, prepared.admission_rules)
    return prepared


def preview_request_rules(values, strategy_context, params, *, cancelled=None):
    from core.engine_v2.admission import admission_cap, execution_admission
    admission_cap(strategy_context.profile, params)
    source = request_source(values)
    if source is None:
        raise DataInputError("risk_bracket_v1 requires a Parquet source with instrument rules; CSV is unsupported.")
    source["pack_root"] = str(resolve_root(source["pack_root"]))
    filters = request_filters(values, strategy_context)
    from core.data_input import pack_session, _admission_rules
    groups = {}
    for item in (() if filters is MISSING else filters):
        root = source_locator(str(resolve_root(item["source"]["pack_root"])))
        groups.setdefault(root, []).append(item["source"])
    with pack_session(source["pack_root"], cancelled=cancelled) as session:
        metadata_entry(session, source)
        rules = _admission_rules(session, source["instrument_id"])
        for binding in groups.pop(source_locator(source["pack_root"]), []):
            metadata_entry(session, binding)
    for root, bindings in groups.items():
        with pack_session(root, cancelled=cancelled) as session:
            for binding in bindings:
                metadata_entry(session, binding)
    execution_admission(strategy_context.profile, params, rules)
    return rules


def effective_params(params, prepared):
    result = dict(params)
    bounds = prepared.facts["bounds"]
    result.update(dateFilter=True, start=bounds["evaluation_start"], end=bounds["evaluation_end"])
    if prepared.entry_admission is not None:
        result["maxEntryLeverage"] = prepared.entry_admission["maxEntryLeverage"]
    return result


def replay_config(config, *, cancelled=None):
    if "data_input" not in config:
        return None
    source = validate_pin(config["data_input"])
    root = resolve_root(config.get("data_input_root_override", source["pack_root"]))
    options = {}
    if "entryFilters" in config or "entry_filter_inputs" in config:
        effective = saved_effective(config)
        overrides = config.get("entry_filter_root_overrides", {})
        if not isinstance(overrides, dict):
            raise DataInputError("Invalid filter root overrides.")
        for item in effective:
            original = item["source"]["pack_root"]
            resolve_root(overrides.get(original, original))
        options = dict(entry_filters=effective, entry_filter_inputs=config["entry_filter_inputs"],
                       filter_root_overrides=overrides)
    return replay_input(config["data_input"], root_override=str(root), cancelled=cancelled,
                        entry_admission=config.get("entry_admission"),
                        admission_cap=(config.get("fixed_params") or {}).get("maxEntryLeverage"), **options)


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
    return frame, idx, dates, prepared.admission_rules, prepared.entry_filter_context


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
            raw_root = body.get("packRoot")
            if not isinstance(raw_root, str) or not raw_root.strip():
                raise DataInputError("packRoot must be a nonblank path string.")
            root = resolve_root(raw_root)
            dependency = body.get("dependency", "target")
            if dependency == "target":
                replay_input(config["data_input"], root_override=str(root),
                    entry_admission=config.get("entry_admission"),
                    admission_cap=(config.get("fixed_params") or {}).get("maxEntryLeverage"))
                updated = {**config, "data_input_root_override": str(root)}
            else:
                from core.entry_filter_input import verify_relocation
                original = source_locator(dependency)
                verify_relocation(config, original, root)
                overrides = {**config.get("entry_filter_root_overrides", {}), original: str(root)}
                updated = {**config, "entry_filter_root_overrides": overrides}
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
        body = request.get_json(silent=True) or {}
        owned_manual = (study.get('config_json') or {}).get('portfolio') and study.get('optimization_mode') == 'wfa' and body.get('module') == 'manual_tests'
        trial = storage.get_study_trial(study_id, trial_number)
        if not owned_manual and (not trial or study.get("optimization_mode") not in {"grid", "optuna"}):
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
                if owned_manual:
                    trial = selected
                start, end = manual.get("start_date"), manual.get("end_date")
            elif module not in {"optuna", "grid", "dsr", "stress_test"}:
                raise DataInputError("Unknown result module.")
            export_options = {}
            if module == "oos_test" and "data_input" in (study.get("config_json") or {}):
                frame, idx, dates, rules, filters = replay_oos_input(study, trial)
                csv_path = ""
                start, end = dates["start"], dates["end"]
                export_options = {"prepared_frame": frame, "prepared_trade_start_idx": idx, "admission_rules": rules, "entry_filter_context": filters}
            else:
                csv_path, prepared = execution_input(study, manual)
                if prepared is not None:
                    export_options = {"prepared_frame": prepared.frame, "admission_rules": prepared.admission_rules, "entry_filter_context": prepared.entry_filter_context}
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
