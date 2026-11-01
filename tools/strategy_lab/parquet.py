"""Sequential Parquet generation with consumption-time replay and portable pins."""
from __future__ import annotations

from copy import deepcopy
import importlib
from pathlib import Path
import platform
import time
from types import SimpleNamespace

import numba
import numpy as np
import pandas as pd

from .config import canonical_sha256, semantic_key_digest
from .config_v2 import build_plan, calendar
from .data_quality import prepare_segment, quality_row
from .dataset import (DatasetError, METRIC_AXIS_V3, SEGMENT_AXIS, artifact_record,
    atomic_write_group, atomic_write_json, atomic_write_quality_csv, group_matrix_v3,
    group_record, load_json, project_candidates, validate_artifact_record)
from core import data_input
from core.engine_v2.admission import read_admission_envelope
from core.grid_v2 import GridV2StrategyHooks, execute_grid_v2_candidates
from strategies import get_strategy

SCHEMA = "strategy_lab_dataset_v3"
SUPPORTING = ("normalized_runspec.json", "candidates.json", "input_bindings.json", "data_quality.csv")


def geometry(plan, versions):
    payload = project_candidates(plan, versions)
    payload["schema_version"] = "strategy_lab_candidates_v2"
    del payload["plan_fingerprint"]
    return payload


def portable_pin(facts):
    pin = deepcopy(facts)
    del pin["source"]["pack_root"]
    return pin


def replay(binding, root):
    """Reconstruct only the host locator; retained root/provenance never changes."""
    pin = deepcopy(binding["input_pin"])
    pin["source"]["pack_root"] = str(Path(root).resolve())
    return data_input.replay_input(pin, entry_admission=binding["entry_admission"],
        admission_cap=binding["entry_admission"]["maxEntryLeverage"],
        entry_filters=[], entry_filter_inputs=binding["entry_filter_inputs"])


def semantic_binding(binding):
    envelope = binding["entry_admission"]
    rules = read_admission_envelope(envelope, envelope["maxEntryLeverage"])
    return {key: binding[key] for key in ("instrument_id", "cell", "input_pin", "plan_fingerprint", "semantic_key_digest", "segments")} | {
        "admission": {"version": envelope["version"], "mode": envelope["mode"],
            "maxEntryLeverage": envelope["maxEntryLeverage"], "rules": rules.semantic()},
        "entry_filter_inputs": binding["entry_filter_inputs"],
    }


def segment(prepared, entry, window, name, spec):
    w = spec.generation["windows"]
    source = SimpleNamespace(dataframe=prepared.frame, entry={"canonical_symbol": entry["instrument_id"]})
    return prepare_segment(source, window, name, warmup_bars=w["warmup_bars"],
        timeframe_minutes=w["timeframe_minutes"], oos_period_months=w["oos_period_months"])


def segment_facts(prepared, timeframe):
    return {"window_id": prepared.window_id, "segment": prepared.segment,
        "first_timestamp": prepared.first_timestamp, "last_timestamp": prepared.last_timestamp,
        "coverage_end": (prepared.dataframe.index[-1] + pd.Timedelta(minutes=timeframe)).isoformat(),
        "warmup_origin": prepared.dataframe.index[0].isoformat(),
        "trade_start_idx": prepared.trade_start_idx, "evaluation_row_count": prepared.evaluation_row_count}


def identity(spec, entries, windows, candidates, bindings):
    from .generate import _window_payload
    count = candidates["candidate_count"]
    return {"dataset_schema": SCHEMA, "run_name": spec.raw["run_name"],
        "generation_sha256": spec.generation_sha256, "pre_registration_sha256": spec.pre_registration_sha256,
        "roster_sha256": canonical_sha256(spec.generation["market_data"]["instruments"]),
        "candidate_count": count, "candidate_axis": list(range(1, count + 1)),
        "geometry_sha256": canonical_sha256(candidates), "semantic_key_digest": candidates["semantic_key_digest"],
        "input_bindings_sha256": canonical_sha256([semantic_binding(b) for b in bindings["instruments"]]),
        "included_tickers": [{"canonical_symbol": e["instrument_id"], "cell": e["cell"]} for e in entries],
        "windows": [_window_payload(w) for w in windows], "segment_axis": list(SEGMENT_AXIS),
        "metric_axis": list(METRIC_AXIS_V3), "dtype": "float64", "expected_group_shape": [count, 2, len(METRIC_AXIS_V3)],
        "metric_requests": {"compute_max_drawdown_mtm": True}, "versions": spec.generation["strategy"],
        "resources": {**spec.generation["resources"], "compiled_workers": 1, "slow_enrich_selected": False}}


def expected_paths(entries, windows):
    return [f"groups/{e['instrument_id']}/window_{w.window_id:02d}.npy" for e in entries for w in windows]


def check_path(output, relative):
    path = output / relative
    try:
        path.resolve().relative_to(output)
    except ValueError:
        raise DatasetError(f"artifact path escapes output: {relative}") from None
    if path.is_symlink():
        raise DatasetError(f"artifact symlink is not allowed: {relative}")
    return path


def verify_artifacts(output, manifest):
    artifacts = manifest.get("artifacts", {})
    if set(artifacts) != set(SUPPORTING):
        raise DatasetError("required supporting artifact inventory mismatch.")
    for name in SUPPORTING:
        record = artifacts[name]
        if record.get("path") != name or not validate_artifact_record(output, record):
            raise DatasetError(f"supporting artifact checksum/ownership failure: {name}")


def validate_bindings(spec, entries, windows, candidates, bindings):
    """Check frozen ownership and segment facts without a pack read or Grid plan."""
    if bindings.get("schema_version") != 1 or type(bindings.get("schema_version")) is not int:
        raise DatasetError("unsupported input bindings schema.")
    rows = bindings.get("instruments")
    if not isinstance(rows, list) or len(rows) != len(entries):
        raise DatasetError("input binding ownership mismatch.")
    timeframe = spec.generation["market_data"]["timeframe_minutes"]
    warmup = spec.generation["windows"]["warmup_bars"]
    delta = pd.Timedelta(minutes=timeframe)
    cap = spec.generation["economics"]["base_params"]["maxEntryLeverage"]
    for entry, b in zip(entries, rows):
        if (b.get("instrument_id") != entry["instrument_id"] or b.get("cell") != entry["cell"] or
            b.get("entry_filter_inputs") != {"version": 1, "state": "off"} or
            type(b["entry_filter_inputs"]["version"]) is not int):
            raise DatasetError("input binding instrument/cell/Off ownership mismatch.")
        pin = b["input_pin"]
        source = pin["source"]
        if source != {"type": "parquet", "version": 1, "instrument_id": entry["instrument_id"], "timeframe_minutes": timeframe}:
            raise DatasetError("portable source descriptor mismatch.")
        local_pin = deepcopy(pin)
        local_pin["source"]["pack_root"] = str(Path.cwd().resolve())
        data_input.validate_pin(local_pin)  # schema only; never opens the locator
        rules = read_admission_envelope(b["entry_admission"], cap)
        if rules.instrument_id != entry["instrument_id"] or pin["instrument"]["instrument_id"] != entry["instrument_id"]:
            raise DatasetError("admission rules belong to a different instrument.")
        from .config import _sha256
        _sha256(b["plan_fingerprint"], "plan_fingerprint")
        if b["semantic_key_digest"] != candidates["semantic_key_digest"]:
            raise DatasetError("binding candidate semantic digest mismatch.")
        bounds = pin["bounds"]
        if (pd.Timestamp(bounds["start"]) != pd.Timestamp(windows[0].is_start) or
            pd.Timestamp(bounds["end"]) != pd.Timestamp(windows[-1].oos_end) + delta or
            bounds["trade_start_idx"] != warmup or bounds["requested"]["warmupBars"] != warmup):
            raise DatasetError("consumed union disagrees with selected calendar/warmup.")
        expected = []
        for w in windows:
            for name in SEGMENT_AXIS:
                first, last = pd.Timestamp(getattr(w, name + "_start")), pd.Timestamp(getattr(w, name + "_end"))
                expected.append({"window_id": w.window_id, "segment": name,
                    "first_timestamp": first.isoformat().replace("+00:00", "Z"),
                    "last_timestamp": last.isoformat().replace("+00:00", "Z"),
                    "coverage_end": (last + delta).isoformat(), "warmup_origin": (first - warmup * delta).isoformat(),
                    "trade_start_idx": warmup, "evaluation_row_count": int((last-first) / delta) + 1})
        if b.get("segments") != expected:
            raise DatasetError("exact segment preparation facts disagree with calendar.")


def verified_groups(output, manifest, entries, windows, bindings, *, complete):
    from .generate import _valid_group_backend_record
    expected = expected_paths(entries, windows)
    by_id = {b["instrument_id"]: b for b in bindings["instruments"]}
    records = manifest.get("groups")
    if not isinstance(records, list):
        raise DatasetError("group inventory must be a list.")
    valid, seen = {}, set()
    for record in records:
        relative = record.get("path")
        if relative not in expected or relative in seen:
            raise DatasetError("foreign or duplicate group ownership.")
        seen.add(relative)
        instrument = relative.split("/")[1]
        binding = by_id[instrument]
        if (record.get("instrument_id") != instrument or
            record.get("plan_fingerprint") != binding["plan_fingerprint"] or
            record.get("input_binding_sha256") != canonical_sha256(semantic_binding(binding)) or
            not _valid_group_backend_record(record)):
            raise DatasetError("group identity/backend ownership mismatch.")
        check_path(output, relative)
        if validate_artifact_record(output, record, expected_shape=manifest["identity"]["expected_group_shape"], expected_dtype="float64"):
            valid[relative] = record
        elif complete:
            raise DatasetError("completed group failed checksum/shape/dtype verification.")
    if complete and list(valid) != expected:
        raise DatasetError("completed group coverage/order mismatch.")
    return valid


def generate_parquet(spec, root, output, *, ticker_selectors, window_selectors, resume,
                     preflight_only, repo, after_group, progress):
    from .generate import (GenerationResult, _selected_windows, _execution_backend_facts,
        _require_compiled_backend_available, _git_facts, _backend_summary)
    started = time.perf_counter()
    roster = spec.generation["market_data"]["instruments"]
    entries = roster
    if ticker_selectors:
        selected = [str(s).upper() for s in ticker_selectors]
        if len(set(selected)) != len(selected) or set(selected) - {e["instrument_id"] for e in roster}:
            raise DatasetError("unknown or duplicate full instrument --ticker selection.")
        entries = [e for e in roster if e["instrument_id"] in selected]
    windows = _selected_windows(calendar(spec.generation), window_selectors)
    scope = "full" if len(entries) == len(roster) and len(windows) == spec.generation["windows"]["expected_window_count"] else "smoke"
    paths = expected_paths(entries, windows)
    for relative in [*SUPPORTING, *paths, "manifest.json", "manifest.partial.json"]:
        check_path(output, relative)
    complete = (output / "manifest.json").exists()
    existing = complete or (output / "manifest.partial.json").exists()
    partial_path = output / "manifest.partial.json"
    timings = {"preflight_seconds": 0., "execution_seconds": 0.}
    if existing:
        if not complete and not resume:
            raise DatasetError("partial output exists; pass --resume.")
        manifest = load_json(output / ("manifest.json" if complete else "manifest.partial.json"), "manifest")
        if manifest.get("schema_version") != SCHEMA or manifest.get("status") != ("complete" if complete else "partial") or manifest.get("scope") != scope:
            raise DatasetError("incompatible manifest schema/status/scope.")
        verify_artifacts(output, manifest)
        saved = load_json(output / "normalized_runspec.json", "run spec")
        if saved != spec.raw:
            raise DatasetError("incompatible run identity.")
        candidates = load_json(output / "candidates.json", "candidates")
        bindings = load_json(output / "input_bindings.json", "bindings")
        validate_bindings(spec, entries, windows, candidates, bindings)
        expected_identity = identity(spec, entries, windows, candidates, bindings)
        if manifest.get("identity") != expected_identity:
            raise DatasetError("incompatible frozen dataset identity.")
        valid = verified_groups(output, manifest, entries, windows, bindings, complete=complete)
        if complete:
            for binding in bindings["instruments"]:
                prepared = replay(binding, root)
                del prepared
            return GenerationResult(output, output / "manifest.json", scope, len(paths), len(paths), 0, True,
                                    {"total_seconds": time.perf_counter()-started})
    else:
        if output.exists() and any(output.iterdir()):
            raise DatasetError("output is nonempty without an owned manifest; refusing overwrite.")
        candidates = None
        bindings = {"schema_version": 1, "instruments": []}
        quality = []
        g = spec.generation
        for entry in entries:
            source = {"type": "parquet", "version": 1, "pack_root": str(root),
                "instrument_id": entry["instrument_id"], "timeframe_minutes": g["market_data"]["timeframe_minutes"]}
            prepared = data_input.prepare_input(source, date_filter=True,
                start=windows[0].is_start.isoformat(), end=windows[-1].oos_end.isoformat(),
                warmup_bars=g["windows"]["warmup_bars"], admission_params=g["economics"]["base_params"], entry_filters=[])
            plan = build_plan(spec, prepared.admission_rules)
            projected = geometry(plan, g["strategy"])
            if candidates is not None and candidates != projected:
                raise DatasetError("per-instrument candidate geometry differs; remapping is unsupported.")
            candidates = projected
            b = {**entry, "input_pin": portable_pin(prepared.facts), "original_root": str(root),
                "entry_admission": prepared.entry_admission, "entry_filter_inputs": prepared.entry_filter_inputs,
                "plan_fingerprint": plan.plan_fingerprint, "semantic_key_digest": semantic_key_digest(plan), "segments": []}
            qentry = {"canonical_symbol": entry["instrument_id"], "cell": entry["cell"], "relative_filename": "", "sha256": ""}
            quality.append(quality_row(qentry, status="ok", first_timestamp=prepared.frame.index[0].isoformat(),
                last_timestamp=prepared.frame.index[-1].isoformat(), row_count=len(prepared.frame),
                expected_row_count=len(prepared.frame), message="consumed union validated"))
            for w in windows:
                for name in SEGMENT_AXIS:
                    part = segment(prepared, entry, w, name, spec)
                    b["segments"].append(segment_facts(part, source["timeframe_minutes"]))
                    quality.append(quality_row(qentry, window_id=w.window_id, segment=name, status="ok",
                        first_timestamp=part.first_timestamp, last_timestamp=part.last_timestamp,
                        row_count=part.evaluation_row_count, expected_row_count=part.evaluation_row_count,
                        warmup_row_count=part.trade_start_idx, trade_start_idx=part.trade_start_idx,
                        message="exact segment and warmup validated"))
                    del part
            bindings["instruments"].append(b)
            del prepared, plan
        validate_bindings(spec, entries, windows, candidates, bindings)
        expected_identity = identity(spec, entries, windows, candidates, bindings)
        for name, value in (("normalized_runspec.json", spec.raw), ("candidates.json", candidates), ("input_bindings.json", bindings)):
            atomic_write_json(output / name, value)
        atomic_write_quality_csv(output / "data_quality.csv", quality)
        manifest = {"schema_version": SCHEMA, "status": "partial", "scope": scope,
            "identity": expected_identity, "artifacts": {name: artifact_record(output/name, output) for name in SUPPORTING}, "groups": []}
        valid = {}
        atomic_write_json(partial_path, manifest)
    timings["preflight_seconds"] = time.perf_counter()-started
    reused = len(valid)
    if progress:
        progress(f"frozen {len(entries)} instrument(s), {len(windows)} window(s), {candidates['candidate_count']} candidates")
    if preflight_only:
        return GenerationResult(output, partial_path, scope, len(valid), reused, 0, False,
                                {**timings, "total_seconds": time.perf_counter()-started})
    _require_compiled_backend_available()
    module = importlib.import_module(get_strategy(spec.strategy_id).__module__)
    prior_threads = numba.get_num_threads()
    try:
        numba.set_num_threads(1)
        for entry, binding in zip(entries, bindings["instruments"]):
            pending = [w for w in windows if f"groups/{entry['instrument_id']}/window_{w.window_id:02d}.npy" not in valid]
            if not pending:
                continue
            prepared = replay(binding, root)
            plan = build_plan(spec, prepared.admission_rules)
            if plan.plan_fingerprint != binding["plan_fingerprint"] or geometry(plan, spec.generation["strategy"]) != candidates:
                raise DatasetError("replayed plan/geometry differs from frozen binding.")
            hooks = GridV2StrategyHooks.from_strategy(module, entry_filter_context=prepared.entry_filter_context)
            for w in pending:
                rows, backends = {}, {}
                for name in SEGMENT_AXIS:
                    part = segment(prepared, entry, w, name, spec)
                    executed = time.perf_counter()
                    run = execute_grid_v2_candidates(plan, part.dataframe, part.trade_start_idx, hooks,
                        candidate_indices=None, compute_sharpe=False, compute_sharpe_daily=True,
                        compute_sqn=True, compute_max_drawdown_mtm=True)
                    timings["execution_seconds"] += time.perf_counter()-executed
                    if run.selected:
                        raise DatasetError("unexpected selected Slow enrichment.")
                    backends[name] = _execution_backend_facts(run)
                    rows[name] = run.rows
                    del part, run
                matrix = group_matrix_v3(rows["is"], rows["oos"], plan)
                relative = f"groups/{entry['instrument_id']}/window_{w.window_id:02d}.npy"
                path = check_path(output, relative)
                atomic_write_group(path, matrix)
                record = group_record(path, output, matrix)
                record.update(instrument_id=entry["instrument_id"], plan_fingerprint=plan.plan_fingerprint,
                    input_binding_sha256=canonical_sha256(semantic_binding(binding)),
                    execution_backend={"backend_kind": "compiled_numba", "compiled_batch_used": True, "segments": backends})
                valid[relative] = record
                manifest["groups"] = [valid[p] for p in paths if p in valid]
                atomic_write_json(partial_path, manifest)
                if progress:
                    progress(f"published {len(valid)}/{len(paths)}: {relative}")
                if after_group:
                    after_group(len(valid), path)
            del prepared, plan, hooks
    finally:
        numba.set_num_threads(prior_threads)
    manifest["groups"] = [valid[p] for p in paths if p in valid]
    verify_artifacts(output, manifest)
    verified_groups(output, manifest, entries, windows, bindings, complete=True)
    head, dirty = _git_facts(repo)
    timings["total_seconds"] = time.perf_counter()-started
    manifest.update(status="complete", economics=spec.generation["economics"], execution_modes=spec.generation["execution"],
        provenance={"source_head": head, "dirty_worktree": dirty, "host": platform.node(),
            "resolved_data_root": str(root), "python_version": platform.python_version(), "timings": timings,
            "source_verification_policy": "frozen_preflight_replay_at_consumption_v1",
            "execution_backend": _backend_summary(manifest["groups"])})
    atomic_write_json(output / "manifest.json", manifest)
    partial_path.unlink()
    return GenerationResult(output, output / "manifest.json", scope, len(valid), reused, len(valid)-reused, False, timings)
